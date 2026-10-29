import fs from 'node:fs';
import { ensureStateDirs, paths, readJsonSafe } from './state.js';
import { readLease, NOT_FOUND_GRACE_MS } from './lease.js';
import { listQueue } from './scheduler.js';
import { readAttempt, supervisorAlive, moveInterruptedLabel } from './attempts.js';
import { cancelCommand } from './cancel.js';
import { loadGlobalConfigOrError } from './config.js';
import { reconcileRemoteAttempt } from './remote-reconcile.js';
import { settleAbandonedLocalAttempt } from './local-reconcile.js';
import { readPending, settleDeadPending } from './pending.js';

const RECONCILE_SSH_DEADLINE_MS = 30_000;
// Under this much of --timeout left, asking the runner cannot finish in time: keep polling so the timeout path answers 75.
const RECONCILE_MIN_REMAINING_MS = 1_000;
// Test-only seam (README "Testing hooks"): how long a runner-reported-running orphan waits before asking the runner again.
const reconcileIntervalMs = () => Number(process.env.LANE_BROKER_TEST_RECONCILE_INTERVAL_MS) || 30_000;

/** BRAIN-515: ask the runner about an ORPHANED-REMOTE attempt, never past `deadline` (ms epoch, or null). An unreadable global config means the runner cannot be reached. */
async function reconcileOrphan(root, id, deadline) {
  const { config, error } = loadGlobalConfigOrError();
  if (error) return { settled: false, state: 'unreachable', reason: `global config unreadable (${error})` };
  return reconcileRemoteAttempt(root, id, { globalCfg: config, deadlineMs: RECONCILE_SSH_DEADLINE_MS, deadlineAt: deadline ?? undefined });
}

/** The exit `lane wait` reports for a published result. A reconciled-as-lost result (exit 75) says why on stderr (BRAIN-594). */
function resultExit(id, result) {
  if (result.signal) {
    process.stderr.write(`lane wait: command terminated by signal ${result.signal}\n`);
    return { exitCode: 1 };
  }
  if (result.reconciled && result.reason) {
    process.stderr.write(`lane wait: ${id} was settled as lost (${result.reason}${result.lostDetail ? `: ${result.lostDetail}` : ''}); the job's outcome is unknown\n`);
  }
  return { exitCode: result.exit ?? 1 };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** `lane wait <id>`: reattach to a running/queued lease and block until its result exists. */
export async function waitCommand(id, { timeoutMs } = {}) {
  const root = ensureStateDirs().root;
  const resultPath = `${paths(root).results}/${id}.json`;

  let cancelling = false;
  let cancelFailed = false;
  // BRAIN-319 T3b-5: delegate to the SAME attempt-aware cancel logic
  // `lane cancel` uses -- forwarding straight to a lease's supervisorPid (as
  // this used to) never even looks at an attempt record, so Ctrl-C on a
  // `lane wait` reattached to a still mid-remote-dispatch (or post-fallback,
  // not-yet-leased) ticket was silently swallowed. `cancelCommand` covers
  // queued/leased/attempt (live or ORPHANED-REMOTE) uniformly; fired here
  // without awaiting it (a signal handler can't usefully await), while this
  // function's own polling loop below observes the eventual result the same
  // way it always does. A cancel that did not succeed (nonzero exit, or a
  // throw) is remembered and `cancelling` is cleared so a later signal tries
  // again; the loop reports it rather than polling a job nobody can stop.
  const forwardCancel = () => {
    if (cancelling) return;
    cancelling = true;
    const failed = () => {
      cancelling = false;
      cancelFailed = true;
    };
    cancelCommand(id).then((res) => {
      if (res?.exitCode !== 0) failed();
    }, failed);
  };
  process.on('SIGINT', forwardCancel);
  process.on('SIGTERM', forwardCancel);

  const deadline = timeoutMs ? Date.now() + timeoutMs : null;
  const timedOutResult = () => {
    process.stderr.write(`lane wait: waited ${timeoutMs}ms, not failed — id ${id} is still queued or running\n`);
    return { exitCode: 75 };
  };
  // Same liveness check `lane run` uses: an id that is neither queued nor
  // leased nor resulted is not something we can ever wait for -- fail fast
  // instead of blocking forever on a typo. Tolerate a short grace window so
  // this doesn't race a `lane run --detach` whose supervisor hasn't finished
  // enqueueing yet. NOT_FOUND_GRACE_MS is shared with run.js's
  // describeLaneState (src/lease.js) -- one tolerance for this race, not two.
  let notFoundSince = null;
  let lastReconcileAt = null;
  try {
    for (;;) {
      const result = readJsonSafe(resultPath);
      if (result) return resultExit(id, result);
      if (cancelFailed) {
        process.stderr.write(`lane wait: cancel of ${id} not confirmed (attempt kept); retry lane cancel ${id}\n`);
        return { exitCode: 1 };
      }
      // BRAIN-319 T3b-4/T3b-5 (C4), fixed by P1 (Codex re-review): an attempt
      // record still mid remote-dispatch, still queued after a local
      // fallback, OR already admitted and running (leased) after that
      // fallback all count as "found" (no result yet, but something is
      // genuinely working on it) UNLESS the attempt's OWN recorded
      // supervisor (pid + start time + boot id) is confirmed dead -- fixed
      // to ANY attempt executor, not just 'remote', and decided purely from
      // `supervisorAlive(attempt)`, never from lease presence: `readLease`
      // never reaps, so an admitted fallback whose supervisor died while its
      // lease is still held would otherwise sit RUNNING forever and this
      // loop would poll it forever too, even though nobody is left to react
      // to the child's eventual 'close' event and write a result. Reported
      // here, never acted on: no kill, no lease release -- that stays `lane
      // cancel`'s job (named in the message below).
      const lease = readLease(root, id);
      const attempt = readAttempt(root, id);
      if (attempt && !supervisorAlive(attempt)) {
        // BRAIN-319 orphan-race fix: the two reads above (result, then
        // attempt+liveness) are not atomic with the supervisor's own
        // publish-then-exit (`publishTerminal` writes result.json and
        // removes the attempt record, then the process exits) -- a
        // supervisor that does exactly that BETWEEN this iteration's
        // earlier `readJsonSafe(resultPath)` (which found nothing) and
        // this liveness check can die (so `supervisorAlive` now reports
        // false) in a run that actually SUCCEEDED. Re-read the result
        // here, right before declaring an orphan: if it showed up in that
        // gap, this is a completed run observed mid-publish, not an
        // orphan. Test-only seam: `LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN`
        // widens this exact gap deterministically (see README's "Testing
        // hooks").
        const pauseFile = process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN;
        if (pauseFile) {
          while (!fs.existsSync(pauseFile)) {
            await sleep(10);
          }
        }
        const settledResult = readJsonSafe(resultPath);
        if (settledResult) return resultExit(id, settledResult);
        // Still no result: re-read the attempt too, in case
        // `publishTerminal` removed it in the same gap without us having
        // observed the result yet (its own write is what we just missed).
        // A re-read that now finds no attempt at all is not an orphan --
        // fall through to the ordinary queued/leased/found check below,
        // which will keep polling (or fail via the NOT_FOUND_GRACE_MS path
        // if it's genuinely gone from everywhere).
        const settledAttempt = readAttempt(root, id);
        if (settledAttempt && !supervisorAlive(settledAttempt)) {
          const moveLabel = moveInterruptedLabel(id, settledAttempt);
          // BRAIN-605: a post-fallback attempt that never reached admission is settled (exit 75) rather than reported forever
          if (!moveLabel && settledAttempt.executor === 'local' && (await settleAbandonedLocalAttempt(root, id)).settled) continue;
          if (moveLabel || settledAttempt.executor !== 'remote') {
            const label = moveLabel ?? 'ORPHANED (post-fallback)';
            process.stderr.write(
              `lane wait: ${id} is ${label} (runner ${settledAttempt.runner ?? 'unknown'}) -- its supervisor is gone and nothing ` +
                `will ever produce a result; reconcile with: lane cancel ${id}\n`,
            );
            return { exitCode: 1 };
          }
          // ORPHANED-REMOTE: the runner knows what happened to the job, so ask it (never re-runs anything). A settled
          // outcome is read back by the top of the loop; a job the runner still has keeps polling like any in-flight
          // ticket, asking again at most every reconcileIntervalMs().
          if ((lastReconcileAt === null || Date.now() - lastReconcileAt >= reconcileIntervalMs()) && !(deadline && deadline - Date.now() < RECONCILE_MIN_REMAINING_MS)) {
            const outcome = await reconcileOrphan(root, id, deadline);
            if (outcome.settled) continue;
            // out of time is not an orphan verdict: the ordinary timeout answer (75) applies
            if (outcome.timedOut || (deadline && Date.now() > deadline)) return timedOutResult();
            if (outcome.state === 'running') lastReconcileAt = Date.now();
            else if (outcome.state !== 'skipped') {
              const runnerName = settledAttempt.runner ?? 'unknown';
              process.stderr.write(
                `lane wait: ${id} is ORPHANED-REMOTE (runner ${runnerName}) -- its supervisor is gone and the runner could not settle it ` +
                  `(${outcome.state}${outcome.reason ? `: ${outcome.reason}` : ''}); it will be reconciled automatically once ${runnerName} ` +
                  `answers (lane status); or: lane cancel ${id}\n`,
              );
              return { exitCode: 1 };
            }
          }
        }
      }
      // BRAIN-494: a detached id whose supervisor has not registered yet is found while that supervisor lives; once it is dead the id settles
      let pendingFound = false;
      if (!lease && !attempt && readPending(root, id)) {
        if ((await settleDeadPending(root, id)).settled) continue;
        pendingFound = true;
      }
      const found = pendingFound || lease || listQueue(root).some((t) => t && t.id === id) || Boolean(attempt);
      if (found) {
        notFoundSince = null;
      } else {
        if (notFoundSince === null) notFoundSince = Date.now();
        if (Date.now() - notFoundSince > NOT_FOUND_GRACE_MS) {
          process.stderr.write(`lane wait: no queued ticket, running lease, or result for ${id} — nothing to wait for\n`);
          return { exitCode: 1 };
        }
      }
      if (deadline && Date.now() > deadline) return timedOutResult();
      await sleep(200);
    }
  } finally {
    process.off('SIGINT', forwardCancel);
    process.off('SIGTERM', forwardCancel);
  }
}
