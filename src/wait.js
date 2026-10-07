import fs from 'node:fs';
import { ensureStateDirs, paths, readJsonSafe } from './state.js';
import { readLease, NOT_FOUND_GRACE_MS } from './lease.js';
import { listQueue } from './scheduler.js';
import { readAttempt, supervisorAlive, moveInterruptedLabel } from './attempts.js';
import { cancelCommand } from './cancel.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** `lane wait <id>`: reattach to a running/queued lease and block until its result exists. */
export async function waitCommand(id, { timeoutMs } = {}) {
  const root = ensureStateDirs().root;
  const resultPath = `${paths(root).results}/${id}.json`;

  let cancelling = false;
  // BRAIN-319 T3b-5: delegate to the SAME attempt-aware cancel logic
  // `lane cancel` uses -- forwarding straight to a lease's supervisorPid (as
  // this used to) never even looks at an attempt record, so Ctrl-C on a
  // `lane wait` reattached to a still mid-remote-dispatch (or post-fallback,
  // not-yet-leased) ticket was silently swallowed. `cancelCommand` covers
  // queued/leased/attempt (live or ORPHANED-REMOTE) uniformly; fired here
  // without awaiting it (a signal handler can't usefully await), while this
  // function's own polling loop below observes the eventual result the same
  // way it always does.
  const forwardCancel = () => {
    if (cancelling) return;
    cancelling = true;
    cancelCommand(id).catch(() => {
      // best-effort: any failure here still leaves the polling loop below to
      // report whatever state actually results.
    });
  };
  process.on('SIGINT', forwardCancel);
  process.on('SIGTERM', forwardCancel);

  const deadline = timeoutMs ? Date.now() + timeoutMs : null;
  // Same liveness check `lane run` uses: an id that is neither queued nor
  // leased nor resulted is not something we can ever wait for -- fail fast
  // instead of blocking forever on a typo. Tolerate a short grace window so
  // this doesn't race a `lane run --detach` whose supervisor hasn't finished
  // enqueueing yet. NOT_FOUND_GRACE_MS is shared with run.js's
  // describeLaneState (src/lease.js) -- one tolerance for this race, not two.
  let notFoundSince = null;
  try {
    for (;;) {
      const result = readJsonSafe(resultPath);
      if (result) {
        if (result.signal) {
          process.stderr.write(`lane wait: command terminated by signal ${result.signal}\n`);
          return { exitCode: 1 };
        }
        return { exitCode: result.exit ?? 1 };
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
        if (settledResult) {
          if (settledResult.signal) {
            process.stderr.write(`lane wait: command terminated by signal ${settledResult.signal}\n`);
            return { exitCode: 1 };
          }
          return { exitCode: settledResult.exit ?? 1 };
        }
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
          const label = moveLabel ?? (settledAttempt.executor === 'remote' ? 'ORPHANED-REMOTE' : 'ORPHANED (post-fallback)');
          process.stderr.write(
            `lane wait: ${id} is ${label} (runner ${settledAttempt.runner ?? 'unknown'}) -- its supervisor is gone and nothing ` +
              `will ever produce a result; reconcile with: lane cancel ${id}\n`,
          );
          return { exitCode: 1 };
        }
      }
      const found = lease || listQueue(root).some((t) => t && t.id === id) || Boolean(attempt);
      if (found) {
        notFoundSince = null;
      } else {
        if (notFoundSince === null) notFoundSince = Date.now();
        if (Date.now() - notFoundSince > NOT_FOUND_GRACE_MS) {
          process.stderr.write(`lane wait: no queued ticket, running lease, or result for ${id} — nothing to wait for\n`);
          return { exitCode: 1 };
        }
      }
      if (deadline && Date.now() > deadline) {
        process.stderr.write(`lane wait: waited ${timeoutMs}ms, not failed — id ${id} is still queued or running\n`);
        return { exitCode: 75 };
      }
      await sleep(200);
    }
  } finally {
    process.off('SIGINT', forwardCancel);
    process.off('SIGTERM', forwardCancel);
  }
}
