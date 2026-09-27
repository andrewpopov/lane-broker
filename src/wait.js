import { ensureStateDirs, paths, readJsonSafe } from './state.js';
import { readLease, NOT_FOUND_GRACE_MS } from './lease.js';
import { listQueue } from './scheduler.js';
import { readAttempt, supervisorAlive } from './attempts.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** `lane wait <id>`: reattach to a running/queued lease and block until its result exists. */
export async function waitCommand(id, { timeoutMs } = {}) {
  const root = ensureStateDirs().root;
  const resultPath = `${paths(root).results}/${id}.json`;

  let cancelling = false;
  const forwardCancel = () => {
    if (cancelling) return;
    cancelling = true;
    const lease = readLease(root, id);
    if (lease && lease.supervisorPid) {
      try {
        process.kill(lease.supervisorPid, 'SIGTERM');
      } catch {
        // supervisor already gone
      }
    }
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
      // BRAIN-319 T3b-4 (C4): an attempt record still mid remote-dispatch
      // counts as "found" (no result yet, but something is genuinely
      // working on it) UNLESS its supervisor is confirmed dead --
      // ORPHANED-REMOTE never has a result coming, so waiting on it must
      // fail fast and name the reconciliation step, not sit in the same
      // not-found grace window a genuine typo would.
      const attempt = readAttempt(root, id);
      if (attempt && attempt.executor === 'remote' && !supervisorAlive(attempt)) {
        process.stderr.write(
          `lane wait: ${id} is ORPHANED-REMOTE (runner ${attempt.runner ?? 'unknown'}) -- its supervisor is gone and nothing ` +
            `will ever produce a result; reconcile with: lane cancel ${id}\n`,
        );
        return { exitCode: 1 };
      }
      const found = readLease(root, id) || listQueue(root).some((t) => t && t.id === id) || Boolean(attempt);
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
