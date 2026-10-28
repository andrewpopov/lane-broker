import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, appendHistory, withLock } from './state.js';
import { readAttempt, listAttempts, supervisorAlive, publishTerminal } from './attempts.js';
import { readLease } from './lease.js';
import { listQueue, listRebindingIds, reapOrphanedQueue } from './scheduler.js';
import { terminalRowDefaults } from './resources.js';
import { writeBrokerLog } from './admission.js';

/**
 * BRAIN-605: settle a LOCAL-executor attempt (a remote attempt that fell back to local, or a rebind that stayed in the local queue)
 * whose supervisor died before the command was admitted. Nothing else ever does: `reconcileRemoteAttempt` skips a non-remote
 * attempt, and the queue reap only publishes for a ticket still in the queue, leaving the attempt record behind.
 *
 * Settled only when nothing could still produce a result: executor `local`, supervisor dead, no lease (an admitted run has its own
 * lease reaping), no queued or parked (rebinding) ticket, and no `moving` record. Exit 75, `supervisor-died-while-queued`: the
 * command never started, so the caller may rerun it. A result the queue reap already published stands; only the attempt record goes.
 *
 * Returns `{ settled: true, exit }` or `{ settled: false, reason }`; never throws.
 */
export async function settleAbandonedLocalAttempt(root, id) {
  try {
    await withLock(root, () => reapOrphanedQueue(root));
    const attempt = readAttempt(root, id);
    if (!attempt) return { settled: false, reason: 'no attempt record' };
    const resultPath = path.join(paths(root).results, `${id}.json`);
    let written = null;
    const published = await publishTerminal(
      root,
      id,
      attempt.generation,
      () => {
        if (fs.existsSync(resultPath)) return;
        written = { id, exit: 75, signal: null, executor: 'local', startedAt: null, endedAt: Date.now(), waitedMs: null, cancelled: false, reason: 'supervisor-died-while-queued', reconciled: true };
        atomicWriteJson(resultPath, written);
        appendHistory(root, { ...terminalRowDefaults(attempt), ...written, runner: attempt.runner ?? null });
      },
      { expect: (cur) => isAbandoned(root, cur) },
    );
    if (published.alreadyPublished) return { settled: true, exit: undefined };
    if (!published.ok) return { settled: false, reason: 'attempt changed or still owned' };
    if (written) writeBrokerLog(root, `lane: reconcile: ${id}: supervisor died while queued locally; settled as lost (exit 75)\n`);
    return { settled: true, exit: written?.exit };
  } catch (err) {
    return { settled: false, reason: err.message };
  }
}

/** Evaluated under the broker lock (inside `publishTerminal`'s `expect`). */
function isAbandoned(root, attempt) {
  return (
    attempt.executor === 'local' &&
    !supervisorAlive(attempt) &&
    !attempt.moving &&
    !readLease(root, attempt.id) &&
    !listQueue(root).some((t) => t && t.id === attempt.id) &&
    !listRebindingIds(root).includes(attempt.id)
  );
}

/** Every local-executor attempt with a dead supervisor, oldest first, settled one at a time. */
export async function settleAbandonedLocalAttempts(root) {
  const results = [];
  const candidates = listAttempts(root)
    .filter((a) => a.executor === 'local' && !supervisorAlive(a) && !a.moving)
    .sort((a, b) => (a.startedAt ?? 0) - (b.startedAt ?? 0));
  for (const attempt of candidates) {
    const outcome = await settleAbandonedLocalAttempt(root, attempt.id);
    if (outcome.settled) results.push({ id: attempt.id, runner: attempt.runner, state: 'lost', exit: outcome.exit, reason: 'supervisor-died-while-queued' });
  }
  return results;
}
