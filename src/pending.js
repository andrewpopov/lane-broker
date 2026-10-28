import path from 'node:path';
import { paths, atomicWriteJson, appendHistory, readJsonSafe, withLock, bootId } from './state.js';
import { processStartTime } from './process-liveness.js';
import { supervisorAlive, readAttempt } from './attempts.js';
import { readLease } from './lease.js';
import { listQueue, listRebindingIds } from './scheduler.js';
import { terminalRowDefaults } from './resources.js';

/**
 * BRAIN-494: `lane run --detach` prints an id the moment the supervisor process exists, but the supervisor's own first record
 * (an attempt or a queue entry) lands later, after Node startup, imports and the state lock. A supervisor that dies in that gap
 * left an id nothing could find. The submitter therefore writes `pending/<id>.json` (fsynced) with the supervisor's identity
 * BEFORE printing the id; `lane wait` treats a live pending supervisor as found and, once it is dead with no other record, settles
 * the id (exit 75, `supervisor-died-before-register`: the command never started). Other records always win, so the pending file is
 * never consulted once the supervisor registered; `lane gc` prunes it with the logs and results.
 */
export function pendingFile(root, id) {
  return path.join(paths(root).pending, `${id}.json`);
}

/** Written by the submitter right after the supervisor spawned. `supervisor` has the shape `supervisorAlive` reads. */
export function writePending(root, { id, pid, logPath, cwd, repo, lane }) {
  atomicWriteJson(
    pendingFile(root, id),
    { id, logPath, cwd, repo, lane, createdAt: Date.now(), supervisor: { pid, startTime: processStartTime(pid) ?? null, bootId: bootId() } },
    { fsync: true },
  );
}

export const readPending = (root, id) => readJsonSafe(pendingFile(root, id));

/**
 * Settle a pending id whose supervisor died before it registered anything. Under the broker lock, so a supervisor that registered
 * (or published) between the caller's reads and now is never overridden. Returns `{ settled: boolean }`; never throws.
 */
export async function settleDeadPending(root, id) {
  try {
    const resultPath = path.join(paths(root).results, `${id}.json`);
    return await withLock(root, () => {
      const pending = readPending(root, id);
      if (!pending || supervisorAlive(pending)) return { settled: false };
      if (readJsonSafe(resultPath)) return { settled: true };
      const registered = readAttempt(root, id) || readLease(root, id) || listQueue(root).some((t) => t && t.id === id) || listRebindingIds(root).includes(id);
      if (registered) return { settled: false };
      const result = { id, exit: 75, signal: null, startedAt: null, endedAt: Date.now(), waitedMs: null, cancelled: false, reason: 'supervisor-died-before-register', reconciled: true };
      atomicWriteJson(resultPath, result);
      appendHistory(root, { ...terminalRowDefaults(null), ...result, repo: pending.repo, lane: pending.lane });
      return { settled: true };
    });
  } catch {
    return { settled: false };
  }
}
