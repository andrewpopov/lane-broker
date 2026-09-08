import path from 'node:path';
import fs from 'node:fs';
import { paths, atomicWriteJson, readJsonSafe, bootId, isPidAlive, processStartTime } from './state.js';

export { isPidAlive, processStartTime };

export const LEASE_STATE = { RUNNING: 'RUNNING', ORPHANED: 'ORPHANED', DONE: 'DONE' };

/**
 * How long a reader tolerates a ticket id being neither queued, leased, nor
 * resulted before concluding it doesn't exist. A `lane run`/`lane wait`
 * supervisor is spawned before it has enqueued its own ticket, and
 * `tryStart` (scheduler.js) dequeues a ticket and only then writes its lease
 * -- an unlocked reader can land in either gap. One shared constant so both
 * readers (src/wait.js, src/run.js) tolerate the same race the same way,
 * rather than each guessing its own window.
 */
export const NOT_FOUND_GRACE_MS = 3000;

export function leaseFile(root, id) {
  return path.join(paths(root).leases, `${id}.json`);
}

export function writeLease(root, lease) {
  atomicWriteJson(leaseFile(root, lease.id), lease);
}

export function readLease(root, id) {
  return readJsonSafe(leaseFile(root, id));
}

export function removeLease(root, id) {
  try {
    fs.unlinkSync(leaseFile(root, id));
  } catch {
    // already gone
  }
}

export function listLeases(root) {
  const dir = paths(root).leases;
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return [];
  }
  return names
    .filter((n) => n.endsWith('.json'))
    .map((n) => readJsonSafe(path.join(dir, n)))
    .filter(Boolean);
}

export function isGroupAlive(pgid) {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Is the supervisor that holds this lease still alive? Fails closed: if the
 * liveness probe itself cannot be completed (e.g. `ps` cannot fork under
 * load), the lease is kept rather than declared dead — a transient probe
 * failure must never look like the supervisor exited.
 */
export function isSupervisorAlive(lease) {
  if (!isPidAlive(lease.supervisorPid)) return false;
  if (!lease.supervisorStart) return true; // couldn't capture a start time at write time; fall back to pid-alive
  const current = processStartTime(lease.supervisorPid);
  if (current === undefined) return true; // probe failed: fail closed, keep the lease
  if (current === null) return false; // confirmed gone
  return current === lease.supervisorStart;
}

/**
 * Apply the exact reap rule to one lease. Returns one of:
 *  - 'kept'    the lease is untouched
 *  - 'reaped'  removed: boot changed, or (supervisor dead AND group gone)
 *  - 'orphaned' marked ORPHANED: supervisor dead but the child group is alive (never auto-removed)
 */
export function reapIfStale(root, lease, currentBootId = bootId()) {
  if (lease.bootId !== currentBootId) {
    removeLease(root, lease.id);
    return 'reaped';
  }
  if (isSupervisorAlive(lease)) return 'kept';
  if (!isGroupAlive(lease.childPgid)) {
    removeLease(root, lease.id);
    return 'reaped';
  }
  if (lease.state !== LEASE_STATE.ORPHANED) {
    writeLease(root, { ...lease, state: LEASE_STATE.ORPHANED });
  }
  return 'orphaned';
}

/** Reap/orphan every lease on disk. Caller must hold the global lock. */
export function reapAll(root, currentBootId = bootId()) {
  const results = [];
  for (const lease of listLeases(root)) {
    results.push({ id: lease.id, action: reapIfStale(root, lease, currentBootId) });
  }
  return results;
}
