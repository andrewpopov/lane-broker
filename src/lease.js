import { execFileSync } from 'node:child_process';
import path from 'node:path';
import fs from 'node:fs';
import { paths, atomicWriteJson, readJsonSafe, bootId } from './state.js';

export const LEASE_STATE = { RUNNING: 'RUNNING', ORPHANED: 'ORPHANED', DONE: 'DONE' };

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

export function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function isGroupAlive(pgid) {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Process start time, used to defeat PID reuse. Returns null if it cannot be determined. */
export function processStartTime(pid) {
  try {
    return execFileSync('ps', ['-o', 'lstart=', '-p', String(pid)], { encoding: 'utf8' }).trim() || null;
  } catch {
    return null;
  }
}

export function isSupervisorAlive(lease) {
  if (!isPidAlive(lease.supervisorPid)) return false;
  if (!lease.supervisorStart) return true; // couldn't capture a start time at write time; fall back to pid-alive
  const current = processStartTime(lease.supervisorPid);
  if (!current) return false; // process vanished between the kill(0) probe and the ps call
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
