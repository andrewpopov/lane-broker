import { execFileSync } from 'node:child_process';

export function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Process start time, used to defeat PID reuse. Returns:
 *  - a string (the `ps` start-time line) if the process is alive,
 *  - `null` if `ps` ran and confirmed the pid does not exist,
 *  - `undefined` if the probe itself failed (e.g. cannot fork under load) —
 *    callers must treat this as "could not determine", never as "gone".
 * Pinned to the C locale so the output format is stable across environments.
 */
export function processStartTime(pid) {
  try {
    const out = execFileSync('ps', ['-o', 'lstart=', '-p', String(pid)], {
      encoding: 'utf8',
      env: { ...process.env, LC_ALL: 'C', LC_TIME: 'C' },
    }).trim();
    return out || null;
  } catch (err) {
    // `ps` ran and exited non-zero (pid not found) -> confirmed gone. A
    // spawn-level failure (EAGAIN under load, ENOENT, EPERM, ...) commonly
    // carries `status: null` too, so only a numeric status counts as "ran".
    if (typeof err.status === 'number') return null;
    // execFileSync itself failed to run `ps` -> indeterminate.
    return undefined;
  }
}

/**
 * Is the process that recorded `pid` and `startTime` still the one running? Fails closed: if the liveness probe itself
 * cannot be completed (e.g. `ps` cannot fork under load) the process counts as alive, so a transient probe failure
 * never looks like an exit. A record with no start time falls back to pid-alive.
 */
export function isProcessAlive(pid, startTime) {
  if (!isPidAlive(pid)) return false;
  if (!startTime) return true; // couldn't capture a start time at write time; fall back to pid-alive
  const current = processStartTime(pid);
  if (current === undefined) return true; // probe failed: fail closed
  if (current === null) return false; // confirmed gone
  return current === startTime;
}
