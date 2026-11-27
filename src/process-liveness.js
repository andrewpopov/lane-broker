import { execFile, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import { PS_TIMEOUT_MS } from '@andrewpopov/bosun/module';

/** The state letter in a `/proc/<pid>/stat` line: the first field after the LAST `)`, since comm may itself contain parens. */
export function procStatState(stat) {
  const afterComm = String(stat).slice(String(stat).lastIndexOf(')') + 1).trimStart();
  return afterComm.length > 0 && afterComm[1] === ' ' ? afterComm[0] : null;
}

function isZombie(pid) {
  if (process.platform !== 'linux') return false;
  try {
    return procStatState(fs.readFileSync(`/proc/${pid}/stat`, 'utf8')) === 'Z';
  } catch {
    return false;
  }
}

export function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
  } catch {
    return false;
  }
  return !isZombie(pid);
}

/**
 * Process start time, used to defeat PID reuse. Returns:
 *  - a string if the process is alive: `ticks:<n>` from /proc on Linux, else the `ps` start-time line,
 *  - `null` if `ps` ran and confirmed the pid does not exist,
 *  - `undefined` if the probe itself failed (e.g. cannot fork under load) —
 *    callers must treat this as "could not determine", never as "gone".
 * Pinned to the C locale so the output format is stable across environments.
 */
export function processStartTime(pid, { timeoutMs = PS_TIMEOUT_MS } = {}) {
  return process.platform === 'linux' ? procStartTicks(pid) : psStartTime(pid, { timeoutMs });
}

/** Linux: `/proc/<pid>/stat` field 22, the start time in clock ticks since boot. Unlike `ps -o lstart=`, which adds the
 *  kernel's boot time and so moves whenever WSL or NTP steps the clock, it never changes for a live process.
 *  `null` when the pid is gone, `undefined` when /proc cannot answer: never a `ps` string, so a token's format is fixed per platform. */
export function procStartTicks(pid, readFile = fs.readFileSync) {
  let stat;
  try {
    stat = readFile(`/proc/${pid}/stat`, 'utf8');
  } catch (err) {
    return err.code === 'ENOENT' || err.code === 'ESRCH' ? null : undefined;
  }
  const ticks = stat.slice(stat.lastIndexOf(')') + 2).split(' ')[19];
  return /^\d+$/.test(ticks ?? '') ? `${TICKS_PREFIX}${ticks}` : undefined;
}

const TICKS_PREFIX = 'ticks:';

export function psStartTime(pid, { timeoutMs = PS_TIMEOUT_MS } = {}) {
  try {
    // A stuck ps is SIGKILLed at the timeout (it may ignore SIGTERM) and reads as indeterminate, so it never holds the event loop.
    const out = execFileSync('ps', ['-o', 'lstart=', '-p', String(pid)], {
      encoding: 'utf8',
      env: { ...process.env, LC_ALL: 'C', LC_TIME: 'C' },
      timeout: timeoutMs,
      killSignal: 'SIGKILL',
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

/** Does `recorded` still name the process now running as `pid`? `true`, `false`, `null` (the probe says the pid is gone) or
 *  `undefined` (the probe could not answer). A `ticks:` token is read back from /proc; any other token is a `ps` string and is
 *  compared exactly, as before: hosts upgrade drained, so no live process holds one on Linux. */
export function sameProcessStart(pid, recorded) {
  const current = String(recorded).startsWith(TICKS_PREFIX) ? procStartTicks(pid) : psStartTime(pid);
  if (current === undefined || current === null) return current;
  return current === recorded;
}

/**
 * `processStartTime` for many pids in one non-blocking `ps`: a Map of pid to start line, where a pid `ps` did not list is absent, or undefined when the
 * probe could not complete (spawn failure, timeout, or `signal` aborted it).
 */
export function processStartTimes(pids, { timeoutMs = PS_TIMEOUT_MS, signal } = {}) {
  if (pids.length === 0) return Promise.resolve(new Map());
  return new Promise((resolve) => {
    const options = { encoding: 'utf8', env: { ...process.env, LC_ALL: 'C', LC_TIME: 'C' }, timeout: timeoutMs, killSignal: 'SIGKILL', ...(signal ? { signal } : {}) };
    execFile('ps', ['-o', 'pid=,lstart=', '-p', pids.join(',')], options, (err, stdout) => {
      if (err && typeof err.code !== 'number') return resolve(undefined);
      const starts = new Map();
      for (const line of stdout.split('\n')) {
        const match = /^\s*(\d+)\s+(.*\S)\s*$/.exec(line);
        if (match) starts.set(Number(match[1]), match[2]);
      }
      resolve(starts);
    });
  });
}

/**
 * Is the process that recorded `pid` and `startTime` still the one running? Fails closed: if the liveness probe itself
 * cannot be completed (e.g. `ps` cannot fork under load) the process counts as alive, so a transient probe failure
 * never looks like an exit. A record with no start time falls back to pid-alive.
 */
export function isProcessAlive(pid, startTime) {
  if (!isPidAlive(pid)) return false;
  if (!startTime) return true; // couldn't capture a start time at write time; fall back to pid-alive
  const same = sameProcessStart(pid, startTime);
  if (same === undefined) return true; // probe failed: fail closed
  if (same === null) return isPidAlive(pid); // the probe alone cannot declare a pid that kill(0) just saw alive dead
  return same;
}

/**
 * Positive identity: the pid is alive AND its readable start time equals the recorded one. A missing recorded start time or a failed probe
 * is NOT identity, so this is the only gate that may authorise a destructive signal (`isProcessAlive` fails closed the other way, for liveness).
 */
export function isVerifiedProcess(pid, startTime) {
  if (!startTime || !isPidAlive(pid)) return false;
  return sameProcessStart(pid, startTime) === true;
}

/** Positively gone: the pid no longer signals, or its readable start token differs from the recorded one. A failed probe, or no recorded start time, is not gone. */
export function isConfirmedGone(pid, startTime) {
  if (!isPidAlive(pid)) return true;
  if (!startTime) return false;
  return sameProcessStart(pid, startTime) === false;
}
