import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

export function stateHome() {
  return process.env.LANE_BROKER_STATE || path.join(os.homedir(), '.cache', 'lane-broker');
}

export function paths(root = stateHome()) {
  return {
    root,
    leases: path.join(root, 'leases'),
    queue: path.join(root, 'queue'),
    logs: path.join(root, 'logs'),
    results: path.join(root, 'results'),
    cancel: path.join(root, 'cancel'),
    history: path.join(root, 'history.jsonl'),
    pause: path.join(root, 'PAUSE'),
    lock: path.join(root, 'lock'),
    loadGate: path.join(root, 'load-gate.json'),
    seq: path.join(root, 'seq'),
  };
}

export function ensureStateDirs(root = stateHome()) {
  const p = paths(root);
  for (const dir of [p.root, p.leases, p.queue, p.logs, p.results, p.cancel]) {
    fs.mkdirSync(dir, { recursive: true });
  }
  return p;
}

/** Write `data` to `file` atomically: write to a temp file in the same dir, then rename. Never follows symlinks. */
export function atomicWriteFile(file, data) {
  const dir = path.dirname(file);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  fs.writeFileSync(tmp, data, { flag: 'wx' });
  fs.renameSync(tmp, file);
}

export function atomicWriteJson(file, obj) {
  atomicWriteFile(file, `${JSON.stringify(obj, null, 2)}\n`);
}

export function readJsonSafe(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return null;
  }
}

export function appendHistory(root, record) {
  try {
    fs.appendFileSync(paths(root).history, `${JSON.stringify(record)}\n`);
  } catch {
    // Disk-full or similar on the history log must never fail the lane.
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

/** Is the owner of the lock alive? Fails closed: an indeterminate probe never counts as stale. */
function isLockOwnerAlive(owner) {
  if (!owner) return false;
  if (!isPidAlive(owner.pid)) return false;
  if (!owner.start) return true; // couldn't capture a start time at acquire time; fall back to pid-alive
  const current = processStartTime(owner.pid);
  if (current === undefined) return true; // probe failed: fail closed, never steal
  if (current === null) return false; // confirmed gone
  return current === owner.start;
}

/**
 * Acquire the global mutex. Ownership is published atomically: the owner
 * file is written inside a staging directory first, then the whole staging
 * directory is renamed onto the lock dir path. A rename onto an existing
 * non-empty directory fails atomically (ENOTEMPTY/EEXIST/ENOTDIR), so a
 * contender can never observe a lock dir with no owner file — the two-step
 * mkdir-then-write race this replaces is exactly what let a mid-acquire lock
 * be mistaken for stale and stolen.
 *
 * Release is compare-and-remove: a lock is only removed by whoever's token
 * is recorded as its owner, so an evicted/timed-out holder's `finally` can
 * never delete a lock that has since been re-acquired by someone else.
 *
 * Recovery (stealing an existing lock) requires the owner's pid to be
 * confirmed dead (pid + start-time, to defeat pid reuse) — never elapsed
 * time alone, and never on an indeterminate liveness probe or an unreadable
 * owner file (both are treated as "young": back off and retry).
 */
export async function withLock(root, fn, { timeoutMs = 15_000, pollMs = 25 } = {}) {
  const lockDir = paths(root).lock;
  fs.mkdirSync(root, { recursive: true });
  const ownerFile = path.join(lockDir, 'owner.json');
  const deadline = Date.now() + timeoutMs;
  const token = crypto.randomUUID();
  const start = processStartTime(process.pid);

  for (;;) {
    const staging = path.join(root, `.lock-stage-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
    let acquired = false;
    try {
      fs.mkdirSync(staging, { recursive: true });
      fs.writeFileSync(
        path.join(staging, 'owner.json'),
        JSON.stringify({ pid: process.pid, token, start: start === undefined ? null : start, at: Date.now() }),
      );
      fs.renameSync(staging, lockDir);
      acquired = true;
    } catch {
      // Any failure here (contention, or a transient ENOENT/EINVAL racing a
      // concurrent recoverer) must never escape as a hard error — it is
      // treated as contention and retried below.
      try {
        fs.rmSync(staging, { recursive: true, force: true });
      } catch {
        // already gone
      }
    }
    if (acquired) break;

    const owner = readJsonSafe(ownerFile);
    if (!owner) {
      // Lock dir exists but its owner file is missing/unreadable: treat as
      // YOUNG (mid-acquire, or a foreign/legacy lock), never as stale.
      if (Date.now() > deadline) {
        throw new Error('lane-broker: timed out waiting for the global lock (owner unreadable)');
      }
      await sleep(pollMs);
      continue;
    }
    if (!isLockOwnerAlive(owner)) {
      try {
        fs.rmSync(lockDir, { recursive: true, force: true });
      } catch {
        // lost the race to another recoverer; loop and try again
      }
      continue;
    }
    if (Date.now() > deadline) {
      throw new Error(`lane-broker: timed out waiting for the global lock held by pid ${owner.pid}`);
    }
    await sleep(pollMs);
  }

  try {
    return await fn();
  } finally {
    const owner = readJsonSafe(ownerFile);
    if (owner && owner.token === token) {
      // A recursive rmSync on the live path is not atomic: it unlinks
      // owner.json, then rmdir's the now-empty lockDir as a second step. In
      // that gap a concurrent contender's rename(staging, lockDir) can land
      // on the momentarily-empty directory and succeed, so our rmdir then
      // fails on a directory someone else just repopulated (ENOTEMPTY).
      // Renaming the whole lock dir away first is one atomic syscall: the
      // "lock" path goes straight from existing-with-our-owner to gone.
      const trash = path.join(root, `.lock-release-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
      let renamedAway = false;
      try {
        fs.renameSync(lockDir, trash);
        renamedAway = true;
      } catch {
        // The rename itself failed (lockDir already gone, or some other
        // transient issue) — fall back to a guarded compare-and-remove: only
        // touch lockDir if it still shows our own token, since a contender
        // may have already acquired it.
        const current = readJsonSafe(ownerFile);
        if (current && current.token === token) {
          try {
            fs.rmSync(lockDir, { recursive: true, force: true });
          } catch {
            // already gone
          }
        }
      }
      if (renamedAway) {
        // We are released: the lockDir path is free (or already reclaimed by
        // a contender). Cleaning up the trash directory is best-effort — a
        // failure here is a leak, never a reason to touch lockDir again,
        // since a contender may already own that path by now.
        try {
          fs.rmSync(trash, { recursive: true, force: true });
        } catch {
          // leaked trash dir; ignore
        }
      }
    }
  }
}

let cachedBootId = process.env.LANE_BROKER_BOOT_ID || null;

/** A boot identifier that changes across a reboot: darwin sysctl, linux /proc/stat btime,
 *  or a computed epoch-boot-time fallback derived from os.uptime(). The fallback is rounded
 *  to the nearest 10s so two processes sampling Date.now()/os.uptime() microseconds apart
 *  agree on the same id instead of drifting by a second and reaping each other's leases. */
export function bootId() {
  if (process.env.LANE_BROKER_BOOT_ID) return process.env.LANE_BROKER_BOOT_ID;
  if (cachedBootId) return cachedBootId;
  try {
    if (process.platform === 'darwin') {
      const out = execFileSync('sysctl', ['-n', 'kern.boottime'], { encoding: 'utf8' });
      const m = out.match(/sec\s*=\s*(\d+)/);
      if (m) {
        cachedBootId = m[1];
        return cachedBootId;
      }
    } else if (process.platform === 'linux') {
      const out = fs.readFileSync('/proc/stat', 'utf8');
      const m = out.match(/^btime\s+(\d+)/m);
      if (m) {
        cachedBootId = m[1];
        return cachedBootId;
      }
    }
  } catch {
    // fall through to the computed fallback
  }
  const approxBootEpoch = Date.now() / 1000 - os.uptime();
  cachedBootId = String(Math.round(approxBootEpoch / 10) * 10);
  return cachedBootId;
}
