import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

const LOCK_STALE_MS = 30_000;

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

function isProcessAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Acquire the global mutex: an mkdir-atomic lock directory holding owner
 * pid + timestamp. Recoverable if the owner process is dead, or the lock
 * is older than LOCK_STALE_MS (a transaction should never legitimately
 * hold it that long).
 */
export async function withLock(root, fn, { timeoutMs = 15_000, pollMs = 25 } = {}) {
  const lockDir = paths(root).lock;
  fs.mkdirSync(root, { recursive: true });
  const ownerFile = path.join(lockDir, 'owner.json');
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      fs.mkdirSync(lockDir);
      fs.writeFileSync(ownerFile, JSON.stringify({ pid: process.pid, at: Date.now() }));
      break;
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      const owner = readJsonSafe(ownerFile);
      const stale = !owner || Date.now() - owner.at > LOCK_STALE_MS || !isProcessAlive(owner.pid);
      if (stale) {
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
  }
  try {
    return await fn();
  } finally {
    fs.rmSync(lockDir, { recursive: true, force: true });
  }
}

let cachedBootId = process.env.LANE_BROKER_BOOT_ID || null;

/** A boot identifier that changes across a reboot: darwin sysctl, linux /proc/stat btime,
 *  or a computed epoch-boot-time fallback derived from os.uptime(). */
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
  cachedBootId = String(Math.round(Date.now() / 1000 - os.uptime()));
  return cachedBootId;
}
