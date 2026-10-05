import fs from 'node:fs';
import { paths, atomicWriteJson, readJsonSafe } from './state.js';

/**
 * BRAIN-379: when sim demand (a queued sim ticket or a charged sim lease) last existed, persisted per
 * state root so the sim soft lock stays armed for simArmWindowMs across supervisor processes.
 * Missing, unreadable or malformed reads as null: unarmed, no history. Never throws.
 */
export function readLastSimDemandAt(root) {
  const raw = readJsonSafe(paths(root).simArm);
  return raw && Number.isFinite(raw.lastSimDemandAt) ? raw.lastSimDemandAt : null;
}

const LOCK_WAIT_MS = 1000;
const LOCK_STALE_MS = 2000;
const LOCK_POLL_MS = 2;

const sleepSync = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

/**
 * A tiny exclusive lock file (O_EXCL create), held only across one read-compare-write. The broker's own lock is
 * async and the release path is not, so this is a separate, synchronous mutex. A lock older than LOCK_STALE_MS belongs
 * to a crashed holder and is broken. Returns whether it was acquired within `waitMs`.
 */
function acquireStampLock(file, waitMs) {
  for (let waited = 0; ; waited += LOCK_POLL_MS) {
    try {
      fs.closeSync(fs.openSync(file, 'wx'));
      return true;
    } catch (err) {
      if (err.code !== 'EEXIST') return false;
    }
    try {
      if (Date.now() - fs.statSync(file).mtimeMs > LOCK_STALE_MS) fs.unlinkSync(file);
    } catch {
      // the holder released it between our attempt and the stat
    }
    if (waited >= waitMs) return false;
    sleepSync(LOCK_POLL_MS);
  }
}

/**
 * Record sim demand at `now`. Genuinely monotonic: the read, the compare and the write happen under the stamp lock,
 * so whatever the interleaving the file ends at the maximum of every value ever offered and never moves back. Callers
 * hold the broker lock or not (the release path does not); this mutex is independent of it. If the lock cannot be had
 * within the wait (a wedged but not yet stale holder) the write is skipped: best-effort, shadow-grade state, "less
 * armed" at worst, and the next locked evaluation with sim demand re-stamps. Returns whether the stamp is now >= `now`.
 * `beforeLock` / `afterRead` are test seams to interleave a competing writer.
 */
export function touchSimArm(root, now = Date.now(), { beforeLock, afterRead, lockWaitMs = LOCK_WAIT_MS } = {}) {
  try {
    beforeLock?.();
    const lock = paths(root).simArmLock;
    if (!acquireStampLock(lock, lockWaitMs)) return false;
    try {
      const held = readLastSimDemandAt(root);
      afterRead?.(held);
      if (held === null || held < now) atomicWriteJson(paths(root).simArm, { lastSimDemandAt: now });
      return true;
    } finally {
      try {
        fs.unlinkSync(lock);
      } catch {
        // already broken as stale by another writer
      }
    }
  } catch {
    return false;
  }
}

/**
 * touchSimArm when the ticket or lease is a sim; the call every enqueue/admit/release/cancel/reap event makes.
 * Tests the class field directly rather than via allocation.js's classOf: that module imports lease.js,
 * which calls this one on a reap, and the cycle would read LEASE_STATE before it is initialised.
 */
export function touchSimArmFor(root, ticketOrLease, now = Date.now(), options) {
  if (ticketOrLease?.class === 'sim') touchSimArm(root, now, options);
}

/** How stale the stamp may get, as a fraction of the arm window, before a sim-bearing evaluation refreshes it. */
export const STAMP_REFRESH_FRACTION = 0.1;

/**
 * Reconcile the stamp from live state: a sim queued, or a sim lease held (RUNNING/ORPHANED, i.e. charged), means
 * demand exists now. Every demand EVENT already stamps, so this is only the repair for a lost or stale file and
 * writes only when the stamp is missing or older than a tenth of the window (the tail can then be at most that
 * much short), never once per poll. Returns the stamp as it stands afterwards.
 */
export function reconcileSimArm(root, queue, held, now, simArmWindowMs) {
  const stamp = readLastSimDemandAt(root);
  const demand = queue.some((t) => t?.class === 'sim') || held.some((l) => l.class === 'sim');
  if (demand && (stamp === null || now - stamp > simArmWindowMs * STAMP_REFRESH_FRACTION)) {
    touchSimArm(root, now);
    return Math.max(stamp ?? now, now);
  }
  return stamp;
}
