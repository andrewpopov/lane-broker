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

const MAX_WRITE_ATTEMPTS = 4;

/**
 * Record sim demand at `now`. Monotonic: the stamp never moves back. The release path calls this without the
 * broker lock, so a plain read-check-write could be clobbered by a slower writer carrying an older time; each
 * write is therefore an atomic temp-then-rename followed by a re-read, repeated (a small, fixed bound) while the
 * file still holds a value older than ours. One window remains (a clobber landing after our last re-read) and it
 * is deliberately accepted: it can only shorten the tail by the gap between two near-simultaneous events, and the
 * next locked evaluation with sim demand re-stamps. Best-effort: a failed write only means "less armed".
 * `afterWrite` is a test seam to interleave a competing writer.
 */
export function touchSimArm(root, now = Date.now(), { afterWrite } = {}) {
  try {
    for (let attempt = 0; attempt < MAX_WRITE_ATTEMPTS; attempt++) {
      const held = readLastSimDemandAt(root);
      if (held !== null && held >= now) return;
      atomicWriteJson(paths(root).simArm, { lastSimDemandAt: now });
      afterWrite?.(attempt);
    }
  } catch {
    // best-effort: shadow-grade state, never allowed to affect scheduling
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
