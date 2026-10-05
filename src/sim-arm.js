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

/** Record sim demand at `now`. Monotonic (never moves the stamp back) and best-effort: a failed write only means "less armed". */
export function touchSimArm(root, now = Date.now()) {
  try {
    const prev = readLastSimDemandAt(root);
    if (prev !== null && prev >= now) return;
    atomicWriteJson(paths(root).simArm, { lastSimDemandAt: now });
  } catch {
    // best-effort telemetry-grade state: shadow mode never lets this affect scheduling
  }
}

/**
 * touchSimArm when the ticket or lease is a sim; the call every enqueue/admit/release/cancel/reap event makes.
 * Tests the class field directly rather than via allocation.js's classOf: that module imports lease.js,
 * which calls this one on a reap, and the cycle would read LEASE_STATE before it is initialised.
 */
export function touchSimArmFor(root, ticketOrLease, now = Date.now()) {
  if (ticketOrLease?.class === 'sim') touchSimArm(root, now);
}

/** Reconcile the stamp from live state: a sim ticket queued, or a sim lease held (RUNNING/ORPHANED, i.e. charged), means demand exists now. */
export function reconcileSimArm(root, queue, held, now) {
  if (queue.some((t) => t?.class === 'sim') || held.some((l) => l.class === 'sim')) touchSimArm(root, now);
}
