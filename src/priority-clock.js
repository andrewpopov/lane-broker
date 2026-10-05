import { paths, atomicWriteJson, readJsonSafe, withLock, assertNotMigrating, clearStaleDrainMarker } from './state.js';

/**
 * BRAIN-380 priority clock. `hwm` is a broker-wide high-water mark of wall time, persisted in
 * the state root; `nowEff = max(wallNow, hwm)` is the only time aging reads, so a wall-clock
 * step backwards freezes aging instead of reversing it. Wall-clock `createdAt` and every
 * deadline derived from it are untouched.
 */

export function readHwm(root) {
  const record = readJsonSafe(paths(root).hwm);
  return Number.isFinite(record?.hwm) ? record.hwm : 0;
}

/** Read-only effective time, for `lane status`: it must never write the mark. */
export function effectiveNow(root, wallNow = Date.now()) {
  return Math.max(wallNow, readHwm(root));
}

/**
 * Advance the mark to `wallNow` and return `nowEff`. Caller MUST hold the broker lock (every
 * locked evaluation and every enqueue does), which is what makes the read-modify-write safe.
 * A failed write degrades to an unadvanced mark: it can only under-state `nowEff`, which never
 * invents age, and it must not fail an admission.
 */
export function advanceHwm(root, wallNow = Date.now()) {
  const stored = readHwm(root);
  if (wallNow <= stored) return stored;
  try {
    atomicWriteJson(paths(root).hwm, { hwm: wallNow });
  } catch {
    // best effort, see above
  }
  return wallNow;
}

/**
 * Ticket creation: take the broker lock briefly, advance the mark and return `nowEff`, the
 * ticket's `prioOriginAt`, in one transaction. Caller must NOT hold the lock. Throws `MigrationInProgressError`
 * while `lane migrate-scheduler` is running or draining: ticket creation is a new-code admission entry point.
 */
export async function stampPriorityOrigin(root) {
  return withLock(root, () => {
    clearStaleDrainMarker(root); // the next `lane run` after a SIGKILLed `--when-idle` clears its marker
    assertNotMigrating(root);
    return advanceHwm(root);
  });
}
