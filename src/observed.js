import { leaseCpuCores } from './resources.js';

/**
 * BRAIN-361: what a lease ACTUALLY used, folded from the supervisor heartbeat's observations.
 * Report-only: nothing here is read by admission.
 */

/** A lease is flagged OVERRUN once its observed cores stay above this multiple of its CHARGED cores... */
export const OVERRUN_FACTOR = 1.25;
/** ...continuously for at least this long (observation timestamps, so one spike never flags). */
export const OVERRUN_MIN_MS = 2 * 60 * 1000;

const round3 = (n) => Math.round(n * 1000) / 1000;

/**
 * Fold one heartbeat reading into the lease's whole-run stats ({ peak, area, spanMs, samples,
 * lastAt, lastCores }). The mean is TIME-WEIGHTED: each reading is held until the next one, so
 * `area` accumulates previousCores * (now - previousAt) core-ms and mean = area / spanMs.
 * (observedCpuHistory is capped at 64 readings, so it cannot give a whole-run peak.)
 */
export function foldObservedCpu(stats, cores, now) {
  if (!stats) return { peak: cores, area: 0, spanMs: 0, samples: 1, lastAt: now, lastCores: cores };
  const dt = Math.max(0, now - stats.lastAt);
  return {
    peak: Math.max(stats.peak, cores),
    area: stats.area + stats.lastCores * dt,
    spanMs: stats.spanMs + dt,
    samples: stats.samples + 1,
    lastAt: now,
    lastCores: cores,
  };
}

/** The history-row shape `{ peak, mean, samples }` (cores, 3 decimals), or null with no observation. */
export function summarizeObservedCpu(stats) {
  if (!stats || !Number.isFinite(stats.peak) || !(stats.samples > 0)) return null;
  const mean = stats.spanMs > 0 ? stats.area / stats.spanMs : stats.lastCores;
  return { peak: round3(stats.peak), mean: round3(mean), samples: stats.samples };
}

/** True when `cores` exceeds OVERRUN_FACTOR x what the lease is charged (its grant, else its declaration). */
export function exceedsBooking(lease, cores) {
  const charged = leaseCpuCores(lease);
  return Number.isFinite(charged) && charged > 0 && cores > OVERRUN_FACTOR * charged;
}

/** `{ sinceMs, observedPeak }` for a lease that has been over its booking for >= OVERRUN_MIN_MS, else null. */
export function leaseOverrun(lease) {
  if (!Number.isFinite(lease.overrunSince) || !Number.isFinite(lease.observedAt)) return null;
  const sinceMs = lease.observedAt - lease.overrunSince;
  if (sinceMs < OVERRUN_MIN_MS) return null;
  return { sinceMs, observedPeak: round3(lease.overrunPeak ?? lease.observedCpuCores ?? 0) };
}
