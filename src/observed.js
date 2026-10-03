import { leaseCpuCores } from './resources.js';

/**
 * BRAIN-361: what a lease ACTUALLY used, folded from the supervisor heartbeat's observations.
 * Report-only: nothing here is read by admission.
 */

/** A lease is flagged OVERRUN once its observed cores stay above this multiple of its CHARGED cores... */
export const OVERRUN_FACTOR = 1.25;
/** ...continuously for at least this long (observation timestamps, so one spike never flags). */
export const OVERRUN_MIN_MS = 2 * 60 * 1000;
/** An overrun must be CONTINUOUS: two default (5s) heartbeats between good readings is the longest gap it bridges. */
export const OVERRUN_MAX_GAP_MS = 10_000;

const round3 = (n) => Math.round(n * 1000) / 1000;

/** Persisted lease/result JSON is untrusted: arithmetic only ever touches values that pass this. */
const isNum = (v) => typeof v === 'number' && Number.isFinite(v);

const validStats = (s) =>
  s !== null && typeof s === 'object' && isNum(s.peak) && isNum(s.area) && isNum(s.spanMs) && isNum(s.samples) && isNum(s.lastAt) && isNum(s.lastCores) && s.samples > 0;

/**
 * Fold one heartbeat reading into the lease's whole-run stats ({ peak, area, spanMs, samples,
 * lastAt, lastCores }). The mean is TIME-WEIGHTED: each reading is held until the next one, so
 * `area` accumulates previousCores * (now - previousAt) core-ms and mean = area / spanMs.
 * (observedCpuHistory is capped at 64 readings, so it cannot give a whole-run peak.)
 * Malformed persisted stats are discarded and restarted. A reading whose time is not after
 * `lastAt` (clock stepped back) still counts toward peak and samples but is not integrated, and
 * `lastAt` never moves backwards, so no interval is counted twice when time catches up.
 */
export function foldObservedCpu(stats, cores, now) {
  if (!validStats(stats)) return { peak: cores, area: 0, spanMs: 0, samples: 1, lastAt: now, lastCores: cores };
  if (now <= stats.lastAt) return { ...stats, peak: Math.max(stats.peak, cores), samples: stats.samples + 1 };
  const dt = now - stats.lastAt;
  return {
    peak: Math.max(stats.peak, cores),
    area: stats.area + stats.lastCores * dt,
    spanMs: stats.spanMs + dt,
    samples: stats.samples + 1,
    lastAt: now,
    lastCores: cores,
  };
}

/** The history-row shape `{ peak, mean, samples }` (cores, 3 decimals), or null with no valid observation. */
export function summarizeObservedCpu(stats) {
  if (!validStats(stats)) return null;
  const mean = stats.spanMs > 0 ? stats.area / stats.spanMs : stats.lastCores;
  return { peak: round3(stats.peak), mean: round3(mean), samples: stats.samples };
}

/** A relayed `observedCpu` rebuilt from its three numeric fields, or null when any is missing or malformed. */
export function sanitizeObservedCpu(o) {
  if (o === null || typeof o !== 'object' || !isNum(o.peak) || !isNum(o.mean) || !isNum(o.samples)) return null;
  return { peak: o.peak, mean: o.mean, samples: o.samples };
}

/** A relayed RSS peak, or undefined when it is not a finite number. */
export const sanitizeRssPeak = (v) => (isNum(v) ? v : undefined);

/** True when `cores` exceeds OVERRUN_FACTOR x what the lease is charged (its grant, else its declaration). */
export function exceedsBooking(lease, cores) {
  const charged = leaseCpuCores(lease);
  return isNum(charged) && charged > 0 && cores > OVERRUN_FACTOR * charged;
}

/** `{ sinceMs, observedPeak }` for a lease that has been over its booking for >= OVERRUN_MIN_MS, else null. */
export function leaseOverrun(lease) {
  if (!isNum(lease.overrunSince) || !isNum(lease.observedAt)) return null;
  const sinceMs = lease.observedAt - lease.overrunSince;
  if (sinceMs < OVERRUN_MIN_MS) return null;
  return { sinceMs, observedPeak: round3(isNum(lease.overrunPeak) ? lease.overrunPeak : isNum(lease.observedCpuCores) ? lease.observedCpuCores : 0) };
}

/**
 * The lease fields BRAIN-361 derives from one heartbeat: whole-run stats, the RSS peak and the overrun
 * anchor. Telemetry only, so it must never throw into the heartbeat: any failure drops every field.
 * `observed` null (a failed probe) resets the anchor; so does a gap since the last good reading longer
 * than `maxGapMs` (a stalled heartbeat or a clock jump), since the overrun must be continuous.
 */
export function observedLeaseFields(lease, observed, observedMemoryBytes, now, maxGapMs = OVERRUN_MAX_GAP_MS) {
  try {
    const fields = {};
    const drop = [];
    if (isNum(observed)) {
      fields.observedCpuStats = foldObservedCpu(lease.observedCpuStats, observed, now);
      // continuity is measured from the previous OVER-BOOKING reading's own time (not the stats' integration
      // boundary, which never moves back), and only a strictly later reading continues: a rolled-back or equal clock restarts the streak
      const gap = isNum(lease.overrunLastAt) ? now - lease.overrunLastAt : NaN;
      const continuing = isNum(lease.overrunSince) && gap > 0 && gap <= maxGapMs;
      if (exceedsBooking(lease, observed)) {
        fields.overrunSince = continuing ? lease.overrunSince : now;
        fields.overrunLastAt = now;
        fields.overrunPeak = Math.max(continuing && isNum(lease.overrunPeak) ? lease.overrunPeak : 0, observed);
      } else drop.push('overrunSince', 'overrunPeak', 'overrunLastAt');
    } else drop.push('overrunSince', 'overrunPeak', 'overrunLastAt');
    if (isNum(observedMemoryBytes)) fields.observedRssPeakBytes = Math.max(isNum(lease.observedRssPeakBytes) ? lease.observedRssPeakBytes : 0, observedMemoryBytes);
    return { fields, drop };
  } catch {
    return { fields: {}, drop: ['observedCpuStats', 'overrunSince', 'overrunPeak', 'overrunLastAt', 'observedRssPeakBytes'] };
  }
}
