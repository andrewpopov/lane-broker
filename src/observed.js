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
  if (!validStats(stats)) return { peak: cores, area: 0, spanMs: 0, samples: 1, lastAt: now, lastCores: cores, firstAt: now, firstCores: cores };
  if (now <= stats.lastAt) return { ...stats, peak: Math.max(stats.peak, cores), samples: stats.samples + 1 };
  const dt = now - stats.lastAt;
  return {
    ...stats,
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

/**
 * BRAIN-425: CPU seconds the lease's process tree consumed, ESTIMATED by integrating the heartbeat samples
 * (core-ms area) and closing both ends: the first reading is held back to `startedAt` and the last forward to
 * `endedAt`. Not exact rusage: the supervisor cannot read reaped grandchildren's rusage, and `ps` pcpu is itself
 * a decayed average. Undefined with no valid observation (never a fabricated zero).
 */
export function integratedCpuSeconds(stats, startedAt, endedAt) {
  if (!validStats(stats)) return undefined;
  // the first reading's own time and cores (BRAIN-425), never a whole-span average: stats from before they were kept get no head
  const head = isNum(stats.firstAt) && isNum(stats.firstCores) && isNum(startedAt) && startedAt < stats.firstAt ? stats.firstCores * (stats.firstAt - startedAt) : 0;
  const tail = isNum(endedAt) && endedAt > stats.lastAt ? stats.lastCores * (endedAt - stats.lastAt) : 0;
  return round3((stats.area + head + tail) / 1000);
}

/** A relayed CPU-seconds figure, or undefined when it is not a finite non-negative number. */
export const sanitizeCpuSeconds = (v) => (isNum(v) && v >= 0 ? v : undefined);

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

const IDLE_ANCHOR_FIELDS = ['cpuIntervalCores', 'cpuIdleSince', 'cpuIdleLastAt', 'cpuHungSince', 'cpuHungLastAt'];

/**
 * BRAIN-532: one continuous-streak anchor, the overrun pattern. The streak continues only from a strictly later reading
 * within `maxGapMs` of the previous idle one; a non-idle reading drops it.
 */
function applyAnchor(lease, sinceKey, lastKey, isIdle, now, maxGapMs, fields, drop) {
  if (!isIdle) {
    drop.push(sinceKey, lastKey);
    return;
  }
  const gap = isNum(lease[lastKey]) ? now - lease[lastKey] : NaN;
  fields[sinceKey] = isNum(lease[sinceKey]) && gap > 0 && gap <= maxGapMs ? lease[sinceKey] : now;
  fields[lastKey] = now;
}

/**
 * BRAIN-532: the CPU-idle anchors, fed by the watchdog's per-interval cores (cumulative deltas, null on the first
 * observation, a null reading, or any process joining or leaving the tree, so churn resets both). `cpuIdleSince` tracks
 * intervals under `belowFraction` x the grant (credit candidates); `cpuHungSince` tracks intervals under the absolute `hungCores`.
 */
function applyIdleAnchors(lease, idle, now, maxGapMs, fields, drop) {
  const cores = idle?.intervalCores;
  const known = isNum(cores) && cores >= 0;
  if (known) fields.cpuIntervalCores = round3(cores);
  else drop.push('cpuIntervalCores');
  const grant = leaseCpuCores(lease);
  applyAnchor(lease, 'cpuIdleSince', 'cpuIdleLastAt', known && isNum(grant) && grant > 0 && cores < idle.belowFraction * grant, now, maxGapMs, fields, drop);
  applyAnchor(lease, 'cpuHungSince', 'cpuHungLastAt', known && cores < idle.hungCores, now, maxGapMs, fields, drop);
}

/**
 * BRAIN-532: what `lane status` says about a run's silence. `silentMs` is time since the command last wrote a byte
 * (the watchdog's own counter, immune to the log writer's discard mode; falls back to startedAt before any output).
 * HUNG? needs silence AND an unbroken CPU-idle streak, both >= hungAfterMs; SILENT is silence alone (a spinning hang).
 * Anchors older than three heartbeats are stale and count for nothing.
 */
export function leaseSilence(lease, now, cfg) {
  const silentSince = isNum(lease.lastOutputAt) ? lease.lastOutputAt : lease.startedAt;
  const silentMs = isNum(silentSince) ? Math.max(0, now - silentSince) : 0;
  const fresh = (lastKey) => isNum(lease[lastKey]) && now - lease[lastKey] <= 3 * cfg.sampleMs;
  const streak = (sinceKey, lastKey) => (fresh(lastKey) && isNum(lease[sinceKey]) ? lease[lastKey] - lease[sinceKey] : 0);
  const silent = silentMs >= cfg.hungAfterMs;
  const idleMs = streak('cpuIdleSince', 'cpuIdleLastAt');
  return {
    silentMs,
    cpu: isNum(lease.cpuIntervalCores) ? lease.cpuIntervalCores : isNum(lease.observedCpuCores) ? lease.observedCpuCores : null,
    idleMs,
    idle: idleMs >= cfg.idleCreditAfterMs,
    silent,
    hung: silent && streak('cpuHungSince', 'cpuHungLastAt') >= cfg.hungAfterMs,
  };
}

/**
 * The lease fields BRAIN-361 derives from one heartbeat: whole-run stats, the RSS peak and the overrun
 * anchor. Telemetry only, so it must never throw into the heartbeat: any failure drops every field.
 * `observed` null (a failed probe) resets the anchor; so does a gap since the last good reading longer
 * than `maxGapMs` (a stalled heartbeat or a clock jump), since the overrun must be continuous.
 */
export function observedLeaseFields(lease, observed, observedMemoryBytes, now, maxGapMs = OVERRUN_MAX_GAP_MS, idle = null) {
  try {
    const fields = {};
    const drop = [];
    applyIdleAnchors(lease, isNum(observed) ? idle : null, now, maxGapMs, fields, drop);
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
    return { fields: {}, drop: ['observedCpuStats', ...IDLE_ANCHOR_FIELDS, 'overrunSince', 'overrunPeak', 'overrunLastAt', 'observedRssPeakBytes'] };
  }
}
