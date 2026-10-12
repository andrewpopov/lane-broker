import { OVERRUN_MIN_MS } from './observed.js';
import { classifyRow, percentile } from './estimates-history.js';
import { WINDOW_RUNS, WINDOW_MS } from './cpu-estimates.js';

/**
 * BRAIN-550: reserve a lane's CPU from what its finished runs actually used (their time-weighted mean cores), not
 * from its declared `cpuCores`. The target is ceil(p90 mean x headroom), within [floor, declared]; a lane with fewer than
 * RIGHT_SIZE_MIN_RUNS usable runs keeps its declaration. The sizing is `lane suggest`'s (BRAIN-361): runs under
 * OVERRUN_MIN_MS are mostly startup, and a run granted less than it requested is scaled back up to a requested-equivalent.
 */
export const RIGHT_SIZE_MIN_RUNS = 5;
export const RIGHT_SIZE_HEADROOM = 1.2;

const rowTime = (r) => r.endedAt;
const runMsOf = (r) => (Number.isFinite(r.runMs) ? r.runMs : r.endedAt - r.startedAt);

/** What one finished run needed, as cores of the claim it was requested at; null when the run says nothing. */
function needOf(row) {
  if (classifyRow(row).kind === 'ignored' || !Number.isFinite(row.endedAt) || runMsOf(row) < OVERRUN_MIN_MS) return null;
  const mean = row.observedCpu?.mean;
  if (!Number.isFinite(mean) || mean < 0) return null;
  const requested = row.resources?.cpuCores;
  const granted = row.grantedCpuCores;
  return Number.isFinite(granted) && granted > 0 && Number.isFinite(requested) ? (mean / granted) * requested : mean;
}

/**
 * Pure: history rows -> `{ cpuCores, runs }`, the right-sized request for (repoId, lane) clamped to [floor, declared], or null
 * when history is too thin. `lane` is the declared lane name an ad-hoc lane inherited, else the lane itself.
 */
export function rightSizedCpuCores(rows, { repoId, lane, declared, floor = 1, now = Date.now() }) {
  const needs = rows
    .filter((r) => r && typeof r === 'object' && r.repo === repoId && (r.configLane ?? r.lane) === lane && Number.isFinite(r.endedAt) && now - rowTime(r) <= WINDOW_MS)
    .sort((a, b) => rowTime(a) - rowTime(b))
    .slice(-WINDOW_RUNS)
    .map(needOf)
    .filter((n) => n !== null)
    .sort((a, b) => a - b);
  if (needs.length < RIGHT_SIZE_MIN_RUNS) return null;
  // the epsilon keeps an exact product (5 x 1.2) from ceiling to the next whole core
  const target = Math.ceil(percentile(needs, 0.9) * RIGHT_SIZE_HEADROOM - 1e-9);
  return { cpuCores: Math.min(declared, Math.max(floor, target)), runs: needs.length };
}
