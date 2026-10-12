import { classifyRow, backfillEstimates } from './estimates-history.js';

/**
 * BRAIN-548: speed-aware placement. A remote-eligible ticket goes wherever it is expected to FINISH soonest, counting each
 * runner's measured slowdown against this machine, instead of going remote whenever a runner admits it. Pure functions: the
 * supervisor gathers rows, queue and runner choice, these only do the arithmetic.
 */

export const MIN_SPEED_PAIRS = 5;
/** What a remote dispatch costs on top of the run itself (snapshot, setup, result delivery) when nothing better is configured. */
export const DEFAULT_REMOTE_OVERHEAD_MS = 60_000;

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

/** Successful wall times (ms) per cmdFingerprint, for rows `pick` accepts. */
function wallByFingerprint(rows, pick) {
  const byFingerprint = new Map();
  for (const row of rows) {
    if (!row?.cmdFingerprint || !pick(row)) continue;
    const c = classifyRow(row);
    if (c.kind !== 'completed') continue;
    byFingerprint.set(row.cmdFingerprint, [...(byFingerprint.get(row.cmdFingerprint) ?? []), c.wallS * 1000]);
  }
  return byFingerprint;
}

/**
 * How many times slower `runner` runs a command than this machine: the median, over every cmdFingerprint with a successful run
 * both locally and on that runner, of (median remote run) / (median local run). Fewer than `minPairs` such commands is not
 * evidence, so it falls back to the runner's configured `speedRatio`, else 1.
 */
export function speedRatio(rows, runner, { minPairs = MIN_SPEED_PAIRS } = {}) {
  const local = wallByFingerprint(rows, (r) => (r.executor ?? 'local') === 'local');
  const remote = wallByFingerprint(rows, (r) => r.executor === 'remote' && r.runner === runner.name);
  const ratios = [];
  for (const [fingerprint, localMs] of local) {
    if (remote.has(fingerprint)) ratios.push(median(remote.get(fingerprint)) / median(localMs));
  }
  if (ratios.length >= minPairs) return median(ratios);
  return Number.isFinite(runner.speedRatio) && runner.speedRatio >= 1 ? runner.speedRatio : 1;
}

/** The median (p50) wall time in ms a (repo, lane) has taken on THIS machine, or null without a usable local run. */
export function localRunEstimateMs(rows, repo, lane) {
  const local = rows.filter((r) => r?.repo === repo && r?.lane === lane && (r.executor ?? 'local') === 'local');
  const key = backfillEstimates(local).keys.find((k) => k.repo === repo && k.lane === lane);
  return key && key.n > 0 ? Math.round(key.p50 * 1000) : null;
}

/**
 * How long a ticket expects to wait for this machine: the run time of everything queued ahead of it plus one run still in
 * flight (the work blocking admission, unknown in length, assumed to need a full run: conservative, it leans to remote), spread
 * over the `slots` lanes of this ticket's weight the machine can run at once.
 * BRAIN-556: the model sees only the queue, not CPU load outside any lease. Time already waited is a lower bound on the wait still
 * ahead once the prediction has visibly failed, so the result is never below `waitedMs`.
 */
export function localWaitMs({ aheadEstimatesMs, estimateMs, slots, waitedMs = 0 }) {
  const work = aheadEstimatesMs.reduce((sum, ms) => sum + ms, 0) + estimateMs;
  return Math.max(work / Math.max(1, slots), waitedMs);
}

/**
 * Local vs the given runner: whichever finishes first. `runnerRatio` is the slowdown of the runner that would take the ticket;
 * a tie stays local (no snapshot to ship). Returns the choice with both estimates for the decision log.
 */
export function chooseSpeedPlacement({ localAdmitsNow, localWaitMs: waitMs = 0, estimateMs, runnerRatio, overheadMs = DEFAULT_REMOTE_OVERHEAD_MS }) {
  const localFinishMs = (localAdmitsNow ? 0 : waitMs) + estimateMs;
  const remoteFinishMs = runnerRatio === null ? null : estimateMs * runnerRatio + overheadMs;
  const choice = remoteFinishMs === null || localFinishMs <= remoteFinishMs ? 'local' : 'remote';
  return { choice, localFinishMs: Math.round(localFinishMs), remoteFinishMs: remoteFinishMs === null ? null : Math.round(remoteFinishMs) };
}
