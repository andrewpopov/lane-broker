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

const ENV_ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=(.*)$/;
const unquoted = (value) => value.replace(/^["']|["']$/g, '');
/** A per-run value: a commit sha (>= 7 hex) or a digit-led id/timestamp (digits with - . _ : T Z separators, >= 6 digits in all). */
const isVolatileValue = (value) => /^[0-9a-fA-F]{7,}$/.test(value) || (/^\d[\d\-._:TZ]*$/.test(value) && value.replace(/\D/g, '').length >= 6);

/**
 * BRAIN-596: a command with its per-run env assignments (`TRAIN_BASE_SHA=<sha>`, a release id or timestamp) removed, and the `env`
 * left bare by that dropped, so every release candidate of one train command pairs with the history of the others. For pairing
 * speed evidence ONLY: the stored cmdFingerprint stays exact.
 */
export function pairingCommand(command) {
  const tokens = String(command ?? '').split(/\s+/).filter((token) => token && !isVolatileValue(unquoted(ENV_ASSIGNMENT.exec(token)?.[1] ?? '')));
  const bare = tokens[0] === 'env' && tokens.length > 1 && !tokens[1].startsWith('-') && !ENV_ASSIGNMENT.test(tokens[1]);
  return (bare ? tokens.slice(1) : tokens).join(' ');
}

const pairingKey = (row) => `${row.repo}\0${row.lane}\0${row.command ? pairingCommand(row.command) : row.cmdFingerprint}`;

/** Successful wall times (ms) per pairing key, for rows `pick` accepts. */
function wallByKey(rows, pick) {
  const byKey = new Map();
  for (const row of rows) {
    if ((!row?.command && !row?.cmdFingerprint) || !pick(row)) continue;
    const c = classifyRow(row);
    if (c.kind !== 'completed') continue;
    const key = pairingKey(row);
    byKey.set(key, [...(byKey.get(key) ?? []), c.wallS * 1000]);
  }
  return byKey;
}

/** Every command with a successful run both locally and on `runner`: its local/remote run counts, local median and remote/local ratio. */
function speedPairs(rows, runner) {
  const local = wallByKey(rows, (r) => (r.executor ?? 'local') === 'local');
  const remote = wallByKey(rows, (r) => r.executor === 'remote' && r.runner === runner.name);
  const pairs = [];
  for (const [key, localMs] of local) {
    if (!remote.has(key)) continue;
    const [repo, lane] = key.split('\0');
    const localMedianMs = median(localMs);
    pairs.push({ key, repo, lane, localRuns: localMs.length, remoteRuns: remote.get(key).length, localMedianMs, ratio: median(remote.get(key)) / localMedianMs });
  }
  return pairs;
}

/** Short jobs (setup dominates) run several times slower on a runner than long ones, so a pair only speaks for a job within this factor of its size. */
export const SIZE_BAND_FACTOR = 2;
/** An exact command is trusted on fewer pairs than the broader tiers, but still needs this many runs on EACH side. */
export const MIN_EXACT_RUNS = 2;

/**
 * How many times slower `runner` runs this job than this machine, from the most specific evidence that holds up, and which tier it was:
 *   exact       the job's own command (volatile env stripped, see pairingCommand), >= MIN_EXACT_RUNS successful runs on each side
 *   repo-lane   median ratio over the commands of the same repo and lane, once there are `minPairs`
 *   size-band   median ratio over commands whose local median is within SIZE_BAND_FACTOR of the job's local estimate, `minPairs`
 *   global      median ratio over every command, `minPairs`
 *   configured  the runner's configured `speedRatio` (>= 1), else 1
 * `job` is { command, repo, lane, estimateMs }; without it only global/configured apply.
 */
export function speedRatio(rows, runner, { minPairs = MIN_SPEED_PAIRS, job = null } = {}) {
  const pairs = speedPairs(rows, runner);
  const tiers = [];
  if (job) {
    const key = pairingKey({ repo: job.repo, lane: job.lane, command: job.command });
    tiers.push(['exact', pairs.filter((p) => p.key === key && p.localRuns >= MIN_EXACT_RUNS && p.remoteRuns >= MIN_EXACT_RUNS), 1]);
    tiers.push(['repo-lane', pairs.filter((p) => p.repo === job.repo && p.lane === job.lane), minPairs]);
    tiers.push(['size-band', pairs.filter((p) => p.localMedianMs >= job.estimateMs / SIZE_BAND_FACTOR && p.localMedianMs <= job.estimateMs * SIZE_BAND_FACTOR), minPairs]);
  }
  tiers.push(['global', pairs, minPairs]);
  for (const [tier, evidence, needed] of tiers) {
    if (evidence.length >= needed) return { ratio: median(evidence.map((p) => p.ratio)), tier, pairs: evidence.length };
  }
  return { ratio: Number.isFinite(runner.speedRatio) && runner.speedRatio >= 1 ? runner.speedRatio : 1, tier: 'configured', pairs: 0 };
}

/**
 * BRAIN-596: how many tickets of this size the host's CPU budget admits right now, counting only load OUTSIDE any lease (the
 * leases' own demand frees up as they finish; ambient load does not): 0 while the CPU gate is closed, else what fits beside it.
 */
export function externalCpuSlots({ gateClosed, budget, externalBusy, claimCores }) {
  if (gateClosed) return 0;
  return Math.floor(Math.max(0, budget - externalBusy) / Math.max(claimCores, Number.EPSILON));
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
 * a tie stays local (no snapshot to ship); `localBlocked` (BRAIN-596) sends it to the runner outright. Returns the choice with both estimates for the decision log.
 */
export function chooseSpeedPlacement({ localAdmitsNow, localWaitMs: waitMs = 0, localBlocked = false, estimateMs, runnerRatio, overheadMs = DEFAULT_REMOTE_OVERHEAD_MS }) {
  const remoteFinishMs = runnerRatio === null ? null : estimateMs * runnerRatio + overheadMs;
  // BRAIN-596: `localBlocked` = host load admits no ticket here, so the local finish is unbounded: remote whenever a runner can take it
  if (localBlocked && !localAdmitsNow) return { choice: remoteFinishMs === null ? 'local' : 'remote', localFinishMs: null, remoteFinishMs: remoteFinishMs === null ? null : Math.round(remoteFinishMs) };
  const localFinishMs = (localAdmitsNow ? 0 : waitMs) + estimateMs;
  const choice = remoteFinishMs === null || localFinishMs <= remoteFinishMs ? 'local' : 'remote';
  return { choice, localFinishMs: Math.round(localFinishMs), remoteFinishMs: remoteFinishMs === null ? null : Math.round(remoteFinishMs) };
}
