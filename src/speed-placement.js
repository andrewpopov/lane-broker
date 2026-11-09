import fs from 'node:fs';
import path from 'node:path';
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

/** How fast a runner is configured to run (higher is faster); unset or malformed is 1. */
const speedOf = (runner) => (Number.isFinite(runner?.speedFactor) && runner.speedFactor > 0 ? runner.speedFactor : 1);

/** The fewest successful remote runs of a (repo, lane) on one runner that stand in for a command never run there. */
export const MIN_REPO_LANE_ROWS = 3;

/** Successful (`exit === 0`, finite duration) remote wall times on `runner` of the rows `match` accepts. */
const remoteRunMs = (rows, runnerName, match) =>
  rows.filter((r) => r?.executor === 'remote' && r?.runner === runnerName && r?.exit === 0 && Number.isFinite(r.runMs) && r.runMs > 0 && match(r)).map((r) => r.runMs);

/**
 * BRAIN-899: how long this ticket is expected to RUN on `runner`, in ms, from the broker's own history rows: the median of the
 * most specific evidence that holds up, first hit wins -- this repo+lane+command on `runner` (`pairingCommand`, so a per-run sha
 * or shard id does not split it, >= MIN_EXACT_RUNS runs); this repo+lane on `runner` (>= MIN_REPO_LANE_ROWS runs); either of
 * those on the OTHER configured runner with the most matching rows (ties: config order), scaled by its `speedFactor` over
 * `runner`'s and named as the `donor`. No evidence anywhere: null, and the caller keeps today's ranking. `job` is { repo, lane, command }.
 */
export function runnerRunEstimateMs(rows, runner, job, runners) {
  const history = Array.isArray(rows) ? rows : [];
  const command = typeof job?.command === 'string' && job.command.trim() !== '' ? pairingCommand(job.command) : null;
  const tiers = [
    { tier: 'exact', min: MIN_EXACT_RUNS, match: command === null ? null : (r) => r.repo === job.repo && r.lane === job.lane && typeof r.command === 'string' && pairingCommand(r.command) === command },
    { tier: 'repo-lane', min: MIN_REPO_LANE_ROWS, match: (r) => r.repo === job?.repo && r.lane === job?.lane },
  ];
  const others = (Array.isArray(runners) ? runners : []).filter((r) => r?.name && r.name !== runner?.name);
  const hit = (ms, tier, n, donor) => (Number.isFinite(ms) && ms > 0 ? { ms: Math.round(ms), tier, n, ...(donor === null ? {} : { donor }) } : null);
  for (const { tier, min, match } of tiers) {
    if (match === null) continue;
    const own = remoteRunMs(history, runner?.name, match);
    if (own.length >= min) {
      const got = hit(median(own), tier, own.length, null);
      if (got) return got;
    }
  }
  for (const { tier, min, match } of tiers) {
    if (match === null) continue;
    let donor = null;
    for (const other of others) {
      const values = remoteRunMs(history, other.name, match);
      if (values.length >= min && (donor === null || values.length > donor.values.length)) donor = { name: other.name, speed: speedOf(other), values };
    }
    if (donor === null) continue;
    const got = hit((median(donor.values) * donor.speed) / speedOf(runner), tier, donor.values.length, donor.name);
    if (got) return got;
  }
  return null;
}

/**
 * BRAIN-899: how long a ticket expects to WAIT before it starts on a runner the probe sees: 0 when the runner is idle with room,
 * else one run of this ticket's size per ticket ahead of it plus one for the work blocking admission. Serial and conservative on
 * purpose -- `probe.running` counts orphaned leases and pressure can block admission, so it never shortens the wait. A `queued`
 * that is not a positive integer counts as 0.
 */
export function runnerWaitMs({ probe, estimateMs, room }) {
  const queued = Number.isInteger(probe?.queued) && probe.queued > 0 ? probe.queued : 0;
  return room && queued === 0 ? 0 : (queued + 1) * estimateMs;
}

/**
 * Local vs the given runner: whichever finishes first. `runnerRatio` is the slowdown of the runner that would take the ticket;
 * a tie stays local (no snapshot to ship); `localBlocked` (BRAIN-596) and `localHeldByKey` (BRAIN-839) send it to the runner outright. Returns the choice with both estimates for the decision log.
 * BRAIN-899: a finite `remoteFinishMs` IS that runner's finish (its placement score), replacing the ratio arithmetic, not adding to it.
 */
export function chooseSpeedPlacement({ localAdmitsNow, localWaitMs: waitMs = 0, localBlocked = false, localHeldByKey = false, estimateMs, runnerRatio, overheadMs = DEFAULT_REMOTE_OVERHEAD_MS, remoteFinishMs: scoredFinishMs = null }) {
  const remoteFinishMs = Number.isFinite(scoredFinishMs) ? scoredFinishMs : runnerRatio === null ? null : estimateMs * runnerRatio + overheadMs;
  // BRAIN-596: `localBlocked` = host load admits no ticket here, so the local finish is unbounded: remote whenever a runner can take it
  if (localBlocked && !localAdmitsNow) return { choice: remoteFinishMs === null ? 'local' : 'remote', localFinishMs: null, remoteFinishMs: remoteFinishMs === null ? null : Math.round(remoteFinishMs) };
  // BRAIN-839: a same-key holder (exclusive lane, or maxConcurrent reached) blocks local admission for as long as it runs; the queue
  // model cannot see that, so its short local finish is fiction. A narrow exception to tie-to-local: remote whenever a runner can take it.
  if (localHeldByKey && !localAdmitsNow && remoteFinishMs !== null) return { choice: 'remote', localFinishMs: null, remoteFinishMs: Math.round(remoteFinishMs) };
  const localFinishMs = (localAdmitsNow ? 0 : waitMs) + estimateMs;
  const choice = remoteFinishMs === null || localFinishMs <= remoteFinishMs ? 'local' : 'remote';
  return { choice, localFinishMs: Math.round(localFinishMs), remoteFinishMs: remoteFinishMs === null ? null : Math.round(remoteFinishMs) };
}

const DEP_LOCKFILES = ['package-lock.json', 'pnpm-lock.yaml', 'yarn.lock', 'npm-shrinkwrap.json'];

/**
 * BRAIN-663: the `remoteDeps` dirs (relative to `treeRoot`) that hold a lockfile but no `node_modules` directory, i.e. what the
 * runner would install and this tree lacks. Running such a lane here fails on its first tool (rouge e2e-scoped: `tsc: command not found`).
 */
export function missingLocalDeps(treeRoot, remoteDeps) {
  return (remoteDeps ?? []).filter((dir) => {
    const abs = path.join(treeRoot, dir);
    if (!DEP_LOCKFILES.some((name) => fs.existsSync(path.join(abs, name)))) return false;
    try {
      return !fs.statSync(path.join(abs, 'node_modules')).isDirectory();
    } catch {
      return true;
    }
  });
}
