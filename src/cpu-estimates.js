import { paths, stateHome } from './state.js';
import { readRows } from './suggest.js';
import { sanitizeKey } from './config.js';
import { classifyRow, percentile } from './estimates-history.js';

/**
 * BRAIN-433: the CPU a lane ACTUALLY uses, from its own history, so admission charges that instead of an inflated declaration.
 * Per history `key` (repo:lane): the p90 of the per-run mean cores over the last WINDOW_RUNS successful runs
 * no older than WINDOW_MS; null below `minRuns`. Reuses estimates-history's row classification and percentile.
 */
export const WINDOW_RUNS = 50;
export const WINDOW_MS = 14 * 24 * 3600 * 1000;
export const MIN_ESTIMATE_CORES = 0.25;
export const REFRESH_MS = 60_000;

/** One run's mean cores: cpuSeconds over wall when both exist, else the sampler's observed mean; null when neither. */
export function runMeanCores(row) {
  const c = classifyRow(row);
  if (c.kind !== 'completed') return null;
  if (Number.isFinite(row.cpuSeconds) && row.cpuSeconds >= 0) return row.cpuSeconds / c.wallS;
  const mean = row.observedCpu?.mean;
  return Number.isFinite(mean) && mean >= 0 ? mean : null;
}

/**
 * The estimate levels, narrowest first: the exact (repo, lane); the (repo, declared lane) an ad-hoc name
 * resolved to (`configLane`, else the lane is its own declared lane); the whole repo, split by declared cores.
 * Level 3 only pools runs that declared the SAME cores as the candidate: scaling by the declared ratio would
 * assume usage is proportional to the declaration, which is exactly what this feature exists to distrust.
 */
export const LEVEL_SOURCES = ['exact', 'configLane', 'repo'];
const repoOfKey = (key) => key.slice(0, key.lastIndexOf(':'));
export const estimateIds = ({ key, repo = repoOfKey(key ?? ''), configLane, declared }) => [
  `exact\0${key}`,
  `configLane\0${repo}:${sanitizeKey(configLane ?? key.slice(repo.length + 1))}`,
  `repo\0${repo}\0${declared}`,
];
const rowRepo = (row) => row.repo ?? repoOfKey(row.key ?? '');
const rowDeclared = (row) => row.resources?.cpuCores;

/** Pure: history rows -> Map<levelId, { p90, n }> for every group with at least `minRuns` usable runs in the window. */
export function computeCpuEstimates(rows, { now, minRuns = 5, windowRuns = WINDOW_RUNS, windowMs = WINDOW_MS } = {}) {
  const groups = new Map();
  const add = (id, run) => {
    if (!groups.has(id)) groups.set(id, []);
    groups.get(id).push(run);
  };
  for (const row of rows) {
    if (!row || typeof row !== 'object' || !Number.isFinite(row.endedAt) || now - row.endedAt > windowMs) continue;
    const cores = runMeanCores(row);
    if (cores === null) continue;
    const key = row.key ?? `${row.repo}:${row.lane}`;
    const run = { endedAt: row.endedAt, cores };
    const [exact, lane, repo] = estimateIds({ key, repo: rowRepo(row), configLane: row.configLane ?? row.lane, declared: rowDeclared(row) });
    add(exact, run);
    add(lane, run);
    if (Number.isFinite(rowDeclared(row))) add(repo, run);
  }
  const out = new Map();
  for (const [id, runs] of groups) {
    const recent = runs.sort((a, b) => a.endedAt - b.endedAt).slice(-windowRuns);
    if (recent.length < minRuns) continue;
    out.set(id, { p90: percentile(recent.map((r) => r.cores).sort((a, b) => a - b), 0.9), n: recent.length });
  }
  return out;
}

/** Pure: the first level (narrowest first) of `estimates` with an entry for this claim, as `{ p90, level }`, else null. */
export function lookupEstimate(estimates, ref) {
  const ids = estimateIds(ref);
  for (let i = 0; i < ids.length; i += 1) {
    const entry = estimates.get(ids[i]);
    if (entry) return { p90: entry.p90, level: LEVEL_SOURCES[i] };
  }
  return null;
}

/** Pure: the charge for a lane declaring `declared` cores: its history p90, floored at MIN_ESTIMATE_CORES, capped at the declaration. */
export const clampEstimate = (p90, declared) => Math.min(declared, Math.max(MIN_ESTIMATE_CORES, p90));

const cache = new Map();

/**
 * history.jsonl folded into estimates, re-read at most once per REFRESH_MS per state root. This does the file I/O,
 * so callers run it BEFORE taking the global admission lock; admission itself only ever peeks.
 */
export function refreshCpuEstimates(cfg, now = Date.now(), root = stateHome()) {
  if (cfg?.historyDemandEnabled !== true) return;
  const minRuns = cfg.historyDemandMinRuns ?? 5;
  const hit = cache.get(root);
  if (hit && hit.minRuns === minRuns && now - hit.at < REFRESH_MS && now >= hit.at) return;
  let map;
  try {
    map = computeCpuEstimates(readRows(paths(root).history), { now, minRuns });
  } catch {
    map = new Map();
  }
  cache.set(root, { at: now, minRuns, map });
}

/** The last refreshed estimates for a root; never reads the file (an unrefreshed root has none, so everything is declared). */
export const peekCpuEstimates = (root = stateHome()) => cache.get(root)?.map ?? new Map();
