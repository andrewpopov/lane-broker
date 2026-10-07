import crypto from 'node:crypto';
import { paths, stateHome } from './state.js';
import { readRows } from './suggest.js';
import { sanitizeKey } from './config.js';
import { classifyRow, percentile } from './estimates-history.js';

/**
 * BRAIN-433: the CPU a workload has needed before, so a CANDIDATE (and a just-admitted lease) is charged that
 * instead of an inflated declaration. A workload is (repo, lane, persisted command fingerprint, granted cores): same lane with another
 * command is a different workload and gets no relief. Only runs that were granted their full declaration count
 * (an elastic-reduced run was throttled, so its peak understates the need), and only runs granted exactly the candidate's cores (a 1-core run says nothing about an 8-worker candidate), only local-executor runs (admission
 * is local; a remote run's CPU was another machine's), and the figure is the p90 of per-run PEAK cores, since
 * admission must cover bursts, not averages. null below `minRuns`.
 */
export const WINDOW_RUNS = 50;
export const WINDOW_MS = 14 * 24 * 3600 * 1000;
export const MIN_ESTIMATE_CORES = 0.5;
export const REFRESH_MS = 60_000;
/** A snapshot older than this is not trusted: admission charges declared rather than act on stale history. */
export const SNAPSHOT_MAX_AGE_MS = 5 * 60_000;
/** `value` with its leading `prefix` (at a path boundary, optionally after a `--flag=`) replaced by `token`. */
function replacePrefix(value, prefix, token) {
  if (!prefix) return value;
  const eq = value.startsWith('-') ? value.indexOf('=') + 1 : 0;
  const head = value.slice(0, eq);
  const rest = value.slice(eq);
  const boundary = rest.length === prefix.length || rest[prefix.length] === '/';
  return rest.startsWith(prefix) && boundary ? `${head}${token}${rest.slice(prefix.length)}` : value;
}

/**
 * The persisted command fingerprint (recorded on the ticket, lease and history row at run start): sha256 of the
 * JSON of the argv, each element with ITS OWN run's checkout root rewritten to `<repo>` and its own TMPDIR to `<tmp>`,
 * and nothing else. No truncation, no join (argv boundaries survive), no pattern guessing: another repo's path, a
 * test-file argument, a flag or a number is never touched, so a focused run and a full run stay distinct workloads.
 */
export function argvFingerprint(argv, { root, tmp }) {
  if (!Array.isArray(argv) || argv.length === 0 || !argv.every((a) => typeof a === 'string')) return null;
  const normalized = argv.map((a) => replacePrefix(replacePrefix(a, root, '<repo>'), tmp?.replace(/\/+$/, ''), '<tmp>'));
  return crypto.createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
}

const repoOfKey = (key) => key.slice(0, key.lastIndexOf(':'));

/** One run's peak cores, only if the run counts: completed, local, granted its full declaration. Otherwise null. */
export function runPeakCores(row) {
  if (classifyRow(row).kind !== 'completed') return null;
  if ((row.executor ?? 'local') !== 'local') return null;
  const declared = row.resources?.cpuCores;
  if (!Number.isFinite(declared) || (row.grantedCpuCores ?? declared) !== declared) return null;
  const peak = row.observedCpu?.peak;
  return Number.isFinite(peak) && peak >= 0 ? peak : null;
}

/** The levels, narrowest first: exact (repo, lane, fingerprint); (repo, declared lane, fingerprint) for an ad-hoc lane name. */
export const LEVEL_SOURCES = ['exact', 'configLane'];
export const estimateIds = ({ key, configLane, fingerprint, cores }) => {
  const repo = repoOfKey(key);
  return [`exact\0${key}\0${fingerprint}\0${cores}`, `configLane\0${repo}:${sanitizeKey(configLane ?? key.slice(repo.length + 1))}\0${fingerprint}\0${cores}`];
};

/** Pure: history rows -> Map<levelId, { p90, n }> for every group with at least `minRuns` usable runs in the window. */
export function computeCpuEstimates(rows, { now, minRuns = 5, windowRuns = WINDOW_RUNS, windowMs = WINDOW_MS } = {}) {
  const groups = new Map();
  for (const row of rows) {
    if (!row || typeof row !== 'object' || !Number.isFinite(row.endedAt) || now - row.endedAt > windowMs) continue;
    const fingerprint = row.cmdFingerprint;
    const peak = runPeakCores(row);
    if (peak === null || typeof fingerprint !== 'string') continue;
    const key = row.key ?? `${row.repo}:${row.lane}`;
    for (const id of estimateIds({ key, configLane: row.configLane ?? row.lane, fingerprint, cores: row.resources.cpuCores })) {
      if (!groups.has(id)) groups.set(id, []);
      groups.get(id).push({ endedAt: row.endedAt, peak });
    }
  }
  const out = new Map();
  for (const [id, runs] of groups) {
    const recent = runs.sort((a, b) => a.endedAt - b.endedAt).slice(-windowRuns);
    if (recent.length < minRuns) continue;
    out.set(id, { p90: percentile(recent.map((r) => r.peak).sort((a, b) => a - b), 0.9), n: recent.length });
  }
  return out;
}

/** Pure: the first level of `estimates` with an entry for this workload at `cores` granted, as `{ p90, level }`, else null. */
export function lookupEstimate(estimates, ref, cores) {
  if (!ref?.key || typeof ref.cmdFingerprint !== 'string') return null;
  const ids = estimateIds({ key: ref.key, configLane: ref.configLane, fingerprint: ref.cmdFingerprint, cores });
  for (let i = 0; i < ids.length; i += 1) {
    const entry = estimates.get(ids[i]);
    if (entry) return { p90: entry.p90, level: LEVEL_SOURCES[i] };
  }
  return null;
}

/** Pure: the charge for a claim of `declared` cores: the p90 peak, floored at MIN_ESTIMATE_CORES, capped at the declaration. */
export const clampEstimate = (p90, declared) => Math.min(declared, Math.max(MIN_ESTIMATE_CORES, p90));

const snapshots = new Map();

/**
 * history.jsonl folded into a snapshot that remembers the config it was built under, re-read at most once per REFRESH_MS
 * per state root. This does the file I/O, so callers run it BEFORE taking the global admission lock; admission only peeks.
 */
export function refreshCpuEstimates(cfg, now = Date.now(), root = stateHome()) {
  if (cfg?.historyDemandEnabled !== true) return;
  const minRuns = cfg.historyDemandMinRuns ?? 5;
  const hit = snapshots.get(root);
  if (hit && hit.minRuns === minRuns && now >= hit.at && now - hit.at < REFRESH_MS) return;
  let map;
  try {
    map = computeCpuEstimates(readRows(paths(root).history), { now, minRuns });
  } catch {
    map = new Map();
  }
  snapshots.set(root, { at: now, minRuns, map });
}

/** The last snapshot's estimates, only when it matches the config admission is running under and is fresh; else empty (declared). Never reads the file. */
export function peekCpuEstimates(cfg, now = Date.now(), root = stateHome()) {
  const snap = snapshots.get(root);
  const usable = snap && cfg?.historyDemandEnabled === true && snap.minRuns === (cfg.historyDemandMinRuns ?? 5) && now >= snap.at && now - snap.at < SNAPSHOT_MAX_AGE_MS;
  return usable ? snap.map : new Map();
}
