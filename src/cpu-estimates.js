import crypto from 'node:crypto';
import { paths, stateHome } from './state.js';
import { readRows } from './suggest.js';
import { sanitizeKey } from './config.js';
import { classifyRow, percentile } from './estimates-history.js';

/**
 * BRAIN-433: the CPU a workload has needed before, so a CANDIDATE (and a just-admitted lease) is charged that
 * instead of an inflated declaration. A workload is (repo, lane, command fingerprint): same lane with another
 * command is a different workload and gets no relief. Only runs that were granted their full declaration count
 * (an elastic-reduced run was throttled, so its peak understates the need), only local-executor runs (admission
 * is local; a remote run's CPU was another machine's), and the figure is the p90 of per-run PEAK cores, since
 * admission must cover bursts, not averages. null below `minRuns`.
 */
export const WINDOW_RUNS = 50;
export const WINDOW_MS = 14 * 24 * 3600 * 1000;
export const MIN_ESTIMATE_CORES = 0.5;
export const REFRESH_MS = 60_000;
/** A snapshot older than this is not trusted: admission charges declared rather than act on stale history. */
export const SNAPSHOT_MAX_AGE_MS = 5 * 60_000;
/** run.js records at most this much of a command (HISTORY_COMMAND_MAX), so the fingerprint hashes the same prefix of a ticket's argv. */
const COMMAND_MAX = 300;

/**
 * The ONLY normalisations applied before hashing, so the same script run from another checkout is one workload:
 * a checkout root (primary `.../proj/<name>`, a worktree `.../worktrees/<name>/<slug>` or `<root>/.worktree/<slug>`, a remote
 * runner's `.../.cache/lane-broker/remote/tickets/<uuid>/work`) becomes `<repo>`, and a temp directory
 * (macOS `/var/folders/../T`, `/tmp`) becomes `<tmp>`. History rows record no cwd, only the command text, so both sides
 * (rows and tickets) are normalised by these same patterns rather than by a recorded root. Test-file arguments, flags,
 * SHAs and numbers are NOT touched: a focused run and a full run stay distinct workloads.
 */
const DIR = '[^\\s/\'"]+';
const NORMALISATIONS = [
  [new RegExp(`(?:~|(?:/${DIR})*)/\\.cache/lane-broker/remote/tickets/[0-9a-f-]{36}/work`, 'g'), '<repo>'],
  [new RegExp(`(?:/${DIR})*/worktrees/${DIR}/${DIR}`, 'g'), '<repo>'],
  [new RegExp(`(?:/${DIR})+/\\.worktree/${DIR}`, 'g'), '<repo>'],
  [new RegExp(`/(?:Volumes|Users)/${DIR}/proj/${DIR}`, 'g'), '<repo>'],
  [new RegExp(`(?:/private)?/var/folders/${DIR}/${DIR}/T`, 'g'), '<tmp>'],
  [/(?<=^|[\s=])(?:\/private)?\/tmp(?=\/|\s|$)/g, '<tmp>'],
];

/** Hash of the normalized command: truncated as run.js records it, roots and temp dirs rewritten, whitespace collapsed. null when there is no command. */
export function commandFingerprint(command) {
  const raw = (Array.isArray(command) ? command.join(' ') : command ?? '').slice(0, COMMAND_MAX);
  const text = NORMALISATIONS.reduce((acc, [re, to]) => acc.replace(re, to), raw).trim().replace(/\s+/g, ' ');
  return text ? crypto.createHash('sha256').update(text).digest('hex').slice(0, 16) : null;
}

/** A ticket's or lease's command: a ticket's recorded `command`, else its `cmd` argv (a lease has only that). */
export const commandOf = (ref) => ref.command ?? ref.cmd;

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
export const estimateIds = ({ key, configLane, fingerprint }) => {
  const repo = repoOfKey(key);
  return [`exact\0${key}\0${fingerprint}`, `configLane\0${repo}:${sanitizeKey(configLane ?? key.slice(repo.length + 1))}\0${fingerprint}`];
};

/** Pure: history rows -> Map<levelId, { p90, n }> for every group with at least `minRuns` usable runs in the window. */
export function computeCpuEstimates(rows, { now, minRuns = 5, windowRuns = WINDOW_RUNS, windowMs = WINDOW_MS } = {}) {
  const groups = new Map();
  for (const row of rows) {
    if (!row || typeof row !== 'object' || !Number.isFinite(row.endedAt) || now - row.endedAt > windowMs) continue;
    const fingerprint = commandFingerprint(row.command);
    const peak = runPeakCores(row);
    if (peak === null || fingerprint === null) continue;
    const key = row.key ?? `${row.repo}:${row.lane}`;
    for (const id of estimateIds({ key, configLane: row.configLane ?? row.lane, fingerprint })) {
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

/** Pure: the first level of `estimates` with an entry for this workload, as `{ p90, level }`, else null. */
export function lookupEstimate(estimates, ref) {
  if (!ref?.key) return null;
  const fingerprint = commandFingerprint(commandOf(ref));
  if (fingerprint === null) return null;
  const ids = estimateIds({ key: ref.key, configLane: ref.configLane, fingerprint });
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
