import { paths, atomicWriteJson, readJsonSafe } from './state.js';
import { backfillEstimates, classifyRow, percentile } from './estimates-history.js';
import { DEFAULT_GLOBAL_CONFIG } from './config.js';
import { readHistoryRows } from './suggest.js';
import { writeBrokerLog } from './admission.js';

/**
 * BRAIN-551: express admission. A ticket whose history says it runs under `expressMaxMs` may start past a waiting head
 * as soon as it fits, so a 10 s check does not queue behind a 30 min job. Pure estimate table plus a per-host rate
 * window; the scheduler decides, these only answer "how long does it run" and "may another one start this minute".
 */

export const EXPRESS_MIN_SAMPLES = 3;
const TABLE_TTL_MS = 5 * 60_000;
const RATE_WINDOW_MS = 60_000;

const laneId = (repo, lane) => JSON.stringify([repo, lane]);
const commandId = (repo, lane, fingerprint) => JSON.stringify([repo, lane, fingerprint]);

function group(map, id, row) {
  if (!map.has(id)) map.set(id, []);
  map.get(id).push(row);
}

export const BYPASS_WINDOW_RUNS = 20;
export const BYPASS_MAX_STRIKES = 3;
const FAMILY_TOKENS = 3;

/** The command's leading words without env assignments, flags, paths or files: `pnpm test src/a.test.js` and `pnpm test` share one. */
export const commandFamily = (command) =>
  String(command ?? '')
    .split(/\s+/)
    .filter((token) => token && !/^[A-Za-z_]\w*=/.test(token) && !token.startsWith('-') && !/[/.]/.test(token))
    .slice(0, FAMILY_TOKENS)
    .join(' ');

const bypassId = (repo, lane, kind, name) => JSON.stringify([repo, lane, kind, name]);
const runMsOf = (row) => (Number.isFinite(row.runMs) && row.runMs > 0 ? row.runMs : row.endedAt - row.startedAt);

/**
 * Pure: history rows -> `{ n, medianMs, strikes }` per (repo, lane, exact command | command family), for groups with at least
 * EXPRESS_MIN_SAMPLES successful runs. The median is over the last BYPASS_WINDOW_RUNS successes; strikes count the runs over
 * `maxMs` among the last BYPASS_WINDOW_RUNS that started, whatever their outcome.
 */
export function buildBypassTable(rows, maxMs) {
  const groups = new Map();
  const add = (id, row) => {
    if (!groups.has(id)) groups.set(id, []);
    groups.get(id).push(row);
  };
  const started = rows.filter((row) => row?.repo && row.lane && (row.executor ?? 'local') === 'local' && Number.isFinite(row.startedAt) && runMsOf(row) > 0).sort((a, b) => (a.endedAt ?? 0) - (b.endedAt ?? 0));
  for (const row of started) {
    if (row.cmdFingerprint) add(bypassId(row.repo, row.lane, 'command', row.cmdFingerprint), row);
    const family = commandFamily(row.command);
    if (family) add(bypassId(row.repo, row.lane, 'family', family), row);
  }
  const out = {};
  for (const [id, members] of groups) {
    const successes = members.filter((row) => classifyRow(row).kind === 'completed').slice(-BYPASS_WINDOW_RUNS).map(runMsOf).sort((a, b) => a - b);
    if (successes.length < EXPRESS_MIN_SAMPLES) continue;
    out[id] = { n: successes.length, medianMs: Math.round(percentile(successes, 0.5)), strikes: members.slice(-BYPASS_WINDOW_RUNS).filter((row) => runMsOf(row) > maxMs).length };
  }
  return out;
}

/** Pure: history rows -> { lane, command } maps of local p50 run ms, only for groups with at least EXPRESS_MIN_SAMPLES runs. */
export function buildExpressTable(rows) {
  const byLane = new Map();
  const byCommand = new Map();
  for (const row of rows) {
    if (!row?.repo || !row.lane || (row.executor ?? 'local') !== 'local') continue;
    group(byLane, laneId(row.repo, row.lane), row);
    if (row.cmdFingerprint) group(byCommand, commandId(row.repo, row.lane, row.cmdFingerprint), row);
  }
  const fold = (groups) => {
    const out = {};
    for (const [id, members] of groups) {
      const key = backfillEstimates(members).keys[0];
      if (key && key.n >= EXPRESS_MIN_SAMPLES) out[id] = Math.round(key.p50 * 1000);
    }
    return out;
  };
  return { lane: fold(byLane), command: fold(byCommand) };
}

/**
 * The table, from `express-estimates.json` while it is younger than TABLE_TTL_MS, else rebuilt from history.jsonl
 * (which is large, so never per poll). `write: false` (lane status) rebuilds in memory only.
 */
export function loadExpressTable(root, now = Date.now(), { write = true, bypassMaxMs = DEFAULT_GLOBAL_CONFIG.queueBypassMaxMs } = {}) {
  const file = paths(root).expressEstimates;
  const cached = readJsonSafe(file);
  if (cached && Number.isFinite(cached.builtAt) && now - cached.builtAt >= 0 && now - cached.builtAt < TABLE_TTL_MS && cached.lane && cached.command && cached.bypass && cached.bypassMaxMs === bypassMaxMs) return cached;
  const rows = readHistoryRows(paths(root).history);
  const table = { builtAt: now, bypassMaxMs, ...buildExpressTable(rows), bypass: buildBypassTable(rows, bypassMaxMs) };
  if (write) {
    try {
      atomicWriteJson(file, table);
    } catch {
      // a cache: the next poll rebuilds
    }
  }
  return table;
}

/** Expected run ms and where it came from: this exact command's p50, else the lane's; null without enough history (not express). */
export function expressEstimate(table, ticket) {
  const repo = ticket.repoId;
  const byCommand = ticket.cmdFingerprint ? table.command[commandId(repo, ticket.lane, ticket.cmdFingerprint)] : undefined;
  if (byCommand !== undefined) return { ms: byCommand, source: 'command' };
  const byLane = table.lane[laneId(repo, ticket.lane)];
  return byLane !== undefined ? { ms: byLane, source: 'lane' } : null;
}

/** The estimate when the ticket is express under `cfg.expressMaxMs`, else null. */
export function expressVerdict(table, ticket, cfg) {
  const estimate = expressEstimate(table, ticket);
  return estimate && estimate.ms < cfg.expressMaxMs ? estimate : null;
}

/**
 * BRAIN-756: the expected run ms and source when the ticket is predicted under `cfg.queueBypassMaxMs`: its exact command's
 * history when it has enough, else its command family's, never the lane's. At most BYPASS_MAX_STRIKES of the last
 * BYPASS_WINDOW_RUNS runs may have been over the threshold. Null otherwise (no history is not a prediction).
 */
export function bypassVerdict(table, ticket, cfg) {
  const byCommand = ticket.cmdFingerprint ? table.bypass[bypassId(ticket.repoId, ticket.lane, 'command', ticket.cmdFingerprint)] : undefined;
  const family = byCommand ? undefined : table.bypass[bypassId(ticket.repoId, ticket.lane, 'family', commandFamily(ticket.command ?? ticket.cmd?.join(' ')))];
  const entry = byCommand ?? family;
  return entry && entry.medianMs < cfg.queueBypassMaxMs && entry.strikes <= BYPASS_MAX_STRIKES ? { ms: entry.medianMs, source: byCommand ? 'command' : 'family' } : null;
}

const recentAdmissions = (root, now) => {
  const stored = readJsonSafe(paths(root).expressAdmissions);
  return Array.isArray(stored) ? stored.filter((at) => Number.isFinite(at) && now - at >= 0 && now - at < RATE_WINDOW_MS) : [];
};

/** How many express admissions this host has made in the last minute (the caller holds the lock). */
export const expressAdmissionsLastMinute = (root, now) => recentAdmissions(root, now).length;

/** Record one admission. Fail CLOSED like recordSkip: false means the count is not durable and the caller must refuse the start. */
export function recordExpressAdmission(root, now) {
  try {
    atomicWriteJson(paths(root).expressAdmissions, [...recentAdmissions(root, now), now]);
    return true;
  } catch {
    return false;
  }
}

export function logExpressAdmission(root, f) {
  writeBrokerLog(
    root,
    `${['lane-broker-express', 'event=express-admit', `candidateId=${f.candidateId}`, `headId=${f.headId}`, `expectedMs=${f.expectedMs}`, `source=${f.source}`, `headWaitedMs=${f.headWaitedMs}`, `inLastMinute=${f.inLastMinute}/${f.limit}`].join(' ')}\n`,
  );
}
