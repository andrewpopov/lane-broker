import { paths, atomicWriteJson, readJsonSafe } from './state.js';
import { backfillEstimates } from './estimates-history.js';
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
export function loadExpressTable(root, now = Date.now(), { write = true } = {}) {
  const file = paths(root).expressEstimates;
  const cached = readJsonSafe(file);
  if (cached && Number.isFinite(cached.builtAt) && now - cached.builtAt >= 0 && now - cached.builtAt < TABLE_TTL_MS && cached.lane && cached.command) return cached;
  const table = { builtAt: now, ...buildExpressTable(readHistoryRows(paths(root).history)) };
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
