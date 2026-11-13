import fs from 'node:fs';
import path from 'node:path';
import { readAttempt } from './attempts.js';
import { DEFAULT_GLOBAL_CONFIG, LEDGER_RETENTION_FLOOR_MS, loadGlobalConfigOrError } from './config.js';
import { ledgerDir, ledgerFile, readLedger } from './pull-ledger.js';
import { pullTicketDir, withTicketLock } from './pull-envelope.js';
import { createQueueClient } from './queue-client.js';
import { blobKey } from './pull-snapshot.js';
import { parseDuration } from './run.js';
import { createS3Client } from './s3-client.js';
import { atomicWriteJson, ensureDurableDir, ensureStateDirs, fsyncDirectory, LockTimeoutError, paths, readJsonSafe } from './state.js';

const HOUR_MS = 3_600_000;
export const OBJECTS_AFTER_MS = 24 * HOUR_MS;
export const BLOB_MIN_AGE_MS = 14 * 24 * HOUR_MS;
export const GC_BUDGET_MS = 5 * 60_000;
const PAGE_LIMIT = 200;
const BLOB_BATCH = 500;
const CHUNKS_TERMINAL_AFTER_MS = HOUR_MS;
const CHUNKS_PER_RUN = 10_000;
const BLOB_KEY = /^blobs\/([^/]+)\/([0-9a-f]{64})$/;

export const gcStateFile = (root) => path.join(paths(root).pull, 'gc-state.json');

function microsOf(stamp) {
  const match = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,6}))?(Z|[+-]\d\d(?::?\d\d)?)$/.exec(stamp);
  if (!match) throw new Error(`unparseable timestamp "${stamp}"`);
  const zone = /^[+-]\d\d$/.test(match[3]) ? `${match[3]}:00` : match[3];
  return BigInt(Date.parse(`${match[1]}${zone}`) / 1000) * 1_000_000n + BigInt((match[2] ?? '').padEnd(6, '0'));
}

const cursorOf = (item) => ({ finished_at: item.finished_at, id: item.id });

function afterCursor(item, cursor) {
  if (cursor === null) return true;
  const [a, b] = [microsOf(item.finished_at), microsOf(cursor.finished_at)];
  return a > b || (a === b && item.id > cursor.id);
}

export function rowsAfterFor(settings, requestedMs) {
  if (requestedMs !== undefined && requestedMs < settings.rowRetentionMs) {
    throw new Error(`rows_after ${requestedMs}ms is below lanePull.rowRetentionMs ${settings.rowRetentionMs}ms: the enqueue horizon assumes a row survives that long`);
  }
  return requestedMs ?? settings.rowRetentionMs;
}

const readGcState = (root) => ({ rowsFrom: null, objectsDone: null, resume: null, ...readJsonSafe(gcStateFile(root)) });

function saveGcState(root, state) {
  ensureDurableDir(paths(root).pull, { boundary: root });
  atomicWriteJson(gcStateFile(root), state);
}

async function cleanObjects({ s3, settings }, id) {
  let deleted = 0;
  for (const [bucket, prefix] of [[settings.rustfs.outBucket, `${id}/`], [settings.rustfs.snapBucket, `streams/${id}-`]]) {
    for (const object of await s3.listObjects(bucket, { prefix })) {
      await s3.deleteObject(bucket, object.key);
      deleted++;
    }
  }
  return deleted;
}

async function cleanPage(ctx, items, state, summary) {
  const cleaned = new Set();
  for (const item of items) {
    if (!afterCursor(item, state.objectsDone)) {
      cleaned.add(item.id);
      continue;
    }
    try {
      summary.objectsDeleted += await cleanObjects(ctx, item.id);
      cleaned.add(item.id);
      if (ctx.prefixClean) state.objectsDone = cursorOf(item);
    } catch (err) {
      ctx.prefixClean = false;
      summary.failures.push({ id: item.id, error: err.message });
    }
  }
  return cleaned;
}

const overBudget = (ctx) => ctx.now() - ctx.startedAt >= ctx.budgetMs;

async function sweepTickets(ctx, state, summary) {
  ctx.prefixClean = state.resume === null;
  let after = state.resume ?? state.rowsFrom;
  let frontierOpen = state.resume === null;
  for (;;) {
    const page = await ctx.queue.pruneCandidates({ objectsAfterMs: OBJECTS_AFTER_MS, rowsAfterMs: ctx.rowsAfterMs, after, limit: ctx.pageLimit });
    summary.pages++;
    const cleaned = await cleanPage(ctx, page.items, state, summary);
    const due = page.items.filter((item) => item.row_due && cleaned.has(item.id));
    const pruned = due.length > 0 ? await ctx.queue.prune(due.map((item) => item.id), ctx.rowsAfterMs) : 0;
    summary.rowsPruned += pruned;
    if (frontierOpen) {
      const blockedAt = pruned < due.length ? 0 : page.items.findIndex((item) => !due.includes(item));
      if (blockedAt === -1 && page.items.length > 0) state.rowsFrom = cursorOf(page.items.at(-1));
      else if (blockedAt > 0) state.rowsFrom = cursorOf(page.items[blockedAt - 1]);
      if (blockedAt !== -1) frontierOpen = false;
    }
    const finished = page.next === null;
    const cut = !finished && overBudget(ctx);
    state.resume = cut ? page.next : null;
    saveGcState(ctx.root, state);
    if (finished) return;
    if (cut) {
      summary.budgetCut = true;
      return;
    }
    after = page.next;
  }
}

const isOldBlob = (lastModified, now) => Number.isFinite(lastModified) && now - lastModified > BLOB_MIN_AGE_MS;

async function sweepBlobs(ctx, summary) {
  const { snapBucket } = ctx.settings.rustfs;
  const listedAt = ctx.now();
  const candidates = (await ctx.s3.listObjects(snapBucket, { prefix: 'blobs/' }))
    .filter((object) => isOldBlob(object.lastModified, listedAt))
    .map((object) => ({ object, match: BLOB_KEY.exec(object.key) }))
    .filter(({ match }) => match !== null)
    .map(({ object, match }) => ({ key: object.key, ref: { ns: match[1], sha256: match[2] } }));
  for (let from = 0; from < candidates.length; from += BLOB_BATCH) {
    const batch = candidates.slice(from, from + BLOB_BATCH);
    const live = new Set((await ctx.queue.blobsLive(batch.map((c) => c.ref))).map((ref) => blobKey(ref.ns, ref.sha256)));
    for (const { key } of batch.filter((c) => !live.has(c.key))) {
      if (overBudget(ctx)) {
        summary.budgetCut = true;
        return;
      }
      await ctx.hooks.beforeBlobDelete?.(key);
      try {
        const head = await ctx.s3.headObject(snapBucket, key);
        if (!head.exists || !isOldBlob(head.lastModified, ctx.now())) continue;
        await ctx.s3.deleteObject(snapBucket, key);
        summary.blobsDeleted++;
      } catch (err) {
        summary.failures.push({ key, error: err.message });
      }
    }
  }
}

/**
 * One retention run, admin login: objects of tickets terminal for 24 h, then the rows older than `rowsAfterMs` (only those whose objects are gone),
 * then orphan log chunks, then unreferenced blobs older than 14 days (re-HEADed before each delete). Paging resumes from `gc-state.json`.
 */
export async function gcOnce({ root, queue, s3, cfg, rowsAfterMs, now = Date.now, budgetMs = GC_BUDGET_MS, pageLimit = PAGE_LIMIT, hooks = {} }) {
  const settings = { ...DEFAULT_GLOBAL_CONFIG.lanePull, ...cfg, rustfs: { ...DEFAULT_GLOBAL_CONFIG.lanePull.rustfs, ...cfg?.rustfs } };
  const ctx = { root, queue, s3, settings, now, budgetMs, pageLimit, hooks, startedAt: now(), prefixClean: true, rowsAfterMs: rowsAfterFor(settings, rowsAfterMs) };
  const summary = { ledgersExpired: 0, pages: 0, objectsDeleted: 0, rowsPruned: 0, chunksPruned: 0, blobsDeleted: 0, budgetCut: false, failures: [] };
  summary.ledgersExpired = await sweepLocal(root, ledgerRetentionFor(settings), ctx.startedAt);
  await sweepTickets(ctx, readGcState(root), summary);
  summary.chunksPruned = await queue.pruneChunks({ terminalAfterMs: CHUNKS_TERMINAL_AFTER_MS, limit: CHUNKS_PER_RUN });
  if (settings.blobGc) await sweepBlobs(ctx, summary);
  return summary;
}

export function ledgerRetentionFor(settings) {
  if (settings.ledgerRetentionMs < LEDGER_RETENTION_FLOOR_MS) throw new Error(`lanePull.ledgerRetentionMs ${settings.ledgerRetentionMs} is below the ${LEDGER_RETENTION_FLOOR_MS}ms queue backup retention`);
  return settings.ledgerRetentionMs;
}

const TICKET_JSON = /^([0-9a-f-]{36})\.json$/;
const TICKET_DIR = /^[0-9a-f-]{36}$/;
const LOCK_FILE = 'recover.lock';
const LOCAL_LOCK_TIMEOUT_MS = 200;

function newestFileMtime(dir) {
  let newest = null;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true }).filter((e) => e.name !== LOCK_FILE)) {
    const full = path.join(dir, entry.name);
    const mtime = entry.isDirectory() ? newestFileMtime(full) : fs.statSync(full).mtimeMs;
    if (mtime !== null) newest = Math.max(newest ?? 0, mtime);
  }
  return newest;
}

const dirWrittenAt = (dir) => newestFileMtime(dir) ?? fs.statSync(dir).mtimeMs;

// the ledger entry goes last: a crash between the two leaves an entry the next run finishes; a ticket another process holds is skipped
async function expireTicket(root, id, due) {
  const dir = pullTicketDir(root, id);
  let expired = false;
  try {
    expired = await withTicketLock(root, id, () => {
      if (readAttempt(root, id) || !due()) return false;
      for (const name of fs.readdirSync(dir).filter((n) => n !== LOCK_FILE)) fs.rmSync(path.join(dir, name), { recursive: true, force: true });
      if (fs.existsSync(ledgerFile(root, id))) {
        fs.rmSync(ledgerFile(root, id), { force: true });
        fsyncDirectory(ledgerDir(root));
      }
      return true;
    }, { lockTimeoutMs: LOCAL_LOCK_TIMEOUT_MS });
  } catch (err) {
    if (!(err instanceof LockTimeoutError)) throw err;
  }
  if (expired) fs.rmSync(dir, { recursive: true, force: true });
  return expired;
}

/** Expires the local record of tickets accepted more than `retentionMs` ago (ledger entry, envelope, cursor, staged copies), and ticket directories nothing owns. */
export async function sweepLocal(root, retentionMs, now) {
  let expired = 0;
  const owned = new Set();
  for (const name of fs.existsSync(ledgerDir(root)) ? fs.readdirSync(ledgerDir(root)) : []) {
    const id = TICKET_JSON.exec(name)?.[1];
    if (!id) continue;
    owned.add(id);
    const ledgerDue = () => now - (readLedger(root, id)?.acceptedAt ?? now) >= retentionMs;
    if (ledgerDue() && (await expireTicket(root, id, ledgerDue))) expired++;
  }
  const pullDir = paths(root).pull;
  for (const name of fs.existsSync(pullDir) ? fs.readdirSync(pullDir) : []) {
    if (!TICKET_DIR.test(name) || owned.has(name)) continue;
    const dir = path.join(pullDir, name);
    const seenAt = dirWrittenAt(dir);
    const dirDue = () => readLedger(root, name) === null && now - seenAt >= retentionMs;
    if (dirDue() && (await expireTicket(root, name, dirDue))) expired++;
  }
  return expired;
}

function parseGcArgs(args) {
  const opts = {};
  for (let i = 0; i < args.length; i += 2) {
    const [flag, value] = [args[i], args[i + 1]];
    if (value === undefined || !['--queue-url', '--password-file', '--rows-after', '--budget'].includes(flag)) throw new Error(`unknown or incomplete argument "${flag}"`);
    opts[flag.slice(2)] = value;
  }
  for (const required of ['queue-url', 'password-file']) if (opts[required] === undefined) throw new Error(`--${required} is required (the admin login's own, never the submitter's)`);
  return opts;
}

export async function pullGcCommand(args = []) {
  try {
    const opts = parseGcArgs(args);
    const { config, error } = loadGlobalConfigOrError();
    if (error) throw new Error(error);
    const { rustfs, caFile } = config.lanePull;
    if (caFile === undefined || rustfs.endpoint === undefined || rustfs.credentialsEnvFile === undefined) throw new Error('config needs "lanePull.caFile", "lanePull.rustfs.endpoint" and "lanePull.rustfs.credentialsEnvFile"');
    const summary = await gcOnce({
      root: ensureStateDirs().root,
      queue: createQueueClient({ url: opts['queue-url'], passwordFile: opts['password-file'], caFile }),
      s3: createS3Client({ endpoint: rustfs.endpoint, region: rustfs.region, credentialsEnvFile: rustfs.credentialsEnvFile }),
      cfg: config.lanePull,
      rowsAfterMs: opts['rows-after'] === undefined ? undefined : parseDuration(opts['rows-after']),
      ...(opts.budget === undefined ? {} : { budgetMs: parseDuration(opts.budget) }),
    });
    process.stdout.write(`${JSON.stringify(summary)}\n`);
    return { exitCode: summary.failures.length > 0 ? 1 : 0 };
  } catch (err) {
    process.stderr.write(`lane pull gc: ${err.message}\n`);
    return { exitCode: 1 };
  }
}
