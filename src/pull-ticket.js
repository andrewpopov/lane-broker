import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { readAttempt, remoteCancelledResult, supervisorAlive, updateAttempt } from './attempts.js';
import { DEFAULT_GLOBAL_CONFIG } from './config.js';
import { enqueueOnce } from './pull-enqueue.js';
import { createEnvelope, pullTicketDir, readEnvelope, recordSnapshot, withTicketLock } from './pull-envelope.js';
import { ledgerAccept, readLedger } from './pull-ledger.js';
import { commitCursor, fileSize, newCursor, readCursor, reconcileLogFile, writeCursor } from './pull-log-cursor.js';
import { blobKey, mapBounded, uploadSnapshot } from './pull-snapshot.js';
import { artifactLimitsOf, installArtifactsDurable, receiveArtifacts } from './remote-artifacts.js';
import { CACHE_REJECT_RE, classifyRemoteResult, provesNeverStarted } from './remote-client.js';
import { lostResult } from './remote-reconcile.js';
import { makeReader } from './remote-stream.js';
import { presignUrl, sha256Hex } from './s3-presign.js';
import { atomicWriteJson, ensureDurableDir, fsyncDirectory, fsyncFile, isCancelled, LockTimeoutError } from './state.js';
import { CappedLogWriter, publishAttemptTerminal } from './supervisor.js';

export const OBJECT_RETRY_PASSES = 5;
export const DEFAULT_OBJECT_DEADLINE_MS = 20_000;
export const DEFAULT_STEP_BUDGET_MS = 60_000;
const LOG_PAGES_PER_STEP = 32;
const CLAIMED_WITHOUT_URLS_POLL_MS = 2_000;
const OBJECT_RETRY_POLL_MS = 5_000;
const BLOB_HEAD_CONCURRENCY = 8;
const LOG_DRAIN_PASSES = 5;
const LOG_DRAIN_RETRY_MS = 2_000;
const DIVERGENCE_DEADLINE_MS = 2_000;
const TRANSIENT_SQLSTATES = new Set(['53300', '57P01', '57P02', '57P03', '57014', '55P03', '40P01', '40001']);
const TLS_FAILURE_CODE = /^(ERR_TLS_|ERR_SSL_|CERT_|UNABLE_TO_|DEPTH_ZERO_|SELF_SIGNED_|HOSTNAME_MISMATCH)/;
const SERVER_CANCEL_SOURCES = new Set(['queued', 'quarantine', 'operator']);
export const GAP_MARKER = '\n[lane] streamed log differed from the verified log; verified log follows\n';

export const outKey = (id, epoch, nonce, name) => `${id}/${epoch}/${nonce}/${name}`;

export const presignerFor = ({ endpoint, region, creds, now = Date.now }) => ({ method, bucket, key, expiresSec }) =>
  presignUrl({ method, bucket, key, expiresSec, creds, endpoint, region, now: now() });

const stageDirOf = (root, id, tag) => path.join(pullTicketDir(root, id), 'stage', tag);
const installedMarkerOf = (root, id) => path.join(pullTicketDir(root, id), 'installed.json');
const hook = (run, name) => run.hooks?.[name]?.();

const lost = (reason, detail) => ({ kind: 'lost', exit: 75, reason, ...(detail ? { detail } : {}) });
const isQueueAuthored = (row) => row?.state === 'cancelled' && row.result?.author === 'queue' && SERVER_CANCEL_SOURCES.has(row.result.source);

/**
 * The decision a terminal row carries, or `{kind:'unproven'}` when a cancel was recorded without proof that the run stopped. A runner
 * result goes through `classifyRemoteResult`; a record the queue wrote itself (SQL cancel) is trusted by row identity and never meets the
 * binding check it cannot satisfy.
 */
export function classifyPullTerminal(row, expected) {
  const { state, result } = row;
  if (state === 'lost') return lost('runner-lost');
  if (state === 'cancelled' && result == null) return { kind: 'cancelled', exit: 130, proof: 'released' };
  if (isQueueAuthored(row)) {
    return result.source === 'queued' ? { kind: 'cancelled', exit: 130, source: 'queued' } : { kind: 'unproven', source: result.source };
  }
  if (result == null) return lost('result-unbound', 'a finished row carries no result');
  if (result.kind === 'rejected' && provesNeverStarted(result, expected)) return lost(CACHE_REJECT_RE.test(result.reason ?? '') ? 'snapshot-blob-missing' : 'runner-rejected', result.reason);
  const classified = classifyRemoteResult(result, expected);
  if (classified.outcome !== 'confirmed') return lost('result-unbound', classified.reason);
  if (result.kind === 'cancelled') return result.cancelConfirmed === true ? { kind: 'cancelled', exit: 130, source: 'runner' } : { kind: 'unproven', source: 'runner' };
  return { kind: 'finished', exit: classified.exitCode, ...(classified.phase ? { phase: classified.phase } : {}) };
}

/** What to append to the streamed segment so the file ends with the verified log, or null when nothing is needed. */
export function gapFillBytes(segment, full, { capped, lossDetected }) {
  if (capped || segment.equals(full)) return null;
  if (!lossDetected && full.subarray(0, segment.length).equals(segment)) return full.subarray(segment.length);
  return Buffer.concat([Buffer.from(GAP_MARKER), full]);
}

const settingsOf = (ctx) => ({ logGapHoldMs: 10_000, ...DEFAULT_GLOBAL_CONFIG.lanePull, ...ctx.cfg, rustfs: { ...DEFAULT_GLOBAL_CONFIG.lanePull.rustfs, ...ctx.cfg?.rustfs } });

function runOf(ctx) {
  const now = ctx.now ?? Date.now;
  return { ...ctx, cfg: settingsOf(ctx), now, startedAt: now(), envelope: null, objectDeadlineMs: ctx.objectDeadlineMs ?? DEFAULT_OBJECT_DEADLINE_MS, stepBudgetMs: ctx.stepBudgetMs ?? DEFAULT_STEP_BUDGET_MS };
}

const overBudget = (run) => run.now() - run.startedAt > run.stepBudgetMs;

async function persistPull(run, patch, attemptFields = {}) {
  const written = await updateAttempt(run.root, run.id, run.generation, (current) => ({ ...attemptFields, pull: { ...current.pull, ...(typeof patch === 'function' ? patch(current.pull ?? {}) : patch) } }), { fsync: true });
  if (!written.ok) throw new Error(`attempt ${run.id} is gone or moved past generation ${run.generation}`);
  return written.attempt;
}

function entryFor(run, row, decision, extra = {}) {
  const runnerResult = row?.result && !isQueueAuthored(row) ? row.result : null;
  return {
    logicalId: run.id,
    binding: run.envelope?.binding ?? null,
    epoch: row?.epoch ?? null,
    attemptRef: runnerResult ? row.attempt_ref ?? null : null,
    resultSha256: runnerResult ? sha256Hex(JSON.stringify(runnerResult)) : null,
    host: row?.host ?? null,
    startedAt: readAttempt(run.root, run.id)?.startedAt ?? null,
    generation: run.generation,
    decision,
    ...(runnerResult ? { result: runnerResult } : {}),
    ...extra,
  };
}

function accept(run, entry) {
  const outcome = ledgerAccept(run.root, entry, { hooks: run.hooks?.ledger });
  if (!outcome.same) {
    const tag = entry.resultSha256?.slice(0, 16);
    if (tag && tag !== outcome.entry.resultSha256?.slice(0, 16)) fs.rmSync(stageDirOf(run.root, run.id, tag), { recursive: true, force: true });
    run.report?.({ kind: 'ledger-conflict', stored: outcome.entry, refused: entry });
  }
  return outcome.entry;
}

async function settle(run, entry) {
  const stored = accept(run, entry);
  await hook(run, 'afterLedger');
  return applyEntry(run, stored);
}

function expectedObjects(run, row) {
  const result = row.result;
  if (result.uploadError) return [];
  return [['log', 'logSha256', 'logBytes'], ['artifacts', 'artifactsSha256', 'artifactsBytes']]
    .filter(([, sha]) => typeof result[sha] === 'string')
    .map(([name, sha, bytes]) => ({ name, key: outKey(run.id, row.epoch, run.envelope.nonce, name), sha256: result[sha], bytes: result[bytes] }));
}

async function fetchObject(run, key, file, expected) {
  ensureDurableDir(path.dirname(file), { boundary: run.root });
  let got;
  try {
    got = await run.s3.getObjectToFile(run.cfg.rustfs.outBucket, key, file, { deadlineMs: run.objectDeadlineMs });
  } catch (err) {
    return { ok: false, retry: true, reason: err.message };
  }
  if (!got.exists) return { ok: false, retry: true, reason: 'object missing' };
  if (got.sha256 !== expected.sha256 || got.size !== expected.bytes) {
    fs.rmSync(file, { force: true });
    return { ok: false, retry: false, reason: 'object does not match the hash the result names' };
  }
  fsyncFile(file);
  fsyncDirectory(path.dirname(file));
  return { ok: true };
}

async function stageObjects(run, row, tag) {
  const parts = {};
  const warnings = {};
  const transient = [];
  for (const object of expectedObjects(run, row)) {
    if (overBudget(run)) {
      transient.push([object.name, 'step budget spent']);
      continue;
    }
    const file = path.join(stageDirOf(run.root, run.id, tag), object.name);
    const got = await fetchObject(run, object.key, file, object);
    if (got.ok) parts[object.name] = { key: object.key, sha256: object.sha256, bytes: object.bytes, stagedPath: file };
    else if (got.retry) transient.push([object.name, got.reason]);
    else warnings[object.name] = got.reason;
  }
  return { parts, warnings, transient };
}

async function verifiedStaged(run, part, warnings, name) {
  if (!part) return null;
  const readVerified = () => {
    try {
      const bytes = fs.readFileSync(part.stagedPath);
      return bytes.length === part.bytes && sha256Hex(bytes) === part.sha256 ? bytes : null;
    } catch {
      return null;
    }
  };
  let bytes = readVerified();
  if (bytes === null && (await fetchObject(run, part.key, part.stagedPath, part)).ok) bytes = readVerified();
  if (bytes === null) warnings[name] ??= 'the staged object is gone and could not be fetched again';
  return bytes;
}

function openLogSession(run) {
  const stored = readCursor(run.root, run.id);
  const cursor = stored === null ? null : reconcileLogFile(run.root, run.id, run.logPath, stored);
  return { cursor, writer: new CappedLogWriter(run.logPath, { capBytes: run.logCapBytes, deriveTruncatedFromMarker: true }) };
}

function contiguousFrom(chunks, firstSeq) {
  const kept = [];
  for (const chunk of chunks) {
    if (chunk.seq !== firstSeq + kept.length) break;
    kept.push(chunk);
  }
  return kept;
}

async function pumpLog(run, row) {
  const session = openLogSession(run);
  const { writer } = session;
  let { cursor } = session;
  try {
    if (cursor === null || cursor.epoch !== row.epoch) {
      if (cursor !== null) {
        writer.write(Buffer.from(`[lane] restarted on ${row.host}\n`));
        await writer.durable();
      }
      cursor = newCursor(row.epoch, run.logPath);
      writeCursor(run.root, run.id, cursor);
    }
    if (cursor.ackedSeq >= 0) await run.queue.logAck(run.id, row.epoch, cursor.ackedSeq);
    for (let page = 0; page < LOG_PAGES_PER_STEP && !overBudget(run); page++) {
      const chunks = (await run.queue.logRead(run.id, row.epoch, cursor.ackedSeq)) ?? [];
      if (chunks.length === 0) return true;
      const contiguous = contiguousFrom(chunks, cursor.ackedSeq + 1);
      const skippingGap = contiguous.length === 0;
      if (skippingGap) {
        const since = cursor.gapSince ?? run.now();
        if (run.now() - since < run.cfg.logGapHoldMs) {
          if (cursor.gapSince === undefined) writeCursor(run.root, run.id, { ...cursor, gapSince: since });
          return false;
        }
      }
      const page = skippingGap ? contiguousFrom(chunks, chunks[0].seq) : contiguous;
      for (const chunk of page) {
        const bytes = Buffer.from(chunk.data, 'base64');
        writer.write(bytes);
        run.forward?.write(bytes);
      }
      const durable = await writer.durable();
      await hook(run, 'afterDurable');
      cursor = commitCursor(run.root, run.id, run.logPath, cursor, { ackedSeq: page.at(-1).seq, gapSince: undefined, ...(skippingGap ? { logGapSkipped: true } : {}), ...(durable ? {} : { logDiscarded: true }) });
      await hook(run, 'afterCursor');
      await run.queue.logAck(run.id, row.epoch, cursor.ackedSeq);
      await hook(run, 'afterAck');
    }
    return false;
  } finally {
    await writer.finish();
  }
}

function readSegment(file, start, end) {
  if (end <= start) return Buffer.alloc(0);
  const fd = fs.openSync(file, 'r');
  try {
    const bytes = Buffer.alloc(end - start);
    fs.readSync(fd, bytes, 0, bytes.length, start);
    return bytes;
  } finally {
    fs.closeSync(fd);
  }
}

async function fillLog(run, entry, warnings) {
  const full = await verifiedStaged(run, entry.log, warnings, 'log');
  if (full === null) return;
  const session = openLogSession(run);
  const { writer } = session;
  try {
    const cursor = session.cursor ?? newCursor(entry.epoch, run.logPath);
    if (session.cursor === null) writeCursor(run.root, run.id, cursor);
    if (cursor.gapFilled) return;
    const end = Math.min(cursor.logBytes, fileSize(run.logPath));
    const segment = readSegment(run.logPath, Math.min(cursor.epochStartBytes, end), end);
    const append = gapFillBytes(segment, full, { capped: writer.truncated, lossDetected: cursor.logLossDetected === true });
    if (append !== null) writer.write(append);
    await writer.durable();
    await hook(run, 'afterFill');
    commitCursor(run.root, run.id, run.logPath, cursor, { gapFilled: true });
  } finally {
    await writer.finish();
  }
}

async function installStaged(run, entry, warnings) {
  if (!entry.artifacts) return {};
  const marker = installedMarkerOf(run.root, run.id);
  if (fs.existsSync(marker)) return { remoteArtifacts: JSON.parse(fs.readFileSync(marker, 'utf8')).paths };
  await hook(run, 'beforeInstall');
  const bytes = await verifiedStaged(run, entry.artifacts, warnings, 'artifacts');
  if (bytes === null) return {};
  assertLedgerUnchanged(run, entry);
  const { envelope } = run;
  const patterns = envelope.declaredArtifacts ?? [];
  try {
    const received = await receiveArtifacts(makeReader(Readable.from(bytes)), { ticketId: envelope.header.ticketId, patterns, limits: envelope.artifactLimits ?? artifactLimitsOf(DEFAULT_GLOBAL_CONFIG) });
    if (!received.ok) {
      warnings.artifacts = `not returned: ${received.reason}`;
      return {};
    }
    const { written, refused } = installArtifactsDurable(envelope.worktreeRoot, received.files, patterns, { installedMarkerPath: marker });
    if (refused.length > 0) warnings.artifacts = refused.map((r) => `refused ${r.path}: ${r.reason}`).join('; ');
    return { remoteArtifacts: written };
  } catch (err) {
    warnings.artifacts = `not returned: ${err.message}`;
    return {};
  }
}

function assertLedgerUnchanged(run, entry) {
  const current = readLedger(run.root, run.id);
  if (JSON.stringify(current) !== JSON.stringify(entry)) throw new Error(`pull ledger entry for ${run.id} changed while it was being applied`);
}

function withDeadline(promise, ms) {
  let timer;
  const expiry = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`no answer within ${ms}ms`)), ms); });
  return Promise.race([promise, expiry]).finally(() => clearTimeout(timer));
}

// after publication and outside the ticket lock, so a queue outage never delays a local publish
async function reportDivergence(run, entry) {
  if (entry.resultSha256 === null) return;
  try {
    const row = await withDeadline(run.queue.get(run.id), DIVERGENCE_DEADLINE_MS);
    const other = row?.result && !isQueueAuthored(row) ? sha256Hex(JSON.stringify(row.result)) : null;
    if (other !== null && other !== entry.resultSha256) run.report?.({ kind: 'ledger-divergence', entry, divergence: { epoch: row.epoch, resultSha256: other } });
  } catch {
    // best effort: the entry already decided
  }
}

const resultWriterOf = (run) => run.writeResult ?? ((result, { fsync }) => atomicWriteJson(run.resultPath, result, { fsync }));

function resultFor(run, attempt, entry, extra) {
  const { decision } = entry;
  if (run.buildResult) return run.buildResult(decision, entry, attempt, extra);
  if (decision.kind === 'cancelled') return { ...remoteCancelledResult(run.id, attempt.startedAt), transport: 'pull', remoteCancelConfirmed: true };
  if (decision.kind === 'lost') return { ...lostResult(run.id, attempt, decision.reason, decision.detail ?? decision.reason), transport: 'pull' };
  return {
    id: run.id, exit: decision.exit, signal: null, executor: 'remote', transport: 'pull', runner: entry.host, startedAt: attempt.startedAt, endedAt: Date.now(),
    ...(decision.phase ? { phase: decision.phase } : {}), ...extra,
  };
}

async function publish(run, attempt, entry, extra) {
  const { published, finalResult } = await publishAttemptTerminal(run.root, run.id, run.generation, {
    drain: async () => {},
    buildConfirmedResult: () => resultFor(run, attempt, entry, extra),
    buildCancelledResult: () => ({ ...remoteCancelledResult(run.id, attempt.startedAt), transport: 'pull' }),
    writeResult: resultWriterOf(run),
    durable: true,
    cancelPrecedence: 'decision',
  });
  if (!published.ok && !published.alreadyPublished) throw new Error(`publishing ${run.id} was refused (${JSON.stringify(published)})`);
  return { state: 'settled', decision: entry.decision, result: finalResult, published: published.ok === true, entry };
}

function reconstructResult(run, entry, extra) {
  const attempt = { id: run.id, startedAt: entry.startedAt ?? entry.acceptedAt, runner: null };
  const result = resultFor(run, attempt, entry, extra);
  resultWriterOf(run)(result, { fsync: true });
  return { state: 'settled', decision: entry.decision, result, reconstructed: true, entry };
}

async function applyEntry(run, entry) {
  run.envelope ??= readEnvelope(run.root, run.id);
  const attempt = readAttempt(run.root, run.id);
  if (fs.existsSync(run.resultPath)) return { state: 'settled', already: true, decision: entry.decision };
  if (attempt && (attempt.generation !== run.generation || entry.generation !== attempt.generation)) return { state: 'stale', decision: entry.decision };
  const warnings = { ...entry.warnings };
  const extra = {};
  if (entry.decision.kind === 'finished') {
    await fillLog(run, entry, warnings);
    Object.assign(extra, await installStaged(run, entry, warnings));
    if (isCancelled(run.root, run.id)) extra.cancelRaced = true;
  }
  if (warnings.log) extra.remoteLogWarning = warnings.log;
  if (warnings.artifacts) extra.remoteArtifactsWarning = warnings.artifacts;
  assertLedgerUnchanged(run, entry);
  return attempt ? publish(run, attempt, entry, extra) : reconstructResult(run, entry, extra);
}

async function cancelIfRequested(run) {
  if (run.cancelRequested?.()) await requestCancel(run);
}

async function requestCancel(run) {
  const { pull } = readAttempt(run.root, run.id);
  if (pull?.cancel?.answer !== undefined) return;
  await persistPull(run, { cancel: { ...pull?.cancel, requestedAt: pull?.cancel?.requestedAt ?? run.now() } });
  const answer = await run.queue.cancel(run.id);
  await persistPull(run, (pull) => ({ cancel: { ...pull.cancel, answer: { state: answer.state, confirmed: answer.confirmed } } }));
}

export const urlsNeeded = (run, row) => !row.urls || row.urls.epoch !== row.epoch || row.urls.expiresAt - run.now() < run.cfg.urlRefreshSec * 1000;

function buildUrls(run, row, { withInputs }) {
  const { snapBucket, outBucket } = run.cfg.rustfs;
  const sign = (method, bucket, key) => run.presign({ method, bucket, key, expiresSec: run.cfg.urlTtlSec });
  const { snapshot, nonce } = run.envelope;
  return {
    v: 1,
    epoch: row.epoch,
    expiresAt: run.now() + run.cfg.urlTtlSec * 1000,
    ...(withInputs ? {
      stream: sign('GET', snapBucket, snapshot.key),
      blobs: Object.fromEntries(snapshot.blobs.map((blob) => [blob.sha256, sign('GET', snapBucket, blobKey(blob.ns, blob.sha256))])),
    } : {}),
    log: sign('PUT', outBucket, outKey(run.id, row.epoch, nonce, 'log')),
    artifacts: sign('PUT', outBucket, outKey(run.id, row.epoch, nonce, 'artifacts')),
  };
}

async function snapshotMissingPass(run, row) {
  const { pull } = readAttempt(run.root, run.id);
  const since = pull?.snapshotMissing?.since ?? run.now();
  if (!pull?.snapshotMissing) await persistPull(run, { snapshotMissing: { since } });
  await requestCancel(run);
  if (run.now() - since < run.cfg.quarantineLostAfterMs) return { state: row.state, next: run.cfg.pollMs };
  await updateAttempt(run.root, run.id, run.generation, { unresolved: 'snapshot-missing' });
  return { state: 'unresolved', reason: 'snapshot-missing', exitCode: 1, next: run.cfg.pollMs };
}

async function heldPass(run, row) {
  const running = row.state === 'running';
  const needsUrls = urlsNeeded(run, row);
  if (needsUrls) {
    const head = running ? { exists: true } : await run.s3.headObject(run.cfg.rustfs.snapBucket, run.envelope.snapshot.key, { deadlineMs: run.objectDeadlineMs });
    if (!head.exists) return snapshotMissingPass(run, row);
    await run.queue.attachUrls(run.id, row.epoch, buildUrls(run, row, { withInputs: !running }));
  }
  await pumpLog(run, row);
  return { state: row.state, next: needsUrls && row.state === 'claimed' ? CLAIMED_WITHOUT_URLS_POLL_MS : run.cfg.pollMs };
}

async function settleAndCancel(run, entry) {
  const stored = accept(run, entry);
  await run.queue.cancel(run.id).catch(() => {});
  await hook(run, 'afterLedger');
  return applyEntry(run, stored);
}

async function unprovenPass(run, row) {
  const { pull } = readAttempt(run.root, run.id);
  const since = pull?.unprovenSince ?? run.now();
  if (pull?.unprovenSince === undefined) await persistPull(run, { unprovenSince: since });
  if (run.now() - since >= run.cfg.quarantineLostAfterMs) return settleAndCancel(run, entryFor(run, row, lost('cancel-unproven')));
  await updateAttempt(run.root, run.id, run.generation, { unresolved: 'cancel-unconfirmed' });
  return { state: 'unresolved', reason: 'cancel-unconfirmed', exitCode: 1, next: run.cfg.pollMs };
}

async function terminalPass(run, row) {
  const { pull } = readAttempt(run.root, run.id);
  let decision = classifyPullTerminal(row, run.envelope.binding);
  if (pull?.snapshotMissing && decision.kind === 'cancelled') decision = lost('snapshot-missing');
  if (decision.kind === 'unproven') return unprovenPass(run, row);
  const drainWarnings = {};
  if (decision.kind !== 'lost' && row.epoch != null && !(await pumpLog(run, row))) {
    const passes = (pull?.drainPasses ?? 0) + 1;
    const drainSince = pull?.drainSince ?? run.now();
    if (passes < LOG_DRAIN_PASSES || run.now() - drainSince < run.cfg.logGapHoldMs + LOG_DRAIN_RETRY_MS) {
      await persistPull(run, { drainPasses: passes, drainSince });
      return { state: 'draining-log', next: LOG_DRAIN_RETRY_MS };
    }
    drainWarnings.log = `the streamed log was not fully drained after ${LOG_DRAIN_PASSES} passes`;
  }
  if (decision.kind !== 'finished') return settle(run, entryFor(run, row, decision));
  const entry = entryFor(run, row, decision);
  const { parts, warnings: stageWarnings, transient } = await stageObjects(run, row, entry.resultSha256.slice(0, 16));
  const warnings = { ...drainWarnings, ...stageWarnings };
  if (transient.length > 0) {
    const retries = (readAttempt(run.root, run.id).pull?.objectRetries ?? 0) + 1;
    if (retries < OBJECT_RETRY_PASSES) {
      await persistPull(run, { objectRetries: retries });
      return { state: 'fetching', next: Math.min(run.cfg.pollMs, OBJECT_RETRY_POLL_MS) };
    }
    for (const [name, reason] of transient) warnings[name] = `not retrieved after ${OBJECT_RETRY_PASSES} passes: ${reason}`;
  }
  return settle(run, { ...entry, ...parts, warnings });
}

async function observe(run) {
  const attempt = readAttempt(run.root, run.id);
  if (!attempt) return fs.existsSync(run.resultPath) ? { state: 'settled', already: true } : { state: 'gone' };
  run.envelope = readEnvelope(run.root, run.id);
  if (run.envelope === null || run.envelope.snapshot === undefined) {
    if (supervisorAlive(attempt)) return { state: 'preparing', next: run.cfg.pollMs };
    return settle(run, entryFor(run, null, lost('prepare-interrupted')));
  }
  if (!attempt.pull?.enqueue?.seenRowAt) return { state: 'needs-enqueue' };
  await cancelIfRequested(run);
  const row = await run.queue.get(run.id);
  if (row === null) return settle(run, entryFor(run, null, lost('queue-row-missing')));
  if (row.state !== 'quarantined' && attempt.pull.quarantinedSince !== undefined) await persistPull(run, { quarantinedSince: undefined });
  if (row.host && row.host !== attempt.pull.host) await persistPull(run, { host: row.host });
  switch (row.state) {
    case 'queued':
      return { state: 'queued', next: run.cfg.pollMs };
    case 'claimed':
    case 'running':
      return heldPass(run, row);
    case 'quarantined':
      return quarantinedPass(run, row, attempt);
    case 'withdrawn':
      return { state: 'withdrawn' };
    default:
      return terminalPass(run, row);
  }
}

async function quarantinedPass(run, row, attempt) {
  const since = attempt.pull.quarantinedSince ?? run.now();
  if (attempt.pull.quarantinedSince === undefined) await persistPull(run, { quarantinedSince: since });
  if (run.now() - since >= run.cfg.quarantineLostAfterMs) return settleAndCancel(run, entryFor(run, row, lost('queue-restored')));
  return { state: 'quarantined', next: run.cfg.pollMs };
}

// ledger first: with an acceptance on disk nothing else is read, from the queue or the store
function guarded(run, fn) {
  return withTicketLock(run.root, run.id, async () => {
    const entry = readLedger(run.root, run.id);
    if (entry !== null) return applyEntry(run, entry);
    const attempt = readAttempt(run.root, run.id);
    if (attempt && attempt.generation !== run.generation) return { state: 'stale' };
    return fn(run);
  }, { lockTimeoutMs: run.lockTimeoutMs });
}

async function missingSnapshotObject(run) {
  const { snapshot } = run.envelope;
  const { snapBucket } = run.cfg.rustfs;
  const stream = await run.s3.headObject(snapBucket, snapshot.key, { deadlineMs: run.objectDeadlineMs });
  if (!stream.exists) return 'snapshot-missing';
  let missing = false;
  await mapBounded(snapshot.blobs, BLOB_HEAD_CONCURRENCY, async (blob) => {
    const head = await run.s3.headObject(snapBucket, blobKey(blob.ns, blob.sha256), { deadlineMs: run.objectDeadlineMs });
    if (!head.exists || head.size !== blob.size) missing = true;
  });
  return missing ? 'snapshot-blob-missing' : null;
}

async function enqueueStage(run) {
  const { attempts = [] } = readAttempt(run.root, run.id).pull?.enqueue ?? {};
  if (attempts.length === 0) {
    const reason = await missingSnapshotObject(run);
    if (reason) return { outcome: 'lost', reason };
  }
  return enqueueOnce({
    root: run.root, id: run.id, generation: run.generation, queue: run.queue, ledgered: () => readLedger(run.root, run.id) !== null,
    now: run.now, rowRetentionMs: run.cfg.rowRetentionMs, lockTimeoutMs: run.lockTimeoutMs,
  });
}

async function afterStep(run, out) {
  const { entry, ...rest } = out;
  if (entry && !out.already) await reportDivergence(run, entry);
  return rest;
}

/** One bounded, idempotent pass. Returns `{state, next?}`; `settled` carries the decision and published result. Retry timing is the caller's. */
export async function stepPullTicket(ctx) {
  const run = runOf(ctx);
  try {
    const first = await guarded(run, observe);
    if (first.state !== 'needs-enqueue') return afterStep(run, first);
    const sent = await enqueueStage(run);
    if (sent.outcome === 'lost') return await afterStep(run, await guarded(run, () => settle(run, entryFor(run, null, lost(sent.reason)))));
    if (sent.outcome === 'queued') await persistPull(run, { stage: 'QUEUED' }, { phase: 'running' });
    return { state: 'waiting', next: 0 };
  } catch (err) {
    if (err instanceof LockTimeoutError) return { state: 'contended', next: run.cfg.pollMs };
    throw err;
  }
}

const OUTCOME_OF = { finished: 'confirmed', cancelled: 'cancelled', lost: 'lost' };

const isNamed = (err, name) => err?.name === name;

/** Only a transport failure, a timeout, 53300, 57P01, 08xxx or an S3 5xx is waited out; any other server SQLSTATE or an S3 4xx will answer the same way again. */
function tlsFailure(err) {
  for (let cause = err, depth = 0; cause && depth < 5; cause = cause.cause, depth++) if (TLS_FAILURE_CODE.test(String(cause.code ?? ''))) return true;
  return false;
}

export function isTransientError(err) {
  if (isNamed(err, 'LockTimeoutError')) return true;
  if (tlsFailure(err)) return false;
  if (isNamed(err, 'S3Error')) return err.status === null || err.status >= 500 || err.status === 408 || err.status === 429;
  if (!isNamed(err, 'QueueError')) return false;
  const code = String(err.code ?? '');
  if (TRANSIENT_SQLSTATES.has(code) || code.startsWith('08')) return true;
  return err.cause !== undefined && typeof err.cause.severity !== 'string';
}

const isPullServiceError = (err) => isNamed(err, 'QueueError') || isNamed(err, 'S3Error');
const sleepMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function preparePullTicket(ctx) {
  const run = runOf(ctx);
  const { envelopeFields, worktreeRoot, manifest } = ctx.prepare;
  const nonce = crypto.randomBytes(8).toString('hex');
  await persistPull(run, { stage: 'PREPARING', nonce }, { transport: 'pull', phase: 'probe' });
  await hook(run, 'afterMarker');
  const envelope = await createEnvelope(run.root, { ...envelopeFields, logicalId: run.id, nonce });
  await hook(run, 'afterEnvelope');
  const { snapshot } = await uploadSnapshot({
    s3: run.s3, bucket: run.cfg.rustfs.snapBucket, root: run.root, worktreeRoot, header: envelope.header, manifest, ticketId: run.id, generation: envelope.binding.generation,
    now: run.now, blobRefreshAfterDays: run.cfg.blobRefreshAfterDays, ...ctx.prepare.uploadOptions,
  });
  await recordSnapshot(run.root, run.id, snapshot);
  await hook(run, 'afterSnapshot');
  await persistPull(run, { stage: 'ENQUEUING' });
}

/** Prepares (when `ctx.prepare` is given), then steps until the ticket settles; a queue, store or lock failure is waited out, never a state change. */
export async function runPullTicket(ctx) {
  const sleep = ctx.sleep ?? sleepMs;
  if (ctx.prepare) await preparePullTicket(ctx);
  for (;;) {
    let step;
    try {
      step = await stepPullTicket(ctx);
    } catch (err) {
      if (isTransientError(err)) {
        await sleep(settingsOf(ctx).pollMs);
        continue;
      }
      if (!isPullServiceError(err)) throw err;
      await updateAttempt(ctx.root, ctx.id, ctx.generation, { unresolved: `pull-error: ${err.message}` }).catch(() => {});
      return { outcome: 'unresolved', reason: 'pull-error', error: err.message, code: err.code ?? err.status ?? null };
    }
    if (step.state === 'settled') return { outcome: OUTCOME_OF[step.decision.kind], decision: step.decision, exitCode: step.decision.exit, result: step.result, already: step.already === true };
    if (step.state === 'unresolved') return { outcome: 'unresolved', reason: step.reason, exitCode: step.exitCode };
    if (step.state === 'withdrawn' || step.state === 'gone' || step.state === 'stale') return { outcome: step.state };
    await sleep(step.next ?? settingsOf(ctx).pollMs);
  }
}

/** `{proof:true}` only when the queue committed queued -> withdrawn; every other answer, and a failed call, is settled by re-reading the row. */
export async function withdrawQueued(ctx) {
  const run = runOf(ctx);
  return withTicketLock(run.root, run.id, async () => {
    if (readLedger(run.root, run.id) !== null) return { proof: false, ledgered: true };
    let answer = null;
    try {
      answer = await run.queue.withdraw(run.id);
    } catch {
      answer = null;
    }
    const reread = answer === 'withdrawn' ? null : await run.queue.get(run.id).catch(() => null);
    if (answer !== 'withdrawn' && reread?.state !== 'withdrawn') return { proof: false, state: reread?.state ?? answer };
    await persistPull(run, { withdrawProof: true });
    return { proof: true };
  }, { lockTimeoutMs: run.lockTimeoutMs });
}
