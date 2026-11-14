import fs from 'node:fs';
import path from 'node:path';
import { readAttempt, remoteCancelledResult, supervisorAlive, updateAttempt } from './attempts.js';
import { DEFAULT_GLOBAL_CONFIG } from './config.js';
import { CANCELLED_BEFORE_ENQUEUE, enqueueOnce } from './pull-enqueue.js';
import { createEnvelope, pullTicketDir, readEnvelope, recordSnapshot, withTicketLock, writeOwedCancel } from './pull-envelope.js';
import { ledgerAccept, readLedger } from './pull-ledger.js';
import { blobKey, mapBounded, uploadSnapshot } from './pull-snapshot.js';
import { artifactLimitsOf, installArtifactsDurable, receiveArtifacts } from './remote-artifacts.js';
import { CACHE_REJECT_RE, classifyRemoteResult, provesNeverStarted } from './remote-client.js';
import { lostResult } from './remote-reconcile.js';
import { makeReader } from './remote-stream.js';
import { sha256AndSize } from './s3-client.js';
import { sha256Hex } from './s3-presign.js';
import { atomicWriteJson, ensureDurableDir, fsyncDirectory, fsyncFile, isCancelled, LockTimeoutError, readJsonSafe } from './state.js';
import { CappedLogWriter, LOG_CAP_BYTES, publishAttemptTerminal } from './supervisor.js';

export const OBJECT_RETRY_PASSES = 5;
export const DEFAULT_OBJECT_DEADLINE_MS = 20_000;
export const DEFAULT_STEP_BUDGET_MS = 60_000;
const OBJECT_RETRY_POLL_MS = 5_000;
const MIN_DEFERRED_POLL_MS = 1_000;
export const STREAM_CHUNK_BYTES = 1024 * 1024;
// room over the log cap and the artifact total for the framing a runner adds around them
export const OUTPUT_MARGIN_BYTES = 1024 * 1024;
const BLOB_HEAD_CONCURRENCY = 8;
const DIVERGENCE_DEADLINE_MS = 2_000;
const TRANSIENT_SQLSTATES = new Set(['53300', '57P01', '57P02', '57P03', '57014', '55P03', '40P01', '40001']);
const TLS_FAILURE_CODE = /^(ERR_TLS_|ERR_SSL_|CERT_|UNABLE_TO_|DEPTH_ZERO_|SELF_SIGNED_|HOSTNAME_MISMATCH)/;
const SERVER_CANCEL_SOURCES = new Set(['queued', 'operator']);
const QUEUE_LOST_REASONS = new Set(['queue-restored', 'release-cap', 'runner-lost']);
const OUTPUTS = [['log', 'logSha256', 'logBytes'], ['artifacts', 'artifactsSha256', 'artifactsBytes']];

/** Both sides derive an output key from the ticket row, so nothing about it is ever transmitted: `<host>/<ticket>/e<epoch>/<name>` in the out bucket. */
export const outKey = (host, id, epoch, name) => `${host}/${id}/e${epoch}/${name}`;

const stageDirOf = (root, id, tag) => path.join(pullTicketDir(root, id), 'stage', tag);
const dropStaged = (run) => fs.rmSync(path.join(pullTicketDir(run.root, run.id), 'stage'), { recursive: true, force: true });
const installedMarkerOf = (root, id) => path.join(pullTicketDir(root, id), 'installed.json');
const hook = (run, name) => run.hooks?.[name]?.();

const lost = (reason, detail) => ({ kind: 'lost', exit: 75, reason, ...(detail ? { detail } : {}) });
const provenNeverStarted = (pull) => pull?.withdrawProof === true || pull?.neverEnqueued === true;
// a supervisor that died after the proof and before the fallback leaves a ticket nothing ran: the retryable refusal its host would have answered (76 where dispatch-only)
const neverStartedDecision = (run, pull) => ({ ...lost(pull.withdrawProof === true ? 'withdrawn-never-started' : 'never-enqueued'), exit: run.envelope?.publication?.refusalExit ?? LOST_EXIT });
const isQueueAuthored = (row) => row?.result?.author === 'queue' && (row.state === 'lost' || (row.state === 'cancelled' && SERVER_CANCEL_SOURCES.has(row.result.source)));
const lostReasonOf = (row) => (QUEUE_LOST_REASONS.has(row.result?.reason) ? row.result.reason : 'runner-lost');

/**
 * The decision a terminal row carries, or `{kind:'unproven'}` when a cancel was recorded without proof that the run stopped. A runner
 * result goes through `classifyRemoteResult`; a record the queue wrote itself (SQL cancel) is trusted by row identity and never meets the
 * binding check it cannot satisfy.
 */
export function classifyPullTerminal(row, expected) {
  const { state, result } = row;
  if (state === 'lost') return lost(lostReasonOf(row));
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

const settingsOf = (ctx) => ({ ...DEFAULT_GLOBAL_CONFIG.lanePull, ...ctx.cfg, rustfs: { ...DEFAULT_GLOBAL_CONFIG.lanePull.rustfs, ...ctx.cfg?.rustfs } });

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
    logBase: logSizeOf(run.logPath),
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

function expectedObjects(run, row, names) {
  const result = row.result;
  if (result == null || result.uploadError) return [];
  return OUTPUTS
    .filter(([name, sha]) => names.includes(name) && typeof result[sha] === 'string')
    .map(([name, sha, bytes]) => ({ name, key: outKey(row.host, run.id, row.epoch, name), sha256: result[sha], bytes: result[bytes] }));
}

const objectCapOf = (run, name) => (name === 'log'
  ? (run.logCapBytes ?? LOG_CAP_BYTES)
  : (run.envelope?.artifactLimits ?? artifactLimitsOf(DEFAULT_GLOBAL_CONFIG)).maxTotalBytes) + OUTPUT_MARGIN_BYTES;

// a runner's claim about its own object decides how much is read, so the claim is bounded before anything is asked of the store
function sizeProblem(run, expected) {
  if (!Number.isInteger(expected.bytes) || expected.bytes < 0) return 'the result names no size for the object';
  const cap = objectCapOf(run, expected.name);
  return expected.bytes > cap ? `the result names ${expected.bytes} bytes, past the ${cap} byte limit` : null;
}

async function fetchObject(run, key, file, expected) {
  const refused = sizeProblem(run, expected);
  if (refused !== null) return { ok: false, retry: false, reason: refused };
  ensureDurableDir(path.dirname(file), { boundary: run.root });
  let got;
  try {
    got = await run.s3.getObjectToFile(run.cfg.rustfs.outBucket, key, file, { deadlineMs: run.objectDeadlineMs, maxBytes: expected.bytes });
  } catch (err) {
    return { ok: false, retry: true, reason: err.message };
  }
  if (!got.exists) return { ok: false, retry: true, reason: 'object missing' };
  if (got.tooLarge || got.sha256 !== expected.sha256 || got.size !== expected.bytes) {
    fs.rmSync(file, { force: true });
    return { ok: false, retry: false, reason: 'object does not match the hash the result names' };
  }
  fsyncFile(file);
  fsyncDirectory(path.dirname(file));
  return { ok: true };
}

async function stageObjects(run, row, tag, names) {
  const parts = {};
  const warnings = {};
  const transient = [];
  const deferred = [];
  let attempted = 0;
  // an object that failed for good (a hash or size mismatch, over the cap) is remembered in the attempt, so no later pass downloads it again
  const settledFailures = readAttempt(run.root, run.id)?.pull?.objectWarnings ?? {};
  for (const object of expectedObjects(run, row, names)) {
    if (settledFailures[object.name] !== undefined) {
      warnings[object.name] = settledFailures[object.name];
      continue;
    }
    const file = path.join(stageDirOf(run.root, run.id, tag), object.name);
    const part = { name: object.name, key: object.key, sha256: object.sha256, bytes: object.bytes, stagedPath: file };
    if (await stagedMatches(part)) {
      parts[object.name] = part;
      continue;
    }
    // the first object a pass has to fetch is always attempted, so a pass that starts with its budget already spent still makes progress
    if (attempted > 0 && overBudget(run)) {
      deferred.push(object.name);
      continue;
    }
    attempted += 1;
    const got = await fetchObject(run, object.key, file, object);
    if (got.ok) parts[object.name] = part;
    else if (got.retry) transient.push([object.name, got.reason]);
    else {
      warnings[object.name] = got.reason;
      await persistPull(run, (pull) => ({ objectWarnings: { ...pull.objectWarnings, [object.name]: got.reason } }));
    }
  }
  return { parts, warnings, transient, deferred };
}

// an object an earlier pass already fetched and verified is never fetched again
async function stagedMatches(part) {
  try {
    const { sha256, size } = await sha256AndSize(part.stagedPath);
    return size === part.bytes && sha256 === part.sha256;
  } catch {
    return false;
  }
}

// the staged file itself, once its size and hash match what the result names (fetched again when it is gone or changed); never read whole into memory
async function verifiedStaged(run, part, warnings, name) {
  if (!part) return null;
  if ((await stagedMatches(part)) || (await fetchObject(run, part.key, part.stagedPath, part)).ok) return part.stagedPath;
  warnings[name] ??= 'the staged object is gone and could not be fetched again';
  return null;
}

// recorded in the ledger entry (the size the log had when the decision was taken), so a replay or a rebuild rewrites from there and never appends twice
const logSizeOf = (logPath) => (fs.existsSync(logPath) ? fs.statSync(logPath).size : 0);

function truncateLogTo(logPath, bytes) {
  try {
    if (fs.statSync(logPath).size > bytes) {
      fs.truncateSync(logPath, bytes);
      fsyncFile(logPath);
    }
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
}

async function copyToLog(writer, file, forward) {
  for await (const chunk of fs.createReadStream(file, { highWaterMark: STREAM_CHUNK_BYTES })) {
    if (writer.truncated || writer.failed) return;
    forward?.write(chunk);
    if (!writer.write(chunk) && !writer.failed) await drained(writer.stream);
  }
}

// a write error or a close after backpressure never emits 'drain', so waiting on it alone would hold the ticket lock forever
function drained(stream) {
  return new Promise((resolve) => {
    const done = () => {
      for (const event of ['drain', 'error', 'close']) stream.off(event, done);
      resolve();
    };
    for (const event of ['drain', 'error', 'close']) stream.once(event, done);
  });
}

async function writeFullLog(run, entry, warnings) {
  const staged = await verifiedStaged(run, entry.log, warnings, 'log');
  if (staged === null) return;
  truncateLogTo(run.logPath, entry.logBase ?? 0);
  const writer = new CappedLogWriter(run.logPath, { capBytes: run.logCapBytes, deriveTruncatedFromMarker: true });
  try {
    await copyToLog(writer, staged, run.forward);
    await writer.durable();
    if (writer.failed) warnings.log = `the log could not be written: ${writer.error?.message ?? 'write failed'}`;
    await hook(run, 'afterFill');
  } finally {
    await writer.finish();
  }
}

async function installStaged(run, entry, warnings) {
  if (!entry.artifacts) return {};
  const marker = installedMarkerOf(run.root, run.id);
  if (fs.existsSync(marker)) return { remoteArtifacts: JSON.parse(fs.readFileSync(marker, 'utf8')).paths };
  await hook(run, 'beforeInstall');
  const staged = await verifiedStaged(run, entry.artifacts, warnings, 'artifacts');
  if (staged === null) return {};
  assertLedgerUnchanged(run, entry);
  const { envelope } = run;
  const patterns = envelope.declaredArtifacts ?? [];
  try {
    const received = await receiveArtifacts(makeReader(fs.createReadStream(staged)), { ticketId: envelope.header.ticketId, patterns, limits: envelope.artifactLimits ?? artifactLimitsOf(DEFAULT_GLOBAL_CONFIG) });
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

const defaultFinishedResult = (run, entry, attempt, extra) => ({
  id: run.id, exit: entry.decision.exit, signal: null, executor: 'remote', transport: 'pull', runner: entry.host, startedAt: attempt.startedAt, endedAt: Date.now(),
  ...(entry.decision.phase ? { phase: entry.decision.phase } : {}), ...extra,
});

function resultFor(run, attempt, entry, extra) {
  const { decision } = entry;
  if (decision.kind === 'cancelled') return { ...remoteCancelledResult(run.id, attempt.startedAt), transport: 'pull', remoteCancelConfirmed: true, ...extra };
  if (decision.kind === 'lost') return { ...lostResult(run.id, attempt, decision.reason, decision.detail ?? decision.reason), exit: decision.exit, transport: 'pull' };
  return (run.buildFinishedResult ?? ((...args) => defaultFinishedResult(run, ...args)))(entry, attempt, extra);
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
  resultWriterOf(run)(result, { fsync: true, rebuilt: true });
  return { state: 'settled', decision: entry.decision, result, reconstructed: true, entry };
}

async function applyEntry(run, entry) {
  run.envelope ??= readEnvelope(run.root, run.id);
  const attempt = readAttempt(run.root, run.id);
  if (fs.existsSync(run.resultPath)) {
    dropStaged(run);
    return { state: 'settled', already: true, decision: entry.decision };
  }
  if (attempt && (attempt.generation !== run.generation || entry.generation !== attempt.generation)) return { state: 'stale', decision: entry.decision };
  const warnings = { ...entry.warnings };
  const extra = {};
  if (entry.decision.kind === 'finished' || entry.decision.kind === 'cancelled') await writeFullLog(run, entry, warnings);
  if (entry.decision.kind === 'finished') {
    Object.assign(extra, await installStaged(run, entry, warnings));
    if (isCancelled(run.root, run.id)) extra.cancelRaced = true;
  }
  if (warnings.log) extra.remoteLogWarning = warnings.log;
  if (warnings.artifacts) extra.remoteArtifactsWarning = warnings.artifacts;
  assertLedgerUnchanged(run, entry);
  const settled = await (attempt ? publish(run, attempt, entry, extra) : reconstructResult(run, entry, extra));
  dropStaged(run);
  return settled;
}

// a cancel recorded before the process died but never answered is sent by whoever recovers the ticket
const cancelPending = (run, pull) => (pull?.cancel?.requestedAt !== undefined && pull.cancel.answer === undefined) || isCancelled(run.root, run.id);

async function cancelIfRequested(run, pull) {
  if (run.cancelRequested?.() || cancelPending(run, pull)) await requestCancel(run);
}

async function requestCancel(run) {
  const { pull } = readAttempt(run.root, run.id);
  if (pull?.cancel?.answer !== undefined) return;
  await persistPull(run, { cancel: { ...pull?.cancel, requestedAt: pull?.cancel?.requestedAt ?? run.now() } });
  const answer = await run.queue.cancel(run.id);
  await persistPull(run, (pull) => ({ cancel: { ...pull.cancel, answer: { state: answer.state, confirmed: answer.confirmed } } }));
}

// a runner holds its own store credentials, so a held row asks nothing of the supervisor; it is read again at the next poll
const heldPass = (run, row) => ({ state: row.state, next: run.cfg.pollMs });

async function unprovenPass(run, row) {
  const { pull } = readAttempt(run.root, run.id);
  const since = pull?.unprovenSince ?? run.now();
  if (pull?.unprovenSince === undefined) await persistPull(run, { unprovenSince: since });
  if (run.now() - since >= run.cfg.unprovenCancelLostAfterMs) return settle(run, entryFor(run, row, lost('cancel-unproven')));
  await updateAttempt(run.root, run.id, run.generation, { unresolved: 'cancel-unconfirmed' });
  return { state: 'unresolved', reason: 'cancel-unconfirmed', exitCode: 1, next: run.cfg.pollMs };
}

async function terminalPass(run, row) {
  const decision = classifyPullTerminal(row, run.envelope.binding);
  if (decision.kind === 'unproven') return unprovenPass(run, row);
  if (decision.kind !== 'finished' && decision.kind !== 'cancelled') return settle(run, entryFor(run, row, decision));
  const entry = entryFor(run, row, decision);
  const names = decision.kind === 'finished' ? OUTPUTS.map(([name]) => name) : ['log'];
  const { parts, warnings, transient, deferred } = await stageObjects(run, row, entry.resultSha256?.slice(0, 16) ?? 'queue', names);
  // an object this step never got to is not a failure and costs no retry: the next pass starts from what is staged
  if (deferred.length > 0 && transient.length === 0) return { state: 'fetching', next: MIN_DEFERRED_POLL_MS };
  if (transient.length > 0) {
    const retries = (readAttempt(run.root, run.id).pull?.objectRetries ?? 0) + 1;
    if (retries < OBJECT_RETRY_PASSES) {
      await persistPull(run, { objectRetries: retries });
      return { state: 'fetching', next: Math.min(run.cfg.pollMs, OBJECT_RETRY_POLL_MS) };
    }
    for (const [name, reason] of transient) warnings[name] = `not retrieved after ${OBJECT_RETRY_PASSES} passes: ${reason}`;
    for (const name of deferred) warnings[name] = `not retrieved after ${OBJECT_RETRY_PASSES} passes: the step budget was spent on another object`;
  }
  return settle(run, { ...entry, ...parts, warnings });
}

function queuedTooLong(run, attempt) {
  const limit = run.envelope.queuedTimeoutMs;
  const seenRowAt = attempt.pull.enqueue?.seenRowAt;
  return limit !== undefined && seenRowAt != null && run.now() - Date.parse(seenRowAt) >= limit;
}

async function expireQueued(run, attempt) {
  if (!(await withdrawLocked(run)).proof) return { state: 'queued', next: run.cfg.pollMs };
  if (supervisorAlive(attempt)) return { state: 'withdrawn', proof: true, reason: `queued-timeout: no runner claimed it within ${run.envelope.queuedTimeoutMs}ms` };
  return settle(run, entryFor(run, null, neverStartedDecision(run, { withdrawProof: true })));
}

async function settleCommittedWithdraw(run) {
  await persistPull(run, { withdrawProof: true });
  return settle(run, entryFor(run, null, neverStartedDecision(run, { withdrawProof: true })));
}

async function observe(run) {
  const attempt = readAttempt(run.root, run.id);
  if (!attempt) return fs.existsSync(run.resultPath) ? { state: 'settled', already: true } : { state: 'gone' };
  if (attempt.executor === 'local') return { state: 'local' };
  run.envelope = readEnvelope(run.root, run.id);
  if (provenNeverStarted(attempt.pull) && !supervisorAlive(attempt)) return settle(run, entryFor(run, null, neverStartedDecision(run, attempt.pull)));
  if (run.envelope === null || run.envelope.snapshot === undefined) {
    if (supervisorAlive(attempt)) return { state: 'preparing', next: run.cfg.pollMs };
    return settle(run, entryFor(run, null, lost('prepare-interrupted')));
  }
  if (!attempt.pull?.enqueue?.seenRowAt) return { state: 'needs-enqueue' };
  await cancelIfRequested(run, attempt.pull);
  const row = await run.queue.get(run.id);
  if (row === null) return settle(run, entryFor(run, null, lost('queue-row-missing')));
  if (row.host && row.host !== attempt.pull.host) await persistPull(run, { host: row.host });
  switch (row.state) {
    case 'queued':
      return queuedTooLong(run, attempt) ? expireQueued(run, attempt) : { state: 'queued', next: run.cfg.pollMs };
    case 'claimed':
    case 'running':
      return heldPass(run, row);
    case 'withdrawn':
      return supervisorAlive(attempt) ? { state: 'withdrawn' } : settleCommittedWithdraw(run);
    default:
      return terminalPass(run, row);
  }
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
    cancelRequested: () => run.cancelRequested?.() === true || isCancelled(run.root, run.id),
    settleCancelled: () => settle(run, entryFor(run, null, { kind: 'cancelled', exit: CANCELLED_EXIT, source: 'never-enqueued' })),
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
    await hook(run, 'afterEnqueueStage');
    if (sent.outcome === 'lost') return await afterStep(run, await guarded(run, () => settle(run, entryFor(run, null, lost(sent.reason)))));
    if (sent.outcome === CANCELLED_BEFORE_ENQUEUE) return await afterStep(run, sent.settled);
    if (sent.outcome === 'queued') await persistPull(run, { stage: 'QUEUED' }, { phase: 'running' });
    return { state: 'waiting', next: 0 };
  } catch (err) {
    if (err instanceof LockTimeoutError) return { state: 'contended', next: run.cfg.pollMs };
    throw err;
  }
}

const OUTCOME_OF = { finished: 'confirmed', cancelled: 'cancelled', lost: 'lost' };

const settledOutcome = (step) => ({ outcome: OUTCOME_OF[step.decision.kind], decision: step.decision, exitCode: step.decision.exit, result: step.result, already: step.already === true });

const CANCELLED_EXIT = 130;
const LOST_EXIT = 75;

/** The process exit a `runPullTicket` outcome ends the supervisor with; undefined for an outcome that is not an exit (a committed withdraw goes to the never-started path). */
export function pullExitCode(outcome) {
  switch (outcome.outcome) {
    case 'confirmed':
      return Number.isInteger(outcome.exitCode) ? outcome.exitCode : 1;
    case 'cancelled':
      return CANCELLED_EXIT;
    case 'lost':
      return Number.isInteger(outcome.exitCode) ? outcome.exitCode : LOST_EXIT;
    case 'unresolved':
      return 1;
    default:
      return undefined;
  }
}

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

/** Resolves after `ms`, or early if `signal` fires during the wait; a signal that had already fired does not shorten a later wait. */
export const sleepUnlessAborted = (ms, signal) => new Promise((resolve) => {
  const timer = setTimeout(done, ms);
  signal?.addEventListener('abort', done, { once: true });
  function done() {
    clearTimeout(timer);
    signal?.removeEventListener('abort', done);
    resolve();
  }
});

export async function preparePullTicket(ctx) {
  const run = runOf(ctx);
  const { envelopeFields, worktreeRoot, manifest } = ctx.prepare;
  const existing = readEnvelope(run.root, run.id);
  await persistPull(run, { stage: 'PREPARING' }, { transport: 'pull', phase: 'probe' });
  await hook(run, 'afterMarker');
  const envelope = existing ?? await createEnvelope(run.root, { ...envelopeFields, logicalId: run.id });
  await hook(run, 'afterEnvelope');
  if (envelope.snapshot === undefined) {
    const { snapshot } = await uploadSnapshot({
      s3: run.s3, bucket: run.cfg.rustfs.snapBucket, root: run.root, worktreeRoot, header: envelope.header, manifest, ticketId: run.id, generation: envelope.binding.generation,
      now: run.now, blobRefreshAfterDays: run.cfg.blobRefreshAfterDays, ...ctx.prepare.uploadOptions,
    });
    await recordSnapshot(run.root, run.id, snapshot);
  }
  await hook(run, 'afterSnapshot');
  await persistPull(run, { stage: 'ENQUEUING' });
}

/** Preparation stopped waiting: the caller cancelled, or the store kept failing for `prepareBudgetMs`. Nothing may have been enqueued yet, which is for `proveNeverEnqueued` to say. */
export class PrepareAbandoned extends Error {
  constructor(reason, cause) {
    super(reason === 'cancelled' ? 'cancelled while the snapshot was being prepared' : `the snapshot could not be prepared within the budget: ${cause?.message}`, { cause });
    this.name = 'PrepareAbandoned';
    this.reason = reason;
  }
}

export async function preparePullTicketWithRetry(ctx) {
  const sleep = ctx.sleep ?? sleepMs;
  const now = ctx.now ?? Date.now;
  const { pollMs, prepareBudgetMs } = settingsOf(ctx);
  const startedAt = now();
  for (;;) {
    if (ctx.cancelRequested?.()) throw new PrepareAbandoned('cancelled');
    try {
      return await preparePullTicket(ctx);
    } catch (err) {
      if (!isTransientError(err)) throw err;
      if (ctx.cancelRequested?.()) throw new PrepareAbandoned('cancelled', err);
      if (now() - startedAt >= prepareBudgetMs) throw new PrepareAbandoned('budget', err);
      await sleep(pollMs);
    }
  }
}

// another publisher (lane cancel, the reconcile sweep) settled the attempt first: its result stands, and its exit is the answer
function settledElsewhere(ctx) {
  const published = readJsonSafe(ctx.resultPath);
  return published === null ? { outcome: 'gone' } : { outcome: 'confirmed', exitCode: published.exit, result: published, already: true };
}

/** After `queueUnreachableAfterMs` of failed queue calls the ticket settles lost 75 at once, ledgered like any decision, and the cancel the row is owed goes into a marker the reconcile sweep sends once the queue answers. */
async function abandonUnreachableQueue(ctx) {
  const run = runOf(ctx);
  const settled = await withTicketLock(run.root, run.id, async () => {
    run.envelope = readEnvelope(run.root, run.id);
    writeOwedCancel(run.root, run.id, { generation: run.generation, at: run.now() });
    return settle(run, entryFor(run, null, lost('queue-unreachable')));
  }, { lockTimeoutMs: run.lockTimeoutMs });
  return settled.decision === undefined ? { outcome: settled.state } : settledOutcome(settled);
}

/** Prepares (when `ctx.prepare` is given), then steps until the ticket settles; a queue, store or lock failure is waited out, never a state change. */
export async function runPullTicket(ctx) {
  const sleep = ctx.sleep ?? sleepMs;
  const now = ctx.now ?? Date.now;
  const { pollMs, queueUnreachableAfterMs } = settingsOf(ctx);
  let queueDownSince = null;
  if (ctx.prepare) await preparePullTicketWithRetry(ctx);
  for (;;) {
    let step;
    try {
      step = await stepPullTicket(ctx);
      queueDownSince = null;
    } catch (err) {
      if (isTransientError(err)) {
        if (isNamed(err, 'QueueError')) queueDownSince ??= now();
        if (queueDownSince !== null && now() - queueDownSince >= queueUnreachableAfterMs) return abandonUnreachableQueue(ctx);
        await sleep(pollMs);
        continue;
      }
      if (!isPullServiceError(err)) throw err;
      await updateAttempt(ctx.root, ctx.id, ctx.generation, { unresolved: `pull-error: ${err.message}` }).catch(() => {});
      return { outcome: 'unresolved', reason: 'pull-error', error: err.message, code: err.code ?? err.status ?? null };
    }
    if (step.state === 'settled' && step.decision === undefined) return settledElsewhere(ctx);
    if (step.state === 'settled') return settledOutcome(step);
    if (step.state === 'unresolved') return { outcome: 'unresolved', reason: step.reason, exitCode: step.exitCode };
    if (step.state === 'withdrawn') return { outcome: 'withdrawn', ...(step.proof ? { proof: true, reason: step.reason } : {}) };
    if (step.state === 'gone' || step.state === 'stale') return { outcome: step.state };
    await sleep(step.next ?? settingsOf(ctx).pollMs);
  }
}

/** `{proof:true}` only when the queue committed queued -> withdrawn; every other answer, and a failed call, is settled by re-reading the row. */
async function withdrawLocked(run) {
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
}

export async function withdrawQueued(ctx) {
  const run = runOf(ctx);
  return withTicketLock(run.root, run.id, () => withdrawLocked(run), { lockTimeoutMs: run.lockTimeoutMs });
}

export async function proveNeverEnqueued(ctx) {
  const run = runOf(ctx);
  return withTicketLock(run.root, run.id, async () => {
    if (readLedger(run.root, run.id) !== null) return { proof: false, ledgered: true };
    if ((readAttempt(run.root, run.id)?.pull?.enqueue?.attempts ?? []).length > 0) return { proof: false };
    await persistPull(run, { neverEnqueued: true });
    return { proof: true };
  }, { lockTimeoutMs: run.lockTimeoutMs });
}

export function assertWithdrawCommitted(root, id) {
  const attempt = readAttempt(root, id);
  if (attempt?.transport === 'pull' && !provenNeverStarted(attempt.pull)) throw new Error(`pull attempt ${id} may not fall back to local before the queue has committed a withdraw or no enqueue was ever recorded`);
}
