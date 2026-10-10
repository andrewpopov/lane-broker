import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import {
  paths,
  ensureStateDirs,
  bootId,
  appendHistory,
  atomicWriteJson,
  readJsonSafe,
  isCancelled,
  cancelMarkerPath,
  expireMarkerPath,
  withdrawMarkerPath,
  isWithdrawn,
  writeCancelMarkerFile,
  LockTimeoutError,
  MigrationInProgressError,
  testDrainAt,
  testHoldAt,
} from './state.js';
import { enqueue, existingTicket, tryStart, dequeueSync, couldAdmitNow, withdrawQueued, restoreQueued, discardRebinding, readQueuedRecord } from './scheduler.js';
import { touchSimArmFor } from './sim-arm.js';
import { readLease, writeLease, removeLease, listLeases, isGroupAlive, processStartTime } from './lease.js';
import { selectLeaseTree } from './cpu.js';
import { DescendantTracker, hasLiveMembers, reapLogLine } from './descendants.js';
import { reloadGlobalConfig } from './config.js';
import { effectiveNow } from './priority-clock.js';
import { isPriorityTier, priorityAuditOf, waitedMs } from './priority.js';
import { CLASS_ENFORCEMENT_ENV, CLASSES_CAPABILITY, isEnforcementRequired, classTransport, resolveClasses } from './classes.js';
import { detectResourceCapacity, checkResourceBudget, localSimRefusal, noRunnerRefusal, leaseCpuCores, terminalRowDefaults } from './resources.js';
import { selectRunner, dispatchRemote, needsProtocol2, rebalanceBlocked, hasMeasuredHeadroom } from './remote-client.js';
import { buildManifest, RemoteIneligibleError, validateRemoteDeps } from './remote-manifest.js';
import { ARTIFACTS_CAPABILITY, artifactLimitsOf, installArtifacts } from './remote-artifacts.js';
import { REMOTE_WITHDRAW_CAPABILITY } from './capabilities.js';
import { createAttempt, readAttempt, updateAttempt, fallbackToLocal, publishTerminal, remoteCancelledResult } from './attempts.js';
import { writeBrokerLog, OBSERVED_HISTORY_MAX } from './admission.js';
import { maybeDailyGc } from './gc.js';
import { NoProgressWatchdog, NO_PROGRESS_EXIT, NO_PROGRESS_REASON, noProgressMessage } from './no-progress.js';
import { observedLeaseFields, summarizeObservedCpu, sanitizeObservedCpu, sanitizeRssPeak, integratedCpuSeconds, sanitizeCpuSeconds } from './observed.js';

const LOG_CAP_BYTES = 50 * 1024 * 1024;
const CANCEL_GRACE_MS = 10_000;
const GROUP_REAPED_REASON = 'leader-exited-group-reaped';
const MAX_OMITTED_SYMLINKS_LISTED = 10;

function readTicketFromEnv() {
  const raw = process.env.LANE_BROKER_TICKET;
  if (!raw) throw new Error('lane-broker supervisor: LANE_BROKER_TICKET not set');
  return JSON.parse(Buffer.from(raw, 'base64').toString('utf8'));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let lastConfigErrorMessage = null;

/**
 * Persist a config reload failure so it's visible via `lane status` instead
 * of silently pinning the last known-good thresholds forever — an operator
 * who saved a broken file otherwise believes their new thresholds are live
 * when they are not, which is operationally indistinguishable from the
 * incident this fix exists to prevent. Only the first occurrence of a given
 * message goes to stderr: the shared config staying broken for many
 * `sampleMs` must not spam a line every poll. The persisted record itself is
 * a single overwritten object, never an unbounded log.
 */
function recordConfigReloadError(root, err) {
  const now = Date.now();
  const prev = readJsonSafe(paths(root).configWarning);
  const firstAt = prev && prev.message === err.message ? prev.firstAt : now;
  if (err.message !== lastConfigErrorMessage) {
    lastConfigErrorMessage = err.message;
    process.stderr.write(`lane-broker supervisor: config reload failed, still using last known-good config: ${err.message}\n`);
  }
  try {
    atomicWriteJson(paths(root).configWarning, { message: err.message, firstAt, lastAt: now });
  } catch {
    // best-effort; a failure here must never affect scheduling
  }
}

/** Clear a previously recorded config warning once a reload succeeds again. */
function clearConfigReloadWarning(root) {
  if (lastConfigErrorMessage === null) return; // nothing was ever recorded this run
  lastConfigErrorMessage = null;
  try {
    fs.unlinkSync(paths(root).configWarning);
  } catch {
    // already gone
  }
}

function cancelRequested(root, id) {
  return isCancelled(root, id);
}

function clearTicketMarkers(root, id) {
  discardRebinding(root, id);
  for (const marker of [cancelMarkerPath(root, id), expireMarkerPath(root, id), withdrawMarkerPath(root, id)]) {
    try {
      fs.unlinkSync(marker);
    } catch {
      // none pending
    }
  }
}

/** TERM the group, wait a grace period, KILL, verify gone. Returns once the group is confirmed dead. */
async function killGroup(pgid) {
  if (!pgid) return;
  try {
    process.kill(-pgid, 'SIGTERM');
  } catch {
    return; // already gone
  }
  const deadline = Date.now() + CANCEL_GRACE_MS;
  while (Date.now() < deadline) {
    if (!isGroupAlive(pgid)) return;
    await sleep(100);
  }
  try {
    process.kill(-pgid, 'SIGKILL');
  } catch {
    return;
  }
  while (isGroupAlive(pgid)) {
    await sleep(50);
  }
}

class CappedLogWriter {
  constructor(logPath) {
    let existingBytes = 0;
    try {
      existingBytes = fs.statSync(logPath).size;
    } catch {
      // new file
    }
    // O_NOFOLLOW refuses to open through a symlink; existing bytes count
    // toward the cap so a pre-existing (or re-opened) log can't bypass it.
    const flags = fs.constants.O_CREAT | fs.constants.O_WRONLY | fs.constants.O_APPEND | (fs.constants.O_NOFOLLOW || 0);
    this.failed = false;
    this.pausedSources = new Set();
    this.stream = fs.createWriteStream(logPath, { flags });
    this.stream.on('error', (err) => {
      // Disk-full, a symlink refusal, or any other stream failure must never
      // crash the supervisor or the lane it is running. Switch to discard
      // mode permanently and resume anything paused waiting on this stream's
      // 'drain' — that event will never come once the stream has errored, so
      // without this the child's stdout/stderr pipes stay paused forever and
      // the run hangs instead of failing.
      this.failed = true;
      this.error = err;
      for (const src of this.pausedSources) src.resume();
      this.pausedSources.clear();
    });
    this.bytes = existingBytes;
    this.truncated = existingBytes >= LOG_CAP_BYTES;
  }

  /** Returns false when the caller should pause its source until 'drain' fires. */
  write(chunk) {
    if (this.truncated || this.failed) return true;
    if (this.bytes + chunk.length > LOG_CAP_BYTES) {
      const remaining = LOG_CAP_BYTES - this.bytes;
      if (remaining > 0) this.stream.write(chunk.subarray(0, remaining));
      this.stream.write('\n[lane-broker] log truncated at 50MB\n');
      this.truncated = true;
      return true;
    }
    this.bytes += chunk.length;
    return this.stream.write(chunk);
  }

  /** Pause `source` until this writer's stream drains. If the writer fails
   *  while `source` is paused, the error handler above resumes it — a
   *  stream error never fires 'drain', so without that, `source` would stay
   *  paused forever. */
  pauseUntilDrain(source) {
    if (this.failed) return;
    source.pause();
    this.pausedSources.add(source);
    this.stream.once('drain', () => {
      this.pausedSources.delete(source);
      source.resume();
    });
  }

  /** Await the stream fully flushing before the process exits, so the log's tail is never lost. */
  finish() {
    return new Promise((resolve) => {
      if (this.failed) {
        resolve();
        return;
      }
      this.stream.end(resolve);
    });
  }
}

/**
 * Tees child output to the supervisor's OWN stdout/stderr, so a foreground
 * `lane run` caller sees it live alongside the `--log` file (BRAIN-308).
 * Same write()/pauseUntilDrain() contract as CappedLogWriter so both share
 * one gated source (see backpressureGate below) without either knowing the
 * other exists -- this class never touches the log or the cap, only the
 * pipe back to the caller.
 *
 * The caller can vanish out from under this pipe (SIGKILL): its read end
 * closes, and the next write here gets an async EPIPE 'error' (macOS pipes
 * are async). Forwarding must then stop permanently without taking the lane
 * down -- the child keeps running, logging, and finishing normally either
 * way. Unlike CappedLogWriter, this stream (the supervisor's own
 * process.stdout/stderr) is never ours to close, so there is no `finish()`
 * here -- see `drain()`, called instead at exit.
 */
class ForwardWriter {
  constructor(stream) {
    this.stream = stream;
    this.failed = false;
    this.pausedSources = new Set();
    stream.on('error', () => {
      this.failed = true;
      for (const src of this.pausedSources) src.resume();
      this.pausedSources.clear();
    });
  }

  write(chunk) {
    if (this.failed) return true;
    return this.stream.write(chunk);
  }

  pauseUntilDrain(source) {
    if (this.failed) return;
    source.pause();
    this.pausedSources.add(source);
    this.stream.once('drain', () => {
      this.pausedSources.delete(source);
      source.resume();
    });
  }

  /** Await pending writes actually reaching the OS pipe before process.exit()
   *  -- on macOS a pipe write is async, and process.exit() can otherwise drop
   *  its tail. Bounded so a caller that stopped reading (not erroring) can't
   *  wedge the lease release forever. */
  drain(timeoutMs = 2000) {
    if (this.failed || this.stream.writableLength === 0) return Promise.resolve();
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, timeoutMs);
      this.stream.once('drain', () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
}

/**
 * The log writer and a ForwardWriter both call pauseUntilDrain(source) on
 * the same child.stdout/child.stderr independently, each unaware of the
 * other. Handed this gate instead of the raw stream, they still just call
 * source.pause()/source.resume() -- but the gate only actually pauses the
 * real stream on the first blocker and only resumes it once every blocker
 * has drained (or failed), so neither writer's backpressure gets silently
 * dropped by the other draining first.
 */
function backpressureGate(source) {
  let blockers = 0;
  return {
    pause() {
      blockers += 1;
      if (blockers === 1) source.pause();
    },
    resume() {
      blockers = Math.max(0, blockers - 1);
      if (blockers === 0) source.resume();
    },
  };
}

/**
 * Pure: resolve a ticket's `cmd` into the actual [cmd, args] a lane's child
 * should be spawned with (BRAIN-207). `nice -n <n> cmd args...` execs `cmd`
 * in place (it doesn't fork+wait), so the spawned process's pid/pgid, exit
 * code, and signal propagation are exactly what a bare spawn of `cmd` would
 * give — the wrapper is a no-op past the initial exec. `nice: 0` (or a
 * missing/invalid `ticket.nice`) spawns the bare command with no wrapper at
 * all, so a niceless lane pays nothing extra. Split out as its own function
 * so the wrapping decision is unit-testable without spawning a real process.
 */
export function resolveNicedSpawn(ticket) {
  const nice = Number.isInteger(ticket.nice) && ticket.nice >= 0 ? ticket.nice : 0;
  if (nice > 0) return ['/usr/bin/nice', ['-n', String(nice), ...ticket.cmd]];
  return [ticket.cmd[0], ticket.cmd.slice(1)];
}

/**
 * Pure: fold one heartbeat's observed-CPU reading into a lease update
 * (BRAIN-207). A finite `observed` stamps observedCpuCores + observedAt on
 * the SAME write as the heartbeat; a null/non-finite reading (probe failed,
 * or returned nothing usable) leaves whatever was there before untouched —
 * a failed probe must never clobber a good prior observation with
 * "unknown". Split out so both branches are unit-testable without a real
 * process group.
 */
/**
 * The lane child's environment. Besides the lease identity it carries the
 * lane's resource grant, so a test runner inside the lane can size its worker
 * pool to what the broker reserved (BRAIN-318): an unsized vitest spawns
 * cores-1 workers and was measured averaging 4.2 cores on a 2-core lease.
 */
export function childEnv(ticket, baseEnv = process.env, grantedCpuCores = ticket.resources?.cpuCores, classEnforcement = null) {
  const env = { ...baseEnv, LANE_BROKER_LEASE: ticket.id, LANE_BROKER_KEY: ticket.key };
  const cpuCores = grantedCpuCores;
  const memoryBytes = ticket.resources?.memoryBytes;
  if (Number.isFinite(cpuCores) && cpuCores > 0) env.LANE_BROKER_CPU_CORES = String(cpuCores);
  else delete env.LANE_BROKER_CPU_CORES;
  if (Number.isFinite(memoryBytes) && memoryBytes > 0) env.LANE_BROKER_MEMORY_BYTES = String(memoryBytes);
  else delete env.LANE_BROKER_MEMORY_BYTES;
  // BRAIN-380: nested `lane run` calls inherit the tier this ticket was ADMITTED at, never the caller's own shell value.
  if (isPriorityTier(ticket.priorityAdmitted)) env.LANE_BROKER_PRIORITY = ticket.priorityAdmitted;
  else delete env.LANE_BROKER_PRIORITY;
  // BRAIN-321: a sim lease granted under live caps says so; any other child never inherits the claim.
  if (classEnforcement) env[CLASS_ENFORCEMENT_ENV] = JSON.stringify(classEnforcement);
  else delete env[CLASS_ENFORCEMENT_ENV];
  return env;
}

export function applyHeartbeatObservation(lease, observed, now = Date.now(), observedMemoryBytes = null, maxGapMs) {
  const update = { ...lease, heartbeatAt: now };
  if (Number.isFinite(observed)) {
    update.observedCpuCores = observed;
    update.observedAt = now;
    // BRAIN-354: bounded trailing history for admission's settled-demand peak.
    update.observedCpuHistory = [...(Array.isArray(lease.observedCpuHistory) ? lease.observedCpuHistory : []), { at: now, cores: observed }].slice(-OBSERVED_HISTORY_MAX);
  }
  if (Number.isFinite(observedMemoryBytes)) {
    update.observedMemoryBytes = observedMemoryBytes;
    update.observedAt = now;
  }
  // BRAIN-361: report-only usage telemetry, isolated so it can never cost the lease its heartbeat.
  const { fields, drop } = observedLeaseFields(lease, observed, observedMemoryBytes, now, maxGapMs);
  for (const key of drop) delete update[key];
  Object.assign(update, fields);
  return update;
}

/** BRAIN-425: the first CPU sample is taken this long after spawn (capped at sampleMs), so a short run is still observed. */
const EARLY_SAMPLE_MS = 1000;

/** BRAIN-361: a runner's usage fields, each kept only when well-formed; a malformed one is dropped, never thrown on. */
function relayedUsage(result) {
  const observedCpu = sanitizeObservedCpu(result?.observedCpu);
  const rssPeak = sanitizeRssPeak(result?.observedRssPeakBytes);
  const cpuSeconds = sanitizeCpuSeconds(result?.cpuSeconds);
  return {
    ...(observedCpu ? { observedCpu } : {}),
    ...(rssPeak !== undefined ? { observedRssPeakBytes: rssPeak } : {}),
    ...(cpuSeconds !== undefined ? { cpuSeconds } : {}),
  };
}

/**
 * BRAIN-425: `remotePhasesMs`, the remote run's phase wall times. The runner's own (`snapshotMs` receive+verify, `setupMs`
 * deps+setup, `commandMs`, `artifactsMs` collection) ride in its result; the submitter adds `uploadMs` (writing the snapshot
 * to ssh) and `artifactReturnMs` (fetching the artifacts back). Each is kept only when a finite non-negative number.
 */
const REMOTE_PHASE_KEYS = ['snapshotMs', 'setupMs', 'commandMs', 'artifactsMs'];
function relayedPhases(dispatch) {
  const own = { uploadMs: dispatch.uploadMs, artifactReturnMs: dispatch.artifactReturnMs };
  for (const key of REMOTE_PHASE_KEYS) own[key] = dispatch.result?.phasesMs?.[key];
  const phases = Object.fromEntries(Object.entries(own).filter(([, v]) => Number.isFinite(v) && v >= 0));
  return Object.keys(phases).length > 0 ? { remotePhasesMs: phases } : {};
}

/** BRAIN-389: the runner's deps-phase outcome (hit|miss|skip) and wall time, each kept only when well-formed. */
function relayedDeps(result) {
  const deps = result?.deps;
  if (!deps || !['hit', 'miss', 'skip'].includes(deps.outcome)) return {};
  return { depsCache: deps.outcome, ...(Number.isFinite(deps.ms) && deps.ms >= 0 ? { depsMs: deps.ms } : {}) };
}

/**
 * BRAIN-398: install the files a confirmed remote run returned into the submitter's worktree and say so. Every outcome
 * short of success is a warning (stderr, the admission log, the history row); the lane's exit code is never touched,
 * and this never throws. Returns the history-row fields.
 */
function returnRemoteArtifacts(root, enriched, dispatch) {
  const declared = enriched.remote.remoteArtifacts;
  const reported = dispatch.result.artifacts;
  const warnings = [];
  let names = [];
  try {
    if (dispatch.artifacts?.ok) {
      const { written, refused } = installArtifacts(enriched.remote.worktreeRoot, dispatch.artifacts.files, declared);
      names = written;
      for (const r of refused) warnings.push(`refused ${r.path}: ${r.reason}`);
    } else if (dispatch.artifacts) {
      warnings.push(`not returned: ${dispatch.artifacts.reason}`);
    } else if (reported?.ok === false) {
      warnings.push(`the runner could not collect them: ${reported.reason}`);
    }
    if (reported?.ok === true) for (const m of reported.missing ?? []) warnings.push(`declared but not produced: ${m}`);
  } catch (err) {
    warnings.push(`not returned: ${err.message}`);
  }
  const lines = [];
  if (dispatch.artifacts?.ok || reported?.ok === true) {
    lines.push(`lane: remote artifacts: ${names.length} file(s) returned${names.length ? `: ${names.join(', ')}` : ''}\n`);
  }
  for (const w of warnings) lines.push(`lane: warning: remote artifacts: ${w}\n`);
  for (const line of lines) {
    process.stderr.write(line);
    writeBrokerLog(root, line);
  }
  return {
    ...(names.length > 0 ? { remoteArtifacts: names } : {}),
    ...(warnings.length > 0 ? { remoteArtifactsWarning: warnings.join('; ') } : {}),
  };
}

/** The exact "requested resources exceed this environment's budget" (checkResourceBudget) or
 *  "this lane is refused for local runs by default" (localSimRefusal) result shape run.js
 *  applies at preflight, re-applied here once a remote-eligible ticket has fallen back to
 *  local -- run.js skipped both checks specifically so a runner could still take an oversized
 *  or local-refused request (BRAIN-319 T3b-1, BRAIN-320). Shared by both refusal reasons since
 *  they carry the same {exitCode, message} shape. */
function localRefusalResult(ticket, refusal) {
  return { id: ticket.id, exit: refusal.exitCode, signal: null, startedAt: null, endedAt: Date.now(), waitedMs: 0, error: refusal.message.trim() };
}

/**
 * BRAIN-347: the ONE history-row builder every supervisor terminal path goes
 * through. `executor` defaults to 'local' (a remote result overrides it via
 * the `result` spread); `localReason` is the ticket's creation-time reason, or
 * `fallback:<reason>` when a remote attempt fell back to local.
 */
function historyRow(ticket, result, { fallbackReason, lease } = {}) {
  const executor = result.executor ?? 'local';
  const localReason = executor === 'local' ? (fallbackReason === undefined ? ticket.localReason : `fallback:${fallbackReason}`) : undefined;
  return {
    id: ticket.id,
    key: ticket.key,
    repo: ticket.repoId,
    lane: ticket.lane,
    ...(ticket.configLane ? { configLane: ticket.configLane } : {}),
    ...(ticket.localFirst ? { localFirst: true } : {}),
    ...(ticket.exclusive === true ? { exclusive: true } : {}),
    weight: ticket.weight,
    resources: ticket.resources,
    command: ticket.command,
    ...(ticket.cmdFingerprint ? { cmdFingerprint: ticket.cmdFingerprint } : {}),
    headTree: ticket.headTree,
    // BRAIN-380 §8: only a ticket that was admitted has a lease, so only its row carries the admission audit
    ...priorityAuditOf(lease),
    // BRAIN-425: every row states what the lane was granted and how it ended; a grant or exit in `result` wins
    ...terminalRowDefaults({ resources: ticket.resources, grantedCpuCores: lease?.grantedCpuCores }),
    ...result,
    executor,
    ...(localReason ? { localReason } : {}),
  };
}

/** BRAIN-380: what a supervisor publishes when `lane migrate-scheduler` refuses its admission; `lane run` returns the 75. */
function migrationRefusalResult(ticket) {
  return { id: ticket.id, exit: 75, signal: null, startedAt: null, endedAt: Date.now(), waitedMs: null, cancelled: false, reason: 'scheduler-migration' };
}

/** Why this machine can never run the ticket (a refusal result), or null: a `localRefused` lane without --allow-local-sim,
 *  else a claim over the local budget. The one definition behind local-first, the queued-runner choice, the fallback and
 *  whether the ticket carries a queue timeout (which exists only to enable a local fallback). */
function localIneligibilityOf(enriched, globalCfg) {
  if (enriched.localRefused && !enriched.allowLocalSim) return localSimRefusal(enriched.lane, enriched.class);
  const budget = checkResourceBudget({ resources: enriched.resources, globalCfg, host: detectResourceCapacity() });
  return budget.ok ? null : budget;
}

/** The client's `remoteQueueTimeoutMs` for this ticket: none when it has no local fallback to expire into (BRAIN-509). */
function queueTimeoutFor(enriched, globalCfg) {
  return localIneligibilityOf(enriched, globalCfg) ? undefined : globalCfg.remoteQueueTimeoutMs;
}

/** What `selectRunner` needs to judge a ticket: its protocol, and its reservation (shared by a first attempt and a rebind). */
export function remoteSelectOptions(enriched, globalCfg) {
  return {
    maxRemoteQueue: globalCfg.maxRemoteQueue,
    requireProtocol2: needsProtocol2(enriched.remote) || Boolean(queueTimeoutFor(enriched, globalCfg)),
    requireCapabilities: isEnforcementRequired(enriched) ? [CLASSES_CAPABILITY] : [],
    reservation: {
      weight: enriched.weight,
      cpuCores: enriched.resources.cpuCores,
      minCpuCores: enriched.resources.minCpuCores,
      memoryBytes: enriched.resources.memoryBytes,
      niceOverride: enriched.niceOverride,
    },
  };
}

/**
 * Attempt a remote runner for a remote-eligible ticket (`ticket.remote`,
 * BRAIN-319 T3b-1's payload), BEFORE the ticket is ever handed to the local
 * scheduler. Resolves to `{ fallback: true, attemptGeneration, fallbackReason }`
 * when the caller must continue into the EXISTING local enqueue+run path
 * below with the same ticket id, or `{ fallback: false }` once the attempt
 * has reached a terminal outcome here (confirmed, cancelled, or refused) --
 * `process.exit()` has already been called on that path, matching every
 * other terminal path in this file.
 *
 * BRAIN-405: `rebind` (`{ generation, runner, probe }`) runs the same attempt for a ticket that already fell back and sits in
 * the local queue: the caller has chosen the runner, this withdraws the ticket from the queue (only if it is still queued)
 * right before dispatch, and any failure after that puts it back at its original queue position and resolves
 * `{ fallback: true, rebindFailed: true }` -- a rebind never terminates a ticket by refusing it.
 */
async function runRemoteAttempt(root, enriched, globalCfg, abortSignal, rebind = null) {
  const attemptStartedAt = Date.now();
  // the attempt generation every fence below acts on: 0 for a first attempt, the fallback's generation for a rebind
  const gen = rebind ? rebind.generation : 0;
  let withdrawn = null;
  if (!rebind) {
    // BRAIN-321: a retried `--id` submission is the same ticket: dedup BEFORE any probe or dispatch, under the attempt's lock.
    const created = await createAttempt(root, enriched.id, { runner: null, resources: enriched.resources, logPath: enriched.logPath, cwd: enriched.cwd, ...(enriched.clientId ? { refuseIf: () => existingTicket(root, enriched.id) } : {}) });
    if (created.existing) process.exit(0);
  }

  // Reuses the SAME two writers a local child's output goes through
  // (CappedLogWriter/ForwardWriter, defined above) -- no second relay.
  // Backpressure is not applied here (unlike the local child path below):
  // `dispatchRemote` hands us plain data chunks, not a pausable stream, so
  // there is no source on our side to gate.
  const logWriter = new CappedLogWriter(enriched.logPath);
  const forwardOutput = enriched.forwardOutput === true;
  const stdoutForward = forwardOutput ? new ForwardWriter(process.stdout) : null;
  const stderrForward = forwardOutput ? new ForwardWriter(process.stderr) : null;
  const onStdout = (chunk) => {
    logWriter.write(chunk);
    if (stdoutForward) stdoutForward.write(chunk);
  };
  const onStderr = (chunk) => {
    logWriter.write(chunk);
    if (stderrForward) stderrForward.write(chunk);
  };

  let selectedRunner = null;
  function writeResultSync(result) {
    atomicWriteJson(enriched.resultPath, result);
    appendHistory(root, historyRow(enriched, { ...result, ...(selectedRunner ? { runner: selectedRunner } : {}) }));
  }

  /**
   * Codex pre-merge BLOCKER #2: drain FIRST (async, outside any lock), THEN
   * publish inside ONE synchronous `publishTerminal` transaction. The old
   * shape called an async `finish()` from INSIDE `resultWriterFn` without
   * awaiting it -- `publishTerminal` removed the attempt and released the
   * mutex immediately, before the drain (or even the `atomicWriteJson`) had
   * actually happened, so a SIGINT or crash in that window left neither an
   * attempt record nor a result.json. `buildConfirmedResult` is called only
   * when `publishTerminal` itself decides this was NOT a cancellation
   * (its `isCancelled` check, the single source of truth here) -- it must
   * never assume that outcome on its own.
   */
  async function publishAndExit(generation, buildConfirmedResult) {
    await logWriter.finish();
    if (stdoutForward) await stdoutForward.drain();
    if (stderrForward) await stderrForward.drain();

    let finalResult = null;
    const published = await publishTerminal(root, enriched.id, generation, ({ cancelled }) => {
      finalResult = cancelled ? remoteCancelledResult(enriched.id, attemptStartedAt) : buildConfirmedResult();
      writeResultSync(finalResult);
    });
    if (!published.ok) {
      // Generation mismatch: unreachable on every call site below (nothing
      // else advances this attempt's generation while this supervisor is
      // its sole writer), but a real terminal outcome must never be
      // silently dropped -- fail loud rather than hang or exit 0.
      process.stderr.write(`lane-broker supervisor: could not publish a result for ${enriched.id}\n`);
      process.exit(1);
      return;
    }
    // Cleanup only, AFTER the terminal write above -- never before (BLOCKER #1).
    clearTicketMarkers(root, enriched.id);
    process.exit(Number.isInteger(finalResult.exit) ? finalResult.exit : 1);
  }

  /**
   * A rebind that did not take. Only an outcome that PROVES the runner never began the job (`neverStarted`, or a confirmed
   * preflight refusal) puts the ticket back in the local queue, at its original seq. Anything else after the dispatch may have
   * begun leaves a remote attempt (it keeps its runner and its `running` phase) for the usual reconciliation -- `lane wait`
   * names it ORPHANED-REMOTE and `lane cancel` cancels it on the runner -- because running it locally too could execute it twice.
   */
  async function abandonRebind(runner, reason, { neverStarted = false } = {}) {
    const where = runner ? runner.name : 'no runner';
    if (withdrawn && !neverStarted) {
      const line = `lane: remote-rebind: ${enriched.id}: ${where}: ${reason} — outcome unknown, the job may be running there; not re-running it locally (see lane status, lane cancel)\n`;
      process.stderr.write(line);
      writeBrokerLog(root, line);
      await updateAttempt(root, enriched.id, gen, { unresolved: reason });
      await logWriter.finish();
      process.exit(1);
      return { fallback: false };
    }
    if (withdrawn) {
      await updateAttempt(root, enriched.id, gen, { executor: 'local', phase: 'queued', runner: null });
      await restoreQueued(root, withdrawn);
    }
    const line = `lane: remote-rebind: ${enriched.id}: ${where}: ${reason} — stays in the local queue${withdrawn ? ` at seq ${withdrawn.seq}` : ''}\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
    await logWriter.finish();
    return { fallback: true, rebindFailed: true, attemptGeneration: gen };
  }

  const localIneligibility = () => localIneligibilityOf(enriched, globalCfg);

  /** `rebindable`: this fallback is only "no runner had room right now", so a later probe may still find one.
   *  `noRunner`: the fallback is "the fleet had no usable runner", which a `localRefused` lane reports as retryable. */
  async function fallbackOrRefuse(runner, reason, { rebindable = false, neverStarted = false, noRunner = false } = {}) {
    if (rebind) return abandonRebind(runner, reason, { neverStarted: neverStarted || withdrawn === null });
    const fb = await fallbackToLocal(root, enriched.id, reason);
    if (!fb.ok) {
      if (fb.cancelled) {
        await publishAndExit(0, () => {
          throw new Error('unreachable: fallbackToLocal reported cancelled');
        });
        return { fallback: false };
      }
      throw new Error(`lane-broker supervisor: could not fall back to local for ${enriched.id} (attempt record missing)`);
    }

    const refusedHere = enriched.localRefused && !enriched.allowLocalSim;
    const skipLine = `lane: remote-skip: ${runner ? runner.name : 'none'}: ${reason} — ${refusedHere ? 'not running locally (localRefused)' : 'running locally'}\n`;
    process.stderr.write(skipLine);
    writeBrokerLog(root, skipLine);

    // BRAIN-320: a `localRefused` lane (rouge's `sim`) that was remote-
    // eligible reaches here only once the remote attempt itself has fallen
    // back to local -- run.js's own immediate refusal was skipped for
    // exactly this lane/eligibility combination, so it must be re-applied
    // now, before the resource-budget check below (mirroring run.js's own
    // ordering: local-sim refusal, then budget).
    if (refusedHere) {
      const refusal = noRunner ? noRunnerRefusal(enriched.lane, reason) : localSimRefusal(enriched.lane, enriched.class);
      process.stderr.write(refusal.message);
      await publishAndExit(fb.attempt.generation, () => localRefusalResult(enriched, refusal));
      return { fallback: false };
    }

    const host = detectResourceCapacity();
    const budget = checkResourceBudget({ resources: enriched.resources, globalCfg, host });
    if (!budget.ok) {
      process.stderr.write(budget.message);
      await publishAndExit(fb.attempt.generation, () => localRefusalResult(enriched, budget));
      return { fallback: false };
    }

    return { fallback: true, attemptGeneration: fb.attempt.generation, fallbackReason: reason, rebindable };
  }

  // BRAIN-442: a local-first ticket skips the initial dispatch and queues locally; only `tryRebind` may move it, after its wait.
  // A ticket this machine would refuse (local-refused lane, over the local budget) can never run here, so it ignores the policy.
  if (enriched.localFirst && !rebind) {
    const localRefusal = localIneligibility();
    if (localRefusal) {
      enriched.localFirst = false;
      const line = `lane: local-first: not locally eligible (${localRefusal.message.trim()}); dispatching remote\n`;
      process.stderr.write(line);
      writeBrokerLog(root, line);
    } else {
      return fallbackOrRefuse(null, 'local-first', { rebindable: true });
    }
  }

  // Codex pre-merge finding #5: decide ELIGIBILITY before ever probing a
  // runner. `buildManifest` used to run only inside `dispatchRemote`, AFTER
  // `selectRunner` had already dialed a runner and this function had
  // already printed "running on <runner>" -- an ineligible tree (e.g. a
  // tracked .env) then fell back only after a real probe and a misleading
  // banner. Built once, here; handed to `dispatchRemote` as `manifest` below
  // so it is never rebuilt (one code path, see dispatchRemote's own doc
  // comment).
  let manifest;
  try {
    manifest = buildManifest(enriched.remote.worktreeRoot, { omitEscapingSymlinks: enriched.remote.remoteOmitEscapingSymlinks === true });
  } catch (err) {
    if (!(err instanceof RemoteIneligibleError)) throw err;
    return fallbackOrRefuse(null, err.message);
  }
  const omittedSymlinks = manifest.omittedSymlinks ?? [];
  if (omittedSymlinks.length > 0) {
    const shown = omittedSymlinks.slice(0, MAX_OMITTED_SYMLINKS_LISTED);
    const more = omittedSymlinks.length > shown.length ? `, +${omittedSymlinks.length - shown.length} more` : '';
    const line = `lane: remote snapshot omits ${omittedSymlinks.length} symlink(s) pointing outside the repo: ${shown.join(', ')}${more}\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
  }

  // BRAIN-320 S1a: client-side remoteDeps eligibility (1c), run right
  // alongside the manifest-build eligibility check above and before any
  // runner is ever probed -- same reasoning as the Codex finding #5 comment
  // above this block.
  if (Array.isArray(enriched.remote.remoteDeps) && enriched.remote.remoteDeps.length > 0) {
    const depsCheck = validateRemoteDeps(manifest, enriched.remote.remoteDeps);
    if (!depsCheck.ok) {
      return fallbackOrRefuse(null, depsCheck.reason);
    }
  }

  // BRAIN-320 review fix D: runner SELECTION requires protocol 2 whenever
  // the lane itself needs it OR the client has a queue timeout configured
  // (only a 0.7.0+ runner honours `queueTimeoutMs`; a locally-ineligible ticket carries none, BRAIN-509) -- but the exec header's
  // own protocol (inside dispatchRemote) still derives from
  // `needsProtocol2(enriched.remote)` alone, so an optionless lane still
  // sends a protocol-1 header even when this is true.
  let { runner, skipped, queuedChoice, probe, busy } = rebind
    ? { runner: rebind.runner, skipped: [], queuedChoice: false, probe: rebind.probe }
    : await selectRunner(globalCfg.runners || [], remoteSelectOptions(enriched, globalCfg));
  // A ticket this machine would refuse has no local queue to wait in, so with every runner busy it waits on the best one: that
  // runner's own broker holds the ticket until it has room (what every empty-queue runner did before headroom ranking).
  let busyQueuedAt;
  if (!runner && busy && !rebind && localIneligibility()) {
    ({ runner, probe } = busy);
    busyQueuedAt = `${runner.name}(0)`;
    const line = `lane: waiting on ${runner.name}: every runner is busy and this lane cannot run locally (${skipped.map((s) => `${s.name}: ${s.reason}`).join('; ')})\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
  }
  if (!runner) {
    const reason = skipped.length ? skipped.map((s) => `${s.name}: ${s.reason}`).join('; ') : 'no runners configured';
    return fallbackOrRefuse(null, reason, { rebindable: true, noRunner: true });
  }

  // BRAIN-338: every runner has a queue. Queue on the least-loaded one only
  // when this machine could not start the ticket right now either;
  // otherwise run locally, exactly as when no runner was idle. A ticket this machine would refuse queues regardless.
  let queuedAt = busyQueuedAt ?? (rebind?.queued ? `${runner.name}(${probe.queued})` : undefined);
  if (queuedChoice) {
    queuedAt = `${runner.name}(${probe.queued})`;
    const ineligible = localIneligibility();
    const local = ineligible ? null : await couldAdmitNow(root, globalCfg, enriched);
    if (local?.admit) {
      const reason = skipped.map((s) => `${s.name}: ${s.reason}`).join('; ');
      return fallbackOrRefuse(null, `${reason}; local can admit now, not queuing on ${queuedAt}`, { rebindable: true });
    }
    const line = ineligible
      ? `lane: queuing on ${queuedAt}: not locally eligible (${ineligible.message.trim()})\n`
      : `lane: queuing on ${queuedAt}: local cannot admit now (${local.reason})\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
  }

  // BRAIN-380: remote dispatch is an admission entry point. The marker check and the attempt write are ONE locked step
  // (`refuseWhileMigrating`), so a migration or drain cannot start between them; refuse before anything is sent to a runner.
  testDrainAt(root, 'dispatch');
  if (rebind) {
    // BRAIN-405: the last step before the ticket leaves the local queue. `withdrawQueued` shares tryStart's lock and its
    // dequeue, so either this takes the ticket or the scheduler already did (or it was cancelled): never both, never neither.
    withdrawn = await withdrawQueued(root, enriched.id);
    if (!withdrawn) {
      await logWriter.finish();
      return { fallback: true, rebindLost: true, attemptGeneration: gen };
    }
    await testHoldAt('rebind-withdrawn');
    const line = `lane: remote-rebind: ${enriched.id}: seq ${withdrawn.seq} -> ${runner.name} (queued locally ${Math.round((Date.now() - withdrawn.createdAt) / 1000)}s${enriched.localFirst ? ', local-first' : ''})\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
  }
  try {
    // Every runner this ticket is ever dispatched to is remembered for good (it survives fallback, restore and rebalance): a runner
    // keeps a ticket dir for this id, so dispatching to it a second time would meet the first dispatch's stale state.
    const dispatchedRunners = [...new Set([...(readAttempt(root, enriched.id)?.dispatchedRunners ?? []), runner.name])];
    await updateAttempt(root, enriched.id, gen, { phase: 'running', runner: runner.name, dispatchedRunners, ...(rebind ? { executor: 'remote' } : {}), ...(queuedAt ? { queuedAt } : {}) }, { refuseWhileMigrating: true });
  } catch (err) {
    if (!(err instanceof MigrationInProgressError)) throw err;
    process.stderr.write(`lane: ${err.message}\n`);
    await publishAndExit(gen, () => migrationRefusalResult(enriched));
    return { fallback: false };
  }
  selectedRunner = runner.name;
  // Printed only now that eligibility is settled and a usable runner is
  // about to actually be dialed for the transfer -- see finding #5 above.
  process.stderr.write(`lane: running on ${runner.name}\n`);

  const declaredArtifacts = enriched.remote.remoteArtifacts;
  let artifactsSupported = false;
  let artifactsUnsupportedWarning;

  // BRAIN-436: a ticket still QUEUED on its runner past `remoteRebalanceMinQueuedMs` is withdrawn (irreversibly, only if the
  // runner has not started it) and dispatched to a runner that has real room now. Each dispatch is an epoch: output and records
  // from an earlier one are ignored. A runner a ticket has left is never revisited.
  const rebalanceOn = globalCfg.remoteRebalanceMinQueuedMs > 0;
  // A rebind retry continues the ticket's earlier moves (they live on the attempt), so the cap and the no-revisit rule hold across it.
  const priorAttempt = rebind ? readAttempt(root, enriched.id) : null;
  const moves = Array.isArray(priorAttempt?.moves) ? [...priorAttempt.moves] : [];
  // the same durable history tryRebind excludes: a runner this ticket was dispatched to or refused by is not a rebalance target either
  const visited = [...new Set([...moves.flatMap((m) => [m.from, m.to]), ...(priorAttempt?.refusedRunners ?? []), ...(priorAttempt?.dispatchedRunners ?? []), runner.name])];
  let epoch = Number.isInteger(priorAttempt?.dispatchEpoch) ? priorAttempt.dispatchEpoch : 1;
  let priorRemoteQueuedMs = 0;
  let lastMoveAt = moves.length > 0 ? moves.at(-1).at : null;
  let movingSet = false;
  const clearMoving = async () => {
    movingSet = false;
    await updateAttempt(root, enriched.id, gen, { moving: undefined });
  };
  const dispatchedAt = Date.now(); // the FIRST dispatch: waitedMs includes the time spent queued on every runner before the last
  let dispatch;
  while (true) {
    // BRAIN-398: a runner without `artifacts/1` would run the command and silently return nothing, so say so up front.
    artifactsSupported = Boolean(declaredArtifacts) && Array.isArray(probe?.capabilities) && probe.capabilities.includes(ARTIFACTS_CAPABILITY);
    artifactsUnsupportedWarning = undefined;
    if (declaredArtifacts && !artifactsSupported) {
      artifactsUnsupportedWarning = `runner ${runner.name} does not support ${ARTIFACTS_CAPABILITY}; nothing will be returned`;
      const line = `lane: warning: remote artifacts: ${artifactsUnsupportedWarning}\n`;
      process.stderr.write(line);
      writeBrokerLog(root, line);
    }
    const thisEpoch = epoch;
    const current = (write) => (chunk) => {
      if (thisEpoch === epoch) write(chunk);
    };
    const canWithdraw = Array.isArray(probe?.capabilities) && probe.capabilities.includes(REMOTE_WITHDRAW_CAPABILITY);
    let target = null;

    // BRAIN-320 S1c: `enriched.remote.remoteDeps`/`remoteSetup` ride along in
    // this spread and are what `dispatchRemote` derives its protocol-2 exec
    // header from (see its own `needsProtocol2` call).
    dispatch = await dispatchRemote({
      ...enriched.remote,
      remoteArtifacts: artifactsSupported ? declaredArtifacts : null,
      artifactLimits: artifactLimitsOf(globalCfg),
      manifest,
      runner,
      argv: enriched.cmd,
      lane: enriched.lane,
      ticketId: enriched.id,
      generation: gen,
      // BRAIN-320 S1d: opt-in, from THIS (client) machine's own global config
      // -- never the runner's -- so an unset value here means the header
      // carries no queueTimeoutMs at all (I6). The timeout only exists to enable a local fallback, so a ticket this
      // machine can never run carries none either: it waits in the runner's queue instead of expiring into a refusal (BRAIN-509).
      queueTimeoutMs: queueTimeoutFor(enriched, globalCfg),
      noProgressTimeoutMs: enriched.noProgressTimeoutMs ?? globalCfg.noProgressTimeoutMs,
      // BRAIN-380 §6: the tier BEFORE any cap, and the wait this ticket has accrued on THIS host's priority clock. The
      // runner re-anchors from the wait, never from our timestamps, so clock skew between hosts cannot matter.
      priorityRequested: enriched.priorityRequested,
      ...classTransport(enriched, resolveClasses(globalCfg, root)),
      priorityAccruedMs: waitedMs(enriched, effectiveNow(root)),
      resultWaitMs: globalCfg.remoteResultWaitMs,
      // BRAIN-437: a runner that can withdraw lets a dropped connection prove the job never started; one that cannot leaves it possibly running
      canWithdraw,
      onStdout: current(onStdout),
      onStderr: current(onStderr),
      abortSignal,
      // The second runner holds the job once it has the snapshot: from there the move is no longer in flight.
      onSnapshotSent: moves.length > 0 && movingSet ? clearMoving : undefined,
      rebalance: rebalanceOn && canWithdraw ? {
        minQueuedMs: globalCfg.remoteRebalanceMinQueuedMs,
        intervalMs: globalCfg.remoteRebalanceIntervalMs,
        pickTarget: async () => {
          if (rebalanceBlocked({ moves: moves.length, maxMoves: globalCfg.remoteRebalanceMaxMoves, lastMoveAt, now: Date.now(), cooldownMs: globalCfg.remoteRebalanceCooldownMs })) return null;
          const candidates = (globalCfg.runners || []).filter((r) => !visited.includes(r.name) && r.rebalanceTarget !== false);
          if (candidates.length === 0) return null;
          const picked = await selectRunner(candidates, { ...remoteSelectOptions(enriched, globalCfg), maxRemoteQueue: 0 });
          target = picked.runner && hasMeasuredHeadroom(picked.probe) ? { runner: picked.runner, probe: picked.probe } : null;
          return target;
        },
        // The worktree must still be what was snapshotted, then the intent is made durable BEFORE the irreversible withdraw.
        beforeWithdraw: async ({ from, to }) => {
          let unchanged = false;
          try {
            unchanged = buildManifest(enriched.remote.worktreeRoot, { omitEscapingSymlinks: enriched.remote.remoteOmitEscapingSymlinks === true }).manifestHash === manifest.manifestHash;
          } catch {
            // ineligible now: it changed
          }
          if (!unchanged) return false;
          const written = await updateAttempt(root, enriched.id, gen, { moving: { from: from.name, to: to.runner.name, phase: 'withdrawing', at: Date.now() } });
          movingSet = written.ok;
          return written.ok;
        },
        // A withdraw that was refused (the ticket started, was cancelled, ...) ends the move; an unknown reply may have taken effect, so `moving` stays.
        afterWithdraw: async (action) => {
          if (action !== 'unknown' && movingSet) await clearMoving();
        },
      } : null,
    });
    if (dispatch.outcome !== 'moved') break;

    const from = runner;
    const to = dispatch.to;
    epoch += 1;
    visited.push(to.runner.name);
    lastMoveAt = Date.now();
    priorRemoteQueuedMs += dispatch.waitedOnRunnerMs;
    const move = { from: from.name, to: to.runner.name, at: lastMoveAt, queuedMs: dispatch.queuedMs, reason: 'queued-too-long' };
    moves.push(move);
    runner = to.runner;
    probe = to.probe;
    selectedRunner = runner.name;
    try {
      await updateAttempt(
        root, enriched.id, gen,
        { runner: runner.name, dispatchedRunners: [...new Set([...(readAttempt(root, enriched.id)?.dispatchedRunners ?? []), runner.name])], dispatchEpoch: epoch, moves: [...moves], moving: { from: from.name, to: runner.name, phase: 'dispatching', at: lastMoveAt } },
        { refuseWhileMigrating: true },
      );
      movingSet = true;
    } catch (err) {
      if (!(err instanceof MigrationInProgressError)) throw err;
      process.stderr.write(`lane: ${err.message}\n`);
      await publishAndExit(gen, () => migrationRefusalResult(enriched));
      return { fallback: false };
    }
    const line = `lane: remote-rebalance: ${enriched.id}: ${from.name} -> ${runner.name} (queued ${Math.round(dispatch.queuedMs / 1000)}s on ${from.name}; ${runner.name} ${probe.headroom.cpuCores} cores free)\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
    process.stderr.write(`lane: running on ${runner.name}\n`);
  }
  // A move that never completed (or one whose second dispatch ended before a snapshot went out) must not outlive this attempt's record.
  if (movingSet && !dispatch.mayStillBeRunning) await clearMoving();
  const rebalanceFields = () => (moves.length ? { rebalancedFrom: moves.at(-1).from, rebalancedAt: moves.at(-1).at, rebalanceReason: `queued ${Math.round(moves.at(-1).queuedMs / 1000)}s on ${moves.at(-1).from}`, moves: moves.length } : {});

  // BRAIN-405: a confirmed preflight refusal means the runner never started the job, so a rebound ticket goes back to its place
  if (rebind && dispatch.outcome === 'confirmed' && dispatch.result.kind === 'refused') {
    // BRAIN-321: a runner that refused this ticket (a class mismatch among other preflight refusals) is never picked for it again,
    // durably on the attempt, or the rebind loop would re-dispatch to it forever.
    const refusedRunners = [...new Set([...(readAttempt(root, enriched.id)?.refusedRunners ?? []), runner.name])];
    await updateAttempt(root, enriched.id, gen, { refusedRunners });
    return abandonRebind(runner, `runner refused the job (exit ${dispatch.exitCode})`, { neverStarted: true });
  }

  if (dispatch.outcome === 'confirmed') {
    const endedAt = Date.now();
    // BRAIN-320 S1c (1b/1g): a deps/setup failure is a TERMINAL red -- it is
    // already 'confirmed' here (never 'unconfirmed'), so it can never reach
    // `fallbackOrRefuse` below; falling back could turn this red into a
    // green against stale local deps. Attribute the phase to the human
    // (stderr) and to the durable records (attempt + terminal result/
    // history) before publishing.
    if (dispatch.phase === 'deps' || dispatch.phase === 'setup') {
      const phaseLine = `lane: remote failed during ${dispatch.phase} (exit ${dispatch.exitCode}); this can also be a registry or network failure on ${runner.name}\n`;
      process.stderr.write(phaseLine);
      writeBrokerLog(root, phaseLine);
    }
    // BRAIN-431: the runner killed the command for making no progress; say so, not just "exit 124"
    const noProgress = dispatch.result.reason === NO_PROGRESS_REASON && Number.isFinite(dispatch.result.noProgressTimeoutMs);
    if (noProgress) {
      const message = noProgressMessage(dispatch.result.noProgressTimeoutMs);
      process.stderr.write(message);
      writeBrokerLog(root, message);
    }
    await updateAttempt(root, enriched.id, gen, { remotePhase: dispatch.phase ?? null });
    const artifactFields = artifactsSupported
      ? returnRemoteArtifacts(root, enriched, dispatch)
      : artifactsUnsupportedWarning
        ? { remoteArtifactsWarning: artifactsUnsupportedWarning }
        : {};
    // BRAIN-341/363: queue wait = everything before the command started, never the run and never the time the result
    // took to reach us (poll backoff, an ssh-drop recovery). The runner reports its own pre-start wait (`queuedMs`, a
    // duration on its clock); we add the local portion we measured ourselves (enqueue -> handed to the runner), so no
    // cross-host clock is compared. An older runner reports no `queuedMs`: fall back to the BRAIN-341 formula
    // (receipt time - enqueue - runMs), which also counts the result-delivery delay as wait. Known underestimate: the
    // remote-exec protocol sends the client no early signal, so the ssh connect and the runner process's startup (seconds)
    // fall between `dispatchedAt` and the runner's own clock start and are counted as neither wait nor run.
    const { queuedMs, runMs } = dispatch.result;
    let remoteWaitedMs = null;
    if (Number.isFinite(queuedMs) && queuedMs >= 0) {
      remoteWaitedMs = Math.max(0, dispatchedAt - enriched.createdAt) + priorRemoteQueuedMs + queuedMs;
    } else if (Number.isFinite(runMs) && runMs >= 0) {
      remoteWaitedMs = Math.max(0, endedAt - enriched.createdAt - runMs);
    }
    await publishAndExit(gen, () => ({
      id: enriched.id,
      exit: dispatch.exitCode,
      signal: null,
      executor: 'remote',
      runner: runner.name,
      ...(queuedAt ? { queuedAt } : {}),
      ...(Number.isFinite(dispatch.result.grantedCpuCores) ? { grantedCpuCores: dispatch.result.grantedCpuCores } : {}),
      ...relayedUsage(dispatch.result),
      ...relayedDeps(dispatch.result),
      ...relayedPhases(dispatch),
      ...rebalanceFields(),
      ...artifactFields,
      ...(omittedSymlinks.length > 0 ? { remoteOmittedSymlinks: omittedSymlinks.length, remoteOmittedSymlinkNames: omittedSymlinks.slice(0, MAX_OMITTED_SYMLINKS_LISTED) } : {}),
      ...(noProgress ? { reason: NO_PROGRESS_REASON, noProgressTimeoutMs: dispatch.result.noProgressTimeoutMs } : {}),
      remoteKind: dispatch.result.kind,
      remotePhase: dispatch.phase ?? null,
      startedAt: remoteWaitedMs === null ? attemptStartedAt : enriched.createdAt + remoteWaitedMs,
      // the runner-measured run duration: endedAt is the local receipt time, so `endedAt - startedAt` also holds the delivery delay
      ...(Number.isFinite(runMs) && runMs >= 0 ? { runMs } : {}),
      endedAt,
      waitedMs: remoteWaitedMs,
    }));
    return { fallback: false };
  }

  if (dispatch.outcome === 'cancelled' && dispatch.remoteCancelConfirmed === false) {
    // The runner did not confirm the cancel, so the command may still be alive: the attempt stays (unresolved, reported), and
    // `lane cancel` fails until a cancel is confirmed -- it is not told the ticket is gone.
    const reason = `remote cancel on ${runner.name} was not confirmed; the command may still be running`;
    const line = `lane: remote: ${enriched.id}: ${reason} (see lane status, lane cancel)\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
    await updateAttempt(root, enriched.id, gen, { unresolved: reason });
    await logWriter.finish();
    process.exit(1);
    return { fallback: false };
  }

  if (dispatch.outcome === 'cancelled') {
    await publishAndExit(gen, () => {
      throw new Error('unreachable: dispatchRemote reported cancelled');
    });
    return { fallback: false };
  }

  // BRAIN-363: the runner's copy may still be running (its cancel was not confirmed). Running it locally too could execute
  // the job twice, so this ticket ends here as a failure instead of falling back; a rebind is refused the same way by abandonRebind.
  // The attempt is KEPT, marked `unresolved`: the possibly-live ticket stays visible (`lane status`/`wait` report it ORPHANED-REMOTE,
  // or MOVE-INTERRUPTED while a move is recorded) and `lane cancel` can still reach it on the runner.
  if (dispatch.mayStillBeRunning && !rebind) {
    const line = `lane: remote: ${enriched.id}: ${runner.name}: ${dispatch.reason} (see lane status, lane cancel)\n`;
    process.stderr.write(line);
    writeBrokerLog(root, line);
    await updateAttempt(root, enriched.id, gen, { unresolved: dispatch.reason });
    await logWriter.finish();
    process.exit(1);
    return { fallback: false };
  }

  // 'ineligible' or 'unconfirmed'
  // Only a runner that provably never started it AND says nothing about other runners (a queue expiry or withdrawal, an incomplete
  // transfer) leaves the fallback rebindable; ineligible, rejected and possibly-run outcomes stay local for good.
  return fallbackOrRefuse(runner, dispatch.reason, { neverStarted: dispatch.neverStarted === true, rebindable: dispatch.outcome === 'unconfirmed' && dispatch.neverStarted === true && dispatch.retryElsewhere === true });
}

async function main() {
  const ticket = readTicketFromEnv();
  const root = ensureStateDirs().root;
  // reloadGlobalConfig(undefined), not loadGlobalConfig(): a supervisor that
  // launches while config.json is missing, mid-write, or invalid must start
  // on defaults rather than crash before it ever reaches the resilient
  // polling loop below — the exact torn-read window this fix exists for.
  let globalCfg = reloadGlobalConfig(undefined, { onError: (err) => recordConfigReloadError(root, err) });
  // BRAIN-438: not awaited, so it never delays the first admission; at most once a day per broker (see maybeDailyGc).
  void maybeDailyGc(root, globalCfg);
  const supervisorStart = processStartTime(process.pid);
  const enriched = {
    ...ticket,
    supervisorPid: process.pid,
    supervisorStart: supervisorStart === undefined ? null : supervisorStart,
    bootId: bootId(),
  };

  let cancelledBeforeStart = false;
  // BRAIN-319 T3b-2: the same SIGTERM/SIGINT this supervisor already reacts
  // to (queued-cancel above, killGroup once running below) also aborts an
  // in-flight remote dispatch, via this AbortController's signal -- no
  // separate remote-specific cancel path.
  const abortController = new AbortController();
  // Codex pre-merge BLOCKER #1: a signal alone used to leave `cancelling`/
  // `cancelledBeforeStart` as in-memory-only state -- nothing durable
  // recorded that a cancellation was ACCEPTED. `publishTerminal`'s cancelled
  // branch (and the queued-cancel check below) both decide purely from the
  // marker, so a caller that signals the supervisor directly (never having
  // gone through `lane cancel`, which already writes it) could have a child
  // that exits 0 on TERM publish a plain green result. Writing the marker
  // HERE, synchronously, in the same tick the signal is accepted, closes
  // that gap for every cancellation source uniformly. These two listeners
  // are never `process.off()`'d, so they stay registered (and keep firing,
  // alongside `onCancelSignal` below once the local child exists) for this
  // supervisor's ENTIRE lifetime -- the single, authoritative marker-write
  // site regardless of which phase (remote-dispatch, queued, or a running
  // local child) the cancellation actually lands in. `onCancelSignal`
  // (below) deliberately does NOT duplicate this write.
  process.on('SIGTERM', () => {
    cancelledBeforeStart = true;
    writeCancelMarkerFile(root, ticket.id);
    abortController.abort();
  });
  process.on('SIGINT', () => {
    cancelledBeforeStart = true;
    writeCancelMarkerFile(root, ticket.id);
    abortController.abort();
  });

  let attemptGeneration = null;
  let fallbackReason;
  let rebindable = false;
  // BRAIN-380: createAttempt and enqueue refuse under the lock while `lane migrate-scheduler` runs. The ticket never
  // started, so publish exit 75 (through the attempt, if a fallback one exists) rather than die with no result.
  async function exitMigrating(err) {
    process.stderr.write(`lane: ${err.message}\n`);
    const publish = () => atomicWriteJson(ticket.resultPath, migrationRefusalResult(ticket));
    if (attemptGeneration !== null) await publishTerminal(root, ticket.id, attemptGeneration, publish);
    else publish();
    clearTicketMarkers(root, ticket.id);
    process.exit(75);
  }
  if (ticket.remote) {
    let outcome;
    try {
      outcome = await runRemoteAttempt(root, enriched, globalCfg, abortController.signal);
    } catch (err) {
      if (!(err instanceof MigrationInProgressError)) throw err;
      return exitMigrating(err);
    }
    if (!outcome.fallback) return; // terminal outcome: runRemoteAttempt already called process.exit()
    attemptGeneration = outcome.attemptGeneration;
    fallbackReason = outcome.fallbackReason;
    // BRAIN-405: only a fallback for want of a runner with room stays rebindable; the marker rides in the queue record.
    rebindable = outcome.rebindable === true;
    if (rebindable) enriched.remote = { ...enriched.remote, fallback: { reason: fallbackReason, at: Date.now() } };
  }

  /**
   * Codex pre-merge SHOULD-FIX #4: a post-fallback ticket cancelled while
   * still QUEUED (never leased) used to just `process.exit(0)` at either of
   * the two call sites below, leaving its attempt record neither published
   * nor removed -- `lane wait` (which counts a live attempt record as
   * "found") then polled forever. Both sites are reached only once the
   * marker is already guaranteed durable (this supervisor's own signal
   * handlers write it; `lane cancel`'s queued-dequeue branch already did
   * too before either site can observe the dequeue), so `publishTerminal`'s
   * own `isCancelled` check resolves this correctly on its own.
   */
  /**
   * BRAIN-320 S1d: shared by the queued-cancel exit (unchanged behaviour)
   * and the new queued queue-timeout exit -- both finalize a ticket that was
   * dequeued without ever being leased, through the same publish/exit shape,
   * differing only in the result they publish and the process exit code.
   * `outcome` is `'cancelled'` (default, today's behaviour) or
   * `'queue-timeout'` (BRAIN-320 1f/1g: exit 75, `reason: 'queue-timeout'`).
   *
   * A queue-timeout ticket is never itself a remote attempt fallback
   * (`ticket.remote` is unset for the runner-side pipeline ticket this
   * expires), so `attemptGeneration` is always null on that path -- unlike
   * the cancel path, a queue-timeout result is published UNCONDITIONALLY,
   * or `remote-exec`'s own kind derivation (reading this exact result.json)
   * would never see it and would fall through to a bare, reasonless
   * 'unfinished'.
   */
  async function finalizeQueuedAndExit(outcome = 'cancelled', { forcePublish = false } = {}) {
    // BRAIN-436: 'withdrawn' (a submitter took the queued ticket back to move it) finalizes exactly like a queue timeout:
    // a structured, unconditionally published exit-75 result that says the command never started.
    const isTimeout = outcome === 'queue-timeout' || outcome === 'withdrawn';
    const buildResult = () =>
      isTimeout
        ? {
            id: enriched.id,
            exit: 75,
            signal: null,
            startedAt: null,
            endedAt: Date.now(),
            waitedMs: null,
            cancelled: false,
            reason: outcome,
          }
        : { ...remoteCancelledResult(enriched.id, null), executor: 'local', fallbackReason };
    const publish = (result) => {
      atomicWriteJson(ticket.resultPath, result);
      appendHistory(root, historyRow(ticket, result, { fallbackReason }));
    };
    if (attemptGeneration !== null) {
      await publishTerminal(root, ticket.id, attemptGeneration, () => publish(buildResult()));
    } else if (isTimeout || forcePublish) {
      // BRAIN-320 review fix B: `forcePublish` is only ever set by the
      // queue-timeout-then-cancel recheck above -- a ticket the queue-
      // timeout branch was ABOUT to publish (unconditionally, since it's
      // never a remote-attempt fallback) still needs a structured result
      // once the outcome flips to 'cancelled', or nothing at all would be
      // recorded here (a plain queued-cancel elsewhere in this function
      // relies on remote-exec's own ticket-local marker instead, but this
      // recheck is reached via a BROKER-level `lane cancel` that never
      // touches that marker).
      publish(buildResult());
    }
    clearTicketMarkers(root, ticket.id);
    process.exit(isTimeout ? 75 : 0);
  }

  try {
    const queued = await enqueue(root, enriched, globalCfg, { ownerGeneration: attemptGeneration });
    // BRAIN-321: this id was already submitted (a retried `lane run --detach --id`); its first supervisor owns it.
    if (queued.existing) process.exit(0);
    if (queued.priorityDemoted) process.stderr.write('lane run: priority high demoted to medium (repo already has a queued high ticket)\n');
  } catch (err) {
    if (!(err instanceof MigrationInProgressError)) throw err;
    return exitMigrating(err);
  }

  /**
   * BRAIN-405: this ticket fell back to the local queue only because no runner had room then. Re-probe the runners and, if one
   * has real room now, move the ticket there. A runner is chosen BEFORE the ticket leaves the queue, so a probe that finds
   * nothing costs nothing; `runRemoteAttempt` then withdraws it atomically and, on any failure, puts it back where it was. It
   * only returns when the ticket is still queued locally -- a dispatched ticket ends in the process exiting, as ever.
   */
  async function tryRebind() {
    // A ticket never goes back to a runner it left, was refused by, or was ever dispatched to (nor to one it is on).
    const attempt = readAttempt(root, ticket.id);
    const left = new Set([...(attempt?.moves ?? []).flatMap((m) => [m.from, m.to]), ...(attempt?.refusedRunners ?? []), ...(attempt?.dispatchedRunners ?? [])]);
    const candidates = (globalCfg.runners || []).filter((r) => !left.has(r.name));
    if (candidates.length === 0) return;
    // An idle runner is always taken; a runner that is itself queued (up to maxRemoteQueue) only for a ticket stuck locally long enough.
    const stuckLocally = globalCfg.remoteRebindMinLocalWaitMs > 0 && Date.now() - (readQueuedRecord(root, ticket.id)?.localSince ?? Date.now()) >= globalCfg.remoteRebindMinLocalWaitMs;
    const { runner, probe, queuedChoice } = await selectRunner(candidates, { ...remoteSelectOptions(enriched, globalCfg), maxRemoteQueue: stuckLocally ? globalCfg.maxRemoteQueue : 0 });
    if (!runner) return;
    await runRemoteAttempt(root, enriched, globalCfg, abortController.signal, { generation: attemptGeneration, runner, probe, queued: queuedChoice === true });
  }
  let nextRebindAt = Date.now() + (globalCfg.remoteRebindIntervalMs || 0);
  // BRAIN-442: a local-first ticket is not rebind-eligible until it has waited in the local queue this long
  const rebindNotBefore = enriched.localFirst ? enriched.createdAt + (enriched.localFirstWaitMs ?? globalCfg.localFirstWaitMs) : 0;

  let started;
  for (;;) {
    // Re-read the config each iteration rather than reusing the snapshot
    // taken at startup: a supervisor can poll for a long time before it
    // starts (queued behind capacity or a closed load gate), and an operator
    // adjusting thresholds mid-run must take effect within one sampleMs, not
    // never. reloadGlobalConfig() falls back to the last known-good value on
    // a missing/invalid file, so a bad edit can't crash or wedge this loop.
    let reloadFailed = false;
    globalCfg = reloadGlobalConfig(globalCfg, {
      onError: (err) => {
        reloadFailed = true;
        recordConfigReloadError(root, err);
      },
    });
    if (!reloadFailed) clearConfigReloadWarning(root);
    if (cancelledBeforeStart || cancelRequested(root, ticket.id)) {
      // BRAIN-436: a withdrawn ticket that is also cancelled must still leave the queue (retrying) and publish its
      // cancelled result BEFORE the withdraw marker goes: that marker is what keeps `tryStart` from admitting it.
      const alsoWithdrawn = isWithdrawn(root, ticket.id);
      if (alsoWithdrawn) while (!dequeueSync(root, ticket.id)) await sleep(100);
      else dequeueSync(root, ticket.id);
      touchSimArmFor(root, ticket);
      await finalizeQueuedAndExit('cancelled', { forcePublish: alsoWithdrawn });
    }
    // tryStart re-reads the config again inside its lock (BRAIN-182): the
    // outer reload above can be superseded by an edit that lands in the gap
    // between this poll's reload and the lock actually being granted. Reuse
    // the same fallback/warning handling so a reload failure inside the lock
    // degrades exactly like one out here.
    try {
      started = await tryStart(root, enriched, globalCfg, undefined, undefined, () => {
        globalCfg = reloadGlobalConfig(globalCfg, {
          onError: (err) => {
            reloadFailed = true;
            recordConfigReloadError(root, err);
          },
        });
        return globalCfg;
      });
    } catch (err) {
      // BRAIN-345: failing to get the global lock within its deadline is
      // contention, not a fault. Dying here published no result and lost the
      // ticket; keep polling instead. Anything else still escapes.
      if (!(err instanceof LockTimeoutError)) throw err;
      writeBrokerLog(root, `lane: lock-timeout: ${enriched.id}: ${err.message} — still queued, retrying\n`);
      await sleep(globalCfg.sampleMs);
      continue;
    }
    // Test seam: lets a test land a cancel between admission and spawn (BRAIN-364).
    await testHoldAt('local-admitted');
    if (started.started) break;
    // BRAIN-436: the withdraw marker is the commit, but the queue file may still be there (the runner's best-effort
    // dequeue failed, or it crashed after the marker). Dequeue it here -- retrying the unlink -- BEFORE publishing, so the
    // published result never contradicts a ticket that is still queued. A cancel that is already pending wins.
    if (started.reason === 'withdrawn' || (started.reason === 'not-head' && started.position === null && isWithdrawn(root, ticket.id))) {
      while (!dequeueSync(root, ticket.id)) await sleep(100);
      // read AFTER the dequeue: the retry above can wait, and a cancel landing meanwhile must still win
      const cancelWon = cancelRequested(root, ticket.id);
      touchSimArmFor(root, ticket);
      await finalizeQueuedAndExit(cancelWon ? 'cancelled' : 'withdrawn', { forcePublish: cancelWon });
    }
    if (started.reason === 'not-head' && started.position === null) {
      // Our own ticket is no longer in the queue without ever having
      // started: it was cancelled out from under us (by `lane cancel`,
      // which dequeues + writes the marker itself before this is ever
      // observed). Same finalize-through-publishTerminal fix as the
      // cancelledBeforeStart branch above -- see finalizeQueuedAndExit.
      await finalizeQueuedAndExit('cancelled');
    }
    if (started.reason === 'queue-timeout') {
      // Test-only seam: `LANE_BROKER_TEST_PAUSE_AFTER_QUEUE_TIMEOUT` widens
      // the gap between tryStart recording the expiry and the cancel
      // recheck below, so a test can deterministically land a `lane cancel`
      // marker write into that exact window (see README's "Testing hooks").
      const pauseFile = process.env.LANE_BROKER_TEST_PAUSE_AFTER_QUEUE_TIMEOUT;
      if (pauseFile) {
        while (!fs.existsSync(pauseFile)) {
          await sleep(10);
        }
      }
      // BRAIN-320 review fix B: a runner-side `lane cancel` can land between
      // tryStart recording the expiry and this branch running. Re-check the
      // durable cancel marker here -- if a cancel was ALSO requested, it
      // wins: publish 'cancelled', never let the expiry silently overwrite
      // an explicit cancel that arrived after it.
      const cancelWon = cancelRequested(root, ticket.id);
      const outcome = cancelWon ? 'cancelled' : 'queue-timeout';
      // BRAIN-320 S1d: tryStart already wrote the durable expiry marker
      // (distinct from a cancel marker) under its own lock before returning
      // this reason -- dequeue and publish the queue-timeout result the same
      // way a queued cancel is finalized, so `remote-exec`'s own kind
      // derivation (reading this ticket's result.json) sees it.
      dequeueSync(root, ticket.id);
      touchSimArmFor(root, ticket);
      await finalizeQueuedAndExit(outcome, { forcePublish: cancelWon });
    }
    if (rebindable && globalCfg.remoteRebindIntervalMs > 0 && Date.now() >= nextRebindAt && Date.now() >= rebindNotBefore && (globalCfg.runners || []).length > 0) {
      nextRebindAt = Date.now() + globalCfg.remoteRebindIntervalMs;
      try {
        await tryRebind();
      } catch (err) {
        if (!(err instanceof MigrationInProgressError)) throw err;
        return exitMigrating(err);
      }
    }
    await sleep(globalCfg.sampleMs);
  }

  const startedAt = Date.now();
  const [cmd, args] = resolveNicedSpawn(ticket);
  const child = spawn(cmd, args, {
    cwd: ticket.cwd,
    detached: true,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: childEnv({ ...ticket, priorityAdmitted: started.lease.priorityAdmitted }, process.env, leaseCpuCores(started.lease), started.lease.classEnforcement),
  });

  const logWriter = new CappedLogWriter(ticket.logPath);
  // ticket.forwardOutput (BRAIN-308) is set explicitly by run.js's foreground
  // path, not inferred from our own fd state -- a --detach caller never even
  // connects a pipe here (see run.js), so there is nothing to guess from.
  const forwardOutput = ticket.forwardOutput === true;
  const stdoutForward = forwardOutput ? new ForwardWriter(process.stdout) : null;
  const stderrForward = forwardOutput ? new ForwardWriter(process.stderr) : null;
  const stdoutGate = backpressureGate(child.stdout);
  const stderrGate = backpressureGate(child.stderr);
  // BRAIN-431: bytes the command wrote, one of the progress signals the heartbeat watches; the kill itself is opt-in
  const watchdog = new NoProgressWatchdog({ timeoutMs: ticket.noProgressTimeoutMs ?? globalCfg.noProgressTimeoutMs });
  child.stdout.on('data', (c) => {
    watchdog.noteOutput(c.length);
    const loggedOk = logWriter.write(c);
    const forwardedOk = stdoutForward ? stdoutForward.write(c) : true;
    if (!loggedOk) logWriter.pauseUntilDrain(stdoutGate);
    if (!forwardedOk) stdoutForward.pauseUntilDrain(stdoutGate);
  });
  child.stderr.on('data', (c) => {
    watchdog.noteOutput(c.length);
    const loggedOk = logWriter.write(c);
    const forwardedOk = stderrForward ? stderrForward.write(c) : true;
    if (!loggedOk) logWriter.pauseUntilDrain(stderrGate);
    if (!forwardedOk) stderrForward.pauseUntilDrain(stderrGate);
  });

  writeLease(root, { ...started.lease, childPgid: child.pid, heartbeatAt: Date.now(), startedAt });

  let finished = false;
  let cancelling = false;
  let noProgressKilled = false;
  let killPromise = null;

  // BRAIN-419: processes that left the leader's group (setsid/setpgid) are invisible to killGroup, so track the tree.
  const descendants = new DescendantTracker(child.pid, { leaseId: ticket.id });
  descendants.scan();
  async function killTree(reason) {
    descendants.scan(); // the leader may still be alive: catches anything spawned since the last heartbeat
    const [, result] = await Promise.all([killGroup(child.pid), descendants.reap({ graceMs: CANCEL_GRACE_MS })]);
    // an incomplete reap is logged but never wedges the lease: the lease is still released
    writeBrokerLog(root, reapLogLine(ticket.id, result, reason));
  }

  /** BRAIN-431: ONE reap however many things ask (watchdog, cancel marker, signal, leader exit); the first caller's reason is logged. */
  function reap(reason) {
    killPromise ??= killTree(reason);
  }

  // BRAIN-425: one observation, shared by the heartbeat and an early sample, so a run shorter than sampleMs still gets a
  // CPU reading. Never taken on the exit path: reap and lease release must not wait on telemetry.
  function observeLease() {
    let idleMs = 0;
    try {
      const lease = readLease(root, ticket.id);
      if (lease) {
        // ONE process-table read per heartbeat feeds both the descendant record and the CPU/RSS observation
        const rows = descendants.scan();
        // BRAIN-431: the same scan feeds the progress watchdog; its streak is stamped on the lease for status/monitoring
        try {
          idleMs = watchdog.observe(rows ? [child.pid, ...descendants.live(rows)] : null);
        } catch {
          idleMs = 0; // a watchdog failure is an unobserved interval, never an idle one
        }
        const tree = rows ? selectLeaseTree(rows, child.pid, otherLeaseStops(root, ticket.id)) : null;
        const observed = tree?.cores ?? null;
        const observedMemory = tree?.memoryBytes ?? null;
        // a gap of more than two heartbeats between good readings ends an overrun streak
        writeLease(root, {
          ...applyHeartbeatObservation(lease, observed, Date.now(), observedMemory, 2 * globalCfg.sampleMs),
          descendants: descendants.snapshot(),
          noProgressSinceMs: Math.round(idleMs),
        });
      }
    } catch {
      // observation is telemetry: nothing in it may take the supervisor down while its child runs
    }
    return idleMs;
  }

  /** BRAIN-431: opt-in kill once the idle streak reaches the timeout; never after a cancel or another reap has begun. */
  function enforceNoProgress(idleMs) {
    if (killPromise || cancelling || !watchdog.shouldKill(idleMs)) return;
    noProgressKilled = true;
    reap(NO_PROGRESS_REASON);
  }
  const earlySample = setTimeout(() => {
    if (!finished) observeLease();
  }, Math.min(EARLY_SAMPLE_MS, globalCfg.sampleMs));

  const heartbeat = setInterval(() => {
    if (finished) return;
    enforceNoProgress(observeLease());
    // Codex pre-merge BLOCKER #1: the marker must survive until AFTER the
    // terminal write below decides on it -- clearing it here (as this used
    // to, the instant it was observed) let a child that exits 0 on TERM
    // race ahead of `finalizeAndExit`'s own check and publish green.
    if (!cancelling && cancelRequested(root, ticket.id)) {
      cancelling = true;
      reap();
    }
  }, globalCfg.sampleMs);

  const onCancelSignal = () => {
    if (!cancelling && !finished) {
      cancelling = true;
      reap();
    }
  };
  process.on('SIGTERM', onCancelSignal);
  process.on('SIGINT', onCancelSignal);
  // BRAIN-364: no `await` runs between the queue loop's successful `break` and here, so a cancel that landed during admission
  // (`cancelledBeforeStart` from a signal, or the marker) is seen exactly once, now, and handled like any cancel of a
  // running child: reaped, then finalized as 130 by finalizeAndExit. Without it a fast child published success.
  if (cancelledBeforeStart || cancelRequested(root, ticket.id)) {
    cancelling = true;
    reap();
  }

  async function finalizeAndExit(result, exitCode) {
    finished = true;
    clearInterval(heartbeat);
    clearTimeout(earlySample);
    // killGroup must be awaited on every path: if the group leader exited
    // but a TERM-resistant descendant is still alive, we must not write the
    // result / release the lease until the whole group is confirmed gone.
    if (killPromise) await killPromise;
    // BRAIN-349: a cancelled run is 'cancelled' (exit 130) whatever the child
    // itself exited with -- a child that traps TERM and exits 0 must not read
    // as success. Same shape as the remote-cancel result (remoteCancelledResult).
    if (cancelling) {
      result = { ...result, exit: 130, signal: null, cancelled: true };
      exitCode = 130;
    } else if (noProgressKilled) {
      // BRAIN-431: the kill's own signal is not the story; it is `timeout`'s 124, with the window that elapsed
      result = { ...result, exit: NO_PROGRESS_EXIT, signal: null, reason: NO_PROGRESS_REASON, noProgressTimeoutMs: watchdog.timeoutMs };
      const message = noProgressMessage(watchdog.timeoutMs);
      logWriter.write(message);
      stderrForward?.write(message);
    }
    await logWriter.finish();
    // Flush the forward pipes before exiting (BRAIN-308) -- process.exit()
    // below can otherwise drop the async tail of a macOS pipe write. Skipped
    // once a writer has failed (dead caller): draining a pipe nobody is
    // reading would just wait out its own bounded timeout for nothing.
    if (stdoutForward) await stdoutForward.drain();
    if (stderrForward) await stderrForward.drain();
    // Test seam: lets a test land a `lane cancel` after the child ended, before the result is written (BRAIN-364).
    await testHoldAt('local-finalize');

    // BRAIN-319 P2 (Codex re-review): `writeResult` only WRITES -- no
    // `process.exit` in here. The old `writeAndExit` called `process.exit`
    // from INSIDE the `publishTerminal` writer below, which runs inside ONE
    // `withLock` transaction -- `process.exit()` terminates the process
    // before that transaction's own `finally` (mutex release) ever runs, and
    // before `publishTerminal` itself gets to `removeAttempt`. A completed
    // fallback run then left both the global lock AND its attempt record
    // stale, so a later `lane cancel` could "reconcile" an orphaned-looking
    // attempt that was actually already done, and overwrite its result.
    // Exactly the shape `publishAndExit` (above, in `runRemoteAttempt`)
    // already gets right: write, let `publishTerminal` return (mutex
    // released, attempt removed), THEN exit.
    const writeResult = (finalResult) => {
      // BRAIN-360: an elastic lease's grant rides on the result (a runner relays it to its submitter)
      if (started.lease.grantedCpuCores !== undefined) finalResult = { ...finalResult, grantedCpuCores: started.lease.grantedCpuCores };
      // BRAIN-361: what the lease actually used, from the last heartbeat's lease file (absent when no
      // heartbeat ever observed it); additive, and relayed by a runner to its submitter like the grant.
      const finalLease = readLease(root, ticket.id);
      const observedCpu = summarizeObservedCpu(finalLease?.observedCpuStats);
      if (observedCpu) finalResult = { ...finalResult, observedCpu };
      const rssPeak = sanitizeRssPeak(finalLease?.observedRssPeakBytes);
      if (rssPeak !== undefined) finalResult = { ...finalResult, observedRssPeakBytes: rssPeak };
      const cpuSeconds = integratedCpuSeconds(finalLease?.observedCpuStats, startedAt, finalResult.endedAt);
      if (cpuSeconds !== undefined) finalResult = { ...finalResult, cpuSeconds };
      // BRAIN-431: the longest stretch with no output and no CPU, whether or not the opt-in kill is on (monitoring reads it)
      finalResult = { ...finalResult, maxNoProgressMs: Math.round(watchdog.maxIdleMs) };
      atomicWriteJson(ticket.resultPath, finalResult);
      appendHistory(root, historyRow(ticket, finalResult, { fallbackReason, lease: started.lease }));
      removeLease(root, ticket.id); // release always comes last
      touchSimArmFor(root, ticket); // after the release: the arm stamp's I/O never delays freeing the lease
      // Cleanup only, AFTER the terminal write above -- never before (BLOCKER #1).
      clearTicketMarkers(root, ticket.id);
    };

    // BRAIN-319 T3b-2: this ticket started life as a remote attempt that fell
    // back to local (`attemptGeneration` non-null) -- go through the SAME
    // one terminal writer every remote-side path uses, so the attempt
    // record is removed once this local run's outcome is actually published.
    // BRAIN-364: the outcome follows `cancelling` (folded into `result`/
    // `exitCode` above), exactly as on the plain local path -- NOT
    // `publishTerminal`'s own `cancelled` flag, which is true for a cancel
    // that landed after the child had already ended and was never acted on.
    if (attemptGeneration !== null) {
      const published = await publishTerminal(root, ticket.id, attemptGeneration, () => {
        if (cancelling) {
          writeResult({ ...remoteCancelledResult(enriched.id, startedAt), executor: 'local' });
        } else {
          writeResult({ ...result, executor: 'local', fallbackReason });
        }
      });
      if (published.ok) {
        process.exit(exitCode);
        return;
      }
      // Generation mismatch: unreachable today (this supervisor is the sole
      // writer of its own attempt record past the fallback), but a real
      // local result must never be silently dropped -- write it directly.
    }
    writeResult(result);
    process.exit(exitCode);
  }

  child.on('error', (err) => {
    if (finished) return;
    const endedAt = Date.now();
    finalizeAndExit(
      { id: ticket.id, exit: 1, signal: null, startedAt, endedAt, waitedMs: startedAt - enriched.createdAt || 0, error: err.message },
      1,
    );
  });

  // Listen on 'close' rather than 'exit': 'close' fires only after stdio is
  // fully drained, so a chatty child's buffered tail is never lost.
  child.on('close', (code, signal) => {
    if (finished) return;
    const endedAt = Date.now();
    const result = {
      id: ticket.id,
      exit: code,
      signal,
      startedAt,
      endedAt,
      waitedMs: startedAt - enriched.createdAt || 0,
    };
    if (groupReaped) result.reason = GROUP_REAPED_REASON;
    finalizeAndExit(result, 0);
  });

  // ROG-2181 T1b: a leader that exits on its own can leave descendants running unleased once the lease is
  // released. 'close' cannot be the trigger: a survivor holding the stdio pipes delays it until the survivor is
  // gone. So reap on 'exit'; finalizeAndExit awaits killPromise before the result is written or the lease released.
  let groupReaped = false;
  child.on('exit', () => {
    if (finished || cancelling || noProgressKilled) return;
    if (!isGroupAlive(child.pid) && !hasLiveMembers(descendants)) return;
    groupReaped = true;
    reap();
  });
}

export { CappedLogWriter, recordConfigReloadError, clearConfigReloadWarning };

// Guard so this module can be imported (e.g. by tests exercising
// CappedLogWriter directly) without running the supervisor for real; it only
// runs when invoked as the entrypoint, as run.js spawns it (`node supervisor.js`).
if (import.meta.url === pathToFileURL(process.argv[1] || '').href) {
  main().catch((err) => {
    process.stderr.write(`lane-broker supervisor error: ${err.stack || err.message}\n`);
    process.exit(1);
  });
}

/** Other held leases' supervisor pids and child groups, so a nested `lane run` is not observed twice (BRAIN-353). */
function otherLeaseStops(root, ownId) {
  const stopPids = new Set();
  const stopPgids = new Set();
  try {
    for (const l of listLeases(root)) {
      if (l.id === ownId) continue;
      if (Number.isFinite(l.supervisorPid)) stopPids.add(l.supervisorPid);
      if (Number.isFinite(l.childPgid)) stopPgids.add(l.childPgid);
    }
  } catch {
    // never let a lease-listing failure break the heartbeat: observe without stops
  }
  return { stopPids, stopPgids };
}
