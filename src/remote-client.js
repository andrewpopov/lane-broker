import { spawn } from 'node:child_process';
import { buildManifest, RemoteIneligibleError } from './remote-manifest.js';
import { encodeSnapshot, serializeHeader, MAX_HEADER_BYTES } from './remote-stream.js';
import { ELASTIC_CLAIMS_CAPABILITY } from './resources.js';
import { isPriorityTier } from './priority.js';

/**
 * BRAIN-319 T3a: the CLIENT side of the remote runner protocol implemented
 * by src/remote-runner.js (`lane remote-probe|remote-exec|remote-result|
 * remote-cancel`). This module dispatches ONE lane run to a remote runner
 * over ssh and returns a verified outcome; wiring it into the supervisor
 * (status/wait/cancel, fallback-to-local) is a later slice.
 */

/**
 * POSIX single-quote a string for use as one shell word: wrap in single
 * quotes, escaping any embedded single quote as `'\''`. Everything else
 * (spaces, `$`, backticks, newlines, double quotes) is inert inside single
 * quotes, so this is the only escaping a shell word here ever needs.
 */
export function shellQuote(str) {
  return `'${String(str).replace(/'/g, "'\\''")}'`;
}

/**
 * Build the command string handed to ssh as its single trailing argv
 * element: `${shell} ${shellQuote(inner)}`, where `inner` is `lane
 * <subcommand> [args...] [--root <root>]`, every argument individually
 * shell-quoted. `shell` defaults to `bash -lc` so a login shell's own
 * PATH/nvm setup resolves `lane` on a non-interactive ssh session.
 */
export function buildRemoteCommand(runner, subcommand, args = []) {
  const parts = ['lane', subcommand, ...args.map((a) => shellQuote(String(a)))];
  if (runner.root) parts.push('--root', shellQuote(runner.root));
  const inner = parts.join(' ');
  const shell = runner.shell || 'bash -lc';
  return `${shell} ${shellQuote(inner)}`;
}

/**
 * ssh's own argv, as an array -- never built by string-concatenating into a
 * local shell. `runner.ssh` and `commandString` are each their own argv
 * element; ssh alone decides how the destination's default shell parses
 * `commandString` on the far end.
 */
function sshArgv(runner, commandString) {
  // The remote-exec session lasts as long as the remote job, so it has no
  // deadline of its own; keepalives are what end a silently dead connection
  // (~60 s) so it surfaces as unconfirmed and falls back instead of hanging.
  return [
    '-o', 'BatchMode=yes',
    '-o', 'ConnectTimeout=3',
    '-o', 'ServerAliveInterval=15',
    '-o', 'ServerAliveCountMax=4',
    runner.ssh,
    commandString,
  ];
}

/** Spawn `cmdBin argv`, capturing stdout/stderr, SIGKILL on `deadlineMs`. Never rejects. */
function runWithDeadline(cmdBin, argv, deadlineMs, env) {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn(cmdBin, argv, { stdio: ['ignore', 'pipe', 'pipe'], env });
    } catch {
      resolve({ code: null, stdout: '', stderr: '', timedOut: false, spawnError: true });
      return;
    }
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
    }, deadlineMs);
    child.stdout.on('data', (d) => {
      stdout += d;
    });
    child.stderr.on('data', (d) => {
      stderr += d;
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ code, stdout, stderr, timedOut });
    });
    child.on('error', () => {
      clearTimeout(timer);
      resolve({ code: null, stdout, stderr, timedOut, spawnError: true });
    });
  });
}

/**
 * BRAIN-320 S1e: true iff `probe.capacity` proves this ticket can NEVER be
 * admitted on this runner -- a STATIC impossibility check using the same
 * inequalities admission uses (resources.js), never a reason to skip on
 * temporary pressure. Missing/malformed capacity fields never cause a skip
 * (an older or misbehaving probe degrades to "assume it might fit", not to
 * refusing every runner).
 */
function neverFits(reservation, probe) {
  const capacity = probe.capacity;
  if (!capacity || typeof capacity !== 'object') return false;
  const { weight, memoryBytes } = reservation;
  // BRAIN-360: only a runner that advertises elastic claims will admit below the declared claim; any
  // other runner is dispatched the full claim and refuses it (exit 64) if its budget is smaller.
  const elastic = Array.isArray(probe.capabilities) && probe.capabilities.includes(ELASTIC_CLAIMS_CAPABILITY);
  const cpuCores = elastic && Number.isFinite(reservation.minCpuCores) ? reservation.minCpuCores : reservation.cpuCores;
  if (Number.isFinite(capacity.weight) && Number.isFinite(weight) && weight > capacity.weight) return true;
  if (Number.isFinite(capacity.cpuCores) && Number.isFinite(cpuCores) && cpuCores > capacity.cpuCores) return true;
  if (
    Number.isFinite(capacity.memoryBytes) &&
    Number.isFinite(capacity.memoryReserveBytes) &&
    Number.isFinite(memoryBytes) &&
    memoryBytes + capacity.memoryReserveBytes > capacity.memoryBytes
  ) {
    return true;
  }
  return false;
}

/**
 * Probe every configured runner SEQUENTIALLY, in preference order (BRAIN-319
 * C8), returning the first usable one -- exit 0, protocol usable, not
 * `paused`, `queued === 0`, and (1e) statically able to fit the ticket -- as
 * `{runner, probe, skipped}` (any earlier candidates passed over on the way
 * to it), or `{runner: null, skipped}` naming why every candidate was passed
 * over. `opts.deadlineMs` (default 6000) bounds each individual probe; a
 * probe that hangs past it is SIGKILLed and counted as a skip, never as a
 * hang for the whole selection.
 *
 * `opts.requireProtocol2` (BRAIN-320 S1a/1d; renamed by the BRAIN-320
 * review-fix-D pass, was `needsProtocol2`): true when the ticket's lane
 * declares `remoteDeps`/`remoteSetup` (it needs protocol 2 to run at all)
 * OR the client has `remoteQueueTimeoutMs` configured (only a 0.7.0+,
 * protocol-2-capable runner honours a queue timeout -- an older runner
 * silently ignores it) -- either way, a runner whose probe does not offer
 * `2` in `protocols` is skipped. This governs RUNNER SELECTION only: the
 * exec header's own protocol is still derived from `needsProtocol2(remote)`
 * alone (see `dispatchRemote`), so an optionless lane still sends a
 * protocol-1 header even when `remoteQueueTimeoutMs` made this true.
 *
 * `opts.reservation` (BRAIN-320 S1e): the ticket's resolved
 * `{weight, cpuCores, memoryBytes}`, checked against each probe's static
 * `capacity` (1e). The existing `queued > 0` skip is unchanged.
 *
 * `opts.maxRemoteQueue` (BRAIN-338, default 0): when NO runner is idle, the
 * usable runner (every check above passed except `queued === 0`, and able to
 * fit the reservation) with the FEWEST queued tickets, provided
 * `1 <= queued <= maxRemoteQueue` (ties go to config order), is returned as
 * `{runner, probe, skipped, queuedChoice: true}`. This only PICKS; whether
 * to queue there instead of running locally is the caller's call, and
 * nothing is dispatched here. 0 keeps the pre-BRAIN-338 behaviour exactly.
 */
export async function selectRunner(runners, opts = {}) {
  const deadlineMs = opts.deadlineMs ?? 6000;
  const sshBin = opts.sshBin ?? 'ssh';
  const { env, requireProtocol2 = false, reservation, maxRemoteQueue = 0 } = opts;
  const skipped = [];
  let queuedBest = null;
  for (const runner of runners) {
    const cmd = buildRemoteCommand(runner, 'remote-probe');
    const res = await runWithDeadline(sshBin, sshArgv(runner, cmd), deadlineMs, env);
    if (res.timedOut) {
      skipped.push({ name: runner.name, reason: 'probe timed out' });
      continue;
    }
    if (res.code !== 0) {
      skipped.push({ name: runner.name, reason: `probe exited ${res.code}` });
      continue;
    }
    let probe;
    try {
      probe = JSON.parse(res.stdout.trim());
    } catch {
      skipped.push({ name: runner.name, reason: 'malformed probe response' });
      continue;
    }
    if (probe.protocol !== 1) {
      skipped.push({ name: runner.name, reason: `unsupported protocol: ${probe.protocol}` });
      continue;
    }
    if (requireProtocol2 && !(Array.isArray(probe.protocols) && probe.protocols.includes(2))) {
      skipped.push({ name: runner.name, reason: 'runner does not support protocol 2 (remoteDeps/remoteSetup/remoteQueueTimeoutMs)' });
      continue;
    }
    if (probe.draining) {
      skipped.push({ name: runner.name, reason: 'draining' });
      continue;
    }
    if (probe.paused) {
      skipped.push({ name: runner.name, reason: 'paused' });
      continue;
    }
    if (probe.queued !== 0) {
      const overCap = maxRemoteQueue > 0 && probe.queued > maxRemoteQueue;
      skipped.push({ name: runner.name, reason: `queued: ${probe.queued}${overCap ? ` (over maxRemoteQueue ${maxRemoteQueue})` : ''}` });
      const fits = !(reservation && neverFits(reservation, probe));
      const queueable = Number.isInteger(probe.queued) && probe.queued > 0 && probe.queued <= maxRemoteQueue;
      if (fits && queueable && (!queuedBest || probe.queued < queuedBest.probe.queued)) queuedBest = { runner, probe };
      continue;
    }
    if (reservation && neverFits(reservation, probe)) {
      skipped.push({ name: runner.name, reason: 'runner capacity can never fit this ticket' });
      continue;
    }
    return { runner, probe, skipped };
  }
  if (queuedBest) return { ...queuedBest, skipped, queuedChoice: true };
  return { runner: null, skipped };
}

/**
 * BRAIN-320 S1c (1d): the SINGLE place that decides whether a ticket's lane
 * needs protocol 2 -- true iff `remote.remoteDeps` or `remote.remoteSetup` is
 * a non-empty array. Both the runner-selection probe filter (supervisor.js's
 * call to `selectRunner`) and `dispatchRemote`'s own exec-header protocol
 * derive from this one function, so the two can never disagree about which
 * protocol a given ticket speaks. An optionless lane (`remoteDeps`/
 * `remoteSetup` absent or empty) always answers false and keeps speaking
 * protocol 1, byte-identical to before this slice (I6).
 */
export function needsProtocol2(remote) {
  return (
    (Array.isArray(remote?.remoteDeps) && remote.remoteDeps.length > 0) ||
    (Array.isArray(remote?.remoteSetup) && remote.remoteSetup.length > 0)
  );
}

/** True iff `record` is bound to `expected` (BRAIN-319 C6: ticketId,
 *  generation, manifestHash all match) AND was sent on the SAME protocol
 *  `expected.protocol` names -- a protocol-2 dispatch must not accept a
 *  protocol-1 record and vice versa (BRAIN-320 S1c). */
function matchesTicket(record, expected) {
  return (
    !!record &&
    record.protocol === expected.protocol &&
    record.ticketId === expected.ticketId &&
    record.generation === expected.generation &&
    record.manifestHash === expected.manifestHash
  );
}

const PIPELINE_PHASES = new Set(['deps', 'setup', 'command']);

/**
 * BRAIN-320 S1c (1b/1g): the ONE place that classifies a fetched
 * `remote-result` record for a given `expected` ticket -- both
 * `dispatchRemote` (below) and `isGreen` call through here, so there is
 * exactly one definition of "bound, confirmed, and green" (I7: green only
 * from phase `command`). Pure; never touches the network or the filesystem.
 *
 * Returns `{outcome: 'unconfirmed', reason}` or `{outcome: 'confirmed',
 * result, exitCode, green, phase?}`. `phase` is only ever set for a
 * protocol-2 record; a protocol-1 record's classification is unchanged from
 * before this slice.
 *
 * Protocol-2 completed rules (I7):
 *  - exit 0, no signal, phase 'command' -> confirmed, green.
 *  - nonzero integer exit, phase 'deps'|'setup'|'command' -> confirmed, red
 *    (terminal -- the caller must never fall back locally for this outcome).
 *  - exit 0 but phase !== 'command', phase missing/unrecognized, or a signal
 *    reported alongside exit 0 (a self-contradictory record) -> unconfirmed,
 *    never green.
 */
export function classifyRemoteResult(record, expected) {
  if (!matchesTicket(record, expected)) {
    return {
      outcome: 'unconfirmed',
      reason: 'result did not bind to this ticket (protocol/ticketId/generation/manifestHash mismatch)',
    };
  }

  if (record.kind === 'completed') {
    if (record.protocol === 2) {
      if (record.exit === 0 && record.signal) {
        return { outcome: 'unconfirmed', reason: 'completed result reports a signal alongside exit 0' };
      }
      const exitCode = record.signal ? 1 : record.exit;
      if (!Number.isInteger(exitCode)) {
        return { outcome: 'unconfirmed', reason: 'completed result has a non-integer exit' };
      }
      if (!PIPELINE_PHASES.has(record.phase)) {
        return { outcome: 'unconfirmed', reason: `completed result has an unrecognized phase: ${JSON.stringify(record.phase)}` };
      }
      if (exitCode === 0 && record.phase !== 'command') {
        return { outcome: 'unconfirmed', reason: `completed result reports exit 0 outside phase command (phase: ${record.phase})` };
      }
      return { outcome: 'confirmed', result: record, exitCode, phase: record.phase, green: exitCode === 0 && record.phase === 'command' };
    }
    const exitCode = record.signal ? 1 : record.exit;
    if (!Number.isInteger(exitCode)) return { outcome: 'unconfirmed', reason: 'completed result has a non-integer exit' };
    return { outcome: 'confirmed', result: record, exitCode, green: exitCode === 0 };
  }
  if (record.kind === 'refused') {
    if (!Number.isInteger(record.exit)) return { outcome: 'unconfirmed', reason: 'refused result has a non-integer exit' };
    return { outcome: 'confirmed', result: record, exitCode: record.exit, green: false };
  }
  if (record.kind === 'cancelled') {
    return { outcome: 'confirmed', result: record, exitCode: 130, green: false };
  }
  // rejected / unfinished / any other kind: no exit code this client may trust.
  return {
    outcome: 'unconfirmed',
    reason: record.reason ? `${record.kind}: ${record.reason}` : `unusable result kind: ${record.kind}`,
  };
}

/**
 * Pure "is this a passing green result" predicate: bound to `expected`
 * (protocol included) AND classified `confirmed` with `green === true` by
 * `classifyRemoteResult` -- the single definition of green.
 */
export function isGreen(result, expected) {
  const classified = classifyRemoteResult(result, expected);
  return classified.outcome === 'confirmed' && classified.green === true;
}

/** Pipe `stream` into `child.stdin`, bounded by `deadlineMs`, killable by `abortSignal`.
 *  Resolves `{ok:true}` once fully written, or `{ok:false, reason}` on error/timeout/abort
 *  or the ssh child closing early -- never rejects, and never waits past the deadline. */
function pipeSnapshot(child, stream, deadlineMs, abortSignal) {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (abortSignal) abortSignal.removeEventListener('abort', onAbort);
      resolve(result);
    };
    const timer = setTimeout(() => {
      stream.destroy();
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
      finish({ ok: false, reason: 'transfer deadline exceeded' });
    }, deadlineMs);
    const onAbort = () => {
      stream.destroy();
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
      finish({ ok: false, reason: 'aborted' });
    };
    if (abortSignal) abortSignal.addEventListener('abort', onAbort, { once: true });
    child.stdin.on('error', () => {
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
      finish({ ok: false, reason: 'ssh stdin error' });
    });
    // An encode/stream error (BRAIN-319 review finding #3) must not leave ssh's stdin open:
    // ssh has no way to know the transfer is over, so it would otherwise wait on stdin EOF
    // forever while the runner waits for bytes that are never coming. Destroy stdin and kill
    // ssh here so `waitClosed` resolves promptly and dispatchRemote can go straight to
    // unconfirmed with this reason, instead of hanging on an ssh session nothing will ever end.
    stream.on('error', (err) => {
      try {
        child.stdin.destroy();
      } catch {
        // already gone
      }
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
      finish({ ok: false, reason: `snapshot stream error: ${err.message}` });
    });
    child.stdin.on('finish', () => finish({ ok: true }));
    child.on('close', () => finish({ ok: false, reason: 'ssh closed before transfer finished' }));
    stream.pipe(child.stdin);
  });
}

/** Resolve once `child` has closed, killing it immediately on `abortSignal` (BRAIN-319:
 *  a caller must never fall through to "unconfirmed" after asking to cancel). */
function waitClosed(child, abortSignal) {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve();
      return;
    }
    const onClose = () => {
      if (abortSignal) abortSignal.removeEventListener('abort', onAbort);
      resolve();
    };
    const onAbort = () => {
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
    };
    child.once('close', onClose);
    if (abortSignal) abortSignal.addEventListener('abort', onAbort, { once: true });
  });
}

const DEFAULT_RESULT_WAIT_MS = 3 * 60 * 60_000;
const RESULT_POLL_START_MS = 5_000;
const RESULT_POLL_MAX_MS = 30_000;

/** Sleep `ms`, resolving early (never rejecting) if `abortSignal` fires. */
function sleepUnlessAborted(ms, abortSignal) {
  return new Promise((resolve) => {
    if (abortSignal && abortSignal.aborted) return resolve();
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      if (abortSignal) abortSignal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    if (abortSignal) abortSignal.addEventListener('abort', onAbort, { once: true });
  });
}

/** `lane remote-result <ticketId>` over ssh, bounded by `deadlineMs`. Returns the parsed
 *  record, `{missing:true}`, or `null` on any transport/parse failure (never throws). */
async function fetchRemoteResult(runner, ticketId, sshBin, deadlineMs, env) {
  const cmd = buildRemoteCommand(runner, 'remote-result', [ticketId]);
  const res = await runWithDeadline(sshBin, sshArgv(runner, cmd), deadlineMs, env);
  if (res.timedOut || res.code !== 0) return null;
  try {
    return JSON.parse(res.stdout.trim());
  } catch {
    return null;
  }
}

/** `lane remote-cancel <ticketId>` over ssh, best-effort: failures are swallowed since this
 *  only ever runs alongside an abort the caller has already decided to honour regardless. */
async function remoteCancelBestEffort(runner, ticketId, sshBin, deadlineMs, env) {
  const cmd = buildRemoteCommand(runner, 'remote-cancel', [ticketId]);
  await runWithDeadline(sshBin, sshArgv(runner, cmd), deadlineMs, env);
}

/** Public entry point for `remoteCancelBestEffort`, for a caller with no `dispatchRemote` of
 *  its own in flight -- BRAIN-319 T3b-4's ORPHANED-REMOTE reconciliation (`lane cancel`, no
 *  live supervisor left to have done this itself). Same best-effort contract: never throws. */
export async function remoteCancel(runner, ticketId, { sshBin = 'ssh', deadlineMs = 15_000, env } = {}) {
  await remoteCancelBestEffort(runner, ticketId, sshBin, deadlineMs, env);
}

/**
 * Dispatch one lane run to `runner` over ssh and resolve to exactly one of
 * `{outcome:'ineligible'|'confirmed'|'unconfirmed'|'cancelled', ...}`
 * (BRAIN-319 I1/C6). Never throws for a remote/transport failure -- only a
 * genuinely unexpected local error (not `RemoteIneligibleError`) from
 * `buildManifest` propagates.
 *
 * `deadlines` overrides (all optional): `transferMs` (default 5 min, bounds
 * only writing the snapshot to ssh's stdin -- the ssh session itself is then
 * awaited unboundedly, since it IS the remote job running), `resultMs`
 * (default 30s per attempt) and `resultAttempts` (default 3) for fetching
 * `remote-result`, `cancelMs` (default 15s) for the best-effort
 * `remote-cancel` on abort. BRAIN-339: while `remote-result` reports the job `queued`/`running`
 * the client polls it (5s backoff growing to 30s) for up to `resultWaitMs`.
 */
export async function dispatchRemote(opts) {
  const {
    runner,
    worktreeRoot,
    relCwd = '',
    repoKey,
    lane,
    weight,
    cpuCores,
    minCpuCores,
    memoryBytes,
    argv,
    ticketId,
    generation,
    remoteDeps = null,
    remoteSetup = null,
    // BRAIN-389: only `false` (the lane opted out) rides in the header; the cache is on by default on the runner.
    remoteDepsCache = true,
    // BRAIN-320 S1d: opt-in, from the CLIENT machine's global config
    // (`remoteQueueTimeoutMs`) -- unset (undefined/null) means the header
    // carries no such key at all, byte-identical to before this slice (I6).
    // Rides on both protocol 1 and 2: an older runner simply ignores an
    // unknown field, which is fine, since the whole feature is opt-in.
    queueTimeoutMs = null,
    // BRAIN-380 §6: the tier the submitter resolved BEFORE any cap, and the wait it has accrued on its own priority
    // clock. Additive header fields: an older runner ignores them, so its ticket is simply medium.
    priorityRequested,
    priorityAccruedMs,
    // BRAIN-339: from the CLIENT's global config (`remoteResultWaitMs`); how long to keep
    // polling a runner that reports the job still queued/running after ssh dropped.
    resultWaitMs = DEFAULT_RESULT_WAIT_MS,
    // Injectable for tests: sleep(ms, abortSignal) resolves early on abort; now() is a clock.
    sleep = sleepUnlessAborted,
    now = Date.now,
    onStdout,
    onStderr,
    abortSignal,
    deadlines = {},
    sshBin = 'ssh',
    env,
    // BRAIN-319 T3b-5: a caller (the supervisor) that has already decided
    // eligibility up front -- BEFORE ever probing/selecting a runner -- can
    // hand the manifest it already built here instead of having this
    // function build it again. Kept as ONE code path either way: this is
    // still the only place `buildManifest` is called for a dispatch: either
    // by the caller (once, up front) or by this function itself when no
    // caller has done so (every existing direct caller/test, unchanged).
    manifest: prebuiltManifest,
  } = opts;

  const transferDeadlineMs = deadlines.transferMs ?? 5 * 60_000;
  const resultDeadlineMs = deadlines.resultMs ?? 30_000;
  const resultAttempts = deadlines.resultAttempts ?? 3;
  const cancelDeadlineMs = deadlines.cancelMs ?? 15_000;

  let manifest = prebuiltManifest;
  if (!manifest) {
    try {
      manifest = buildManifest(worktreeRoot);
    } catch (err) {
      if (err instanceof RemoteIneligibleError) return { outcome: 'ineligible', reason: err.message };
      throw err;
    }
  }

  if (abortSignal && abortSignal.aborted) return { outcome: 'cancelled' };

  // BRAIN-320 S1c (1d): the protocol this dispatch speaks is derived here,
  // from `needsProtocol2`, and nowhere else -- `header.protocol` is the
  // single source `encodeSnapshot` serializes and `expected.protocol` (below)
  // binds the fetched result against. remoteDeps/remoteSetup only ever ride
  // in the header when they are actually present (undefined, never null --
  // a protocol-1 header must never carry either key at all, see
  // remote-runner.js's `validateHeaderFields`).
  const protocol = needsProtocol2({ remoteDeps, remoteSetup }) ? 2 : 1;
  const header = { protocol, ticketId, generation, repoKey, lane, weight, cpuCores, memoryBytes, argv, relCwd };
  if (protocol === 2) {
    if (Array.isArray(remoteDeps) && remoteDeps.length > 0) header.remoteDeps = remoteDeps;
    if (Array.isArray(remoteSetup) && remoteSetup.length > 0) header.remoteSetup = remoteSetup;
    if (remoteDepsCache === false) header.remoteDepsCache = false;
  }
  if (Number.isFinite(minCpuCores)) header.minCpuCores = minCpuCores;
  if (Number.isInteger(queueTimeoutMs) && queueTimeoutMs > 0) header.queueTimeoutMs = queueTimeoutMs;
  if (isPriorityTier(priorityRequested)) header.priorityRequested = priorityRequested;
  if (Number.isInteger(priorityAccruedMs) && priorityAccruedMs >= 0) header.priorityAccruedMs = priorityAccruedMs;

  // BRAIN-320 follow-up: reject an oversized header locally, before ever
  // dialing ssh. `serializeHeader` is the exact function `encodeSnapshot`
  // uses for this same line (below), so the byte count checked here can
  // never disagree with what actually gets sent -- a repo whose manifest
  // alone exceeds the runner's `MAX_HEADER_BYTES` used to dial ssh, get its
  // stdin closed by `lane remote-exec` before a ticket dir even existed,
  // and surface only as an opaque "ssh stdin error" with a silent fallback
  // to local. This turns that into a named ineligible reason up front.
  const headerLine = serializeHeader(header, manifest.entries);
  const headerBytes = Buffer.byteLength(headerLine, 'utf8');
  if (headerBytes > MAX_HEADER_BYTES) {
    return {
      outcome: 'ineligible',
      reason: `snapshot header is ${headerBytes} bytes, over the runner limit of ${MAX_HEADER_BYTES} (too many files)`,
    };
  }

  const snapshotStream = encodeSnapshot(worktreeRoot, header, manifest.entries);

  const execCommand = buildRemoteCommand(runner, 'remote-exec');
  const child = spawn(sshBin, sshArgv(runner, execCommand), { stdio: ['pipe', 'pipe', 'pipe'], env });
  if (onStdout) child.stdout.on('data', onStdout);
  if (onStderr) child.stderr.on('data', onStderr);

  // ssh's own EXIT CODE is ignored deliberately below (BRAIN-319: "IGNORE
  // ssh's exit code") -- the authoritative outcome only ever comes from the
  // fetched result record. pipeResult itself is still consulted: an encode
  // failure (finding #3) means no complete manifest-bound stream was ever
  // sent, so the runner cannot plausibly hold a matching result yet -- go
  // straight to unconfirmed instead of burning up to resultAttempts *
  // resultDeadlineMs retrying a fetch that was never going to bind.
  const pipeResult = await pipeSnapshot(child, snapshotStream, transferDeadlineMs, abortSignal);
  // The ssh session IS the remote job; wait for it to close (unbounded here
  // -- an outer --timeout is the caller's concern), killable by abort.
  await waitClosed(child, abortSignal);

  if (abortSignal && abortSignal.aborted) {
    await remoteCancelBestEffort(runner, ticketId, sshBin, cancelDeadlineMs, env);
    return { outcome: 'cancelled' };
  }

  if (!pipeResult.ok) {
    return { outcome: 'unconfirmed', reason: pipeResult.reason };
  }

  const expected = { protocol, ticketId, generation, manifestHash: manifest.manifestHash };
  let record = null;
  let attemptsMade = 0;
  let waitedFrom = null;
  let backoffMs = RESULT_POLL_START_MS;
  let waitExpired = false;
  while (true) {
    if (abortSignal && abortSignal.aborted) {
      await remoteCancelBestEffort(runner, ticketId, sshBin, cancelDeadlineMs, env);
      return { outcome: 'cancelled' };
    }
    const fetched = await fetchRemoteResult(runner, ticketId, sshBin, resultDeadlineMs, env);
    if (fetched && !fetched.missing) {
      record = fetched;
      break;
    }
    attemptsMade += 1;
    // BRAIN-339: a runner that reports the job alive (state queued/running) is waited on, since
    // the ssh session dropping does not stop the job; `gone` is final. A runner with no `state`
    // (older version) or a failed fetch keeps the bounded back-to-back retries.
    // Once alive was seen, a failed fetch (transport still down) is tolerated for the wait budget.
    const alive = fetched
      ? fetched.state === 'queued' || fetched.state === 'running'
      : waitedFrom !== null;
    if (fetched && fetched.state === 'gone') break;
    if (!alive) {
      if (attemptsMade >= resultAttempts) break;
      // A failed fetch (null) means the transport is likely still down: back off between tries.
      // A {missing:true} with no state (older runner) keeps the back-to-back retries.
      if (!fetched) {
        await sleep(backoffMs, abortSignal);
        backoffMs = Math.min(backoffMs * 2, RESULT_POLL_MAX_MS);
      }
      continue;
    }
    waitedFrom ??= now();
    if (now() - waitedFrom >= resultWaitMs) {
      waitExpired = true;
      break;
    }
    await sleep(backoffMs, abortSignal);
    backoffMs = Math.min(backoffMs * 2, RESULT_POLL_MAX_MS);
  }

  if (abortSignal && abortSignal.aborted) {
    await remoteCancelBestEffort(runner, ticketId, sshBin, cancelDeadlineMs, env);
    return { outcome: 'cancelled' };
  }

  if (!record) {
    return {
      outcome: 'unconfirmed',
      reason: waitExpired
        ? `remote job still running after waiting ${resultWaitMs}ms for its result`
        : 'result not available after retries',
    };
  }

  // BRAIN-320 S1c: classification (bound? kind? green?) all happens in ONE
  // place, `classifyRemoteResult`, shared with `isGreen` -- see its own doc
  // comment for the phase rules (I7: green only from phase 'command').
  const classification = classifyRemoteResult(record, expected);
  if (classification.outcome === 'unconfirmed') {
    return { outcome: 'unconfirmed', reason: classification.reason };
  }
  return { outcome: 'confirmed', result: record, exitCode: classification.exitCode, phase: classification.phase };
}
