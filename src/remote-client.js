import { spawn } from 'node:child_process';
import { buildManifest, RemoteIneligibleError } from './remote-manifest.js';
import { encodeSnapshot, serializeHeader, makeReader, MAX_HEADER_BYTES } from './remote-stream.js';
import { receiveArtifacts, artifactLimitsOf } from './remote-artifacts.js';
import { DEFAULT_GLOBAL_CONFIG } from './config.js';
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

// BRAIN-398: a runner is less trusted than this machine, so what it prints is bounded. Every capture keeps its tail.
const DEFAULT_CAPTURE_BYTES = 1024 * 1024;
const ARTIFACT_CAPTURE_BYTES = 64 * 1024;

/** Collects chunks, keeping only the last `max` bytes. `text()` marks a truncated capture. */
function tailCapture(max) {
  let buf = Buffer.alloc(0);
  let truncated = false;
  return {
    push(chunk) {
      buf = Buffer.concat([buf, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);
      if (buf.length > max) {
        buf = buf.subarray(buf.length - max);
        truncated = true;
      }
    },
    text: () => (truncated ? `[truncated] ${buf.toString('utf8')}` : buf.toString('utf8')),
  };
}

/** Spawn `cmdBin argv`, capturing (the tail of) stdout/stderr, SIGKILL on `deadlineMs`. Never rejects. */
export function runWithDeadline(cmdBin, argv, deadlineMs, env, maxCaptureBytes = DEFAULT_CAPTURE_BYTES) {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn(cmdBin, argv, { stdio: ['ignore', 'pipe', 'pipe'], env });
    } catch {
      resolve({ code: null, stdout: '', stderr: '', timedOut: false, spawnError: true });
      return;
    }
    const out = tailCapture(maxCaptureBytes);
    const err = tailCapture(maxCaptureBytes);
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
    }, deadlineMs);
    child.stdout.on('data', (d) => out.push(d));
    child.stderr.on('data', (d) => err.push(d));
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ code, stdout: out.text(), stderr: err.text(), timedOut });
    });
    child.on('error', () => {
      clearTimeout(timer);
      resolve({ code: null, stdout: out.text(), stderr: err.text(), timedOut, spawnError: true });
    });
  });
}

/** The CPU a runner must be able to give this ticket: the elastic floor only on a runner that admits below the declared claim. */
function cpuNeedOf(reservation, probe) {
  const elastic = Array.isArray(probe.capabilities) && probe.capabilities.includes(ELASTIC_CLAIMS_CAPABILITY);
  return elastic && Number.isFinite(reservation.minCpuCores) ? reservation.minCpuCores : reservation.cpuCores;
}

/**
 * BRAIN-405: true iff the runner has REAL room for this ticket right now: free CPU (the probe's measured `headroom`, else its
 * CPU budget minus reserved) covers the ticket's CPU (its elastic floor on an elastic runner) and available memory covers its
 * memory. An unmeasured figure never counts against a runner (an older probe or shadow mode degrades to "assume room").
 * Returns `{ room, cpuFree }`; `cpuFree` ranks runners that tie.
 */
export function runnerRoom(reservation, probe) {
  const headroom = probe.headroom && typeof probe.headroom === 'object' ? probe.headroom : null;
  let cpuFree = headroom && Number.isFinite(headroom.cpuCores) ? headroom.cpuCores : null;
  if (cpuFree === null && Number.isFinite(probe.capacity?.cpuCores) && Number.isFinite(probe.reservedCpuCores)) {
    cpuFree = Math.max(0, probe.capacity.cpuCores - probe.reservedCpuCores);
  }
  const cpuNeed = cpuNeedOf(reservation, probe);
  if (cpuFree !== null && Number.isFinite(cpuNeed) && cpuNeed > cpuFree) return { room: false, cpuFree, reason: `no CPU headroom (${cpuFree.toFixed(2)} free, need ${cpuNeed})` };
  const memFree = headroom && Number.isFinite(headroom.memoryBytes) ? headroom.memoryBytes : null;
  if (memFree !== null && Number.isFinite(reservation.memoryBytes) && reservation.memoryBytes > memFree) {
    return { room: false, cpuFree, reason: `no memory headroom (${memFree} bytes available, need ${reservation.memoryBytes})` };
  }
  return { room: true, cpuFree };
}

/**
 * BRAIN-405: estimated finish on this runner, in cpu-core-units of work: the ticket's own cores, once for each ticket already
 * queued ahead of it and once for itself, divided by the runner's configured `speedFactor` (higher is faster, default 1).
 * Deliberately simple: it orders runners, it is not a prediction.
 */
export function estimatedFinish(reservation, probe, runner) {
  const work = Number.isFinite(reservation?.cpuCores) && reservation.cpuCores > 0 ? reservation.cpuCores : 1;
  const queued = Number.isInteger(probe.queued) && probe.queued > 0 ? probe.queued : 0;
  const speed = Number.isFinite(runner.speedFactor) && runner.speedFactor > 0 ? runner.speedFactor : 1;
  return ((queued + 1) * work) / speed;
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
  const cpuCores = cpuNeedOf(reservation, probe);
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
 * Probe every configured runner SEQUENTIALLY (BRAIN-319 C8), returning the best usable one -- exit 0, protocol usable, not
 * `paused`, `queued === 0`, (1e) statically able to fit the ticket, and (BRAIN-405) with real headroom now (`runnerRoom`) --
 * as `{runner, probe, skipped}`, or `{runner: null, skipped}` naming why every candidate was passed over. Among usable
 * runners the earliest `estimatedFinish` wins, then the most free CPU, then config order. With none usable, `busy`
 * (hotfix 0.22.1) names the best runner that failed only the headroom check, for a lane that has no local queue to wait in. `opts.deadlineMs` (default 6000) bounds each individual probe; a
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
 * `opts.requireCapabilities` (BRAIN-321): capability strings the runner's probe must advertise, else it is skipped.
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
  const { env, requireProtocol2 = false, requireCapabilities = [], reservation, maxRemoteQueue = 0 } = opts;
  const skipped = [];
  // BRAIN-405: every runner is probed, then the idle one with real room and the earliest estimated finish wins (ties: more
  // free CPU, then config order). A runner with an empty queue but no headroom is not idle, however empty its queue.
  let idleBest = null;
  let queuedBest = null;
  let busyBest = null; // passed every check except headroom, empty queue: its own broker would hold the ticket until it has room
  const better = (cand, best) => !best || cand.finish < best.finish || (cand.finish === best.finish && cand.cpuFree > best.cpuFree);
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
    // BRAIN-321: a ticket whose lane REQUIRES a feature (class enforcement) never goes to a runner that cannot enforce it; an older
    // runner would ignore the unknown lane key and run it unenforced. A ticket with no requirement is unchanged.
    const missing = requireCapabilities.filter((c) => !(Array.isArray(probe.capabilities) && probe.capabilities.includes(c)));
    if (missing.length) {
      skipped.push({ name: runner.name, reason: `runner does not advertise ${missing.join(', ')} (required by the lane)` });
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
      if (fits && queueable) {
        const cand = { runner, probe, finish: estimatedFinish(reservation, probe, runner), cpuFree: 0 };
        if (!queuedBest || cand.probe.queued < queuedBest.probe.queued || (cand.probe.queued === queuedBest.probe.queued && better(cand, queuedBest))) queuedBest = cand;
      }
      continue;
    }
    if (reservation && neverFits(reservation, probe)) {
      skipped.push({ name: runner.name, reason: 'runner capacity can never fit this ticket' });
      continue;
    }
    const { room, cpuFree, reason } = reservation ? runnerRoom(reservation, probe) : { room: true, cpuFree: 0 };
    if (!room) {
      skipped.push({ name: runner.name, reason });
      const cand = { runner, probe, finish: estimatedFinish(reservation, probe, runner), cpuFree };
      if (better(cand, busyBest)) busyBest = cand;
      continue;
    }
    const cand = { runner, probe, finish: estimatedFinish(reservation, probe, runner), cpuFree };
    if (better(cand, idleBest)) idleBest = cand;
  }
  if (idleBest) return { runner: idleBest.runner, probe: idleBest.probe, skipped };
  if (queuedBest) return { runner: queuedBest.runner, probe: queuedBest.probe, skipped, queuedChoice: true };
  return { runner: null, skipped, ...(busyBest ? { busy: { runner: busyBest.runner, probe: busyBest.probe } } : {}) };
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
    // An abort that fired before this wait began (no listener existed to hear it) must still end the session.
    if (abortSignal && abortSignal.aborted) {
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
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

/**
 * BRAIN-398: `lane remote-artifacts <ticketId>` over ssh -- read the framed stream, verify it (`receiveArtifacts`),
 * then ask the runner to delete its copies (best-effort). Resolves `{ok:true, files}` or `{ok:false, reason}`; never
 * throws or rejects, and never writes anything: installing is the caller's step.
 */
async function fetchRemoteArtifacts(runner, ticketId, { patterns, limits, sshBin, deadlineMs, env }) {
  try {
    return await readRemoteArtifacts(runner, ticketId, { patterns, limits, sshBin, deadlineMs, env });
  } finally {
    // Every exit path -- success, refusal, truncation, error -- tells the runner to drop its copies.
    await runWithDeadline(sshBin, sshArgv(runner, buildRemoteCommand(runner, 'remote-artifacts-release', [ticketId])), deadlineMs, env, ARTIFACT_CAPTURE_BYTES);
  }
}

async function readRemoteArtifacts(runner, ticketId, { patterns, limits, sshBin, deadlineMs, env }) {
  const cmd = buildRemoteCommand(runner, 'remote-artifacts', [ticketId]);
  let child;
  try {
    child = spawn(sshBin, sshArgv(runner, cmd), { stdio: ['ignore', 'pipe', 'pipe'], env });
  } catch (err) {
    return { ok: false, reason: `ssh failed to start: ${err.message}` };
  }
  const stderr = tailCapture(ARTIFACT_CAPTURE_BYTES);
  child.stderr.on('data', (d) => stderr.push(d));
  const exited = new Promise((resolve) => {
    child.on('close', (code) => resolve(code));
    child.on('error', () => resolve(null));
  });
  const timer = setTimeout(() => child.kill('SIGKILL'), deadlineMs);
  const received = await receiveArtifacts(makeReader(child.stdout), { ticketId, patterns, limits });
  if (!received.ok) {
    // Nothing reads the rest of the stream, and an unread pipe never closes the child.
    child.stdout.destroy();
    child.kill('SIGKILL');
  }
  const code = await exited;
  clearTimeout(timer);
  const detail = stderr.text().trim();
  if (!received.ok) return { ok: false, reason: detail ? `${received.reason} (${detail})` : received.reason };
  if (code !== 0) return { ok: false, reason: `remote-artifacts exited ${code}` };
  return received;
}

/** `lane remote-cancel <ticketId>` over ssh, best-effort: failures are swallowed since this
 *  only ever runs alongside an abort the caller has already decided to honour regardless.
 *  Resolves true only when the runner CONFIRMED the cancel: exit 0 within the deadline AND its JSON reply says
 *  `cancelConfirmed: true` (exit 0 alone also covers a cancel that was only requested; an older runner prints no JSON). */
async function remoteCancelBestEffort(runner, ticketId, sshBin, deadlineMs, env) {
  const cmd = buildRemoteCommand(runner, 'remote-cancel', [ticketId]);
  const res = await runWithDeadline(sshBin, sshArgv(runner, cmd), deadlineMs, env);
  if (res.code !== 0 || res.timedOut) return false;
  try {
    return JSON.parse(String(res.stdout).trim().split('\n').pop()).cancelConfirmed === true;
  } catch {
    return false;
  }
}

/** Public entry point for `remoteCancelBestEffort` (resolves whether the runner CONFIRMED the cancel), for a caller with no `dispatchRemote` of
 *  its own in flight -- BRAIN-319 T3b-4's ORPHANED-REMOTE reconciliation (`lane cancel`, no
 *  live supervisor left to have done this itself). Same best-effort contract: never throws. */
export async function remoteCancel(runner, ticketId, { sshBin = 'ssh', deadlineMs = 15_000, env } = {}) {
  return remoteCancelBestEffort(runner, ticketId, sshBin, deadlineMs, env);
}

/**
 * BRAIN-436: `lane remote-withdraw <ticketId>` over ssh -- ask the runner to take back a ticket that is still QUEUED there,
 * irreversibly. Resolves `{action}` where `action` is the runner's own word (`withdrawn`, `started`, `cancelled`,
 * `not-queued`, `finished`, `no-such-ticket`), or `{action:'unknown'}` for a nonzero exit, a timeout or a reply that does not
 * parse and bind to this ticket. Only `withdrawn` is ever acted on; never throws.
 */
export async function remoteWithdraw(runner, ticketId, { sshBin = 'ssh', deadlineMs = 15_000, env } = {}) {
  const unknown = { action: 'unknown' };
  const res = await runWithDeadline(sshBin, sshArgv(runner, buildRemoteCommand(runner, 'remote-withdraw', [ticketId])), deadlineMs, env);
  if (res.timedOut || res.code !== 0) return unknown;
  try {
    const reply = JSON.parse(String(res.stdout).trim().split('\n').pop());
    return reply?.protocol === 1 && reply.ticketId === ticketId && typeof reply.action === 'string' ? { action: reply.action } : unknown;
  } catch {
    return unknown;
  }
}

/** True iff `record` binds to `expected` AND is the runner's own proof that nothing ran: the ticket was withdrawn while queued. */
function isWithdrawnRecord(record, expected) {
  return matchesTicket(record, expected) && record.kind === 'unfinished' && record.reason === 'withdrawn';
}

/** True iff `record` binds to `expected` and is the runner's own statement that nothing ran: rejected before running, expired
 *  in its queue, or withdrawn while queued. */
function provesNeverStarted(record, expected) {
  return (
    matchesTicket(record, expected) &&
    (record.kind === 'rejected' || (record.kind === 'unfinished' && (record.reason === 'queue-timeout' || record.reason === 'withdrawn')))
  );
}

const DEFAULT_REBALANCE_INTERVAL_MS = 30_000;

/**
 * BRAIN-436: why a ticket may not move right now (null: it may). Pure, so the cooldown is testable on a fake clock: a ticket
 * moves at most `maxMoves` times, and not again until `cooldownMs` after its last move.
 */
export function rebalanceBlocked({ moves, maxMoves, lastMoveAt, now, cooldownMs }) {
  if (moves >= maxMoves) return 'max-moves';
  if (lastMoveAt !== null && now - lastMoveAt < cooldownMs) return 'cooldown';
  return null;
}

/** BRAIN-436: a move needs a POSITIVE measurement -- unlike dispatch (`runnerRoom`), a probe with no measured free CPU is not room. */
export function hasMeasuredHeadroom(probe) {
  return Boolean(probe?.headroom) && typeof probe.headroom === 'object' && Number.isFinite(probe.headroom.cpuCores);
}

/**
 * BRAIN-436: while the exec ssh session is open (the ticket is on runner A), observe A and, once the ticket has sat `queued`
 * for `minQueuedMs` with a better runner on offer, withdraw it. ONE serial loop: every step is awaited in turn, so there is
 * never more than one move in flight, and every await is followed by a re-check that this dispatch is still current (same
 * runner, same epoch), the ssh session is still open and nobody cancelled. Resolves `{from, to, queuedMs}` only on positive
 * proof the runner never started the ticket (a `withdrawn` reply, or a bound `unfinished/withdrawn` record fetched from A);
 * anything else keeps waiting on A. Stops for good once the ticket is `admitted` or terminal. Resolves null when it did not move.
 */
async function watchQueuedTicket({ runner, expected, state, rebalance, isClosed, signal, abortSignal, now, sleep, fetchDeadlineMs, withdrawDeadlineMs, sshBin, env }) {
  const { minQueuedMs, intervalMs = DEFAULT_REBALANCE_INTERVAL_MS, pickTarget, beforeWithdraw, afterWithdraw } = rebalance;
  const fence = { runner, epoch: state.epoch };
  const current = () => state.runner === fence.runner && state.epoch === fence.epoch;
  const watching = () => current() && !isClosed() && !signal.aborted && !abortSignal?.aborted;
  const moved = (to, since) => {
    state.epoch += 1;
    return { from: runner, to, queuedMs: now() - since, withdrawnAt: now() };
  };
  let queuedSince = null;
  while (true) {
    await sleep(intervalMs, signal);
    if (!watching()) return null;
    const fetched = await fetchRemoteResult(runner, expected.ticketId, sshBin, fetchDeadlineMs, env);
    if (!watching()) return null;
    if (!fetched) continue;
    if (!fetched.missing) {
      // A record is terminal. Only a bound `withdrawn` one (from A, this epoch) proves a lost withdraw reply took effect.
      if (state.attemptedTarget && isWithdrawnRecord(fetched, expected)) return moved(state.attemptedTarget, state.queuedSince);
      return null;
    }
    const phase = fetched.phase ?? fetched.state;
    // `gone` is read before the runner has written its publisher record (the snapshot may still be landing), so it is not final here.
    if (phase !== 'preparing' && phase !== 'queued' && phase !== 'gone') return null;
    if (phase !== 'queued') continue;
    queuedSince ??= now();
    state.queuedSince = queuedSince;
    if (now() - queuedSince < minQueuedMs) continue;
    // A callback that throws or rejects is a failed tick: no move, keep watching. The target is awaited (the real picker is async).
    let to;
    try {
      to = await pickTarget();
      if (!to || !watching()) continue;
      if (beforeWithdraw && (await beforeWithdraw({ from: runner, to, queuedMs: now() - queuedSince })) === false) continue;
    } catch {
      continue;
    }
    if (!watching()) return null;
    state.attemptedTarget = to; // kept on the dispatch's own state: a lost reply plus a closed session is resolved from the record by the caller
    const { action } = await remoteWithdraw(runner, expected.ticketId, { sshBin, deadlineMs: withdrawDeadlineMs, env });
    // The runner decided; a cancel or an ssh close that raced it is the caller's to resolve, not ours to undo.
    if (!current()) return null;
    if (action === 'withdrawn') return moved(to, queuedSince);
    try {
      await afterWithdraw?.(action);
    } catch {
      // a failed notification is not a move
    }
  }
}

/**
 * Dispatch one lane run to `runner` over ssh and resolve to exactly one of
 * `{outcome:'ineligible'|'confirmed'|'unconfirmed'|'cancelled', ...}`
 * (BRAIN-319 I1/C6). `neverStarted: true` (BRAIN-405) marks an ineligible/unconfirmed outcome that proves the runner never
 * began the job (nothing, or only part of the snapshot, was sent); any other unconfirmed outcome may have a live job behind it, and `mayStillBeRunning: true` (BRAIN-363) marks one whose remote cancel was not confirmed, which the caller must not re-run locally. Never throws for a remote/transport failure -- only a
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
    remoteDepsCacheRootScriptsSafe = false,
    // BRAIN-398: the lane's declared artifact paths/globs and policy (null: none, or the runner cannot return them),
    // and the submitter's own caps on what it will accept back.
    remoteArtifacts = null,
    remoteOmitEscapingSymlinks = false,
    remoteArtifactsOn = 'success',
    artifactLimits,
    // BRAIN-320 S1d: opt-in, from the CLIENT machine's global config
    // (`remoteQueueTimeoutMs`) -- unset (undefined/null) means the header
    // carries no such key at all, byte-identical to before this slice (I6).
    // Rides on both protocol 1 and 2: an older runner simply ignores an
    // unknown field, which is fine, since the whole feature is opt-in.
    queueTimeoutMs = null,
    // BRAIN-431: the submitter's resolved no-progress timeout; the runner applies it instead of its own config
    noProgressTimeoutMs = null,
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
    // BRAIN-436: whether `runner` advertises `remote-withdraw/1`. Without it a dispatch the runner may have accepted can
    // never be proven unstarted, so a lost connection leaves it `mayStillBeRunning` instead of falling back.
    canWithdraw = false,
    // BRAIN-436: `{minQueuedMs, intervalMs, pickTarget, beforeWithdraw, afterWithdraw(action), canWithdraw}` -- take the ticket back from `runner`
    // once it has sat queued for `minQueuedMs` and `pickTarget()` names a better runner; resolves `{outcome:'moved'}`.
    // `rebalance.canWithdraw` overrides the top-level one; with no capability nothing is watched and nothing is withdrawn.
    rebalance = null,
    // BRAIN-436: called once the whole snapshot has been handed to `runner` (it holds the job from here on).
    onSnapshotSent,
  } = opts;
  const withdrawCapable = (rebalance?.canWithdraw ?? canWithdraw) === true;

  const transferDeadlineMs = deadlines.transferMs ?? 5 * 60_000;
  const resultDeadlineMs = deadlines.resultMs ?? 30_000;
  const resultAttempts = deadlines.resultAttempts ?? 3;
  const cancelDeadlineMs = deadlines.cancelMs ?? 15_000;
  // A cancel the runner did not confirm leaves the command possibly alive; the caller keeps the attempt (and reports it) until it is.
  const cancelledOutcome = async () => ({ outcome: 'cancelled', remoteCancelConfirmed: await remoteCancelBestEffort(runner, ticketId, sshBin, cancelDeadlineMs, env) });

  let manifest = prebuiltManifest;
  if (!manifest) {
    try {
      manifest = buildManifest(worktreeRoot, { omitEscapingSymlinks: remoteOmitEscapingSymlinks });
    } catch (err) {
      if (err instanceof RemoteIneligibleError) return { outcome: 'ineligible', reason: err.message, neverStarted: true };
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
    if (remoteDepsCacheRootScriptsSafe === true) header.remoteDepsCacheRootScriptsSafe = true;
  }
  if (Array.isArray(remoteArtifacts) && remoteArtifacts.length > 0) {
    header.remoteArtifacts = remoteArtifacts;
    header.remoteArtifactsOn = remoteArtifactsOn;
  }
  if (Number.isFinite(minCpuCores)) header.minCpuCores = minCpuCores;
  if (Number.isInteger(queueTimeoutMs) && queueTimeoutMs > 0) header.queueTimeoutMs = queueTimeoutMs;
  if (Number.isInteger(noProgressTimeoutMs) && noProgressTimeoutMs >= 0) header.noProgressTimeoutMs = noProgressTimeoutMs;
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
      neverStarted: true,
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
  const dispatchedAt = now();
  const uploadStartedAt = Date.now();
  const pipeResult = await pipeSnapshot(child, snapshotStream, transferDeadlineMs, abortSignal);
  const uploadMs = Date.now() - uploadStartedAt;
  // The ssh session IS the remote job; wait for it to close (unbounded here
  // -- an outer --timeout is the caller's concern), killable by abort.
  if (pipeResult.ok && onSnapshotSent) {
    try {
      await onSnapshotSent();
    } catch {
      // a bookkeeping failure never changes the dispatch
    }
  }
  const expected = { protocol, ticketId, generation, manifestHash: manifest.manifestHash };
  const closed = waitClosed(child, abortSignal);
  let move = null;
  const state = { runner, epoch: 1, attemptedTarget: null, queuedSince: null };
  if (rebalance && withdrawCapable && pipeResult.ok) {
    // One serial watcher beside the open ssh session; it is stopped and JOINED before anything below looks at the outcome.
    const watchAbort = new AbortController();
    let sessionClosed = false;
    const watcher = watchQueuedTicket({
      runner, expected, state, rebalance, signal: watchAbort.signal, abortSignal, now, sleep,
      isClosed: () => sessionClosed,
      fetchDeadlineMs: resultDeadlineMs,
      withdrawDeadlineMs: deadlines.withdrawMs ?? 15_000,
      sshBin, env,
    });
    // The watcher ends the session itself once it has moved the ticket (it is queued: nothing is running behind the ssh).
    const settled = watcher.then((m) => {
      if (m) {
        try {
          child.kill('SIGKILL');
        } catch {
          // already gone
        }
      }
      return m;
    });
    // Only a move ends the wait early; a watcher that stopped for good (admitted, terminal) leaves the session to close on its own.
    try {
      await Promise.race([closed, settled.then((m) => m ?? closed)]);
    } finally {
      sessionClosed = true;
      watchAbort.abort();
      move = await settled.catch(() => null);
      await closed;
    }
  } else {
    await closed;
  }

  if (abortSignal && abortSignal.aborted) {
    return cancelledOutcome();
  }

  if (move) return { outcome: 'moved', from: move.from, to: move.to, queuedMs: move.queuedMs, waitedOnRunnerMs: Math.max(0, move.withdrawnAt - dispatchedAt), neverStarted: true };

  if (!pipeResult.ok) {
    // BRAIN-405: the snapshot was never completely sent, and a runner runs nothing before it has received and verified all of it
    return { outcome: 'unconfirmed', reason: pipeResult.reason, neverStarted: true };
  }

  let record = null;
  let attemptsMade = 0;
  let waitedFrom = null;
  let backoffMs = RESULT_POLL_START_MS;
  let waitExpired = false;
  while (true) {
    if (abortSignal && abortSignal.aborted) {
      return cancelledOutcome();
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
    return cancelledOutcome();
  }

  // BRAIN-363: set when the expiry cancel was not confirmed: whatever the late record turns out to be, the job may still be running.
  let unconfirmedCancel = false;
  let confirmedExpiryCancel = false; // BRAIN-363: the remote command was cancelled (confirmed) before a late record arrived
  if (!record && waitExpired) {
    // BRAIN-363: the caller falls back to running locally, so the runner's copy must not keep running beside it.
    const cancelConfirmed = await remoteCancelBestEffort(runner, ticketId, sshBin, cancelDeadlineMs, env);
    if (abortSignal && abortSignal.aborted) return { outcome: 'cancelled' };
    // The result may have landed while the cancel was in flight; a finished remote run is used, never discarded for a re-run.
    const late = await fetchRemoteResult(runner, ticketId, sshBin, resultDeadlineMs, env);
    if (abortSignal && abortSignal.aborted) return { outcome: 'cancelled' };
    if (late && !late.missing) {
      record = late;
      unconfirmedCancel = !cancelConfirmed;
      confirmedExpiryCancel = cancelConfirmed;
    } else if (cancelConfirmed) {
      return {
        outcome: 'unconfirmed',
        reason: `remote job still running after waiting ${resultWaitMs}ms for its result; remote copy cancelled`,
      };
    } else {
      // An unconfirmed cancel leaves the remote job possibly alive: the caller must not run it a second time.
      return {
        outcome: 'unconfirmed',
        reason: `remote job on ${runner.name} still running after waiting ${resultWaitMs}ms for its result; remote cancel could not be confirmed, so it may still be running and was not re-run`,
        mayStillBeRunning: true,
      };
    }
  }
  if (!record) {
    // BRAIN-437: ssh dropped after a dispatch the runner may have accepted, and no result could be fetched. Falling back to
    // local here can run the job twice, so the runner is asked to give up the ticket; only a `withdrawn` answer proves it never
    // ran. Anything else (or a runner that cannot withdraw) leaves it possibly running, which the caller must not re-run.
    const withdraw = withdrawCapable ? await remoteWithdraw(runner, ticketId, { sshBin, deadlineMs: deadlines.withdrawMs ?? 15_000, env }) : { action: 'unknown' };
    if (abortSignal && abortSignal.aborted) {
      // The cancel arrived while the withdraw was in flight; unless the runner gave the ticket up, it must still be told.
      return cancelledOutcome();
    }
    if (withdraw.action === 'withdrawn' && state.attemptedTarget) {
      // The move's own withdraw reply was lost and the session closed: this retry is the proof, and the chosen target is where it goes.
      return { outcome: 'moved', from: runner, to: state.attemptedTarget, queuedMs: state.queuedSince === null ? 0 : now() - state.queuedSince, waitedOnRunnerMs: Math.max(0, now() - dispatchedAt), neverStarted: true };
    }
    if (withdraw.action === 'withdrawn') return { outcome: 'unconfirmed', reason: 'result not available after retries; the runner confirmed it never started the ticket', neverStarted: true };
    return {
      outcome: 'unconfirmed',
      reason: `result not available after retries from ${runner.name}; the dispatch may have been accepted and ${withdrawCapable ? `could not be withdrawn (${withdraw.action})` : 'the runner cannot withdraw it'}, so it may still be running and was not re-run`,
      mayStillBeRunning: true,
    };
  }

  // BRAIN-320 S1c: classification (bound? kind? green?) all happens in ONE
  // place, `classifyRemoteResult`, shared with `isGreen` -- see its own doc
  // comment for the phase rules (I7: green only from phase 'command').
  const classification = classifyRemoteResult(record, expected);
  if (classification.outcome === 'unconfirmed') {
    // A withdraw whose reply was lost and whose session closed before the watcher saw it: the runner's own bound record
    // proves the withdrawal, and the target the watcher had chosen is where the ticket goes.
    if (state.attemptedTarget && isWithdrawnRecord(record, expected)) {
      return { outcome: 'moved', from: runner, to: state.attemptedTarget, queuedMs: state.queuedSince === null ? 0 : now() - state.queuedSince, waitedOnRunnerMs: Math.max(0, now() - dispatchedAt), neverStarted: true };
    }
    // Only a record that BINDS to this ticket and says nothing ran proves it never started. Any other unusable record leaves
    // the job possibly running (a mismatched or contradictory record says nothing about the runner), and so does an
    // unconfirmed expiry cancel: the caller must not run it a second time.
    if (confirmedExpiryCancel) return { outcome: 'unconfirmed', reason: classification.reason };
    if (provesNeverStarted(record, expected) && !unconfirmedCancel) return { outcome: 'unconfirmed', reason: classification.reason, neverStarted: true };
    return { outcome: 'unconfirmed', reason: classification.reason, mayStillBeRunning: true };
  }
  const confirmed = { outcome: 'confirmed', result: record, exitCode: classification.exitCode, phase: classification.phase, uploadMs };
  // BRAIN-398: only a runner that stored files for this result is asked for them; a failure here is reported, never
  // allowed to change the confirmed outcome.
  if (header.remoteArtifacts && record.artifacts?.ok === true && record.artifacts.count > 0) {
    const artifactFetchStartedAt = Date.now();
    confirmed.artifacts = await fetchRemoteArtifacts(runner, ticketId, {
      patterns: remoteArtifacts,
      limits: artifactLimits ?? artifactLimitsOf(DEFAULT_GLOBAL_CONFIG),
      sshBin,
      deadlineMs: deadlines.artifactsMs ?? 2 * 60_000,
      env,
    });
    confirmed.artifactReturnMs = Date.now() - artifactFetchStartedAt;
  }
  return confirmed;
}
