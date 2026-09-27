import { spawn } from 'node:child_process';
import { buildManifest, RemoteIneligibleError } from './remote-manifest.js';
import { encodeSnapshot } from './remote-stream.js';

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
 * Probe every configured runner SEQUENTIALLY, in preference order (BRAIN-319
 * C8), returning the first usable one -- exit 0, `protocol === 1`,
 * `paused === false`, `queued === 0` -- as `{runner, probe, skipped}` (any
 * earlier candidates passed over on the way to it), or `{runner: null,
 * skipped}` naming why every candidate was passed over. `opts.deadlineMs`
 * (default 6000) bounds each individual probe; a probe that hangs past it is
 * SIGKILLed and counted as a skip, never as a hang for the whole selection.
 */
export async function selectRunner(runners, opts = {}) {
  const deadlineMs = opts.deadlineMs ?? 6000;
  const sshBin = opts.sshBin ?? 'ssh';
  const { env } = opts;
  const skipped = [];
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
    if (probe.paused) {
      skipped.push({ name: runner.name, reason: 'paused' });
      continue;
    }
    if (probe.queued !== 0) {
      skipped.push({ name: runner.name, reason: `queued: ${probe.queued}` });
      continue;
    }
    return { runner, probe, skipped };
  }
  return { runner: null, skipped };
}

/** True iff `record` is bound to `expected` (BRAIN-319 C6: ticketId, generation, manifestHash all match). */
function matchesTicket(record, expected) {
  return (
    !!record &&
    record.protocol === 1 &&
    record.ticketId === expected.ticketId &&
    record.generation === expected.generation &&
    record.manifestHash === expected.manifestHash
  );
}

/**
 * Pure "is this a passing green result" predicate: bound to `expected` AND
 * `kind === 'completed'` with a real integer zero exit and no signal. The
 * single definition of green -- `dispatchRemote` reuses `matchesTicket`
 * (the same binding half of this predicate) for its own confirm/unconfirm
 * decision, so there is exactly one place that decides whether a result
 * belongs to a given ticket.
 */
export function isGreen(result, expected) {
  return (
    matchesTicket(result, expected) &&
    result.kind === 'completed' &&
    Number.isInteger(result.exit) &&
    result.exit === 0 &&
    !result.signal
  );
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
 * `remote-cancel` on abort.
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
    memoryBytes,
    argv,
    ticketId,
    generation,
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

  const header = { ticketId, generation, repoKey, lane, weight, cpuCores, memoryBytes, argv, relCwd };
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

  const expected = { ticketId, generation, manifestHash: manifest.manifestHash };
  let record = null;
  for (let attempt = 0; attempt < resultAttempts; attempt += 1) {
    if (abortSignal && abortSignal.aborted) {
      await remoteCancelBestEffort(runner, ticketId, sshBin, cancelDeadlineMs, env);
      return { outcome: 'cancelled' };
    }
    const fetched = await fetchRemoteResult(runner, ticketId, sshBin, resultDeadlineMs, env);
    if (fetched && !fetched.missing) {
      record = fetched;
      break;
    }
  }

  if (abortSignal && abortSignal.aborted) {
    await remoteCancelBestEffort(runner, ticketId, sshBin, cancelDeadlineMs, env);
    return { outcome: 'cancelled' };
  }

  if (!record) return { outcome: 'unconfirmed', reason: 'result not available after retries' };
  if (!matchesTicket(record, expected)) {
    return { outcome: 'unconfirmed', reason: 'result did not bind to this ticket (protocol/ticketId/generation/manifestHash mismatch)' };
  }

  if (record.kind === 'completed') {
    const exitCode = record.signal ? 1 : record.exit;
    if (!Number.isInteger(exitCode)) return { outcome: 'unconfirmed', reason: 'completed result has a non-integer exit' };
    return { outcome: 'confirmed', result: record, exitCode };
  }
  if (record.kind === 'refused') {
    if (!Number.isInteger(record.exit)) return { outcome: 'unconfirmed', reason: 'refused result has a non-integer exit' };
    return { outcome: 'confirmed', result: record, exitCode: record.exit };
  }
  if (record.kind === 'cancelled') {
    return { outcome: 'confirmed', result: record, exitCode: 130 };
  }
  // rejected / unfinished / any other kind: no exit code this client may trust.
  return {
    outcome: 'unconfirmed',
    reason: record.reason ? `${record.kind}: ${record.reason}` : `unusable result kind: ${record.kind}`,
  };
}
