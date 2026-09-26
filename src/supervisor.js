import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { paths, ensureStateDirs, appendHistory, atomicWriteJson, readJsonSafe } from './state.js';
import { enqueue, tryStart, dequeueSync } from './scheduler.js';
import { readLease, writeLease, removeLease, isGroupAlive, processStartTime } from './lease.js';
import { observedGroupCpuCores, observedGroupMemoryBytes } from './cpu.js';
import { reloadGlobalConfig } from './config.js';

const LOG_CAP_BYTES = 50 * 1024 * 1024;
const CANCEL_GRACE_MS = 10_000;

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
  return fs.existsSync(`${paths(root).cancel}/${id}`);
}

function clearCancelRequest(root, id) {
  try {
    fs.unlinkSync(`${paths(root).cancel}/${id}`);
  } catch {
    // none pending
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
export function childEnv(ticket, baseEnv = process.env) {
  const env = { ...baseEnv, LANE_BROKER_LEASE: ticket.id, LANE_BROKER_KEY: ticket.key };
  const cpuCores = ticket.resources?.cpuCores;
  const memoryBytes = ticket.resources?.memoryBytes;
  if (Number.isFinite(cpuCores) && cpuCores > 0) env.LANE_BROKER_CPU_CORES = String(cpuCores);
  else delete env.LANE_BROKER_CPU_CORES;
  if (Number.isFinite(memoryBytes) && memoryBytes > 0) env.LANE_BROKER_MEMORY_BYTES = String(memoryBytes);
  else delete env.LANE_BROKER_MEMORY_BYTES;
  return env;
}

export function applyHeartbeatObservation(lease, observed, now = Date.now(), observedMemoryBytes = null) {
  const update = { ...lease, heartbeatAt: now };
  if (Number.isFinite(observed)) {
    update.observedCpuCores = observed;
    update.observedAt = now;
  }
  if (Number.isFinite(observedMemoryBytes)) {
    update.observedMemoryBytes = observedMemoryBytes;
    update.observedAt = now;
  }
  return update;
}

async function main() {
  const ticket = readTicketFromEnv();
  const root = ensureStateDirs().root;
  // reloadGlobalConfig(undefined), not loadGlobalConfig(): a supervisor that
  // launches while config.json is missing, mid-write, or invalid must start
  // on defaults rather than crash before it ever reaches the resilient
  // polling loop below — the exact torn-read window this fix exists for.
  let globalCfg = reloadGlobalConfig(undefined, { onError: (err) => recordConfigReloadError(root, err) });
  const supervisorStart = processStartTime(process.pid);
  const enriched = {
    ...ticket,
    supervisorPid: process.pid,
    supervisorStart: supervisorStart === undefined ? null : supervisorStart,
  };

  let cancelledBeforeStart = false;
  process.on('SIGTERM', () => {
    cancelledBeforeStart = true;
  });
  process.on('SIGINT', () => {
    cancelledBeforeStart = true;
  });

  await enqueue(root, enriched);

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
      dequeueSync(root, ticket.id);
      clearCancelRequest(root, ticket.id);
      process.exit(0);
    }
    // tryStart re-reads the config again inside its lock (BRAIN-182): the
    // outer reload above can be superseded by an edit that lands in the gap
    // between this poll's reload and the lock actually being granted. Reuse
    // the same fallback/warning handling so a reload failure inside the lock
    // degrades exactly like one out here.
    started = await tryStart(root, enriched, globalCfg, undefined, undefined, () => {
      globalCfg = reloadGlobalConfig(globalCfg, {
        onError: (err) => {
          reloadFailed = true;
          recordConfigReloadError(root, err);
        },
      });
      return globalCfg;
    });
    if (started.started) break;
    if (started.reason === 'not-head' && started.position === null) {
      // Our own ticket is no longer in the queue without ever having
      // started: it was cancelled out from under us. Without this, a queued
      // cancel leaves this supervisor polling forever.
      process.exit(0);
    }
    await sleep(globalCfg.sampleMs);
  }

  const startedAt = Date.now();
  const [cmd, args] = resolveNicedSpawn(ticket);
  const child = spawn(cmd, args, {
    cwd: ticket.cwd,
    detached: true,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: childEnv(ticket),
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
  child.stdout.on('data', (c) => {
    const loggedOk = logWriter.write(c);
    const forwardedOk = stdoutForward ? stdoutForward.write(c) : true;
    if (!loggedOk) logWriter.pauseUntilDrain(stdoutGate);
    if (!forwardedOk) stdoutForward.pauseUntilDrain(stdoutGate);
  });
  child.stderr.on('data', (c) => {
    const loggedOk = logWriter.write(c);
    const forwardedOk = stderrForward ? stderrForward.write(c) : true;
    if (!loggedOk) logWriter.pauseUntilDrain(stderrGate);
    if (!forwardedOk) stderrForward.pauseUntilDrain(stderrGate);
  });

  writeLease(root, { ...started.lease, childPgid: child.pid, heartbeatAt: Date.now(), startedAt });

  let finished = false;
  let cancelling = false;
  let killPromise = null;

  const heartbeat = setInterval(() => {
    if (finished) return;
    const lease = readLease(root, ticket.id);
    if (lease) {
      const observed = child.pid ? observedGroupCpuCores(child.pid) : null;
      const observedMemory = child.pid ? observedGroupMemoryBytes(child.pid) : null;
      writeLease(root, applyHeartbeatObservation(lease, observed, Date.now(), observedMemory));
    }
    if (!cancelling && cancelRequested(root, ticket.id)) {
      cancelling = true;
      clearCancelRequest(root, ticket.id);
      killPromise = killGroup(child.pid);
    }
  }, globalCfg.sampleMs);

  const onCancelSignal = () => {
    if (!cancelling && !finished) {
      cancelling = true;
      killPromise = killGroup(child.pid);
    }
  };
  process.on('SIGTERM', onCancelSignal);
  process.on('SIGINT', onCancelSignal);

  async function finalizeAndExit(result, exitCode) {
    finished = true;
    clearInterval(heartbeat);
    // killGroup must be awaited on every path: if the group leader exited
    // but a TERM-resistant descendant is still alive, we must not write the
    // result / release the lease until the whole group is confirmed gone.
    if (killPromise) await killPromise;
    await logWriter.finish();
    // Flush the forward pipes before exiting (BRAIN-308) -- process.exit()
    // below can otherwise drop the async tail of a macOS pipe write. Skipped
    // once a writer has failed (dead caller): draining a pipe nobody is
    // reading would just wait out its own bounded timeout for nothing.
    if (stdoutForward) await stdoutForward.drain();
    if (stderrForward) await stderrForward.drain();
    atomicWriteJson(ticket.resultPath, result);
    appendHistory(root, {
      id: ticket.id,
      key: ticket.key,
      repo: ticket.repoId,
      lane: ticket.lane,
      weight: ticket.weight,
      resources: ticket.resources,
      ...result,
    });
    removeLease(root, ticket.id); // release always comes last
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
    finalizeAndExit(result, 0);
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
