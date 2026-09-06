import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { paths, ensureStateDirs, appendHistory, atomicWriteJson } from './state.js';
import { enqueue, tryStart, dequeueSync } from './scheduler.js';
import { readLease, writeLease, removeLease, isGroupAlive, processStartTime } from './lease.js';
import { loadGlobalConfig } from './config.js';

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
    this.stream = fs.createWriteStream(logPath, { flags });
    this.stream.on('error', (err) => {
      // Disk-full, a symlink refusal, or any other stream failure must never
      // crash the supervisor or the lane it is running.
      this.failed = true;
      this.error = err;
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

async function main() {
  const ticket = readTicketFromEnv();
  const root = ensureStateDirs().root;
  const globalCfg = loadGlobalConfig();
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
    if (cancelledBeforeStart || cancelRequested(root, ticket.id)) {
      dequeueSync(root, ticket.id);
      clearCancelRequest(root, ticket.id);
      process.exit(0);
    }
    started = await tryStart(root, enriched, globalCfg);
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
  const [cmd, ...args] = ticket.cmd;
  const child = spawn(cmd, args, {
    cwd: ticket.cwd,
    detached: true,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, LANE_BROKER_LEASE: ticket.id, LANE_BROKER_KEY: ticket.key },
  });

  const logWriter = new CappedLogWriter(ticket.logPath);
  child.stdout.on('data', (c) => {
    if (!logWriter.write(c)) {
      child.stdout.pause();
      logWriter.stream.once('drain', () => child.stdout.resume());
    }
  });
  child.stderr.on('data', (c) => {
    if (!logWriter.write(c)) {
      child.stderr.pause();
      logWriter.stream.once('drain', () => child.stderr.resume());
    }
  });

  writeLease(root, { ...started.lease, childPgid: child.pid, heartbeatAt: Date.now(), startedAt });

  let finished = false;
  let cancelling = false;
  let killPromise = null;

  const heartbeat = setInterval(() => {
    if (finished) return;
    const lease = readLease(root, ticket.id);
    if (lease) writeLease(root, { ...lease, heartbeatAt: Date.now() });
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
    atomicWriteJson(ticket.resultPath, result);
    appendHistory(root, {
      id: ticket.id,
      key: ticket.key,
      repo: ticket.repoId,
      lane: ticket.lane,
      weight: ticket.weight,
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

main().catch((err) => {
  process.stderr.write(`lane-broker supervisor error: ${err.stack || err.message}\n`);
  process.exit(1);
});
