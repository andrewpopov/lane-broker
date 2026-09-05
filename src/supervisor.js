import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { paths, ensureStateDirs, appendHistory, atomicWriteJson } from './state.js';
import { enqueue, tryStart, dequeueSync } from './scheduler.js';
import { readLease, writeLease, removeLease, isGroupAlive, processStartTime, LEASE_STATE } from './lease.js';
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
    this.stream = fs.createWriteStream(logPath, { flags: 'a' });
    this.bytes = 0;
    this.truncated = false;
  }

  write(chunk) {
    if (this.truncated) return;
    if (this.bytes + chunk.length > LOG_CAP_BYTES) {
      const remaining = LOG_CAP_BYTES - this.bytes;
      if (remaining > 0) this.stream.write(chunk.subarray(0, remaining));
      this.stream.write('\n[lane-broker] log truncated at 50MB\n');
      this.truncated = true;
      return;
    }
    this.bytes += chunk.length;
    this.stream.write(chunk);
  }

  close() {
    this.stream.end();
  }
}

async function main() {
  const ticket = readTicketFromEnv();
  const root = ensureStateDirs(process.env.LANE_BROKER_STATE_DIR || undefined).root;
  const globalCfg = loadGlobalConfig();
  const supervisorStart = processStartTime(process.pid);
  const enriched = {
    ...ticket,
    supervisorPid: process.pid,
    supervisorStart,
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
  child.stdout.on('data', (c) => logWriter.write(c));
  child.stderr.on('data', (c) => logWriter.write(c));

  writeLease(root, { ...started.lease, childPgid: child.pid, heartbeatAt: Date.now() });

  let finished = false;
  let cancelling = false;

  const heartbeat = setInterval(() => {
    if (finished) return;
    const lease = readLease(root, ticket.id);
    if (lease) writeLease(root, { ...lease, heartbeatAt: Date.now() });
    if (!cancelling && cancelRequested(root, ticket.id)) {
      cancelling = true;
      clearCancelRequest(root, ticket.id);
      killGroup(child.pid);
    }
  }, globalCfg.sampleMs);

  process.on('SIGTERM', () => {
    if (!cancelling && !finished) {
      cancelling = true;
      killGroup(child.pid);
    }
  });
  process.on('SIGINT', () => {
    if (!cancelling && !finished) {
      cancelling = true;
      killGroup(child.pid);
    }
  });

  child.on('exit', (code, signal) => {
    finished = true;
    clearInterval(heartbeat);
    logWriter.close();
    const endedAt = Date.now();
    const result = {
      id: ticket.id,
      exit: code,
      signal,
      startedAt,
      endedAt,
      waitedMs: startedAt - enriched.createdAt || 0,
    };
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
    process.exit(0);
  });
}

main().catch((err) => {
  process.stderr.write(`lane-broker supervisor error: ${err.stack || err.message}\n`);
  process.exit(1);
});
