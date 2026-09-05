import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, readJsonSafe, withLock, bootId } from './state.js';
import { sampleAndUpdateGate } from './load.js';
import { listLeases, reapAll, writeLease, LEASE_STATE } from './lease.js';

function nextSeq(root) {
  const file = paths(root).seq;
  let n = 0;
  const cur = readJsonSafe(file);
  if (cur && Number.isFinite(cur.n)) n = cur.n;
  n += 1;
  atomicWriteJson(file, { n });
  return n;
}

function queueFile(root, seq, id) {
  return path.join(paths(root).queue, `${String(seq).padStart(12, '0')}-${id}.json`);
}

export function listQueue(root) {
  const dir = paths(root).queue;
  let names;
  try {
    names = fs.readdirSync(dir).sort();
  } catch {
    return [];
  }
  return names.filter((n) => n.endsWith('.json')).map((n) => readJsonSafe(path.join(dir, n)));
}

function findQueueFile(root, id) {
  const dir = paths(root).queue;
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return null;
  }
  const match = names.find((n) => n.endsWith(`-${id}.json`));
  return match ? path.join(dir, match) : null;
}

/** Enqueue a ticket at the tail of the global FIFO. Caller must NOT hold the lock. */
export async function enqueue(root, ticket) {
  return withLock(root, () => {
    const seq = nextSeq(root);
    const record = { ...ticket, seq, createdAt: ticket.createdAt || Date.now() };
    atomicWriteJson(queueFile(root, seq, ticket.id), record);
    return record;
  });
}

export function dequeueSync(root, id) {
  const file = findQueueFile(root, id);
  if (file) {
    try {
      fs.unlinkSync(file);
    } catch {
      // already gone
    }
  }
}

function conflicts(ticket, lease) {
  if (lease.key === ticket.key) return true;
  return Array.isArray(ticket.conflicts) && ticket.conflicts.includes(lease.key);
}

/**
 * The single atomic transaction: a ticket starts only when it is the FIFO
 * head AND has no conflicting RUNNING lease AND fits capacity AND the load
 * gate is open AND the broker is not paused. No backfill behind the head.
 */
export async function tryStart(root, ticket, globalCfg, loadSampler) {
  return withLock(root, () => {
    reapAll(root, bootId());
    const queue = listQueue(root);
    const position = queue.findIndex((t) => t && t.id === ticket.id);
    if (position !== 0) {
      return { started: false, reason: 'not-head', position: position === -1 ? null : position + 1, queueLength: queue.length };
    }
    if (fs.existsSync(paths(root).pause)) {
      const reason = fs.readFileSync(paths(root).pause, 'utf8').trim();
      return { started: false, reason: 'paused', pauseReason: reason };
    }
    const gate = sampleAndUpdateGate(root, globalCfg, loadSampler);
    if (gate.closed) {
      return { started: false, reason: 'load-gate-closed', load: gate.lastLoad };
    }
    const running = listLeases(root).filter((l) => l.state === LEASE_STATE.RUNNING);
    const blocker = running.find((l) => conflicts(ticket, l));
    if (blocker) {
      return { started: false, reason: 'conflict', with: blocker.id, key: blocker.key };
    }
    const runningWeight = running.reduce((sum, l) => sum + (l.weight || 0), 0);
    if (runningWeight + ticket.weight > globalCfg.capacity) {
      return { started: false, reason: 'capacity', runningWeight, capacity: globalCfg.capacity };
    }
    dequeueSync(root, ticket.id);
    const lease = {
      id: ticket.id,
      key: ticket.key,
      bootId: bootId(),
      supervisorPid: ticket.supervisorPid,
      supervisorStart: ticket.supervisorStart,
      childPgid: null,
      heartbeatAt: Date.now(),
      cwd: ticket.cwd,
      cmd: ticket.cmd,
      weight: ticket.weight,
      logPath: ticket.logPath,
      resultPath: ticket.resultPath,
      state: LEASE_STATE.RUNNING,
    };
    writeLease(root, lease);
    return { started: true, lease };
  });
}
