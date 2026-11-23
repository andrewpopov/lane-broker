import fs from 'node:fs';
import path from 'node:path';
import { pullLocalFallbackEnabled } from './config.js';
import { isQueueUnreachable } from './queue-client.js';
import { atomicWriteJson, paths, readJsonSafe, withLock } from './state.js';

export const QUEUE_RECOVERY_SPACING_MS = 30_000;
const LOCK_WAIT_MS = 2_000;

const healthFile = (root) => path.join(paths(root).pull, 'queue-health.json');
export const queueHealthLock = (root) => path.join(paths(root).pull, 'queue-health.lock');

const EMPTY = { downSince: null, lastFailureAt: null, okAt: null };

export function readQueueHealth(root) {
  const stored = readJsonSafe(healthFile(root));
  return { ...EMPTY, ...(stored && typeof stored === 'object' ? stored : {}) };
}

export const queueDownSince = (root) => readQueueHealth(root).downSince;

export const localFallbackActive = (root, cfg, ticket) => pullLocalFallbackEnabled(cfg, ticket) && queueDownSince(root) !== null;

function write(root, health) {
  fs.mkdirSync(paths(root).pull, { recursive: true });
  atomicWriteJson(healthFile(root), health);
}

function apply(root, ok, now) {
  const health = readQueueHealth(root);
  if (!ok) {
    write(root, { downSince: health.downSince ?? now, lastFailureAt: now, okAt: null });
    return;
  }
  if (health.downSince === null) return;
  if (health.okAt === null) write(root, { ...health, okAt: now });
  else if (now - health.okAt >= QUEUE_RECOVERY_SPACING_MS) write(root, { downSince: null, lastFailureAt: health.lastFailureAt, okAt: null });
}

// a leaf lock, held only for this read-modify-write; same group as the ticket lock a queue call may run under
export function recordQueueCall(root, ok, now = Date.now()) {
  fs.mkdirSync(paths(root).pull, { recursive: true });
  return withLock(root, () => apply(root, ok, now), { lockPath: queueHealthLock(root), group: 'broker', timeoutMs: LOCK_WAIT_MS });
}

const recordQuietly = async (root, ok, now) => {
  try {
    await recordQueueCall(root, ok, now());
  } catch {
    return;
  }
};

// a server error is an answer (the queue is reachable); an S3 failure never passes through here, so it cannot mark the queue down
export function trackQueueHealth(queue, root, { now = Date.now } = {}) {
  return Object.fromEntries(Object.entries(queue).map(([name, call]) => [name, async (...args) => {
    try {
      const answer = await call(...args);
      await recordQuietly(root, true, now);
      return answer;
    } catch (err) {
      if (isQueueUnreachable(err)) await recordQuietly(root, false, now);
      else if (err?.name === 'QueueError') await recordQuietly(root, true, now);
      throw err;
    }
  }]));
}
