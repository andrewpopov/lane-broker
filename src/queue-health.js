import fs from 'node:fs';
import path from 'node:path';
import { pullLocalFallbackEnabled } from './config.js';
import { isQueueUnreachable } from './queue-client.js';
import { atomicWriteJson, paths, readJsonSafe } from './state.js';

export const QUEUE_RECOVERY_SPACING_MS = 30_000;

const healthFile = (root) => path.join(paths(root).pull, 'queue-health.json');

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

// last writer wins and nothing is locked: a queue call may run under the ticket lock, and no other lock group may be taken beneath it
export function recordQueueCall(root, ok, now = Date.now()) {
  const health = readQueueHealth(root);
  if (!ok) {
    write(root, { downSince: health.downSince ?? now, lastFailureAt: now, okAt: null });
    return;
  }
  if (health.downSince === null) return;
  if (health.okAt === null) write(root, { ...health, okAt: now });
  else if (now - health.okAt >= QUEUE_RECOVERY_SPACING_MS) write(root, { downSince: null, lastFailureAt: health.lastFailureAt, okAt: null });
}

const recordQuietly = (root, ok, now) => {
  try {
    recordQueueCall(root, ok, now());
  } catch {
    return;
  }
};

// a server error is an answer (the queue is reachable); an S3 failure never passes through here, so it cannot mark the queue down
export function trackQueueHealth(queue, root, { now = Date.now } = {}) {
  return Object.fromEntries(Object.entries(queue).map(([name, call]) => [name, async (...args) => {
    try {
      const answer = await call(...args);
      recordQuietly(root, true, now);
      return answer;
    } catch (err) {
      if (isQueueUnreachable(err)) recordQuietly(root, false, now);
      else if (err?.name === 'QueueError') recordQuietly(root, true, now);
      throw err;
    }
  }]));
}
