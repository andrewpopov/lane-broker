import fs from 'node:fs';
import path from 'node:path';
import { pullConfigured } from './config.js';
import { lifecycleVerdict } from './pull-check.js';
import { pullServices } from './pull-services.js';
import { QUEUE_PROBE } from './queue-client.js';
import { atomicWriteJson, paths, readJsonSafe } from './state.js';

export const PULL_READY_TTL_MS = 5 * 60_000;
export const PULL_PROBE_DEADLINE_MS = 3_000;

const readinessFile = (root) => path.join(paths(root).pull, 'readiness.json');
function fileStamp(file) {
  try {
    return `${file}:${fs.statSync(file).mtimeMs}`;
  } catch {
    return `${file}:missing`;
  }
}

const connectionKey = (cfg) => {
  const { endpoint, region, snapBucket, outBucket, credentialsEnvFile } = cfg.lanePull.rustfs;
  return JSON.stringify([cfg.laneQueueUrl, endpoint, region, snapBucket, outBucket, ...[cfg.lanePull.passwordFile, cfg.lanePull.caFile, credentialsEnvFile].map(fileStamp)]);
};

async function readinessOf(cfg, services) {
  if (!services.available) return { ready: false, reason: services.reason };
  const { refusals } = await lifecycleVerdict({ cfg, s3: services.s3, deadlineMs: PULL_PROBE_DEADLINE_MS });
  if (refusals.length > 0) return { ready: false, reason: refusals[0] };
  try {
    await services.queue.queueRef(QUEUE_PROBE);
  } catch (err) {
    return { ready: false, reason: `queue login failed: ${err.message}` };
  }
  return { ready: true };
}

export function createPullReadiness({ root, ttlMs = PULL_READY_TTL_MS, now = Date.now }) {
  return async (cfg, services) => {
    const key = connectionKey(cfg);
    const stored = readJsonSafe(readinessFile(root));
    const age = now() - (stored?.at ?? -Infinity);
    if (stored?.key === key && age >= 0 && age < ttlMs) return stored.verdict;
    const verdict = await readinessOf(cfg, services);
    fs.mkdirSync(paths(root).pull, { recursive: true });
    atomicWriteJson(readinessFile(root), { key, at: now(), verdict });
    return verdict;
  };
}

// a chosen push runner returns before any pull config, file or network is read; an unreachable pull side is `legacy`, never a half-placed enqueue
export async function placeForPull({
  root, pushChosen, cfg, repoKey, requiresTags = [], locallyIneligible, couldRunLocally, servicesFor = pullServices, readiness = createPullReadiness({ root }),
}) {
  if (pushChosen) return { route: 'push' };
  if (!pullConfigured(cfg)) return { route: 'legacy' };
  const services = servicesFor(cfg);
  const verdict = await readiness(cfg, services);
  if (!verdict.ready) return { route: 'legacy', reason: `pull dispatch unavailable: ${verdict.reason}` };
  let placeable;
  try {
    placeable = await services.queue.placeable(repoKey, requiresTags, QUEUE_PROBE);
  } catch (err) {
    return { route: 'legacy', reason: `pull dispatch unavailable: ld_placeable failed: ${err.message}` };
  }
  if (placeable === 0) return { route: 'refuse', reason: 'no queue principal is eligible for this ticket' };
  if (locallyIneligible()) return { route: 'pull', services };
  return (await couldRunLocally()) ? { route: 'local' } : { route: 'pull', services };
}
