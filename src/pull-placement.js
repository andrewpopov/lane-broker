import fs from 'node:fs';
import path from 'node:path';
import { pullConfigured } from './config.js';
import { lifecycleVerdict } from './pull-check.js';
import { pullServices } from './pull-services.js';
import { PLACEMENT_USAGE_EXIT, placementRefusal } from './placement.js';
import { QUEUE_PROBE } from './queue-client.js';
import { readQueueHealth } from './queue-health.js';
import { atomicWriteJson, paths, readJsonSafe } from './state.js';

export const PULL_READY_TTL_MS = 5 * 60_000;
export const PULL_PROBE_DEADLINE_MS = 3_000;
export const PULL_DOWN_NEGATIVE_TTL_MS = 30_000;

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
    const health = cfg.lanePull?.localFallbackWhenUnreachable === true ? readQueueHealth(root) : null;
    const failedSince = health !== null && stored?.verdict?.ready === true && health.lastFailureAt !== null && health.lastFailureAt > stored.at;
    const ttl = health !== null && stored?.verdict?.ready === false && health.downSince !== null ? Math.min(ttlMs, PULL_DOWN_NEGATIVE_TTL_MS) : ttlMs;
    if (stored?.key === key && age >= 0 && age < ttl && !failedSince) return stored.verdict;
    const probedAt = now();
    const verdict = await readinessOf(cfg, services);
    fs.mkdirSync(paths(root).pull, { recursive: true });
    atomicWriteJson(readinessFile(root), { key, at: probedAt, verdict });
    return verdict;
  };
}

// a chosen push runner returns before any pull config, file or network is read; an unreachable pull side is `legacy`, never a half-placed enqueue
// a placed ticket (`placement.constrained`) is never routed local or push: the queue refuses it (`refuse`, with the exit its answer calls for) or it is queued
export async function placeForPull({
  root, pushChosen, cfg, repoKey, requiresTags = [], placement, aliasesApplied = [], locallyIneligible, couldRunLocally, servicesFor = pullServices, readiness = createPullReadiness({ root }),
}) {
  if (pushChosen) return { route: 'push' };
  if (!pullConfigured(cfg)) return { route: 'legacy' };
  const services = servicesFor(cfg, root);
  const verdict = await readiness(cfg, services);
  if (!verdict.ready) return { route: 'legacy', reason: `pull dispatch unavailable: ${verdict.reason}` };
  const constrained = placement?.constrained === true;
  if (constrained && aliasesApplied.length > 0) {
    const shadowed = await shadowedAlias(services, aliasesApplied);
    if (shadowed !== null) return shadowed;
  }
  let placeable;
  try {
    placeable = await services.queue.placeable(repoKey, constrained ? placement.requiresTags : requiresTags, QUEUE_PROBE, placement);
  } catch (err) {
    const refusal = constrained ? placementRefusal(err) : null;
    if (refusal !== null) return { route: 'refuse', ...refusal };
    return { route: 'legacy', reason: `pull dispatch unavailable: ld_placeable failed: ${err.message}` };
  }
  if (placeable === 0) return { route: 'refuse', reason: constrained ? 'no queue principal satisfies the requested placement' : 'no queue principal is eligible for this ticket' };
  if (constrained) return { route: 'pull', services };
  if (locallyIneligible()) return { route: 'pull', services };
  return (await couldRunLocally()) ? { route: 'local' } : { route: 'pull', services };
}

// an alias that is itself a runner id would silently send a request for one real host to another
export async function shadowedAlias(services, aliasesApplied) {
  let known;
  try {
    known = await services.queue.knownHosts(QUEUE_PROBE);
  } catch (err) {
    const refusal = placementRefusal(err);
    return refusal === null ? { route: 'legacy', reason: `pull dispatch unavailable: ld_known_hosts failed: ${err.message}` } : { route: 'refuse', ...refusal };
  }
  const shadow = aliasesApplied.find((alias) => known.includes(alias));
  return shadow === undefined ? null : { route: 'refuse', exitCode: PLACEMENT_USAGE_EXIT, reason: `hostAliases key "${shadow}" is itself a runner id the queue knows; an alias must never map one real runner to another` };
}
