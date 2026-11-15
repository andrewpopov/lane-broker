import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { drainBlocksIntake, executionIdOf, readDrainMarker, readRemoteDrain, stateHome, ensureStateDirs } from '../state.js';
import { runnerAvailability } from '../runner-schedule.js';
import { closedGateReason } from '../device-gates.js';
import { loadGlobalConfig } from '../config.js';
import { collectStatus } from '../status.js';
import { listLeases } from '../lease.js';
import { HELD_STATES, listQueue } from '../scheduler.js';
import { clampQueuedWeight, queuedWeightLimit } from '../exclusive.js';
import { detectResourceCapacity, cpuBudgetCores, effectiveWeightCapacity } from '../resources.js';
import { PROBE_LOCK_WAIT_MS, probeCpuHeadroom, probeHeadroom } from '../remote-runner.js';
import { clearPendingClaim, newExecutionRecord, readPendingClaim, writePendingClaim } from './store.js';

export const MAX_FREE_MEM = Number.MAX_SAFE_INTEGER;

export const isDraining = (stateDir = stateHome()) => readRemoteDrain(stateDir) !== null || drainBlocksIntake(readDrainMarker(stateDir));

/** Why this runner takes no new claim now, or null: a drain marker, outside its schedule window, too close to the window closing, or a closed device gate. */
export function claimBlock({ schedule, stopClaimingBeforeCloseMs, now = Date.now(), draining = isDraining, deviceGates }) {
  if (draining()) return 'draining';
  const { available, closesAt } = runnerAvailability(schedule, now);
  if (!available) return 'outside-schedule';
  if (closesAt !== null && closesAt - now <= stopClaimingBeforeCloseMs) return 'closing-soon';
  return closedGateReason(deviceGates);
}

const clampMem = (bytes) => Math.min(Math.max(0, Math.floor(bytes)), MAX_FREE_MEM);

const UNADMITTED_SUBPHASES = new Set(['FETCHING', 'STARTING', 'SPAWNING', 'RUNNING']);

const readLaneId = (remoteRoot, execId) => {
  try {
    return fs.readFileSync(path.join(path.resolve(remoteRoot), 'tickets', execId, 'remote-id'), 'utf8').trim() || null;
  } catch {
    return null;
  }
};

export const liveExecutions = (records, remoteRoot) => records.filter((r) => UNADMITTED_SUBPHASES.has(r.subphase))
  .map((r) => ({ weight: Number(r.claim?.header?.weight) || 0, laneId: readLaneId(remoteRoot, r.execId) }));

export const unadmittedWeights = (executions, knownLaneIds) => executions.filter((e) => e.laneId === null || !knownLaneIds.has(e.laneId)).map((e) => e.weight);

export function freeWeightOf({ capacity, used, queued, pendingWeights, cfg }) {
  const limit = queuedWeightLimit(cfg.maxLaneWeight, capacity);
  const queuedWeight = queued.filter(Boolean).reduce((sum, t) => sum + (Number(clampQueuedWeight(t, cfg, capacity).weight) || 0), 0);
  return Math.max(0, capacity - used - queuedWeight - pendingWeights.reduce((sum, w) => sum + Math.min(w, limit), 0));
}

export async function claimFigures({ cfg = loadGlobalConfig(), host = detectResourceCapacity(), executions = [] } = {}) {
  const capacity = effectiveWeightCapacity(cfg, host.cpuCores);
  const totals = { totalCpu: cpuBudgetCores(host, cfg), totalMem: clampMem(host.memoryBytes - cfg.memoryReserveBytes), weightLimit: queuedWeightLimit(cfg.maxLaneWeight, capacity) };
  if (cfg.schedulerMode !== 'active') return { freeCpu: totals.totalCpu, freeMem: totals.totalMem, freeWeight: capacity, ...totals };
  const status = await collectStatus({ lockTimeoutMs: PROBE_LOCK_WAIT_MS });
  const brokerRoot = ensureStateDirs().root;
  const leases = listLeases(brokerRoot);
  const cpuHeadroom = await probeCpuHeadroom(brokerRoot, cfg, leases.filter((l) => HELD_STATES.has(l.state)));
  const headroom = probeHeadroom(status.resources, cfg, true, undefined, cpuHeadroom);
  if (!headroom || !Number.isFinite(headroom.cpuCores) || !Number.isFinite(headroom.memoryBytes)) return null;
  const queued = listQueue(brokerRoot);
  const known = new Set([...leases, ...queued.filter(Boolean)].map((t) => t.id));
  const freeWeight = freeWeightOf({ capacity: status.capacity, used: status.used, queued, pendingWeights: unadmittedWeights(executions, known), cfg });
  return { freeCpu: headroom.cpuCores, freeMem: clampMem(headroom.memoryBytes), freeWeight, ...totals };
}

/**
 * One `ld_claim`. The request id is persisted before the call and kept until a definite answer, however long ago: a replay returns the
 * claim the lost answer carried. A `claimed` answer persists the execution record (adopting one that a replay already wrote) before the
 * request is deleted. Returns `{status}`: `claimed` (with `execId`), `none`, `upgrade` (this agent's protocol is below the queue's
 * minimum: claims must stop), or `skipped` (with `reason`).
 */
export async function claimOnce({ core, remoteRoot, figures = claimFigures, block, now = Date.now }) {
  const reason = block();
  if (reason) return { status: 'skipped', reason };
  const offered = await figures({ executions: liveExecutions(core.table.all(), remoteRoot) });
  if (!offered) return { status: 'skipped', reason: 'no-headroom' };
  const pending = readPendingClaim(remoteRoot);
  const req = pending?.req ?? randomUUID();
  if (!pending) writePendingClaim(remoteRoot, req);
  const heldKeys = core.table.all().filter((r) => r.subphase !== 'DONE').map((r) => r.claim.laneKey);
  let execId;
  const capacity = offered.totalCpu === undefined ? undefined : offered;
  const answer = await core.ld.claim(req, offered.freeCpu, offered.freeMem, heldKeys, {
    live: () => block() === null,
    persist: (a) => {
      if (a.status === 'claimed') {
        execId = executionIdOf(a.id, a.epoch);
        if (!core.table.get(execId)) core.table.put(newExecutionRecord(a, { now: now() }));
      } else if (a.status !== 'none' && a.status !== 'upgrade') {
        throw new Error(`ld_claim answered ${JSON.stringify(a)}`);
      }
      clearPendingClaim(remoteRoot);
    },
  }, capacity);
  if (answer.skipped) return { status: 'skipped', reason: 'not-live' };
  const fallback = capacity !== undefined && core.ld.legacyQueue === true;
  return { ...(execId ? { status: 'claimed', execId } : { status: answer.status }), ...(fallback ? { fallback: true } : {}) };
}
