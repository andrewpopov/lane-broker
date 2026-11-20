import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { drainBlocksIntake, executionIdOf, readDrainMarker, readDrainMarkers, stateHome, ensureStateDirs } from '../state.js';
import { runnerAvailability } from '../runner-schedule.js';
import { closedGateReason } from '../device-gates.js';
import { loadGlobalConfig, reloadGlobalConfig } from '../config.js';
import { collectStatus } from '../status.js';
import { listLeases } from '../lease.js';
import { HELD_STATES, listQueue } from '../scheduler.js';
import { clampQueuedWeight, queuedWeightLimit } from '../exclusive.js';
import { detectResourceCapacity, cpuBudgetCores, effectiveWeightCapacity } from '../resources.js';
import { PROBE_LOCK_WAIT_MS, probeCpuHeadroom, probeHeadroom } from '../remote-runner.js';
import { clearPendingClaim, newExecutionRecord, readPendingClaim, writePendingClaim } from './store.js';
import { reportReadiness } from '../runner-readiness.js';

export const MAX_FREE_MEM = Number.MAX_SAFE_INTEGER;

export const drainOwners = (stateDir = stateHome()) => [...readDrainMarkers(stateDir).map((m) => m.owner), ...(drainBlocksIntake(readDrainMarker(stateDir)) ? ['migration'] : [])];

/** What a tick publishes so a drain's owner can see its marker took hold: each unexpired marker's owner and, when it carries one, its drain id. */
export const drainsOf = (stateDir = stateHome()) => readDrainMarkers(stateDir).map(({ owner, drainId }) => ({ owner, ...(drainId === undefined ? {} : { id: drainId }) }));

export const isDraining = (stateDir = stateHome()) => drainOwners(stateDir).length > 0;

/** Why this runner takes no new claim now, or null: a drain marker, outside its schedule window, too close to the window closing, or a closed device gate. */
export function claimBlock({ schedule, stopClaimingBeforeCloseMs, now = Date.now(), draining = isDraining, owners = () => [], deviceGates }) {
  if (draining()) {
    const by = owners();
    return by.length ? `draining (${by.join(', ')})` : 'draining';
  }
  const { available, closesAt } = runnerAvailability(schedule, now);
  if (!available) return 'outside-schedule';
  if (closesAt !== null && closesAt - now <= stopClaimingBeforeCloseMs) return 'closing-soon';
  return closedGateReason(deviceGates);
}

export function brokerConfigReader({ log }) {
  let cfg;
  let reported = null;
  return () => {
    let problem = null;
    cfg = reloadGlobalConfig(cfg, { onError: (err) => { problem = `unreadable, keeping the last good config: ${err.message}`; } });
    problem ??= cfg.overlayWarnings?.join('; ') ?? null;
    if (problem && problem !== reported) log(`broker config ${problem}`);
    reported = problem;
    return cfg;
  };
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

/**
 * The free CPU, memory and weight a claim offers, the CPU budget and the broker's per-ticket weight limit. Active mode measures this host as
 * `remote-probe` does and answers null (skip the tick) when the measurement is missing; shadow mode offers the static budget, which is finite.
 * `executions` is read after the lease and queue snapshots, and `used` comes from that same lease snapshot, so an execution that is enqueued or
 * leased mid-call is charged exactly once.
 */
export async function claimFigures({ cfg = loadGlobalConfig(), host = detectResourceCapacity(), executions = () => [] } = {}) {
  const budgetCpu = cpuBudgetCores(host, cfg);
  const capacity = effectiveWeightCapacity(cfg, host.cpuCores);
  const weightLimit = queuedWeightLimit(cfg.maxLaneWeight, capacity);
  if (cfg.schedulerMode !== 'active') return { freeCpu: budgetCpu, freeMem: clampMem(host.memoryBytes - cfg.memoryReserveBytes), budgetCpu, freeWeight: capacity, weightLimit };
  const status = await collectStatus({ lockTimeoutMs: PROBE_LOCK_WAIT_MS });
  const brokerRoot = ensureStateDirs().root;
  const leases = listLeases(brokerRoot);
  const cpuHeadroom = await probeCpuHeadroom(brokerRoot, cfg, leases.filter((l) => HELD_STATES.has(l.state)));
  const headroom = probeHeadroom(status.resources, cfg, true, undefined, cpuHeadroom);
  if (!headroom || !Number.isFinite(headroom.cpuCores) || !Number.isFinite(headroom.memoryBytes)) return null;
  const queued = listQueue(brokerRoot);
  const known = new Set([...leases, ...queued.filter(Boolean)].map((t) => t.id));
  const used = leases.filter((l) => HELD_STATES.has(l.state)).reduce((sum, l) => sum + (l.weight || 0), 0);
  const freeWeight = freeWeightOf({ capacity: status.capacity, used, queued, pendingWeights: unadmittedWeights(executions(), known), cfg });
  return { freeCpu: headroom.cpuCores, freeMem: clampMem(headroom.memoryBytes), budgetCpu, freeWeight, weightLimit };
}

/**
 * BRAIN-1099: what this runner could offer once its own claims end, its free figures plus what those claims hold (CPU capped at the budget). Outside
 * load stays counted, so the queue holds back only for a head this runner can really reach. Null unless the queue supports the reservation.
 */
export function claimCapacity(offered, records, registered) {
  if (registered?.head_reserve !== true || !Number.isFinite(offered.budgetCpu)) return null;
  const live = records.filter((r) => r.subphase !== 'DONE');
  const heldCpu = live.reduce((sum, r) => sum + (r.claim.cpuCores ?? r.claim.header?.cpuCores ?? 0), 0);
  const heldMem = live.reduce((sum, r) => sum + (r.claim.header?.memoryBytes ?? 0), 0);
  const weight = registered.claim_weight === true && Number.isFinite(offered.freeWeight) ? { freeWeight: offered.freeWeight, weightLimit: offered.weightLimit } : {};
  return { cpu: Math.min(offered.budgetCpu, offered.freeCpu + heldCpu), mem: clampMem(offered.freeMem + heldMem), ...weight };
}

const UNDEFINED_FUNCTION = '42883';

/** The names in `names` that the readiness `tools` check finds on `envPath`: the one answer to "does this runner have the tool", push or pull. */
export const toolsOnPath = (names, envPath) => {
  const found = reportReadiness({ tools: names }, { envPath }).tools ?? {};
  return names.filter((name) => found[name] === true);
};

/** BRAIN-1135: the tools the queued tickets this agent may claim require that are on `envPath` (its lanes' PATH); null on a queue before migration 008. */
export async function presentTools({ ld, envPath }) {
  let needed;
  try {
    needed = await ld.neededTools();
  } catch (err) {
    if (err.code === UNDEFINED_FUNCTION) return null;
    throw err;
  }
  return toolsOnPath(needed ?? [], envPath);
}

let fallbackLogged = false;

/**
 * One `ld_claim`. The request id is persisted before the call and kept until a definite answer, however long ago: a replay returns the
 * claim the lost answer carried. A `claimed` answer persists the execution record (adopting one that a replay already wrote) before the
 * request is deleted. Returns `{status}`: `claimed` (with `execId`), `none`, `upgrade` (this agent's protocol is below the queue's
 * minimum: claims must stop), or `skipped` (with `reason`). Only tickets whose required tools `tools()` names are offered.
 */
export async function claimOnce({ core, remoteRoot, figures = claimFigures, block, now = Date.now, tools = () => presentTools({ ld: core.ld, envPath: process.env.PATH }), log = () => {} }) {
  const reason = block();
  if (reason) return { status: 'skipped', reason };
  const offered = await figures({ executions: () => liveExecutions(core.table.all(), remoteRoot) });
  if (!offered) return { status: 'skipped', reason: 'no-headroom' };
  const hasTools = await tools();
  const pending = readPendingClaim(remoteRoot);
  const req = pending?.req ?? randomUUID();
  if (!pending) writePendingClaim(remoteRoot, req);
  const heldKeys = core.table.all().filter((r) => r.subphase !== 'DONE').map((r) => r.claim.laneKey);
  let execId;
  const capacity = claimCapacity(offered, core.table.all(), core.ld.registered);
  const answer = await core.ld.claim(req, offered.freeCpu, offered.freeMem, heldKeys, hasTools, {
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
  const fallback = capacity?.freeWeight === undefined && core.ld.registered?.claim_weight !== true;
  if (fallback && !fallbackLogged) {
    fallbackLogged = true;
    log('ld_claim: the queue predates migration 010; claiming without weight figures (the broker may be offered tickets it cannot start yet)');
  }
  return { ...(execId ? { status: 'claimed', execId } : { status: answer.status }), ...(fallback ? { fallback: true } : {}) };
}
