import { randomUUID } from 'node:crypto';
import { drainBlocksIntake, executionIdOf, readDrainMarker, readRemoteDrain, stateHome, ensureStateDirs } from '../state.js';
import { runnerAvailability } from '../runner-schedule.js';
import { loadGlobalConfig } from '../config.js';
import { collectStatus } from '../status.js';
import { listLeases } from '../lease.js';
import { HELD_STATES } from '../scheduler.js';
import { detectResourceCapacity, cpuBudgetCores } from '../resources.js';
import { PROBE_LOCK_WAIT_MS, probeCpuHeadroom, probeHeadroom } from '../remote-runner.js';
import { clearPendingClaim, newExecutionRecord, readPendingClaim, writePendingClaim } from './store.js';

export const MAX_FREE_MEM = Number.MAX_SAFE_INTEGER;

export const isDraining = (stateDir = stateHome()) => readRemoteDrain(stateDir) !== null || drainBlocksIntake(readDrainMarker(stateDir));

/** Why this runner takes no new claim now, or null: a drain marker, outside its schedule window, or too close to the window closing. */
export function claimBlock({ schedule, stopClaimingBeforeCloseMs, now = Date.now(), draining = isDraining }) {
  if (draining()) return 'draining';
  const { available, closesAt } = runnerAvailability(schedule, now);
  if (!available) return 'outside-schedule';
  if (closesAt !== null && closesAt - now <= stopClaimingBeforeCloseMs) return 'closing-soon';
  return null;
}

const clampMem = (bytes) => Math.min(Math.max(0, Math.floor(bytes)), MAX_FREE_MEM);

/**
 * The free CPU and memory a claim offers. Active mode measures this host as `remote-probe` does and answers null (skip the tick) when
 * the measurement is missing; shadow mode has no measurement to trust and offers the static budget, which is finite.
 */
export async function claimFigures({ cfg = loadGlobalConfig(), host = detectResourceCapacity() } = {}) {
  if (cfg.schedulerMode !== 'active') return { freeCpu: cpuBudgetCores(host, cfg), freeMem: clampMem(host.memoryBytes - cfg.memoryReserveBytes) };
  const status = await collectStatus({ lockTimeoutMs: PROBE_LOCK_WAIT_MS });
  const brokerRoot = ensureStateDirs().root;
  const cpuHeadroom = await probeCpuHeadroom(brokerRoot, cfg, listLeases(brokerRoot).filter((l) => HELD_STATES.has(l.state)));
  const headroom = probeHeadroom(status.resources, cfg, true, undefined, cpuHeadroom);
  if (!headroom || !Number.isFinite(headroom.cpuCores) || !Number.isFinite(headroom.memoryBytes)) return null;
  return { freeCpu: headroom.cpuCores, freeMem: clampMem(headroom.memoryBytes) };
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
  const offered = await figures();
  if (!offered) return { status: 'skipped', reason: 'no-headroom' };
  const pending = readPendingClaim(remoteRoot);
  const req = pending?.req ?? randomUUID();
  if (!pending) writePendingClaim(remoteRoot, req);
  const heldKeys = core.table.all().filter((r) => r.subphase !== 'DONE').map((r) => r.claim.laneKey);
  let execId;
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
  });
  if (answer.skipped) return { status: 'skipped', reason: 'not-live' };
  return execId ? { status: 'claimed', execId } : { status: answer.status };
}
