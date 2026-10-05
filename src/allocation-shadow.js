import { cpuBudget, writeBrokerLog } from './admission.js';
import { classOf, evaluateQueue, ticketClaims, TEST_LOCK_FRACTION, SIM_LOCK_FRACTION } from './allocation.js';
import { evaluateMemoryAdmission, resolveTicketResources } from './resources.js';

/**
 * BRAIN-379 slice 2: the SHADOW wiring of the pure evaluator in allocation.js.
 *
 * tryStart takes a cheap snapshot (captureShadowInputs, plus the `live` facts its own CPU evaluation produced)
 * under the global lock and hands it here AFTER the lock is released: the evaluation and the one log append never
 * lengthen a live admission or release. Everything here reads only that snapshot (structuredClone copies), and
 * writes only one admission-log line: never a lease, ticket, skip counter, reservation record or the arm stamp.
 */

const SHORT = 8;
const short = (id) => (typeof id === 'string' ? id.slice(0, SHORT) : 'none');
const fmt = (n) => (Number.isFinite(n) ? Number(n.toFixed(2)).toString() : 'n/a');

/** Copies of everything the evaluator reads, so nothing it could do reaches a live object. */
export function captureShadowInputs({ queue, held, now, cfg, cpuCores, lastSimDemandAt, skipBudget, reservation, conflicted, safeBackfill, conflictPickId, runningWeight, weightCapacity, paused }) {
  return {
    queue: structuredClone(queue),
    held: structuredClone(held),
    cfg: structuredClone(cfg),
    now,
    cpuCores,
    lastSimDemandAt,
    skipBudget: { ...skipBudget },
    reservation: { ...reservation },
    conflicted,
    safeBackfill,
    conflictPickId,
    runningWeight,
    weightCapacity,
    paused,
  };
}

/** CPU-guard denials that do not depend on which candidate is asked about (evaluateCpuAdmission's order: sample, gate, cooldown). */
const CANDIDATE_INDEPENDENT_CPU_DENIALS = new Set(['cpu-gate-closed', 'cooldown', 'sample-unavailable-held']);

/**
 * Per-candidate reasons the live scheduler's EXISTING guards (other than the held-key conflict and the CPU
 * projection, which the evaluator itself covers) deny it: pause, weight capacity, load gate, memory brake, the
 * CPU gate / cooldown / unavailable-sample denials and the memory admission (the last three only in active mode,
 * as live). Memory admission counts the blocked head's reservation for a safe backfill / conflict pick exactly as
 * live does, and is evaluated at the captured memory observation time. When live admission returned before it
 * sampled (no `live`), the CPU, load and memory guards are UNKNOWN: every candidate carries `unknown` and is never
 * hypothetically selected.
 */
export function existingGuardsFor(inputs, live, indexOf) {
  const { cfg, held } = inputs;
  const active = cfg.schedulerMode === 'active';
  const head = inputs.queue[0];
  const headReservation = head ? { id: head.id, key: head.key, weight: head.weight, resources: head.resources } : null;
  return (ticket) => {
    const reasons = [];
    if (inputs.paused) reasons.push('paused');
    if (inputs.runningWeight + ticket.weight > inputs.weightCapacity) reasons.push('capacity');
    if (live?.loadGateBlocking) reasons.push('load-gate-closed');
    if (live?.memInfo?.macPressure === 'critical') reasons.push('memory-critical');
    if (active && live) {
      if (CANDIDATE_INDEPENDENT_CPU_DENIALS.has(live.cpuReason)) reasons.push(live.cpuReason);
      if (live.memInfo) {
        const candidateResources = resolveTicketResources({
          weight: ticket.weight,
          cpuCores: ticket.resources?.cpuCores,
          memoryBytes: ticket.resources?.memoryBytes,
          defaultMemoryBytesPerWeight: cfg.defaultMemoryBytesPerWeight,
        });
        const reservesHead = headReservation && (inputs.safeBackfill[indexOf.get(ticket)] === true || ticket.id === inputs.conflictPickId);
        const heldLeases = reservesHead ? [...held, headReservation] : held;
        const memory = evaluateMemoryAdmission({ memoryInfo: live.memInfo, heldLeases, candidateResources, cfg, now: live.memAt });
        if (!memory.admit) reasons.push(memory.reason);
      }
    }
    if (!live) reasons.push('unknown');
    return reasons;
  };
}

function verdictOf(d) {
  if (d.eligible) return d.reason === 'ok' ? 'ok' : 'idle-exempt';
  return d.reason;
}

function candidateToken(d) {
  if (d.id === null) return 'unreadable';
  const parts = [short(d.id), d.class, `c=${fmt(d.claim)}`, `e=${fmt(d.effectiveClaim)}`, `k=${fmt(d.clampedClaim)}`, verdictOf(d)];
  if (d.reservedHead > 0) parts.push(`resv=${fmt(d.reservedHead)}`);
  if (d.guards.length > 0) parts.push(`guards=${d.guards.join('|')}`);
  return parts.join(':');
}

/** The decision-relevant config values, so a record can be replayed without the config file of that moment. */
function configEvidence(cfg) {
  return [
    `lockT:${TEST_LOCK_FRACTION}`,
    `lockS:${SIM_LOCK_FRACTION}`,
    `armWindowMs:${cfg.simArmWindowMs}`,
    `mode:${cfg.schedulerMode}`,
    `capacity:${cfg.capacity}`,
    `cpuPct:${cfg.cpuAdmissionPercent}`,
    `cpuReserve:${cfg.cpuReserveCores}`,
    `memReserveB:${cfg.memoryReserveBytes}`,
    `memCloseB:${cfg.memoryCloseBytes}`,
    `memOpenB:${cfg.memoryOpenBytes}`,
    `conflictSkip:${cfg.conflictSkipLimit}`,
    `resourceSkip:${cfg.resourceSkipLimit}`,
    `loadGate:${cfg.admissionLoadGate}`,
    `safeBackfill:${cfg.conflictSafeBackfill}`,
  ].join(',');
}

export function formatAllocationShadowLog(f) {
  const e = f.evaluation;
  return [
    'lane-broker-allocation-shadow',
    `ts=${f.ts}`,
    `poller=${short(f.pollerId)}`,
    `head=${short(f.headId)}`,
    `actual=${f.actual}`,
    `select=${short(e.selection)}`,
    `selectReason=${e.selectionReason}`,
    `B=${fmt(e.B)}`,
    `budgetSource=${f.budgetSource}`,
    `chargeSource=${f.chargeSource}`,
    `externalBusy=${fmt(f.externalBusy)}`,
    `used_t=${fmt(e.used.test)}`,
    `used_s=${fmt(e.used.sim)}`,
    `L_t=${fmt(e.locks.L_t)}`,
    `L_s=${fmt(e.locks.L_s)}`,
    `armed=${e.armed}`,
    `lastSimDemandAt=${f.lastSimDemandAt ?? 'none'}`,
    `cfg=${configEvidence(f.cfg)}`,
    `skip=${f.skipBudget.kind}:${e.skipBudget.used}+${e.skipBudget.consumed}/${e.skipBudget.limit}`,
    `reserved=${f.reservation.reserved}`,
    `candidates=${e.decisions.length ? e.decisions.map(candidateToken).join(',') : 'none'}`,
  ].join(' ');
}

/**
 * Evaluate and log once, from the snapshot. `shadow.live` is what tryStart's own CPU evaluation produced
 * (absent when the decision returned before CPU sampling): B, externalBusy and the per-lease charges are then
 * LIVE's, not recomputed; without it B comes from the machine's core count, externalBusy is 0 and charges are
 * recomputed from the captured clock. Any throw is logged as a shadow error and swallowed: this function can
 * never change the live decision.
 */
export function recordAllocationShadow({ root, shadow, result, pollerId, evaluator = evaluateQueue }) {
  try {
    if (shadow.captureError) throw shadow.captureError;
    const { inputs, live } = shadow;
    const liveBudget = Number.isFinite(live?.budget);
    const B = liveBudget ? live.budget : cpuBudget({ cores: Number.isFinite(live?.cores) ? live.cores : inputs.cpuCores }, inputs.cfg);
    const externalBusy = Number.isFinite(live?.externalBusy) ? live.externalBusy : 0;
    const charged = live && live.leaseCharges.length > 0;
    const indexOf = new Map(inputs.queue.map((t, i) => [t, i]));
    const guardsOf = existingGuardsFor(inputs, live, indexOf);
    const evaluation = evaluator({
      queue: inputs.queue,
      held: inputs.held,
      now: inputs.now,
      cfg: inputs.cfg,
      B,
      externalBusy,
      lastSimDemandAt: inputs.lastSimDemandAt ?? undefined,
      skipBudget: inputs.skipBudget,
      reservation: inputs.reservation,
      conflictBlocked: (t) => inputs.conflicted[indexOf.get(t)] === true,
      safeBackfill: (t) => inputs.safeBackfill[indexOf.get(t)] === true,
      existingGuards: guardsOf,
      charges: charged ? new Map(live.leaseCharges) : null,
      idleExempt: live?.idleExempt === true,
    });
    writeBrokerLog(
      root,
      `${formatAllocationShadowLog({
        ts: inputs.now,
        pollerId,
        headId: inputs.queue[0]?.id,
        actual: result.started ? 'started' : result.reason,
        evaluation,
        budgetSource: liveBudget ? 'live' : 'capacity',
        chargeSource: charged ? 'live' : 'recomputed',
        externalBusy,
        lastSimDemandAt: inputs.lastSimDemandAt,
        cfg: inputs.cfg,
        skipBudget: inputs.skipBudget,
        reservation: inputs.reservation,
      })}\n`,
    );
  } catch (err) {
    writeBrokerLog(root, `lane-broker-allocation-shadow-error poller=${short(pollerId)} error=${String(err?.message ?? err).replace(/\s+/g, '_').slice(0, 120)}\n`);
  }
}

/** Queued CPU claim per class, for `lane status`. */
export function queuedClaimsByClass(queue, cfg) {
  const queued = { test: 0, sim: 0 };
  for (const t of queue) if (t) queued[classOf(t)] += ticketClaims(t, cfg).claim;
  return queued;
}
