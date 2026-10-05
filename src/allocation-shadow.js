import { cpuBudget, writeBrokerLog } from './admission.js';
import { classOf, evaluateQueue, ticketClaims } from './allocation.js';
import { readLastSimDemandAt } from './sim-arm.js';

/**
 * BRAIN-379 slice 2: the SHADOW wiring of the pure evaluator in allocation.js. It runs inside tryStart's
 * locked evaluation, after the live decision is made, over a structuredClone taken BEFORE the live
 * decision could write anything. It reads files (the arm stamp) but writes only one admission-log line:
 * never a lease, a ticket, a skip counter, a reservation record or the arm stamp.
 */

const SHORT = 8;
const short = (id) => (typeof id === 'string' ? id.slice(0, SHORT) : 'none');
const fmt = (n) => (Number.isFinite(n) ? Number(n.toFixed(2)).toString() : 'n/a');

/** Copies of everything the evaluator reads, so nothing it could do reaches a live object. */
export function captureShadowInputs({ queue, held, now, cfg, cpuCores, skipBudget, reservation, conflictBlocked, safeBackfill }) {
  return { queue: structuredClone(queue), held: structuredClone(held), now, cfg: structuredClone(cfg), cpuCores, skipBudget: { ...skipBudget }, reservation: { ...reservation }, conflictBlocked, safeBackfill };
}

function verdictOf(d) {
  if (d.eligible) return d.reason === 'ok' ? 'ok' : 'idle-exempt';
  return d.reason;
}

export function formatAllocationShadowLog(f) {
  const candidates = f.evaluation.decisions.map((d) => (d.id === null ? 'unreadable' : `${short(d.id)}:${d.class}:${fmt(d.claim)}:${verdictOf(d)}`));
  return [
    'lane-broker-allocation-shadow',
    `poller=${short(f.pollerId)}`,
    `head=${short(f.headId)}`,
    `actual=${f.actual}`,
    `select=${short(f.evaluation.selection)}`,
    `selectReason=${f.evaluation.selectionReason}`,
    `B=${fmt(f.evaluation.B)}`,
    `budgetSource=${f.budgetSource}`,
    `externalBusy=${fmt(f.externalBusy)}`,
    `used_t=${fmt(f.evaluation.used.test)}`,
    `used_s=${fmt(f.evaluation.used.sim)}`,
    `L_t=${fmt(f.evaluation.locks.L_t)}`,
    `L_s=${fmt(f.evaluation.locks.L_s)}`,
    `armed=${f.evaluation.armed}`,
    `lastSimDemandAt=${f.lastSimDemandAt ?? 'none'}`,
    `skip=${f.skipBudget.kind}:${f.evaluation.skipBudget.used}+${f.evaluation.skipBudget.consumed}/${f.evaluation.skipBudget.limit}`,
    `reserved=${f.reservation.reserved}`,
    `candidates=${candidates.length ? candidates.join(',') : 'none'}`,
  ].join(' ');
}

/** What the live decision actually did, as one token: `started` or the denial reason. */
export function liveOutcomeOf(outcome) {
  return outcome.result.started ? 'started' : outcome.result.reason;
}

/**
 * Evaluate and log once. `shadow` is what tryStart gathered while deciding: `inputs` (captureShadowInputs, at
 * selection time), `cpuSample`/`externalBusy` (absent when the decision returned before CPU sampling, in which
 * case B comes from the machine's core count and externalBusy is 0) and `idleExempt`. Any throw is logged as a
 * shadow error and swallowed: this function can never change the live decision.
 */
export function recordAllocationShadow({ root, shadow, outcome, pollerId, evaluator = evaluateQueue }) {
  try {
    if (shadow.captureError) throw shadow.captureError;
    const inputs = shadow.inputs;
    const sampled = Number.isFinite(shadow.cpuSample?.cores);
    const B = cpuBudget({ cores: sampled ? shadow.cpuSample.cores : inputs.cpuCores }, inputs.cfg);
    const externalBusy = Number.isFinite(shadow.externalBusy) ? shadow.externalBusy : 0;
    const lastSimDemandAt = readLastSimDemandAt(root);
    const evaluation = evaluator({
      queue: inputs.queue,
      held: inputs.held,
      now: inputs.now,
      cfg: inputs.cfg,
      B,
      externalBusy,
      lastSimDemandAt: lastSimDemandAt ?? undefined,
      skipBudget: inputs.skipBudget,
      reservation: inputs.reservation,
      conflictBlocked: inputs.conflictBlocked,
      safeBackfill: inputs.safeBackfill,
      idleExempt: shadow.idleExempt === true,
    });
    writeBrokerLog(
      root,
      `${formatAllocationShadowLog({
        pollerId,
        headId: inputs.queue[0]?.id,
        actual: liveOutcomeOf(outcome),
        evaluation,
        budgetSource: sampled ? 'sample' : 'capacity',
        externalBusy,
        lastSimDemandAt,
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
