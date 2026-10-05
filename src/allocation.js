import { leaseDemandBasis, ticketCpuEstimate } from './admission.js';
import { LEASE_STATE } from './lease.js';

/**
 * BRAIN-379 (balancer P2): the PURE class-allocation evaluator. Shadow mode only: nothing here
 * reads the clock, the disk or any global, and nothing mutates a lease, ticket, skip counter or
 * reservation record. `now` and every input are parameters, so a later slice can call it from the
 * locked admission evaluation with the same captured `now`, charges and budget B live admission uses.
 */

export const TEST_LOCK_FRACTION = 0.15;
export const SIM_LOCK_FRACTION = 0.3;
export const DEFAULT_SIM_ARM_WINDOW_MS = 300_000;

const CHARGED_STATES = new Set([LEASE_STATE.RUNNING, LEASE_STATE.ORPHANED]);

/** A ticket's or lease's class; anything without an explicit 'sim' (old files included) is a test. */
export function classOf(ticketOrLease) {
  return ticketOrLease?.class === 'sim' ? 'sim' : 'test';
}

/** Armed = a sim is queued or charged now, or either was true within the last simArmWindowMs. */
export function simArmed({ now, lastSimDemandAt, simQueued = false, simCharged = false, simArmWindowMs = DEFAULT_SIM_ARM_WINDOW_MS }) {
  if (simQueued || simCharged) return true;
  return Number.isFinite(lastSimDemandAt) && now - lastSimDemandAt <= simArmWindowMs;
}

/** Soft locks, real-valued (the whole-core fit test is what rounds). Tests: permanent; sims: only while armed. */
export function classLocks({ B, armed }) {
  return { L_t: TEST_LOCK_FRACTION * B, L_s: armed ? SIM_LOCK_FRACTION * B : 0 };
}

/** The sim lock's size whether or not it is armed right now: what an oversized test claim is clamped against. */
const simLockCeiling = (B) => SIM_LOCK_FRACTION * B;

/**
 * CHARGED demand per class: every RUNNING/ORPHANED lease at its leaseDemandBasis demand. A
 * synthetic head reservation (BRAIN-355) is never a lease and is never counted here.
 */
export function usedByClass(leases, now, cfg) {
  const used = { test: 0, sim: 0 };
  for (const lease of leases) {
    if (lease.synthetic === true || !CHARGED_STATES.has(lease.state)) continue;
    used[classOf(lease)] += leaseDemandBasis(lease, now, cfg).demand;
  }
  return used;
}

/** BRAIN-355's synthetic head reservation, carrying its head's class explicitly. */
export function headReservationOf(head) {
  return { id: head.id, key: head.key, weight: head.weight, resources: head.resources, class: classOf(head), synthetic: true };
}

/** A ticket's full CPU claim and the smallest claim it can be admitted at (BRAIN-360 elastic floor). */
export function ticketClaims(ticket, cfg) {
  const claim = ticketCpuEstimate(ticket, cfg);
  const min = ticket.resources?.minCpuCores;
  return { claim, floorClaim: Number.isFinite(min) ? Math.min(claim, Math.ceil(min)) : claim };
}

/**
 * One candidate against the class locks, ON TOP of the existing guards (never replacing them):
 * eligible iff claim <= B - sum(used) - max(reservedOther, externalBusy).
 *
 * `reason`: 'ok'; 'class-lock' (fits without the class lock, not with it: the only class-ineligible
 * outcome); 'over-free' (does not fit even with no lock, the existing CPU guard denies it anyway).
 * `reservedHead` is a BRAIN-355 synthetic reservation's claim: it shrinks free but is in no used_c.
 * `idleExempt` is the BRAIN-346 idle overshoot: it waives only over-free, never the class lock.
 * `simsCanArm` is whether this runner can ever arm sims; if so an oversized TEST claim is clamped
 * to B - L_s (real-valued; the grant floors to whole cores, never below 1) so it can never wait forever on a lock that arms later.
 */
export function evaluateCandidate({ candidate, cfg, B, usedT, usedS, externalBusy = 0, reservedHead = 0, armed, locks = classLocks({ B, armed }), simsCanArm = true, idleExempt = false }) {
  const klass = classOf(candidate);
  const { claim, floorClaim } = ticketClaims(candidate, cfg);
  const clampedClaim = klass === 'test' && simsCanArm ? Math.max(1, Math.floor(Math.min(claim, B - simLockCeiling(B)))) : claim;
  const effectiveClaim = Math.min(floorClaim, clampedClaim);
  const reservedOther = klass === 'sim' ? Math.max(0, locks.L_t - usedT) : Math.max(0, locks.L_s - usedS);
  const free = B - (usedT + usedS) - reservedHead;
  const limit = free - Math.max(reservedOther, externalBusy);
  const fitsWithoutLock = effectiveClaim <= free - externalBusy;
  const fits = effectiveClaim <= limit;
  const reason = fits ? 'ok' : fitsWithoutLock ? 'class-lock' : 'over-free';
  const eligible = fits || (idleExempt && reason === 'over-free' && effectiveClaim <= free - reservedOther);
  return { eligible, claim, clampedClaim, effectiveClaim, reservedOther, reason, class: klass, limit };
}

/**
 * The hypothetical selection over a FIFO queue (index 0 is the head), per the BRAIN-379 selection
 * order. Evaluate only: it never touches live selection, skip counters or reservation records.
 *
 *  - class-ineligible candidates are filtered out BEFORE the smallest fitting claim is chosen;
 *  - a class-ineligible head does not block backfill of eligible candidates;
 *  - a live reservation (`reservation.reserved`) stops backfill even when the head is class-ineligible;
 *  - a backfill past the head (blocked for class OR cpu/conflict) consumes one skip from the same
 *    bounded budget (`skipBudget.limit`/`.used`), except a BRAIN-355 safe backfill, which is uncounted;
 *  - `conflictBlocked(ticket)` / `safeBackfill(ticket)` are the live scheduler's own verdicts, passed in.
 *
 * A null queue entry (an unreadable record) stops the walk, as in every live selector.
 */
export function evaluateQueue({ queue, held, now, cfg, B, externalBusy = 0, lastSimDemandAt, simsCanArm = true, skipBudget, reservation = null, conflictBlocked = () => false, safeBackfill = () => false, idleExempt = false }) {
  const used = usedByClass(held, now, cfg);
  const simQueued = queue.some((t) => t && classOf(t) === 'sim');
  const simCharged = held.some((l) => !l.synthetic && CHARGED_STATES.has(l.state) && classOf(l) === 'sim');
  const armed = simsCanArm && simArmed({ now, lastSimDemandAt, simQueued, simCharged, simArmWindowMs: cfg.simArmWindowMs });
  const locks = classLocks({ B, armed });
  const head = queue[0] ?? null;
  const headClaim = head ? ticketClaims(head, cfg).floorClaim : 0;

  const decisions = [];
  for (let index = 0; index < queue.length; index++) {
    const ticket = queue[index];
    if (!ticket) {
      decisions.push({ id: null, index, eligible: false, reason: 'unreadable', skipped: false });
      break;
    }
    const safe = index > 0 && safeBackfill(ticket) === true;
    const verdict = evaluateCandidate({ candidate: ticket, cfg, B, usedT: used.test, usedS: used.sim, externalBusy, reservedHead: safe ? headClaim : 0, armed, locks, simsCanArm, idleExempt: idleExempt && index === 0 });
    const conflicted = conflictBlocked(ticket) === true;
    decisions.push({ id: ticket.id, index, ...verdict, eligible: verdict.eligible && !conflicted, reason: conflicted ? 'conflict' : verdict.reason, safeBackfill: safe, skipped: false });
  }

  const budgetUsed = skipBudget?.used ?? 0;
  const budgetLimit = skipBudget?.limit ?? 0;
  const budgetOpen = budgetUsed < budgetLimit;
  const finish = (pick, selectionReason, consumed) => {
    for (const d of decisions) d.skipped = pick !== null && d.index < pick.index && !d.eligible;
    return {
      selection: pick ? pick.id : null,
      selectionReason,
      decisions,
      skipBudget: { limit: budgetLimit, used: budgetUsed, consumed, remaining: budgetLimit - budgetUsed - consumed },
      used,
      B,
      armed,
      locks,
    };
  };

  if (decisions.length === 0) return finish(null, 'empty-queue', 0);
  if (decisions[0].eligible) return finish(decisions[0], 'head', 0);
  if (reservation?.reserved === true) return finish(null, 'reserved-head', 0);

  const behind = decisions.slice(1).filter((d) => d.eligible);
  const safePick = behind.find((d) => d.safeBackfill);
  if (safePick && !budgetOpen) return finish(safePick, 'safe-backfill', 0);
  if (!budgetOpen) return finish(null, 'skip-budget-exhausted', 0);
  const pick = behind.reduce((best, d) => (best === null || d.effectiveClaim < best.effectiveClaim ? d : best), null);
  if (!pick) return finish(null, 'no-eligible-backfill', 0);
  return finish(pick, pick.safeBackfill ? 'safe-backfill' : 'backfill', pick.safeBackfill ? 0 : 1);
}
