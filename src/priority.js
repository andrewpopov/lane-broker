/**
 * BRAIN-380: pure priority scoring and queue ordering. Nothing here reads the clock, the
 * filesystem or the config file: callers pass `nowEff` (the priority clock, see priority-clock.js)
 * and the priority config slice, so every function is deterministic and testable on a fake clock.
 *
 * Slice 1 only BUILDS these; no selector calls `orderQueue` yet, so scheduling stays FIFO.
 */

export const PRIORITY_TIERS = ['low', 'medium', 'high'];
export const DEFAULT_PRIORITY = 'medium';

/** BRAIN-380 §6: the capability a runner advertises when it reads the priority header fields (`priority/1`). */
export const PRIORITY_CAPABILITY = 'priority/1';
/** A submitter's accrued wait is trusted only up to a day, the same bound as the age cap in config. */
const MAX_ACCRUED_MS = 86_400_000;

const TIER_BASE = { low: 0, medium: 1, high: 2 };
const MAX_BASE = 2;

export function isPriorityTier(value) {
  return typeof value === 'string' && Object.hasOwn(TIER_BASE, value);
}

/** The tier a ticket was ADMITTED at; a record without a readable one is medium. */
export function priorityOf(ticket) {
  return isPriorityTier(ticket?.priorityAdmitted) ? ticket.priorityAdmitted : DEFAULT_PRIORITY;
}

/**
 * A ticket's priority origin: `prioOriginAt` when it is a trustworthy instant (finite, not in
 * the future of the priority clock), otherwise `nowEff` -- a ticket first seen without an
 * origin starts with zero age. Never derived from the wall-clock `createdAt`.
 */
export function originOrNow(prioOriginAt, nowEff) {
  return Number.isFinite(prioOriginAt) && prioOriginAt >= 0 && prioOriginAt <= nowEff ? prioOriginAt : nowEff;
}

/** Remote header: accrued wait is a finite integer in [0, 24h], otherwise 0. */
export function sanitizeAccruedMs(value) {
  return Number.isInteger(value) && value >= 0 && value <= MAX_ACCRUED_MS ? value : 0;
}

/** Remote header: a runner's view of what the submitter sent. An unknown tier is medium and an invalid wait is 0. */
export function remotePriorityFrom(header) {
  return {
    priority: isPriorityTier(header?.priorityRequested) ? header.priorityRequested : DEFAULT_PRIORITY,
    accruedMs: sanitizeAccruedMs(header?.priorityAccruedMs),
  };
}

/** The audit fields a lease and its terminal history row carry (BRAIN-380 §8); only those the lease actually has. */
export function priorityAuditOf(record) {
  const audit = {};
  for (const key of ['priorityRequested', 'priorityAdmitted', 'priorityDemoted', 'effectiveRankAtStart', 'scoreAtStart']) {
    if (record?.[key] !== undefined) audit[key] = record[key];
  }
  return audit;
}

export function waitedMs(ticket, nowEff) {
  return Math.max(0, nowEff - originOrNow(ticket?.prioOriginAt, nowEff));
}

/** ROG-2181: age that counts toward rank and score. A ticket from an `aging: false` lane never accrues any. */
function creditedWaitMs(ticket, nowEff) {
  // BRAIN-403 A1: an exclusive never ages, so aging cannot promote it past a higher-priority arrival
  return ticket?.aging === false || ticket?.exclusive === true ? 0 : waitedMs(ticket, nowEff);
}

/** BRAIN-504: how far below a fresh high's score an aged non-high is held, as a fraction of `W_tier`. */
const AGED_CEILING_EPSILON = 0.01;
/** BRAIN-504: a non-high that has waited this many `priorityAgeMaxMs` periods may tie a fresh high again. */
const STARVATION_HORIZON_FACTOR = 3;

/** Credited wait at which a non-high ticket's score ceiling rises to a high's (default 3 x 20 min = 60 min). */
export function starvationHorizonMs(cfg) {
  return STARVATION_HORIZON_FACTOR * cfg.priorityAgeMaxMs;
}

/**
 * Slurm-style multifactor score: `min(ceiling, W_tier * tierFactor + W_age * ageFactor)`.
 * The ceiling is `W_tier` for an admitted high and for a non-high whose credited wait has reached
 * `starvationHorizonMs` (then it ties a fresh high and seq decides, so nothing starves past that bound).
 * Any other non-high is held at `W_tier * (1 - AGED_CEILING_EPSILON)`, strictly below a fresh high.
 * Exclusive and `aging: false` tickets credit no wait, so they never reach the horizon.
 * This is a guarantee about SCORE ordering only: reservation promotion (`promoteReservationOwner`),
 * backfill (smallest fitting claim), unreadable-record barriers and `simRank` (which sorts before seq
 * when `simsAfterTests`) are applied outside the score and can still start a non-high before a high.
 */
export function score(ticket, nowEff, cfg) {
  const { tier, age } = cfg.priorityWeights;
  const tierFactor = TIER_BASE[priorityOf(ticket)] / MAX_BASE;
  const creditedMs = creditedWaitMs(ticket, nowEff);
  const ageFactor = Math.min(1, creditedMs / cfg.priorityAgeMaxMs);
  const reachesTop = priorityOf(ticket) === 'high' || creditedMs >= starvationHorizonMs(cfg);
  const ceiling = reachesTop ? tier : tier * (1 - AGED_CEILING_EPSILON);
  return Math.min(ceiling, tier * tierFactor + age * ageFactor);
}

/**
 * Display-only rank (0 low, 1 medium, 2 high), the band of the ticket's `score`: 2 at the full-tier
 * ceiling (a high, or a non-high past the starvation horizon), 1 from half of `W_tier`, otherwise 0.
 */
export function effectiveRank(ticket, nowEff, cfg) {
  const s = score(ticket, nowEff, cfg);
  const { tier } = cfg.priorityWeights;
  return s >= tier ? 2 : s >= tier / 2 ? 1 : 0;
}

export function tierName(rank) {
  return PRIORITY_TIERS[rank];
}

/**
 * BRAIN-321: with class caps in force, at an equal score a test goes before a sim, whichever was enqueued first. A sim that
 * has waited its full `priorityAgeMaxMs` (elapsed queue wait, so an exclusive or `aging: false` sim, whose score never ages, counts too) is exempt, so it is ordered by seq like anything else: only tests enqueued
 * BEFORE it can stay ahead, so a cap-eligible sim cannot be overtaken forever by newer tests of the same score.
 */
const simRank = (ticket, nowEff, cfg, simsAfterTests) =>
  simsAfterTests && ticket.class === 'sim' && waitedMs(ticket, nowEff) < cfg.priorityAgeMaxMs ? 1 : 0;

/**
 * The order every selector walks: (score desc, test before a not-fully-aged sim when `simsAfterTests`, seq asc) within each maximal run of readable
 * records. An unreadable (`null`) record is a barrier: nothing moves across it, so the
 * scheduler's protection against an unreadable head survives.
 */
export function orderQueue(raw, nowEff, cfg, simsAfterTests = false) {
  const ordered = [];
  let run = [];
  const flush = () => {
    const scored = run.map((ticket, index) => ({ ticket, index, score: score(ticket, nowEff, cfg) }));
    scored.sort((a, b) => b.score - a.score || simRank(a.ticket, nowEff, cfg, simsAfterTests) - simRank(b.ticket, nowEff, cfg, simsAfterTests) || (a.ticket.seq ?? 0) - (b.ticket.seq ?? 0) || a.index - b.index);
    for (const { ticket } of scored) ordered.push(ticket);
    run = [];
  };
  for (const record of raw) {
    if (record === null) {
      flush();
      ordered.push(null);
    } else {
      run.push(record);
    }
  }
  flush();
  return ordered;
}

/**
 * BRAIN-380 R4-2/R4-3: the effective view. `reservations` are `{id, seq}` for every ticket holding a
 * BRAIN-346 reservation latch, `seq` being its `reservationSeq`. Exactly one is ACTIVE: the lowest `seq`
 * whose owner is queued and not behind an unreadable (`null`) record. The owner is moved to index 0 and
 * everything else keeps its relative order; the other reservations are dormant (they reserve nothing).
 * A sim-class owner with a non-sim ticket ahead of it in `ordered` is dormant too (ROG-2181).
 * Never a timestamp: equal wall times or a clock rollback cannot reorder reservations.
 */
export function promoteReservationOwner(ordered, reservations) {
  const barrier = ordered.indexOf(null);
  const reach = barrier === -1 ? ordered.length : barrier;
  const byAge = [...reservations].sort((a, b) => a.seq - b.seq || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  for (const { id } of byAge) {
    const index = ordered.findIndex((t) => t !== null && t.id === id);
    if (index === -1 || index >= reach) continue;
    // ROG-2181: a sim reservation owner never overrides a test ticket that ranks ahead of it; its reservation stays dormant.
    if (ordered[index].class === 'sim' && ordered.slice(0, index).some((t) => t !== null && t.class !== 'sim')) continue;
    const queue = index === 0 ? ordered : [ordered[index], ...ordered.slice(0, index), ...ordered.slice(index + 1)];
    return { queue, ownerId: id };
  }
  return { queue: ordered, ownerId: null };
}
