/**
 * BRAIN-380: pure priority scoring and queue ordering. Nothing here reads the clock, the
 * filesystem or the config file: callers pass `nowEff` (the priority clock, see priority-clock.js)
 * and the priority config slice, so every function is deterministic and testable on a fake clock.
 *
 * Slice 1 only BUILDS these; no selector calls `orderQueue` yet, so scheduling stays FIFO.
 */

export const PRIORITY_TIERS = ['low', 'medium', 'high'];
export const DEFAULT_PRIORITY = 'medium';

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

export function waitedMs(ticket, nowEff) {
  return Math.max(0, nowEff - originOrNow(ticket?.prioOriginAt, nowEff));
}

/**
 * Slurm-style multifactor score: `min(W_tier, W_tier * tierFactor + W_age * ageFactor)`.
 * The ceiling means aging can tie a fresh high but never pass it; at a tie, seq decides.
 */
export function score(ticket, nowEff, cfg) {
  const { tier, age } = cfg.priorityWeights;
  const tierFactor = TIER_BASE[priorityOf(ticket)] / MAX_BASE;
  const ageFactor = Math.min(1, waitedMs(ticket, nowEff) / cfg.priorityAgeMaxMs);
  return Math.min(tier, tier * tierFactor + age * ageFactor);
}

/** Display-only rank (0 low, 1 medium, 2 high): one tier gained per `priorityAgingMs` waited, capped at high. */
export function effectiveRank(ticket, nowEff, cfg) {
  return Math.min(MAX_BASE, TIER_BASE[priorityOf(ticket)] + Math.floor(waitedMs(ticket, nowEff) / cfg.priorityAgingMs));
}

export function tierName(rank) {
  return PRIORITY_TIERS[rank];
}

/**
 * The order every selector walks: (score desc, seq asc) within each maximal run of readable
 * records. An unreadable (`null`) record is a barrier: nothing moves across it, so the
 * scheduler's protection against an unreadable head survives.
 */
export function orderQueue(raw, nowEff, cfg) {
  const ordered = [];
  let run = [];
  const flush = () => {
    const scored = run.map((ticket, index) => ({ ticket, index, score: score(ticket, nowEff, cfg) }));
    scored.sort((a, b) => b.score - a.score || (a.ticket.seq ?? 0) - (b.ticket.seq ?? 0) || a.index - b.index);
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
