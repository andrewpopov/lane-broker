/**
 * BRAIN-403 v1: exclusive lanes. Pure predicates and the one deny rule; nothing here touches the filesystem, the
 * clock or the lock. An exclusive ticket claims the whole admission budget while it runs and, while it is the
 * effective head of the queue, nothing else is admitted. v1 is stateless (no hold file) and hookless: the only
 * durable facts are the ticket's `exclusive` flag and the full-budget lease it is admitted with.
 */

/** The capability a broker advertises when it enforces the exclusive head gate (`exclusive/1`). */
export const EXCLUSIVE_CAPABILITY = 'exclusive/1';

/** Config keys that belong to the not-yet-supported acquire/release hook lifecycle (a BRAIN-403 follow-up). */
export const UNSUPPORTED_HOOK_KEYS = ['exclusiveHooks', 'acquire', 'release'];

export function isExclusive(ticket) {
  return ticket?.exclusive === true;
}

/**
 * BRAIN-452: a non-exclusive lane may not weigh more than half the weight capacity (a near-capacity weight silently serializes
 * the machine; `exclusive: true` says so honestly and `cpuCores` sizes a lane). `limit` is the largest weight it may hold.
 */
export function oversizedWeight(weight, capacity, exclusive) {
  const limit = Math.max(1, Math.floor(capacity / 2)); // a weight of 1 is the smallest a lane can be, so it is never oversized
  if (exclusive === true || !(Number(weight) > Math.max(1, capacity / 2))) return null;
  return { weight, limit };
}

export function oversizedWeightMessage(weight) {
  return `weight ${weight} would hold most of this machine's capacity; declare \`exclusive: true\` or use \`cpuCores\` for size (BRAIN-452)`;
}

/** A queued ticket with its weight clamped to the shared limit (an older config enqueued it); same object when it is not oversized. */
export function clampQueuedWeight(ticket, capacity) {
  const over = ticket && oversizedWeight(ticket.weight, capacity, ticket.exclusive);
  return over ? { ...ticket, weight: over.limit, weightClampedFrom: ticket.weight } : ticket;
}

/** The hook keys present at the top level of a parsed config object, in `UNSUPPORTED_HOOK_KEYS` order. */
export function unsupportedHookKeys(config) {
  if (!config || typeof config !== 'object' || Array.isArray(config)) return [];
  return UNSUPPORTED_HOOK_KEYS.filter((key) => Object.hasOwn(config, key));
}

export function hookRefusalMessage(sourcePath, keys) {
  return `${sourcePath}: ${keys.map((k) => `"${k}"`).join(', ')} not supported yet (BRAIN-403 follow-up); exclusive lanes are hookless in this release`;
}

/**
 * The walk view: exclusive tickets behind the head are invisible to every skip walk and selector, so an exclusive
 * starts ONLY as the effective head. Position 0 and unreadable (null) barriers are kept as they are.
 */
export function withoutBackfillExclusives(queue) {
  return queue.filter((t, i) => i === 0 || t === null || !isExclusive(t));
}

/**
 * The exclusive deny for one decision, or null when the ticket may continue to the ordinary selection.
 * A held exclusive lease denies everything (independent of weight arithmetic, which a 1e-20 weight defeats);
 * an exclusive effective head denies everyone but itself, and the head only once nothing else is held.
 */
export function exclusiveDeny(queue, ticket, held) {
  const holder = held.find(isExclusive);
  if (holder) return { reason: 'exclusive-held', holder: holder.id };
  const head = queue[0];
  if (!isExclusive(head)) return null;
  if (ticket.id !== head.id) return { reason: 'exclusive-head', holder: head.id };
  if (held.length > 0) return { reason: 'exclusive-draining', running: held.length };
  return null;
}

/** Is `ticket` the exclusive effective head (the only thing that may start under the gate)? */
export function isExclusiveHead(queue, ticket) {
  return isExclusive(queue[0]) && queue[0].id === ticket.id;
}

/** Lease fields that make an exclusive run claim the whole admission budget, so every capacity and resource path refuses the rest. */
export function exclusiveLeaseFields({ weightCapacity, cpuBudget, memoryBudgetBytes, declaredResources }) {
  return {
    exclusive: true,
    weight: weightCapacity,
    resources: { cpuCores: cpuBudget, memoryBytes: memoryBudgetBytes },
    declaredResources,
  };
}

/** What `lane status` shows for a queued exclusive head: who, and how many running lanes it is still waiting for. */
export function exclusiveHoldView(queue, held) {
  const head = queue[0];
  return isExclusive(head) ? { id: head.id, key: head.key, waitingFor: held.length } : null;
}
