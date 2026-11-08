import { listLeases } from './lease.js';
import { listQueueCapped, HELD_STATES } from './scheduler.js';
import { resolveClasses, classQueuePolicy } from './classes.js';
import { resolveScheduler, legacyStore, fairnessStore, effectiveView } from './fairness.js';
import { effectiveNow } from './priority-clock.js';
import { waitedMs } from './priority.js';
import { pushHolds } from './push-gate.js';
import { detectResourceCapacity } from './resources.js';

/**
 * BRAIN-888: advisory, read-only facts about where ticket `laneId` stands in the broker queue at `root`. It takes no lock and
 * reaps nothing, and runs the same pieces `collectStatus` does (class policy, the priority view with the reservation owner's
 * promotion) plus admission's push-hold exclusion, so `order` is `lane status`'s queue without the push-held tickets and
 * without the cap-denied ones that status lists after the order. Admission never reads this: it only decides whether a
 * submitter attempts a move, and the withdraw proof is what keeps a ticket from running twice.
 *
 * `behindOtherOwner` and `reservationOwnerWaitedMs` exist only in the validated v2 scheduler, where a reservation owner is
 * defined; `behindOtherOwner` means another eligible ticket ahead of ours holds the reservation. The owner's wait is its total
 * priority wait, the one the scheduler ages it by. Throws on any read failure: callers omit every fact then.
 */
export function queueFacts(root, cfg, laneId, now = Date.now(), { readQueue = listQueueCapped } = {}) {
  const sched = resolveScheduler(root, { log: false });
  const rawQueue = readQueue(root, cfg);
  const held = listLeases(root).filter((l) => HELD_STATES.has(l.state));
  const classes = resolveClasses(cfg, root);
  const classHost = classes.mode === 'off' && classes.valid ? null : detectResourceCapacity();
  const { eligible: classEligible, simsAfterTests } = classQueuePolicy(rawQueue, classes, held, classHost, cfg);
  const pushHeld = pushHolds(root, rawQueue, held);
  const eligible = (t) => classEligible(t) && !pushHeld.has(t.id);
  const nowEff = effectiveNow(root, now);
  const store = sched.v2 ? fairnessStore(root, sched.tickets) : legacyStore(root);
  const { queue, ownerId } = sched.v2 ? effectiveView(rawQueue, nowEff, cfg, store, eligible, { simsAfterTests }) : { queue: rawQueue, ownerId: null };
  const order = queue.filter((t) => t !== null && eligible(t));
  const index = order.findIndex((t) => t.id === laneId);
  const position = index < 0 ? null : index + 1;
  const facts = { v2: sched.v2 === true, order: order.map((t) => t.id), position, ownerId };
  if (!facts.v2 || position === null) return facts;
  const behindOtherOwner = ownerId !== null && ownerId !== laneId;
  return { ...facts, behindOtherOwner, ...(behindOtherOwner ? { reservationOwnerWaitedMs: waitedMs(order.find((t) => t.id === ownerId), nowEff) } : {}) };
}
