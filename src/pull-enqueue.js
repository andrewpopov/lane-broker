import { readAttempt, updateAttempt } from './attempts.js';
import { DEFAULT_GLOBAL_CONFIG } from './config.js';
import { DISPATCH_KEY_REFUSED_EXIT, dispatchKeyRefusal } from './dispatch-key.js';
import { readEnvelope, withTicketLock } from './pull-envelope.js';

export const STALE_QUEUE_REF = 'stale-queue-ref';
export const ENQUEUE_CROSSED_RESTORE = 'enqueue-crossed-restore';
export const ENQUEUE_PAST_RETENTION = 'enqueue-past-retention';

export const DISPATCH_KEY_REFUSED = 'dispatch-key-refused';
export const DISPATCH_KEY_REFUSED_REASON = 'dispatch-key-refused';

const EMPTY_INTENT = { attempts: [], seenRowAt: null };

export function enqueueArgs(envelope) {
  if (envelope.snapshot === undefined) throw new Error(`pull envelope for ${envelope.logicalId} has no snapshot descriptor, so nothing may be enqueued`);
  return {
    id: envelope.logicalId,
    repoKey: envelope.repoKey,
    lane: envelope.lane,
    laneKey: envelope.laneKey,
    binding: envelope.binding,
    header: envelope.header,
    snapshot: envelope.snapshot,
    requiresTags: envelope.requiresTags,
    avoidTags: envelope.avoidTags ?? [],
    requires: envelope.requires ?? [],
    cpuCores: envelope.cpuCores,
    ...(envelope.minCpuCores === undefined ? {} : { minCpuCores: envelope.minCpuCores }),
    memoryBytes: envelope.memoryBytes,
    priority: envelope.priority,
    ...(envelope.minProtocol === undefined ? {} : { minProtocol: envelope.minProtocol }),
    ...(envelope.dispatchKey === undefined ? {} : { dispatchKey: envelope.dispatchKey }),
  };
}

const intentOf = (attempt) => attempt?.pull?.enqueue ?? EMPTY_INTENT;

// an attempt not durably stale may have inserted a row, so it pins the ref and, if the ref later goes stale, the ticket is lost
const mayHaveBeenSent = (attempt) => attempt.answer !== 'stale';

const crossedRestore = (attempts) => attempts.some((attempt, i) => attempt.answer === 'stale' && attempts.slice(0, i).some(mayHaveBeenSent));

const pastRetention = (attempts, nowMs, rowRetentionMs) => attempts.some((attempt) => mayHaveBeenSent(attempt) && nowMs - attempt.sentAt > rowRetentionMs);

async function persistIntent({ root, id, generation }, change) {
  let intent;
  const written = await updateAttempt(root, id, generation, (current) => {
    intent = change(intentOf(current));
    return { pull: { ...current.pull, enqueue: intent } };
  }, { fsync: true });
  if (!written.ok) throw new Error(`attempt ${id} is gone or moved past generation ${generation}; the enqueue intent was not recorded`);
  return intent;
}

// takes the ticket lock itself; `ledgered` is asked inside it so a ledgered decision is never followed by a send; returns queued, refresh (call again), lost or settled; a throw leaves the intent pending for the replay
export const CANCELLED_BEFORE_ENQUEUE = 'cancelled-before-enqueue';

export function enqueueOnce({ root, id, generation, queue, ledgered = () => false, cancelRequested = () => false, settleCancelled = async () => null, now = Date.now, rowRetentionMs = DEFAULT_GLOBAL_CONFIG.lanePull.rowRetentionMs, lockTimeoutMs }) {
  return withTicketLock(root, id, async () => {
    if (ledgered()) return { outcome: 'settled' };
    const args = enqueueArgs(readEnvelope(root, id));
    const intent = intentOf(readAttempt(root, id));
    if (intent.seenRowAt !== null) return { outcome: 'queued', view: null };
    // the queue already refused this submission for good: it is decided again from the record, never sent again
    if (intent.refusal !== undefined) return { outcome: DISPATCH_KEY_REFUSED, ...intent.refusal };
    if (crossedRestore(intent.attempts)) return { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE };
    if (pastRetention(intent.attempts, now(), rowRetentionMs)) return { outcome: 'lost', reason: ENQUEUE_PAST_RETENTION };
    // nothing has been sent, so a cancel that has arrived ends the ticket here and no row is ever created; checked under the lock, before the intent is recorded
    // settled here, still under the lock that proved nothing was sent, so no other stepper can record an intent between the proof and the decision
    if (intent.attempts.length === 0 && cancelRequested()) return { outcome: CANCELLED_BEFORE_ENQUEUE, settled: await settleCancelled() };

    const ref = intent.attempts.find(mayHaveBeenSent)?.ref ?? (await queue.queueRef());
    let sending;
    await persistIntent({ root, id, generation }, (current) => {
      sending = { n: current.attempts.length + 1, ref, answer: null, sentAt: now() };
      return { ...current, attempts: [...current.attempts, sending] };
    });

    // `ticketId` (the ticket the key already names) is kept on the intent so a detached caller can print it
    const refuse = async (refusal) => {
      await persistIntent({ root, id, generation }, (current) => ({ ...current, refusal, ...(refusal.ticketId === undefined ? {} : { heldTicketId: refusal.ticketId }), attempts: current.attempts.map((attempt) => (attempt.n === sending.n ? { ...attempt, answer: 'refused' } : attempt)) }));
      return { outcome: DISPATCH_KEY_REFUSED, ...refusal };
    };
    let view;
    try {
      view = await queue.enqueueFenced({ expectRef: ref, ...args });
    } catch (err) {
      const refusal = args.dispatchKey === undefined ? null : dispatchKeyRefusal(err);
      if (refusal === null) throw err;
      return refuse(refusal);
    }
    // the key already names another of this owner's tickets (a concurrent or earlier submit): nothing was inserted, and this id must not shadow that ticket
    if (args.dispatchKey !== undefined && view.id !== undefined && view.id !== id) {
      return refuse({ reason: `dispatch key ${args.dispatchKey} is already ticket ${view.id}; resubmit to adopt it`, exitCode: DISPATCH_KEY_REFUSED_EXIT, ticketId: view.id });
    }
    // a replay of a keyed enqueue whose row was withdrawn meanwhile is answered with that withdrawn row: not a queued ticket, so it is refused
    if (args.dispatchKey !== undefined && view.state === 'withdrawn') {
      return refuse({ reason: `dispatch key ${args.dispatchKey} names a withdrawn ticket; resubmit with a new key`, exitCode: DISPATCH_KEY_REFUSED_EXIT });
    }
    const stale = view.state === STALE_QUEUE_REF;
    const settled = await persistIntent({ root, id, generation }, (current) => ({
      attempts: current.attempts.map((attempt) => (attempt.n === sending.n ? { ...attempt, answer: stale ? 'stale' : 'row' } : attempt)),
      seenRowAt: stale ? null : new Date(now()).toISOString(),
    }));

    if (!stale) return { outcome: 'queued', view };
    return crossedRestore(settled.attempts) ? { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE } : { outcome: 'refresh' };
  }, { lockTimeoutMs });
}
