import { readAttempt, updateAttempt } from './attempts.js';
import { DEFAULT_GLOBAL_CONFIG } from './config.js';
import { readEnvelope, withTicketLock } from './pull-envelope.js';

export const STALE_QUEUE_REF = 'stale-queue-ref';
export const ENQUEUE_CROSSED_RESTORE = 'enqueue-crossed-restore';
export const ENQUEUE_PAST_RETENTION = 'enqueue-past-retention';

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
    cpuCores: envelope.cpuCores,
    memoryBytes: envelope.memoryBytes,
    priority: envelope.priority,
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
export function enqueueOnce({ root, id, generation, queue, ledgered = () => false, now = Date.now, rowRetentionMs = DEFAULT_GLOBAL_CONFIG.lanePull.rowRetentionMs, lockTimeoutMs }) {
  return withTicketLock(root, id, async () => {
    if (ledgered()) return { outcome: 'settled' };
    const args = enqueueArgs(readEnvelope(root, id));
    const intent = intentOf(readAttempt(root, id));
    if (intent.seenRowAt !== null) return { outcome: 'queued', view: null };
    if (crossedRestore(intent.attempts)) return { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE };
    if (pastRetention(intent.attempts, now(), rowRetentionMs)) return { outcome: 'lost', reason: ENQUEUE_PAST_RETENTION };

    const ref = intent.attempts.find(mayHaveBeenSent)?.ref ?? (await queue.queueRef());
    let sending;
    await persistIntent({ root, id, generation }, (current) => {
      sending = { n: current.attempts.length + 1, ref, answer: null, sentAt: now() };
      return { ...current, attempts: [...current.attempts, sending] };
    });

    const view = await queue.enqueueFenced({ expectRef: ref, ...args });
    const stale = view.state === STALE_QUEUE_REF;
    const settled = await persistIntent({ root, id, generation }, (current) => ({
      attempts: current.attempts.map((attempt) => (attempt.n === sending.n ? { ...attempt, answer: stale ? 'stale' : 'row' } : attempt)),
      seenRowAt: stale ? null : new Date(now()).toISOString(),
    }));

    if (!stale) return { outcome: 'queued', view };
    return crossedRestore(settled.attempts) ? { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE } : { outcome: 'refresh' };
  }, { lockTimeoutMs });
}
