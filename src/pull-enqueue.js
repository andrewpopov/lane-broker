import { readAttempt, updateAttempt } from './attempts.js';
import { readEnvelope } from './pull-envelope.js';

export const STALE_QUEUE_REF = 'stale-queue-ref';
export const ENQUEUE_CROSSED_RESTORE = 'enqueue-crossed-restore';

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

async function persistIntent({ root, id, generation }, enqueue) {
  const current = readAttempt(root, id);
  const written = await updateAttempt(root, id, generation, { pull: { ...current?.pull, enqueue } }, { fsync: true });
  if (!written.ok) throw new Error(`attempt ${id} is gone or moved past generation ${generation}; the enqueue intent was not recorded`);
}

// returns queued, refresh (call again) or lost; a throw leaves the intent pending for the replay
export async function enqueueOnce({ root, id, generation, queue, now = Date.now }) {
  const args = enqueueArgs(readEnvelope(root, id));
  const intent = intentOf(readAttempt(root, id));
  if (intent.seenRowAt !== null) return { outcome: 'queued', view: null };
  if (crossedRestore(intent.attempts)) return { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE };

  const ref = intent.attempts.find(mayHaveBeenSent)?.ref ?? (await queue.queueRef());
  const sending = { n: intent.attempts.length + 1, ref, answer: null };
  await persistIntent({ root, id, generation }, { ...intent, attempts: [...intent.attempts, sending] });

  const view = await queue.enqueueFenced({ expectRef: ref, ...args });
  const stale = view.state === STALE_QUEUE_REF;
  const attempts = [...intent.attempts, { ...sending, answer: stale ? 'stale' : 'row' }];
  await persistIntent({ root, id, generation }, { attempts, seenRowAt: stale ? null : new Date(now()).toISOString() });

  if (!stale) return { outcome: 'queued', view };
  return crossedRestore(attempts) ? { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE } : { outcome: 'refresh' };
}
