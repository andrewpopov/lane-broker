import path from 'node:path';
import { readAttempt } from './attempts.js';
import { adoptsPlacement } from './placement.js';
import { enqueueAckFile } from './pull-enqueue.js';
import { existingTicket } from './scheduler.js';
import { envelopeFile } from './pull-envelope.js';
import { paths, readJsonSafe } from './state.js';

/**
 * The placement this machine recorded for ticket `id`, or undefined when it has no record of the id: the live attempt's, else the retained pull envelope's,
 * else the published result's, else an ack with no envelope left (which cannot vouch for one, so reads as unconstrained). Reads only, no validation or directory creation, so it may run under the broker lock.
 */
export function recordedPlacement(root, id) {
  const attempt = readAttempt(root, id);
  if (attempt !== null) return attempt.placementRequest ?? null;
  const envelope = readJsonSafe(envelopeFile(root, id));
  if (envelope !== null) return envelope.placementRequest ?? null;
  const result = readJsonSafe(path.join(paths(root).results, `${id}.json`));
  if (result !== null) return result.placementRequest ?? null;
  return readJsonSafe(enqueueAckFile(root, id)) === null ? undefined : null;
}

/** Whether a request for `placement` may be answered by whatever this machine recorded under `id`. */
export const idAdoptsPlacement = (root, id, placement) => {
  const recorded = recordedPlacement(root, id);
  return recorded === undefined || adoptsPlacement(recorded, placement);
};

/** Under the broker lock: the ticket already known under `id`, flagged `placementMismatch` when a placed request is not what it was placed as; null when the id is new. */
export function duplicateSubmission(root, id, placement) {
  const existing = existingTicket(root, id);
  return existing === null ? null : { ...existing, placementMismatch: !idAdoptsPlacement(root, id, placement) };
}
