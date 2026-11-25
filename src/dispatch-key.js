// PKG-438: a caller-supplied key that makes a submission idempotent. The queue owns the rule (migration 012); this is the same one for the CLI.
export const DISPATCH_KEY_RE = /^[A-Za-z0-9:._/-]{1,200}$/;
export const DISPATCH_KEY_REQUIRES_PULL = 'dispatch key needs the pull queue';

/** Exit of every refusal made before anything is enqueued because the key cannot be honoured (retryable, like the other no-runner refusals). */
export const DISPATCH_KEY_REFUSED_EXIT = 76;
/** A key another principal holds will not free up for this caller, so it is a usage error. */
export const DISPATCH_KEY_FOREIGN_EXIT = 64;

/** SQLSTATE the queue raises when the key belongs to another owner (migration 012). */
export const DISPATCH_KEY_HELD_SQLSTATE = 'LK409';
const UNDEFINED_FUNCTION_SQLSTATE = '42883';

export const isDispatchKey = (value) => typeof value === 'string' && DISPATCH_KEY_RE.test(value);

/**
 * How a queue's answer to a keyed enqueue ends it, or null when it is not a definite refusal. 42883 is a queue without the 19-argument
 * enqueue; the key is never trimmed off to fit it, so the submission is refused instead.
 */
export function dispatchKeyRefusal(err) {
  if (err?.code === UNDEFINED_FUNCTION_SQLSTATE) return { reason: `${DISPATCH_KEY_REQUIRES_PULL} with this version (the queue predates migration 012)`, exitCode: DISPATCH_KEY_REFUSED_EXIT };
  if (err?.code === DISPATCH_KEY_HELD_SQLSTATE) return { reason: 'dispatch key is held by another owner', exitCode: DISPATCH_KEY_FOREIGN_EXIT };
  return null;
}

/** The refusal of a keyed ticket that would run anywhere but the pull queue: it publishes like the other no-runner refusals and exits 76. */
export const dispatchKeyPullRefusal = (lane, reason) => ({
  ok: false,
  exitCode: DISPATCH_KEY_REFUSED_EXIT,
  message: `lane run: ${DISPATCH_KEY_REQUIRES_PULL}, so lane "${lane}" was not run${reason ? ` (${reason})` : ''}.\n`,
});
