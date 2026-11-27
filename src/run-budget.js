/**
 * One deadline for everything `lane run --timeout` bounds: a timeout of 0 or none is no deadline, any other is a single instant, and every queue call is given what is left of it
 * (connect and query limits that together cannot exceed it), never the original figure.
 */
export function timeoutBudget(timeoutMs, startedAt = Date.now(), now = Date.now) {
  const bounded = Number.isFinite(timeoutMs) && timeoutMs > 0;
  const left = () => (bounded ? startedAt + timeoutMs - now() : null);
  return {
    bounded,
    left,
    expired: () => bounded && left() <= 0,
    limits(connectCapMs, queryCapMs) {
      if (!bounded) return undefined;
      const connectTimeoutMs = Math.min(connectCapMs, Math.max(1, Math.floor(left() / 2)));
      return { attempts: 1, connectTimeoutMs, queryTimeoutMs: Math.min(queryCapMs, Math.max(1, Math.floor(left()) - connectTimeoutMs)) };
    },
  };
}
