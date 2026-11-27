/**
 * One deadline for everything `lane run --timeout` bounds: a timeout of 0 or none is no deadline, any other is a single instant. Each queue call is given that instant
 * (`deadlineAt`), so connect, query and teardown together cannot outlast it, and a wait is given what is left, read once.
 */
export function timeoutBudget(timeoutMs, startedAt = Date.now(), now = Date.now) {
  const bounded = Number.isFinite(timeoutMs) && timeoutMs > 0;
  const deadlineAt = startedAt + timeoutMs;
  const left = () => (bounded ? deadlineAt - now() : null);
  return {
    bounded,
    left,
    expired: () => bounded && left() <= 0,
    limits: (connectCapMs, queryCapMs) => (bounded ? { attempts: 1, connectTimeoutMs: connectCapMs, queryTimeoutMs: queryCapMs, deadlineAt } : undefined),
    waitLeft() {
      const ms = left();
      if (ms === null) return { expired: false, timeoutMs: undefined };
      return ms <= 0 ? { expired: true } : { expired: false, timeoutMs: Math.ceil(ms) };
    },
  };
}
