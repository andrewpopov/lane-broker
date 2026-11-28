import { NO_PROGRESS_REASON, SILENCE_REASON } from './no-progress.js';

export const RUN_LIMIT_REASON = 'run-limit';
export const DEADLINE_EXCEEDED_REASON = 'deadline_exceeded';

/** Each reason a settled command can carry, mapped to the field holding the window that elapsed. */
const WINDOW_FIELD = Object.freeze({
  [NO_PROGRESS_REASON]: 'noProgressTimeoutMs',
  [SILENCE_REASON]: 'silenceTimeoutMs',
  [RUN_LIMIT_REASON]: 'runLimitMs',
  [DEADLINE_EXCEEDED_REASON]: 'deadlineMs',
});

export const SETTLEMENT_REASONS = Object.freeze(Object.keys(WINDOW_FIELD));

/**
 * The settlement fields every relay of a command's terminal record forwards, so a kill's reason survives each hop instead of reading as a bare exit.
 * A reason is relayed only with its finite window; `deadlineMs`, `deadline`, `guard` and `stopProven` ride with it. Empty for any other record.
 */
export function relayedSettlement(record) {
  const windowField = WINDOW_FIELD[record?.reason];
  if (windowField === undefined || !Number.isFinite(record[windowField])) return {};
  return {
    reason: record.reason,
    [windowField]: record[windowField],
    ...(Number.isFinite(record.deadlineMs) ? { deadlineMs: record.deadlineMs } : {}),
    ...(record.deadline && typeof record.deadline === 'object' ? { deadline: record.deadline } : {}),
    ...(typeof record.guard === 'string' ? { guard: record.guard } : {}),
    ...(typeof record.stopProven === 'boolean' ? { stopProven: record.stopProven } : {}),
  };
}
