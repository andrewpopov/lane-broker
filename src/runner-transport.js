import { fetchRemoteResult, remoteCancel, remoteWithdraw } from './remote-client.js';

/**
 * The seam between "ask the runner about ticket <id>" and how that is carried. `{fetchResult(id), cancel(id), withdraw(id)}` with
 * the return shapes of the ssh calls they wrap: the record, `{missing:true}` or null; whether the cancel was confirmed; `{action}`.
 */
export function sshTransport(runner, { sshBin = 'ssh', env, fetchDeadlineMs, cancelDeadlineMs, withdrawDeadlineMs } = {}) {
  return {
    fetchResult: (ticketId, signal) => fetchRemoteResult(runner, ticketId, sshBin, fetchDeadlineMs, env, signal),
    cancel: (ticketId) => remoteCancel(runner, ticketId, { sshBin, env, ...(cancelDeadlineMs === undefined ? {} : { deadlineMs: cancelDeadlineMs }) }),
    withdraw: (ticketId) => remoteWithdraw(runner, ticketId, { sshBin, env, ...(withdrawDeadlineMs === undefined ? {} : { deadlineMs: withdrawDeadlineMs }) }),
  };
}

const HELD = new Set(['claimed', 'running']);
const WITHDRAW_ACTION = { withdrawn: 'withdrawn', missing: 'no-such-ticket', claimed: 'started', running: 'started', finished: 'finished', cancelled: 'cancelled' };

/** The same three calls over the lane queue: a runner-authored result is the record; a row still queued or held is `{missing, state}`; a failed call is null/false/unknown, as over ssh. */
export function pullTransport(queue) {
  return {
    async fetchResult(ticketId) {
      let row;
      try {
        row = await queue.get(ticketId);
      } catch {
        return null;
      }
      if (row === null) return { missing: true, state: 'gone' };
      if (row.state === 'queued') return { missing: true, state: 'queued' };
      if (HELD.has(row.state)) return { missing: true, state: 'running' };
      return row.result && row.result.author !== 'queue' ? row.result : { missing: true, state: 'gone' };
    },
    async cancel(ticketId) {
      try {
        return (await queue.cancel(ticketId)).confirmed === true;
      } catch {
        return false;
      }
    },
    async withdraw(ticketId) {
      try {
        return { action: WITHDRAW_ACTION[await queue.withdraw(ticketId)] ?? 'not-queued' };
      } catch {
        return { action: 'unknown' };
      }
    },
  };
}

export const transportFor = (attempt, { runner, queue, ...sshOptions }) => (attempt.transport === 'pull' ? pullTransport(queue) : sshTransport(runner, sshOptions));
