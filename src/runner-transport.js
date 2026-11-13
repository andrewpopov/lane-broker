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
