import { RUNNER_SHUTDOWN_REASON } from '../state.js';

/**
 * What proves that an execution can never run, or has stopped, from a local cancel's answer and the runner's result record; null when
 * neither does. A tombstone or an absent `remote-id` under the broker lock means it was never admitted; a record is proof only through
 * the explicit fields the runner writes (`neverStarted`, `stopProven`, a shutdown with its proof). Not evidence: `not-registered`,
 * `broker-cancel`, a result existing, or a `cancelled` label.
 */
export function stopEvidence(cancel, result) {
  if (cancel?.confirmedBy === 'tombstone' || cancel?.confirmedBy === 'no-remote-id') return 'never-admitted';
  if (!result || result.missing) return null;
  if (result.neverStarted === true) return 'never-started';
  if (result.stopProven === true) return 'stop-proven';
  if (result.reason === RUNNER_SHUTDOWN_REASON && result.shutdownProof) return 'shutdown-proof';
  return null;
}
