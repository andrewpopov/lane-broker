import { cancelCommand } from '../cancel.js';
import { trackerForLease, hasLiveMembers } from '../descendants.js';
import { isGroupAlive, isSupervisorAlive, readLease } from '../lease.js';
import { isProcessAlive } from '../process-liveness.js';
import { ensureStateDirs } from '../state.js';
import { readLaneId } from './claims.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const SUPERVISOR_EXIT_WAIT_MS = 5000;

async function untilDead(isAlive, waitMs) {
  for (const deadline = Date.now() + waitMs; isAlive() && Date.now() < deadline;) await sleep(50);
  return !isAlive();
}

/**
 * PKG-397: the graceful cancel did not stop the run (the supervisor is stopped or wedged, which `lane cancel` refuses to take over). SIGKILL the
 * supervisor by identity (a stopped process dies to it), then let `lane cancel`'s own orphaned-lease takeover reap the command's group and its
 * recorded descendants. The stop counts as proven only when the supervisor, the command group and every recorded or lease-marked member are gone.
 */
export async function forceStopRun({ remoteRoot, record }) {
  const root = ensureStateDirs().root;
  const leaseId = readLaneId(remoteRoot, record.execId);
  const lease = leaseId ? readLease(root, leaseId) : null;
  const supervisor = lease ? { pid: lease.supervisorPid, start: lease.supervisorStart ?? undefined } : { pid: record.pid, start: record.pidStart ?? undefined };
  const supervisorAlive = () => isProcessAlive(supervisor.pid, supervisor.start);
  if (supervisorAlive()) {
    try {
      process.kill(supervisor.pid, 'SIGKILL');
    } catch {
      // exited meanwhile
    }
  }
  if (!(await untilDead(supervisorAlive, SUPERVISOR_EXIT_WAIT_MS))) return false;
  if (lease === null) return true;
  await cancelCommand(leaseId, { quiet: true });
  return !isSupervisorAlive(lease) && !isGroupAlive(lease.childPgid) && !hasLiveMembers(trackerForLease(lease));
}

