import fs from 'node:fs';
import path from 'node:path';
import { cancelCommand } from '../cancel.js';
import { trackerForLease, hasLiveMembers } from '../descendants.js';
import { isGroupAlive, leaseFile } from '../lease.js';
import { isConfirmedGone, isVerifiedProcess } from '../process-liveness.js';
import { ensureStateDirs } from '../state.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const SUPERVISOR_EXIT_WAIT_MS = 5000;

/** The file's text, or null when it is genuinely absent; every other read error surfaces, because an unreadable record is not an absent one. */
function readOrNull(file) {
  try {
    return fs.readFileSync(file, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
}

async function untilTrue(check, waitMs) {
  for (const deadline = Date.now() + waitMs; !check() && Date.now() < deadline;) await sleep(50);
  return check();
}

/**
 * PKG-397: the graceful cancel did not stop the run (the supervisor is stopped or wedged, which `lane cancel` refuses to take over).
 * Nothing is signalled and nothing is proven without positive evidence: the lease and its supervisor identity must be readable and the
 * start time must match before the supervisor is SIGKILLed; `lane cancel`'s own orphaned-lease takeover (which itself signals the command
 * group only while a live member proves it is still this lease's) then reaps the rest, bounded by `deadline`. The stop is proven only when the
 * supervisor, the command group and every recorded or lease-marked member are positively gone. Anything unknown returns false.
 */
export async function forceStopRun({ remoteRoot, record, deadline }) {
  const leaseId = readOrNull(path.join(path.resolve(remoteRoot), 'tickets', record.execId, 'remote-id'))?.trim();
  if (!leaseId) return false;
  const leaseText = readOrNull(leaseFile(ensureStateDirs().root, leaseId));
  if (leaseText === null) return false;
  const lease = JSON.parse(leaseText);
  const { supervisorPid: pid, supervisorStart: start } = lease;
  if (!isConfirmedGone(pid, start)) {
    if (!isVerifiedProcess(pid, start)) return false;
    process.kill(pid, 'SIGKILL');
    if (!(await untilTrue(() => isConfirmedGone(pid, start), Math.min(SUPERVISOR_EXIT_WAIT_MS, Math.max(0, deadline - Date.now()))))) return false;
  }
  const { exitCode } = await cancelCommand(leaseId, { quiet: true, deadline });
  return exitCode === 0 && !isGroupAlive(lease.childPgid) && !hasLiveMembers(trackerForLease(lease));
}
