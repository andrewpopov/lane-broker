import path from 'node:path';
import fs from 'node:fs';
import { paths, atomicWriteJson, readJsonSafe, bootId, listJsonRecordsStrict } from './state.js';
import { trackerForLease, reapLogLine } from './descendants.js';
import { writeBrokerLog } from './admission.js';
import { isPidAlive, processStartTime, isProcessAlive } from './process-liveness.js';
import { touchSimArmFor } from './sim-arm.js';

export { isPidAlive, processStartTime };

export const LEASE_STATE = { RUNNING: 'RUNNING', ORPHANED: 'ORPHANED', DONE: 'DONE' };

/**
 * How long a reader tolerates a ticket id being neither queued, leased, nor
 * resulted before concluding it doesn't exist. A `lane run`/`lane wait`
 * supervisor is spawned before it has enqueued its own ticket, and
 * `tryStart` (scheduler.js) dequeues a ticket and only then writes its lease
 * -- an unlocked reader can land in either gap. One shared constant so both
 * readers (src/wait.js, src/run.js) tolerate the same race the same way,
 * rather than each guessing its own window.
 */
export const NOT_FOUND_GRACE_MS = 3000;

export function leaseFile(root, id) {
  return path.join(paths(root).leases, `${id}.json`);
}

export function writeLease(root, lease) {
  atomicWriteJson(leaseFile(root, lease.id), lease);
}

export function readLease(root, id) {
  return readJsonSafe(leaseFile(root, id));
}

export function removeLease(root, id) {
  try {
    fs.unlinkSync(leaseFile(root, id));
  } catch {
    // already gone
  }
}

export function listLeases(root) {
  const dir = paths(root).leases;
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return [];
  }
  return names
    .filter((n) => n.endsWith('.json'))
    .map((n) => readJsonSafe(path.join(dir, n)))
    .filter(Boolean);
}

/** Every lease record, or a thrown `UnreadableRecordError`: for decisions that must not proceed past a record they cannot read. */
export function listLeasesStrict(root) {
  return listJsonRecordsStrict(paths(root).leases);
}

export function isGroupAlive(pgid) {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Is the supervisor that holds this lease still alive? Fails closed: if the
 * liveness probe itself cannot be completed (e.g. `ps` cannot fork under
 * load), the lease is kept rather than declared dead — a transient probe
 * failure must never look like the supervisor exited.
 */
export function isSupervisorAlive(lease) {
  return isProcessAlive(lease.supervisorPid, lease.supervisorStart);
}

const ORPHAN_REAP_GRACE_MS = 10_000;
const ORPHAN_REAP_GIVE_UP_MS = 60_000;

/**
 * Dead supervisor, dead group: the lease is the only record of descendants that left the group (BRAIN-419), so it is
 * kept ORPHANED while any are alive, and every call advances a synchronous, non-blocking state machine so the lease
 * heals itself instead of waiting for `lane cancel`: first pass TERMs the live members and records `orphanReap`;
 * once the grace period has passed a later pass KILLs the survivors; any pass that finds none removes the lease.
 * Past ORPHAN_REAP_GIVE_UP_MS from the TERM (members unkillable, or the process table unreadable) the lease is
 * released anyway, with the incomplete-reap log line, so it can never block its lane forever.
 * Returns 'released' (lease removed) or 'held'. `clock.now`/`clock.kill` are injectable for tests.
 */
function stepOrphanReap(root, lease, { now = Date.now(), kill } = {}) {
  const release = (result) => {
    if (lease.orphanReap || !result.complete) writeBrokerLog(root, reapLogLine(lease.id, result));
    removeLease(root, lease.id);
    return 'released';
  };
  const tracker = trackerForLease(lease);
  const rows = tracker.scan();
  const live = rows ? tracker.live(rows) : null;
  if (live && live.length === 0) return release({ signalled: lease.orphanReap?.signalled ?? 0, survivors: [], complete: true });
  const orphanReap = lease.orphanReap ?? { termAt: now, signalled: 0 };
  if (now - orphanReap.termAt >= ORPHAN_REAP_GIVE_UP_MS) {
    return release({ signalled: orphanReap.signalled, survivors: live ?? [], complete: false });
  }
  const advance = (patch, signal) => {
    const signalled = signal && rows ? tracker.signalLive(rows, signal, kill) : [];
    if (signalled.length > 0) writeBrokerLog(root, reapLogLine(lease.id, { signalled: signalled.length, survivors: [], complete: true }));
    writeLease(root, {
      ...lease,
      state: LEASE_STATE.ORPHANED,
      descendants: tracker.snapshot(),
      orphanReap: { ...orphanReap, ...patch, signalled: orphanReap.signalled + signalled.length },
    });
  };
  if (!lease.orphanReap) advance({ termAt: now }, 'SIGTERM');
  else if (rows && !orphanReap.killAt && now - orphanReap.termAt >= ORPHAN_REAP_GRACE_MS) advance({ killAt: now }, 'SIGKILL');
  return 'held';
}

/**
 * Apply the exact reap rule to one lease. Returns one of:
 *  - 'kept'    the lease is untouched
 *  - 'reaped'  removed: boot changed, or (supervisor dead AND group gone AND no recorded/marked descendant alive)
 *  - 'orphaned' marked ORPHANED: supervisor dead but the child group or a descendant is alive (never auto-removed)
 */
export function reapIfStale(root, lease, currentBootId = bootId(), clock = {}) {
  if (lease.bootId !== currentBootId) {
    removeLease(root, lease.id);
    touchSimArmFor(root, lease);
    return 'reaped';
  }
  if (isSupervisorAlive(lease)) return 'kept';
  if (!isGroupAlive(lease.childPgid)) {
    const outcome = stepOrphanReap(root, lease, clock); // when held, it has already written the ORPHANED lease
    touchSimArmFor(root, lease);
    return outcome === 'released' ? 'reaped' : 'orphaned';
  }
  if (lease.state !== LEASE_STATE.ORPHANED) {
    writeLease(root, { ...lease, state: LEASE_STATE.ORPHANED });
  }
  touchSimArmFor(root, lease);
  return 'orphaned';
}

/** Reap/orphan every lease on disk. Caller must hold the global lock. */
export function reapAll(root, currentBootId = bootId()) {
  const results = [];
  for (const lease of listLeases(root)) {
    results.push({ id: lease.id, action: reapIfStale(root, lease, currentBootId) });
  }
  return results;
}
