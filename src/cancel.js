import path from 'node:path';
import { ensureStateDirs, paths, appendHistory, atomicWriteJson, withLock, writeCancelMarkerFile } from './state.js';
import { readLease, removeLease, isSupervisorAlive, isGroupAlive } from './lease.js';
import { dequeueSync, listQueue } from './scheduler.js';
import { touchSimArmFor } from './sim-arm.js';
import { readAttempt, supervisorAlive, publishTerminal, remoteCancelledResult } from './attempts.js';
import { remoteCancel } from './remote-client.js';
import { loadGlobalConfig } from './config.js';
import { DescendantTracker } from './descendants.js';

const GRACE_MS = 10_000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** TERM the group, wait a grace period, KILL, verify gone. Used when there is no supervisor left to do it.
 *  Also reaps the descendants the dead supervisor last recorded on the lease (BRAIN-419), token-checked. */
async function killGroupDirectly(pgid, recordedDescendants) {
  if (!pgid) return;
  const descendants = DescendantTracker.fromSnapshot(null, recordedDescendants, { protectedPids: [process.pid, process.ppid] });
  const reaping = descendants.reap({ graceMs: GRACE_MS });
  try {
    await killGroupOnly(pgid);
  } finally {
    await reaping;
  }
}

async function killGroupOnly(pgid) {
  try {
    process.kill(-pgid, 'SIGTERM');
  } catch {
    return;
  }
  const deadline = Date.now() + GRACE_MS;
  while (Date.now() < deadline && isGroupAlive(pgid)) {
    await sleep(100);
  }
  if (isGroupAlive(pgid)) {
    try {
      process.kill(-pgid, 'SIGKILL');
    } catch {
      // gone
    }
    while (isGroupAlive(pgid)) {
      await sleep(50);
    }
  }
}

/**
 * Cancel a ticket still tracked ONLY as an attempt record (BRAIN-319 T3b-4,
 * C4/C5) -- mid remote-dispatch, or ORPHANED-REMOTE.
 *  - Live supervisor: write the cancel marker and SIGTERM it, exactly the
 *    lease path above -- the supervisor's own abort -> remote-cancel ->
 *    `publishTerminal` chain (supervisor.js) does the actual cancellation;
 *    this just waits for the attempt record to disappear as proof it did.
 *  - ORPHANED-REMOTE (dead supervisor): no supervisor left to react, so
 *    reconcile it here directly: best-effort `remote-cancel` against the
 *    runner recorded on the attempt (looked up BY NAME in the CURRENT
 *    global config -- a runner that has since been removed/renamed is
 *    reported and skipped, never a reason to fail the reconciliation), then
 *    `publishTerminal` with the shared cancelled-result shape, which also
 *    removes the record. Idempotent: once reconciled, a second call finds
 *    no attempt record (and no lease/queue entry) and falls through to the
 *    ordinary "nothing to cancel" branch above -- same as re-cancelling any
 *    already-finished ticket today.
 */
async function cancelAttempt(root, id, attempt) {
  if (supervisorAlive(attempt)) {
    writeCancelMarkerFile(root, id);
    try {
      process.kill(attempt.supervisor.pid, 'SIGTERM');
    } catch {
      // race: supervisor just exited
    }
    const deadline = Date.now() + GRACE_MS + 5000;
    while (Date.now() < deadline && readAttempt(root, id)) {
      await sleep(100);
    }
    const stillHeld = readAttempt(root, id);
    if (stillHeld) {
      process.stderr.write(
        `lane cancel: supervisor ${attempt.supervisor.pid} did not release ${id} within the grace period; ` +
          `remote attempt is still held (runner=${stillHeld.runner ?? '-'})\n`,
      );
      return { exitCode: 1 };
    }
    process.stdout.write(`lane cancel: cancelled ${id}\n`);
    return { exitCode: 0 };
  }

  // ORPHANED-REMOTE: reconcile directly, no supervisor left to do it.
  writeCancelMarkerFile(root, id);
  const globalCfg = loadGlobalConfig();
  const runnerCfg = (globalCfg.runners || []).find((r) => r.name === attempt.runner);
  if (runnerCfg) {
    await remoteCancel(runnerCfg, id);
  } else if (attempt.runner) {
    process.stderr.write(`lane cancel: runner "${attempt.runner}" is no longer configured; skipping remote-cancel\n`);
  }

  const resultPath = path.join(paths(root).results, `${id}.json`);
  const published = await publishTerminal(root, id, attempt.generation, () => {
    atomicWriteJson(resultPath, remoteCancelledResult(id, attempt.startedAt));
    appendHistory(root, { id, cancelled: true, endedAt: Date.now(), executor: 'remote', runner: attempt.runner });
  });
  if (!published.ok) {
    process.stderr.write(`lane cancel: could not reconcile orphaned remote attempt ${id}\n`);
    return { exitCode: 1 };
  }
  process.stdout.write(`lane cancel: cancelled orphaned remote attempt ${id} (runner ${attempt.runner ?? '-'})\n`);
  return { exitCode: 0 };
}

/** `lane cancel <id>`: cancels a queued ticket or a running/ORPHANED lease. */
export async function cancelCommand(id) {
  const root = ensureStateDirs().root;

  // Dequeue and mark cancelled atomically, under the same lock the scheduler
  // uses to move a ticket from queue to lease -- otherwise the supervisor
  // can win the race and start the ticket between our (unlocked) read of the
  // queue and the dequeue, and we'd report "removed" for a ticket that is
  // actually now running.
  const dequeuedHere = await withLock(root, () => {
    const stillQueued = listQueue(root).find((t) => t && t.id === id);
    if (!stillQueued) return false;
    dequeueSync(root, id);
    writeCancelMarkerFile(root, id);
    touchSimArmFor(root, stillQueued);
    return true;
  });
  if (dequeuedHere) {
    process.stdout.write(`lane cancel: removed queued ticket ${id}\n`);
    return { exitCode: 0 };
  }

  const lease = readLease(root, id);
  if (!lease) {
    // BRAIN-319 T3b-4 (C4): a ticket that fell back to local already shows
    // up as a queue entry or a lease above/below -- this branch is reached
    // only while it is still genuinely mid remote-dispatch (or ORPHANED-
    // REMOTE), so today's queued/leased cancel behaviour is unchanged.
    const attempt = readAttempt(root, id);
    if (attempt) {
      return cancelAttempt(root, id, attempt);
    }
    process.stderr.write(`lane cancel: no queued ticket or lease for ${id}\n`);
    return { exitCode: 1 };
  }

  writeCancelMarkerFile(root, id);

  if (isSupervisorAlive(lease)) {
    try {
      process.kill(lease.supervisorPid, 'SIGTERM');
    } catch {
      // race: supervisor just exited
    }
    const deadline = Date.now() + GRACE_MS + 5000;
    while (Date.now() < deadline && readLease(root, id)) {
      await sleep(100);
    }
    const stillHeld = readLease(root, id);
    if (stillHeld) {
      // The supervisor is alive but did not react (e.g. stopped/starved) --
      // do not take over killing it ourselves, since it may resume and race
      // our cleanup. Report honestly instead of lying about success.
      process.stderr.write(
        `lane cancel: supervisor ${lease.supervisorPid} did not release ${id} within the grace period; ` +
          `lease is still held (pgid=${stillHeld.childPgid ?? '-'})\n`,
      );
      return { exitCode: 1 };
    }
    process.stdout.write(`lane cancel: cancelled ${id}\n`);
    return { exitCode: 0 };
  }

  // ORPHANED: no supervisor left to react to the cancel request. Do the kill
  // sequence ourselves, then release.
  await killGroupDirectly(lease.childPgid, lease.descendants);

  // BRAIN-319 (older bug, fixed alongside P1/P2/P3): a ticket that fell back
  // to local and was ADMITTED (leased) still has its attempt record --
  // attempts.js's own doc comment is explicit that it survives until
  // `publishTerminal`, exactly like the still-queued/mid-remote-dispatch
  // cases `cancelAttempt` above already finalizes through. Writing the
  // plain lease-only SIGKILL result directly here (as this used to) leaves
  // that record stale once the lease is removed below: a LATER `lane
  // cancel` on the same id would find no lease, find the stale attempt, and
  // "reconcile" it via `cancelAttempt`'s ORPHANED-REMOTE branch --
  // overwriting this already-published result with a synthetic one.
  // Publishing through the SAME `publishTerminal` transaction removes the
  // attempt atomically with the result write, and reuses `remoteCancelled
  // Result` (exit 130, signal null) -- the same shape every other
  // attempt-tracked cancellation path in this file already produces --
  // rather than the plain-lease SIGKILL/exit-null shape, so an
  // attempt-tracked ticket's cancellation result looks the same regardless
  // of which of the three reconciliation paths actually caught it.
  const attempt = readAttempt(root, id);
  if (attempt) {
    const published = await publishTerminal(root, id, attempt.generation, () => {
      atomicWriteJson(lease.resultPath, { ...remoteCancelledResult(id, attempt.startedAt), executor: attempt.executor });
      appendHistory(root, { id, key: lease.key, cancelled: true, endedAt: Date.now(), executor: attempt.executor });
    });
    if (!published.ok) {
      process.stderr.write(`lane cancel: could not reconcile orphaned lease ${id} against its own attempt record\n`);
      return { exitCode: 1 };
    }
  } else {
    // Plain lease-only ticket (no attempt record) -- unchanged behaviour.
    const endedAt = Date.now();
    atomicWriteJson(lease.resultPath, { id, exit: null, signal: 'SIGKILL', startedAt: null, endedAt, waitedMs: null, cancelled: true });
    appendHistory(root, { id, key: lease.key, cancelled: true, endedAt, executor: 'local' });
  }
  removeLease(root, id);
  touchSimArmFor(root, lease);
  process.stdout.write(`lane cancel: cancelled orphaned lease ${id}\n`);
  return { exitCode: 0 };
}
