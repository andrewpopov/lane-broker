import fs from 'node:fs';
import path from 'node:path';
import { ensureStateDirs, paths, atomicWriteFile, appendHistory, atomicWriteJson, withLock } from './state.js';
import { readLease, removeLease, isSupervisorAlive, isGroupAlive } from './lease.js';
import { dequeueSync, listQueue } from './scheduler.js';

const GRACE_MS = 10_000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function writeCancelMarker(p, id) {
  fs.mkdirSync(p.cancel, { recursive: true });
  atomicWriteFile(path.join(p.cancel, id), String(Date.now()));
}

/** TERM the group, wait a grace period, KILL, verify gone. Used when there is no supervisor left to do it. */
async function killGroupDirectly(pgid) {
  if (!pgid) return;
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

/** `lane cancel <id>`: cancels a queued ticket or a running/ORPHANED lease. */
export async function cancelCommand(id) {
  const root = ensureStateDirs().root;
  const p = paths(root);

  // Dequeue and mark cancelled atomically, under the same lock the scheduler
  // uses to move a ticket from queue to lease -- otherwise the supervisor
  // can win the race and start the ticket between our (unlocked) read of the
  // queue and the dequeue, and we'd report "removed" for a ticket that is
  // actually now running.
  const dequeuedHere = await withLock(root, () => {
    const stillQueued = listQueue(root).find((t) => t && t.id === id);
    if (!stillQueued) return false;
    dequeueSync(root, id);
    writeCancelMarker(p, id);
    return true;
  });
  if (dequeuedHere) {
    process.stdout.write(`lane cancel: removed queued ticket ${id}\n`);
    return { exitCode: 0 };
  }

  const lease = readLease(root, id);
  if (!lease) {
    process.stderr.write(`lane cancel: no queued ticket or lease for ${id}\n`);
    return { exitCode: 1 };
  }

  writeCancelMarker(p, id);

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
  await killGroupDirectly(lease.childPgid);
  const endedAt = Date.now();
  atomicWriteJson(lease.resultPath, { id, exit: null, signal: 'SIGKILL', startedAt: null, endedAt, waitedMs: null, cancelled: true });
  appendHistory(root, { id, key: lease.key, cancelled: true, endedAt });
  removeLease(root, id);
  process.stdout.write(`lane cancel: cancelled orphaned lease ${id}\n`);
  return { exitCode: 0 };
}
