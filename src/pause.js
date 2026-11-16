import fs from 'node:fs';
import { ensureStateDirs, paths, atomicWriteFile, withLock, LockTimeoutError, readRemoteDrain, readDrainMarkers, writeRemoteDrain, clearRemoteDrain, HUMAN_DRAIN_OWNER } from './state.js';
import { remoteShutdownCommand } from './remote-runner.js';
import { collectStatus, renderRemoteDrainLine, renderDrainMarkerLines } from './status.js';

export async function pauseCommand(reason = '') {
  const p = ensureStateDirs();
  await withLock(p.root, () => {
    atomicWriteFile(paths(p.root).pause, reason || 'paused by operator');
  });
  process.stdout.write(`lane paused${reason ? `: ${reason}` : ''}\n`);
  return { exitCode: 0 };
}

export async function resumeCommand() {
  const p = ensureStateDirs();
  await withLock(p.root, () => {
    try {
      fs.unlinkSync(paths(p.root).pause);
    } catch {
      // wasn't paused
    }
  });
  const drain = readRemoteDrain(p.root);
  process.stdout.write(`lane resumed${drain ? ` (still draining: ${drain.reason}; lane undrain to take remote work again)` : ''}\n`);
  return { exitCode: 0 };
}

/** BRAIN-520: take this host out of remote selection. Local admission, queued tickets and running leases are untouched. */
export async function drainCommand(reason = '', { owner = HUMAN_DRAIN_OWNER, expiresAt } = {}) {
  const p = ensureStateDirs();
  const marker = await writeRemoteDrain(p.root, reason, { owner, expiresAt });
  const status = await collectStatus();
  process.stdout.write(`lane draining: no new remote work is sent here; this host still runs its own\n${renderRemoteDrainLine(marker, { running: status.running.length, queued: status.queued.length })}\n${renderDrainMarkerLines(readDrainMarkers(p.root))}\n`);
  return { exitCode: 0 };
}

export async function undrainCommand({ owner = HUMAN_DRAIN_OWNER } = {}) {
  const p = ensureStateDirs();
  try {
    await clearRemoteDrain(p.root, owner);
  } catch (err) {
    process.stderr.write(`lane undrain: ${err.message}\n`);
    return { exitCode: 1 };
  }
  const pausedReason = fs.existsSync(paths(p.root).pause) ? fs.readFileSync(paths(p.root).pause, 'utf8').trim() : null;
  const holders = readDrainMarkers(p.root);
  const hints = [
    ...(pausedReason ? [`still paused: ${pausedReason}; lane resume to admit work again`] : []),
    ...(holders.length ? [`still draining: ${holders.map((m) => `${m.owner}: ${m.reason}`).join(', ')}`] : []),
  ];
  process.stdout.write(`lane undrained${hints.length ? ` (${hints.join('; ')})` : ''}\n`);
  return { exitCode: 0 };
}

/**
 * BRAIN-350: drain for a host shutdown: no new remote work, and every in-flight remote ticket stopped with a proof-of-death record.
 * `deadline` (ms epoch, default `waitMs` from now) is the one absolute time everything below must finish by.
 */
export async function drainShutdownCommand(reason = '', { waitMs = 60_000, root, deadline = Date.now() + waitMs } = {}) {
  const p = ensureStateDirs();
  try {
    await writeRemoteDrain(p.root, reason || 'host shutting down', { shutdown: true, timeoutMs: Math.max(0, deadline - Date.now()) });
  } catch (err) {
    process.stderr.write(`lane drain --shutdown: cannot write the drain marker: ${err.message}\n`);
    return { exitCode: err instanceof LockTimeoutError ? 75 : 1 };
  }
  return remoteShutdownCommand({ deadline, ...(root ? { root } : {}) });
}
