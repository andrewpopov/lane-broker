import fs from 'node:fs';
import { ensureStateDirs, paths, atomicWriteFile, withLock, readRemoteDrain, writeRemoteDrain, clearRemoteDrain } from './state.js';
import { collectStatus, renderRemoteDrainLine } from './status.js';

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
export async function drainCommand(reason = '') {
  const p = ensureStateDirs();
  const marker = await writeRemoteDrain(p.root, reason);
  const status = await collectStatus();
  process.stdout.write(`lane draining: no new remote work is sent here; this host still runs its own\n${renderRemoteDrainLine(marker, { running: status.running.length, queued: status.queued.length })}\n`);
  return { exitCode: 0 };
}

export async function undrainCommand() {
  const p = ensureStateDirs();
  try {
    await clearRemoteDrain(p.root);
  } catch (err) {
    process.stderr.write(`lane undrain: ${err.message}\n`);
    return { exitCode: 1 };
  }
  const pausedReason = fs.existsSync(paths(p.root).pause) ? fs.readFileSync(paths(p.root).pause, 'utf8').trim() : null;
  process.stdout.write(`lane undrained${pausedReason ? ` (still paused: ${pausedReason}; lane resume to admit work again)` : ''}\n`);
  return { exitCode: 0 };
}
