import fs from 'node:fs';
import { ensureStateDirs, paths, atomicWriteFile, withLock } from './state.js';

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
  process.stdout.write('lane resumed\n');
  return { exitCode: 0 };
}
