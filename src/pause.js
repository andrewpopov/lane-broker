import fs from 'node:fs';
import { ensureStateDirs, paths, atomicWriteFile } from './state.js';

export function pauseCommand(reason = '') {
  const p = ensureStateDirs();
  atomicWriteFile(paths(p.root).pause, reason || 'paused by operator');
  process.stdout.write(`lane paused${reason ? `: ${reason}` : ''}\n`);
  return { exitCode: 0 };
}

export function resumeCommand() {
  const p = ensureStateDirs();
  try {
    fs.unlinkSync(paths(p.root).pause);
  } catch {
    // wasn't paused
  }
  process.stdout.write('lane resumed\n');
  return { exitCode: 0 };
}
