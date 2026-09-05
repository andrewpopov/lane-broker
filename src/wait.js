import { ensureStateDirs, paths, readJsonSafe } from './state.js';
import { readLease } from './lease.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** `lane wait <id>`: reattach to a running/queued lease and block until its result exists. */
export async function waitCommand(id, { timeoutMs } = {}) {
  const root = ensureStateDirs().root;
  const resultPath = `${paths(root).results}/${id}.json`;

  let cancelling = false;
  const forwardCancel = () => {
    if (cancelling) return;
    cancelling = true;
    const lease = readLease(root, id);
    if (lease && lease.supervisorPid) {
      try {
        process.kill(lease.supervisorPid, 'SIGTERM');
      } catch {
        // supervisor already gone
      }
    }
  };
  process.on('SIGINT', forwardCancel);
  process.on('SIGTERM', forwardCancel);

  const deadline = timeoutMs ? Date.now() + timeoutMs : null;
  try {
    for (;;) {
      const result = readJsonSafe(resultPath);
      if (result) {
        if (result.signal) {
          process.stderr.write(`lane wait: command terminated by signal ${result.signal}\n`);
          return { exitCode: 1 };
        }
        return { exitCode: result.exit ?? 1 };
      }
      if (deadline && Date.now() > deadline) {
        process.stderr.write(`lane wait: waited ${timeoutMs}ms, not failed — id ${id} is still queued or running\n`);
        return { exitCode: 75 };
      }
      await sleep(200);
    }
  } finally {
    process.off('SIGINT', forwardCancel);
    process.off('SIGTERM', forwardCancel);
  }
}
