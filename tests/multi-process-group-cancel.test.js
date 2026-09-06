import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, waitFor, sleep } from './helpers.js';
import { paths } from '../src/state.js';
import { readLease, isGroupAlive } from '../src/lease.js';

test('SIGTERM to `lane run` still kills a TERM-ignoring grandchild before releasing the lease', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  // The grandchild traps and ignores TERM; only SIGKILL (the escalation step)
  // can end it. Its parent (this process) responds normally to TERM.
  const cmd = ['sh', '-c', "sh -c 'trap \"\" TERM; sleep 30' & wait"];
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...cmd], { env, cwd: repoDir });

  const leaseId = await waitFor(() => {
    try {
      const names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
      return names[0] ? names[0].replace(/\.json$/, '') : null;
    } catch {
      return null;
    }
  });
  const lease = await waitFor(() => {
    const l = readLease(state, leaseId);
    return l && l.childPgid ? l : null;
  });
  // Give the grandchild's trap a moment to install.
  await sleep(300);
  assert.ok(isGroupAlive(lease.childPgid), 'the process group should be alive before cancellation');

  process.kill(child.pid, 'SIGTERM');

  await waitFor(() => !fs.existsSync(path.join(paths(state).leases, `${leaseId}.json`)), { timeoutMs: 15000 });
  assert.equal(isGroupAlive(lease.childPgid), false, 'the whole process group, including the TERM-ignoring grandchild, must be gone');

  if (child.exitCode === null && child.signalCode === null) {
    await new Promise((resolve) => child.on('exit', resolve));
  }
});
