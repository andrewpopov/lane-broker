import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, waitFor, sleep } from './helpers.js';
import { paths } from '../src/state.js';
import { readLease, isGroupAlive } from '../src/lease.js';

test('SIGTERM to `lane run` cancels the child group and releases the lease only after the group is gone', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '30'], { env, cwd: repoDir });

  const leaseId = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    return names[0] ? names[0].replace(/\.json$/, '') : null;
  });
  const lease = await waitFor(() => {
    const l = readLease(state, leaseId);
    return l && l.childPgid ? l : null;
  });
  assert.ok(isGroupAlive(lease.childPgid), 'the child group should be alive before cancellation');

  let sawLeaseGoneWhileGroupAlive = false;
  const poll = setInterval(() => {
    const stillHasLease = fs.existsSync(path.join(paths(state).leases, `${leaseId}.json`));
    if (!stillHasLease && isGroupAlive(lease.childPgid)) {
      sawLeaseGoneWhileGroupAlive = true;
    }
  }, 10);

  process.kill(child.pid, 'SIGTERM');

  await waitFor(() => !fs.existsSync(path.join(paths(state).leases, `${leaseId}.json`)), { timeoutMs: 15000 });
  clearInterval(poll);
  await sleep(50);

  assert.equal(sawLeaseGoneWhileGroupAlive, false, 'the lease must never disappear while the child group is still alive');
  assert.equal(isGroupAlive(lease.childPgid), false, 'the child group must be gone once the lease is released');

  if (child.exitCode === null && child.signalCode === null) {
    await new Promise((resolve) => child.on('exit', resolve));
  }
});
