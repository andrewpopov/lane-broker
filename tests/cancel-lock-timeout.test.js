import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor } from './helpers.js';
import { paths, withLock, isCancelled } from '../src/state.js';
import { isPidAlive } from '../src/lease.js';

// PKG-462: a cancel that cannot get the global lock still lands, as the durable marker the waiting supervisor acts on.
test('lane cancel under a held global lock writes the marker, exits 0, and the supervisor then finalizes it cancelled', async () => {
  const { base, home, state, env } = freshEnv({ LANE_BROKER_TEST_LOCK_TIMEOUT_MS: '300' });
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const STARTUP_TIMEOUT_MS = 60_000;
  const listing = (dir) => {
    try {
      return fs.readdirSync(dir).filter((n) => n.endsWith('.json'));
    } catch {
      return [];
    }
  };

  const blocker = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '30'], { env, cwd: repoDir });
  try {
    await waitFor(() => listing(paths(state).leases).length > 0, { timeoutMs: STARTUP_TIMEOUT_MS });
    const detached = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'true'], { env, cwd: repoDir });
    const id = detached.stdout.trim();
    assert.ok(id, `no id from detach: ${detached.stderr}`);
    await waitFor(() => listing(paths(state).queue).some((n) => n.includes(id)), { timeoutMs: STARTUP_TIMEOUT_MS });
    const ticket = JSON.parse(fs.readFileSync(path.join(paths(state).queue, listing(paths(state).queue).find((n) => n.includes(id))), 'utf8'));

    let cancel;
    await withLock(state, async () => {
      cancel = await laneRun(['cancel', id], { env, cwd: repoDir });
      assert.equal(cancel.code, 0, `cancel under a held lock: ${cancel.stderr}`);
      assert.match(cancel.stdout, /cancel requested .*marker written/);
      assert.equal(isCancelled(state, id), true, 'the durable cancel marker must exist');
      assert.ok(listing(paths(state).queue).some((n) => n.includes(id)), 'the ticket is still queued until the supervisor dequeues it');
    }, { timeoutMs: 10_000 });

    await waitFor(() => !isPidAlive(ticket.supervisorPid), { timeoutMs: 30_000 });
    assert.equal(listing(paths(state).queue).some((n) => n.includes(id)), false, 'the supervisor must dequeue the cancelled ticket');
  } finally {
    if (blocker.exitCode === null) blocker.kill('SIGKILL');
  }
});
