import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, sleep, waitFor } from './helpers.js';
import { paths } from '../src/state.js';
import { listQueue } from '../src/scheduler.js';

/**
 * BRAIN-185's claim: a waiting `lane run` reads global config once, so a
 * capacity raise never reaches already-queued heads. This drives the
 * end-to-end scenario the ticket describes: fill capacity with a long-running
 * lane, queue two more behind it, raise capacity mid-flight with no
 * resubmission, and assert the queued ones start without ever being re-run.
 *
 * A and B/C deliberately use DIFFERENT repos (and so different conflict
 * keys: `key` is `repoId:lane`) so B and C are blocked only by capacity, not
 * by the same-key mutual-exclusion conflict rule — mixing those two would
 * make B/C wait for A's lease to release regardless of capacity, which
 * proves nothing about the capacity path this ticket is about.
 */

function leaseExists(state, id) {
  return fs.existsSync(path.join(paths(state).leases, `${id}.json`));
}

async function spawnDetached(env, repoDir, repoName, sleepSecs) {
  const child = laneSpawn(['run', '--repo', repoName, '--lane', 'default', '--detach', '--', 'sleep', String(sleepSecs)], { env, cwd: repoDir });
  const id = await new Promise((resolve) => {
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.on('exit', () => resolve(out.trim()));
  });
  return id;
}

test('capacity raise: mid-flight config raise reaches already-queued heads without resubmission', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 100000, sampleMs: 150 });
  const repoADir = path.join(base, 'repoA');
  const repoBDir = path.join(base, 'repoB');
  const repoCDir = path.join(base, 'repoC');
  for (const dir of [repoADir, repoBDir, repoCDir]) {
    writeRepoConfig(dir, { version: 1, lanes: { default: { weight: 2 } } });
  }

  const idA = await spawnDetached(env, repoADir, 'ra', 30);
  assert.ok(idA, 'lane run --detach should print an id for A');
  const leaseA = await waitFor(() => leaseExists(state, idA), { timeoutMs: 5000 });
  assert.ok(leaseA, 'A should hold all capacity (weight 2 == capacity 2)');

  const idB = await spawnDetached(env, repoBDir, 'rb', 1);
  const idC = await spawnDetached(env, repoCDir, 'rc', 1);
  assert.ok(idB && idC, 'lane run --detach should print ids for B and C');

  const queued = await waitFor(() => {
    const q = listQueue(state).map((t) => t.id);
    return q.includes(idB) && q.includes(idC) ? q : null;
  }, { timeoutMs: 5000 });
  assert.ok(queued, 'B and C should both reach the queue');

  // A few poll cycles (sampleMs 150) at the original capacity: both must stay
  // capacity-blocked, not started.
  await sleep(450);
  assert.equal(leaseExists(state, idB), false, 'B must not start while capacity is exhausted by A');
  assert.equal(leaseExists(state, idC), false, 'C must not start while capacity is exhausted by A');

  // Raise capacity mid-flight, with NO resubmission of B or C.
  const raisedAt = Date.now();
  writeGlobalConfig(home, { version: 1, capacity: 6, loadClose: 100000, sampleMs: 150 });

  const leaseB = await waitFor(() => leaseExists(state, idB), { timeoutMs: 5000 });
  const leaseC = await waitFor(() => leaseExists(state, idC), { timeoutMs: 5000 });
  assert.ok(leaseB, 'B should start once the reloaded config raises capacity, without resubmission');
  assert.ok(leaseC, 'C should start once the reloaded config raises capacity, without resubmission');
  assert.ok(leaseExists(state, idA), 'A should still be running throughout');
  assert.ok(Date.now() - raisedAt < 2000, 'B/C should start within ~2s of the raise, not wait for A to finish (~30s)');

  await laneRun(['cancel', idA], { env, cwd: repoADir });
  await waitFor(() => !leaseExists(state, idB), { timeoutMs: 5000 });
  await waitFor(() => !leaseExists(state, idC), { timeoutMs: 5000 });
});

test('capacity raise control: without the raise, queued heads stay capacity-blocked (proves the test above is load-bearing)', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 100000, sampleMs: 150 });
  const repoADir = path.join(base, 'repoA');
  const repoBDir = path.join(base, 'repoB');
  const repoCDir = path.join(base, 'repoC');
  for (const dir of [repoADir, repoBDir, repoCDir]) {
    writeRepoConfig(dir, { version: 1, lanes: { default: { weight: 2 } } });
  }

  const idA = await spawnDetached(env, repoADir, 'ra', 30);
  assert.ok(idA, 'lane run --detach should print an id for A');
  const leaseA = await waitFor(() => leaseExists(state, idA), { timeoutMs: 5000 });
  assert.ok(leaseA, 'A should hold all capacity (weight 2 == capacity 2)');

  const idB = await spawnDetached(env, repoBDir, 'rb', 1);
  const idC = await spawnDetached(env, repoCDir, 'rc', 1);
  assert.ok(idB && idC, 'lane run --detach should print ids for B and C');

  const queued = await waitFor(() => {
    const q = listQueue(state).map((t) => t.id);
    return q.includes(idB) && q.includes(idC) ? q : null;
  }, { timeoutMs: 5000 });
  assert.ok(queued, 'B and C should both reach the queue');

  await sleep(450);
  assert.equal(leaseExists(state, idB), false, 'B must not start while capacity is exhausted by A');
  assert.equal(leaseExists(state, idC), false, 'C must not start while capacity is exhausted by A');

  // No config change here (the control): after another 2s, both should
  // still be blocked.
  await sleep(2000);
  assert.equal(leaseExists(state, idB), false, 'control: B must remain blocked without a capacity raise');
  assert.equal(leaseExists(state, idC), false, 'control: C must remain blocked without a capacity raise');

  await laneRun(['cancel', idA], { env, cwd: repoADir });
});
