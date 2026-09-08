import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor } from './helpers.js';
import { paths, readJsonSafe } from '../src/state.js';
import { readLease, isGroupAlive } from '../src/lease.js';
import { resolveNicedSpawn } from '../src/supervisor.js';

// BRAIN-207: lanes are spawned under `nice -n <laneNice>` by default. `nice`
// EXECS the command in place (it doesn't fork+wait), so pid/pgid, exit code,
// and signal propagation must all be unaffected -- verified against a real
// supervisor run, not just read off the source.

async function firstLeaseWithPgid(state) {
  const leaseId = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    return names[0] ? names[0].replace(/\.json$/, '') : null;
  });
  return waitFor(() => {
    const l = readLease(state, leaseId);
    return l && l.childPgid ? l : null;
  });
}

test('a niced lane\'s child process actually runs at nice value >= laneNice', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, laneNice: 12 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '2'], { env, cwd: repoDir });
  const lease = await firstLeaseWithPgid(state);

  const niceValue = execFileSync('ps', ['-o', 'nice=', '-p', String(lease.childPgid)], { encoding: 'utf8' }).trim();
  assert.ok(Number(niceValue) >= 12, `expected nice >= 12, got "${niceValue}"`);

  await new Promise((resolve) => child.on('exit', resolve));
});

test('a niced lane still propagates the child\'s real exit code', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, laneNice: 10 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'exit 3'], { env, cwd: repoDir });
  assert.equal(result.code, 3, `nice execs in place, so the real exit code must still surface; stderr: ${result.stderr}`);
});

test('lane cancel on a niced RUNNING lane still kills the whole process group', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, laneNice: 10 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '30'], { env, cwd: repoDir });
  const lease = await firstLeaseWithPgid(state);
  assert.ok(isGroupAlive(lease.childPgid), 'the niced group should be alive before cancellation');

  const result = await laneRun(['cancel', lease.id], { env, cwd: repoDir });
  assert.equal(result.code, 0, `lane cancel should succeed; stderr: ${result.stderr}`);
  assert.equal(isGroupAlive(lease.childPgid), false, 'the whole niced process group must be gone');

  if (child.exitCode === null && child.signalCode === null) {
    await new Promise((resolve) => child.on('exit', resolve));
  }
});

test('a missing command under nice surfaces exit 127 by name (the wrapper, not a spawn error)', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, laneNice: 10 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const child = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'definitely-not-a-command-xyz'],
    { env, cwd: repoDir },
  );
  const id = await new Promise((resolve) => {
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.on('exit', () => resolve(out.trim()));
  });
  assert.ok(id, 'lane run --detach should print an id even though the command will fail');

  const resultPath = path.join(paths(state).results, `${id}.json`);
  const result = await waitFor(() => readJsonSafe(resultPath), { timeoutMs: 5000 });
  assert.equal(result.exit, 127, `nice execs the missing command in place and reports its own not-found exit; got ${JSON.stringify(result)}`);
  assert.equal(result.error, undefined, 'this is the wrapper exiting 127, not a structured node spawn-error result');
});

test('with laneNice: 0, the SAME missing command still produces the old structured spawn-error result (the contract change is bounded to the wrapper)', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, laneNice: 0 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const child = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'definitely-not-a-command-xyz'],
    { env, cwd: repoDir },
  );
  const id = await new Promise((resolve) => {
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.on('exit', () => resolve(out.trim()));
  });
  assert.ok(id, 'lane run --detach should print an id even though the command will fail');

  const resultPath = path.join(paths(state).results, `${id}.json`);
  const result = await waitFor(() => readJsonSafe(resultPath), { timeoutMs: 5000 });
  assert.equal(result.exit, 1, `a bare spawn of a missing command hits Node's spawn-error handler; got ${JSON.stringify(result)}`);
  assert.equal(typeof result.error, 'string', 'the old shape carries a structured spawn-error message');
});

test('laneNice: 0 spawns the bare command with no nice wrapper', () => {
  // ps/command inspection can't distinguish this: `nice` execs its target IN
  // PLACE, so the observed process's own argv/command line is identical to a
  // bare spawn either way (that's the whole point of using nice here). The
  // wrapping decision itself is what must be asserted, via the pure function
  // supervisor.js actually spawns from.
  assert.deepEqual(resolveNicedSpawn({ nice: 0, cmd: ['sleep', '2'] }), ['sleep', ['2']]);
  assert.deepEqual(resolveNicedSpawn({ cmd: ['sleep', '2'] }), ['sleep', ['2']], 'a missing nice field defaults to no wrapper too');
  assert.deepEqual(resolveNicedSpawn({ nice: 5, cmd: ['sleep', '2'] }), ['/usr/bin/nice', ['-n', '5', 'sleep', '2']]);
});
