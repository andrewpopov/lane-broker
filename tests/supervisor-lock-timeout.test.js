import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, markerCmd, resultOf } from './remote-harness.js';
import { paths, withLock, LockTimeoutError, NestedLockError, setLockContentionPolicy, resetLockContentionPolicy } from '../src/state.js';
import { readRows } from '../src/suggest.js';

// BRAIN-730: the lock is busy ~99% of the time under load, so a supervisor's default 15s acquire deadline expires. That is
// contention: the supervisor waits instead of dying with no result.

function collect(child) {
  return new Promise((resolve) => {
    let stderr = '';
    child.stderr.on('data', (d) => {
      stderr += d;
    });
    child.on('close', (code) => resolve({ code, stderr }));
  });
}

test('a supervisor whose first enqueue meets a held lock waits for it and runs the ticket', async () => {
  const { base, home, state, env } = freshEnv({ LANE_BROKER_TEST_LOCK_TIMEOUT_MS: '150' });
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 50 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  let child;
  try {
    await withLock(state, async () => {
      child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
      await sleep(1500);
    }, { timeoutMs: 10_000 });
    const { code, stderr } = await collect(child);
    assert.doesNotMatch(stderr, /exited unexpectedly|supervisor error/);
    assert.equal(code, 0, `stderr: ${stderr}`);
  } finally {
    if (child && child.exitCode === null) child.kill('SIGKILL');
  }
});

test('a fallback ticket whose finalize meets a held lock still publishes its result', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down' });
  const dir = tmpDir('b730');
  const ready = path.join(dir, 'ready');
  const go = path.join(dir, 'go');
  const holdEnv = { ...env, LANE_BROKER_TEST_LOCK_TIMEOUT_MS: '150', LANE_BROKER_TEST_HOLD_AT: 'local-finalize', LANE_BROKER_TEST_HOLD_READY: ready, LANE_BROKER_TEST_HOLD_GO: go };
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(path.join(dir, 'where'), 0)], { env: holdEnv, cwd: repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 120_000 });
  await withLock(state, async () => {
    fs.writeFileSync(go, 'x');
    await sleep(1500);
  }, { timeoutMs: 10_000 });
  await waitFor(() => fs.existsSync(path.join(paths(state).results, `${id}.json`)), { timeoutMs: 60_000 });
  const result = resultOf(state, id);
  assert.equal(result.exit, 0);
  assert.equal(result.executor, 'local');
});

test('a supervisor crash leaves exactly one supervisor-error history row that readers skip', async () => {
  const { base, home, state, env } = freshEnv({ LANE_BROKER_TEST_SUPERVISOR_ERROR_AT: 'queued' });
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 50 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  const { code, stderr } = await collect(child);
  assert.notEqual(code, 0, stderr);
  const lines = fs.readFileSync(paths(state).history, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
  const crashRows = lines.filter((r) => r.event === 'supervisor-error');
  assert.equal(crashRows.length, 1, JSON.stringify(lines));
  assert.equal(crashRows[0].errorClass, 'TypeError');
  assert.equal(crashRows[0].reason, 'supervisor-error');
  assert.equal(crashRows[0].phase, 'queued');
  assert.equal(crashRows[0].repo, 'r');
  assert.equal('exit' in crashRows[0], false);
  assert.equal(readRows(paths(state).history).some((r) => r.event === 'supervisor-error'), false);
});

function withShortDeadline(fn) {
  return async () => {
    const prior = process.env.LANE_BROKER_TEST_LOCK_TIMEOUT_MS;
    process.env.LANE_BROKER_TEST_LOCK_TIMEOUT_MS = '150';
    try {
      await fn();
    } finally {
      resetLockContentionPolicy();
      if (prior === undefined) delete process.env.LANE_BROKER_TEST_LOCK_TIMEOUT_MS;
      else process.env.LANE_BROKER_TEST_LOCK_TIMEOUT_MS = prior;
    }
  };
}

test('under the contention policy a default-deadline withLock outlives a holder and reports contention', withShortDeadline(async () => {
  const { state } = freshEnv();
  const contentions = [];
  setLockContentionPolicy({ onContention: (err) => contentions.push(err) });
  const holder = withLock(state, () => sleep(700), { timeoutMs: 10_000 });
  await sleep(50);
  let ran = 0;
  await withLock(state, () => {
    ran += 1;
  });
  await holder;
  assert.equal(ran, 1);
  assert.ok(contentions.length >= 1);
  assert.ok(contentions.every((e) => e instanceof LockTimeoutError));
}));

test('without the policy a default-deadline withLock still throws LockTimeoutError', withShortDeadline(async () => {
  const { state } = freshEnv();
  const holder = withLock(state, () => sleep(700), { timeoutMs: 10_000 });
  await sleep(50);
  await assert.rejects(withLock(state, () => {}), LockTimeoutError);
  await holder;
}));

test('under the policy an explicit timeoutMs still throws LockTimeoutError', withShortDeadline(async () => {
  const { state } = freshEnv();
  setLockContentionPolicy({ onContention: () => {} });
  const holder = withLock(state, () => sleep(700), { timeoutMs: 10_000 });
  await sleep(50);
  await assert.rejects(withLock(state, () => {}, { timeoutMs: 100 }), LockTimeoutError);
  await holder;
}));

test('under the policy a LockTimeoutError thrown inside fn is not retried', withShortDeadline(async () => {
  const { state } = freshEnv();
  setLockContentionPolicy({ onContention: () => {} });
  let ran = 0;
  await assert.rejects(
    withLock(state, () => {
      ran += 1;
      throw new LockTimeoutError('from fn');
    }),
    LockTimeoutError,
  );
  assert.equal(ran, 1);
}));

test('acquiring the same lock from inside a held one throws NestedLockError at once', async () => {
  const { state } = freshEnv();
  const started = Date.now();
  await withLock(state, async () => {
    await assert.rejects(withLock(state, () => {}), NestedLockError);
  });
  assert.ok(Date.now() - started < 5000);
});
