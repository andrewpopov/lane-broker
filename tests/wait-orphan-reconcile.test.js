import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, laneSpawn, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, resultOf } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { paths, atomicWriteJson, ensureStateDirs } from '../src/state.js';

/**
 * BRAIN-515 step 3: `lane wait <id>` on an ORPHANED-REMOTE ticket asks the runner instead of giving up. Same fake-ssh transport
 * as tests/remote-reconcile.test.js; every test drives the real `lane wait` CLI.
 */

const DEAD_SUPERVISOR = { pid: 2 ** 22 + 7, startTime: null, bootId: 'dead' };
const MANIFEST = 'm'.repeat(64);
const BINDING = { protocol: 1, generation: 0, manifestHash: MANIFEST, runner: 'skybox' };

/** A dead-supervisor bound remote attempt, and (optionally) the runner's own record for it. */
function seedOrphan(ctx, { record } = {}) {
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), {
    id,
    generation: 0,
    executor: 'remote',
    phase: 'running',
    runner: 'skybox',
    startedAt: Date.now() - 60_000,
    supervisor: DEAD_SUPERVISOR,
    binding: BINDING,
  });
  if (record) {
    const dir = path.join(ctx.runnerRoot, 'tickets', id);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'result.json'), JSON.stringify({ protocol: 1, ticketId: id, generation: 0, manifestHash: MANIFEST, ...record }));
  }
  return id;
}

const completed = (exit) => ({ kind: 'completed', exit, signal: null, reason: null, runMs: 10, observedCpu: { peak: 0.5, mean: 0.25, samples: 2 }, deps: { outcome: 'hit', ms: 7 } });

/** A real dispatch of a job that holds until `release()`; returns once it is running on the runner, supervisor SIGKILLed. */
async function startOrphanedHeldRemote(ctx) {
  const dir = tmpDir('wait-reconcile-hold');
  const started = path.join(dir, 'started');
  const releaseFile = path.join(dir, 'release');
  const cmd = [
    process.execPath,
    '-e',
    `const fs = require('fs'); fs.writeFileSync(${JSON.stringify(started)}, 'x'); const t = setInterval(() => { if (fs.existsSync(${JSON.stringify(releaseFile)})) { clearInterval(t); process.exit(3); } }, 50);`,
  ];
  const run = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...cmd], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(run.code, 0, run.stderr);
  const id = run.stdout.trim();
  await waitFor(() => fs.existsSync(started), { timeoutMs: 15_000 });
  process.kill(readAttempt(ctx.state, id).supervisor.pid, 'SIGKILL');
  await sleep(300);
  return { id, release: () => fs.writeFileSync(releaseFile, '') };
}

// process start-up and the 200 ms poll on top of --timeout
const SLACK_MS = 1_500;

const wait = (ctx, id, extra = [], env = {}) => laneRun(['wait', id, ...extra], { env: { ...ctx.env, ...env }, cwd: ctx.repoDir });

test('orphan + runner reports the job gone: lane wait exits 75 (runner-lost) without any lane cancel', async () => {
  const ctx = setup();
  const id = seedOrphan(ctx);
  const out = await wait(ctx, id);
  assert.equal(out.code, 75, out.stderr);
  const result = resultOf(ctx.state, id);
  assert.equal(result.exit, 75);
  assert.equal(result.reason, 'runner-lost');
  assert.equal(result.reconciled, true);
  assert.notEqual(result.cancelled, true, 'settled by reconcile, not by a cancel');
  assert.equal(readAttempt(ctx.state, id), null);
});

test('orphan + runner finished with exit 3 (bound record): lane wait exits 3', async () => {
  const ctx = setup();
  const id = seedOrphan(ctx, { record: completed(3) });
  const out = await wait(ctx, id);
  assert.equal(out.code, 3, out.stderr);
  assert.equal(resultOf(ctx.state, id).exit, 3);
});

test('orphan + runner still running, then finishes: lane wait keeps waiting and exits with the runner exit', async () => {
  const ctx = setup();
  const { id, release } = await startOrphanedHeldRemote(ctx);
  const child = laneSpawn(['wait', id], { env: { ...ctx.env, LANE_BROKER_TEST_RECONCILE_INTERVAL_MS: '300' }, cwd: ctx.repoDir });
  let stderr = '';
  child.stderr.on('data', (d) => (stderr += d));
  const closed = new Promise((resolve) => child.on('close', resolve));
  try {
    // it must still be waiting after several reconcile intervals, with the attempt untouched
    const early = await Promise.race([closed, sleep(2_000).then(() => 'still-waiting')]);
    assert.equal(early, 'still-waiting', `lane wait gave up early: ${stderr}`);
    assert.ok(readAttempt(ctx.state, id), 'the attempt is kept while the runner still has the job');
    assert.equal(resultOf(ctx.state, id), null);
    release();
    const code = await Promise.race([closed, sleep(30_000).then(() => 'hung')]);
    assert.equal(code, 3, stderr);
  } finally {
    release();
    child.kill('SIGKILL');
  }
  assert.equal(resultOf(ctx.state, id).exit, 3);
});

test('orphan + runner unreachable: exit 1, message keeps ORPHANED-REMOTE and names the state, the auto-reconcile and lane cancel', async () => {
  const ctx = setup({ ssh: 'down' });
  const id = seedOrphan(ctx, { record: completed(3) });
  const out = await wait(ctx, id);
  assert.equal(out.code, 1);
  assert.match(out.stderr, /ORPHANED-REMOTE/);
  assert.match(out.stderr, /unreachable/);
  assert.match(out.stderr, /reconciled automatically once skybox answers/);
  assert.match(out.stderr, new RegExp(`lane cancel ${id}`));
  assert.ok(readAttempt(ctx.state, id), 'the attempt is kept');
  assert.equal(resultOf(ctx.state, id), null);
});

test('orphan + unreadable global config: treated as unreachable (exit 1)', async () => {
  const ctx = setup();
  const id = seedOrphan(ctx, { record: completed(3) });
  fs.writeFileSync(path.join(ctx.home, 'config.json'), '{ not json');
  const out = await wait(ctx, id);
  assert.equal(out.code, 1);
  assert.match(out.stderr, /ORPHANED-REMOTE/);
  assert.match(out.stderr, /unreachable/);
  assert.ok(readAttempt(ctx.state, id));
});

test('--timeout is honoured while the runner reports the job running (exit 75 on time, attempt kept)', async () => {
  const ctx = setup();
  const { id, release } = await startOrphanedHeldRemote(ctx);
  try {
    const t0 = Date.now();
    const out = await wait(ctx, id, ['--timeout', '2s'], { LANE_BROKER_TEST_RECONCILE_INTERVAL_MS: '300' });
    const elapsed = Date.now() - t0;
    assert.equal(out.code, 75, out.stderr);
    assert.match(out.stderr, /waited 2000ms, not failed/);
    assert.ok(elapsed < 2_000 + SLACK_MS, `took ${elapsed}ms`);
    assert.ok(readAttempt(ctx.state, id));
  } finally {
    release();
  }
});

test('a post-fallback orphan (executor local) is still reported ORPHANED, never reconciled', async () => {
  const ctx = setup();
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), { id, generation: 0, executor: 'local', phase: 'queued', runner: null, startedAt: Date.now(), supervisor: DEAD_SUPERVISOR });
  const out = await wait(ctx, id);
  assert.equal(out.code, 1);
  assert.match(out.stderr, /ORPHANED \(post-fallback\)/);
  assert.match(out.stderr, /reconcile with: lane cancel/);
});

/** A PATH entry whose `ssh` never answers: it sleeps far past any deadline (and dies with its parent's SIGKILL). */
function unresponsiveSsh(ctx) {
  const dir = tmpDir('wait-unresponsive-ssh');
  fs.writeFileSync(path.join(dir, 'ssh'), '#!/usr/bin/env node\nsetTimeout(() => process.exit(255), 120_000);\n');
  fs.chmodSync(path.join(dir, 'ssh'), 0o755);
  return { ...ctx.env, PATH: `${dir}${path.delimiter}${ctx.env.PATH}` };
}

test('--timeout is honoured against a runner that never answers: exit 75 (not the exit-1 orphan message) within the timeout, attempt kept', async () => {
  const ctx = setup();
  const id = seedOrphan(ctx);
  const t0 = Date.now();
  const out = await laneRun(['wait', id, '--timeout', '2s'], { env: unresponsiveSsh(ctx), cwd: ctx.repoDir });
  const elapsed = Date.now() - t0;
  assert.equal(out.code, 75, out.stderr);
  assert.match(out.stderr, /waited 2000ms, not failed/);
  assert.doesNotMatch(out.stderr, /ORPHANED-REMOTE/);
  assert.ok(elapsed < 2_000 + SLACK_MS, `took ${elapsed}ms`);
  assert.ok(readAttempt(ctx.state, id), 'the attempt is kept');
  assert.equal(resultOf(ctx.state, id), null);
});

test('a signal to lane wait on an orphan whose cancel cannot be confirmed: it exits 1 naming lane cancel, the attempt kept (no polling for ever)', async () => {
  const ctx = setup({ ssh: 'stuck-running' }); // the runner always reports the job running, and never confirms a cancel
  const id = seedOrphan(ctx);
  const child = laneSpawn(['wait', id], { env: ctx.env, cwd: ctx.repoDir });
  let stderr = '';
  child.stderr.on('data', (d) => (stderr += d));
  const closed = new Promise((resolve) => child.on('close', (code) => resolve(code)));
  try {
    const early = await Promise.race([closed, sleep(2_000).then(() => 'still-waiting')]);
    assert.equal(early, 'still-waiting', `lane wait gave up early: ${stderr}`);
    child.kill('SIGINT');
    const code = await Promise.race([closed, sleep(30_000).then(() => 'hung')]);
    assert.equal(code, 1, `lane wait did not exit after the unconfirmed cancel: ${stderr}`);
    assert.match(stderr, new RegExp(`lane wait: cancel of ${id} not confirmed \\(attempt kept\\); retry lane cancel ${id}`));
    assert.ok(readAttempt(ctx.state, id), 'the attempt is kept');
    assert.equal(resultOf(ctx.state, id), null);
  } finally {
    child.kill('SIGKILL');
  }
});
