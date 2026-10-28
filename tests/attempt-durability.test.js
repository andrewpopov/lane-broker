import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, sleep, waitFor } from './helpers.js';
import { paths, atomicWriteJson, ensureStateDirs, withLock } from '../src/state.js';
import { readAttempt } from '../src/attempts.js';
import { settleAbandonedLocalAttempts } from '../src/local-reconcile.js';
import { sweepOrphans } from '../src/reconcile-sweep.js';
import { readPending } from '../src/pending.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

/**
 * BRAIN-605: a local-executor attempt (a remote attempt that fell back to local) whose supervisor died before admission is settled
 * by the reconcile sweep and by `lane wait`. BRAIN-494: `lane run --detach` writes a durable pending record before printing the id.
 */

const DEAD_SUPERVISOR = { pid: 2 ** 22 + 7, startTime: null, bootId: 'dead' };

function fixture() {
  const fx = freshEnv();
  writeGlobalConfig(fx.home, { version: 1, capacity: 4 });
  const repoDir = path.join(fx.base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  ensureStateDirs(fx.state);
  return { ...fx, repoDir };
}

function seedLocalQueuedAttempt(fx, supervisor) {
  const id = crypto.randomUUID();
  atomicWriteJson(path.join(paths(fx.state).attempts, `${id}.json`), {
    id,
    generation: 1,
    executor: 'local',
    phase: 'queued',
    runner: 'wintop',
    fallbackReason: 'rebind',
    startedAt: Date.now() - 60_000,
    supervisor,
  });
  return id;
}

const resultFile = (fx, id) => path.join(paths(fx.state).results, `${id}.json`);
const readResult = (fx, id) => JSON.parse(fs.readFileSync(resultFile(fx, id), 'utf8'));

test('605: one sweep settles a queued local attempt whose supervisor is gone, as a definite exit-75 result', async () => {
  const fx = fixture();
  const id = seedLocalQueuedAttempt(fx, DEAD_SUPERVISOR);
  const sweep = await sweepOrphans(fx.state, { globalCfg: { ...DEFAULT_GLOBAL_CONFIG }, force: true });
  assert.equal(sweep.ran, true);
  assert.deepEqual(sweep.results.map((r) => [r.id, r.state, r.exit]), [[id, 'lost', 75]]);
  assert.equal(readResult(fx, id).reason, 'supervisor-died-while-queued');
  assert.equal(readAttempt(fx.state, id), null, 'the attempt record is gone, so nothing reports it outstanding');
  const history = fs.readFileSync(paths(fx.state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.ok(history.some((r) => r.id === id && r.exit === 75));
});

test('605: an attempt whose supervisor is alive is left alone', async () => {
  const fx = fixture();
  const id = seedLocalQueuedAttempt(fx, null);
  // the live supervisor is this very process
  const { supervisorAlive } = await import('../src/attempts.js');
  const { processStartTime } = await import('../src/process-liveness.js');
  const { bootId } = await import('../src/state.js');
  const alive = { pid: process.pid, startTime: processStartTime(process.pid), bootId: bootId() };
  const file = path.join(paths(fx.state).attempts, `${id}.json`);
  atomicWriteJson(file, { ...readAttempt(fx.state, id), supervisor: alive });
  assert.equal(supervisorAlive(readAttempt(fx.state, id)), true);
  assert.deepEqual(await settleAbandonedLocalAttempts(fx.state), []);
  assert.ok(readAttempt(fx.state, id), 'attempt kept');
  assert.equal(fs.existsSync(resultFile(fx, id)), false);
});

test('605: lane wait on a dead-supervisor queued local attempt exits 75 instead of reporting it orphaned forever', async () => {
  const fx = fixture();
  const id = seedLocalQueuedAttempt(fx, DEAD_SUPERVISOR);
  const out = await laneRun(['wait', id, '--timeout', '20s'], { env: fx.env, cwd: fx.repoDir });
  assert.equal(out.code, 75, out.stderr);
  assert.equal(readResult(fx, id).reason, 'supervisor-died-while-queued');
});

test('494: --detach writes a durable record before the id is printed; a supervisor killed before registering settles as exit 75', async () => {
  const fx = fixture();
  // Hold the broker lock so the supervisor cannot register (enqueue takes it) until it has been killed.
  let release;
  const held = new Promise((resolve) => {
    release = resolve;
  });
  const locked = withLock(fx.state, () => held);
  await sleep(100);
  const run = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', process.execPath, '-e', 'process.exit(0)'], { env: fx.env, cwd: fx.repoDir });
  assert.equal(run.code, 0, run.stderr);
  const id = run.stdout.trim();
  const pending = readPending(fx.state, id);
  assert.ok(pending, 'the printed id resolves to a durable record immediately');
  assert.equal(readAttempt(fx.state, id), null, 'nothing else knows the id yet');
  process.kill(pending.supervisor.pid, 'SIGKILL');
  await waitFor(() => {
    try {
      process.kill(pending.supervisor.pid, 0);
      return false;
    } catch {
      return true;
    }
  }, { timeoutMs: 10_000 });
  release();
  await locked;
  const out = await laneRun(['wait', id, '--timeout', '20s'], { env: fx.env, cwd: fx.repoDir });
  assert.equal(out.code, 75, out.stderr);
  assert.equal(readResult(fx, id).reason, 'supervisor-died-before-register');
});

test('494: a normal detached run is unaffected by its pending record', async () => {
  const fx = fixture();
  const run = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', process.execPath, '-e', 'process.exit(3)'], { env: fx.env, cwd: fx.repoDir });
  const id = run.stdout.trim();
  const out = await laneRun(['wait', id, '--timeout', '30s'], { env: fx.env, cwd: fx.repoDir });
  assert.equal(out.code, 3, out.stderr);
});
