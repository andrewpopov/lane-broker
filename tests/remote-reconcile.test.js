import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, resultOf, linesOf, makeFakeSshBin, makeRunner, clientEnv, makeGitWorktree, makeDispatchArgs } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { cancelAttempt } from '../src/cancel.js';
import { paths, atomicWriteJson, writeCancelMarkerFile, ensureStateDirs, withLock } from '../src/state.js';
import { reapStale } from '../src/scheduler.js';
import { dispatchRemote } from '../src/remote-client.js';
import { reconcileRemoteAttempt } from '../src/remote-reconcile.js';
import { relayedUsage } from '../src/remote-relay.js';

/**
 * BRAIN-515: `reconcileRemoteAttempt` settles an ORPHANED-REMOTE attempt from the runner's own answer, and never runs anything
 * again. The same fake-ssh transport as tests/remote-lifecycle.test.js; most cases seed the orphan (a dead-supervisor attempt
 * plus the runner's own ticket dir) so each runner answer is exact, and two drive a real dispatch end to end.
 */

const DEAD_SUPERVISOR = { pid: 2 ** 22 + 7, startTime: null, bootId: 'dead' };
const MANIFEST = 'm'.repeat(64);

function context(overrides = {}) {
  const ctx = setup();
  const globalCfg = JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8'));
  return { ...ctx, globalCfg: { ...globalCfg, ...overrides }, sshBin: 'ssh' };
}

const reconcile = (ctx, id, extra = {}) => reconcileRemoteAttempt(ctx.state, id, { globalCfg: ctx.globalCfg, env: ctx.env, sshBin: ctx.sshBin, deadlineMs: 20_000, ...extra });

const BINDING = { protocol: 1, generation: 0, manifestHash: MANIFEST, runner: 'skybox' };

/** A dead-supervisor remote attempt, optionally bound, and (optionally) the runner's own record for it. */
function seedOrphan(ctx, { binding = BINDING, record, attempt = {} } = {}) {
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
    ...(binding ? { binding } : {}),
    ...attempt,
  });
  if (record) {
    const dir = path.join(ctx.runnerRoot, 'tickets', id);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'result.json'), JSON.stringify({ protocol: 1, ticketId: id, generation: 0, manifestHash: MANIFEST, ...record }));
  }
  return id;
}

const completed = (extra = {}) => ({ kind: 'completed', exit: 3, signal: null, reason: null, runMs: 10, observedCpu: { peak: 0.5, mean: 0.25, samples: 2 }, deps: { outcome: 'hit', ms: 7 }, ...extra });

const historyRows = (state) => (fs.existsSync(paths(state).history) ? fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l)) : []);
const jsonFiles = (dir) => (fs.existsSync(dir) ? fs.readdirSync(dir).filter((n) => n.endsWith('.json')) : []);

function assertNothingRan(ctx) {
  assert.deepEqual(jsonFiles(paths(ctx.state).queue), [], 'reconcile must never enqueue the job locally');
  assert.deepEqual(jsonFiles(paths(ctx.state).leases), [], 'reconcile must never lease the job locally');
}

function assertKept(ctx, id) {
  assert.ok(readAttempt(ctx.state, id), 'the attempt must be kept');
  assert.equal(resultOf(ctx.state, id), null, 'no result may be written');
  assert.deepEqual(historyRows(ctx.state), [], 'no history row may be written');
}

/** A real dispatch of a job that holds until `release()`; returns once it is running on the runner. The job appends one line
 *  ('remote' or 'local') to `executions()`'s counter file on EVERY execution, so a second run anywhere is measurable. */
async function startHeldRemote(ctx) {
  const dir = tmpDir('reconcile-hold');
  const started = path.join(dir, 'started');
  const releaseFile = path.join(dir, 'release');
  const counter = path.join(dir, 'executions');
  const cmd = [
    process.execPath,
    '-e',
    `const fs = require('fs'); fs.appendFileSync(${JSON.stringify(counter)}, (process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local') + '\\n'); fs.writeFileSync(${JSON.stringify(started)}, 'x'); const t = setInterval(() => { if (fs.existsSync(${JSON.stringify(releaseFile)})) { clearInterval(t); process.exit(3); } }, 50);`,
  ];
  const run = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...cmd], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(run.code, 0, run.stderr);
  const id = run.stdout.trim();
  await waitFor(() => fs.existsSync(started), { timeoutMs: 15_000 });
  return { id, release: () => fs.writeFileSync(releaseFile, ''), executions: () => linesOf(counter) };
}

/** What the scheduler does at the start of every admission decision, under the lock. */
const schedulerPass = (ctx) => withLock(ctx.state, () => reapStale(ctx.state));

/** The job has executed exactly once, on the runner: right after the reconcile AND after a following scheduler pass. */
async function assertRanOnce(ctx, held, when) {
  assert.deepEqual(held.executions(), ['remote'], `${when}: one execution, on the runner`);
  await schedulerPass(ctx);
  await sleep(500);
  assert.deepEqual(held.executions(), ['remote'], `${when}, after a scheduler pass: still one execution`);
  assertNothingRan(ctx);
}

const killSupervisor = async (ctx, id) => {
  process.kill(readAttempt(ctx.state, id).supervisor.pid, 'SIGKILL');
  await sleep(300);
};

const runnerResultPath = (ctx, id) => path.join(ctx.runnerRoot, 'tickets', id, 'result.json');

/** An `ssh` that answers like the fake runner except for a `remote-cancel`, which it fails or leaves unconfirmed. */
function sshWithCancel(ctx, mode) {
  const real = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  const wrapper = path.join(tmpDir('reconcile-ssh'), 'ssh');
  fs.writeFileSync(
    wrapper,
    `#!/usr/bin/env node
const { spawnSync } = require('child_process');
const args = process.argv.slice(2);
if (args.join(' ').includes('remote-cancel')) {
  ${mode === 'down' ? 'process.exit(255);' : "process.stdout.write(JSON.stringify({ protocol: 1, cancelConfirmed: false }) + '\\n'); process.exit(0);"}
}
const res = spawnSync(${JSON.stringify(real)}, args, { stdio: 'inherit', env: process.env });
process.exit(res.status == null ? 1 : res.status);
`,
  );
  fs.chmodSync(wrapper, 0o755);
  return wrapper;
}

test('a real dispatch persists its binding on the attempt before the run finishes', async () => {
  const ctx = context();
  const { id, release } = await startHeldRemote(ctx);
  try {
    const attempt = readAttempt(ctx.state, id);
    assert.equal(attempt.binding.protocol, 1);
    assert.equal(attempt.binding.generation, attempt.generation);
    assert.match(attempt.binding.manifestHash, /^[0-9a-f]{64}$/);
    assert.equal(attempt.binding.runner, 'skybox');
    assert.equal(attempt.binding.runner, attempt.runner);
  } finally {
    release();
    await waitFor(() => readAttempt(ctx.state, id) === null, { timeoutMs: 30_000 });
  }
  assert.equal(resultOf(ctx.state, id).exit, 3);
});

test('a completed runner result matching the dispatch binding is adopted: exit kept, attempt removed, one reconciled history row', async () => {
  const ctx = context();
  const held = await startHeldRemote(ctx);
  const { id, release } = held;
  const bound = readAttempt(ctx.state, id).binding;
  await killSupervisor(ctx, id);
  release();
  await waitFor(() => fs.existsSync(runnerResultPath(ctx, id)), { timeoutMs: 30_000 });
  const record = JSON.parse(fs.readFileSync(runnerResultPath(ctx, id), 'utf8'));
  assert.equal(record.manifestHash, bound.manifestHash, 'the stored binding is what the runner echoes back');

  const outcome = await reconcile(ctx, id);
  assert.deepEqual(outcome, { settled: true, state: 'adopted', exit: 3 });
  assert.equal(readAttempt(ctx.state, id), null);
  const result = resultOf(ctx.state, id);
  assert.equal(result.exit, 3);
  assert.equal(result.reconciled, true);
  assert.equal(result.executor, 'remote');
  assert.equal(result.runner, 'skybox');
  assert.equal(result.remoteKind, 'completed');
  // A short job may finish before the runner's first CPU sample, so compare against what the runner actually recorded.
  const usageOf = (r) => Object.fromEntries(['observedCpu', 'observedRssPeakBytes', 'cpuSeconds'].filter((k) => k in r).map((k) => [k, r[k]]));
  assert.deepEqual(usageOf(result), relayedUsage(record), 'the runner usage is relayed exactly');
  const rows = historyRows(ctx.state);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].reconciled, true);
  assert.equal(rows[0].exit, 3);
  assert.match(fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'), new RegExp(`lane: reconcile: ${id}: skybox: adopted exit 3`));
  await assertRanOnce(ctx, held, 'after adopting');
  assert.equal(resultOf(ctx.state, id).exit, 3, 'the adopted result is unchanged by the scheduler pass');
  assert.equal(historyRows(ctx.state).length, 1);

  const again = await reconcile(ctx, id);
  assert.equal(again.state, 'skipped', 'a second reconcile finds nothing to do');
  assert.equal(historyRows(ctx.state).length, 1);
});

test('a runner result of kind cancelled is adopted as exit 130', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: { kind: 'cancelled' } });
  const outcome = await reconcile(ctx, id);
  assert.deepEqual(outcome, { settled: true, state: 'adopted', exit: 130 });
  assert.equal(resultOf(ctx.state, id).exit, 130);
  assert.equal(historyRows(ctx.state).length, 1);
});

test('a runner result of kind unfinished is ambiguous: attempt kept, nothing written', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: { kind: 'unfinished', reason: 'runner-restarted' } });
  const outcome = await reconcile(ctx, id);
  assert.equal(outcome.settled, false);
  assert.equal(outcome.state, 'ambiguous');
  assert.match(outcome.reason, /unfinished/);
  assertKept(ctx, id);
});

test('a runner result that does not bind to the stored manifestHash is ambiguous: attempt kept', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: completed({ manifestHash: 'x'.repeat(64) }) });
  const outcome = await reconcile(ctx, id);
  assert.equal(outcome.settled, false);
  assert.equal(outcome.state, 'ambiguous');
  assert.match(outcome.reason, /did not bind/);
  assertKept(ctx, id);
});

test('a legacy attempt (no dispatch binding) is never adopted from a runner result', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { binding: null, record: completed() });
  const outcome = await reconcile(ctx, id);
  assert.equal(outcome.settled, false);
  assert.equal(outcome.state, 'ambiguous');
  assert.match(outcome.reason, /no dispatch binding/);
  assertKept(ctx, id);
});

test('a job the runner reports gone, with its cancel confirmed, is settled as lost (exit 75, runner-lost) and never re-run', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { attempt: { unresolved: 'ssh dropped' } });
  const outcome = await reconcile(ctx, id);
  assert.deepEqual(outcome, { settled: true, state: 'lost', exit: 75 });
  assert.equal(readAttempt(ctx.state, id), null);
  const result = resultOf(ctx.state, id);
  assert.equal(result.exit, 75);
  assert.equal(result.reason, 'runner-lost');
  assert.equal(result.lostDetail, 'ssh dropped');
  assert.equal(result.reconciled, true);
  const rows = historyRows(ctx.state);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].reason, 'runner-lost');
  assert.match(fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'), new RegExp(`lane: reconcile: ${id}: skybox: lost \\(runner-lost\\)`));
  assertNothingRan(ctx);
});

for (const mode of ['down', 'unconfirmed']) {
  test(`a job the runner reports gone whose cancel is not confirmed (${mode}) keeps the attempt`, async () => {
    const ctx = context();
    ctx.sshBin = sshWithCancel(ctx, mode);
    const id = seedOrphan(ctx);
    const outcome = await reconcile(ctx, id);
    assert.equal(outcome.settled, false);
    assert.equal(outcome.state, 'unreachable');
    assert.match(outcome.reason, /remote cancel not confirmed/);
    assertKept(ctx, id);
    assertNothingRan(ctx);
  });
}

// BRAIN-570: the runner's own record can prove the job never started; a probe-phase orphan never reached a runner at all.
for (const record of [{ kind: 'unfinished', reason: 'queue-timeout' }, { kind: 'unfinished', reason: 'withdrawn' }, { kind: 'rejected', reason: 'truncated stream' }]) {
  test(`a binding-matched ${record.kind}/${record.reason} runner record is settled as lost (exit 75, runner-never-started)`, async () => {
    const ctx = context();
    const id = seedOrphan(ctx, { record });
    const outcome = await reconcile(ctx, id);
    assert.deepEqual(outcome, { settled: true, state: 'lost', exit: 75 });
    assert.equal(readAttempt(ctx.state, id), null);
    const result = resultOf(ctx.state, id);
    assert.equal(result.exit, 75);
    assert.equal(result.reason, 'runner-never-started');
    assert.equal(result.lostDetail, `runner skybox reported ${record.kind}: ${record.reason}`);
    assert.equal(result.reconciled, true);
    const rows = historyRows(ctx.state);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].reason, 'runner-never-started');
    assert.match(fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'), new RegExp(`lane: reconcile: ${id}: skybox: lost \\(runner-never-started\\)`));
    assertNothingRan(ctx);
  });
}

// BRAIN-350: a runner's host shutdown proves the job is dead there, exactly like a never-started record.
test('a binding-matched unfinished/runner-shutdown runner record is settled as lost (exit 75, runner-shutdown) and never re-run', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: { kind: 'unfinished', reason: 'runner-shutdown' } });
  const outcome = await reconcile(ctx, id);
  assert.deepEqual(outcome, { settled: true, state: 'lost', exit: 75 });
  assert.equal(readAttempt(ctx.state, id), null);
  const result = resultOf(ctx.state, id);
  assert.equal(result.reason, 'runner-shutdown');
  assert.equal(result.lostDetail, 'runner skybox stopped the job: host shutting down');
  const rows = historyRows(ctx.state);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].reason, 'runner-shutdown');
  assertNothingRan(ctx);
});

test('a runner-shutdown record with a mismatched binding stays ambiguous: attempt kept', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: { kind: 'unfinished', reason: 'runner-shutdown', manifestHash: 'x'.repeat(64) } });
  assert.equal((await reconcile(ctx, id)).state, 'ambiguous');
  assertKept(ctx, id);
});

test('a never-started runner record with a mismatched binding stays ambiguous: attempt kept', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: { kind: 'unfinished', reason: 'queue-timeout', manifestHash: 'x'.repeat(64) } });
  const outcome = await reconcile(ctx, id);
  assert.equal(outcome.state, 'ambiguous');
  assert.match(outcome.reason, /did not bind/);
  assertKept(ctx, id);
});

test('a never-started runner record on a legacy attempt (no binding) stays ambiguous', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { binding: null, record: { kind: 'unfinished', reason: 'queue-timeout' } });
  assert.equal((await reconcile(ctx, id)).state, 'ambiguous');
  assertKept(ctx, id);
});

test('a cancel marker wins over a never-started runner record', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: { kind: 'unfinished', reason: 'queue-timeout' } });
  writeCancelMarkerFile(ctx.state, id);
  assert.deepEqual(await reconcile(ctx, id), { settled: true, state: 'cancelled', exit: 130 });
  assert.equal(resultOf(ctx.state, id).cancelled, true);
});

/** An ssh that records every invocation and fails: proves a reconcile made no ssh call. */
function sshThatMustNotRun() {
  const dir = tmpDir('reconcile-no-ssh');
  const calls = path.join(dir, 'calls');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(bin, `#!/bin/sh\necho "$@" >> ${JSON.stringify(calls)}\nexit 1\n`);
  fs.chmodSync(bin, 0o755);
  return { bin, calls: () => (fs.existsSync(calls) ? fs.readFileSync(calls, 'utf8') : '') };
}

const probeOrphan = (ctx, attempt = {}) => seedOrphan(ctx, { binding: null, attempt: { phase: 'probe', runner: null, ...attempt } });

test('a probe-phase orphan that never reached a runner is settled as lost (exit 75, never-dispatched) with no ssh call', async () => {
  const ctx = context();
  const ssh = sshThatMustNotRun();
  const id = probeOrphan(ctx);
  const outcome = await reconcile({ ...ctx, sshBin: ssh.bin }, id);
  assert.deepEqual(outcome, { settled: true, state: 'lost', exit: 75 });
  assert.equal(ssh.calls(), '', 'no ssh invocation');
  assert.equal(readAttempt(ctx.state, id), null);
  const result = resultOf(ctx.state, id);
  assert.equal(result.exit, 75);
  assert.equal(result.reason, 'never-dispatched');
  assert.equal(result.runner, null);
  assert.equal(historyRows(ctx.state).length, 1);
  assert.match(fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'), new RegExp(`lane: reconcile: ${id}: no runner: lost \\(never-dispatched\\)`));
  assertNothingRan(ctx);
});

for (const [name, attempt] of [
  ['dispatchedRunners', { dispatchedRunners: ['skybox'] }],
  ['a binding', { binding: BINDING }],
  ['a running phase', { phase: 'running' }],
]) {
  test(`a runner-less orphan with ${name} is NOT settled as never-dispatched`, async () => {
    const ctx = context();
    const id = probeOrphan(ctx, attempt);
    const outcome = await reconcile(ctx, id);
    assert.equal(outcome.settled, false);
    assert.equal(outcome.state, 'unreachable');
    assert.equal(resultOf(ctx.state, id), null);
    assert.deepEqual(historyRows(ctx.state), []);
  });
}

test('a never-dispatched orphan that gained a dispatch record before the publish is left alone (stale)', async () => {
  const ctx = context();
  const id = probeOrphan(ctx);
  const attemptFile = path.join(paths(ctx.state).attempts, `${id}.json`);
  // the supervisor persists a dispatch (runner still null in this window) after the read, before the publish fence
  const realStat = fs.readFileSync;
  const attemptBefore = JSON.parse(realStat(attemptFile, 'utf8'));
  let flipped = false;
  const original = fs.existsSync;
  fs.existsSync = (p) => {
    if (!flipped && p === path.join(paths(ctx.state).results, `${id}.json`)) {
      flipped = true;
      fs.writeFileSync(attemptFile, JSON.stringify({ ...attemptBefore, dispatchedRunners: ['skybox'], binding: BINDING }));
    }
    return original(p);
  };
  try {
    const outcome = await reconcile(ctx, id);
    assert.equal(outcome.settled, false);
    assert.equal(resultOf(ctx.state, id), null);
    assert.deepEqual(readAttempt(ctx.state, id).dispatchedRunners, ['skybox']);
  } finally {
    fs.existsSync = original;
  }
});

test('a cancel marker wins over a never-dispatched orphan', async () => {
  const ctx = context();
  const id = probeOrphan(ctx);
  writeCancelMarkerFile(ctx.state, id);
  assert.deepEqual(await reconcile(ctx, id), { settled: true, state: 'cancelled', exit: 130 });
  assert.equal(resultOf(ctx.state, id).cancelled, true);
});

test('a job still running on the runner keeps the attempt (and a live supervisor is skipped)', async () => {
  const ctx = context();
  const held = await startHeldRemote(ctx);
  const { id, release } = held;
  try {
    const live = await reconcile(ctx, id);
    assert.equal(live.state, 'skipped');
    assert.match(live.reason, /supervisor is alive/);

    await killSupervisor(ctx, id);
    const outcome = await reconcile(ctx, id);
    assert.deepEqual(outcome, { settled: false, state: 'running' });
    assertKept(ctx, id);
    await assertRanOnce(ctx, held, 'while the runner still has it');
    assertKept(ctx, id);
  } finally {
    release();
    await waitFor(() => fs.existsSync(runnerResultPath(ctx, id)), { timeoutMs: 30_000 });
  }
});

test('a runner the transport cannot reach is unreachable: attempt kept', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: completed() });
  const down = await reconcile({ ...ctx, globalCfg: { ...ctx.globalCfg, runners: ctx.globalCfg.runners.map((r) => ({ ...r, ssh: 'down' })) } }, id);
  assert.equal(down.settled, false);
  assert.equal(down.state, 'unreachable');
  assertKept(ctx, id);

  const gone = await reconcile({ ...ctx, globalCfg: { ...ctx.globalCfg, runners: [] } }, id);
  assert.deepEqual(gone, { settled: false, state: 'unreachable', reason: 'runner not configured' });
  assertKept(ctx, id);
});

test('an interrupted move is skipped', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: completed(), attempt: { moving: { from: 'skybox', to: 'other', phase: 'dispatching', at: Date.now() } } });
  const outcome = await reconcile(ctx, id);
  assert.equal(outcome.state, 'skipped');
  assertKept(ctx, id);
});

test('an attempt that changed runner between the read and the publish is left alone (stale)', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: completed() });
  // the runner's answer arrives, and meanwhile the attempt is re-pointed at another runner
  const real = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  const attemptFile = path.join(paths(ctx.state).attempts, `${id}.json`);
  const wrapper = path.join(tmpDir('reconcile-ssh'), 'ssh');
  fs.writeFileSync(
    wrapper,
    `#!/usr/bin/env node
const fs = require('fs');
const { spawnSync } = require('child_process');
const res = spawnSync(${JSON.stringify(real)}, process.argv.slice(2), { stdio: 'inherit', env: process.env });
const attempt = JSON.parse(fs.readFileSync(${JSON.stringify(attemptFile)}, 'utf8'));
fs.writeFileSync(${JSON.stringify(attemptFile)}, JSON.stringify({ ...attempt, runner: 'elsewhere' }));
process.exit(res.status == null ? 1 : res.status);
`,
  );
  fs.chmodSync(wrapper, 0o755);
  const outcome = await reconcile({ ...ctx, sshBin: wrapper }, id);
  assert.deepEqual(outcome, { settled: false, state: 'skipped', reason: 'attempt changed' });
  assert.equal(readAttempt(ctx.state, id).runner, 'elsewhere');
  assert.equal(resultOf(ctx.state, id), null);
  assert.deepEqual(historyRows(ctx.state), []);
});

test('a cancel marker wins over the runner outcome: the cancelled shape is published', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: completed() });
  writeCancelMarkerFile(ctx.state, id);
  const outcome = await reconcile(ctx, id);
  assert.deepEqual(outcome, { settled: true, state: 'cancelled', exit: 130 });
  assert.equal(resultOf(ctx.state, id).cancelled, true);
  const rows = historyRows(ctx.state);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].cancelled, true);
});

test('two concurrent reconcilers publish once', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: completed() });
  const outcomes = await Promise.all([reconcile(ctx, id), reconcile(ctx, id)]);
  assert.equal(outcomes.filter((o) => o.state === 'adopted').length, 1, JSON.stringify(outcomes));
  assert.ok(outcomes.every((o) => o.state === 'adopted' || o.state === 'already-settled' || o.state === 'skipped'), JSON.stringify(outcomes));
  assert.equal(historyRows(ctx.state).length, 1);
  assert.equal(readAttempt(ctx.state, id), null);
});

test('reconcile racing lane cancel, in every order, leaves exactly one result and one history row', async (t) => {
  const ctx = context();
  // cancelAttempt reads the global config and spawns ssh from THIS process's environment
  const saved = { ...process.env };
  Object.assign(process.env, ctx.env);
  t.after(() => {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
  });

  for (const order of ['concurrent', 'reconcile-first', 'cancel-first']) {
    const id = seedOrphan(ctx, { record: completed() });
    const snapshot = readAttempt(ctx.state, id);
    const before = historyRows(ctx.state).length;
    if (order === 'concurrent') await Promise.all([reconcile(ctx, id), cancelAttempt(ctx.state, id, snapshot)]);
    else if (order === 'reconcile-first') {
      await reconcile(ctx, id);
      await cancelAttempt(ctx.state, id, snapshot);
    } else {
      await cancelAttempt(ctx.state, id, snapshot);
      await reconcile(ctx, id);
    }
    assert.equal(readAttempt(ctx.state, id), null, order);
    assert.ok(resultOf(ctx.state, id), `${order}: a result exists`);
    assert.equal(historyRows(ctx.state).filter((r) => r.id === id).length, 1, `${order}: exactly one history row`);
    assert.equal(historyRows(ctx.state).length, before + 1, order);
    assert.equal(jsonFiles(paths(ctx.state).results).filter((n) => n === `${id}.json`).length, 1, order);
  }
});

/** An `ssh` that logs every call it receives, then delegates to the harness's fake transport. */
function countingSsh(real) {
  const dir = tmpDir('binding-ssh');
  const log = path.join(dir, 'calls.log');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const { spawnSync } = require('child_process');
require('fs').appendFileSync(${JSON.stringify(log)}, 'call\\n');
const res = spawnSync(${JSON.stringify(real)}, process.argv.slice(2), { stdio: 'inherit', env: process.env });
process.exit(res.status == null ? 1 : res.status);
`,
  );
  fs.chmodSync(bin, 0o755);
  return { bin, calls: () => linesOf(log).length };
}

for (const [name, onBinding] of [
  ['resolves false (the attempt write was refused)', async () => false],
  ['throws (the lock or the write failed)', async () => {
    throw new Error('lock timeout');
  }],
]) {
  test(`a dispatch whose binding cannot be persisted (onBinding ${name}) never starts: nothing is sent, and it is a never-started unconfirmed outcome`, async () => {
    const { binDir, sshBin } = makeFakeSshBin();
    const { env } = clientEnv(binDir);
    const counting = countingSsh(sshBin);
    const root = tmpDir('binding-runner-root');
    const src = makeGitWorktree({ 'a.txt': 'hello' });
    const args = { ...makeDispatchArgs(), runner: makeRunner({ ssh: 'normal', root }), worktreeRoot: src, sshBin: counting.bin, env };

    const outcome = await dispatchRemote({ ...args, onBinding });
    assert.equal(outcome.outcome, 'unconfirmed');
    assert.equal(outcome.reason, 'could not persist the dispatch binding');
    assert.equal(outcome.neverStarted, true, 'nothing was sent, so the existing never-started fallback rules apply');
    assert.equal(outcome.retryElsewhere, undefined, 'not a reason to try another runner');
    assert.equal(counting.calls(), 0, 'no ssh was spawned');
    assert.equal(fs.existsSync(path.join(root, 'tickets')), false, 'the runner never saw the ticket');

    // the same dispatch with a persisting onBinding does reach the runner, so the assertions above can fail
    const bindings = [];
    const sent = await dispatchRemote({ ...args, ticketId: crypto.randomUUID(), onBinding: async (b) => (bindings.push(b), true) });
    assert.equal(sent.outcome, 'confirmed');
    assert.ok(counting.calls() > 0);
    assert.equal(bindings.length, 1);
  });
}
