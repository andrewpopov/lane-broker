import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, resultOf, linesOf, makeFakeSshBin, makeRunner, clientEnv, makeGitWorktree, makeDispatchArgs } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { processStartTime } from '../src/process-liveness.js';
import { cancelAttempt } from '../src/cancel.js';
import { paths, atomicWriteJson, writeCancelMarkerFile, ensureStateDirs, withLock, tombstonePath, bootId, kernelBootId, RUNNER_SHUTDOWN_REASON } from '../src/state.js';
import { readUnreachable, recordContact } from '../src/runner-unreachable.js';
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
    'node',
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

// ---- BRAIN-759: an attempt stranded in a `withdrawing` move settles only on proof from BOTH hosts ----

const HOUR = 3_600_000;
const WITHDRAWN = { kind: 'unfinished', reason: 'withdrawn' };
const withdrawingMove = () => ({ from: 'skybox', to: 'dest', phase: 'withdrawing', at: 1234 });
const thisBootDead = () => ({ ...DEAD_SUPERVISOR, bootId: bootId(), kernelBootId: kernelBootId() });
const strandedAttempt = (extra = {}) => ({ supervisor: thisBootDead(), moving: withdrawingMove(), ...extra });

/** A second runner `dest` (its own root, so its tickets differ from skybox's) in the client config; returns its root. */
function addDestination(ctx, ssh = 'normal') {
  const root = tmpDir('reconcile-dest-root');
  ctx.globalCfg = { ...ctx.globalCfg, runners: [...ctx.globalCfg.runners, { name: 'dest', ssh, shell: 'sh -c', root }] };
  return root;
}

function writeRunnerRecord(root, ticketId, record) {
  const dir = path.join(root, 'tickets', ticketId);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'result.json'), JSON.stringify({ protocol: 1, ticketId, generation: 0, manifestHash: MANIFEST, ...record }));
}

/** An ssh that logs `<subcommand> <src|dest>` per call, answers `remote-result` on dest from `destResults` (one entry per call:
 *  'real', 'fail' or a reply object; 'real' once exhausted), can answer dest's `remote-withdraw` with not-queued, and can run `afterDestCancel`. */
function twoHostSsh(ctx, destRoot, { destResults = [], srcReply = null, cancel = 'real', afterDestCancel = '' } = {}) {
  const real = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  const dir = tmpDir('reconcile-two-host-ssh');
  const log = path.join(dir, 'calls.log');
  const counter = path.join(dir, 'dest-results');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const fs = require('fs');
const { spawnSync } = require('child_process');
const args = process.argv.slice(2);
const cmd = args[args.length - 1];
const onDest = cmd.includes(${JSON.stringify(destRoot)});
const sub = (/remote-(result|withdraw|cancel)/.exec(cmd) || [])[1];
fs.appendFileSync(${JSON.stringify(log)}, sub + ' ' + (onDest ? 'dest' : 'src') + '\\n');
if (!onDest && sub === 'result' && ${JSON.stringify(srcReply)} !== null) { process.stdout.write(JSON.stringify(${JSON.stringify(srcReply)}) + '\\n'); process.exit(0); }
if (onDest && sub === 'result') {
  const n = fs.existsSync(${JSON.stringify(counter)}) ? Number(fs.readFileSync(${JSON.stringify(counter)}, 'utf8')) : 0;
  fs.writeFileSync(${JSON.stringify(counter)}, String(n + 1));
  const reply = ${JSON.stringify(destResults)}[n] ?? 'real';
  if (reply === 'fail') process.exit(255);
  if (reply !== 'real') { process.stdout.write(JSON.stringify(reply) + '\\n'); process.exit(0); }
}
if (onDest && sub === 'withdraw' && ${JSON.stringify(cancel)} === 'no-tombstone') { process.stdout.write(JSON.stringify({ protocol: 1, ticketId: /[0-9a-f]{8}-[0-9a-f-]{27}/.exec(cmd)[0], action: 'no-such-ticket' }) + '\\n'); process.exit(0); }
if (onDest && sub === 'withdraw' && ${JSON.stringify(cancel)} === 'unconfirmed') { process.stdout.write(JSON.stringify({ protocol: 1, ticketId: /[0-9a-f]{8}-[0-9a-f-]{27}/.exec(cmd)[0], action: 'not-queued' }) + '\\n'); process.exit(0); }
const res = spawnSync(${JSON.stringify(real)}, args, { stdio: 'inherit', env: process.env });
if (onDest && sub === 'withdraw') { ${afterDestCancel} }
process.exit(res.status == null ? 1 : res.status);
`,
  );
  fs.chmodSync(bin, 0o755);
  const calls = () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter(Boolean) : []);
  return { bin, calls, callsTo: (entry) => calls().filter((c) => c === entry).length };
}

const assertHeld = (ctx, id, outcome, state) => {
  assert.equal(outcome.settled, false, JSON.stringify(outcome));
  assert.equal(outcome.state, state, JSON.stringify(outcome));
  assertKept(ctx, id);
};

test('withdrawing move: source says withdrawn and destination is gone and a withdraw confirms the tombstone -> lost runner-never-started, destination tombstoned', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assert.deepEqual(outcome, { settled: true, state: 'lost', exit: 75 });
  const result = resultOf(ctx.state, id);
  assert.equal(result.reason, 'runner-never-started');
  assert.match(result.lostDetail, /skybox.*withdrawn.*dest.*absent and tombstoned/);
  assert.equal(readAttempt(ctx.state, id), null);
  assert.equal(historyRows(ctx.state).length, 1);
  assert.equal(ssh.callsTo('withdraw dest'), 1, 'the destination withdraw was issued');
  assert.equal(ssh.callsTo('cancel src'), 0, 'the source is never cancelled');
  assert.ok(fs.existsSync(tombstonePath(destRoot, id)), 'the destination refuses a later exec for this ticket');
  assertNothingRan(ctx);
});

test('withdrawing move: destination reports the job running -> held, and it is never cancelled', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot, { destResults: [{ missing: true, state: 'running' }] });
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assertHeld(ctx, id, outcome, 'running');
  assert.match(outcome.reason, /destination dest/);
  assert.equal(ssh.callsTo('withdraw dest'), 0);
  assert.equal(fs.existsSync(tombstonePath(destRoot, id)), false);
});

test('withdrawing move: destination reports the job queued -> held, and it is never cancelled', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot, { destResults: [{ missing: true, state: 'queued' }] });
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'running');
  assert.equal(ssh.callsTo('withdraw dest'), 0);
});

test('withdrawing move: destination holds a result record for the job -> held, no cancel', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  writeRunnerRecord(destRoot, id, completed());
  const ssh = twoHostSsh(ctx, destRoot);
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assertHeld(ctx, id, outcome, 'ambiguous');
  assert.match(outcome.reason, /destination dest has a record/);
  assert.equal(ssh.callsTo('withdraw dest'), 0);
});

test('withdrawing move: destination unreachable -> held, and the source is not marked down for the sweep', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx, 'down');
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assertHeld(ctx, id, outcome, 'unreachable');
  assert.equal(outcome.transport, undefined, 'a down destination must not flag the source as down');
  assert.equal(readUnreachable(ctx.state).skybox, undefined);
  assert.equal(ssh.callsTo('withdraw dest'), 0);
});

test('withdrawing move: destination withdraw does not confirm a tombstone -> held', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot, { cancel: 'unconfirmed' });
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assertHeld(ctx, id, outcome, 'unreachable');
  assert.equal(outcome.transport, undefined);
  assert.equal(ssh.callsTo('withdraw dest'), 1);
});

test('withdrawing move: a record appears on the destination after the withdraw -> held', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot, { destResults: ['real', completed({ protocol: 1, ticketId: id, generation: 0, manifestHash: MANIFEST })] });
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'ambiguous');
});

test('withdrawing move: source unreachable past runnerLostAfterMs is held (the bound never settles a move); a source marked offline is held without contact', async () => {
  const ctx = context({ runnerLostAfterMs: 1000 });
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  ctx.globalCfg = { ...ctx.globalCfg, runners: ctx.globalCfg.runners.map((r) => (r.name === 'skybox' ? { ...r, ssh: 'down' } : r)) };
  await recordContact(ctx.state, 'skybox', false, Date.now() - HOUR, {});
  const ssh = twoHostSsh(ctx, destRoot);
  const down = await reconcile(ctx, id, { sshBin: ssh.bin });
  assertHeld(ctx, id, down, 'unreachable');
  assert.equal(down.transport, true);
  assert.equal(ssh.callsTo('result dest'), 0, 'the destination is not asked while the source is silent');

  const callsBeforeOffline = ssh.calls().length;
  ctx.globalCfg = { ...ctx.globalCfg, runners: ctx.globalCfg.runners.map((r) => (r.name === 'skybox' ? { ...r, ssh: 'normal', offline: true } : r)) };
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'unreachable');
  assert.equal(ssh.calls().length, callsBeforeOffline, 'an offline source is not contacted, so its withdrawn record cannot settle anything');
});

test('withdrawing move: source record with a mismatched manifestHash is ambiguous, destination never asked', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: { ...WITHDRAWN, manifestHash: 'x'.repeat(64) }, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'ambiguous');
  assert.equal(ssh.callsTo('result dest'), 0);
});

test('withdrawing move: source has no record of the job (gone) -> held and the source is not cancelled', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'ambiguous');
  assert.deepEqual(ssh.calls(), ['result src']);
});

test('a move in phase dispatching is still skipped', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt({ moving: { ...withdrawingMove(), phase: 'dispatching' } }) });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'skipped');
  assert.deepEqual(ssh.calls(), []);
});

test('a stale withdrawing move on an attempt that is no longer on moving.from (BRAIN-350 redispatch) is skipped', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt({ runner: 'dest' }) });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'skipped');
  assert.deepEqual(ssh.calls(), []);
});

test('withdrawing move: a completed source record is adopted and the destination is never contacted', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: completed(), attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  assert.deepEqual(await reconcile(ctx, id, { sshBin: ssh.bin }), { settled: true, state: 'adopted', exit: 3 });
  assert.equal(readAttempt(ctx.state, id), null);
  assert.equal(resultOf(ctx.state, id).exit, 3);
  assert.deepEqual(ssh.calls(), ['result src']);
});

test('withdrawing move fence: the move advancing to dispatching between the read and the publish is not published over', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const attemptFile = path.join(paths(ctx.state).attempts, `${id}.json`);
  const advance = `const a = JSON.parse(fs.readFileSync(${JSON.stringify(attemptFile)}, 'utf8')); fs.writeFileSync(${JSON.stringify(attemptFile)}, JSON.stringify({ ...a, moving: { ...a.moving, phase: 'dispatching' } }));`;
  const ssh = twoHostSsh(ctx, destRoot, { afterDestCancel: advance });
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assert.equal(outcome.settled, false, JSON.stringify(outcome));
  assert.equal(outcome.state, 'skipped');
  assert.equal(resultOf(ctx.state, id), null, 'no result may be published over an advanced move');
  assert.equal(readAttempt(ctx.state, id).moving.phase, 'dispatching');
  assert.deepEqual(historyRows(ctx.state), []);
});

test('withdrawing move fence: a supervisor found alive between the read and the publish is not settled', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const attemptFile = path.join(paths(ctx.state).attempts, `${id}.json`);
  const live = { pid: process.pid, startTime: processStartTime(process.pid), bootId: 'a-different-boot', kernelBootId: kernelBootId() };
  const revive = `const a = JSON.parse(fs.readFileSync(${JSON.stringify(attemptFile)}, 'utf8')); fs.writeFileSync(${JSON.stringify(attemptFile)}, JSON.stringify({ ...a, supervisor: ${JSON.stringify(live)} }));`;
  const ssh = twoHostSsh(ctx, destRoot, { afterDestCancel: revive });
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assert.equal(outcome.settled, false, JSON.stringify(outcome));
  assert.equal(outcome.state, 'skipped');
  assert.equal(resultOf(ctx.state, id), null, 'no result may be published over a live supervisor');
  assert.deepEqual(readAttempt(ctx.state, id).supervisor, live);
  assert.deepEqual(historyRows(ctx.state), []);
});

test('withdrawing move: a repaired ticket id is asked of the source only; the destination is asked for, and tombstoned under, the lane id', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const repaired = crypto.randomUUID();
  const id = seedOrphan(ctx, { binding: { ...BINDING, ticketId: repaired }, attempt: strandedAttempt() });
  writeRunnerRecord(ctx.runnerRoot, repaired, WITHDRAWN);
  const ssh = twoHostSsh(ctx, destRoot);
  assert.deepEqual(await reconcile(ctx, id, { sshBin: ssh.bin }), { settled: true, state: 'lost', exit: 75 });
  assert.ok(fs.existsSync(tombstonePath(destRoot, id)), 'the destination tombstone is for the lane id');
  assert.equal(fs.existsSync(tombstonePath(destRoot, repaired)), false, 'not for the source-side repaired id');
  assert.deepEqual(ssh.calls(), ['result src', 'result dest', 'withdraw dest', 'result dest']);
});

test('withdrawing move: a destination job under the lane id is held even when the binding holds a repaired id', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const repaired = crypto.randomUUID();
  const id = seedOrphan(ctx, { binding: { ...BINDING, ticketId: repaired }, attempt: strandedAttempt() });
  writeRunnerRecord(ctx.runnerRoot, repaired, WITHDRAWN);
  writeRunnerRecord(destRoot, id, completed());
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'ambiguous');
  assert.equal(ssh.callsTo('withdraw dest'), 0);
});

test('withdrawing move: a source record of kind cancelled is held, not adopted', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: { kind: 'cancelled' }, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'ambiguous');
  assert.deepEqual(ssh.calls(), ['result src']);
});

test('withdrawing move: a destination ticket dir with no job or record (withdraw does not answer no-such-ticket) is held, no tombstone', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  fs.mkdirSync(path.join(destRoot, 'tickets', id), { recursive: true });
  const ssh = twoHostSsh(ctx, destRoot, { destResults: [{ missing: true, state: 'gone' }] });
  const outcome = await reconcile(ctx, id, { sshBin: ssh.bin });
  assertHeld(ctx, id, outcome, 'unreachable');
  assert.match(outcome.reason, /not-queued/);
  assert.equal(fs.existsSync(tombstonePath(destRoot, id)), false);
});

test('withdrawing move whose supervisor died in an earlier boot is skipped: no host contacted, attempt kept (stays cancel-only)', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt({ supervisor: { ...DEAD_SUPERVISOR, bootId: 'an-earlier-boot', kernelBootId: 'an-earlier-kernel-boot' } }) });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'skipped');
  assert.deepEqual(ssh.calls(), []);
});

test('withdrawing move: the same bootId but a different kernelBootId is skipped, no host contacted', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt({ supervisor: { ...thisBootDead(), kernelBootId: 'another-kernel-boot' } }) });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'skipped');
  assert.deepEqual(ssh.calls(), []);
});

test('withdrawing move: a different bootId but the same kernelBootId with a live supervisor is skipped, no host contacted', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const live = { pid: process.pid, startTime: processStartTime(process.pid), bootId: 'a-skewed-boot-id', kernelBootId: kernelBootId() };
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt({ supervisor: live }) });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'skipped');
  assert.deepEqual(ssh.calls(), []);
});

test('withdrawing move: an attempt with no kernelBootId is skipped, no host contacted', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const { kernelBootId: _omitted, ...supervisor } = thisBootDead();
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt({ supervisor }) });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'skipped');
  assert.deepEqual(ssh.calls(), []);
});

test('withdrawing move: self=true on a stranded attempt is skipped, no host contacted', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin, self: true }), 'skipped');
  assert.deepEqual(ssh.calls(), []);
});

test('withdrawing move: assumeDown is not settled and no host is contacted', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin, assumeDown: true }), 'unreachable');
  assert.deepEqual(ssh.calls(), []);
});

for (const state of ['running', 'queued']) {
  test(`withdrawing move: source reports the job ${state} -> running, nothing published, destination never asked`, async () => {
    const ctx = context();
    const destRoot = addDestination(ctx);
    const id = seedOrphan(ctx, { attempt: strandedAttempt() });
    const ssh = twoHostSsh(ctx, destRoot, { srcReply: { missing: true, state } });
    assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'running');
    assert.deepEqual(ssh.calls(), ['result src']);
  });
}

test('withdrawing move: a source record that provesStopped is held ambiguous, destination never asked', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: { kind: 'unfinished', reason: RUNNER_SHUTDOWN_REASON }, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot);
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'ambiguous');
  assert.deepEqual(ssh.calls(), ['result src']);
});

test('withdrawing move: destination withdraw answering no-such-ticket without tombstoned is held', async () => {
  const ctx = context();
  const destRoot = addDestination(ctx);
  const id = seedOrphan(ctx, { record: WITHDRAWN, attempt: strandedAttempt() });
  const ssh = twoHostSsh(ctx, destRoot, { cancel: 'no-tombstone' });
  assertHeld(ctx, id, await reconcile(ctx, id, { sshBin: ssh.bin }), 'unreachable');
  assert.equal(fs.existsSync(tombstonePath(destRoot, id)), false);
});
