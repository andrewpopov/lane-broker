import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, waitFor, sleep, writeGlobalConfig, freshEnv } from './helpers.js';
import { resultOf, rebalanceSetup, setup, execArrived, T } from './remote-harness.js';
import { readAttempt, supervisorAlive } from '../src/attempts.js';
import { paths, atomicWriteJson, ensureStateDirs, bootId } from '../src/state.js';
import { processStartTime } from '../src/process-liveness.js';
import { reconcileRemoteAttempt } from '../src/remote-reconcile.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig, ConfigError } from '../src/config.js';

/**
 * BRAIN-350 S3: a runner that drops WITHOUT the shutdown hook (crash, forced restart) leaves the job's fate unknown. The supervisor stays
 * alive, asks reconcile (`self` mode) every `remoteLostPollMs`, and once the job is PROVEN gone (offline flag, `runnerLostAfterMs`, a
 * runner record) re-dispatches it to a runner it has not been on, else local. A runner that answers `running` is never re-run (BRAIN-437).
 */

const OPTS = { remoteRebalanceMinQueuedMs: 0, remoteLostPollMs: 200 };
const historyRow = (ctx, id) => fs.readFileSync(path.join(ctx.state, 'history.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l)).find((r) => r.id === id);

/** Runner `a` goes dark mid-attempt: every ssh call fails and the exec connection dies; resolves once the supervisor is waiting for proof. */
async function dropA(ctx) {
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.fake.setDown('a', true);
  ctx.fake.dropExec('a');
  await waitFor(() => readAttempt(ctx.state, id)?.awaitingProof, { timeoutMs: 60_000 });
  return id;
}

const setOffline = (ctx, name) => {
  const cfg = JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8'));
  cfg.runners.find((r) => r.name === name).offline = true;
  writeGlobalConfig(ctx.home, cfg);
};

test('(a) the operator marks A offline while the supervisor waits: re-dispatched to B promptly, once, not local', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a'], rebalance: { ...OPTS, runnerLostAfterMs: 0 } });
  const id = await dropA(ctx);
  assert.equal(readAttempt(ctx.state, id).awaitingProof.runner, 'a');
  assert.ok(readAttempt(ctx.state, id).unresolved, 'a supervisor killed now still leaves a reconcilable orphan');
  const status = await laneRun(['status'], { env: ctx.env });
  assert.match(status.stdout, new RegExp(`${id}  runner=a .*awaiting proof from a since \\d\\d:\\d\\d \\(dropped\\)`));
  assert.equal(ctx.fake.count('exec b'), 0, 'nothing is re-dispatched without proof');
  setOffline(ctx, 'a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.equal(ctx.fake.count('exec b'), 1);
  const result = resultOf(ctx.state, id);
  assert.equal(result.runner, 'b');
  assert.deepEqual(result.redispatchedFrom, ['a']);
  assert.equal(result.redispatchReason, 'runner-offline');
  assert.equal(result.rebalanceReason, 'a lost the job (runner-offline)');
  const row = historyRow(ctx, id);
  assert.deepEqual(row.redispatchedFrom, ['a']);
  assert.equal(row.redispatchReason, 'runner-offline');
  assert.equal(row.localReason, undefined);
  const attempt = readAttempt(ctx.state, id);
  assert.equal(attempt, null, 'the settled attempt is gone');
});

test('(b) A stays unreachable past runnerLostAfterMs: re-dispatched to B', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a'], rebalance: { ...OPTS, runnerLostAfterMs: 1500 } });
  const id = await dropA(ctx);
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  const row = historyRow(ctx, id);
  assert.deepEqual(row.redispatchedFrom, ['a']);
  assert.equal(row.redispatchReason, 'runner-lost');
  assert.equal(row.runner, 'b');
  assert.equal(row.localReason, undefined);
});

test('(c) BRAIN-437: A comes back answering running before the bound: nothing is re-dispatched and the result from A is adopted', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a'], rebalance: { ...OPTS, runnerLostAfterMs: 1500 } });
  const id = await dropA(ctx);
  ctx.fake.setDown('a', false);
  await sleep(3500); // well past the bound: only a failed contact may start the clock, and A answers `running` (queued) every poll
  assert.ok(readAttempt(ctx.state, id)?.awaitingProof, 'still waiting, not settled as lost');
  assert.equal(ctx.fake.count('exec b'), 0, 'never dispatched to B');
  assert.deepEqual(ctx.runs(), []);
  ctx.unpause('a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['a'], 'ran exactly once, on A');
  assert.equal(ctx.fake.count('exec b'), 0);
  const result = resultOf(ctx.state, id);
  assert.equal(result.runner, 'a');
  assert.equal(result.reconciled, true);
  assert.equal(result.redispatchedFrom, undefined);
  assert.equal(readAttempt(ctx.state, id), null);
});

test('(d) no other runner: falls back to local with localReason fallback:runner-lost, and runs once', T, async () => {
  const ctx = rebalanceSetup({ names: ['a'], paused: ['a'], rebalance: { ...OPTS, runnerLostAfterMs: 1500 } });
  const id = await dropA(ctx);
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(ctx.fake.count('exec a'), 1, 'never sent to A a second time');
  assert.deepEqual(ctx.runs(), ['undefined'], 'ran once, locally');
  assert.equal(historyRow(ctx, id).localReason, 'fallback:runner-lost');
});

test('(e) remoteLostPollMs 0 keeps today\'s behaviour: the supervisor exits at once, the attempt is left unresolved and nothing is re-run', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a'], rebalance: { ...OPTS, remoteLostPollMs: 0, runnerLostAfterMs: 1500 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.fake.setDown('a', true);
  ctx.fake.dropExec('a');
  await waitFor(() => readAttempt(ctx.state, id)?.unresolved, { timeoutMs: 60_000 });
  await waitFor(() => !supervisorAlive(readAttempt(ctx.state, id)), { timeoutMs: 30_000 });
  await sleep(2500);
  const attempt = readAttempt(ctx.state, id);
  assert.equal(attempt.awaitingProof, undefined);
  assert.equal(ctx.fake.count('exec b'), 0);
  assert.deepEqual(ctx.runs(), []);
  assert.equal(resultOf(ctx.state, id), null, 'not settled by the supervisor');
});

test('(f) lane cancel while waiting: the runner is asked to cancel and the ticket ends cancelled (130), nothing re-dispatched', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a'], rebalance: { ...OPTS, runnerLostAfterMs: 0 } });
  const id = await dropA(ctx);
  ctx.fake.setDown('a', false);
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  assert.ok(ctx.fake.count('cancel a') >= 1, 'the runner was asked to cancel');
  assert.equal(ctx.fake.count('exec b'), 0);
  assert.deepEqual(ctx.runs(), []);
  assert.equal(resultOf(ctx.state, id).exit, 130);
  assert.equal(readAttempt(ctx.state, id), null);
});

test('(f) lane cancel while waiting and the runner cannot confirm: the attempt stays unresolved and cancel fails', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a'], rebalance: { ...OPTS, runnerLostAfterMs: 0 } });
  const id = await dropA(ctx);
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 1, cancelled.stderr);
  const attempt = readAttempt(ctx.state, id);
  assert.match(attempt.unresolved, /remote cancel on a was not confirmed/);
  assert.equal(attempt.awaitingProof, undefined);
  assert.equal(ctx.fake.count('exec b'), 0);
});

// ---- reconcileRemoteAttempt's owner mode, called directly ----

function seedOwned(ctx, runnerName = 'skybox') {
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), {
    id, generation: 0, executor: 'remote', phase: 'running', runner: runnerName, startedAt: Date.now() - 60_000,
    supervisor: { pid: process.pid, startTime: processStartTime(process.pid) ?? null, bootId: bootId() },
    binding: { protocol: 1, generation: 0, manifestHash: 'm'.repeat(64), runner: runnerName },
  });
  return id;
}

test('self mode: a live owner is reconciled, a lost outcome is returned and NOT published; without self the live supervisor is skipped', async () => {
  const ctx = setup();
  const id = seedOwned(ctx);
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, ...JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8')) };
  cfg.runners = cfg.runners.map((r) => ({ ...r, offline: true }));
  const args = { globalCfg: cfg, env: ctx.env };
  assert.equal((await reconcileRemoteAttempt(ctx.state, id, args)).state, 'skipped', 'every other caller is unchanged');
  const lost = await reconcileRemoteAttempt(ctx.state, id, { ...args, self: true });
  assert.equal(lost.settled, false);
  assert.equal(lost.state, 'lost');
  assert.equal(lost.reason, 'runner-offline');
  assert.match(lost.lostDetail, /marked offline/);
  assert.ok(readAttempt(ctx.state, id), 'the attempt is still there');
  assert.equal(resultOf(ctx.state, id), null, 'no result was written');
  assert.equal(fs.existsSync(path.join(ctx.state, 'history.jsonl')), false, 'no history row either');
});

test('self mode only accepts the supervisor that is this process', async () => {
  const ctx = setup();
  const id = seedOwned(ctx);
  const file = path.join(paths(ctx.state).attempts, `${id}.json`);
  const attempt = JSON.parse(fs.readFileSync(file, 'utf8'));
  attempt.supervisor.pid = process.ppid; // a live process that is not us
  attempt.supervisor.startTime = processStartTime(process.ppid) ?? null;
  atomicWriteJson(file, attempt);
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, ...JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8')) };
  cfg.runners = cfg.runners.map((r) => ({ ...r, offline: true }));
  const out = await reconcileRemoteAttempt(ctx.state, id, { globalCfg: cfg, env: ctx.env, self: true });
  assert.equal(out.state, 'skipped');
});

test('config: remoteLostPollMs defaults to 60 s, accepts 0, rejects negatives and non-integers', () => {
  const { home } = freshEnv();
  const load = (patch) => {
    writeGlobalConfig(home, { version: 1, ...patch });
    const prev = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      process.env.LANE_BROKER_HOME = prev;
    }
  };
  assert.equal(load({}).remoteLostPollMs, 60_000);
  assert.equal(load({ remoteLostPollMs: 0 }).remoteLostPollMs, 0);
  for (const bad of [-1, 1.5, '60000']) assert.throws(() => load({ remoteLostPollMs: bad }), ConfigError, String(bad));
});
