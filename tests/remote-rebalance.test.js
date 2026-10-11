import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, waitFor, sleep, writeGlobalConfig, gitFixture } from './helpers.js';
import { tmpDir, setup, resultOf, holdLaneLocally, IDLE, rebalanceSetup, execArrived, T } from './remote-harness.js';
import { rebalanceBlocked, hasMeasuredHeadroom } from '../src/remote-client.js';
import { readAttempt, patchAttemptLocked } from '../src/attempts.js';
import { dispatchRemote } from '../src/remote-client.js';
import { makeGitWorktree } from './remote-harness.js';
import crypto from 'node:crypto';
import { listQueue } from '../src/scheduler.js';
import { withLock } from '../src/state.js';
import { loadGlobalConfig, ConfigError } from '../src/config.js';

/**
 * BRAIN-436 S3: the policy around the S1/S2 mechanism. A ticket queued on runner A past `remoteRebalanceMinQueuedMs` is
 * withdrawn and moved to a runner with measured room, at most `remoteRebalanceMaxMoves` times, never back to a runner it left;
 * a supervisor that dies mid-move leaves a record `lane wait`/`status`/`cancel` can reconcile. Driven through the real
 * `lane run` over a two-runner fake ssh whose runners are real brokers (A is paused, so a ticket sent there stays queued).
 */

// ---- the pure policy ----

test('rebalanceBlocked: the cap and the cooldown, on a fake clock', () => {
  const policy = { maxMoves: 2, cooldownMs: 600_000 };
  assert.equal(rebalanceBlocked({ ...policy, moves: 0, lastMoveAt: null, now: 1_000 }), null);
  assert.equal(rebalanceBlocked({ ...policy, moves: 1, lastMoveAt: 1_000, now: 1_000 + 599_999 }), 'cooldown');
  assert.equal(rebalanceBlocked({ ...policy, moves: 1, lastMoveAt: 1_000, now: 1_000 + 600_000 }), null);
  assert.equal(rebalanceBlocked({ ...policy, moves: 2, lastMoveAt: 1_000, now: 9_999_999 }), 'max-moves');
  assert.equal(rebalanceBlocked({ maxMoves: 1, cooldownMs: 0, moves: 1, lastMoveAt: 5, now: 5 }), 'max-moves', 'MaxMoves 1: one move, never a second');
});

test('hasMeasuredHeadroom: only a measured free-CPU figure counts as room for a move', () => {
  assert.equal(hasMeasuredHeadroom({ headroom: { cpuCores: 4, memoryBytes: 1 } }), true);
  assert.equal(hasMeasuredHeadroom({ headroom: { cpuCores: 0 } }), true);
  for (const probe of [{}, { headroom: {} }, { headroom: { cpuCores: null } }, { capacity: { cpuCores: 8 }, reservedCpuCores: 0 }, null, undefined]) {
    assert.equal(hasMeasuredHeadroom(probe), false, JSON.stringify(probe));
  }
});

test('config: the four remoteRebalance* keys default, accept 0 where it means off, and reject bad values', () => {
  const load = (extra) => {
    const { home } = setup();
    writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000, ...extra });
    const prev = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      process.env.LANE_BROKER_HOME = prev;
    }
  };
  const cfg = load({});
  assert.deepEqual(
    [cfg.remoteRebalanceMinQueuedMs, cfg.remoteRebalanceIntervalMs, cfg.remoteRebalanceCooldownMs, cfg.remoteRebalanceMaxMoves],
    [300_000, 60_000, 600_000, 1],
  );
  assert.equal(load({ remoteRebalanceMinQueuedMs: 0 }).remoteRebalanceMinQueuedMs, 0);
  for (const key of ['remoteRebalanceMinQueuedMs', 'remoteRebalanceIntervalMs', 'remoteRebalanceCooldownMs', 'remoteRebalanceMaxMoves']) {
    for (const bad of [-1, 1.5, '5']) assert.throws(() => load({ [key]: bad }), ConfigError, `${key}=${bad}`);
  }
  assert.throws(() => load({ remoteRebalanceIntervalMs: 0 }), ConfigError, 'a zero interval would spin');
});

// ---- end to end ----

test('rebalance: a ticket queued on A moves to B and runs exactly once, on B, with A\'s wait counted', T, async () => {
  const ctx = rebalanceSetup();
  const id = await ctx.start();
  const waited = await ctx.finish(id, '25s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${ctx.fake.calls().join(',')}`);
  assert.deepEqual(ctx.runs(), ['b'], 'one run, on B');
  assert.equal(ctx.fake.count('withdraw a'), 1);
  assert.equal(ctx.fake.count('exec a'), 1);
  assert.equal(ctx.fake.count('exec b'), 1);
  const result = resultOf(ctx.state, id);
  assert.equal(result.executor, 'remote');
  assert.equal(result.runner, 'b');
  assert.equal(result.rebalancedFrom, 'a');
  assert.equal(result.moves, 1);
  assert.ok(Number.isFinite(result.rebalancedAt));
  assert.match(result.rebalanceReason, /queued \d+s on a/);
  assert.notEqual(result.reason, 'rebalanced', 'reason is not reused');
  assert.ok(result.waitedMs >= 350, `waitedMs ${result.waitedMs} must include the ~400ms wait on A`);
  const row = JSON.parse(fs.readFileSync(path.join(ctx.state, 'history.jsonl'), 'utf8').trim().split('\n').at(-1));
  assert.equal(row.rebalancedFrom, 'a');
  assert.equal(row.moves, 1);
  assert.equal(row.runner, 'b');
  assert.equal(readAttempt(ctx.state, id), null);
});

test('rebalance: no ping-pong -- with B queued too and A idle again, the ticket stays on B (and MaxMoves 1 holds with a third runner)', T, async () => {
  const ctx = rebalanceSetup({ names: ['a', 'b', 'c'], paused: ['a', 'b'], rebalance: { remoteRebalanceMaxMoves: 3, remoteRebalanceMinQueuedMs: 300 } });
  const id = await ctx.start();
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 30_000 });
  // `c` is idle with room and A is idle again: with MaxMoves 3 the only thing keeping the ticket on B is the no-revisit rule for A
  // (and, for C, nothing -- so C is taken out of play to pin that down)
  ctx.fake.setProbe('c', { ...IDLE, paused: true });
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw b'), 0, 'never moved off B back to A');
  assert.equal(ctx.fake.count('exec a'), 1, 'A was not dispatched to again');
  assert.equal(ctx.fake.count('withdraw a'), 1);
  ctx.unpause('b');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.equal(resultOf(ctx.state, id).moves, 1);
});

test('rebalance: MaxMoves 1 -- after A -> B the ticket does not move on to an idle C', T, async () => {
  const ctx = rebalanceSetup({ names: ['a', 'b', 'c'], paused: ['a', 'b'], rebalance: { remoteRebalanceMinQueuedMs: 300 } });
  ctx.fake.setProbe('c', { ...IDLE, paused: true }); // first move can only go to B
  const id = await ctx.start();
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 30_000 });
  ctx.fake.setProbe('c', IDLE);
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw b'), 0, 'the cap stops a second move');
  assert.equal(ctx.fake.count('exec c'), 0);
  ctx.unpause('b');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['b']);
});

test('rebalance: a changed worktree is never moved (the snapshot would be stale)', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 600 } });
  gitFixture(['add', '-f', '.lane-broker.json'], ctx.repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], ctx.repoDir);
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  fs.appendFileSync(path.join(ctx.repoDir, '.lane-broker.json'), '\n');
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0, 'no withdraw once the tree changed');
  assert.equal(ctx.fake.count('exec b'), 0);
  assert.equal(readAttempt(ctx.state, id)?.moving, undefined);
  ctx.unpause('a');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['a']);
});

test('rebalance: a B whose probe has no measured headroom is not a destination', T, async () => {
  const { headroom, ...unmeasured } = IDLE;
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 300 } });
  ctx.fake.setProbe('b', { ...unmeasured, capacity: { cpuCores: 8 }, reservedCpuCores: 0 });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0);
  ctx.unpause('a');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['a']);
});

test('rebalance: remoteRebalanceMinQueuedMs 0 disables it entirely -- nothing watches the runner', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await sleep(2500);
  assert.equal(ctx.fake.count('result a'), 0, 'no watcher poll');
  assert.equal(ctx.fake.count('withdraw a'), 0);
  ctx.unpause('a');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['a']);
});

test('rebalance: a cancel that lands while the destination is being chosen stops the move before any withdraw', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 300 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  const probesBefore = ctx.fake.count('probe b');
  ctx.fake.setProbe('b', { ...IDLE, delayMs: 3000 });
  await waitFor(() => ctx.fake.count('probe b') > probesBefore, { timeoutMs: 30_000 });
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  const waited = await ctx.finish(id, '30s');
  assert.equal(waited.code, 130, waited.stderr);
  await sleep(3500);
  assert.equal(ctx.fake.count('withdraw a'), 0);
  assert.deepEqual(ctx.runs(), []);
});

test('rebalance: a supervisor killed with `moving` recorded is MOVE-INTERRUPTED in wait and status, and cancel clears it on every runner', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  const attempt = readAttempt(ctx.state, id);
  process.kill(attempt.supervisor.pid, 'SIGKILL');
  await sleep(300);
  await withLock(ctx.state, () => patchAttemptLocked(ctx.state, id, { runner: 'b', moving: { from: 'a', to: 'b', phase: 'dispatching', at: Date.now() } }));

  const waited = await laneRun(['wait', id, '--timeout', '5s'], { env: ctx.env });
  assert.equal(waited.code, 1, waited.stderr);
  assert.match(waited.stderr, new RegExp(`MOVE-INTERRUPTED ${id} withdrawn from a`));
  assert.doesNotMatch(waited.stderr, /ORPHANED-REMOTE/);
  const status = await laneRun(['status'], { env: ctx.env });
  assert.match(status.stdout, /MOVE-INTERRUPTED/);

  const before = ctx.fake.calls().length;
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  const sent = ctx.fake.calls().slice(before);
  assert.ok(sent.includes('cancel a') && sent.includes('cancel b'), `remote-cancel on both runners, saw ${sent.join(',')}`);
  assert.equal(readAttempt(ctx.state, id), null, 'the move record is gone with the attempt');
  assert.equal(resultOf(ctx.state, id).exit, 130);
});

// ---- review fixes ----

test('P1-3: a cancel that overtakes a runner\'s pending exec leaves a tombstone, and the exec that arrives afterwards is refused -- nothing runs', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = crypto.randomUUID();
  const { createAttempt } = await import('../src/attempts.js');
  await createAttempt(ctx.state, id, { runner: 'b' });
  await withLock(ctx.state, () => patchAttemptLocked(ctx.state, id, { supervisor: { pid: 2 ** 22 + 7, startTime: null, bootId: 'dead' }, moving: { from: 'a', to: 'b', phase: 'dispatching', at: Date.now() } }));
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  const bRunner = ctx.globalRunners.find((r) => r.name === 'b');
  assert.ok(fs.existsSync(path.join(bRunner.root, 'tombstones', id)), 'b recorded that this ticket must never be created');
  // the exec that was still in flight now arrives
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const arrived = await dispatchRemote({
    ticketId: id, generation: 0, repoKey: 'r', lane: 'default', relCwd: '', argv: ctx.cmdArgv, runner: bRunner, worktreeRoot: src,
    sshBin: path.join(ctx.binDir, 'ssh'), env: ctx.env, deadlines: { resultMs: 5000, resultAttempts: 1 },
  });
  assert.notEqual(arrived.outcome, 'confirmed', JSON.stringify(arrived));
  assert.deepEqual(ctx.runs(), [], 'nothing ran');
  assert.equal(fs.existsSync(path.join(bRunner.root, 'tickets', id)), false, 'the ticket directory was never created');
});

test('P1-3: cancelling a stranded move keeps the attempt while a named runner has not confirmed', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  const attempt = readAttempt(ctx.state, id);
  process.kill(attempt.supervisor.pid, 'SIGKILL');
  await sleep(300);
  await withLock(ctx.state, () => patchAttemptLocked(ctx.state, id, { runner: 'b', moving: { from: 'a', to: 'b', phase: 'dispatching', at: Date.now() } }));
  const runnersPath = path.join(path.dirname(ctx.fake.probesPath), 'runners.json');
  const all = JSON.parse(fs.readFileSync(runnersPath, 'utf8'));
  const keep = all.b;
  delete all.b; // b is unreachable: its cancel cannot be confirmed
  fs.writeFileSync(runnersPath, JSON.stringify(all));
  const held = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(held.code, 1, held.stderr);
  assert.match(held.stderr, /not confirmed on b/);
  assert.ok(readAttempt(ctx.state, id), 'the attempt is still there');
  all.b = keep;
  fs.writeFileSync(runnersPath, JSON.stringify(all));
  const done = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(done.code, 0, done.stderr);
  assert.equal(readAttempt(ctx.state, id), null);
});

test('P1-2: an unknown withdraw keeps `moving`, and the possibly-live ticket stays reported (MOVE-INTERRUPTED), not run, not forgotten', T, async () => {
  const ctx = rebalanceSetup({ runnerOpts: { a: { withdrawReplyLost: true, resultDownAfterWithdraw: true } }, rebalance: { remoteRebalanceMinQueuedMs: 300 } });
  const id = await ctx.start();
  const waited = await ctx.finish(id, '60s');
  assert.equal(waited.code, 1, waited.stderr);
  assert.match(waited.stderr, /MOVE-INTERRUPTED/);
  const attempt = readAttempt(ctx.state, id);
  assert.ok(attempt, 'the attempt is kept');
  assert.equal(attempt.moving?.from, 'a');
  assert.ok(attempt.unresolved);
  assert.equal(ctx.fake.count('exec b'), 0, 'never dispatched without proof');
  assert.deepEqual(ctx.runs(), []);
});

test('P2-5: after A -> B fails before starting, the rebind never goes back to A and the cap is not reset', T, async () => {
  const ctx = rebalanceSetup({ runnerOpts: { b: { execFails: true } }, localPaused: true, rebalance: { remoteRebalanceMinQueuedMs: 300, remoteRebindIntervalMs: 200 } });
  const id = await ctx.start();
  await waitFor(() => ctx.fake.count('exec b') >= 1, { timeoutMs: 30_000 });
  await sleep(3000);
  assert.equal(ctx.fake.count('exec a'), 1, 'A was never dispatched to again');
  assert.equal(ctx.fake.count('exec b'), 1);
  assert.equal(readAttempt(ctx.state, id)?.moves?.length, 1);
  await laneRun(['cancel', id], { env: ctx.env });
});

test('P2-8: a runner with rebalanceTarget false is never a destination', T, async () => {
  const ctx = rebalanceSetup({ configOpts: { b: { rebalanceTarget: false } }, rebalance: { remoteRebalanceMinQueuedMs: 300 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0);
  ctx.unpause('a');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['a']);
});

test('config: a runner\'s rebalanceTarget must be a boolean', () => {
  const { home } = setup();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000, runners: [{ name: 'w', ssh: 'w', rebalanceTarget: 'no' }] });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    assert.throws(() => loadGlobalConfig(), ConfigError);
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});

test('a live cancel the runner does not confirm keeps the attempt and fails `lane cancel`; once confirmed it clears', T, async () => {
  const ctx = rebalanceSetup({ runnerOpts: { a: { cancelFails: true } }, rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await waitFor(() => listQueue(ctx.runners.a.state).length === 1, { timeoutMs: 30_000 }); // queued on A, so its cancel is a real, confirmable one
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 1, cancelled.stderr);
  assert.match(cancelled.stderr, /still held/);
  const attempt = readAttempt(ctx.state, id);
  assert.ok(attempt?.unresolved, 'the attempt is kept and marked');
  const waited = await ctx.finish(id, '10s');
  assert.match(waited.stderr, /ORPHANED-REMOTE/);
  const runnersPath = path.join(path.dirname(ctx.fake.probesPath), 'runners.json');
  const all = JSON.parse(fs.readFileSync(runnersPath, 'utf8'));
  delete all.a.cancelFails;
  fs.writeFileSync(runnersPath, JSON.stringify(all));
  const again = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(again.code, 0, again.stderr);
  assert.equal(readAttempt(ctx.state, id), null);
  assert.equal(resultOf(ctx.state, id).exit, 130);
});

test('a confirmed live cancel is unchanged: exit 0, attempt gone, result 130', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  assert.equal(readAttempt(ctx.state, id), null);
  assert.equal(resultOf(ctx.state, id).exit, 130);
});

test('P2: a ticket rejected before its remote-id exists answers cancelConfirmed, so cancelling its unresolved attempt completes', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = crypto.randomUUID();
  const { createAttempt } = await import('../src/attempts.js');
  await createAttempt(ctx.state, id, { runner: 'b' });
  await withLock(ctx.state, () => patchAttemptLocked(ctx.state, id, { supervisor: { pid: 2 ** 22 + 9, startTime: null, bootId: 'dead' }, unresolved: 'test' }));
  const bRoot = ctx.globalRunners.find((r) => r.name === 'b').root;
  fs.mkdirSync(path.join(bRoot, 'tickets', id), { recursive: true });
  fs.writeFileSync(path.join(bRoot, 'tickets', id, 'result.json'), JSON.stringify({ protocol: 1, ticketId: id, kind: 'rejected', reason: 'bad header' })); // no remote-id, ever
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  assert.equal(readAttempt(ctx.state, id), null);
});

test('rebalance never picks a runner the ticket was already dispatched to: A times out, the ticket rebinds to B, and B does not rebalance it back to A', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a', 'b'], rebalance: { remoteRebalanceMinQueuedMs: 0, remoteQueueTimeoutMs: 800, remoteRebindIntervalMs: 300 } });
  const hold = await holdLaneLocally(ctx);
  const id = await ctx.start();
  const queuedLocally = () => listQueue(ctx.state).some((t) => t.id === id);
  await waitFor(() => queuedLocally() && readAttempt(ctx.state, id)?.dispatchedRunners?.length === 1, { timeoutMs: 60_000 });
  // A's timeout is behind us; from here a ticket queued on B is eligible to be rebalanced, and it outlives the threshold on B
  const globalPath = path.join(ctx.home, 'config.json');
  fs.writeFileSync(globalPath, JSON.stringify({ ...JSON.parse(fs.readFileSync(globalPath, 'utf8')), remoteRebalanceMinQueuedMs: 300, remoteQueueTimeoutMs: 6000 }));
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 60_000 });
  await sleep(2500); // many rebalance ticks while it sits queued on B, with A idle in every probe
  assert.equal(ctx.fake.count('withdraw b'), 0, 'never withdrawn from B back to A');
  assert.equal(ctx.fake.count('exec a'), 1, 'A was dispatched to once');
  assert.deepEqual(readAttempt(ctx.state, id).dispatchedRunners, ['a', 'b']);
  await waitFor(() => queuedLocally() && !ctx.runs().length, { timeoutMs: 60_000 });
  hold.release();
  await hold.done;
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs().filter((r) => r === 'a' || r === 'b'), [], 'never ran on a runner');
  assert.equal(ctx.runs().length, 1, 'the command ran exactly once, locally');
});
