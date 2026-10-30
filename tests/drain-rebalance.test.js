import test from 'node:test';
import assert from 'node:assert/strict';
import { laneRun, waitFor, sleep } from './helpers.js';
import { resultOf, IDLE, rebalanceSetup, execArrived, T } from './remote-harness.js';
import { readAttempt, patchAttemptLocked } from '../src/attempts.js';
import { withLock, writeRemoteDrain } from '../src/state.js';
import { CAPABILITIES, REMOTE_WITHDRAW_CAPABILITY } from '../src/capabilities.js';

/**
 * BRAIN-520 slice 2: the SUBMITTER side of `lane drain`. A ticket already queued on a runner whose operator drained it is
 * withdrawn and moved without waiting out `remoteRebalanceMinQueuedMs`, past the cooldown and the max-moves cap, but never
 * back to a runner it left. The runner side is real here (a real drain marker on the runner's state root, read by the real
 * `remote-result`); probes are synthetic, so a drained destination is also marked in its probe.
 */

const drainRunner = (ctx, name, reason = 'maint') => writeRemoteDrain(ctx.runners[name].state, reason);
const drainedProbe = (reason = 'maint') => ({ ...IDLE, draining: true, paused: true, remoteDrain: { reason, since: Date.now() } });

test('drain: a ticket queued on a drained runner moves to B long before the minimum-queued wait, and runs once, on B', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a', 'b'], rebalance: { remoteRebalanceMinQueuedMs: 60_000 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await sleep(600);
  assert.equal(ctx.fake.count('withdraw a'), 0, 'not drained yet: the 60s wait still applies');
  const drainedAt = Date.now();
  await drainRunner(ctx, 'a', 'disk swap');
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 20_000 });
  assert.ok(Date.now() - drainedAt < 20_000, 'moved well under the 60s minimum');
  await waitFor(() => readAttempt(ctx.state, id)?.moves?.length === 1, { timeoutMs: 10_000 });
  const [move] = readAttempt(ctx.state, id).moves;
  assert.equal(move.reason, 'runner-draining');
  assert.equal(move.drainReason, 'disk swap');
  assert.deepEqual([move.from, move.to], ['a', 'b']);
  ctx.unpause('b');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b'], 'exactly once, on B');
  assert.equal(ctx.fake.count('withdraw a'), 1);
  const result = resultOf(ctx.state, id);
  assert.equal(result.rebalancedFrom, 'a');
  assert.equal(result.rebalanceReason, 'a draining (disk swap)');
});

test('drain: bypasses the cooldown and the max-moves cap (a heuristic move A -> B, then B drained -> C)', T, async () => {
  const ctx = rebalanceSetup({
    names: ['a', 'b', 'c'], paused: ['a', 'b'],
    rebalance: { remoteRebalanceMinQueuedMs: 300, remoteRebalanceMaxMoves: 1, remoteRebalanceCooldownMs: 600_000 },
  });
  ctx.fake.setProbe('c', { ...IDLE, paused: true }); // the first move can only go to B
  const id = await ctx.start();
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 30_000 });
  await sleep(1500);
  assert.equal(ctx.fake.count('withdraw b'), 0, 'the cap and cooldown hold a heuristic move');
  ctx.fake.setProbe('c', IDLE);
  await drainRunner(ctx, 'b', 'maint b');
  ctx.fake.setProbe('b', drainedProbe('maint b'));
  await waitFor(() => ctx.fake.count('exec c') === 1, { timeoutMs: 20_000 });
  assert.equal(ctx.fake.count('withdraw b'), 1);
  assert.equal(ctx.fake.count('exec a'), 1, 'never back to A');
  const moves = readAttempt(ctx.state, id).moves;
  assert.deepEqual(moves.map((m) => m.reason), ['queued-too-long', 'runner-draining']);
  const result = await ctx.finish(id);
  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(ctx.runs(), ['c']);
  assert.equal(resultOf(ctx.state, id).moves, 2);
  assert.match(resultOf(ctx.state, id).rebalanceReason, /^b draining \(maint b\)$/);
});

test('drain: with no destination the ticket stays on the drained runner and completes there, exit 0', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 60_000 } });
  ctx.fake.setProbe('b', { ...IDLE, paused: true });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await drainRunner(ctx, 'a');
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0, 'nowhere to go: not withdrawn');
  assert.equal(ctx.fake.count('exec b'), 0);
  ctx.unpause('a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['a'], 'a drained runner still runs what is queued on it');
});

test('drain: the no-revisit rule holds -- A -> B by the heuristic, A idle, B drained: the ticket does not go back to A', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a', 'b'], rebalance: { remoteRebalanceMinQueuedMs: 300 } });
  const id = await ctx.start();
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 30_000 });
  await drainRunner(ctx, 'b');
  ctx.fake.setProbe('b', drainedProbe());
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw b'), 0, 'A is not a destination');
  assert.equal(ctx.fake.count('exec a'), 1);
  ctx.unpause('b');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
});

test('drain: a runner with rebalanceTarget false is never a drain destination', T, async () => {
  const ctx = rebalanceSetup({ configOpts: { b: { rebalanceTarget: false } }, rebalance: { remoteRebalanceMinQueuedMs: 60_000 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await drainRunner(ctx, 'a');
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0);
  ctx.unpause('a');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['a']);
});

test('drain (B1): a destination that cannot withdraw is not chosen for a drain move, while the heuristic rule is unchanged', T, async () => {
  const noWithdraw = { ...IDLE, capabilities: CAPABILITIES.filter((c) => c !== REMOTE_WITHDRAW_CAPABILITY) };
  assert.notDeepEqual(noWithdraw.capabilities, CAPABILITIES, 'the fixture really lacks the capability');
  const drained = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 60_000 } });
  drained.fake.setProbe('b', noWithdraw);
  const drainedId = await drained.start();
  await execArrived(drained, 'a');
  await drainRunner(drained, 'a');
  await sleep(2500);
  assert.equal(drained.fake.count('withdraw a'), 0, 'drain move refused: B lacks remote-withdraw/1');
  drained.unpause('a');
  assert.equal((await drained.finish(drainedId)).code, 0);
  assert.deepEqual(drained.runs(), ['a']);

  const heuristic = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 300 } });
  heuristic.fake.setProbe('b', noWithdraw);
  const heuristicId = await heuristic.start();
  assert.equal((await heuristic.finish(heuristicId, '25s')).code, 0);
  assert.deepEqual(heuristic.runs(), ['b'], 'the queued-too-long rule still moves to such a runner');
});

test('drain (B1): an incompatible runner ranked first is skipped during selection, so the ticket moves to the compatible runner behind it', T, async () => {
  const noWithdraw = { ...IDLE, capabilities: CAPABILITIES.filter((c) => c !== REMOTE_WITHDRAW_CAPABILITY) };
  const ctx = rebalanceSetup({ names: ['a', 'b', 'c'], paused: ['a', 'c'], rebalance: { remoteRebalanceMinQueuedMs: 60_000 } });
  ctx.fake.setProbe('b', noWithdraw);
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await drainRunner(ctx, 'a');
  await waitFor(() => ctx.fake.count('exec c') === 1, { timeoutMs: 20_000 });
  assert.equal(ctx.fake.count('exec b'), 0, 'B cannot withdraw, so it is never a drain destination');
  ctx.unpause('c');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['c']);
});

test('drain: remoteRebalanceMinQueuedMs 0 still disables every move, drain moves included', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await drainRunner(ctx, 'a');
  await sleep(2500);
  assert.equal(ctx.fake.count('result a'), 0, 'nothing watches the runner');
  assert.equal(ctx.fake.count('withdraw a'), 0);
  ctx.unpause('a');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['a']);
});

test('drain (B3): with no destination and a remote queue timeout, a drained runner behaves exactly as an undrained one', T, async () => {
  const scenario = async (drain) => {
    const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 60_000, remoteQueueTimeoutMs: 800, remoteRebindIntervalMs: 300 } });
    ctx.fake.setProbe('b', { ...IDLE, paused: true });
    const id = await ctx.start();
    await execArrived(ctx, 'a');
    if (drain) await drainRunner(ctx, 'a');
    const waited = await ctx.finish(id);
    return { code: waited.code, onRunners: ctx.runs().filter((r) => r === 'a' || r === 'b'), total: ctx.runs().length, withdrawn: ctx.fake.count('withdraw a'), execB: ctx.fake.count('exec b') };
  };
  const plain = await scenario(false);
  const drained = await scenario(true);
  assert.deepEqual(drained, plain);
  assert.equal(plain.code, 0);
});

test('drain (B5): a lost withdraw reply still records the drain reason once the runner\'s record proves the withdrawal', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a', 'b'], runnerOpts: { a: { withdrawReplyLost: true } }, rebalance: { remoteRebalanceMinQueuedMs: 60_000 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await drainRunner(ctx, 'a', 'lost reply');
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 30_000 });
  const move = readAttempt(ctx.state, id).moves.at(-1);
  assert.equal(move.reason, 'runner-draining');
  assert.equal(move.drainReason, 'lost reply');
  ctx.unpause('b');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.equal(resultOf(ctx.state, id).rebalanceReason, 'a draining (lost reply)');
});

test('drain (B7): a submitter killed during a drain move is MOVE-INTERRUPTED, exactly as for any other move', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 60_000, remoteRebalanceIntervalMs: 60_000 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  process.kill(readAttempt(ctx.state, id).supervisor.pid, 'SIGKILL');
  await sleep(300);
  await withLock(ctx.state, () => patchAttemptLocked(ctx.state, id, {
    runner: 'b',
    moves: [{ from: 'a', to: 'b', at: Date.now(), queuedMs: 1, reason: 'runner-draining', drainReason: 'maint' }],
    moving: { from: 'a', to: 'b', phase: 'dispatching', at: Date.now() },
  }));
  const waited = await laneRun(['wait', id, '--timeout', '5s'], { env: ctx.env });
  assert.equal(waited.code, 74, waited.stderr);
  assert.match(waited.stderr, new RegExp(`MOVE-INTERRUPTED ${id} withdrawn from a`));
  assert.doesNotMatch(waited.stderr, /ORPHANED-REMOTE/);

  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
});
