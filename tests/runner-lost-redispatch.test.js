import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, waitFor } from './helpers.js';
import { resultOf, rebalanceSetup, execArrived, T } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { provesStopped } from '../src/remote-client.js';

/**
 * BRAIN-350 S2, the SUBMITTER side: a runner whose host shuts down ends the job with a `runner-shutdown` record (real here: `lane
 * drain --shutdown` on that runner's own state root). That record proves the job is dead there, so the ticket is dispatched to another
 * runner it has not been on, or falls back to local when none is left. Any other unusable record still leaves it unresolved (BRAIN-437).
 */

const NO_REBALANCE = { remoteRebalanceMinQueuedMs: 0 };

/** `lane drain --shutdown` on runner `name`: the real runner-side stop, which publishes the runner-shutdown record. */
async function shutdownRunner(ctx, name) {
  const r = ctx.runners[name];
  const root = ctx.globalRunners.find((g) => g.name === name).root;
  const res = await laneRun(['drain', '--shutdown', '--root', root, '--wait-ms', '20000'], { env: { ...ctx.env, LANE_BROKER_HOME: r.home, LANE_BROKER_STATE: r.state } });
  assert.equal(res.code, 0, res.stderr);
}

const historyRow = (ctx, id) => fs.readFileSync(path.join(ctx.state, 'history.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l)).find((r) => r.id === id);

test('provesStopped: only a binding-matched unfinished/runner-shutdown record', () => {
  const expected = { protocol: 1, ticketId: 't', generation: 1, manifestHash: 'h' };
  const bound = { protocol: 1, ticketId: 't', generation: 1, manifestHash: 'h' };
  assert.equal(provesStopped({ ...bound, kind: 'unfinished', reason: 'runner-shutdown' }, expected), true);
  assert.equal(provesStopped({ ...bound, kind: 'unfinished' }, expected), false);
  assert.equal(provesStopped({ ...bound, kind: 'unfinished', reason: 'queue-timeout' }, expected), false);
  assert.equal(provesStopped({ ...bound, kind: 'completed', reason: 'runner-shutdown' }, expected), false);
  assert.equal(provesStopped({ ...bound, manifestHash: 'other', kind: 'unfinished', reason: 'runner-shutdown' }, expected), false);
});

test('a runner that shuts down under the job: it is dispatched to B, runs once on B, and the row says it was re-dispatched', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a'], rebalance: NO_REBALANCE });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await shutdownRunner(ctx, 'a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b'], 'exactly once, on B, not local');
  assert.equal(ctx.fake.count('exec b'), 1);
  const result = resultOf(ctx.state, id);
  assert.equal(result.executor, 'remote');
  assert.equal(result.runner, 'b');
  assert.deepEqual(result.redispatchedFrom, ['a']);
  assert.equal(result.redispatchReason, 'runner-shutdown');
  assert.equal(result.rebalanceReason, 'a stopped the job (runner-shutdown)');
  const row = historyRow(ctx, id);
  assert.deepEqual(row.redispatchedFrom, ['a']);
  assert.equal(row.redispatchReason, 'runner-shutdown');
  assert.equal(row.localReason, undefined);
});

test('the move is recorded on the attempt while it runs on B', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a', 'b'], rebalance: NO_REBALANCE });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await shutdownRunner(ctx, 'a');
  await waitFor(() => readAttempt(ctx.state, id)?.moves?.length === 1, { timeoutMs: 30_000 });
  const attempt = readAttempt(ctx.state, id);
  assert.equal(attempt.moves[0].reason, 'runner-shutdown');
  assert.deepEqual([attempt.moves[0].from, attempt.moves[0].to], ['a', 'b']);
  assert.equal(attempt.runner, 'b');
  assert.equal(attempt.moving, undefined, 'the first runner holds nothing: no move in flight');
  assert.ok(attempt.dispatchedRunners.includes('a') && attempt.dispatchedRunners.includes('b'));
  ctx.unpause('b');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['b']);
});

test('no other runner eligible: the ticket falls back to local with localReason fallback:runner-shutdown', T, async () => {
  const ctx = rebalanceSetup({ names: ['a'], paused: ['a'], rebalance: NO_REBALANCE });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await shutdownRunner(ctx, 'a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(ctx.fake.count('exec a'), 1, 'never sent to A a second time');
  assert.deepEqual(ctx.runs(), ['undefined'], 'ran once, locally (no runner id in its env)');
  assert.equal(historyRow(ctx, id).localReason, 'fallback:runner-shutdown');
});

test('never back to a runner it already ran on: A stops, B stops too, then it runs locally', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a', 'b'], rebalance: NO_REBALANCE });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await shutdownRunner(ctx, 'a');
  await execArrived(ctx, 'b');
  await shutdownRunner(ctx, 'b');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(ctx.fake.count('exec a'), 1, 'A is not revisited');
  assert.equal(ctx.fake.count('exec b'), 1);
  assert.deepEqual(ctx.runs(), ['undefined'], 'exactly once, locally');
  assert.equal(historyRow(ctx, id).localReason, 'fallback:runner-shutdown');
});

for (const [name, patch] of [
  ['an unfinished record with no reason', { reason: null }],
  ['a runner-shutdown record that does not bind to the dispatched manifest', { manifestHash: 'f'.repeat(64) }],
]) {
  test(`BRAIN-437 guard: ${name} leaves the attempt unresolved and re-runs nothing`, T, async () => {
    const ctx = rebalanceSetup({ paused: ['a'], rebalance: NO_REBALANCE, runnerOpts: { a: { resultPatch: patch } } });
    const id = await ctx.start();
    await execArrived(ctx, 'a');
    await shutdownRunner(ctx, 'a');
    await waitFor(() => readAttempt(ctx.state, id)?.unresolved, { timeoutMs: 30_000 });
    assert.equal(ctx.fake.count('exec b'), 0, 'never dispatched elsewhere');
    assert.deepEqual(ctx.runs(), [], 'never run');
    assert.equal(readAttempt(ctx.state, id).moves, undefined);
  });
}
