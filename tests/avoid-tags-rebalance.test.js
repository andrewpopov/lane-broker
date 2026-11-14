import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { sleep, waitFor, writeRepoConfig } from './helpers.js';
import { resultOf, IDLE, rebalanceSetup, execArrived, T } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';

// BRAIN-1072. a's real broker is paused to keep the ticket queued; the submitter sees only a's synthetic probe, set per test.

const PREFERRED = { ...IDLE, tags: { gpu: false } };

function avoidSetup({ rebalance = {}, lane = {} } = {}) {
  const ctx = rebalanceSetup({ rebalance });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remotePolicy: 'remote-first', avoidTags: ['gpu'], ...lane } } });
  ctx.fake.setProbe('a', PREFERRED);
  ctx.fake.setProbe('b', { ...IDLE, tags: { gpu: true } });
  return ctx;
}

/** a holds the ticket in its queue and is otherwise up: the submitter's view of a busy preferred runner. */
const holdWithQueue = (ctx) => ctx.fake.setProbe('a', { ...PREFERRED, queued: 1 });

test('avoidTags: a ticket queued on an up runner without the tag is never moved to an idle avoided runner', T, async () => {
  const ctx = avoidSetup();
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  holdWithQueue(ctx);
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0, `queued past remoteRebalanceMinQueuedMs (400ms) yet kept on a: ${ctx.fake.calls().join(',')}`);
  assert.equal(ctx.fake.count('exec b'), 0);
  ctx.unpause('a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['a']);
});

test('avoidTags: a paused source runner is not up, so the ticket moves to the avoided runner after remoteRebalanceMinQueuedMs', T, async () => {
  const ctx = avoidSetup();
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.fake.setProbe('a', { ...PREFERRED, queued: 1, paused: true });
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 20_000 });
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.match(resultOf(ctx.state, id).rebalanceReason, /queued \d+s on a/);
});

test('avoidTags: at the refusal deadline an idle avoided runner takes the ticket instead of a 76', T, async () => {
  const ctx = avoidSetup({ rebalance: { remoteRebalanceMinQueuedMs: 300, remoteRefusedMaxQueuedMs: 4000 }, lane: { localRefused: true } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  holdWithQueue(ctx);
  await sleep(1500);
  assert.equal(ctx.fake.count('exec b'), 0, 'before the deadline a is up, so b is avoided');
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 30_000 });
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.equal(readAttempt(ctx.state, id), null);
});

test('avoidTags: once the source runner is leaving, the avoided runner is the fallback destination', T, async () => {
  const ctx = avoidSetup({ rebalance: { remoteRebalanceMinQueuedMs: 600_000 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  holdWithQueue(ctx);
  const file = path.join(ctx.home, 'config.json');
  const cfg = JSON.parse(fs.readFileSync(file, 'utf8'));
  cfg.runners = cfg.runners.map((r) => (r.name === 'a' ? { ...r, offline: true } : r));
  fs.writeFileSync(file, JSON.stringify(cfg, null, 2));
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 20_000 });
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.equal(resultOf(ctx.state, id).rebalanceReason, 'a unavailable (marked offline)');
});
