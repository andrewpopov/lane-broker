import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { waitFor, sleep } from './helpers.js';
import { resultOf, rebalanceSetup, execArrived, T } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { runnerUnavailableReason } from '../src/runner-schedule.js';

// BRAIN-1058: 2026-11-13, zirkbot PR CI sat queued@mac for 5 h after mac's window closed, with skybox idle.

const ALL_DAYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
const hh = (h) => `${String(((h % 24) + 24) % 24).padStart(2, '0')}:00`;
const closedSchedule = () => {
  const h = new Date().getUTCHours();
  return { timezone: 'UTC', available: [{ days: ALL_DAYS, from: hh(h + 6), to: hh(h + 7) }] };
};

function patchRunner(ctx, name, patch) {
  const file = path.join(ctx.home, 'config.json');
  const cfg = JSON.parse(fs.readFileSync(file, 'utf8'));
  cfg.runners = cfg.runners.map((r) => (r.name === name ? { ...r, ...patch } : r));
  fs.writeFileSync(file, JSON.stringify(cfg, null, 2));
}

test('runnerUnavailableReason: offline and outside-schedule runners are unavailable, an in-window or unscheduled one is not', () => {
  const now = Date.now();
  assert.equal(runnerUnavailableReason({ name: 'a' }, now), null);
  assert.equal(runnerUnavailableReason(undefined, now), null);
  assert.equal(runnerUnavailableReason({ name: 'a', offline: true }, now), 'marked offline');
  assert.equal(runnerUnavailableReason({ name: 'a', schedule: closedSchedule() }, now), 'outside its schedule');
  const h = new Date(now).getUTCHours();
  assert.equal(runnerUnavailableReason({ name: 'a', schedule: { timezone: 'UTC', available: [{ days: ALL_DAYS, from: hh(h), to: hh(h + 2) }] } }, now), null);
});

test('off-schedule: a ticket queued on a runner whose window closes moves to an idle runner long before the minimum-queued wait', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 600_000, remoteRebalanceMaxMoves: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await sleep(600);
  assert.equal(ctx.fake.count('withdraw a'), 0, 'in its window: the heuristic wait still applies');
  const closedAt = Date.now();
  patchRunner(ctx, 'a', { schedule: closedSchedule() });
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 20_000 });
  assert.ok(Date.now() - closedAt < 20_000, 'moved well under the 600s minimum, past a max-moves of 0');
  await waitFor(() => readAttempt(ctx.state, id)?.moves?.length === 1, { timeoutMs: 10_000 });
  const [move] = readAttempt(ctx.state, id).moves;
  assert.equal(move.reason, 'runner-unavailable');
  assert.equal(move.drainReason, 'outside its schedule');
  assert.deepEqual([move.from, move.to], ['a', 'b']);
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b'], 'exactly once, on B');
  assert.equal(resultOf(ctx.state, id).rebalanceReason, 'a unavailable (outside its schedule)');
});

test('off-schedule: a runner marked offline in config gives up its queued ticket the same way', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 600_000 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  patchRunner(ctx, 'a', { offline: true });
  await waitFor(() => ctx.fake.count('exec b') === 1, { timeoutMs: 20_000 });
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.equal(resultOf(ctx.state, id).rebalanceReason, 'a unavailable (marked offline)');
});

test('off-schedule: with no available destination the ticket stays queued and is never refused (refusal cap 0)', T, async () => {
  const ctx = rebalanceSetup({ rebalance: { remoteRebalanceMinQueuedMs: 600_000, remoteRefusedMaxQueuedMs: 0 } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  patchRunner(ctx, 'b', { schedule: closedSchedule() });
  patchRunner(ctx, 'a', { schedule: closedSchedule() });
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0, 'nowhere to go: not withdrawn');
  assert.equal(ctx.fake.count('exec b'), 0);
  ctx.unpause('a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['a']);
});
