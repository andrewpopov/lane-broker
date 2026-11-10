import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { writeRemoteDrain, paths } from '../src/state.js';
import { rebalanceSetup, execArrived, T } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { resolveTicketConfig } from '../src/config.js';
import { historyRow } from '../src/supervisor.js';
import { placementOf } from '../src/speed-placement.js';

/**
 * BRAIN-899 A2: the winning score of EVERY dispatch is recorded as `placement` -- on the attempt, so a reconciliation that
 * settles the ticket can see it, and on the history row the ticket publishes, so `lane report placement` can join the prediction
 * to the wait and run that same dispatch actually saw. A dispatch nothing ranked records no prediction at all.
 */

const SCORE = { name: 'skybox', finishMs: 280_000, waitMs: 0, runMs: 220_000, tier: 'repo-lane', n: 3, idle: true, queued: 0, slots: 1 };
const PLACEMENT = { runner: 'skybox', rawWaitMs: 0, factor: 1, predictedWaitMs: 0, predictedRunMs: 220_000, tier: 'repo-lane', n: 3, slots: 1, queued: 0 };

// ---- the pure seams ----

test('placementOf: the winning score becomes the prediction, and an unranked dispatch has none', () => {
  assert.deepEqual(placementOf(SCORE), PLACEMENT);
  assert.deepEqual(placementOf({ ...SCORE, waitMs: 150_000, queued: 2, slots: 3, finishMs: 430_000 }), { ...PLACEMENT, rawWaitMs: 150_000, predictedWaitMs: 150_000, slots: 3, queued: 2 });
  for (const score of [null, undefined, {}, { name: 'skybox' }, { ...SCORE, finishMs: null }]) {
    assert.equal(placementOf(score), null, JSON.stringify(score));
  }
});

test('historyRow: the placement the publisher passes rides into the row, and its absence leaves no key', () => {
  const ticket = { id: 't-1', key: 'k', repoId: 'r', lane: 'pr', weight: 1, resources: { cpuCores: 2 }, command: 'sh x.sh' };
  const result = { id: 't-1', exit: 0, executor: 'remote', runner: 'skybox', startedAt: 1, endedAt: 2 };
  assert.deepEqual(historyRow(ticket, { ...result, placement: PLACEMENT }, {}).placement, PLACEMENT);
  assert.equal('placement' in historyRow(ticket, result, {}), false);
  assert.equal('placement' in historyRow(ticket, { ...result, placement: undefined }, {}), false, 'a cleared prediction writes no key');
});

// ---- end to end, through the real supervisor and the two-runner fake ssh ----

/** Enough successful remote runs of this repo+lane for the repo-lane tier to estimate a runner (donor-scaled where it has none). */
function seedEstimates(ctx, byRunner) {
  const { repoId } = resolveTicketConfig({ cwd: ctx.repoDir, repo: 'r', lane: 'default' });
  const rows = Object.entries(byRunner).flatMap(([runner, durations]) =>
    durations.map((runMs, i) => ({ id: `seed-${runner}-${i}`, repo: repoId, lane: 'default', command: 'sh seeded.sh', executor: 'remote', runner, exit: 0, startedAt: 1000 + i, endedAt: 1000 + runMs, runMs })));
  fs.appendFileSync(paths(ctx.state).history, `${rows.map((r) => JSON.stringify(r)).join('\n')}\n`);
}

const lastRow = (ctx) => JSON.parse(fs.readFileSync(paths(ctx.state).history, 'utf8').trim().split('\n').at(-1));
const admissionLog = (ctx) => fs.readFileSync(paths(ctx.state).admissionLog, 'utf8');

test('place: a scored first placement persists its prediction on the attempt and publishes it on the history row', T, async () => {
  const ctx = rebalanceSetup({ names: ['b'], paused: [] });
  seedEstimates(ctx, { b: [200_000, 220_000, 240_000] });
  const id = await ctx.start();
  const waited = await ctx.finish(id, '60s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${admissionLog(ctx)}`);
  assert.deepEqual(ctx.runs(), ['b']);
  const row = lastRow(ctx);
  assert.equal(row.executor, 'remote');
  assert.deepEqual(row.placement, { runner: 'b', rawWaitMs: 0, factor: 1, predictedWaitMs: 0, predictedRunMs: 220_000, tier: 'repo-lane', n: 3, slots: 1, queued: 0 });
  assert.match(admissionLog(ctx), new RegExp(`finish-placement: ts=\\S+ ${id}: b 280s \\(wait 0s, run 220s, repo-lane n=3, q=0 slots=1\\) -> b`));
});

test('place: nothing ranked records no prediction, and the decision is the one the unscored ranking always made', T, async () => {
  const ctx = rebalanceSetup({ names: ['b'], paused: [] });
  const id = await ctx.start();
  const waited = await ctx.finish(id, '60s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${admissionLog(ctx)}`);
  assert.deepEqual(ctx.runs(), ['b']);
  const row = lastRow(ctx);
  assert.equal(row.runner, 'b');
  assert.equal('placement' in row, false, 'an unscored dispatch has no prediction to record');
  assert.match(admissionLog(ctx), new RegExp(`finish-placement: ts=\\S+ ${id}: unscored -> b`));
});

test('rebalance: a move replaces the prediction with the destination\'s, on the attempt and on the published row', T, async () => {
  const ctx = rebalanceSetup({ paused: ['a', 'b'], rebalance: { remoteRebalanceMinQueuedMs: 60_000 } });
  seedEstimates(ctx, { a: [100_000, 110_000, 120_000], b: [500_000, 520_000, 540_000] });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  // the faster estimate won the first placement, so the ticket is queued on the paused a with a's prediction on the attempt
  await waitForPlacement(ctx, id, 'a');
  assert.equal(readAttempt(ctx.state, id).placement.predictedRunMs, 110_000, 'the attempt carries it, which is what a reconciliation reads');
  assert.match(admissionLog(ctx), new RegExp(`finish-placement: ts=\\S+ ${id}: a 170s \\(wait 0s, run 110s, repo-lane n=3, q=0 slots=1\\) \\| b 580s \\(wait 0s, run 520s, repo-lane n=3, q=0 slots=1\\) -> a`));
  // a drain is ungated by the gain rule, so it is the move that proves the destination's score replaces the source's
  await writeRemoteDrain(ctx.runners.a.state, 'disk swap');
  await waitForPlacement(ctx, id, 'b');
  const moved = readAttempt(ctx.state, id);
  assert.deepEqual(moved.placement, { runner: 'b', rawWaitMs: 0, factor: 1, predictedWaitMs: 0, predictedRunMs: 520_000, tier: 'repo-lane', n: 3, slots: 1, queued: 0 });
  assert.match(admissionLog(ctx), new RegExp(`finish-placement: ts=\\S+ ${id}: rebalance: b 580s \\(wait 0s, run 520s, repo-lane n=3, q=0 slots=1\\) -> b`));
  ctx.unpause('b');
  const waited = await ctx.finish(id, '60s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${admissionLog(ctx)}`);
  assert.deepEqual(ctx.runs(), ['b']);
  const row = lastRow(ctx);
  assert.equal(row.rebalancedFrom, 'a');
  assert.equal(row.runner, 'b');
  assert.equal(row.placement.runner, 'b', 'the FINAL dispatch\'s prediction, not the first one\'s');
  assert.equal(row.placement.predictedRunMs, 520_000);
});

/** Polls the attempt record until its placement names `runner`; fails by name rather than sleeping a fixed window. */
async function waitForPlacement(ctx, id, runner) {
  const deadline = Date.now() + 60_000;
  for (;;) {
    const attempt = readAttempt(ctx.state, id);
    if (attempt?.placement?.runner === runner) return;
    if (Date.now() > deadline) assert.equal(attempt?.placement?.runner, runner, `the attempt's placement never became ${runner}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}
