import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { sleep, waitFor, writeRepoConfig } from './helpers.js';
import { resultOf, IDLE, rebalanceSetup, execArrived, T } from './remote-harness.js';

// BRAIN-1072: towerpower `train` landed on wintop (the only gpu-d3d12-iris-xe runner, which `release` needs) while skybox was up.
// Probes are synthetic here, so each runner's answer to `--tags gpu` is written into its probe.

function avoidSetup(rebalance = {}) {
  const ctx = rebalanceSetup({ rebalance });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remotePolicy: 'remote-first', avoidTags: ['gpu'] } } });
  ctx.fake.setProbe('a', { ...IDLE, tags: { gpu: false } });
  ctx.fake.setProbe('b', { ...IDLE, tags: { gpu: true } });
  return ctx;
}

test('avoidTags: a ticket queued on a runner without the tag is never moved to an idle avoided runner', T, async () => {
  const ctx = avoidSetup();
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  await sleep(2500);
  assert.equal(ctx.fake.count('withdraw a'), 0, `queued past remoteRebalanceMinQueuedMs (400ms) yet kept on a: ${ctx.fake.calls().join(',')}`);
  assert.equal(ctx.fake.count('exec b'), 0);
  ctx.unpause('a');
  const waited = await ctx.finish(id);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(ctx.runs(), ['a']);
  const log = fs.readFileSync(path.join(ctx.state, 'admission-decisions.log'), 'utf8');
  assert.doesNotMatch(log, /remote-rebalance: .* a -> b/);
});

test('avoidTags: once the source runner is leaving, the avoided runner is the fallback destination', T, async () => {
  const ctx = avoidSetup({ remoteRebalanceMinQueuedMs: 600_000 });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
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
