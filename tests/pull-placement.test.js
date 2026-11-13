import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { runnerWatchOnce } from '../src/runner-watch.js';
import { collectRunnerStates, downRunnerReasons, heartbeatUpRunners, readHeartbeat, recordPings } from '../src/runner-unreachable.js';
import { selectRunner, fetchRemoteResult, remoteCancel, remoteWithdraw } from '../src/remote-client.js';
import { sshTransport } from '../src/runner-transport.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths } from '../src/state.js';
import { holdLaneLocally, IDLE, rebalanceSetup, resultOf, T, tmpDir } from './remote-harness.js';
import { waitFor } from './helpers.js';

/**
 * BRAIN-1036 R2 (T15): a `transport: "pull"` runner is never dialled. Every placement, rebind, rebalance, ping and status path reads
 * the push runners only. The pull entries below carry an `ssh` destination on purpose: the guard is the transport, not a missing ssh.
 */

const NOW = Date.parse('2026-10-15T12:00:00Z');
const PUSH = { name: 'skybox', ssh: 'skybox-runner' };
const PULL = { name: 'pullbox', ssh: 'pullbox-runner', transport: 'pull' };
const CFG = { ...DEFAULT_GLOBAL_CONFIG, runnerDownAfterMs: 60_000, runnerLostAfterMs: 600_000, runners: [PULL, PUSH] };

/** A fake ssh that records every argv and answers a probe like an idle runner and anything else with success. */
function loggingSsh() {
  const dir = tmpDir('pull-placement-ssh');
  const log = path.join(dir, 'calls.log');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const argv = process.argv.slice(2);
require('fs').appendFileSync(${JSON.stringify(log)}, argv.join(' ') + '\\n');
if (argv.join(' ').includes('remote-probe')) process.stdout.write(JSON.stringify(${JSON.stringify(IDLE)}) + '\\n');
`,
  );
  fs.chmodSync(bin, 0o755);
  const calls = () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter(Boolean) : []);
  return { bin, calls, callsTo: (dest) => calls().filter((c) => c.split(' ').includes(dest)).length };
}

test('T15 selectRunner never probes or chooses a pull runner, even one listed first with an ssh destination', async () => {
  const ssh = loggingSsh();
  const res = await selectRunner([PULL, PUSH], { sshBin: ssh.bin, deadlineMs: 3000 });
  assert.equal(res.runner.name, 'skybox');
  assert.equal(ssh.callsTo('pullbox-runner'), 0);
  assert.deepEqual(res.skipped, [{ name: 'pullbox', reason: 'pull transport' }]);
});

test('T15 the heartbeat pings push runners only, and records nothing for a pull runner', async () => {
  const root = tmpDir('pull-placement-watch');
  const ssh = loggingSsh();
  const res = await runnerWatchOnce(root, { cfg: CFG, sshBin: ssh.bin, now: NOW });
  assert.deepEqual(res.pings.map((p) => p.name), ['skybox']);
  assert.equal(ssh.callsTo('pullbox-runner'), 0);
  assert.deepEqual(Object.keys(readHeartbeat(root).runners), ['skybox']);
});

test('T15 runner states, down reasons and the up-list never include a pull runner', async () => {
  const root = tmpDir('pull-placement-states');
  await recordPings(root, [{ name: 'skybox', ok: true }, { name: 'pullbox', ok: false }], NOW - 1000);
  assert.deepEqual(collectRunnerStates(root, CFG, NOW).map((r) => r.name), ['skybox']);
  assert.deepEqual(downRunnerReasons(root, CFG, NOW + 3_600_000), {}, 'the failed pullbox ping started a clock, but a pull runner is never reported down');
  assert.deepEqual(heartbeatUpRunners(root, CFG, NOW), ['skybox']);
  assert.deepEqual(collectRunnerStates(root, { ...CFG, runners: [PULL] }, NOW), []);
});

test('T15 a ticket is dispatched to the push runner, never the pull runner listed before it', T, async () => {
  const ctx = rebalanceSetup({ names: ['pull-box', 'a'], paused: [], configOpts: { 'pull-box': { transport: 'pull' } } });
  const id = await ctx.start();
  const waited = await ctx.finish(id, '25s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${ctx.fake.calls().join(',')}`);
  assert.deepEqual(ctx.runs(), ['a']);
  assert.deepEqual(ctx.fake.calls().filter((c) => c.endsWith(' pull-box')), []);
  assert.equal(resultOf(ctx.state, id).runner, 'a');
});

test('T15 a rebalance never moves a queued ticket to the pull runner', T, async () => {
  const ctx = rebalanceSetup({ names: ['pull-box', 'a', 'b'], paused: ['a'], configOpts: { 'pull-box': { transport: 'pull' } } });
  const id = await ctx.start();
  const waited = await ctx.finish(id, '25s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${ctx.fake.calls().join(',')}`);
  assert.deepEqual(ctx.runs(), ['b']);
  assert.equal(resultOf(ctx.state, id).rebalancedFrom, 'a');
  assert.deepEqual(ctx.fake.calls().filter((c) => c.endsWith(' pull-box')), []);
});

test('T15 a locally queued ticket is rebound to the push runner, never the pull runner', T, async () => {
  const ctx = rebalanceSetup({ names: ['pull-box', 'a'], paused: [], rebalance: { remoteRebindIntervalMs: 300 }, configOpts: { 'pull-box': { transport: 'pull' } } });
  ctx.fake.setDown('a', true);
  const hold = await holdLaneLocally(ctx);
  const id = await ctx.start();
  await waitFor(() => fs.existsSync(paths(ctx.state).queue) && fs.readdirSync(paths(ctx.state).queue).length > 0, { timeoutMs: 30_000 });
  ctx.fake.setDown('a', false);
  const waited = await ctx.finish(id, '45s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${ctx.fake.calls().join(',')}`);
  assert.deepEqual(ctx.runs(), ['a']);
  assert.deepEqual(ctx.fake.calls().filter((c) => c.endsWith(' pull-box')), []);
  hold.release();
  await hold.done;
});

test('the ssh transport wraps fetchRemoteResult, remoteCancel and remoteWithdraw unchanged: same argv, same answers', async () => {
  const viaSeam = loggingSsh();
  const direct = loggingSsh();
  const runner = { name: 'skybox', ssh: 'skybox-runner', shell: 'sh -c', root: '/r' };
  const seam = sshTransport(runner, { sshBin: viaSeam.bin, fetchDeadlineMs: 3000 });
  const answers = [await seam.fetchResult('t-1'), await seam.cancel('t-1'), await seam.withdraw('t-1')];
  const wrapped = [await fetchRemoteResult(runner, 't-1', direct.bin, 3000, undefined, undefined), await remoteCancel(runner, 't-1', { sshBin: direct.bin }), await remoteWithdraw(runner, 't-1', { sshBin: direct.bin })];
  assert.deepEqual(answers, wrapped);
  assert.deepEqual(viaSeam.calls(), direct.calls());
  assert.deepEqual(viaSeam.calls().map((c) => /remote-(\w+)/.exec(c)[1]), ['result', 'cancel', 'withdraw']);
});
