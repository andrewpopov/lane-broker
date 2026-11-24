import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { laneRun, waitFor } from './helpers.js';
import { setup, tmpDir, resultOf } from './remote-harness.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { paths } from '../src/state.js';

// PKG-402: a run cancelled after it started still says where it ran, on push and on pull.

const MiB = 1024 * 1024;
const RUNNER_RAN_ON = { host: 'wintop', transport: 'pull', ticket: 't-1', cpuCores: 2, memoryBytes: 64 * MiB, runner: 'wintop' };
const happens = (what, check, timeoutMs = 60_000) => waitFor(check, { timeoutMs }).then(() => true, () => false).then((ok) => assert.ok(ok, what));

const pullScenario = async (load) => {
  const { pullCancelledResult } = await load('pull-ticket.js');
  const cancelled = pullCancelledResult('t-1', 0, { kind: 'cancelled', cancelConfirmed: true, ranOn: RUNNER_RAN_ON });
  assert.deepEqual([cancelled.exit, cancelled.transport, cancelled.ranOn], [130, 'pull', RUNNER_RAN_ON], 'a confirmed pull cancel relays where the runner ran it');
  assert.equal('ranOn' in pullCancelledResult('t-1', 0, null), false, 'a cancel before the runner said anything has none');
};

test('pull: a cancelled result carries the runner ranOn', async () => pullScenario(realSrc));

test('canary: without the relay in the pull cancel builder, the pull scenario fails by name', async () => {
  await assertCanaryFails('pull-ticket.js', ["transport: 'pull', ...relayedRanOn(runnerResult) });", "transport: 'pull' });"], pullScenario);
});

test('ranOnFromLog reads the first executing-on line for this ticket only', async () => {
  const { ranOnFromLog } = await realSrc('ran-on.js');
  const log = path.join(tmpDir('ran-on-log'), 'out.log');
  fs.writeFileSync(log, 'lane: executing on wintop via push (ticket t-1, cpu 2, mem 64MB, runner wintop)\nlane: executing on evil via local (ticket t-1, cpu 9, mem unset)\n');
  assert.deepEqual(ranOnFromLog(log, 't-1').ranOn, { ...RUNNER_RAN_ON, transport: 'push' });
  assert.deepEqual(ranOnFromLog(log, 't-2'), {}, 'another ticket\'s line is not ours');
  assert.deepEqual(ranOnFromLog(path.join(path.dirname(log), 'missing.log'), 't-1'), {});
});

const pushScenario = async (load) => {
  const { env, state, repoDir } = setup({ cpuCores: 1 });
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'node', '-e', 'console.log("started"); setInterval(() => {}, 1000)'], { env, cwd: repoDir, bin: load.laneBin });
  const id = started.stdout.trim();
  const log = path.join(paths(state).logs, `${id}.log`);
  await happens('the command is running on the runner', () => fs.existsSync(log) && fs.readFileSync(log, 'utf8').includes('started'));
  const cancelled = await laneRun(['cancel', id], { env, bin: load.laneBin });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  await happens('the cancel settled', () => resultOf(state, id) !== null);
  const result = resultOf(state, id);
  assert.equal(result.exit, 130);
  assert.deepEqual(result.ranOn, { host: os.hostname(), transport: 'push', ticket: id, cpuCores: 1, memoryBytes: 1024 * MiB, runner: 'skybox' }, 'a cancelled push run still says where it ran');
};

test('push: a run cancelled while running keeps ranOn on its result', { timeout: 180_000 }, async () => pushScenario(realSrc));

test('canary: without the push cancel reading ranOn, the push scenario fails by name', { timeout: 180_000 }, async () => {
  await assertCanaryFails('supervisor.js', ['...remoteCancelledResult(enriched.id, attemptStartedAt), ...ranOnFromLog(enriched.logPath, enriched.id) })', '...remoteCancelledResult(enriched.id, attemptStartedAt) })'], pushScenario);
});
