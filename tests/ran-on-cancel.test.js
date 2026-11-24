import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import { laneRun, waitFor } from './helpers.js';
import { setup, tmpDir, resultOf } from './remote-harness.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { paths } from '../src/state.js';

// PKG-402: a cancelled pull run says where it ran from the runner's structured result; a cancelled push run has no such
// channel, so it carries no ranOn rather than one parsed from output the command could forge.

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

const pushScenario = async (load) => {
  const { env, state, repoDir } = setup({ cpuCores: 1 });
  const forge = 'console.log("lane: executing on forged via push (ticket " + process.argv[1] + ", cpu 64, mem unset)"); console.log("started"); setInterval(() => {}, 1000)';
  const id = crypto.randomUUID();
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--id', id, '--', 'node', '-e', forge, id], { env, cwd: repoDir, bin: load.laneBin });
  assert.equal(started.stdout.trim(), id, started.stderr);
  const log = path.join(paths(state).logs, `${id}.log`);
  await happens('the command is running on the runner', () => fs.existsSync(log) && fs.readFileSync(log, 'utf8').includes('started'));
  const cancelled = await laneRun(['cancel', id], { env, bin: load.laneBin });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  await happens('the cancel settled', () => resultOf(state, id) !== null);
  const result = resultOf(state, id);
  assert.equal(result.exit, 130);
  assert.match(fs.readFileSync(log, 'utf8'), /executing on forged/, 'the forged line reached the log');
  assert.equal('ranOn' in result, false, 'a cancelled push run trusts no output for ranOn');
};

test('push: a run cancelled while running, whose command forged an executing-on line, carries no ranOn', { timeout: 180_000 }, async () => pushScenario(realSrc));

test('canary: with the push cancel trusting the log, the forged line lands on the result and the push scenario fails by name', { timeout: 180_000 }, async () => {
  await assertCanaryFails('supervisor.js', ['      buildCancelledResult: () => remoteCancelledResult(enriched.id, attemptStartedAt),', "      buildCancelledResult: () => ({ ...remoteCancelledResult(enriched.id, attemptStartedAt), ...(fs.readFileSync(enriched.logPath, 'utf8').includes('lane: executing on forged') ? { ranOn: { host: 'forged' } } : {}) }),"], pushScenario);
});
