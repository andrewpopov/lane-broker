import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { writeRepoConfig, laneRun } from './helpers.js';
import { tmpDir, setup, detachAndWait } from './remote-harness.js';
import { scrubDepsEnv } from '../src/deps-cache.js';
import { childEnv } from '../src/supervisor.js';

/**
 * BRAIN-597: the executor contract. A command (and remoteSetup) sees LANE_BROKER_EXECUTOR=remote and LANE_BROKER_RUNNER=<the
 * submitter's runner name> on a runner, LANE_BROKER_EXECUTOR=local anywhere else. The harness names its runner `skybox`.
 */

/** A command that records the two variables to `file`. */
const dumpEnv = (file) => [
  process.execPath,
  '-e',
  `require('fs').writeFileSync(${JSON.stringify(file)}, JSON.stringify({ executor: process.env.LANE_BROKER_EXECUTOR ?? null, runner: process.env.LANE_BROKER_RUNNER ?? null }));`,
];
const read = (file) => JSON.parse(fs.readFileSync(file, 'utf8'));

test('on a runner, remoteSetup and the command both see LANE_BROKER_EXECUTOR=remote and LANE_BROKER_RUNNER=<submitter name>', async () => {
  const { env, repoDir } = setup();
  const dir = tmpDir('executor-env');
  const setupFile = path.join(dir, 'setup.json');
  const commandFile = path.join(dir, 'command.json');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remoteSetup: [dumpEnv(setupFile)] } } });
  const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...dumpEnv(commandFile)], env, repoDir);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(read(setupFile), { executor: 'remote', runner: 'skybox' }, 'remoteSetup');
  assert.deepEqual(read(commandFile), { executor: 'remote', runner: 'skybox' }, 'command');
});

test('a local run sees LANE_BROKER_EXECUTOR=local and no LANE_BROKER_RUNNER, even when the caller\'s shell exported both', async () => {
  const { env, repoDir } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const file = path.join(tmpDir('executor-env-local'), 'command.json');
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...dumpEnv(file)], { env: { ...env, LANE_BROKER_EXECUTOR: 'remote', LANE_BROKER_RUNNER: 'stale' }, cwd: repoDir });
  assert.equal(res.code, 0, res.stderr);
  assert.deepEqual(read(file), { executor: 'local', runner: null });
});

test('childEnv: remote only when the runner\'s own marker accompanies it', () => {
  const ticket = { id: 't', key: 'k' };
  assert.equal(childEnv(ticket, { LANE_BROKER_EXECUTOR: 'remote' }).LANE_BROKER_EXECUTOR, 'local');
  const onRunner = childEnv(ticket, { LANE_BROKER_EXECUTOR: 'remote', LANE_BROKER_RUNNER: 'skybox', LANE_BROKER_LOCAL_REASON: 'remote-exec runner' });
  assert.deepEqual([onRunner.LANE_BROKER_EXECUTOR, onRunner.LANE_BROKER_RUNNER], ['remote', 'skybox']);
});

test('the remoteDeps env keeps the two contract variables and still drops lane\'s own ticket variables', () => {
  const env = scrubDepsEnv({ LANE_BROKER_EXECUTOR: 'remote', LANE_BROKER_RUNNER: 'skybox', LANE_BROKER_LEASE: 'x', LANE_BROKER_KEY: 'y', PATH: '/bin' });
  assert.deepEqual(Object.keys(env).sort(), ['LANE_BROKER_EXECUTOR', 'LANE_BROKER_RUNNER', 'PATH']);
});
