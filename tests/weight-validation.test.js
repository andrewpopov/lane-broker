import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { paths } from '../src/state.js';

test('--weight abc is rejected before enqueueing, with exit 2', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 } } });

  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--weight', 'abc', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 2, `stderr: ${result.stderr}`);
  assert.match(result.stderr, /--weight must be a positive number/);
  const readdirOrEmpty = (dir) => {
    try {
      return fs.readdirSync(dir);
    } catch {
      return [];
    }
  };
  assert.equal(readdirOrEmpty(paths(state).queue).length, 0, 'nothing should have been enqueued');
  assert.equal(readdirOrEmpty(paths(state).leases).length, 0, 'nothing should have started');
});

test('--weight -1 and --weight 0 are also rejected', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 } } });

  for (const bad of ['-1', '0']) {
    const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--weight', bad, '--', 'true'], { env, cwd: repoDir });
    assert.equal(result.code, 2, `--weight ${bad} should be rejected; stderr: ${result.stderr}`);
  }
});

test('--cpu and --memory reject malformed values before enqueueing', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  for (const args of [
    ['--cpu', 'zero'],
    ['--cpu', '0'],
    ['--memory', 'lots'],
    ['--memory', '0'],
  ]) {
    const result = await laneRun(['run', ...args, '--', 'true'], { env, cwd: repoDir });
    assert.equal(result.code, 2, `${args.join(' ')} should be rejected; stderr: ${result.stderr}`);
  }
});

test('active admission rejects a permanently impossible request before enqueueing', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 'auto',
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    schedulerMode: 'active',
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const result = await laneRun(['run', '--memory', '999TiB', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 64, `stderr: ${result.stderr}`);
  assert.match(result.stderr, /requested resources exceed this environment's budget/);
  assert.equal(fs.existsSync(paths(state).queue), false, 'state is not created for an impossible request');
});
