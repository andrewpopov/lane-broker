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
