import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, BIN } from './helpers.js';

test('a nested "lane run --lane prepush" under an inherited lease of the same repo reuses it (no refusal)', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 }, prepush: { weight: 1 } } });

  const nested = [process.execPath, BIN, 'run', '--repo', 'r', '--lane', 'prepush', '--', 'echo', 'PREPUSH-RAN'];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...nested], { env, cwd: repoDir });
  assert.equal(result.code, 0, `outer run should succeed and the nested prepush should reuse the inherited lease; stderr: ${result.stderr}`);
});

test('a nested run under an inherited lease of a DIFFERENT repo is still refused with exit 64', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 }, prepush: { weight: 1 } } });
  const otherRepoDir = path.join(base, 'other-repo');
  writeRepoConfig(otherRepoDir, { version: 1, lanes: { default: { weight: 1 }, prepush: { weight: 1 } } });

  const nested = [process.execPath, BIN, 'run', '--repo', 'other', '--lane', 'prepush', '--', 'true'];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...nested], { env, cwd: repoDir });
  assert.equal(result.code, 64, `a prepush run under a different repo's lease must still be refused; stderr: ${result.stderr}`);
});
