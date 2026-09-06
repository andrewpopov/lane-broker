import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, BIN } from './helpers.js';

test('a nested reentrant run may not widen the inherited weight', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const nested = [process.execPath, BIN, 'run', '--repo', 'r', '--lane', 'default', '--weight', '2', '--', 'true'];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...nested], { env, cwd: repoDir });
  assert.equal(result.code, 64, `widening the reentrant weight (1 -> 2) must be refused; stderr: ${result.stderr}`);
});

test('a nested reentrant run at the same or a lower weight is allowed', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 } } });

  const nested = [process.execPath, BIN, 'run', '--repo', 'r', '--lane', 'default', '--weight', '1', '--', 'true'];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...nested], { env, cwd: repoDir });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
});
