import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, BIN } from './helpers.js';

test('a nested reentrant run for the same key does not deadlock (no new lease needed)', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const nested = [process.execPath, BIN, 'run', '--repo', 'r', '--lane', 'default', '--', 'true'];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...nested], { env, cwd: repoDir });
  assert.equal(result.code, 0, `outer run should succeed; stderr: ${result.stderr}`);
});

test('a nested run that would widen the inherited lease (different key) is refused with exit 64', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 }, lint: { weight: 1 } } });

  const nested = [process.execPath, BIN, 'run', '--repo', 'r', '--lane', 'lint', '--', 'true'];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...nested], { env, cwd: repoDir });
  assert.equal(result.code, 64, `outer run should surface the nested refusal's exit code; stderr: ${result.stderr}`);
});
