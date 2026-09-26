import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { childEnv } from '../src/supervisor.js';

// BRAIN-318: the lane child learns its CPU/memory grant from the environment,
// so a test runner can cap its worker pool to what the broker reserved.

test('childEnv exports the grant alongside the lease identity', () => {
  const env = childEnv({ id: 'lease-1', key: 'k', resources: { cpuCores: 3, memoryBytes: 1024 } }, { PATH: '/bin' });
  assert.equal(env.LANE_BROKER_LEASE, 'lease-1');
  assert.equal(env.LANE_BROKER_KEY, 'k');
  assert.equal(env.LANE_BROKER_CPU_CORES, '3');
  assert.equal(env.LANE_BROKER_MEMORY_BYTES, '1024');
  assert.equal(env.PATH, '/bin');
});

test('childEnv never passes through an inherited grant it did not set', () => {
  const env = childEnv({ id: 'lease-2', key: 'k' }, { LANE_BROKER_CPU_CORES: '12', LANE_BROKER_MEMORY_BYTES: '99' });
  assert.equal(env.LANE_BROKER_CPU_CORES, undefined);
  assert.equal(env.LANE_BROKER_MEMORY_BYTES, undefined);
});

test('a real lane child sees the --cpu grant in LANE_BROKER_CPU_CORES', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const result = await laneRun(
    ['run', '--repo', 'r', '--lane', 'default', '--cpu', '3', '--', 'sh', '-c', 'echo "grant=$LANE_BROKER_CPU_CORES"'],
    { env, cwd: repoDir },
  );
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.match(result.stdout, /grant=3\b/);
});
