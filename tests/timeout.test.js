import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';

test('lane run --timeout exits 75 ("waited, not failed") when the lane has not finished in time', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  // Occupy the only capacity slot on the same key for long enough that the
  // second run is still queued (blocked by the conflict) when its timeout fires.
  const blockerPromise = laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '2'], { env, cwd: repoDir });
  await new Promise((resolve) => setTimeout(resolve, 300));

  const timedOut = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--timeout', '300ms', '--', 'true'], {
    env,
    cwd: repoDir,
  });
  assert.equal(timedOut.code, 75, `expected exit 75; got ${timedOut.code}, stderr: ${timedOut.stderr}`);
  assert.match(timedOut.stderr, /waited/);

  await blockerPromise;
});
