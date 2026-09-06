import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeRepoConfig, writeLoadFile, laneRun } from './helpers.js';

/**
 * A supervisor launching while config.json is missing, mid-write, or invalid
 * must start on defaults rather than crash before it ever reaches the
 * resilient polling loop -- the exact torn-read window reloadGlobalConfig()
 * exists to survive. Before the startup fix, `main()` called the throwing
 * `loadGlobalConfig()` directly, so this run would fail with "supervisor
 * exited unexpectedly with no result" instead of completing.
 */
test('a malformed global config at supervisor startup does not crash the lane; it runs on defaults', async () => {
  const { base, home, env: baseEnv } = freshEnv();
  fs.writeFileSync(path.join(home, 'config.json'), '{ this is not valid json');
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  // Defaults close the gate above loadClose: 15 -- pin the injected load well
  // under that so this test's outcome depends only on startup surviving the
  // bad config, not on the real host's load average.
  const loadFile = writeLoadFile(base, 1);
  const env = { ...baseEnv, LANE_BROKER_LOADAVG_FILE: loadFile };

  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 0, `expected the lane to run on default config; stderr: ${result.stderr}`);
});

test('a global config that fails validation at supervisor startup runs on defaults', async () => {
  const { base, home, env: baseEnv } = freshEnv();
  // loadOpen >= loadClose is rejected by validateGlobalConfig.
  fs.writeFileSync(
    path.join(home, 'config.json'),
    JSON.stringify({ version: 1, capacity: 2, loadClose: 15, loadOpen: 20, loadOpenSamples: 3, sampleMs: 5000 }),
  );
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const loadFile = writeLoadFile(base, 1);
  const env = { ...baseEnv, LANE_BROKER_LOADAVG_FILE: loadFile };

  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 0, `expected the lane to run on default config; stderr: ${result.stderr}`);
});
