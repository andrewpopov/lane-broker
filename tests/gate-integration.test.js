import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeLoadFile, laneRun, laneSpawn, sleep, waitFor } from './helpers.js';
import { paths } from '../src/state.js';

test('load gate: refuses to start while closed, starts once it reopens after enough consecutive low samples', async () => {
  const { base, home, state, env: baseEnv } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 2, sampleMs: 150 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const loadFile = writeLoadFile(base, 20); // closed
  const env = { ...baseEnv, LANE_BROKER_LOADAVG_FILE: loadFile };

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'sleep', '1'], { env, cwd: repoDir });
  const id = await new Promise((resolve) => {
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.on('exit', () => resolve(out.trim()));
  });
  assert.ok(id, 'lane run --detach should print an id');

  // Give it a couple of poll cycles: it must still be queued, not running, while load stays high.
  await sleep(400);
  assert.equal(fs.existsSync(path.join(paths(state).leases, `${id}.json`)), false, 'must not start while the load gate is closed');

  // Drop the load; the gate needs loadOpenSamples consecutive low samples before it reopens.
  fs.writeFileSync(loadFile, '2');
  const lease = await waitFor(() => fs.existsSync(path.join(paths(state).leases, `${id}.json`)), { timeoutMs: 5000 });
  assert.ok(lease, 'should eventually start once the gate reopens');
});
