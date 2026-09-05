import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor } from './helpers.js';
import { paths } from '../src/state.js';

test('the caller being SIGKILLed does not affect the lease; `lane wait` reattaches and returns the child exit', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'sleep 0.6; exit 7'], { env, cwd: repoDir });

  const leaseId = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    return names[0] ? names[0].replace(/\.json$/, '') : null;
  });

  child.kill('SIGKILL');
  if (child.exitCode === null && child.signalCode === null) {
    await new Promise((resolve) => child.on('exit', resolve));
  }

  // The lease must survive the caller's death.
  assert.ok(fs.existsSync(path.join(paths(state).leases, `${leaseId}.json`)), 'lease must still exist after the caller is killed');

  const waited = await laneRun(['wait', leaseId], { env, cwd: repoDir });
  assert.equal(waited.code, 7, `lane wait should surface the child's real exit code; stderr: ${waited.stderr}`);
});
