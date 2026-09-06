import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, waitFor, sleep } from './helpers.js';
import { paths } from '../src/state.js';

test('strict FIFO: a heavy ticket at the head blocks a lighter, non-conflicting ticket behind it (no backfill)', async () => {
  const { base, home, state, env } = freshEnv();
  // Capacity 2: the heavy (weight 2) head ticket uses the whole thing, so a
  // weight-1 ticket behind it *could* fit capacity-wise, but strict FIFO
  // forbids starting anything out of order.
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { heavy: { weight: 2 }, light: { weight: 1 } } });

  const startFile = path.join(base, 'starts');
  const heavy = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'heavy', '--', 'sh', '-c', `echo heavy >> "${startFile}"; sleep 1`],
    { env, cwd: repoDir },
  );
  await waitFor(
    () => {
      try {
        return fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json')).length > 0;
      } catch {
        return false;
      }
    },
    { timeoutMs: 30000 },
  );

  const light = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'light', '--', 'sh', '-c', `echo light >> "${startFile}"; sleep 0.1`],
    { env, cwd: repoDir },
  );

  // While the heavy ticket is still running, the light ticket (queued behind
  // it) must not have started yet, even though weight 1 would fit alongside
  // weight 2 under capacity 2.
  await sleep(400);
  const linesWhileHeavyRuns = fs.existsSync(startFile) ? fs.readFileSync(startFile, 'utf8').trim().split('\n') : [];
  assert.deepEqual(linesWhileHeavyRuns, ['heavy'], 'the light ticket must not backfill ahead of the heavy head');

  await new Promise((resolve) => {
    if (heavy.exitCode !== null || heavy.signalCode !== null) return resolve();
    heavy.on('exit', resolve);
  });
  await new Promise((resolve) => {
    if (light.exitCode !== null || light.signalCode !== null) return resolve();
    light.on('exit', resolve);
  });

  const finalLines = fs.readFileSync(startFile, 'utf8').trim().split('\n');
  assert.deepEqual(finalLines, ['heavy', 'light'], 'the light ticket must start only after the heavy one releases');
});
