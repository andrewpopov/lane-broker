import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeLoadFile, laneRun, laneSpawn, sleep, waitFor } from './helpers.js';
import { paths, readJsonSafe } from '../src/state.js';

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

test('load gate: a config edit raising loadOpen mid-flight lets a stuck gate reopen, matching the real incident', async () => {
  const { base, home, state, env: baseEnv } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 15, loadOpen: 11, loadOpenSamples: 2, sampleMs: 150 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const loadFile = writeLoadFile(base, 20); // above loadClose (15); closes the gate
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

  // Wait for the gate to actually observe the high load and close, rather
  // than a fixed sleep racing the supervisor's own startup/spawn overhead.
  await waitFor(() => readJsonSafe(paths(state).loadGate)?.closed === true, { timeoutMs: 5000 });
  assert.equal(fs.existsSync(path.join(paths(state).leases, `${id}.json`)), false, 'must not start while the load gate is closed');

  // Drop the load to 12.83, matching the real incident: above the original
  // loadOpen (11) but below loadClose (15). Under the ORIGINAL config this
  // never reopens the gate — every sample in that band resets the
  // consecutive-under counter to 0 forever, exactly as observed live.
  fs.writeFileSync(loadFile, '12.83');
  // Wait for at least one sample to land at the new load, then give a few
  // more sample cycles' margin before asserting it stays closed.
  await waitFor(() => readJsonSafe(paths(state).loadGate)?.lastLoad === 12.83, { timeoutMs: 5000 });
  await sleep(400);
  assert.equal(fs.existsSync(path.join(paths(state).leases, `${id}.json`)), false, 'must still be closed: load sits between the original loadOpen and loadClose');

  // Raise loadOpen above the (unchanged) current load, exactly as the
  // operator did in the incident (loadOpen must stay below loadClose, so
  // loadClose rises too). A supervisor that only ever samples the config it
  // started with can never reopen the gate here, since the load never drops
  // below the original loadOpen (11). This is the end-to-end proof that a
  // running supervisor picks up a config change instead of being stuck on
  // its startup snapshot.
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 40, loadOpen: 14, loadOpenSamples: 2, sampleMs: 150 });

  const lease = await waitFor(() => fs.existsSync(path.join(paths(state).leases, `${id}.json`)), { timeoutMs: 5000 });
  assert.ok(lease, 'should start once the reloaded config raises loadOpen above the current load');
});
