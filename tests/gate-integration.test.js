import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeLoadFile, laneRun, laneSpawn, sleep, waitFor } from './helpers.js';
import { paths, readJsonSafe, bootId } from '../src/state.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';

/**
 * BRAIN-197: the load gate no longer blocks an idle broker, so a gate test
 * that expects a closed gate to refuse a start needs a live, NON-CONFLICTING
 * holder lease (different key, capacity to spare) or the candidate would sail
 * through on the idle exemption before the gate mechanics under test ever
 * run. Fresh heartbeat + this test process's own (alive) pid, matching
 * bootId, so it survives the real supervisor's own reapAll pass.
 */
function holdLease(state, overrides = {}) {
  const lease = {
    id: 'holder-1',
    key: 'repo/holder-lane',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    admittedAt: Date.now(),
    cwd: '/tmp',
    cmd: ['sleep', '999'],
    weight: 1,
    logPath: '/tmp/holder-log',
    resultPath: '/tmp/holder-result',
    state: LEASE_STATE.RUNNING,
    ...overrides,
  };
  writeLease(state, lease);
  return lease;
}

test('load gate: refuses to start while closed, starts once it reopens after enough consecutive low samples', async () => {
  const { base, home, state, env: baseEnv } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 2, sampleMs: 150 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  holdLease(state);

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
  holdLease(state);

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
  const closed = await waitFor(() => readJsonSafe(paths(state).loadGate)?.closed === true, { timeoutMs: 5000 });
  assert.ok(closed, 'the gate must actually observe the high load and close before the rest of this test means anything');
  assert.equal(fs.existsSync(path.join(paths(state).leases, `${id}.json`)), false, 'must not start while the load gate is closed');

  // Drop the load to 12.83, matching the real incident: above the original
  // loadOpen (11) but below loadClose (15). Under the ORIGINAL config this
  // never reopens the gate — every sample in that band resets the
  // consecutive-under counter to 0 forever, exactly as observed live.
  fs.writeFileSync(loadFile, '12.83');
  // Wait for at least one sample to land at the new load, then give a few
  // more sample cycles' margin before asserting it stays closed. Asserting
  // this wait (not just the sleep after it) matters: without it, a slow
  // supervisor's first sample could still be pending when the config gets
  // rewritten below, and the test would pass against pre-fix code for the
  // wrong reason (timing luck rather than proof of the fix).
  const sampledOldLoad = await waitFor(() => readJsonSafe(paths(state).loadGate)?.lastLoad === 12.83, { timeoutMs: 5000 });
  assert.ok(sampledOldLoad, 'the gate must sample the pre-incident load (12.83) before the config is rewritten below');
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

test('BRAIN-197: an idle broker (no held leases) starts its head immediately even while the load gate is closed', async () => {
  const { base, home, state, env: baseEnv } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 2, sampleMs: 150 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  // Deliberately no holdLease() here: the broker holds nothing, so the
  // closed gate below must not block this admission at all.

  const loadFile = writeLoadFile(base, 20); // closed, and stays closed for the whole test
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

  const lease = await waitFor(() => fs.existsSync(path.join(paths(state).leases, `${id}.json`)), { timeoutMs: 5000 });
  assert.ok(lease, 'an idle broker must start its head even though the load gate never reopened');

  const gateState = readJsonSafe(paths(state).loadGate);
  assert.equal(gateState?.closed, true, 'the gate itself must still read closed: the exemption bypasses it, it does not reopen it');
});
