import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeCpuBusyFile, laneSpawn, laneRun, waitFor } from './helpers.js';
import { sampleHostCpu, parseProcessTable, preemptibleCoresFromRows } from '../src/cpu.js';
import { sampleAndUpdateCpuGate, evaluateCpuAdmission, nonPreemptibleBusy } from '../src/admission.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths, readJsonSafe } from '../src/state.js';

/** BRAIN-428: niced CPU is preemptible, not busy. 10 cores; cumulative tick counters per core. */
const CORES = 10;
const cpus = (ticks) =>
  Array.from({ length: CORES }, () => ({ model: 't', speed: 0, times: { user: ticks.user, nice: ticks.nice, sys: 0, idle: ticks.idle, irq: 0 } }));
const cfg = { ...DEFAULT_GLOBAL_CONFIG, cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 1, cpuAdmissionPercent: 90, cpuReserveCores: 0 };

/** Baseline then a delta where every core spends 95 of 100 ticks busy, split `userShare`/`niceShare`. */
function sampleDelta(state, { user, nice }, opts) {
  sampleHostCpu(state, cpus({ user: 0, nice: 0, idle: 0 }), opts);
  return sampleHostCpu(state, cpus({ user, nice, idle: 5 }), opts);
}

test('Linux: a /proc/stat delta that is mostly nice leaves the CPU gate open', () => {
  const { state } = freshEnv();
  const sample = sampleDelta(state, { user: 10, nice: 85 }, { platform: 'linux' });
  assert.ok(Math.abs(sample.hostBusyCores - 9.5) < 1e-9);
  assert.ok(Math.abs(sample.preemptibleBusyCores - 8.5) < 1e-9, `got ${sample.preemptibleBusyCores}`);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, sample).closed, false, '9.5 busy - 0.8*8.5 = 2.7 cores of 10');
});

test('Linux: the same load as user time closes the gate', () => {
  const { state } = freshEnv();
  const sample = sampleDelta(state, { user: 95, nice: 0 }, { platform: 'linux' });
  assert.equal(sample.preemptibleBusyCores, 0);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, sample).closed, true);
});

test('preemptibleNiceMin 0 disables the feature: nice time counts as busy again', () => {
  const { state } = freshEnv();
  const sample = sampleDelta(state, { user: 10, nice: 85 }, { platform: 'linux', preemptibleNiceMin: 0 });
  assert.equal(sample.preemptibleBusyCores, 0);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, sample).closed, true);
});

const PS_NICED = ['1 0 1 400.0 100 10', '2 0 2 400.0 100 10', '3 0 3 150.0 100 0'].join('\n');

test('macOS: nice-10 heavy processes from the ps table are preemptible and keep the gate open', () => {
  const { state } = freshEnv();
  const exec = () => PS_NICED;
  const sample = sampleDelta(state, { user: 95, nice: 0 }, { platform: 'darwin', exec });
  assert.ok(Math.abs(sample.preemptibleBusyCores - 8) < 1e-9, `got ${sample.preemptibleBusyCores}`);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, sample).closed, false, '9.5 - 0.8*8 = 3.1 of 10');
});

test('macOS: the same processes at nice 0 are ordinary load and close the gate', () => {
  const { state } = freshEnv();
  const exec = () => PS_NICED.replaceAll(' 10', ' 0');
  const sample = sampleDelta(state, { user: 95, nice: 0 }, { platform: 'darwin', exec });
  assert.equal(sample.preemptibleBusyCores, 0);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, sample).closed, true);
});

test('ps parsing: nice column is read, absent column leaves it undefined (never preemptible), a "-" nice drops the line', () => {
  const rows = parseProcessTable('1 0 1 50.0 10 5\n2 0 2 50.0 10\n3 0 3 50.0 10 -');
  assert.deepEqual(rows.map((r) => r.nice), [5, undefined]);
  assert.equal(preemptibleCoresFromRows(rows, 1), 0.5);
});

test('admission: preemptible load is discounted by preemptibleShare, normal load in full, broker CPU not discounted twice', () => {
  const sample = { hostBusyCores: 9, preemptibleBusyCores: 8, cores: 10, stale: false };
  assert.ok(Math.abs(nonPreemptibleBusy(sample, cfg) - 2.6) < 1e-9);
  assert.equal(nonPreemptibleBusy({ ...sample, preemptibleBusyCores: 0 }, cfg), 9);
  // 4 cores of the niced figure are our own (already subtracted as broker CPU): only 4 are external niced
  assert.ok(Math.abs(nonPreemptibleBusy(sample, cfg, 4) - 5.8) < 1e-9);
  const decision = (s) => evaluateCpuAdmission({ cpuSample: s, heldLeases: [], candidateWeight: 2, cpuGateState: { closed: false }, cooldownBlocked: false, cfg });
  const niced = decision(sample);
  assert.equal(niced.admit, true, `projected ${niced.projectedBusy} of budget ${niced.budget}`);
  assert.ok(Math.abs(niced.externalBusy - 2.6) < 1e-9);
  assert.equal(decision({ ...sample, preemptibleBusyCores: 0 }).reason, 'projected-over-budget');
});

test('integration: a lane is admitted while a fake niced load is present, and not when it is normal load', async () => {
  const { base, home, state, env: baseEnv } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 150, schedulerMode: 'active', cpuAdmissionPercent: 90, cpuReserveCores: 0, cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 1 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const busyFile = writeCpuBusyFile(base, 9.5, 10);
  fs.writeFileSync(busyFile, '9.5,10,0'); // 95% busy, none of it preemptible
  const env = { ...baseEnv, LANE_BROKER_CPU_BUSY_FILE: busyFile };
  const spawnId = (args) =>
    new Promise((resolve) => {
      const child = laneSpawn(args, { env, cwd: repoDir });
      let out = '';
      child.stdout.on('data', (d) => (out += d));
      child.on('exit', () => resolve(out.trim()));
    });
  const blocked = await spawnId(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'sleep', '3']);
  assert.ok(await waitFor(() => readJsonSafe(paths(state).cpuGate)?.closed === true, { timeoutMs: 10000 }), 'normal load closes the gate');
  assert.equal(fs.existsSync(path.join(paths(state).leases, `${blocked}.json`)), false);

  fs.writeFileSync(busyFile, '9.5,10,8.5'); // same total, 8.5 cores of it niced
  assert.ok(await waitFor(() => fs.existsSync(path.join(paths(state).leases, `${blocked}.json`)), { timeoutMs: 15000 }), 'the lane starts once the load is niced');
  await laneRun(['cancel', blocked], { env, cwd: repoDir });
});
