import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeCpuBusyFile, laneSpawn, laneRun, waitFor } from './helpers.js';
import { sampleHostCpu } from '../src/cpu.js';
import { parseProcCpuStat, parsePsCpuTable, parsePsCpuTime, readLeaseMarkers } from '../src/preemptible.js';
import { sampleAndUpdateCpuGate, evaluateCpuAdmission, nonPreemptibleBusy } from '../src/admission.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths, readJsonSafe } from '../src/state.js';

/**
 * BRAIN-428: CPU of processes the candidate lane outranks (strictly higher nice), measured per process by identity
 * over the same window as the host sample, is preemptible. 10 cores; each core 95 of 100 ticks busy over a 1s window.
 */
const CORES = 10;
const cpus = (user, idle) => Array.from({ length: CORES }, () => ({ model: 't', speed: 0, times: { user, nice: 0, sys: 0, idle, irq: 0 } }));
const cfg = { ...DEFAULT_GLOBAL_CONFIG, cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 1, cpuAdmissionPercent: 90, cpuReserveCores: 0 };
const proc = (pid, nice, cpuSec, extra = {}) => ({ pid, ppid: 1, pgid: pid, nice, cpuSec, token: `t${pid}`, ...extra });

/** Baseline sample at t=1000, then a second 1000 ms later. `before`/`after` are the process tables at the two instants. */
function sampleWindow(t, state, before, after, preemptible) {
  let clock = 1000;
  t.mock.method(Date, 'now', () => clock);
  const opts = (rows) => ({ preemptible, readProcs: () => rows });
  sampleHostCpu(state, cpus(0, 0), opts(before));
  clock = 2000;
  return sampleHostCpu(state, cpus(95, 5), opts(after));
}
const pair = (nice, perProc = 4.25) => [[proc(11, nice, 100), proc(12, nice, 100)], [proc(11, nice, 100 + perProc), proc(12, nice, 100 + perProc)]];
const lane = (laneNice, extra = {}) => ({ laneNice, niceMin: 1, heldLeases: [], ...extra });

test('nice-0 lane: nice-10 external load is preemptible and keeps the gate open', (t) => {
  const { state } = freshEnv();
  const sample = sampleWindow(t, state, ...pair(10), lane(0));
  assert.ok(Math.abs(sample.hostBusyCores - 9.5) < 1e-9);
  assert.ok(Math.abs(sample.preemptibleBusyCores - 8.5) < 1e-9, `got ${sample.preemptibleBusyCores}`);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, sample).closed, false, '9.5 - 0.8*8.5 = 2.7 of 10');
});

test('nice-10 lane: the same nice-10 load is a peer, not preemptible, and closes the gate', (t) => {
  const { state } = freshEnv();
  const sample = sampleWindow(t, state, ...pair(10), lane(10));
  assert.equal(sample.preemptibleBusyCores, 0);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, sample).closed, true);
});

test('nice-10 lane: nice 15 external load is lower priority than the lane and does help', (t) => {
  const { state } = freshEnv();
  const sample = sampleWindow(t, state, ...pair(15), lane(10));
  assert.ok(Math.abs(sample.preemptibleBusyCores - 8.5) < 1e-9);
});

test('a lane\'s own niced processes are never preemptible: a host saturated by one niced lane is not over-admitted', (t) => {
  const { state } = freshEnv();
  // lane leader 100 (pgid 100) plus a setsid'd server (pid 300, own pgid, ppid unrelated) recorded in lease.descendants
  const tree = (cpu) => [proc(100, 10, cpu / 2), proc(300, 10, cpu / 2, { ppid: 1, pgid: 300 })];
  const lease = { id: 'L1', childPgid: 100, descendants: [{ pid: 300, token: 't300' }], weight: 4, resources: { cpuCores: 4 } };
  const sample = sampleWindow(t, state, tree(0), tree(9.5), lane(0, { heldLeases: [lease] }));
  assert.equal(sample.preemptibleBusyCores, 0, 'the lane tree burning 9.5 cores at nice 10 is lane CPU, not external preemptible');
  const decision = evaluateCpuAdmission({ cpuSample: sample, heldLeases: [lease], candidateWeight: 2, cpuGateState: { closed: false }, cooldownBlocked: false, cfg });
  assert.equal(decision.admit, false);
});

test('fail closed: a held lease whose tree is not known yet, or an unreadable process table, discounts nothing', (t) => {
  const { state } = freshEnv();
  const spawning = { id: 'L2', childPgid: null, descendants: [] };
  assert.equal(sampleWindow(t, state, ...pair(10), lane(0, { heldLeases: [spawning] })).preemptibleBusyCores, 0);
  const s2 = freshEnv().state;
  let clock = 1000;
  t.mock.method(Date, 'now', () => clock);
  const broken = { preemptible: lane(0), readProcs: () => { throw new Error('ps failed'); } };
  sampleHostCpu(s2, cpus(0, 0), broken);
  clock = 2000;
  assert.equal(sampleHostCpu(s2, cpus(95, 5), broken).preemptibleBusyCores, 0);
});

test('a process with no previous reading, a reused pid, or one that just went idle contributes 0', (t) => {
  const { state } = freshEnv();
  const before = [proc(11, 10, 100), proc(12, 10, 100), proc(13, 10, 50)];
  const after = [
    proc(11, 10, 100), // went idle: interval delta 0
    { ...proc(12, 10, 999), token: 'new-start' }, // same pid, new start time: a different process
    proc(14, 10, 500), // first seen this window
    proc(13, 10, 51.5), // the only real contributor: 1.5 cores
  ];
  assert.ok(Math.abs(sampleWindow(t, state, before, after, lane(0)).preemptibleBusyCores - 1.5) < 1e-9);
});

test('a reused sample is recomputed per candidate: the nice-0 discount never crosses to a nice-10 candidate', (t) => {
  const { state } = freshEnv();
  let clock = 1000;
  t.mock.method(Date, 'now', () => clock);
  const [before, after] = pair(10);
  const read = (rows) => ({ readProcs: () => rows, reuseWindowMs: 60_000 });
  sampleHostCpu(state, cpus(0, 0), { ...read(before), preemptible: lane(0) });
  clock = 2000;
  const first = sampleHostCpu(state, cpus(95, 5), { ...read(after), preemptible: lane(0) });
  assert.equal(first.reused, undefined);
  assert.ok(Math.abs(first.preemptibleBusyCores - 8.5) < 1e-9);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, first).closed, false);
  clock = 2100; // counters unchanged: both candidates below get the REUSED measurement
  const nice0 = sampleHostCpu(state, cpus(95, 5), { ...read(after), preemptible: lane(0) });
  const nice10 = sampleHostCpu(state, cpus(95, 5), { ...read(after), preemptible: lane(10) });
  assert.equal(nice0.reused, true);
  assert.equal(nice10.reused, true);
  assert.ok(Math.abs(nice0.preemptibleBusyCores - 8.5) < 1e-9);
  assert.equal(nice10.preemptibleBusyCores, 0);
  assert.equal(sampleAndUpdateCpuGate(state, cfg, nice10).closed, true, 'the nice-10 candidate is gated on its own reading, not the nice-0 one');
});

test('a detached niced lane descendant carrying the lease marker is excluded; one with an unreadable environment fails closed', (t) => {
  const { state } = freshEnv();
  const lease = { id: 'L3', childPgid: 100, descendants: [] };
  const rows = (cpu) => [proc(100, 0, 0), proc(500, 10, cpu), proc(600, 10, cpu)]; // 500/600: own pgid, ppid 1, not recorded
  const run = (markers) => {
    const dir = freshEnv().state;
    return sampleWindow(t, dir, rows(0), rows(4), lane(0, { heldLeases: [lease], readMarkers: () => new Map(markers) }));
  };
  assert.ok(Math.abs(run([[500, false], [600, false]]).preemptibleBusyCores - 8) < 1e-9, 'no marker: genuinely external');
  assert.ok(Math.abs(run([[500, true], [600, false]]).preemptibleBusyCores - 4) < 1e-9, 'marker: a lane worker, excluded');
  assert.ok(Math.abs(run([[500, null], [600, false]]).preemptibleBusyCores - 4) < 1e-9, 'unreadable environment: contributes 0');
  void state;
});

test('marker readers: Linux /proc/<pid>/environ and macOS ps -E with the argv prefix stripped', () => {
  const env = { '/proc/7/environ': 'A=1\0LANE_BROKER_LEASE=L9\0', '/proc/8/environ': 'A=1\0' };
  const fsApi = { readFileSync: (f) => { if (f in env) return env[f]; throw Object.assign(new Error('x'), { code: 'EACCES' }); } };
  assert.deepEqual([...readLeaseMarkers([7, 8, 9], ['L9'], { platform: 'linux', fsApi })], [[7, true], [8, false], [9, null]]);
  const exec = (_cmd, args) =>
    args.includes('-E')
      ? ' 7 /bin/node server.js HOME=/h LANE_BROKER_LEASE=L9\n 8 /bin/node LANE_BROKER_LEASE=L9\n'
      : ' 7 /bin/node server.js\n 8 /bin/node LANE_BROKER_LEASE=L9\n';
  assert.deepEqual([...readLeaseMarkers([7, 8, 9], ['L9'], { platform: 'darwin', exec })], [[7, true], [8, false], [9, null]], 'an argument spelling the marker is not a member');
});

test('preemptibleNiceMin 0 disables the feature', (t) => {
  const { state } = freshEnv();
  assert.equal(sampleWindow(t, state, ...pair(10), lane(0, { niceMin: 0 })).preemptibleBusyCores, 0);
});

test('Linux parser: nice, CPU ticks and start token come from /proc/<pid>/stat, comm may hold parens and spaces', () => {
  const line = (utime, stime, nice) => `4242 (we ird) name) S 1 4242 4242 0 -1 4194560 10 0 0 0 ${utime} ${stime} 0 0 30 ${nice} 1 0 98765 1000 100`;
  const a = parseProcCpuStat(4242, line(300, 100, 10), 100);
  assert.deepEqual(a, { pid: 4242, ppid: 1, pgid: 4242, nice: 10, cpuSec: 4, token: '98765' });
  assert.equal(parseProcCpuStat(4242, line(300, 100, 10), 100).cpuSec - a.cpuSec, 0, 'an idle process has a zero interval delta');
  assert.equal(parseProcCpuStat(1, 'garbage', 100), null);
});

test('macOS parser: nice, cumulative time (M:SS.cc, H:MM:SS, D-HH:MM:SS) and lstart token come from one ps read', () => {
  const rows = parsePsCpuTable(
    [
      '  501     1   501  10   0:04.25 Mon Oct  6 12:00:00 2026',
      '  502     1   502   0  12:34.50 Mon Oct  6 12:00:01 2026',
      '  503     1   503   5 1-02:03:04 Mon Oct  6 12:00:02 2026',
      'junk line',
    ].join('\n'),
  );
  assert.deepEqual(rows.map((r) => [r.pid, r.nice, r.cpuSec, r.token]), [
    [501, 10, 4.25, 'Mon Oct 6 12:00:00 2026'],
    [502, 0, 754.5, 'Mon Oct 6 12:00:01 2026'],
    [503, 5, 93784, 'Mon Oct 6 12:00:02 2026'],
  ]);
  assert.equal(parsePsCpuTime('1:02:03'), 3723);
  assert.equal(parsePsCpuTime('??'), null);
});

test('admission: preemptible load is discounted by preemptibleShare, normal load in full, clamped to busy', () => {
  const sample = { hostBusyCores: 9, preemptibleBusyCores: 8, cores: 10, stale: false };
  assert.ok(Math.abs(nonPreemptibleBusy(sample, cfg) - 2.6) < 1e-9);
  assert.equal(nonPreemptibleBusy({ ...sample, preemptibleBusyCores: 0 }, cfg), 9);
  assert.ok(Math.abs(nonPreemptibleBusy({ ...sample, preemptibleBusyCores: 50 }, cfg) - 1.8) < 1e-9);
  assert.equal(nonPreemptibleBusy(sample, { ...cfg, preemptibleNiceMin: 0 }), 9);
  const decision = (s) => evaluateCpuAdmission({ cpuSample: s, heldLeases: [], candidateWeight: 2, cpuGateState: { closed: false }, cooldownBlocked: false, cfg });
  const niced = decision(sample);
  assert.equal(niced.admit, true, `projected ${niced.projectedBusy} of budget ${niced.budget}`);
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
