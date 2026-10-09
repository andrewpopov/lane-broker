import test from 'node:test';
import fs from 'node:fs';
import assert from 'node:assert/strict';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { topExternalCpu, readCommands } from '../src/external-cpu.js';
import { sampleHostCpu } from '../src/cpu.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';

// BRAIN-463: name the process trees burning CPU outside any lease.

const row = (pid, ppid, deltaSec, pgid = pid) => ({ pid, ppid, pgid, nice: 0, token: `t${pid}`, deltaSec });
const COMMANDS = new Map([
  [100, '-zsh'],
  [101, 'xargs -P4 -n1 node --test'],
  [102, 'timeout 600 node --test'],
  [103, 'node --test src/'],
  [104, 'node --test worker'],
  [105, 'node --test worker'],
  [106, 'node --test worker'],
  [107, 'node --test worker'],
  [500, 'lane run -- npm test'],
  [501, 'node vitest'],
  [900, 'cc1plus big.cc'],
]);
const readCmds = (pids) => new Map(pids.filter((p) => COMMANDS.has(p)).map((p) => [p, COMMANDS.get(p)]));
const cwdOf = (pid) => (pid === 101 ? '/work/tree' : undefined);

/** The incident: zsh -> xargs -> timeout -> node --test with 4 workers at ~100% each, 1s window. */
const incidentRows = () => [row(1, 0, 0), row(100, 1, 0), row(101, 100, 0), row(102, 101, 0), row(103, 102, 0.1), row(104, 103, 1), row(105, 103, 1), row(106, 103, 1), row(107, 103, 1)];
const procWindow = (rows) => ({ rows, windowMs: 1000 });

test('an unlaned 4-worker tree is one entry: summed cores, topmost non-shell ancestor, its cwd', () => {
  const top = topExternalCpu({ procWindow: procWindow(incidentRows()), readCmds, cwdOf });
  assert.deepEqual(top, [{ pid: 101, cores: 4.1, cmd: 'xargs -P4 -n1 node --test', cwd: '/work/tree' }]);
});

test('lease-owned processes are excluded, however much CPU they burn', () => {
  const rows = [...incidentRows(), row(500, 1, 0, 500), row(501, 500, 3, 500)];
  const lease = { id: 'lease-1', childPgid: 500, supervisorPid: 499, descendants: [] };
  const top = topExternalCpu({ procWindow: procWindow(rows), heldLeases: [lease], readCmds, cwdOf, readMarkers: () => new Map() });
  assert.deepEqual(top.map((t) => t.pid), [101]);
  assert.ok(!top.some((t) => t.pid === 500 || t.pid === 501));
});

test('a lease whose tree is not known yet makes the answer null, not a guess', () => {
  const lease = { id: 'lease-1', childPgid: null };
  assert.equal(topExternalCpu({ procWindow: procWindow(incidentRows()), heldLeases: [lease], readCmds, cwdOf }), null);
});

test('ranks by cores, caps at the limit, skips kernel threads and this process', () => {
  const rows = [row(2, 0, 9), row(3, 2, 9), row(1, 0, 0), row(900, 1, 2), row(77, 1, 5), row(104, 1, 1), row(105, 1, 0.5)];
  const top = topExternalCpu({ procWindow: procWindow(rows), readCmds, cwdOf, selfPid: 77, limit: 2 });
  assert.deepEqual(top.map((t) => [t.pid, t.cores]), [[900, 2], [104, 1]]);
});

test('readCommands: macOS path parses one ps listing', () => {
  const exec = (cmd, args) => (args.includes('-p') ? ' 101 xargs -P4\n 103 node --test\n' : '');
  assert.deepEqual([...readCommands([101, 103], { platform: 'darwin', exec })], [[101, 'xargs -P4'], [103, 'node --test']]);
});

// ---- starvation record and status ----

const GIB = 1024 ** 3;
const cfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'active', capacity: 10, maxLaneWeight: 5, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, cpuAdmissionPercent: 100, cpuReserveCores: 1, admissionCooldownMs: 0, resourceIdleOvershootCores: 0 };
const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const ticket = (id, weight = 1) => ({ id, key: `r:${id}`, weight, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' });
const poll = (state, t, ext) => tryStart(state, t, cfg, undefined, sampler(ext), undefined, memory);
const starvedFile = (state) => paths(state).externalStarved;

async function withEnv(home, state, fn) {
  writeGlobalConfig(home, { version: 1, capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  const prev = { h: process.env.LANE_BROKER_HOME, s: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await fn();
  } finally {
    process.env.LANE_BROKER_HOME = prev.h;
    process.env.LANE_BROKER_STATE = prev.s;
  }
}

test('starved-since is set by a high-external projected-over-budget denial, persists, and clears on admit', async () => {
  const { home, state } = freshEnv();
  const head = ticket('head', 4);
  await enqueue(state, head);
  assert.equal((await poll(state, head, 7)).reason, 'cpu-admission'); // 7 + 4 > 9, external 7 >= 25% of 9
  const first = JSON.parse(fs.readFileSync(starvedFile(state), 'utf8'));
  await new Promise((r) => setTimeout(r, 20));
  await poll(state, head, 7);
  const second = JSON.parse(fs.readFileSync(starvedFile(state), 'utf8'));
  assert.deepEqual(second, first, 'a denial inside one sample interval does not rewrite the record');
  atomicWriteJson(starvedFile(state), { ...first, lastAt: first.lastAt - cfg.sampleMs - 1 });
  await poll(state, head, 7);
  const third = JSON.parse(fs.readFileSync(starvedFile(state), 'utf8'));
  assert.equal(third.since, first.since, 'the start of the episode survives later denials');
  assert.ok(third.lastAt > first.lastAt - 1, 'an older record is refreshed');

  await withEnv(home, state, async () => {
    const a = await collectStatus();
    assert.ok(a.externalCpu.starvedSinceMs >= 0);
    await new Promise((r) => setTimeout(r, 20));
    const b = await collectStatus();
    assert.ok(b.externalCpu.starvedSinceMs > a.externalCpu.starvedSinceMs, 'persists across status calls');
    assert.match(renderStatusText(b), /admission starved by external CPU for/);
  });

  const admitted = await poll(state, head, 0);
  assert.equal(admitted.started, true);
  await withEnv(home, state, async () => assert.equal((await collectStatus()).externalCpu.starvedSinceMs, null));
  removeLease(state, head.id);
});

test('a denial caused by the lanes themselves (low external busy) does not start, and ends, the starved state', async () => {
  const { state } = freshEnv();
  const head = ticket('head', 4);
  await enqueue(state, head);
  await poll(state, head, 7);
  atomicWriteJson(starvedFile(state), { since: 1, lastAt: Date.now(), externalBusy: 7, budget: 9 });
  writeLease(state, { id: 'held', key: 'held:x', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 6, resources: { cpuCores: 6 }, state: LEASE_STATE.RUNNING });
  assert.equal((await poll(state, head, 1)).reason, 'cpu-admission'); // 1 + 6 + 4 > 9, external 1 < 25% of 9
  assert.equal(fs.existsSync(starvedFile(state)), false);
});

test('a starved record that stopped being refreshed is not reported', async () => {
  const { home, state } = freshEnv();
  atomicWriteJson(starvedFile(state), { since: Date.now() - 3_600_000, lastAt: Date.now() - 3_000_000, externalBusy: 7, budget: 9 });
  await withEnv(home, state, async () => assert.equal((await collectStatus()).externalCpu.starvedSinceMs, null));
});

test('status --json shape: externalCpu {busyCores, top[{pid,cores,cmd,cwd?}], starvedSinceMs} and the text block', async () => {
  const { home, state } = freshEnv();
  atomicWriteJson(paths(state).cpuSample, {
    at: Date.now(),
    cpus: [],
    lastValid: { hostBusyCores: 12, cores: 10, at: Date.now(), procWindow: { rows: [{ ...row(1, 0, 0) }, { ...row(4242, 1, 6) }, { ...row(4243, 4242, 6) }], windowMs: 1000 } },
  });
  await withEnv(home, state, async () => {
    const status = await collectStatus();
    const ex = JSON.parse(JSON.stringify(status)).externalCpu;
    assert.equal(ex.busyCores, 12);
    assert.equal(ex.starvedSinceMs, null);
    assert.equal(ex.top.length, 1);
    assert.equal(ex.top[0].pid, 4242);
    assert.equal(ex.top[0].cores, 12);
    assert.equal(typeof ex.top[0].cmd, 'string');
    const text = renderStatusText(status);
    assert.match(text, /external CPU: 12\.00 busy cores outside any lease, top process trees:/);
    assert.match(text, /12\.00 cores  pid 4242/);
    assert.doesNotMatch(text, /starved/);
  });
});

test('niceMin=0: the admission sampler never reads the process table', () => {
  const { state } = freshEnv();
  let reads = 0;
  const cpus = (user, idle) => Array.from({ length: 4 }, () => ({ model: 't', speed: 0, times: { user, nice: 0, sys: 0, idle, irq: 0 } }));
  const readProcs = () => {
    reads += 1;
    return [];
  };
  sampleHostCpu(state, cpus(0, 0), { preemptible: { laneNice: 0, niceMin: 0, heldLeases: [] }, readProcs });
  sampleHostCpu(state, cpus(50, 50), { preemptible: { laneNice: 0, niceMin: 0, heldLeases: [] }, readProcs });
  assert.equal(reads, 0);
  sampleHostCpu(state, cpus(90, 90), { preemptible: { laneNice: 0, niceMin: 1, heldLeases: [] }, readProcs });
  assert.equal(reads, 1, 'control: with discounting on the table is read');
});

test('a stale stored window is not shown as current: status measures afresh only when asked', async () => {
  const { home, state } = freshEnv();
  const old = Date.now() - 600_000;
  atomicWriteJson(paths(state).cpuSample, { at: old, cpus: [], lastValid: { hostBusyCores: 12, cores: 10, at: old, procWindow: { rows: [row(1, 0, 0), row(4242, 1, 12)], windowMs: 1000 } } });
  const fresh = { rows: [row(1, 0, 0), row(7777, 1, 3)], windowMs: 1000 };
  await withEnv(home, state, async () => {
    const quiet = await collectStatus();
    assert.deepEqual(quiet.externalCpu.top, [], 'programmatic callers (remote-probe) never scan, and never see the old window');
    assert.equal(quiet.externalCpu.busyCores, null);
    const asked = await collectStatus({ scanExternal: true, scan: async () => fresh });
    assert.deepEqual(asked.externalCpu.top.map((t) => t.pid), [7777]);
    assert.equal(asked.externalCpu.busyCores, 3);
  });
});

test('status external busy matches admission: 8 busy cores, all preemptible, share 0.8 -> 1.6', async () => {
  const { home, state } = freshEnv();
  atomicWriteJson(paths(state).cpuSample, { at: Date.now(), cpus: [], lastValid: { hostBusyCores: 8, preemptibleBusyCores: 8, cores: 10, at: Date.now(), procWindow: { rows: [row(1, 0, 0), row(4242, 1, 8)], windowMs: 1000 } } });
  await withEnv(home, state, async () => {
    const status = await collectStatus();
    assert.ok(Math.abs(status.externalCpu.busyCores - 1.6) < 1e-9, `got ${status.externalCpu.busyCores}`);
  });
});
