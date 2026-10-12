import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { collectStatus, renderStatusText } from '../src/status.js';

/**
 * BRAIN-532: `lane status` flags a run that is silent AND CPU-idle for 30 minutes as `HUNG?`, a run that is only silent
 * as `SILENT` (rouge e2e-scoped 8bb08cca hung at ~1.1 cores), and notes a 10-minute CPU-idle streak. Warning only.
 */
const MIN = 60_000;
const GIB = 1024 ** 3;

async function statusOf(leases) {
  const { state } = freshEnv();
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    const now = Date.now();
    const base = { bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: process.pid, heartbeatAt: now, weight: 1, state: LEASE_STATE.RUNNING, startedAt: now - 3 * 60 * MIN, resources: { cpuCores: 1, memoryBytes: GIB }, observedAt: now };
    for (const l of leases) writeLease(state, { ...base, ...l(now) });
    const status = await collectStatus();
    const lines = renderStatusText(status).split('\n');
    return { status, line: (id) => lines.find((x) => x.trim().startsWith(id)) };
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
  }
}

const idle = (now, mins, cores = 0.01) => ({ observedCpuCores: cores, cpuIntervalCores: cores, cpuIdleSince: now - mins * MIN, cpuIdleLastAt: now, cpuHungSince: now - mins * MIN, cpuHungLastAt: now });

test('silent 40m and CPU-idle 40m: HUNG? with the cancel command, and the --json fields', async () => {
  const { status, line } = await statusOf([(now) => ({ id: 'hung1', key: 'r:hung', lastOutputAt: now - 40 * MIN, ...idle(now, 40) })]);
  assert.match(line('hung1'), /\[HUNG\? silent 40m0s, cpu 0\.01 — lane cancel hung1\]/);
  const row = status.running[0];
  assert.equal(row.hung, true);
  assert.equal(row.silent, true);
  assert.ok(Math.abs(row.silentMs - 40 * MIN) < 5_000, 'silence is measured from lastOutputAt');
  assert.equal(row.idleMs, 40 * MIN);
});

test('silent 40m but spinning CPU (rouge e2e shape): SILENT, never HUNG?', async () => {
  const { status, line } = await statusOf([(now) => ({ id: 'spin1', key: 'r:spin', lastOutputAt: now - 40 * MIN, observedCpuCores: 1.14, cpuIntervalCores: 1.14 })]);
  assert.match(line('spin1'), /\[SILENT 40m0s, cpu 1\.14\]/);
  assert.doesNotMatch(line('spin1'), /HUNG/);
  assert.equal(status.running[0].hung, false);
});

test('silent 40m but CPU-idle only 10m: SILENT, not HUNG?', async () => {
  const { line } = await statusOf([(now) => ({ id: 'part1', key: 'r:part', lastOutputAt: now - 40 * MIN, ...idle(now, 10) })]);
  assert.match(line('part1'), /\[SILENT 40m0s/);
  assert.doesNotMatch(line('part1'), /HUNG/);
});

test('CPU-idle 40m but printed 1m ago: no flag, only the informational idle note', async () => {
  const { line } = await statusOf([(now) => ({ id: 'quiet1', key: 'r:quiet', lastOutputAt: now - MIN, ...idle(now, 40) })]);
  assert.doesNotMatch(line('quiet1'), /HUNG|SILENT/);
  assert.match(line('quiet1'), / idle 40m0s/);
});

test('idle note appears from 10 minutes, not before', async () => {
  const { line } = await statusOf([(now) => ({ id: 'a1', key: 'r:a', lastOutputAt: now - MIN, ...idle(now, 9) }), (now) => ({ id: 'b1', key: 'r:b', lastOutputAt: now - MIN, ...idle(now, 10) })]);
  assert.doesNotMatch(line('a1'), / idle /);
  assert.match(line('b1'), / idle 10m0s/);
});

test('a run that never wrote falls back to startedAt for silence; a stale idle anchor counts for nothing', async () => {
  const { line } = await statusOf([
    (now) => ({ id: 'mute1', key: 'r:mute', startedAt: now - 45 * MIN, ...idle(now, 45) }),
    (now) => ({ id: 'stale1', key: 'r:stale', lastOutputAt: now - 40 * MIN, ...idle(now, 40), cpuHungLastAt: now - 5 * MIN, cpuIdleLastAt: now - 5 * MIN }),
  ]);
  assert.match(line('mute1'), /\[HUNG\? silent 45m0s/);
  assert.doesNotMatch(line('stale1'), /HUNG/);
});

test('a lease with none of the new fields renders no flag (an old supervisor)', async () => {
  const { line, status } = await statusOf([(now) => ({ id: 'old1', key: 'r:old', startedAt: now - 5 * MIN })]);
  assert.doesNotMatch(line('old1'), /HUNG|SILENT| idle /);
  assert.equal(status.running[0].hung, false);
});
