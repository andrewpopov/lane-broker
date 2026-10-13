import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { idleCreditCores, wouldAdmitWithCredit } from '../src/idle-credit.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { collectStatus, renderStatusText } from '../src/status.js';

/** BRAIN-532 step 2: shadow idle credit. It is computed, reported and logged, and never changes an admission decision. */
const MIN = 60_000;
const GIB = 1024 ** 3;
const CFG = { ...DEFAULT_GLOBAL_CONFIG, capacity: 20, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, schedulerMode: 'active', cpuAdmissionPercent: 90, cpuReserveCores: 0, idleCreditMode: 'shadow' };
const SAMPLE = { hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() };

/** A held 8-core lease that has been CPU-idle (0.1 cores) for `idleMin` minutes. */
const heldLease = (now, over = {}) => ({
  id: 'holder',
  key: 'other:key',
  bootId: bootId(),
  supervisorPid: process.pid,
  supervisorStart: null,
  childPgid: process.pid,
  heartbeatAt: now,
  startedAt: now - 60 * MIN,
  lastOutputAt: now,
  weight: 8,
  state: LEASE_STATE.RUNNING,
  resources: { cpuCores: 8, memoryBytes: GIB },
  observedCpuCores: 0.1,
  observedAt: now,
  cpuIntervalCores: 0.1,
  cpuIdleSince: now - 11 * MIN,
  cpuIdleLastAt: now,
  ...over,
});

test('pure credit: an eligible idle lease is credited its grant minus what it uses, capped at host cores - budget', () => {
  const now = Date.now();
  const lease = heldLease(now);
  const wide = { cores: 16, hostBusyCores: 0 }; // budget 14.4 -> cap 1.6; raw 7.9
  const capped = idleCreditCores([lease], wide, CFG, now);
  assert.ok(Math.abs(capped.total - 1.6) < 1e-9);
  assert.deepEqual(capped.perLease, [{ ticketId: 'holder', credit: 7.9 }]);
  const big = { cores: 40, hostBusyCores: 0 }; // cap 4, still below raw -> capped; use a lower-budget cfg for the uncapped case
  assert.equal(idleCreditCores([lease], big, { ...CFG, cpuAdmissionPercent: 50 }, now).total, 7.9);
  assert.equal(idleCreditCores([lease], big, CFG, now).total, 4);
});

test('pure credit: a lease idle for less than idleCreditAfterMs earns nothing', () => {
  const now = Date.now();
  const young = heldLease(now, { cpuIdleSince: now - 2 * MIN });
  assert.deepEqual(idleCreditCores([young], { cores: 40 }, { ...CFG, cpuAdmissionPercent: 50 }, now), { total: 0, perLease: [] });
});

test('pure credit: a burst revokes the credit at the next sample', () => {
  const now = Date.now();
  const sample = { cores: 40 };
  const cfg = { ...CFG, cpuAdmissionPercent: 50 };
  assert.equal(idleCreditCores([heldLease(now)], sample, cfg, now).total, 7.9);
  // the supervisor drops the idle anchors on a busy interval; the interval figure alone also revokes
  assert.equal(idleCreditCores([heldLease(now, { cpuIntervalCores: 3, observedCpuCores: 3 })], sample, cfg, now).total, 0);
  assert.equal(idleCreditCores([heldLease(now, { cpuIdleSince: undefined, cpuIdleLastAt: undefined })], sample, cfg, now).total, 0);
});

test('pure credit: the elastic floor is never credited away', () => {
  const now = Date.now();
  const lease = heldLease(now, { resources: { cpuCores: 8, minCpuCores: 3, memoryBytes: GIB } });
  assert.equal(idleCreditCores([lease], { cores: 40 }, { ...CFG, cpuAdmissionPercent: 50 }, now).total, 5);
});

test('pure credit: CPU only, so a memory denial is never rescued', () => {
  const credit = { total: 5, perLease: [] };
  const cpuDenied = { admit: false, cpuReason: 'projected-over-budget', memoryReason: 'ok', projectedBusy: 10, budget: 9 };
  assert.equal(wouldAdmitWithCredit(cpuDenied, credit), true);
  assert.equal(wouldAdmitWithCredit({ ...cpuDenied, memoryReason: 'projected-over-budget' }, credit), false);
  assert.equal(wouldAdmitWithCredit({ ...cpuDenied, cpuReason: 'cooldown' }, credit), false);
  assert.equal(wouldAdmitWithCredit({ ...cpuDenied, admit: true }, credit), false);
  assert.equal(wouldAdmitWithCredit({ ...cpuDenied, projectedBusy: 20 }, credit), false);
});

async function headDenied(mode) {
  const { state } = freshEnv();
  const now = Date.now();
  writeLease(state, heldLease(now));
  const cfg = { ...CFG, idleCreditMode: mode };
  const ticket = { id: 'head', key: 'r:default', weight: 2, resources: { cpuCores: 2, memoryBytes: GIB }, createdAt: now - 5000, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' };
  await enqueue(state, ticket);
  const result = await tryStart(state, ticket, cfg, undefined, () => ({ ...SAMPLE, sampledAt: Date.now() }), undefined, () => null);
  return { state, result, cfg, now };
}

test('shadow admission: the real decision is byte-identical to mode off, and the would-admit is recorded once', async () => {
  const off = await headDenied('off');
  const shadow = await headDenied('shadow');
  assert.equal(shadow.result.started, false);
  assert.equal(shadow.result.cpuReason, 'projected-over-budget');
  const strip = (r) => JSON.stringify(r, (k, v) => (k === 'sampledAt' ? undefined : v));
  assert.equal(strip(shadow.result), strip(off.result));
  const log = fs.readFileSync(paths(shadow.state).admissionLog, 'utf8');
  assert.match(log, /event=idle-credit-shadow ticket=head shadowWouldAdmit=true creditTotal=1\.00 waitedMs=\d+/);
  assert.equal(fs.existsSync(paths(off.state).admissionLog) && /idle-credit/.test(fs.readFileSync(paths(off.state).admissionLog, 'utf8')), false);
  // a second poll with the same verdict logs nothing more
  const ticket = { id: 'head', key: 'r:default', weight: 2, resources: { cpuCores: 2, memoryBytes: GIB } };
  await tryStart(shadow.state, ticket, shadow.cfg, undefined, () => ({ ...SAMPLE, sampledAt: Date.now() }), undefined, () => null);
  assert.equal(fs.readFileSync(paths(shadow.state).admissionLog, 'utf8').match(/event=idle-credit-shadow /g).length, 1);
});

/** Run `body` with an isolated state root and a global config of `cfg`, so `lane status` never reads the real one. */
async function withBroker(cfg, body) {
  const { state, home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 20, cpuAdmissionPercent: 90, cpuReserveCores: 0, ...cfg });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await body(state);
  } finally {
    for (const [k, v] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
}

test('status: shows the idle credit line and the idleCredit JSON field in shadow mode (the default)', () =>
  withBroker({}, async (state) => {
    writeLease(state, heldLease(Date.now()));
    const status = await collectStatus();
    assert.equal(status.idleCredit.mode, 'shadow');
    assert.equal(status.idleCredit.headWouldAdmit, false);
    assert.deepEqual(status.idleCredit.perLease.map((p) => p.ticketId), ['holder']);
    assert.ok(status.idleCredit.total >= 0);
    const text = renderStatusText(status);
    assert.match(text, /idle credit \(shadow\): \d+\.\d\d cores from 1 lease\(s\); head would admit: no/);
    assert.match(text, /idle 11m0s/);
  }));

test("status: mode 'off' shows nothing", () =>
  withBroker({ idleCreditMode: 'off' }, async (state) => {
    writeLease(state, heldLease(Date.now()));
    const status = await collectStatus();
    assert.equal('idleCredit' in status, false);
    assert.doesNotMatch(renderStatusText(status), /idle credit/);
  }));

test('status: headWouldAdmit is true when the credit covers the gap between the denied head and its budget', async () => {
  await withBroker({}, async (state) => {
    writeLease(state, heldLease(Date.now()));
    atomicWriteJson(paths(state).cpuSample, { lastValid: { hostBusyCores: 0, preemptibleBusyCores: 0, cores: 10, at: Date.now() } });
    await enqueue(state, { id: 'head', key: 'r:default', weight: 2, resources: { cpuCores: 2, memoryBytes: GIB }, createdAt: Date.now() - 5000, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' });
    const status = await collectStatus();
    assert.equal(status.idleCredit.total, 1);
    assert.equal(status.idleCredit.headWouldAdmit, true);
    assert.match(renderStatusText(status), /head would admit: yes/);
  });
});
