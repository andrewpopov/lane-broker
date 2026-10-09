import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, writeCancelMarkerFile, writeWithdrawMarkerFile } from '../src/state.js';
import { ADMISSION_LOG_REFRESH_MS } from '../src/admission.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { waitLabel, recordTicketWait, readTicketWait, pruneTicketWaits } from '../src/ticket-wait.js';

// BRAIN-504 part 3: the per-ticket wait record, written from tryStart's real outcome and read by `lane status`.

const GIB = 1024 ** 3;

const cfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'active', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, cpuAdmissionPercent: 100, cpuReserveCores: 1, admissionCooldownMs: 0 };
const sampler = () => ({ hostBusyCores: 1, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t) => tryStart(state, t, cfg, undefined, sampler, undefined, memory);
const ticket = (id, overrides = {}) => ({ id, key: `r:${id}`, repoId: id, weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', ...overrides });
const heldLease = (id, weight) => ({ id, key: `r:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight, state: LEASE_STATE.RUNNING });

test('waitLabel: every reason has its label, and an unknown reason is returned raw', () => {
  const cases = [
    ['capacity', { runningWeight: 7, capacity: 10 }, 'weight (7/10 slots in use)'],
    ['cpu-admission', { cpuReason: 'projected-over-budget', projectedBusy: 5.456, budget: 4 }, 'CPU budget (projected-over-budget: projected 5.46 > budget 4.00 cores)'],
    ['memory-admission', { memoryReason: 'insufficient-memory' }, 'memory budget (insufficient-memory)'],
    ['load-gate-closed', { load: 12.5 }, 'the load gate (load 12.5)'],
    ['memory-critical', { macPressure: 'critical' }, 'memory pressure (critical)'],
    ['class-cap', { capReason: 'sim-cap' }, 'a class cap (sim-cap)'],
    ['conflict', { with: 'x', key: 'r:k' }, 'a conflicting lease (r:k)'],
    ['paused', { pauseReason: 'upgrade' }, 'the broker pause (upgrade)'],
    ['elastic-below-floor', {}, "CPU budget below this lane's minCpuCores"],
    ['exclusive-held', { holder: 'h' }, 'an exclusive lease (exclusive-held)'],
    ['exclusive-head', { holder: 'h' }, 'an exclusive lease (exclusive-head)'],
    ['exclusive-draining', { running: 2 }, 'an exclusive lease (exclusive-draining)'],
    ['not-head', { position: 3, queueLength: 5 }, '2 ticket(s) ahead (position 3/5)'],
    ['something-new', {}, 'something-new'],
  ];
  for (const [reason, detail, label] of cases) assert.equal(waitLabel(reason, detail), label, reason);
});

test('a head denied on capacity records reason capacity; the same reason keeps since, a changed reason resets it', async () => {
  const { state } = freshEnv();
  writeLease(state, heldLease('holder', 7));
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  const first = await poll(state, head);
  assert.equal(first.reason, 'capacity');
  const rec1 = readTicketWait(state, 'head');
  assert.equal(rec1.reason, 'capacity');
  assert.deepEqual(rec1.detail, { runningWeight: 7, capacity: 10 });
  assert.equal(rec1.ticketId, 'head');

  // rewrite with an old `at` so a second denial is past the refresh window: since must survive the rewrite
  const old = { ...rec1, since: rec1.since - 5000, at: rec1.at - ADMISSION_LOG_REFRESH_MS - 1 };
  fs.writeFileSync(`${paths(state).waits}/head.json`, JSON.stringify(old));
  await poll(state, head);
  const rec2 = readTicketWait(state, 'head');
  assert.equal(rec2.since, old.since, 'same reason keeps since');
  assert.ok(rec2.at > old.at, 'the stale record was refreshed');

  fs.writeFileSync(`${paths(state).waits}/head.json`, JSON.stringify({ ...rec2, reason: 'conflict', detail: {} }));
  await poll(state, head);
  const rec3 = readTicketWait(state, 'head');
  assert.equal(rec3.reason, 'capacity');
  assert.ok(rec3.since > old.since, 'a changed reason resets since');
});

test('a non-head poll records not-head with its position', async () => {
  const { state } = freshEnv();
  writeLease(state, heldLease('holder', 7));
  await enqueue(state, ticket('head', { weight: 4 }));
  const behind = ticket('behind', { weight: 4 });
  await enqueue(state, behind);
  const result = await poll(state, behind);
  assert.equal(result.reason, 'not-head');
  const rec = readTicketWait(state, 'behind');
  assert.equal(rec.reason, 'not-head');
  assert.equal(rec.detail.position, 2);
});

test('a started ticket loses its wait file', async () => {
  const { state } = freshEnv();
  const t = ticket('go');
  await enqueue(state, t);
  recordTicketWait(state, 'go', { started: false, reason: 'capacity', runningWeight: 1, capacity: 2 });
  assert.ok(readTicketWait(state, 'go'));
  assert.equal((await poll(state, t)).started, true);
  assert.equal(readTicketWait(state, 'go'), null);
});

for (const [name, mark, reason] of [['withdrawn', writeWithdrawMarkerFile, 'withdrawn'], ['cancelled', writeCancelMarkerFile, 'cancelled']]) {
  test(`a ${name} ticket loses its wait file`, async () => {
    const { state } = freshEnv();
    const t = ticket('gone');
    await enqueue(state, t);
    recordTicketWait(state, 'gone', { started: false, reason: 'capacity', runningWeight: 1, capacity: 2 });
    mark(state, 'gone');
    assert.equal((await poll(state, t)).reason, reason);
    assert.equal(readTicketWait(state, 'gone'), null);
  });
}

test('refresh: an unchanged reason is not rewritten inside ADMISSION_LOG_REFRESH_MS, and is after it', () => {
  const { state } = freshEnv();
  const result = { started: false, reason: 'load-gate-closed', load: 9 };
  recordTicketWait(state, 't', result, 1000);
  const file = `${paths(state).waits}/t.json`;
  const before = fs.statSync(file).ino;
  recordTicketWait(state, 't', result, 1000 + ADMISSION_LOG_REFRESH_MS - 1);
  assert.equal(readTicketWait(state, 't').at, 1000, 'not rewritten');
  assert.equal(fs.statSync(file).ino, before);
  recordTicketWait(state, 't', result, 1000 + ADMISSION_LOG_REFRESH_MS);
  const rec = readTicketWait(state, 't');
  assert.equal(rec.at, 1000 + ADMISSION_LOG_REFRESH_MS);
  assert.equal(rec.since, 1000, 'since survives the refresh');
  recordTicketWait(state, 't', { started: false, reason: 'paused', pauseReason: 'x' }, 1001);
  assert.equal(readTicketWait(state, 't').reason, 'paused', 'a changed reason writes immediately');
});

test('a write failure never throws', () => {
  const { state } = freshEnv();
  fs.writeFileSync(paths(state).waits, 'not a directory');
  assert.doesNotThrow(() => recordTicketWait(state, 't', { started: false, reason: 'capacity', runningWeight: 1, capacity: 2 }));
});

test('pruneTicketWaits removes files of tickets that are not live and keeps the rest', () => {
  const { state } = freshEnv();
  for (const id of ['live', 'dead']) recordTicketWait(state, id, { started: false, reason: 'paused', pauseReason: 'x' });
  pruneTicketWaits(state, ['live']);
  assert.ok(readTicketWait(state, 'live'));
  assert.equal(readTicketWait(state, 'dead'), null);
});

test('tryStart prunes the wait file of a ticket that is no longer queued', async () => {
  const { state } = freshEnv();
  recordTicketWait(state, 'orphan', { started: false, reason: 'paused', pauseReason: 'x' });
  const t = ticket('real');
  await enqueue(state, t);
  await poll(state, t);
  assert.equal(readTicketWait(state, 'orphan'), null);
});

async function withStatusEnv(fn) {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await fn(state);
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

test('status: the head line and queue suffix come from the wait file; JSON carries wait per entry', async () => {
  await withStatusEnv(async (state) => {
    await enqueue(state, ticket('headticket-1', { weight: 4 }));
    await enqueue(state, ticket('behind-1', { weight: 4 }));
    const now = Date.now();
    recordTicketWait(state, 'headticket-1', { started: false, reason: 'capacity', runningWeight: 7, capacity: 10 }, now - 90_000);
    const status = await collectStatus();
    assert.equal(status.queued[0].wait.reason, 'capacity');
    assert.equal(status.queued[0].wait.label, 'weight (7/10 slots in use)');
    assert.equal(status.queued[1].wait, null);
    const text = renderStatusText(status);
    assert.match(text, /^head headtick: waiting on weight \(7\/10 slots in use\) for 1m\d+s \(checked 1m\d+s ago\)$/m);
    assert.match(text, /#1 headticket-1 .*  waiting: weight \(7\/10 slots in use\)$/m);
    assert.doesNotMatch(text, /#2 behind-1 .*waiting:/);
  });
});

test('status: "no recorded wait yet" when the head has no file; a wait file for an unqueued ticket is ignored', async () => {
  await withStatusEnv(async (state) => {
    await enqueue(state, ticket('headticket-2'));
    recordTicketWait(state, 'ghost', { started: false, reason: 'paused', pauseReason: 'x' });
    const status = await collectStatus();
    assert.equal(status.queued.length, 1);
    assert.equal(status.queued[0].wait, null);
    const text = renderStatusText(status);
    assert.match(text, /^head headtick: no recorded wait yet$/m);
    assert.doesNotMatch(text, /ghost|broker pause/);
  });
});
