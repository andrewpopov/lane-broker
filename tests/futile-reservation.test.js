import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { enqueue, tryStart, listQueue, readResourceSkipState } from '../src/scheduler.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { resolveScheduler, fairnessStore, effectiveView } from '../src/fairness.js';
import { fenceLegacyQueue } from '../src/migrate.js';

/**
 * BRAIN-418: a head whose reservation is futile (it would still be denied with every lane lease drained, because
 * external load or memory alone rule it out) holds no reservation and spends no skip budget.
 * Fixture machine as in resource-backfill.test.js: 10 cores, reserve 1 -> CPU budget 9.
 */

const GIB = 1024 ** 3;

const cfg = {
  ...DEFAULT_GLOBAL_CONFIG,
  schedulerMode: 'active',
  capacity: 10,
  loadClose: 1000,
  loadOpen: 900,
  loadOpenSamples: 1,
  cpuAdmissionPercent: 100,
  cpuReserveCores: 1,
  admissionCooldownMs: 0,
  resourceSkipLimit: 3,
  resourceIdleOvershootCores: 0,
};

const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = (overrides = {}) => () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test', ...overrides });
const poll = (state, t, { ext, mem = memory(), cfg: c = cfg }) => tryStart(state, t, c, undefined, sampler(ext), undefined, mem);
const readLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

function ticket(id, overrides = {}) {
  return { id, key: `r:${id}`, weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', ...overrides };
}

async function statusText(home, state) {
  writeGlobalConfig(home, { version: 1, capacity: 10, resourceSkipLimit: 3, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  const prev = { h: process.env.LANE_BROKER_HOME, s: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return renderStatusText(await collectStatus());
  } finally {
    process.env.LANE_BROKER_HOME = prev.h;
    process.env.LANE_BROKER_STATE = prev.s;
  }
}

function holdLease(state, id, weight, extra = {}) {
  writeLease(state, { ...extra, id, key: `held:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight, state: LEASE_STATE.RUNNING });
}

test('futile: external load alone rules the head out, so a ticket behind it is admitted without spending the skip budget', async () => {
  const { home, state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  const denied = await poll(state, head, { ext: 7 }); // 7 + 4 = 11 > 9 even with no lane running
  assert.equal(denied.reason, 'cpu-admission');
  assert.equal(readResourceSkipState(state).futile, 'cpu');

  for (let i = 0; i < 5; i += 1) {
    const small = ticket(`s${i}`);
    await enqueue(state, small);
    const result = await poll(state, small, { ext: 7 }); // 7 + 1 = 8 <= 9
    assert.equal(result.started, true, `backfill ${i} past a futile head is never refused, even past resourceSkipLimit`);
    removeLease(state, small.id);
  }
  const record = readResourceSkipState(state);
  assert.deepEqual({ count: record.count, reserved: record.reserved }, { count: 0, reserved: false }, 'the skip budget is untouched');
  assert.equal(record.headId, 'head', 'the head keeps its place');
  assert.match(readLog(state), /candidate=s0 .*reservation=futile futileCause=cpu headCpu=4\.00/);

  const status = await statusText(home, state);
  assert.match(status, /resource-blocked: head head .*\(futile: external load alone exceeds budget; backfilling\)/);
  assert.doesNotMatch(status, /RESERVED/);
});

test('not futile: lane load is what blocks the head, so the reservation holds once the skip budget is exhausted', async () => {
  const { home, state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);
  holdLease(state, 'busy', 6); // 2 + 6 + 4 > 9, but 2 + 4 fits once the lane drains
  const denied = await poll(state, head, { ext: 2 });
  assert.equal(denied.reason, 'cpu-admission');
  assert.equal(readResourceSkipState(state).futile, undefined, 'draining the lanes would let it start');

  atomicWriteJson(paths(state).resourceSkipState, { ...readResourceSkipState(state), count: 3, reserved: true });
  const refused = await poll(state, small, { ext: 2 });
  assert.equal(refused.started, false);
  assert.equal(refused.reason, 'not-head', 'the reservation holds');
  assert.match(await statusText(home, state), /RESERVED/);
});

test('memory-futile: CPU fits but memory cannot fit even with zero lane reservations', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 1, resources: { cpuCores: 1, memoryBytes: 8 * GIB } });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);
  const mem = memory({ availableBytes: 6 * GIB }); // 6 - 8 < the 2 GiB reserve
  const denied = await poll(state, head, { ext: 1, mem });
  assert.equal(denied.reason, 'memory-admission');
  assert.equal(readResourceSkipState(state).futile, 'memory');
  assert.equal((await poll(state, small, { ext: 1, mem })).started, true, 'a 1 GiB ticket fits and backfills');
  assert.match(readLog(state), /reservation=futile futileCause=memory/);
});

test('when external load drops the head starts, ahead of tickets that queued after it', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  const first = ticket('first');
  const later = ticket('later');
  await enqueue(state, head);
  await poll(state, head, { ext: 7 });
  await enqueue(state, first);
  assert.equal((await poll(state, first, { ext: 7 })).started, true);
  await enqueue(state, later);

  assert.equal((await poll(state, head, { ext: 2 })).started, true, 'the head starts as soon as external load allows');
  assert.equal(readResourceSkipState(state), null);
  const queueIds = fs.readdirSync(paths(state).queue).filter((f) => f.includes('later') || f.includes('head'));
  assert.ok(queueIds.every((f) => !f.includes('head')), 'head left the queue');
  assert.ok(queueIds.some((f) => f.includes('later')), 'the later ticket is still waiting behind it');
});

// Codex review of BRAIN-418: futility must mean "denied even if every lane lease drained", by the live predicates.
const observed = (cores, memoryBytes) => ({ observedCpuCores: cores, observedAt: Date.now(), observedMemoryBytes: memoryBytes });

test('P1 memory: RAM the running lanes occupy is handed back before judging, so a head that fits once they drain is not futile', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 1, resources: { cpuCores: 1, memoryBytes: 8 * GIB } });
  await enqueue(state, head);
  holdLease(state, 'lane-a', 1, observed(0.1, 3 * GIB));
  holdLease(state, 'lane-b', 1, observed(0.1, 3 * GIB));
  const denied = await poll(state, head, { ext: 0.2, mem: memory({ availableBytes: 6 * GIB }) }); // 6 + 6 - 8 >= 2 reserve once drained
  assert.equal(denied.reason, 'memory-admission');
  assert.equal(readResourceSkipState(state)?.futile, undefined);
});

test('P1 idle exemption: a head the exemption would start on a drained broker is not futile', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 5 }); // BRAIN-452: 5 is the heaviest weight capacity 10 allows
  await enqueue(state, head);
  holdLease(state, 'lane-a', 1, observed(0.1));
  // external 5 + head 5 = 10 > 9 but within the 1-core overshoot the idle exemption grants
  const denied = await poll(state, head, { ext: 5.1, cfg: { ...cfg, resourceIdleOvershootCores: 1 } });
  assert.equal(denied.cpuReason, 'projected-over-budget');
  assert.equal(readResourceSkipState(state).futile, undefined);
});

test('P2 attribution: a lane with no fresh CPU observation is indistinguishable from external load, so no futility', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  holdLease(state, 'unobserved', 1);
  const denied = await poll(state, head, { ext: 7 }); // 7 "external" + 4 > 9, but part of the 7 may be the lane
  assert.equal(denied.cpuReason, 'projected-over-budget');
  assert.equal(readResourceSkipState(state).futile, undefined);
});

test('P2 v2: a futile head drops its persisted reservation so the next dormant owner is promoted', async () => {
  const { state } = freshEnv();
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: Date.now() });
  fenceLegacyQueue(state, 'test');
  const rec = (seq) => ({ reason: 'resource', skipsCharged: 3, reserved: true, reservationSeq: seq, inScope: true, behindConflict: false, deniedAt: Date.now(), budget: 9, externalBusy: 7 });
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: { a: { resource: rec(10) }, b: { resource: rec(11) } } });
  const a = ticket('a', { weight: 4 });
  await enqueue(state, a);
  await enqueue(state, ticket('b', { weight: 4 }));
  const owner = () => {
    const sched = resolveScheduler(state, { log: false });
    return effectiveView(listQueue(state), Date.now(), cfg, fairnessStore(state, sched.tickets)).ownerId;
  };
  assert.equal(owner(), 'a');
  await poll(state, a, { ext: 7 }); // futile: 7 + 4 > 9 with nothing running
  const stored = JSON.parse(fs.readFileSync(paths(state).fairness, 'utf8')).tickets.a.resource;
  assert.equal(stored.reserved, false);
  assert.equal(stored.reservationSeq, undefined);
  assert.equal(owner(), 'b', 'B\'s dormant reservation becomes the active one');
});

test('P1 expiry: a futile verdict not recomputed (cooldown) stops excusing backfill, so skips count again and the head starts', async () => {
  const realNow = Date.now;
  let t = 1_700_000_000_000;
  Date.now = () => t;
  try {
    const { state } = freshEnv();
    const head = ticket('head', { weight: 4 });
    await enqueue(state, head);
    await poll(state, head, { ext: 7 });
    assert.equal(readResourceSkipState(state).futile, 'cpu');

    // fresh verdict: backfill is free
    const first = ticket('s0');
    await enqueue(state, first);
    assert.equal((await poll(state, first, { ext: 7 })).started, true);
    removeLease(state, 's0');
    assert.equal(readResourceSkipState(state).count, 0);

    // external load drops, but the head only ever polls inside cooldown, so the verdict is never recomputed
    t += 3 * cfg.sampleMs; // older than 2 x the sample interval
    for (let i = 1; i <= 3; i += 1) {
      const s = ticket(`s${i}`);
      await enqueue(state, s);
      assert.equal((await poll(state, s, { ext: 7 })).started, true, `backfill ${i} fits the allowance`);
      removeLease(state, s.id);
    }
    assert.deepEqual({ count: readResourceSkipState(state).count, reserved: readResourceSkipState(state).reserved }, { count: 3, reserved: true }, 'skips count again and the reservation latches');
    const extra = ticket('extra');
    await enqueue(state, extra);
    assert.equal((await poll(state, extra, { ext: 0 })).reason, 'not-head', 'the reservation holds against further backfill');
    assert.equal((await poll(state, head, { ext: 0 })).started, true, 'the head starts');
  } finally {
    Date.now = realNow;
  }
});
