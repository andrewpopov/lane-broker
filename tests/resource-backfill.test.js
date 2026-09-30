import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, readResourceSkipState } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, removeLease, readLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';

/**
 * BRAIN-346: bounded backfill past a FIFO head denied only by `projected-over-budget`, a latched
 * reservation once the allowance is used, and a bounded idle exemption for the reserved head.
 * Selection-level tests in the style of no-backfill.test.js / conflict-skip.test.js: real
 * tryStart against a temp LANE_BROKER state dir, with the CPU sampler and memory reader injected.
 *
 * The fixture machine: 10 cores, reserve 1, 100% -> CPU budget 9. Ambient load 5.4 makes a
 * weight-4 head project 9.4 > 9 (denied) while a weight-1 ticket projects 6.4 (fits).
 */

const GIB = 1024 ** 3;

function baseCfg(overrides = {}) {
  return {
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
    ...overrides,
  };
}

const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = (overrides = {}) => () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test', ...overrides });

function ticket(id, overrides = {}) {
  return {
    id,
    key: `r:${id}`,
    weight: 1,
    cwd: process.cwd(),
    cmd: ['true'],
    supervisorPid: process.pid,
    supervisorStart: null,
    logPath: '/dev/null',
    resultPath: '/dev/null',
    ...overrides,
  };
}

function heldLease(id, key, weight = 1) {
  return { id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight, state: LEASE_STATE.RUNNING };
}

function poll(state, t, cfg, { ext = 5.4, mem = memory(), write } = {}) {
  return tryStart(state, t, cfg, undefined, sampler(ext), undefined, mem, write);
}

function seedRecord(state, headId, overrides = {}) {
  atomicWriteJson(paths(state).resourceSkipState, { headId, count: 0, reserved: false, deniedAt: Date.now(), budget: 9, externalBusy: 5.4, ...overrides });
}

const readLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

test('1: a resource-denied head lets a later small ticket start, and the head still evaluates and starts when it fits', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = ticket('head', { weight: 4 });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);

  const beforeRecord = await poll(state, small, cfg);
  assert.equal(beforeRecord.reason, 'not-head', 'no record yet: strict FIFO, the small ticket waits');

  const headDenied = await poll(state, head, cfg);
  assert.equal(headDenied.started, false);
  assert.equal(headDenied.reason, 'cpu-admission');
  assert.equal(headDenied.cpuReason, 'projected-over-budget');
  assert.equal(readResourceSkipState(state)?.headId, 'head', 'the head\'s own denial records the allowance');

  const smallResult = await poll(state, small, cfg);
  assert.equal(smallResult.started, true, 'a resource-denied head may be backfilled past');
  assert.equal(readResourceSkipState(state).count, 1);
  assert.match(readLog(state), /lane-broker-head-block event=resource-backfill-start skipPast=head count=1/);

  const headAgain = await poll(state, head, cfg);
  assert.equal(headAgain.reason, 'cpu-admission', 'with a valid record the head runs real admission instead of returning not-head');

  removeLease(state, 'small');
  const headStarts = await poll(state, head, cfg, { ext: 2 });
  assert.equal(headStarts.started, true, 'the head starts the moment it fits');
  assert.equal(readResourceSkipState(state), null, 'starting clears the record');
});

test('2: the walk skips a ticket too big for the headroom and picks a smaller one behind it', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = ticket('head', { weight: 4 });
  const big = ticket('big', { weight: 4 }); // 5.4 + 4 = 9.4 > 9: does not fit
  const small = ticket('small', { weight: 1 });
  await enqueue(state, head);
  await enqueue(state, big);
  await enqueue(state, small);
  await poll(state, head, cfg);

  assert.equal((await poll(state, big, cfg)).reason, 'not-head', 'the too-big ticket is not selected');
  assert.equal((await poll(state, small, cfg)).started, true, 'the smaller ticket behind it is');

  // With ONLY a too-big ticket behind the head nothing fits, so nothing may start (a smallest-first
  // pick alone would happily choose it).
  const { state: state2 } = freshEnv();
  const head2 = ticket('head2', { weight: 4 });
  const onlyBig = ticket('only-big', { weight: 4 });
  await enqueue(state2, head2);
  await enqueue(state2, onlyBig);
  await poll(state2, head2, cfg);
  // The refusal must come from selection ('not-head'), not from the unchanged fresh admission
  // ('cpu-admission') that would deny it anyway; only the former proves the fit filter ran.
  const onlyBigResult = await poll(state2, onlyBig, cfg);
  assert.equal(onlyBigResult.started, false);
  assert.equal(onlyBigResult.reason, 'not-head', 'nothing fits the headroom, so the ticket is never even selected');
});

test('3: a missing, wrong-head or malformed record means no backfill', async () => {
  const cfg = baseCfg();
  const cases = {
    missing: null,
    'wrong head': { headId: 'some-other-head' },
    'malformed count': { count: 'x' },
    'malformed budget': { budget: null },
    'reserved not boolean': { reserved: 'false' },
  };
  for (const [name, overrides] of Object.entries(cases)) {
    const { state } = freshEnv();
    const head = ticket('head', { weight: 4 });
    const small = ticket('small');
    await enqueue(state, head);
    await enqueue(state, small);
    if (overrides) seedRecord(state, 'head', overrides);
    const result = await poll(state, small, cfg);
    assert.equal(result.started, false, `${name}: no valid record, no backfill`);
    assert.equal(result.reason, 'not-head', name);
  }
});

test('4: after resourceSkipLimit backfills the reservation latches and a further admissible ticket is refused', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  await poll(state, head, cfg);

  for (let i = 0; i < 3; i += 1) {
    const s = ticket(`s${i}`);
    await enqueue(state, s);
    assert.equal((await poll(state, s, cfg)).started, true, `backfill ${i} is within the allowance`);
    removeLease(state, s.id);
  }
  assert.deepEqual(
    { count: readResourceSkipState(state).count, reserved: readResourceSkipState(state).reserved },
    { count: 3, reserved: true },
    'the third backfill latches the reservation',
  );
  assert.match(readLog(state), /event=resource-reserved headId=head count=3 limit=3/);

  const extra = ticket('extra');
  await enqueue(state, extra);
  const refused = await poll(state, extra, cfg);
  assert.equal(refused.started, false, 'cooldown elapsed and resources fine, yet the reserved head may not be overtaken again');
  assert.equal(refused.reason, 'not-head');
  assert.equal(readLease(state, 'extra'), null);
});

test('5: while reserved, capacity-path backfill is refused too, with the capacity skip count below its limit', async () => {
  const cfg = baseCfg();
  async function scenario(reserved) {
    const { state } = freshEnv();
    writeLease(state, heldLease('holder', 'other:key', 7));
    const head = ticket('head', { weight: 4 }); // 7 + 4 > capacity 10: capacity-blocked
    const small = ticket('small');
    await enqueue(state, head);
    await enqueue(state, small);
    if (reserved) seedRecord(state, 'head', { count: 3, reserved: true });
    return poll(state, small, cfg, { ext: 0 });
  }
  assert.equal((await scenario(false)).started, true, 'control: without a reservation the capacity backfill proceeds');
  const refused = await scenario(true);
  assert.equal(refused.started, false, 'the resource reservation also stops capacity backfill past that head');
  assert.equal(refused.reason, 'not-head');
});

test('6: reserved + idle + over by <= the overshoot: the head starts as resource-idle-exempt', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ resourceIdleOvershootCores: 1 });
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  seedRecord(state, 'head', { count: 3, reserved: true });

  const result = await poll(state, head, cfg); // 5.4 + 4 = 9.4, over 9 by 0.4
  assert.equal(result.started, true);
  assert.match(readLog(state), /current=start:resource-idle-exempt/);
  assert.match(readLog(state), /lane-broker-head-block event=resource-idle-exempt headId=head/);
  assert.equal(readResourceSkipState(state), null);
});

test('6b: the exemption needs the reservation and an idle broker', async () => {
  const cfg = baseCfg({ resourceIdleOvershootCores: 1 });
  const { state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  seedRecord(state, 'head', { count: 1, reserved: false });
  assert.equal((await poll(state, head, cfg)).started, false, 'not reserved: no exemption');

  const { state: busy } = freshEnv();
  await enqueue(busy, head);
  seedRecord(busy, 'head', { count: 3, reserved: true });
  writeLease(busy, heldLease('holder', 'other:key', 1));
  assert.equal((await poll(busy, head, cfg, { ext: 4.6 })).started, false, 'a held lease means the broker is not idle');
});

test('7: reserved + idle + over by more than the overshoot: the head is not started', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ resourceIdleOvershootCores: 1 });
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  seedRecord(state, 'head', { count: 3, reserved: true });

  const result = await poll(state, head, cfg, { ext: 6.5 }); // 10.5, over 9 by 1.5
  assert.equal(result.started, false);
  assert.equal(result.reason, 'cpu-admission');
});

test('8: the exemption never fires when memory also denies (headroom or reservations)', async () => {
  const cfg = baseCfg({ resourceIdleOvershootCores: 1 });
  const readers = {
    'memory-headroom': memory({ availableBytes: 1 * GIB }),
    'memory-reservations': memory({ totalBytes: 3 * GIB }),
  };
  for (const [reason, mem] of Object.entries(readers)) {
    const { state } = freshEnv();
    const head = ticket('head', { weight: 4 });
    await enqueue(state, head);
    seedRecord(state, 'head', { count: 3, reserved: true });
    const result = await poll(state, head, cfg, { mem });
    assert.equal(result.memoryReason, reason, 'fixture sanity: a joint CPU and memory denial');
    assert.equal(result.cpuReason, 'projected-over-budget');
    assert.equal(result.started, false, `${reason}: no exemption on a joint denial`);
  }
});

test('9: fail-closed: a failing skip WRITE refuses the backfill every time', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = ticket('head', { weight: 4 });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);
  seedRecord(state, 'head');
  const failingWrite = () => {
    throw new Error('disk full');
  };
  for (let i = 0; i < 3; i += 1) {
    const result = await poll(state, small, cfg, { write: failingWrite });
    assert.equal(result.started, false, `attempt ${i}: never backfilled while the count cannot be recorded`);
    assert.equal(result.reason, 'not-head');
  }
  assert.equal(readLease(state, 'small'), null);
  assert.equal(readResourceSkipState(state).count, 0, 'the allowance is never reset or advanced by a failed write');
});

test('10: a preloaded active-mode record plus shadow mode means no backfill', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);
  seedRecord(state, 'head');
  const result = await poll(state, small, baseCfg({ schedulerMode: 'shadow' }));
  assert.equal(result.started, false);
  assert.equal(result.reason, 'not-head');
});

test('11: a same-key ticket that would make the head conflicted is not backfilled', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = ticket('head', { key: 'shared:key', weight: 4 });
  const sameKey = ticket('same-key', { key: 'shared:key' });
  const other = ticket('other', { key: 'other:key' });
  await enqueue(state, head);
  await enqueue(state, sameKey);
  await enqueue(state, other);
  seedRecord(state, 'head');

  assert.equal((await poll(state, sameKey, cfg)).started, false, 'starting it would conflict the head');
  assert.equal((await poll(state, other, cfg)).started, true, 'a different-key ticket still backfills');
});

test('12: resourceSkipLimit 0 is strict FIFO: nothing is recorded and nothing backfills', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ resourceSkipLimit: 0 });
  const head = ticket('head', { weight: 4 });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);

  assert.equal((await poll(state, head, cfg)).reason, 'cpu-admission');
  assert.equal(fs.existsSync(paths(state).resourceSkipState), false, 'a zero limit never records an allowance');
  seedRecord(state, 'head');
  assert.equal((await poll(state, small, cfg)).started, false);
});
