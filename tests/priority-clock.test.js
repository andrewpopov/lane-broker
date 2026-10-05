import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { paths } from '../src/state.js';
import { enqueue, listQueue } from '../src/scheduler.js';
import { readHwm, effectiveNow, advanceHwm, stampPriorityOrigin } from '../src/priority-clock.js';
import { waitedMs } from '../src/priority.js';

// BRAIN-380 slice 1: the priority clock (persisted hwm, nowEff = max(wall, hwm), origin stamped under the lock).

const MIN = 60_000;
const T0 = 1_700_000_000_000;

/** Run `fn` with Date.now() under the test's control; `clock.now` is the scripted wall time. */
async function withClock(start, fn) {
  const clock = { now: start };
  const real = Date.now;
  Date.now = () => clock.now;
  try {
    return await fn(clock);
  } finally {
    Date.now = real;
  }
}

const ticket = (id, extra = {}) => ({ id, key: `r:${id}`, weight: 1, supervisorPid: process.pid, supervisorStart: null, ...extra });

test('hwm only moves forward, is persisted in the state root, and nowEff is max(wall, hwm)', () => {
  const { state } = freshEnv();
  assert.equal(readHwm(state), 0);
  assert.equal(advanceHwm(state, T0), T0);
  assert.equal(advanceHwm(state, T0 - 30 * MIN), T0, 'a wall clock behind the mark returns the mark and does not lower it');
  assert.equal(readHwm(state), T0);
  assert.equal(JSON.parse(fs.readFileSync(paths(state).hwm, 'utf8')).hwm, T0);
  assert.equal(effectiveNow(state, T0 - MIN), T0);
  assert.equal(effectiveNow(state, T0 + MIN), T0 + MIN);
  assert.equal(readHwm(state), T0, 'effectiveNow is read-only: it never writes the mark');
});

test('clock rollback: an existing ticket keeps its age and a ticket created during the rollback starts at zero', async () => {
  const { state } = freshEnv();
  await withClock(T0, async (clock) => {
    const originA = await stampPriorityOrigin(state);
    await enqueue(state, ticket('a', { prioOriginAt: originA }));
    clock.now = T0 + 5 * MIN;
    advanceHwm(state); // any locked evaluation
    const queuedA = () => listQueue(state).find((t) => t.id === 'a');
    assert.equal(waitedMs(queuedA(), effectiveNow(state)), 5 * MIN);

    clock.now = T0 - 25 * MIN; // wall clock steps back 30 minutes
    assert.equal(waitedMs(queuedA(), effectiveNow(state)), 5 * MIN, 'the existing ticket keeps the age it had accrued');
    const originB = await stampPriorityOrigin(state);
    assert.equal(originB, T0 + 5 * MIN, 'a ticket created during the rollback is stamped with the mark, not the wall clock');
    await enqueue(state, ticket('b', { prioOriginAt: originB }));
    const queuedB = listQueue(state).find((t) => t.id === 'b');
    assert.equal(waitedMs(queuedB, effectiveNow(state)), 0, 'zero age during the rollback');

    clock.now = T0 + 5 * MIN + 2 * MIN; // wall passes the mark again
    assert.equal(waitedMs(queuedB, effectiveNow(state)), 2 * MIN, 'aging resumes from the mark');
    assert.equal(waitedMs(queuedA(), effectiveNow(state)), 7 * MIN);
  });
});

test('a rollback between ticket creation and enqueue does not give the ticket negative or inflated age', async () => {
  const { state } = freshEnv();
  await withClock(T0, async (clock) => {
    const origin = await stampPriorityOrigin(state);
    clock.now = T0 - 10 * MIN;
    const record = await enqueue(state, ticket('a', { prioOriginAt: origin }));
    assert.equal(record.prioOriginAt, T0, 'the stamped origin survives the rollback');
    assert.equal(waitedMs(record, effectiveNow(state)), 0);
    assert.equal(record.createdAt, T0 - 10 * MIN, 'wall-clock createdAt is unchanged by the priority clock');
  });
});

test('an origin-less arrival during a rollback is stamped with nowEff, never createdAt', async () => {
  const { state } = freshEnv();
  await withClock(T0, async (clock) => {
    advanceHwm(state);
    clock.now = T0 - 20 * MIN;
    const record = await enqueue(state, ticket('legacy', { createdAt: T0 - 3 * 60 * MIN }));
    assert.equal(record.prioOriginAt, T0, 'a ticket seen without an origin starts at the mark');
    assert.equal(waitedMs(record, effectiveNow(state)), 0, 'even though its createdAt is hours old');
  });
});

test('an origin ahead of the clock (forged or from a skewed machine) is replaced by nowEff at enqueue', async () => {
  const { state } = freshEnv();
  await withClock(T0, async () => {
    const record = await enqueue(state, ticket('future', { prioOriginAt: T0 + 60 * MIN }));
    assert.equal(record.prioOriginAt, T0);
  });
});

test('concurrent creations and enqueues serialize on the broker lock: the mark never goes backwards and no origin passes it', async () => {
  const { state } = freshEnv();
  let wall = T0;
  const real = Date.now;
  Date.now = () => (wall += 7); // every read is later than the last, so a lost update would lower the mark
  try {
    const stamped = await Promise.all(Array.from({ length: 12 }, () => stampPriorityOrigin(state)));
    const enqueued = await Promise.all(Array.from({ length: 6 }, (_, i) => enqueue(state, ticket(`c${i}`))));
    const origins = [...stamped, ...enqueued.map((r) => r.prioOriginAt)];
    assert.equal(new Set(stamped).size, stamped.length, 'each creation advanced the mark to a distinct instant');
    assert.ok(origins.every((o) => o <= readHwm(state)), 'no origin is ahead of the persisted mark');
    assert.equal(readHwm(state), Math.max(...origins), 'the mark equals the latest instant any transaction observed');
  } finally {
    Date.now = real;
  }
});

test('enqueue persists the priority fields: requested, admitted (== requested in slice 1), origin, schedVersion 2', async () => {
  const { state } = freshEnv();
  const high = await enqueue(state, ticket('h', { priorityRequested: 'high', priorityAdmitted: 'low' }));
  assert.equal(high.priorityRequested, 'high');
  assert.equal(high.priorityAdmitted, 'high', 'nothing demotes in this slice, whatever the caller claimed');
  assert.equal(high.schedVersion, 2);
  assert.ok(Number.isFinite(high.prioOriginAt));
  const plain = await enqueue(state, ticket('m'));
  assert.equal(plain.priorityRequested, 'medium');
  assert.equal(plain.priorityAdmitted, 'medium');
  const bogus = await enqueue(state, ticket('x', { priorityRequested: 'urgent' }));
  assert.equal(bogus.priorityRequested, 'medium', 'an unknown tier is medium');
  assert.deepEqual(listQueue(state).map((t) => t.id), ['h', 'm', 'x'], 'queue order stays FIFO by seq');
});
