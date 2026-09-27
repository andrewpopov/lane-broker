import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { readLease } from '../src/lease.js';
import { writeCancelMarkerFile, isExpired } from '../src/state.js';

/**
 * BRAIN-320 S1d: `tryStart` expires a queued ticket carrying a (now-passed)
 * `startDeadline` -- decided under the same admission lock, right after
 * `position` is resolved, so a ticket blocked behind someone else (a
 * conflict, or simply not the head) still expires on schedule instead of
 * only ever being checked once it would otherwise have been selected. Same
 * selection-level, timing-free shape as scheduler-cancel.test.js: drive
 * `tryStart` directly rather than racing real subprocesses/sleeps.
 */

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 8, ...overrides };
}

function baseTicket(id, overrides = {}) {
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

test('a queued ticket past its startDeadline expires: no lease, reason queue-timeout, expiry marker written', async () => {
  const { state } = freshEnv();
  const ticket = baseTicket('will-expire', { startDeadline: Date.now() - 1000 });
  await enqueue(state, ticket);

  const result = await tryStart(state, ticket, baseCfg());
  assert.equal(result.started, false);
  assert.equal(result.reason, 'queue-timeout');

  assert.equal(readLease(state, ticket.id), null, 'an expired ticket must never get a lease written');
  const stillQueued = listQueue(state).some((t) => t && t.id === ticket.id);
  assert.equal(stillQueued, true, 'tryStart must leave the ticket queued -- dequeue/finalize is the supervisor poll loop\'s job');
  assert.equal(isExpired(state, ticket.id), true, 'a durable expiry marker must be written');
});

test('a NON-HEAD ticket queued behind an unrelated, still-queued head still expires on schedule', async () => {
  const { state } = freshEnv();
  // `head` stays queued (never started) at position 0, so `behind` is
  // genuinely the literal NON-head at position 1 when tryStart evaluates it
  // -- unlike a ticket that lost its FIFO position only because the real
  // head was already admitted and dequeued out from under it.
  const head = baseTicket('still-queued-head', { key: 'r:head' });
  const behind = baseTicket('will-expire-behind', { key: 'r:behind', startDeadline: Date.now() - 1000 });
  await enqueue(state, head);
  await enqueue(state, behind);

  const positions = listQueue(state).map((t) => t.id);
  assert.deepEqual(positions, [head.id, behind.id], 'behind must be the literal non-head entry for this test to prove anything');

  const result = await tryStart(state, behind, baseCfg());
  assert.equal(result.started, false);
  assert.equal(result.reason, 'queue-timeout');
  assert.equal(isExpired(state, behind.id), true);
});

test('a cancelled ticket past its deadline is finalized as cancelled, not queue-timeout', async () => {
  const { state } = freshEnv();
  const ticket = baseTicket('cancelled-and-expired', { startDeadline: Date.now() - 1000 });
  await enqueue(state, ticket);
  writeCancelMarkerFile(state, ticket.id);

  const result = await tryStart(state, ticket, baseCfg());
  assert.equal(result.started, false);
  assert.notEqual(result.reason, 'queue-timeout');
  assert.equal(result.reason, 'cancelled', 'the cancel marker must take precedence over expiry');
  assert.equal(isExpired(state, ticket.id), false, 'a cancelled ticket must never also be marked expired');
});

test('a ticket without startDeadline never expires, however old its createdAt', async () => {
  const { state } = freshEnv();
  const ticket = baseTicket('no-deadline', { createdAt: Date.now() - 10_000_000 });
  await enqueue(state, ticket);

  const result = await tryStart(state, ticket, baseCfg());
  assert.equal(result.started, true);
  assert.equal(isExpired(state, ticket.id), false);
});
