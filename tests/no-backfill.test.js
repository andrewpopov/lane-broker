import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { readLease, writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';

/**
 * BRAIN-249 part 2 CHANGED THE BEHAVIOR THIS FILE PINS. It used to assert
 * strict "no backfill" for a capacity-blocked head — exactly the shape that
 * turned out to be a live incident: `savoro:prepush` (weight 8) against
 * `capacity: 8` never fits alongside ANY other running lane, so under the
 * old strict rule it blocked every ticket behind it, forever, with no way
 * out short of the whole machine going idle first. Andrew decided a
 * capacity-blocked head should get the same bounded backfill a
 * conflict-blocked head already gets (see conflict-skip.test.js): a later,
 * non-conflicting ticket that DOES fit may start ahead of it, up to
 * `conflictSkipLimit` times, after which backfill is refused and the head
 * blocks strict-FIFO style until it fits on its own. Unlike the conflict
 * path, there is deliberately NO time-based grace/resume here (see
 * src/scheduler.js's resolveCapacityBlock) — capacity backfill consumes the
 * very capacity the head needs, so refusing it forever is fine: running
 * work drains on its own and the head eventually fits.
 *
 * This is still a SELECTION test, not an end-to-end one — see
 * tests/conflict-skip.test.js's header for why real subprocess scheduling
 * is a bad fit for this kind of test; the end-to-end path is independently
 * covered by scheduler-integration.test.js's "capacity holds" test.
 */

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, ...overrides };
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

function heldLease(id, key, weight = 1) {
  return {
    id,
    key,
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    weight,
    state: LEASE_STATE.RUNNING,
  };
}

test('a still-running lease denies a head that does not fit capacity, and the head starts once it is released', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();

  // Capacity 2: the heavy (weight 2) ticket uses the whole thing once
  // running, so a weight-1 ticket enqueued after it (and now the sole,
  // literal head of the remaining queue) does not fit under capacity
  // either. This is the base case BRAIN-249 part 2 does NOT change: a
  // ticket that is itself the head, with nothing behind it to backfill,
  // is denied exactly as before.
  const heavy = baseTicket('heavy', { key: 'r:heavy', weight: 2 });
  const light = baseTicket('light', { key: 'r:light', weight: 1 });
  await enqueue(state, heavy);
  await enqueue(state, light);

  const heavyResult = await tryStart(state, heavy, globalCfg);
  assert.equal(heavyResult.started, true, 'the heavy ticket is the head and fits capacity, so it starts');

  const lightResult = await tryStart(state, light, globalCfg);
  assert.equal(lightResult.started, false, 'light is now the sole, literal head and still does not fit capacity');
  assert.equal(lightResult.reason, 'capacity');

  const heavyLease = readLease(state, heavy.id);
  assert.ok(heavyLease && heavyLease.state === LEASE_STATE.RUNNING, 'heavy should hold a running lease');
  removeLease(state, heavy.id);

  const lightResultAfterRelease = await tryStart(state, light, globalCfg);
  assert.equal(lightResultAfterRelease.started, true, 'light must start only after heavy releases and capacity is free');
});

test('BRAIN-249 part 2: a capacity-blocked head backfills a later, non-conflicting, lighter ticket up to conflictSkipLimit times, then refuses, and eventually starts once the machine drains', async () => {
  const { state } = freshEnv();
  const limit = 2;
  const globalCfg = baseCfg({ capacity: 8, conflictSkipLimit: limit });

  // The live incident's shape: a running weight-2 lane leaves no room for a
  // weight-8 head (2 + 8 = 10 > 8) -- a weight-8 lane only ever fits on a
  // completely empty machine.
  writeLease(state, heldLease('other-holder', 'rouge:sim', 2));

  const head = baseTicket('savoro-prepush', { key: 'savoro:prepush', weight: 8 });
  await enqueue(state, head);

  const headResult = await tryStart(state, head, globalCfg);
  assert.equal(headResult.started, false);
  assert.equal(headResult.reason, 'capacity', 'the weight-8 head does not fit alongside the running weight-2 lease');

  // Backfill up to `limit` times: each round enqueues a fresh, lighter,
  // non-conflicting ticket behind the still-queued head, proves it starts
  // (consuming one unit of the capacity-skip allowance), then releases it
  // so the next round has capacity room again -- isolating "the allowance
  // is exhausted" from "capacity happens to be full".
  for (let i = 0; i < limit; i += 1) {
    const later = baseTicket(`later-${i}`, { key: `savoro:other-${i}`, weight: 1 });
    await enqueue(state, later);
    const laterResult = await tryStart(state, later, globalCfg);
    assert.equal(
      laterResult.started,
      true,
      `round ${i}: skip count ${i} < limit ${limit}, so a lighter, non-conflicting, fitting ticket may backfill past the capacity-blocked head`,
    );
    removeLease(state, later.id);
  }

  // The allowance is now exhausted: even a ticket that would itself fit and
  // does not conflict is refused -- proving the refusal actually happens,
  // not merely that the allowance has a ceiling in principle.
  const exhaustedTicket = baseTicket('later-final', { key: 'savoro:other-final', weight: 1 });
  await enqueue(state, exhaustedTicket);
  const exhaustedResult = await tryStart(state, exhaustedTicket, globalCfg);
  assert.equal(exhaustedResult.started, false, 'once the capacity-skip allowance is exhausted, backfill is refused even for a ticket that would fit');
  assert.equal(exhaustedResult.reason, 'not-head');

  const headStillBlocked = await tryStart(state, head, globalCfg);
  assert.equal(headStillBlocked.started, false);
  assert.equal(headStillBlocked.reason, 'capacity', 'the head itself is still denied by the real capacity check, same as always');

  // Termination -- the assertion that matters most, since it is what stops
  // this fix from trading one starvation for another: once the running
  // lease that made the head not fit is released, the head DOES start.
  // Unlike the conflict path, capacity backfill is never resumed on a
  // timer; it simply stops mattering once the machine actually drains.
  removeLease(state, 'other-holder');
  const headStarts = await tryStart(state, head, globalCfg);
  assert.equal(headStarts.started, true, 'once running work drains and the head fits, it starts -- exhaustion never becomes permanent starvation');
});
