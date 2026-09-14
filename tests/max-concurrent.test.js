import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeRepoConfig, writeGlobalConfig } from './helpers.js';
import { enqueue, tryStart, blockedBy } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, resolveTicketConfig, ConfigError } from '../src/config.js';
import { collectStatus } from '../src/status.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, atomicWriteJson, paths } from '../src/state.js';

/**
 * BRAIN-255: per-lane `maxConcurrent` relaxes the previously-unconditional
 * same-key mutual exclusion (see src/scheduler.js's blockedBy) so a worker
 * pool lane can run N at once. Default is 1 -- today's exact behaviour --
 * for any lane that doesn't declare it; that regression (the first test
 * below) must keep passing against the pre-existing suite unmodified.
 */

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, ...overrides };
}

function baseTicket(id, overrides = {}) {
  return {
    id,
    key: 'r:default',
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

function heldLease(id, key, overrides = {}) {
  return {
    id,
    key,
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    weight: 1,
    state: LEASE_STATE.RUNNING,
    ...overrides,
  };
}

test('a lane with no maxConcurrent admits exactly one at a time (the key regression)', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  writeLease(state, heldLease('holder', 'fleet:default'));

  const second = baseTicket('second', { key: 'fleet:default' });
  await enqueue(state, second);

  const result = await tryStart(state, second, globalCfg);
  assert.equal(result.started, false, 'no maxConcurrent declared: the pre-BRAIN-255 same-key exclusion must still apply');
  assert.equal(result.reason, 'conflict');
});

test('maxConcurrent: 3 admits three same-key tickets and queues the fourth', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  const key = 'fleet:sim';

  const first = baseTicket('t1', { key, maxConcurrent: 3 });
  const second = baseTicket('t2', { key, maxConcurrent: 3 });
  const third = baseTicket('t3', { key, maxConcurrent: 3 });
  const fourth = baseTicket('t4', { key, maxConcurrent: 3 });
  await enqueue(state, first);
  await enqueue(state, second);
  await enqueue(state, third);
  await enqueue(state, fourth);

  const r1 = await tryStart(state, first, globalCfg);
  assert.equal(r1.started, true);
  const r2 = await tryStart(state, second, globalCfg);
  assert.equal(r2.started, true);
  const r3 = await tryStart(state, third, globalCfg);
  assert.equal(r3.started, true, 'a ceiling of 3 must admit a third simultaneous same-key holder');

  const r4 = await tryStart(state, fourth, globalCfg);
  assert.equal(r4.started, false, 'the fourth same-key ticket must be queued once the ceiling is reached');
  assert.equal(r4.reason, 'conflict');
});

test('the fourth starts once one of the three releases', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  const key = 'fleet:sim';

  const first = baseTicket('t1', { key, maxConcurrent: 3 });
  const second = baseTicket('t2', { key, maxConcurrent: 3 });
  const third = baseTicket('t3', { key, maxConcurrent: 3 });
  const fourth = baseTicket('t4', { key, maxConcurrent: 3 });
  await enqueue(state, first);
  await enqueue(state, second);
  await enqueue(state, third);
  await enqueue(state, fourth);

  await tryStart(state, first, globalCfg);
  await tryStart(state, second, globalCfg);
  const r3 = await tryStart(state, third, globalCfg);
  assert.equal(r3.started, true);

  let r4 = await tryStart(state, fourth, globalCfg);
  assert.equal(r4.started, false);

  // The lease objects live under `t1`'s id -- releasing it frees one slot.
  const { removeLease } = await import('../src/lease.js');
  removeLease(state, 't1');

  r4 = await tryStart(state, fourth, globalCfg);
  assert.equal(r4.started, true, 'a freed slot must let the queued fourth ticket start');
});

test('a maxConcurrent: 3 lane that also declares a conflict with another lane is still fully blocked by a held lease on that other lane', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  writeLease(state, heldLease('other-holder', 'fleet:other'));

  const ticket = baseTicket('candidate', { key: 'fleet:sim', maxConcurrent: 3, conflicts: ['fleet:other'] });
  await enqueue(state, ticket);

  const result = await tryStart(state, ticket, globalCfg);
  assert.equal(result.started, false, 'a declared conflicts entry is absolute regardless of maxConcurrent');
  assert.equal(result.reason, 'conflict');
  assert.equal(result.key, 'fleet:other');
});

test('maxConcurrent does not bypass CPU/memory admission: a critical memory reading still denies a slot within the ceiling', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg({ schedulerMode: 'active', capacity: 100 });
  const key = 'fleet:sim';

  // Two already running, well within the ceiling of 4.
  writeLease(state, heldLease('h1', key));
  writeLease(state, heldLease('h2', key));

  const ticket = baseTicket('t3', { key, maxConcurrent: 4 });
  await enqueue(state, ticket);

  const memoryReader = () => ({ macPressure: 'critical', availableBytes: 0 });
  const result = await tryStart(state, ticket, globalCfg, () => 0, undefined, undefined, memoryReader);

  assert.equal(result.started, false, 'a maxConcurrent ceiling must never bypass the memory admission brake');
  assert.equal(result.reason, 'memory-critical', 'assert on the actual admission decision, not merely a count');
});

test('maxConcurrent does not bypass capacity: a budget fitting only two runs two and queues the rest', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg({ capacity: 2 });
  const key = 'fleet:sim';

  const t1 = baseTicket('t1', { key, weight: 1, maxConcurrent: 4 });
  const t2 = baseTicket('t2', { key, weight: 1, maxConcurrent: 4 });
  const t3 = baseTicket('t3', { key, weight: 1, maxConcurrent: 4 });
  const t4 = baseTicket('t4', { key, weight: 1, maxConcurrent: 4 });
  await enqueue(state, t1);
  await enqueue(state, t2);
  await enqueue(state, t3);
  await enqueue(state, t4);

  assert.equal((await tryStart(state, t1, globalCfg)).started, true);
  assert.equal((await tryStart(state, t2, globalCfg)).started, true);

  const r3 = await tryStart(state, t3, globalCfg);
  assert.equal(r3.started, false, 'a maxConcurrent: 4 ceiling must never bypass the capacity budget');
  assert.equal(r3.reason, 'capacity', 'assert on the actual admission decision, not merely a count');

  const r4 = await tryStart(state, t4, globalCfg);
  assert.equal(r4.started, false);
  assert.equal(r4.reason, 'not-head', 'capacity never lets a later ticket skip ahead, same as before BRAIN-255');
});

test('invalid maxConcurrent values are rejected at config load with a message naming the lane', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  for (const bad of [0, -1, 2.5, '3', null]) {
    writeRepoConfig(repoDir, { version: 1, lanes: { fleet: { weight: 1, maxConcurrent: bad } } });
    assert.throws(
      () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'fleet' }),
      (err) => err instanceof ConfigError && /lane "fleet"\.maxConcurrent/.test(err.message),
      `expected a ConfigError naming lane "fleet" for maxConcurrent=${JSON.stringify(bad)}`,
    );
  }
});

test('a head blocked purely by its own ceiling still participates in the bounded skip/grace path and does not stall the queue behind it', async () => {
  const { state } = freshEnv();
  const limit = 3;
  const globalCfg = baseCfg({ conflictSkipLimit: limit });
  const key = 'fleet:sim';

  // Ceiling of 1 (default) already reached by one held same-key lease.
  writeLease(state, heldLease('holder', key));

  const head = baseTicket('head', { key }); // no maxConcurrent declared -> ceiling 1, blocked by holder
  await enqueue(state, head);

  const { removeLease } = await import('../src/lease.js');
  for (let i = 0; i < limit; i += 1) {
    const other = baseTicket(`other-${i}`, { key: `other-key-${i}` }); // conflicts with nothing
    await enqueue(state, other);

    const headResult = await tryStart(state, head, globalCfg);
    assert.equal(headResult.started, false);
    assert.equal(headResult.reason, 'not-head', `cycle ${i}: head is skipped, not specially blocked, while under the skip limit`);

    const otherResult = await tryStart(state, other, globalCfg);
    assert.equal(otherResult.started, true, `cycle ${i}: skip count ${i} < limit ${limit}, so the non-conflicting ticket may still jump ahead`);

    removeLease(state, `other-${i}`); // that cycle's ticket "finishes"
  }

  const headResult = await tryStart(state, head, globalCfg);
  assert.equal(headResult.started, false);
  assert.equal(headResult.reason, 'conflict', 'once the skip allowance for the ceiling-blocked head is exhausted, it is reported as conflict-blocked');
});

test('status.js reports a ceiling-blocked head consistently with what the scheduler decided', async () => {
  const { home, state } = freshEnv();
  const limit = 1;
  const globalCfg = baseCfg({ conflictSkipLimit: limit });
  writeGlobalConfig(home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 5000, conflictSkipLimit: limit });
  const key = 'fleet:sim';

  writeLease(state, heldLease('holder', key));
  const head = baseTicket('head', { key });
  await enqueue(state, head);

  // Exhaust the skip allowance directly, same technique as head-block-grace.test.js.
  atomicWriteJson(paths(state).conflictSkipState, {
    headId: head.id,
    count: limit,
    blockedSince: Date.now(),
    loggedPhase: 'exhausted',
  });

  const schedResult = await tryStart(state, head, globalCfg);
  assert.equal(schedResult.started, false);
  assert.equal(schedResult.reason, 'conflict');
  assert.equal(schedResult.with, 'holder');

  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    const status = await collectStatus();
    assert.ok(status.headBlock, 'status must report the same stalled head the scheduler is refusing to start');
    assert.equal(status.headBlock.headId, 'head');
    assert.equal(status.headBlock.blockingLeaseId, 'holder');
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
  }
});

test('status.js does not report a false stall for a head still within its own maxConcurrent ceiling', async () => {
  // Distinguishes blockedBy's ceiling-aware check from a naive same-key
  // match: one held same-key lease against a ceiling of 2 must NOT block —
  // if status.js applied a plain "same key" test instead of the scheduler's
  // own blockedBy (BRAIN-255), it would report this head as stalled even
  // though tryStart would happily start it.
  const { home, state } = freshEnv();
  const limit = 1;
  const globalCfg = baseCfg({ conflictSkipLimit: limit });
  writeGlobalConfig(home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 5000, conflictSkipLimit: limit });
  const key = 'fleet:pool';

  writeLease(state, heldLease('holder', key));
  const head = baseTicket('head', { key, maxConcurrent: 2 });
  await enqueue(state, head);

  const schedResult = await tryStart(state, head, globalCfg);
  assert.equal(schedResult.started, true, 'one held same-key lease is within a ceiling of 2, so the scheduler must start it');

  // Re-queue a SECOND ticket of the same key/ceiling behind the now-started
  // one, and pre-write a stale skip-state record naming it as if it had
  // already exhausted its skip allowance — the only way to exercise
  // computeHeadBlock's own logic (not just tryStart's) while a same-key
  // head genuinely still fits under its ceiling.
  const second = baseTicket('second-in-pool', { key, maxConcurrent: 3 });
  await enqueue(state, second);
  atomicWriteJson(paths(state).conflictSkipState, {
    headId: second.id,
    count: limit,
    blockedSince: Date.now(),
    loggedPhase: 'exhausted',
  });

  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    const status = await collectStatus();
    assert.equal(status.headBlock, null, 'status must not describe a head as stalled that the scheduler itself just started');
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
  }
});

test('blockedBy: a same-key lease within the ceiling does not block', () => {
  const held = [heldLease('h1', 'fleet:sim')];
  const ticket = baseTicket('candidate', { key: 'fleet:sim', maxConcurrent: 2 });
  assert.equal(blockedBy(held, ticket), null);
});

test('blockedBy: same-key leases at the ceiling block, returning a held lease', () => {
  const held = [heldLease('h1', 'fleet:sim'), heldLease('h2', 'fleet:sim')];
  const ticket = baseTicket('candidate', { key: 'fleet:sim', maxConcurrent: 2 });
  const blocker = blockedBy(held, ticket);
  assert.ok(blocker && blocker.key === 'fleet:sim');
});

test('blockedBy: a declared conflict blocks unconditionally regardless of maxConcurrent', () => {
  const held = [heldLease('h1', 'fleet:other')];
  const ticket = baseTicket('candidate', { key: 'fleet:sim', maxConcurrent: 10, conflicts: ['fleet:other'] });
  const blocker = blockedBy(held, ticket);
  assert.ok(blocker && blocker.key === 'fleet:other');
});
