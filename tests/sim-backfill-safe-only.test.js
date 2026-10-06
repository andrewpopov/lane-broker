import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, readSkipState, readResourceSkipState } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { listQueue } from '../src/scheduler.js';
import { resolveScheduler, fairnessStore, effectiveView } from '../src/fairness.js';
import { fenceLegacyQueue } from '../src/migrate.js';

/**
 * ROG-2181: behind a non-sim head, a sim-class ticket starts ONLY through BRAIN-355's safe backfill (which
 * proves it cannot delay the head), never through a conflict, capacity or resource skip. A test-class ticket
 * in the same position is the control and keeps today's behaviour. Fixture machine: 10 cores, reserve 1,
 * budget 9, weight capacity 10.
 */

const GIB = 1024 ** 3;
const LIMIT = 3;

const baseCfg = (overrides = {}) => ({
  ...DEFAULT_GLOBAL_CONFIG,
  schedulerMode: 'active',
  capacity: 10,
  loadClose: 1000,
  loadOpen: 900,
  loadOpenSamples: 1,
  cpuAdmissionPercent: 100,
  cpuReserveCores: 1,
  admissionCooldownMs: 0,
  conflictSkipLimit: LIMIT,
  resourceSkipLimit: 3,
  resourceIdleOvershootCores: 0,
  ...overrides,
});

const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t, cfg, ext = 1) => tryStart(state, t, cfg, undefined, sampler(ext), undefined, memory);

const ticket = (id, overrides = {}) => ({
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
});

const heldLease = (id, key, weight = 1) => ({ id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight, state: LEASE_STATE.RUNNING });

// a conflict-blocked head: weight 3, conflicting with the running rouge:sim lease
async function conflictedHead(state, skipCount) {
  writeLease(state, heldLease('holder', 'rouge:sim'));
  await enqueue(state, ticket('head', { key: 'rouge:default', weight: 3, conflicts: ['rouge:sim'] }));
  atomicWriteJson(paths(state).conflictSkipState, { headId: 'head', count: skipCount, blockedSince: Date.now(), loggedPhase: skipCount >= LIMIT ? 'exhausted' : 'blocked' });
}

test('capacity skip: a test ticket passes a head that does not fit capacity, a sim ticket does not', async () => {
  for (const [klass, expected] of [['test', true], ['sim', false]]) {
    const { state } = freshEnv();
    writeLease(state, heldLease('holder', 'r:holder', 5));
    await enqueue(state, ticket('head', { weight: 8 }));
    const behind = ticket('behind', { class: klass });
    await enqueue(state, behind);
    const result = await poll(state, behind, baseCfg());
    assert.equal(result.started, expected, `${klass}: ${JSON.stringify(result)}`);
    if (!expected) assert.equal(result.reason, 'not-head');
  }
});

test('capacity skip: a sim ticket behind a SIM head keeps today\'s skip', async () => {
  const { state } = freshEnv();
  writeLease(state, heldLease('holder', 'r:holder', 5));
  await enqueue(state, ticket('head', { weight: 8, class: 'sim' }));
  const behind = ticket('behind', { class: 'sim' });
  await enqueue(state, behind);
  assert.equal((await poll(state, behind, baseCfg())).started, true);
});

test('capacity skip: a sim ticket is stepped over so a LATER test ticket still backfills', async () => {
  const { state } = freshEnv();
  writeLease(state, heldLease('holder', 'r:holder', 5));
  await enqueue(state, ticket('head', { weight: 8 }));
  await enqueue(state, ticket('sim', { class: 'sim' }));
  const test1 = ticket('test1');
  await enqueue(state, test1);
  assert.equal((await poll(state, test1, baseCfg())).started, true);
});

test('resource skip: a test ticket backfills a CPU-denied head, a sim ticket does not and spends no allowance', async () => {
  for (const [klass, expected] of [['test', true], ['sim', false]]) {
    const { state } = freshEnv();
    // BRAIN-418: the head must be blocked by LANE load (2-core lease + 3.4 ambient + 4-core head = 9.4 > 9); blocked by
    // ambient load alone it would be futile, and a futile head spends no allowance.
    writeLease(state, heldLease('lane-load', 'lane:load', 2));
    const head = ticket('head', { weight: 4 });
    const behind = ticket('behind', { class: klass });
    await enqueue(state, head);
    await enqueue(state, behind);
    const denied = await poll(state, head, baseCfg(), 3.4);
    assert.equal(denied.cpuReason, 'projected-over-budget', 'the head is denied and records the allowance');
    const result = await poll(state, behind, baseCfg(), 3.4);
    assert.equal(result.started, expected, `${klass}: ${JSON.stringify(result)}`);
    assert.equal(readResourceSkipState(state).count, expected ? 1 : 0);
  }
});

test('futile resource head (ambient 7 + 4-core head > budget 9): a test ticket backfills, a sim ticket stays safe-only', async () => {
  for (const [klass, expected] of [['test', true], ['sim', false]]) {
    const { state } = freshEnv();
    const head = ticket('head', { weight: 4 });
    const behind = ticket('behind', { class: klass });
    await enqueue(state, head);
    await enqueue(state, behind);
    await poll(state, head, baseCfg(), 7);
    assert.equal(readResourceSkipState(state).futile, 'cpu', 'the head is futile');
    const result = await poll(state, behind, baseCfg(), 7);
    assert.equal(result.started, expected, `${klass}: ${JSON.stringify(result)}`);
    if (!expected) assert.equal(result.reason, 'cpu-admission', 'a sim only ever goes through safe backfill, which reserves the head\'s claim: 7 + 4 + 1 > 9');
  }
});

test('conflict skip (allowance remaining): a test ticket passes the head, a sim ticket does not', async () => {
  for (const [klass, expected] of [['test', true], ['sim', false]]) {
    const { state } = freshEnv();
    await conflictedHead(state, 0);
    const behind = ticket('behind', { key: 'jun:default', weight: 7, class: klass }); // 1 held + 3 head + 7 > 10: not provably harmless
    await enqueue(state, behind);
    const result = await poll(state, behind, baseCfg());
    assert.equal(result.started, expected, `${klass}: ${JSON.stringify(result)}`);
    assert.equal(readSkipState(state).count, expected ? 1 : 0);
  }
});

test('safe backfill: an exhausted conflict-blocked head still admits a sim ticket that provably cannot delay it', async () => {
  const { state } = freshEnv();
  await conflictedHead(state, LIMIT);
  const sim = ticket('sim', { key: 'jun:default', weight: 2, class: 'sim' });
  await enqueue(state, sim);
  const result = await poll(state, sim, baseCfg());
  assert.equal(result.started, true);
  assert.equal(readSkipState(state).count, LIMIT, 'a safe backfill is not a skip');
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /event=safe-backfill headId=head candidate=sim /);
});

test('safe backfill: a sim ticket that WOULD delay the exhausted head is refused', async () => {
  const { state } = freshEnv();
  await conflictedHead(state, LIMIT);
  const sim = ticket('sim', { key: 'jun:default', weight: 9, class: 'sim' }); // 1 held + 3 head + 9 > 10
  await enqueue(state, sim);
  assert.equal((await poll(state, sim, baseCfg())).reason, 'not-head');
});

test('safe backfill is available to a sim on every path: a sim behind a conflict head still inside its skip allowance, counting no skip', async () => {
  const { state } = freshEnv();
  await conflictedHead(state, 0);
  const sim = ticket('sim', { key: 'jun:default', weight: 2, class: 'sim' });
  await enqueue(state, sim);
  const result = await poll(state, sim, baseCfg());
  assert.equal(result.started, true, JSON.stringify(result));
  assert.equal(readSkipState(state).count, 0, 'a safe backfill is not a skip');
});

test('conflict grace expiry: backfill resumes for tests, but a sim behind the test head still needs the safe-backfill proof', async () => {
  const run = async (klass, weight) => {
    const { state } = freshEnv();
    writeLease(state, heldLease('holder', 'rouge:sim'));
    await enqueue(state, ticket('head', { key: 'rouge:default', weight: 3, conflicts: ['rouge:sim'] }));
    const longAgo = Date.now() - 2 * baseCfg().headBlockGraceMs;
    atomicWriteJson(paths(state).conflictSkipState, { headId: 'head', count: LIMIT, blockedSince: longAgo, loggedPhase: 'exhausted' });
    const behind = ticket('behind', { key: 'jun:default', weight, class: klass });
    await enqueue(state, behind);
    return { state, result: await poll(state, behind, baseCfg()) };
  };
  assert.equal((await run('test', 7)).result.started, true, 'control: an ordinary skip after the grace expired');
  const unsafe = await run('sim', 7);
  assert.equal(unsafe.result.reason, 'not-head', 'a sim that could delay the head is refused after grace expiry');
  assert.equal(readSkipState(unsafe.state).count, LIMIT, 'and spends nothing');
  assert.equal((await run('sim', 2)).result.started, true, 'a provably harmless sim still passes');
});

test('safe backfill preserves the head\'s CPU claim: a sim that fits CPU alone but not beside the head is denied', async () => {
  const run = async (ext) => {
    const { state } = freshEnv();
    await conflictedHead(state, 0);
    const sim = ticket('sim', { key: 'jun:default', weight: 2, class: 'sim' });
    await enqueue(state, sim);
    return poll(state, sim, baseCfg(), ext);
  };
  assert.equal((await run(1)).started, true, 'room for holder + head + sim');
  const tight = await run(4); // 4 ambient + 1 holder + 3 head + 2 sim = 10 > budget 9; without the head 7
  assert.equal(tight.started, false);
  assert.equal(tight.reason, 'cpu-admission');
});

test('safe backfill preserves the head\'s weight: a sim that fits capacity alone but not beside the head is refused', async () => {
  const { state } = freshEnv();
  await conflictedHead(state, LIMIT);
  const sim = ticket('sim', { key: 'jun:default', weight: 6, class: 'sim' }); // 1 held + 6 = 7 fits; + head 3 = 10 fits exactly
  const tooBig = ticket('too-big', { key: 'jun:big', weight: 7, class: 'sim' }); // 1 + 7 = 8 fits; + head 3 = 11 > 10
  await enqueue(state, sim);
  await enqueue(state, tooBig);
  assert.equal((await poll(state, tooBig, baseCfg())).reason, 'not-head');
});

const T0 = 1_700_000_000_000;
function fencedState() {
  const { state } = freshEnv();
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
  fenceLegacyQueue(state, 'test');
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
  return state;
}
const reserved = (seq) => ({ reason: 'resource', skipsCharged: 3, reserved: true, reservationSeq: seq, inScope: true, behindConflict: false, deniedAt: T0, budget: 9, externalBusy: 5.4 });
function viewOf(state, cfg) {
  const sched = resolveScheduler(state, { log: false });
  const { queue, ownerId } = effectiveView(listQueue(state), T0, cfg, fairnessStore(state, sched.tickets));
  return { ids: queue.map((t) => (t ? t.id : null)), ownerId };
}

test('mixed reservations: a sim reservation owner never overrides a higher-priority test that arrives; a test owner and a lone sim owner still promote', async () => {
  const realNow = Date.now;
  Date.now = () => T0;
  try {
    const cfg = baseCfg();
    const withOwner = async (ownerClass, arrivals) => {
      const state = fencedState();
      await enqueue(state, ticket('owner', { class: ownerClass, priorityRequested: 'low' }));
      for (const [id, klass, tier] of arrivals) await enqueue(state, ticket(id, { class: klass, priorityRequested: tier }));
      atomicWriteJson(paths(state).fairness, { version: 2, tickets: { owner: { resource: reserved(1) } } });
      return viewOf(state, cfg);
    };
    assert.deepEqual(await withOwner('sim', [['test-high', 'test', 'high']]), { ids: ['test-high', 'owner'], ownerId: null }, 'the sim reservation is dormant');
    assert.deepEqual(await withOwner('test', [['test-high', 'test', 'high']]), { ids: ['owner', 'test-high'], ownerId: 'owner' }, 'control: a test owner still jumps ahead');
    assert.deepEqual(await withOwner('sim', [['sim-high', 'sim', 'high']]), { ids: ['owner', 'sim-high'], ownerId: 'owner' }, 'no test ahead: the sim owner promotes as before');
  } finally {
    Date.now = realNow;
  }
});

test('a safe sim backfill past a resource-denied head spends none of the head\'s resource allowance', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  const sim = ticket('sim', { class: 'sim' });
  await enqueue(state, head);
  await enqueue(state, sim);
  assert.equal((await poll(state, head, baseCfg(), 5.4)).cpuReason, 'projected-over-budget', 'the head records its denial');
  const result = await poll(state, sim, baseCfg(), 1); // the load has since dropped: head + sim both fit, so the sim is provably harmless
  assert.equal(result.started, true, JSON.stringify(result));
  assert.equal(readResourceSkipState(state).count, 0);
});
