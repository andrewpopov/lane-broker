import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { enqueue, tryStart, dequeueSync, listQueue, readResourceSkipState } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson, readJsonSafe } from '../src/state.js';
import { resolveScheduler, fairnessStore, effectiveView } from '../src/fairness.js';
import { fenceLegacyQueue } from '../src/migrate.js';
import { collectStatus, renderStatusText } from '../src/status.js';

/**
 * BRAIN-380 slice 2: priority-ordered selection behind the scheduler fence. Real tryStart against a temp state
 * dir on a fake wall clock. The fence (`sched-v2.json`) and fairness-v2.json are written directly here; the
 * command that writes them is a later slice.
 */

const MIN = 60_000;
const T0 = 1_700_000_000_000;
const GIB = 1024 ** 3;

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, conflictSafeBackfill: false, ...overrides };
}

// the BRAIN-346 machine: 10 cores, reserve 1 -> budget 9; ambient 5.4 denies a weight-4 head and admits a weight-1 ticket
function activeCfg(overrides = {}) {
  return baseCfg({ schedulerMode: 'active', cpuAdmissionPercent: 100, cpuReserveCores: 1, admissionCooldownMs: 0, resourceSkipLimit: 3, resourceIdleOvershootCores: 0, ...overrides });
}
const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t, cfg, ext = 5.4, seams = {}) => tryStart(state, t, cfg, undefined, sampler(ext), undefined, memory, undefined, seams);

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

async function withClock(fn) {
  const realNow = Date.now;
  const clock = { now: T0 };
  Date.now = () => clock.now;
  try {
    return await fn(clock);
  } finally {
    Date.now = realNow;
  }
}

// the migrator always writes both files, and a valid fence over a missing fairness file is invalid
const writeFence = (state, overrides = {}) => {
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0, ...overrides });
  fenceLegacyQueue(state, 'test'); // the real layout: old code's queue/ is now a file, new code queues in queue-v2/
  if (!fs.existsSync(paths(state).fairness)) atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
};
const writeFairness = (state, tickets) => atomicWriteJson(paths(state).fairness, { version: 2, tickets });
const readFairness = (state) => JSON.parse(fs.readFileSync(paths(state).fairness, 'utf8')).tickets;
const fenced = (cfg = baseCfg()) => {
  const { state } = freshEnv();
  writeFence(state);
  return { state, cfg };
};
const conflictRec = (skipsCharged, since, loggedPhase = null) => ({ reason: 'conflict', skipsCharged, blockedSince: since, graceStartedAt: since, loggedPhase });
const reservedRec = (seq, overrides = {}) => ({ reason: 'resource', skipsCharged: 3, reserved: true, reservationSeq: seq, inScope: true, behindConflict: false, deniedAt: T0, budget: 9, externalBusy: 5.4, ...overrides });
const readLog = (state) => (fs.existsSync(paths(state).admissionLog) ? fs.readFileSync(paths(state).admissionLog, 'utf8') : '');
const singletons = (state) => [paths(state).conflictSkipState, paths(state).capacitySkipState, paths(state).resourceSkipState];

function viewOf(state, cfg) {
  const sched = resolveScheduler(state, { log: false });
  assert.equal(sched.v2, true, 'the fence is valid');
  const { queue, ownerId } = effectiveView(listQueue(state), T0, cfg, fairnessStore(state, sched.tickets));
  return { ids: queue.map((t) => (t ? t.id : null)), ownerId };
}

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const { pid } = child;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

test('no fence: a high ticket does not jump the FIFO, and nothing of the new scheduler is written', async () => {
  await withClock(async () => {
    const { state } = freshEnv();
    const cfg = baseCfg({ capacity: 1 });
    writeLease(state, heldLease('holder', 'other:key'));
    const med = ticket('med');
    const high = ticket('high', { priorityRequested: 'high' });
    await enqueue(state, med);
    await enqueue(state, high);
    removeLease(state, 'holder');
    assert.equal((await poll(state, high, cfg)).reason, 'not-head', 'strict FIFO: the high waits behind the medium');
    assert.equal((await poll(state, med, cfg)).started, true);
    assert.equal(fs.existsSync(paths(state).fairness), false);
    assert.equal(readLog(state).includes('scheduler-fence-invalid'), false, 'a missing fence is silent');
  });
});

test('an invalid fence or fairness file runs legacy mode and logs scheduler-fence-invalid', async () => {
  const cases = {
    'not json': (state) => fs.writeFileSync(paths(state).schedFence, '{nope'),
    'future version': (state) => writeFence(state, { version: 3 }),
    'missing migratedAt': (state) => atomicWriteJson(paths(state).schedFence, { version: 2 }),
    'malformed fairness file': (state) => {
      writeFence(state);
      atomicWriteJson(paths(state).fairness, { version: 2, tickets: { x: { conflict: { reason: 'capacity', skipsCharged: 1 } } } });
    },
    'missing fairness file': (state) => {
      writeFence(state);
      fs.rmSync(paths(state).fairness);
    },
    'wrong fairness version': (state) => {
      writeFence(state);
      atomicWriteJson(paths(state).fairness, { version: 1, tickets: {} });
    },
  };
  for (const [name, arrange] of Object.entries(cases)) {
    await withClock(async () => {
      const { state } = freshEnv();
      const cfg = baseCfg({ capacity: 1 });
      arrange(state);
      const med = ticket('med');
      const high = ticket('high', { priorityRequested: 'high' });
      await enqueue(state, med);
      await enqueue(state, high);
      assert.equal((await poll(state, high, cfg)).reason, 'not-head', `${name}: legacy FIFO`);
      assert.match(readLog(state), /event=scheduler-fence-invalid reason=(fence|fairness)-/, name);
    });
  }
});

test('a high ticket queued behind medium and low tickets starts first when capacity frees', async () => {
  await withClock(async () => {
    const { state, cfg } = fenced(baseCfg({ capacity: 1 }));
    writeLease(state, heldLease('holder', 'other:key'));
    const low = ticket('low', { priorityRequested: 'low' });
    const med = ticket('med');
    const high = ticket('high', { priorityRequested: 'high' });
    for (const t of [low, med, high]) await enqueue(state, t);
    assert.equal((await poll(state, high, cfg)).reason, 'capacity', 'high is the effective head, held back only by capacity');
    removeLease(state, 'holder');
    assert.equal((await poll(state, low, cfg)).reason, 'not-head');
    assert.equal((await poll(state, med, cfg)).reason, 'not-head');
    assert.equal((await poll(state, high, cfg)).started, true);
  });
});

test('a low ticket aged 20 minutes starts before a fresh high with a higher seq', async () => {
  await withClock(async () => {
    const { state, cfg } = fenced();
    const aged = ticket('aged-low', { priorityRequested: 'low', prioOriginAt: T0 - 20 * MIN });
    const high = ticket('fresh-high', { priorityRequested: 'high' });
    await enqueue(state, aged);
    await enqueue(state, high);
    assert.equal((await poll(state, high, cfg)).reason, 'not-head', 'a tie at the ceiling goes to the lower seq');
    assert.equal((await poll(state, aged, cfg)).started, true);
  });
});

test('ordering does not cross a null barrier, and a reservation owner behind one is not promoted', async () => {
  await withClock(async () => {
    const { state } = fenced(activeCfg());
    const cfg = activeCfg();
    const first = ticket('first', { priorityRequested: 'low' });
    await enqueue(state, first);
    fs.writeFileSync(path.join(paths(state).queue, '000000000001-zzz-garbage.json'), '{not json');
    const high = ticket('high', { priorityRequested: 'high' });
    await enqueue(state, high);
    assert.deepEqual(viewOf(state, cfg).ids, ['first', null, 'high'], 'the high stays behind the unreadable record');
    assert.equal((await poll(state, high, cfg, 0)).reason, 'not-head');

    writeFairness(state, { high: { resource: reservedRec(5) } });
    assert.deepEqual(viewOf(state, cfg), { ids: ['first', null, 'high'], ownerId: null }, 'the owner is not promoted across the null; its reservation is suspended');
    assert.equal((await poll(state, first, cfg, 0)).started, true);
  });
});

test('head displacement keeps each ticket\'s counters: the displaced head resumes where it left off', async () => {
  await withClock(async (clock) => {
    const { state, cfg } = fenced(baseCfg({ conflictSkipLimit: 3 }));
    writeLease(state, heldLease('holder', 'k:busy'));
    const a = ticket('a', { key: 'k:a', conflicts: ['k:busy'] });
    const [b1, b2, b3] = ['b1', 'b2', 'b3'].map((id) => ticket(id));
    for (const t of [a, b1, b2, b3]) await enqueue(state, t);

    assert.equal((await poll(state, b1, cfg)).started, true);
    removeLease(state, 'b1');
    assert.equal(readFairness(state).a.conflict.skipsCharged, 1);

    clock.now = T0 + MIN;
    await enqueue(state, ticket('h', { key: 'k:h', conflicts: ['k:busy'], priorityRequested: 'high' }));
    assert.equal((await poll(state, b2, cfg)).started, true, 'skips past the new, conflicted head');
    removeLease(state, 'b2');
    let records = readFairness(state);
    assert.equal(records.h.conflict.skipsCharged, 1, 'the charge goes to the head of the moment');
    assert.equal(records.a.conflict.skipsCharged, 1, 'the displaced ticket\'s counter paused: neither reset nor charged');
    assert.equal(records.a.conflict.graceStartedAt, T0);

    dequeueSync(state, 'h');
    clock.now = T0 + 5 * MIN;
    assert.equal((await poll(state, b3, cfg)).started, true);
    records = readFairness(state);
    assert.equal(records.a.conflict.skipsCharged, 2, 'a resumes from 1, not from 0');
    assert.equal(records.h, undefined, 'h departed, so its record went');
  });
});

test('grace keeps running while a conflicted ticket is displaced', async () => {
  await withClock(async (clock) => {
    const { state, cfg } = fenced(baseCfg({ conflictSkipLimit: 1, headBlockGraceMs: 10 * MIN }));
    writeLease(state, heldLease('holder', 'k:busy'));
    const a = ticket('a', { key: 'k:a', conflicts: ['k:busy'] });
    const [b1, b2, b3] = ['b1', 'b2', 'b3'].map((id) => ticket(id));
    for (const t of [a, b1, b2, b3]) await enqueue(state, t);
    assert.equal((await poll(state, b1, cfg)).started, true, 'uses a\'s whole skip allowance');
    removeLease(state, 'b1');

    clock.now = T0 + MIN;
    await enqueue(state, ticket('h', { key: 'k:h', conflicts: ['k:busy'], priorityRequested: 'high' }));
    assert.equal((await poll(state, b2, cfg)).started, true);
    removeLease(state, 'b2');
    dequeueSync(state, 'h');

    clock.now = T0 + 5 * MIN;
    assert.equal((await poll(state, b3, cfg)).reason, 'not-head', 'a is head again, exhausted, still inside grace measured from T0');
    clock.now = T0 + 11 * MIN;
    assert.equal((await poll(state, b3, cfg)).started, true, 'grace lapsed 10 min after T0, though a was displaced for part of it');
  });
});

test('behind the fence the singleton skip files are neither read nor written', async () => {
  await withClock(async () => {
    const { state, cfg } = fenced(baseCfg({ conflictSkipLimit: 3 }));
    writeLease(state, heldLease('holder', 'k:busy'));
    const a = ticket('a', { key: 'k:a', conflicts: ['k:busy'] });
    const b = ticket('b');
    await enqueue(state, a);
    await enqueue(state, b);
    // a legacy file saying a's allowance is long used up: reading it would refuse b
    const seeded = { headId: 'a', count: 99, blockedSince: T0, loggedPhase: 'exhausted' };
    atomicWriteJson(paths(state).conflictSkipState, seeded);
    atomicWriteJson(paths(state).capacitySkipState, { headId: 'a', count: 99, loggedPhase: 'exhausted' });
    const before = singletons(state).map((f) => (fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : null));

    assert.equal((await poll(state, b, cfg)).started, true, 'the singleton\'s count of 99 was ignored');
    assert.equal(readFairness(state).a.conflict.skipsCharged, 1);
    const after = singletons(state).map((f) => (fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : null));
    assert.deepEqual(after, before, 'and not one byte of the singleton files changed (none created)');
  });
});

test('a reservation is not taken over by an arriving high', async () => {
  await withClock(async () => {
    const cfg = activeCfg();
    const run = async (reserve) => {
      const { state } = fenced(cfg);
      const owner = ticket('owner', { weight: 4 });
      await enqueue(state, owner);
      if (reserve) writeFairness(state, { owner: { resource: reservedRec(7) } });
      const high = ticket('arriving-high', { priorityRequested: 'high' });
      await enqueue(state, high);
      return { state, owner, high };
    };
    const reserved = await run(true);
    assert.equal((await poll(reserved.state, reserved.high, cfg)).reason, 'not-head', 'waits like any other follower');
    assert.deepEqual(viewOf(reserved.state, cfg), { ids: ['owner', 'arriving-high'], ownerId: 'owner' });
    const control = await run(false);
    assert.equal((await poll(control.state, control.high, cfg)).started, true, 'control: with no reservation the high goes first');
  });
});

test('R4-3: A reserves, A is suspended, B earns, A returns and is active with B dormant, A starts and B is active', async () => {
  await withClock(async () => {
    const cfg = activeCfg();
    const { state } = fenced(cfg);
    const b = ticket('b-ticket', { weight: 4 });
    const a = ticket('a-ticket', { weight: 4, priorityRequested: 'high' });
    await enqueue(state, b);
    await enqueue(state, a);
    writeFairness(state, { 'a-ticket': { resource: reservedRec(10) } });
    assert.deepEqual(viewOf(state, cfg), { ids: ['a-ticket', 'b-ticket'], ownerId: 'a-ticket' }, 'A reserves and heads the queue');

    const garbage = path.join(paths(state).queue, '000000000001-zzz-garbage.json');
    fs.writeFileSync(garbage, '{not json');
    assert.deepEqual(viewOf(state, cfg), { ids: ['b-ticket', null, 'a-ticket'], ownerId: null }, 'A is behind a barrier: suspended, dormant');

    writeFairness(state, { 'a-ticket': { resource: reservedRec(10) }, 'b-ticket': { resource: reservedRec(11, { skipsCharged: 3 }) } });
    assert.deepEqual(viewOf(state, cfg), { ids: ['b-ticket', null, 'a-ticket'], ownerId: 'b-ticket' }, 'with none active, B\'s reservation is the active one');

    fs.unlinkSync(garbage);
    assert.deepEqual(viewOf(state, cfg), { ids: ['a-ticket', 'b-ticket'], ownerId: 'a-ticket' }, 'A is back and older: A active, B dormant');
    assert.equal(readFairness(state)['b-ticket'].resource.skipsCharged, 3, 'B keeps its counters while dormant');
    assert.equal((await poll(state, b, cfg, 0)).reason, 'not-head', 'a dormant reservation reserves nothing and does not start');

    assert.equal((await poll(state, a, cfg, 0)).started, true, 'A starts');
    assert.equal(readFairness(state)['a-ticket'], undefined, 'A\'s reservation is released with it');
    assert.deepEqual(viewOf(state, cfg), { ids: ['b-ticket'], ownerId: 'b-ticket' }, 'B becomes the active reservation');
  });
});

test('reservations are ordered by reservationSeq, never by timestamps: equal wall times and a clock rollback do not reorder them', async () => {
  await withClock(async (clock) => {
    const cfg = activeCfg();
    for (const [name, xDenied, yDenied] of [['equal wall times', T0, T0], ['x earned later by the wall clock', T0 + 5 * MIN, T0], ['rollback: x earned at a smaller time', T0 - 60 * MIN, T0]]) {
      const { state } = fenced(cfg);
      await enqueue(state, ticket('y'));
      await enqueue(state, ticket('x'));
      clock.now = xDenied;
      writeFairness(state, { y: { resource: reservedRec(9, { deniedAt: yDenied }) }, x: { resource: reservedRec(4, { deniedAt: xDenied }) } });
      assert.equal(viewOf(state, cfg).ownerId, 'x', `${name}: the lower reservationSeq is the older reservation`);
    }
  });
});

test('a reservation earned by backfill takes its reservationSeq from the monotonic queue counter', async () => {
  await withClock(async () => {
    const cfg = activeCfg();
    const { state } = fenced(cfg);
    const head = ticket('head', { weight: 4 });
    await enqueue(state, head);
    await poll(state, head, cfg);
    for (let i = 0; i < 3; i += 1) {
      const small = ticket(`s${i}`);
      await enqueue(state, small);
      assert.equal((await poll(state, small, cfg)).started, true);
      removeLease(state, small.id);
    }
    const rec = readFairness(state).head.resource;
    const counter = readJsonSafe(paths(state).seq).n;
    assert.equal(rec.reserved, true);
    assert.equal(rec.reservationSeq, counter, 'drawn once, from the counter the queue files use, at the latch');
    assert.ok(rec.reservationSeq > 4, 'later than every queue seq issued before it');
    assert.equal(readResourceSkipState(state), null, 'the legacy singleton was never written');
    assert.equal(fs.existsSync(paths(state).resourceSkipState), false);
  });
});

test('disabling the resource limit releases the reservation', async () => {
  await withClock(async () => {
    const { state } = fenced(activeCfg());
    await enqueue(state, ticket('owner', { weight: 4 }));
    await enqueue(state, ticket('high', { priorityRequested: 'high' }));
    writeFairness(state, { owner: { resource: reservedRec(3) } });
    assert.equal(viewOf(state, activeCfg()).ownerId, 'owner');
    assert.equal(viewOf(state, activeCfg({ resourceSkipLimit: 0 })).ownerId, null);
    assert.deepEqual(viewOf(state, activeCfg({ resourceSkipLimit: 0 })).ids, ['high', 'owner']);
  });
});

test('disabling the limit RELEASES the reservation latch, and re-enabling it requires earning one again', async () => {
  await withClock(async () => {
    const { state } = fenced(activeCfg());
    await enqueue(state, ticket('owner', { weight: 4 }));
    await enqueue(state, ticket('high', { priorityRequested: 'high' }));
    writeFairness(state, { owner: { resource: reservedRec(3) } });
    assert.equal(viewOf(state, activeCfg()).ownerId, 'owner');
    fs.writeFileSync(paths(state).pause, 'test');
    await poll(state, ticket('high', { priorityRequested: 'high' }), activeCfg({ resourceSkipLimit: 0 }));
    const released = readFairness(state).owner.resource;
    assert.equal(released.reserved, false, 'the latch is deleted from the store, not merely ignored');
    assert.equal(released.reservationSeq, undefined);
    assert.equal(viewOf(state, activeCfg()).ownerId, null, 'turning the limit back on does not resurrect it');
  });
});

test('class eligibility: an ineligible owner\'s reservation is suspended, and the next eligible one is active', async () => {
  await withClock(async () => {
    const { state } = fenced(activeCfg());
    await enqueue(state, ticket('a', { weight: 4 }));
    await enqueue(state, ticket('b', { weight: 4 }));
    writeFairness(state, { a: { resource: reservedRec(3) }, b: { resource: reservedRec(5) } });
    const sched = resolveScheduler(state, { log: false });
    const store = fairnessStore(state, sched.tickets);
    const raw = listQueue(state);
    assert.equal(effectiveView(raw, T0, activeCfg(), store).ownerId, 'a', 'allow-all by default');
    assert.equal(effectiveView(raw, T0, activeCfg(), store, (t) => t.id !== 'a').ownerId, 'b', 'a is suspended, the older eligible reservation takes over');
    assert.equal(effectiveView(raw, T0, activeCfg(), store, () => false).ownerId, null);
  });
});

test('fairness validation: every field a selector reads is type-checked, and a reserved latch needs its seq', async () => {
  const bad = {
    'conflict blockedSince': { conflict: { ...conflictRec(1, T0), blockedSince: 'soon' } },
    'conflict graceStartedAt': { conflict: { ...conflictRec(1, T0), graceStartedAt: {} } },
    'conflict loggedPhase': { conflict: { ...conflictRec(1, T0), loggedPhase: 5 } },
    'capacity loggedPhase': { capacity: { reason: 'capacity', skipsCharged: 1, loggedPhase: [] } },
    'resource reserved': { resource: { ...reservedRec(3), reserved: 'yes' } },
    'resource reservationSeq': { resource: { ...reservedRec(3), reservationSeq: '3' } },
    'resource reserved without a seq': { resource: { ...reservedRec(3), reservationSeq: undefined } },
    'resource inScope': { resource: { ...reservedRec(3), inScope: 1 } },
    'resource deniedAt': { resource: { ...reservedRec(3), deniedAt: null } },
    'resource budget': { resource: { ...reservedRec(3), budget: 'big' } },
  };
  for (const [name, record] of Object.entries(bad)) {
    const { state } = fenced();
    writeFairness(state, { x: record });
    assert.equal(resolveScheduler(state, { log: false }).v2, false, `${name}: not trusted`);
  }
  const { state } = fenced();
  writeFairness(state, { x: { conflict: conflictRec(1, T0), resource: reservedRec(3) } });
  assert.equal(resolveScheduler(state, { log: false }).v2, true, 'a well-formed file is');
});

test('a ticket\'s fairness record is deleted when the ticket departs: start, cancel, expiry or reap', async () => {
  const seed = (state) => writeFairness(state, { gone: { conflict: conflictRec(2, T0) }, stay: { conflict: conflictRec(1, T0) } });
  const setup = async (goneOverrides = {}) => {
    const { state, cfg } = fenced();
    const gone = ticket('gone', goneOverrides);
    const stay = ticket('stay');
    await enqueue(state, gone);
    await enqueue(state, stay);
    seed(state);
    fs.writeFileSync(paths(state).pause, 'test');
    return { state, cfg, gone, stay };
  };
  const expectGone = (state, how) => {
    const records = readFairness(state);
    assert.equal(records.gone, undefined, `${how}: the departed ticket's record is gone`);
    assert.equal(records.stay.conflict.skipsCharged, 1, `${how}: a queued bystander's record is untouched`);
  };
  await withClock(async () => {
    {
      const { state, cfg, gone } = await setup();
      fs.unlinkSync(paths(state).pause);
      assert.equal((await poll(state, gone, cfg)).started, true);
      expectGone(state, 'start');
    }
    {
      const { state, cfg, stay } = await setup();
      dequeueSync(state, 'gone');
      assert.equal((await poll(state, stay, cfg)).reason, 'paused');
      expectGone(state, 'cancel');
    }
    {
      const { state, cfg, gone, stay } = await setup({ startDeadline: T0 - 1 });
      assert.equal((await poll(state, gone, cfg)).reason, 'queue-timeout');
      dequeueSync(state, 'gone');
      await poll(state, stay, cfg);
      expectGone(state, 'expiry');
    }
    {
      const { state, cfg, stay } = await setup({ supervisorPid: await deadPid() });
      assert.equal((await poll(state, stay, cfg)).reason, 'paused');
      expectGone(state, 'reap');
    }
    {
      // an unreadable record has no id: nothing can be proven departed, so nothing is dropped
      const { state, cfg, stay } = await setup();
      dequeueSync(state, 'gone');
      fs.writeFileSync(path.join(paths(state).queue, '000000000000-garbage.json'), '{not json');
      await poll(state, stay, cfg);
      assert.ok(readFairness(state).gone, 'kept while the queue holds an unreadable record');
    }
  });
});

test('BRAIN-355 safe backfill is judged against the effective head, not the raw FIFO head', async () => {
  await withClock(async () => {
    const LIMIT = 3;
    const cfg = activeCfg({ conflictSafeBackfill: true, conflictSkipLimit: LIMIT });
    const { state } = fenced(cfg);
    writeLease(state, heldLease('holder', 'rouge:sim'));
    await enqueue(state, ticket('old-head', { key: 'rouge:default', conflicts: ['rouge:sim'] }));
    await enqueue(state, ticket('new-head', { key: 'rouge:fresh', weight: 3, conflicts: ['rouge:sim', 'rouge:lint'], priorityRequested: 'high' }));
    const jun = ticket('jun', { key: 'jun:default', weight: 2 });
    const lint = ticket('lint', { key: 'rouge:lint' });
    await enqueue(state, jun);
    await enqueue(state, lint);
    writeFairness(state, { 'new-head': { conflict: conflictRec(LIMIT, T0, 'exhausted') } });
    assert.equal(viewOf(state, cfg).ids[0], 'new-head');

    const refused = await poll(state, lint, cfg, 1);
    assert.equal(refused.reason, 'not-head', 'the effective head declares a conflict with this key');
    const started = await poll(state, jun, cfg, 1);
    assert.equal(started.started, true);
    assert.match(readLog(state), /event=safe-backfill headId=new-head candidate=jun /);
    assert.equal(readFairness(state)['new-head'].conflict.skipsCharged, LIMIT, 'a safe backfill is not a skip');
  });
});

test('BRAIN-365 conflict backfill walks the effective head and breaks claim ties by ordered-view position', async () => {
  await withClock(async () => {
    const cfg = activeCfg();
    const { state } = fenced(cfg);
    writeLease(state, { ...heldLease('sim', 'rouge:sim', 4) });
    await enqueue(state, ticket('old-head', { key: 'rouge:default', conflicts: ['rouge:sim'] }));
    const big = ticket('big', { key: 'jun:prepush', weight: 4 });
    const lowSmall = ticket('low-small', { key: 'jun:117', priorityRequested: 'low' });
    const medSmall = ticket('med-small', { key: 'mobile:test' });
    for (const t of [big, lowSmall, medSmall]) await enqueue(state, t);
    await enqueue(state, ticket('new-head', { key: 'rouge:fresh', conflicts: ['rouge:sim'], priorityRequested: 'high' }));
    assert.deepEqual(viewOf(state, cfg).ids, ['new-head', 'old-head', 'big', 'med-small', 'low-small']);

    const denied = await poll(state, big, cfg, 2);
    assert.equal(denied.cpuReason, 'projected-over-budget');
    assert.equal(readFairness(state)['new-head'].resource.behindConflict, true, 'the denial is recorded under the effective head');
    assert.equal((await poll(state, lowSmall, cfg, 2)).reason, 'not-head', 'equal claims: the earlier FIFO ticket loses to the earlier ordered-view ticket');
    const picked = await poll(state, medSmall, cfg, 2);
    assert.equal(picked.started, true);
    assert.equal(readFairness(state)['new-head'].conflict.skipsCharged, 1, 'the skip is charged to the effective head');
    assert.equal(readFairness(state)['old-head'], undefined);
  });
});

test('the shadow snapshot sees the same ordered view the live selection used', async () => {
  await withClock(async () => {
    const cfg = baseCfg({ allocationShadow: true });
    const { state } = fenced(cfg);
    await enqueue(state, ticket('low', { priorityRequested: 'low' }));
    const high = ticket('high', { priorityRequested: 'high' });
    await enqueue(state, high);
    let seen = null;
    await poll(state, high, cfg, 1, { evaluator: ({ queue }) => { seen = queue.map((t) => t.id); throw new Error('stop after capturing'); } });
    assert.deepEqual(seen, ['high', 'low']);
  });
});

async function withStatusEnv(cfg, fn) {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, ...cfg });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await withClock(() => fn(state));
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

test('lane status with a valid fence shows the ordered effective view, positions, the promoted owner and priority: active', async () => {
  await withStatusEnv({ schedulerMode: 'active' }, async (state) => {
    writeFence(state);
    await enqueue(state, ticket('owner', { priorityRequested: 'low' }));
    await enqueue(state, ticket('mid'));
    await enqueue(state, ticket('top', { priorityRequested: 'high' }));
    const hwmBefore = fs.readFileSync(paths(state).hwm, 'utf8');

    let status = await collectStatus();
    assert.deepEqual(status.queued.map((q) => [q.id, q.position]), [['top', 1], ['mid', 2], ['owner', 3]], 'score order, not FIFO');
    assert.deepEqual(status.priority, { active: true, mode: 'v2', nowEff: T0, reservationOwner: null });

    writeFairness(state, { owner: { resource: reservedRec(2) } });
    status = await collectStatus();
    assert.deepEqual(status.queued.map((q) => [q.id, q.position]), [['owner', 1], ['top', 2], ['mid', 3]], 'the owner is promoted, the rest keep their order');
    assert.equal(status.queued[0].reservationOwner, true);
    const text = renderStatusText(status);
    assert.match(text, /^priority: active \(reservation owner owner promoted to the front\)$/m);
    assert.match(text, /QUEUE \(priority order\):\n {2}#1 owner .*\[reservation owner\]\n {2}#2 top /);
    assert.equal(fs.readFileSync(paths(state).hwm, 'utf8'), hwmBefore, 'status never writes the clock');
  });
});

test('lane status without a fence still shows FIFO and says priority is inactive', async () => {
  await withStatusEnv({}, async (state) => {
    await enqueue(state, ticket('first', { priorityRequested: 'low' }));
    await enqueue(state, ticket('second', { priorityRequested: 'high' }));
    const status = await collectStatus();
    assert.deepEqual(status.queued.map((q) => q.id), ['first', 'second']);
    assert.equal(status.priority.active, false);
    assert.match(renderStatusText(status), /QUEUE \(FIFO\):/);
  });
});
