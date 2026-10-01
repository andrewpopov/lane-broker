import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { enqueue, tryStart, readResourceSkipState } from '../src/scheduler.js';
import { sampleHostCpu } from '../src/cpu.js';
import { collectStatus, renderStatusText } from '../src/status.js';
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
  atomicWriteJson(paths(state).resourceSkipState, { headId, count: 0, reserved: false, inScope: true, deniedAt: Date.now(), budget: 9, externalBusy: 5.4, ...overrides });
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

test('4b: a reserved record refuses backfill even with the count below the limit', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { weight: 4 });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);
  seedRecord(state, 'head', { count: 1, reserved: true });
  const result = await poll(state, small, baseCfg());
  assert.equal(result.started, false, 'reserved alone is enough to refuse');
  assert.equal(result.reason, 'not-head');
});

test('4c: the reservation and count survive out-of-scope denials; only the latest denial gates backfill', async () => {
  const cfg = baseCfg();
  for (const kind of ['cpu-gate-closed', 'memory-headroom']) {
    const { state } = freshEnv();
    const head = ticket('head', { weight: 4 });
    const extra = ticket('extra');
    await enqueue(state, head);
    await enqueue(state, extra);
    seedRecord(state, 'head', { count: 3, reserved: true });

    const out =
      kind === 'cpu-gate-closed'
        ? await poll(state, head, cfg, { ext: 9.5 }) // 95% busy closes the CPU gate
        : await poll(state, head, cfg, { mem: memory({ availableBytes: 1 * GIB }) });
    assert.equal(out.started, false, kind);
    const mid = readResourceSkipState(state);
    assert.deepEqual({ count: mid.count, reserved: mid.reserved, inScope: mid.inScope }, { count: 3, reserved: true, inScope: false }, `${kind}: allowance kept, backfill paused`);

    atomicWriteJson(paths(state).cpuGate, { closed: false, consecutiveUnder: 0 });
    const back = await poll(state, head, cfg); // projected-over-budget again
    assert.equal(back.cpuReason, 'projected-over-budget', kind);
    const after = readResourceSkipState(state);
    assert.deepEqual({ count: after.count, reserved: after.reserved, inScope: after.inScope }, { count: 3, reserved: true, inScope: true }, `${kind}: still reserved, not recreated at 0`);
    assert.equal((await poll(state, extra, cfg)).started, false, `${kind}: an otherwise-admissible ticket is still refused`);
  }
});

test('4d: an out-of-scope latest denial pauses an unreserved head\'s backfill without resetting the count', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = ticket('head', { weight: 4 });
  const small = ticket('small');
  await enqueue(state, head);
  await enqueue(state, small);
  seedRecord(state, 'head', { count: 2 });
  await poll(state, head, cfg, { mem: memory({ availableBytes: 1 * GIB }) });
  assert.equal(readResourceSkipState(state).count, 2);
  assert.equal((await poll(state, small, cfg)).reason, 'not-head', 'paused while the latest denial is out of scope');
  await poll(state, head, cfg); // in scope again
  assert.equal((await poll(state, small, cfg)).started, true, 'resumes, one backfill left of the allowance');
  assert.equal(readResourceSkipState(state).count, 3);
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

test('6b: the exemption needs an idle broker (a held lease means no exemption)', async () => {
  const cfg = baseCfg({ resourceIdleOvershootCores: 1 });
  const head = ticket('head', { weight: 4 });
  const { state } = freshEnv();
  await enqueue(state, head);
  writeLease(state, heldLease('holder', 'other:key', 1));
  assert.equal((await poll(state, head, cfg, { ext: 4.6 })).started, false, 'a held lease means the broker is not idle');
});

test('6c: a lone over-budget head on an idle broker starts within the overshoot, with no backfill needed', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ resourceIdleOvershootCores: 1 });
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  assert.equal(readResourceSkipState(state), null, 'no record, no reservation, nobody behind it');

  const result = await poll(state, head, cfg); // 5.4 + 4 = 9.4 vs budget 9: today's real stall
  assert.equal(result.started, true);
  assert.match(readLog(state), /current=start:resource-idle-exempt/);
  assert.match(readLog(state), /event=resource-idle-exempt headId=head overshoot=0.40/);
});

test('6d: an ORPHANED lease counts as held, so a lone over-budget head is not exempted', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ resourceIdleOvershootCores: 1 });
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  writeLease(state, { ...heldLease('orphan', 'other:key', 1), state: LEASE_STATE.ORPHANED });
  assert.equal((await poll(state, head, cfg, { ext: 4.6 })).started, false);
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
  seedRecord(state, 'head', { count: 2 });
  const failingWrite = () => {
    throw new Error('disk full');
  };
  for (let i = 0; i < 3; i += 1) {
    const result = await poll(state, small, cfg, { write: failingWrite });
    assert.equal(result.started, false, `attempt ${i}: never backfilled while the count cannot be recorded`);
    assert.equal(result.reason, 'not-head');
  }
  assert.equal(readLease(state, 'small'), null);
  assert.equal(readResourceSkipState(state).count, 2, 'the allowance is never reset or advanced by a failed write');

  const recovered = await poll(state, small, cfg); // writes work again
  assert.equal(recovered.started, true, 'the partially consumed allowance recovers once writes succeed');
  const after = readResourceSkipState(state);
  assert.deepEqual({ count: after.count, reserved: after.reserved }, { count: 3, reserved: true });
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

/** os.cpus()-shaped, 10 cores, each core `busy` fraction busy over a 1000-tick delta from `base`. */
function cpus10(base, busy) {
  return Array.from({ length: 10 }, () => {
    const total = 2000 + base * 1000;
    const idle = 1000 + base * 1000 * (1 - busy);
    return { model: 'test', speed: 0, times: { user: total - idle, nice: 0, sys: 0, idle, irq: 0 } };
  });
}
const realSampler = (cpus) => (root, _cpus, opts) => sampleHostCpu(root, cpus, opts);

test('13: a back-to-back sample reuses the last valid measurement (real sampler path) and never becomes unavailable-then-admit', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  sampleHostCpu(state, cpus10(0, 0.54)); // baseline snapshot
  const advanced = cpus10(1, 0.54); // 10 cores x 0.54 busy = 5.4 busy cores

  const first = await tryStart(state, head, cfg, undefined, realSampler(advanced), undefined, memory());
  assert.equal(first.reason, 'cpu-admission', 'a valid measurement: 5.4 + 4 > 9');
  assert.equal(first.cpuReason, 'projected-over-budget');

  const gateBefore = fs.readFileSync(paths(state).cpuGate, 'utf8');
  await new Promise((r) => setTimeout(r, 10));
  // Same counters again: nothing advanced, so the raw delta is "stale". On an idle broker that
  // used to read sample-unavailable-empty and ADMIT.
  const second = await tryStart(state, head, cfg, undefined, realSampler(advanced), undefined, memory());
  assert.equal(second.started, false, 'the reused measurement still denies');
  assert.equal(second.cpuReason, 'projected-over-budget');
  assert.equal(fs.readFileSync(paths(state).cpuGate, 'utf8'), gateBefore, 'a reused measurement is not a new observation: hysteresis does not advance');
  assert.match(readLog(state), /sample=ok/);
});

test('13b: reuse is bounded by the window; without it (or past it) the raw stale reading stands', async () => {
  const { state } = freshEnv();
  sampleHostCpu(state, cpus10(0, 0.54));
  const advanced = cpus10(1, 0.54);
  assert.equal(sampleHostCpu(state, advanced).stale, false);
  assert.equal(sampleHostCpu(state, advanced).stale, true, 'no window: today\'s behaviour');
  assert.equal(sampleHostCpu(state, advanced, { reuseWindowMs: 60_000 }).reused, true);
  await new Promise((r) => setTimeout(r, 15));
  assert.equal(sampleHostCpu(state, advanced, { reuseWindowMs: 5 }).stale, true, 'older than the window: not reused');
});

async function withStatusEnv(home, state, fn) {
  const prev = { h: process.env.LANE_BROKER_HOME, s: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await fn();
  } finally {
    process.env.LANE_BROKER_HOME = prev.h;
    process.env.LANE_BROKER_STATE = prev.s;
  }
}

test('status: a capacity-blocked head is reported under capacity "auto" (effective capacity, not the raw string)', async () => {
  const { home, state } = freshEnv();
  const { detectResourceCapacity } = await import('../src/resources.js');
  const cores = detectResourceCapacity().cpuCores;
  // effective capacity = floor(cores - reserve) = 2
  writeGlobalConfig(home, { version: 1, capacity: 'auto', cpuReserveCores: cores - 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, conflictSkipLimit: 1 });
  writeLease(state, heldLease('holder', 'other:key', 2));
  const head = ticket('head', { weight: 1 });
  await enqueue(state, head);
  atomicWriteJson(paths(state).capacitySkipState, { headId: 'head', count: 1, loggedPhase: 'exhausted' });

  const status = await withStatusEnv(home, state, () => collectStatus());
  assert.equal(status.headBlock?.kind, 'capacity', 'the head does not fit 2/2 and its allowance is exhausted');
  assert.equal(status.headBlock.capacity, 2);
});

test('status: reports the resource block (count/limit, reserved, projected vs budget)', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 10, resourceSkipLimit: 3, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  const head = ticket('head', { weight: 4 });
  await enqueue(state, head);
  assert.equal((await withStatusEnv(home, state, () => collectStatus())).resourceBlock, null, 'no record, no block');

  seedRecord(state, 'head', { count: 3, reserved: true });
  const status = await withStatusEnv(home, state, () => collectStatus());
  assert.equal(status.resourceBlock.headId, 'head');
  assert.equal(status.resourceBlock.count, 3);
  assert.equal(status.resourceBlock.limit, 3);
  assert.equal(status.resourceBlock.reserved, true);
  assert.ok(Math.abs(status.resourceBlock.projectedBusy - 9.4) < 1e-9);
  assert.equal(status.resourceBlock.budget, 9);
  assert.match(renderStatusText(status), /resource-blocked: head head projects 9\.40 > budget 9\.00 cores .*backfill 3\/3, RESERVED/);
});

function noTimes(n) {
  return Array.from({ length: n }, () => ({ model: 'test', speed: 0 }));
}

test('13c: reuse is only for validated, unchanged counters: malformed, regressing or re-shaped reads stay unavailable', async () => {
  const window = { reuseWindowMs: 60_000 };
  const fresh = () => {
    const { state } = freshEnv();
    sampleHostCpu(state, cpus10(0, 0.54));
    assert.equal(sampleHostCpu(state, cpus10(1, 0.54)).stale, false, 'fixture: one valid measurement');
    return state;
  };

  const malformed = sampleHostCpu(fresh(), noTimes(10), window);
  assert.equal(malformed.stale, true, 'a malformed read is unavailable, not reuse');
  assert.equal(malformed.reused, undefined);

  const regressed = sampleHostCpu(fresh(), cpus10(0, 0.54), window); // counters went backwards
  assert.equal(regressed.stale, true, 'a counter regression is unavailable, not reuse');
  assert.equal(regressed.reused, undefined);

  const state = fresh();
  const eight = cpus10(1, 0.54).slice(0, 8);
  assert.equal(sampleHostCpu(state, eight, window).stale, true, 'a topology change is unavailable');
  assert.equal(sampleHostCpu(state, eight, window).stale, true, 'and the old measurement is not carried over the new topology');
});
