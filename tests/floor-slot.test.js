import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, listLeases, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, readJsonSafe } from '../src/state.js';

/**
 * BRAIN-727: with external load over the budget and no lease held, one local push gate whose projection fits within
 * floorOvershootCores starts even behind a head that cannot. Fixture machine: 12 cores, reserve 1 -> CPU budget 11.
 */

const GIB = 1024 ** 3;

const cfg = {
  ...DEFAULT_GLOBAL_CONFIG,
  schedulerMode: 'active',
  capacity: 12,
  maxLaneWeight: 6,
  loadClose: 1000,
  loadOpen: 900,
  loadOpenSamples: 1,
  cpuAdmissionPercent: 100,
  cpuReserveCores: 1,
  admissionCooldownMs: 0,
  resourceSkipLimit: 3,
  resourceIdleOvershootCores: 1,
  floorOvershootCores: 3,
};

const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 12, stale: false, sampledAt: Date.now() });
const memory = (overrides = {}) => () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test', ...overrides });
const poll = (state, t, ext, mem = memory()) => tryStart(state, t, cfg, undefined, sampler(ext), undefined, mem);
const readLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

function ticket(id, overrides = {}) {
  return { id, key: `r:${id}`, lane: 'prepush', weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', ...overrides };
}

async function queueAll(state, tickets) {
  for (const t of tickets) await enqueue(state, t);
}

test('floor slot: with external load over budget and no lease, a 1-core lane-local gate starts behind a 6-core remote-eligible head', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6, remote: { worktreeRoot: '/wt/head' } });
  const gate = ticket('gate');
  await queueAll(state, [head, gate]);
  assert.equal((await poll(state, head, 10.5)).started, false, '10.5 + 6 > 14: the head cannot start');
  const result = await poll(state, gate, 10.5); // 11.5 > 11 but within the 3-core overshoot
  assert.equal(result.started, true);
  assert.equal(result.floor, true);
  assert.deepEqual(listLeases(state).map((l) => l.id), ['gate']);
  assert.deepEqual(listQueue(state).map((t) => t.id), ['head'], 'the head keeps its place');
  assert.match(readLog(state), /candidate=gate .*current=start:floor-slot .* floor=1 /);
  assert.equal(readJsonSafe(paths(state).resourceSkipState)?.count ?? 0, 0, 'no skip allowance is spent');
});

test('floor slot: a second lane-local ticket stays queued while a floor lease is held', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const first = ticket('first');
  const second = ticket('second');
  await queueAll(state, [head, first, second]);
  assert.equal((await poll(state, first, 10.5)).started, true);
  const refused = await poll(state, second, 10.5);
  assert.equal(refused.started, false);
  assert.notEqual(refused.floor, true);
  assert.deepEqual(listLeases(state).map((l) => [l.id, l.floor]), [['first', true]]);
});

test('floor slot: a forced --local ticket and a remote-eligible ticket never get the floor slot', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const forced = ticket('forced', { remote: { worktreeRoot: '/wt/forced' }, localReason: 'forced-flag' });
  const eligible = ticket('eligible', { remote: { worktreeRoot: '/wt/eligible' } });
  await queueAll(state, [head, forced, eligible]);
  for (const t of [forced, eligible]) {
    const result = await poll(state, t, 10.5);
    assert.equal(result.started, false, `${t.id} is denied`);
    assert.notEqual(result.floor, true);
  }
  assert.deepEqual(listLeases(state), []);
});

test('floor slot: applies under a closed CPU gate when the projection fits within the overshoot', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const gate = ticket('gate');
  await queueAll(state, [head, gate]);
  const result = await poll(state, gate, 11.5); // 95.8% busy closes the CPU gate; 12.5 <= 11 + 3
  assert.equal(result.started, true);
  assert.equal(result.floor, true);
  assert.equal(readJsonSafe(paths(state).cpuGate).closed, true, 'the persisted CPU gate is closed');
  assert.match(readLog(state), /candidate=gate .*cpuGate=closed .*floor=1 /);
});

test('floor slot: a later small gate passes an earlier lane-local ticket that does not fit; an earlier memory-denied one does not pin it', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const big = ticket('big', { weight: 4 }); // 10.5 + 4 = 14.5 > 14
  const starved = ticket('starved', { resources: { cpuCores: 1, memoryBytes: 80 * GIB } });
  const small = ticket('small');
  await queueAll(state, [head, big, starved, small]);
  for (const t of [big, starved]) {
    const result = await poll(state, t, 10.5);
    assert.equal(result.started, false, `${t.id} is not the floor ticket`);
    assert.notEqual(result.floor, true);
  }
  const result = await poll(state, small, 10.5);
  assert.equal(result.started, true);
  assert.equal(result.floor, true);
});

const ordinaryLease = (state) =>
  writeLease(state, { id: 'busy', key: 'held:busy', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING });

test('floor slot: applies while ordinary leases are held when the projection including them fits, and not when the head itself can start', async () => {
  const held = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const gate = ticket('gate');
  await queueAll(held.state, [head, gate]);
  ordinaryLease(held.state);
  const started = await poll(held.state, gate, 10); // 10 + 1 (lease) + 1 = 12 <= 11 + 3
  assert.equal(started.started, true);
  assert.equal(started.floor, true);
  assert.deepEqual(listLeases(held.state).map((l) => l.id).sort(), ['busy', 'gate']);

  const tooMuch = freshEnv();
  await queueAll(tooMuch.state, [head, gate]);
  ordinaryLease(tooMuch.state);
  const refused = await poll(tooMuch.state, gate, 12.5); // 12.5 + 1 + 1 = 14.5 > 11 + 3
  assert.equal(refused.started, false);

  const idle = freshEnv();
  await queueAll(idle.state, [head, gate]);
  const behindStartable = await poll(idle.state, gate, 1); // 1 + 6 <= 11: the head starts on its own poll
  assert.equal(behindStartable.started, false);
  assert.equal(behindStartable.reason, 'not-head');
  assert.deepEqual(listLeases(idle.state), []);
  assert.equal((await poll(idle.state, head, 1)).started, true);
});

test('floor slot: an ordinary lease does not count as the floor lease', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const gate = ticket('gate');
  await queueAll(state, [head, gate]);
  ordinaryLease(state);
  assert.equal(listLeases(state)[0].floor, undefined);
  assert.equal((await poll(state, gate, 10)).floor, true, 'the floor slot is free although a lease is held');
});

test('floor slot: a head within the overshoot does not block the floor slot while ordinary leases are held', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 3 });
  const gate = ticket('gate');
  await queueAll(state, [head, gate]);
  ordinaryLease(state);
  // head: 10 + 1 + 3 = 14 <= 11 + 3, but the idle exemption needs an empty broker, so it is denied
  assert.equal((await poll(state, head, 10)).started, false);
  const result = await poll(state, gate, 10);
  assert.equal(result.started, true);
  assert.equal(result.floor, true);
});

test('floor slot: a lane-local ticket picked by express or capacity backfill that fails CPU still gets the floor slot', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const gate = ticket('gate');
  await queueAll(state, [head, gate]);
  // 8 + 6 > 12 capacity: the head is capacity-blocked and capacity backfill picks the gate, which then fails CPU (4 + 8 + 1 = 13 > 11)
  writeLease(state, { id: 'busy', key: 'held:busy', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 8, state: LEASE_STATE.RUNNING });
  const result = await poll(state, gate, 4);
  assert.equal(result.started, true);
  assert.equal(result.floor, true);
  assert.equal(readJsonSafe(paths(state).capacitySkipState)?.count ?? 0, 0, 'no capacity skip is recorded');
});

test('floor slot (BRAIN-740): a prepush head denied by a closed CPU gate starts via the floor slot', async () => {
  const { state } = freshEnv();
  const gate = ticket('gate');
  await queueAll(state, [gate]);
  const result = await poll(state, gate, 11.5); // 95.8% busy closes the gate; 11.5 + 1 - 11 = 1.5 <= 3
  assert.equal(result.started, true);
  assert.equal(result.floor, true);
  assert.deepEqual(listLeases(state).map((l) => [l.id, l.floor]), [['gate', true]]);
});

test('floor slot (BRAIN-740): a prepush behind a blocked non-push head starts via the floor slot while the gate is closed', async () => {
  const { state } = freshEnv();
  const head = ticket('head', { lane: 'local', weight: 6 });
  const gate = ticket('gate', { lane: 'prepush-wide' });
  await queueAll(state, [head, gate]);
  const result = await poll(state, gate, 11.5);
  assert.equal(result.started, true);
  assert.equal(result.floor, true);
  assert.equal(readJsonSafe(paths(state).cpuGate).closed, true);
});

test('floor slot (BRAIN-740): an ordinary lane-local ticket never takes the floor slot', async () => {
  for (const lane of ['local', 'default']) {
    const { state } = freshEnv();
    const head = ticket('head', { lane: 'local', weight: 6 });
    const other = ticket('other', { lane });
    await queueAll(state, [head, other]);
    const result = await poll(state, other, 10.5);
    assert.equal(result.started, false, lane);
    assert.notEqual(result.floor, true);
    assert.deepEqual(listLeases(state), []);
  }
});

test('floor slot (BRAIN-740): denied past floorOvershootCores, and floorOvershootCores 0 disables it', async () => {
  const over = freshEnv();
  const gate = ticket('gate');
  await queueAll(over.state, [gate]);
  assert.equal((await poll(over.state, gate, 13.5)).started, false, '13.5 + 1 - 11 = 3.5 > 3');
  const off = freshEnv();
  await queueAll(off.state, [gate]);
  const result = await tryStart(off.state, gate, { ...cfg, floorOvershootCores: 0 }, undefined, sampler(11.5), undefined, memory());
  assert.equal(result.started, false);
  assert.deepEqual(listLeases(off.state), []);
});

test('floor slot (BRAIN-740): a second push gate waits while a floor lease is held', async () => {
  const { state } = freshEnv();
  const first = ticket('first');
  const second = ticket('second');
  await queueAll(state, [first, second]);
  assert.equal((await poll(state, first, 11.5)).floor, true);
  const refused = await poll(state, second, 11.5);
  assert.equal(refused.started, false);
  assert.deepEqual(listLeases(state).map((l) => l.id), ['first']);
});

test('floor slot (BRAIN-740): a forced --local or remote-eligible prepush head never takes it; memory denial and critical pressure still deny', async () => {
  for (const extra of [{ localReason: 'forced-flag' }, { remote: { worktreeRoot: '/wt/g' } }]) {
    const { state } = freshEnv();
    const gate = ticket('gate', extra);
    await queueAll(state, [gate]);
    assert.equal((await poll(state, gate, 11.5)).started, false);
  }
  const hungry = freshEnv();
  const big = ticket('big', { resources: { cpuCores: 1, memoryBytes: 80 * GIB } });
  await queueAll(hungry.state, [big]);
  assert.equal((await poll(hungry.state, big, 11.5)).started, false, 'memory-denied');
  const critical = freshEnv();
  const gate = ticket('gate');
  await queueAll(critical.state, [gate]);
  assert.equal((await poll(critical.state, gate, 11.5, memory({ macPressure: 'critical' }))).started, false);
  assert.deepEqual(listLeases(critical.state), []);
});

test('floor slot (BRAIN-740): a BRAIN-731-held push gate is never the floor winner', async () => {
  const { state } = freshEnv();
  const gate = (id, gitPid, gitStartEpoch) => ticket(id, { repoId: id, pushGate: { caller: 'c', gitPid, gitStartEpoch, clientPid: gitPid } });
  const first = gate('first', process.pid, 1);
  const second = gate('second', process.pid + 1, 2);
  await queueAll(state, [first, second]);
  assert.equal((await poll(state, first, 11.5)).floor, true);
  const held = await poll(state, second, 11.5);
  assert.equal(held.reason, 'push-hold');
  assert.equal(held.started, false);
  assert.notEqual(held.floor, true);
});
