import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, readResourceSkipState } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths, atomicWriteJson } from '../src/state.js';
import fs from 'node:fs';
import { readLease, writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';

/**
 * BRAIN-630: an aged, reserved head must not starve a ticket of a strictly higher tier. Fixture machine: 10 cores, reserve 1 ->
 * CPU budget 9, weight capacity 10. The head (low, 8 cores, reserved, waiting 90 min) never fits against 6 external cores.
 */

const GIB = 1024 ** 3;
const cfg = (overrides = {}) => ({
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
});
const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t, c = cfg(), ext = 6) => tryStart(state, t, c, undefined, sampler(ext), undefined, memory);

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
  resources: { cpuCores: 1, memoryBytes: GIB },
  ...overrides,
});

async function reservedAgedHead(state) {
  const head = ticket('head', { priorityRequested: 'low', prioOriginAt: Date.now() - 90 * 60_000, resources: { cpuCores: 8, memoryBytes: GIB } });
  await enqueue(state, head);
  atomicWriteJson(paths(state).resourceSkipState, { headId: 'head', count: 3, reserved: true, inScope: true, deniedAt: Date.now(), budget: 9, externalBusy: 6 });
  return head;
}

test('a high ticket that fits starts past an aged, reserved low head, which keeps its reservation', async () => {
  const { state } = freshEnv();
  await reservedAgedHead(state);
  const gate = ticket('gate', { priorityRequested: 'high' });
  await enqueue(state, gate);
  const result = await poll(state, gate);
  assert.equal(result.started, true, JSON.stringify(result));
  assert.ok(readLease(state, 'gate'));
  const record = readResourceSkipState(state);
  assert.equal(record.headId, 'head');
  assert.equal(record.reserved, true);
  assert.equal(record.count, 3, 'the yield is not a skip against the head');
});

test('a lower-tier ticket stays behind a reserved medium head', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticket('head', { priorityRequested: 'medium', prioOriginAt: Date.now() - 90 * 60_000, resources: { cpuCores: 8, memoryBytes: GIB } }));
  atomicWriteJson(paths(state).resourceSkipState, { headId: 'head', count: 3, reserved: true, inScope: true, deniedAt: Date.now(), budget: 9, externalBusy: 6 });
  const low = ticket('low1', { priorityRequested: 'low' });
  await enqueue(state, low);
  const result = await poll(state, low);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'not-head');
});

const hold = (state, id, cpuCores) =>
  writeLease(state, { id, key: `held:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING, resources: { cpuCores, memoryBytes: GIB } });

test('a sustained high stream cannot starve a non-futile reserved head: after resourceSkipLimit yields further ones must leave its claim', async () => {
  const { state } = freshEnv();
  const head = await reservedAgedHead(state);
  const c = cfg({ expressPerMinute: 100 });
  const started = [];
  for (let i = 0; i < 5; i += 1) {
    const gate = ticket(`gate${i}`, { priorityRequested: 'high' });
    await enqueue(state, gate);
    if ((await poll(state, gate, c, 0)).started) started.push(gate.id);
  }
  assert.deepEqual(started, ['gate0', 'gate1', 'gate2'], 'the 4th gate would leave no room for the 8-core head (3 + 8 + 1 > 9)');
  assert.equal(readResourceSkipState(state).yields, 3);
  assert.equal((await poll(state, head, c, 0)).started, false, 'the head still waits for the running gates');
  for (const id of started) removeLease(state, id);
  assert.equal((await poll(state, head, c, 0)).started, true, 'and starts once they finish');
});

test('an express-sized gate past a protected (waited > expressHeadMaxWaitMs) reserved head yields instead of being denied against the head claim', async () => {
  const { state } = freshEnv();
  await reservedAgedHead(state);
  hold(state, 'busy', 2);
  const rows = Array.from({ length: 5 }, (_, i) => ({ id: `q-${i}`, repo: 'r', lane: 'quick', startedAt: 1000 + i, endedAt: 6000 + i, runMs: 5000, exit: 0 }));
  fs.appendFileSync(paths(state).history, rows.map((r) => `${JSON.stringify(r)}\n`).join(''));
  const gate = ticket('gate', { priorityRequested: 'high', repoId: 'r', lane: 'quick' });
  await enqueue(state, gate);
  const result = await poll(state, gate, cfg(), 0);
  assert.equal(result.started, true, JSON.stringify(result));
});

test('a same-tier ticket stays behind the reserved head', async () => {
  for (const tier of ['low']) {
    const { state } = freshEnv();
    await reservedAgedHead(state);
    const other = ticket('other', { priorityRequested: tier });
    await enqueue(state, other);
    const result = await poll(state, other);
    assert.equal(result.started, false, tier);
    assert.equal(result.reason, 'not-head', tier);
  }
});

test('a high ticket that does not fit on its own still waits (the yield is not an admission bypass)', async () => {
  const { state } = freshEnv();
  await reservedAgedHead(state);
  const big = ticket('big', { priorityRequested: 'high', resources: { cpuCores: 4, memoryBytes: GIB } });
  await enqueue(state, big);
  assert.equal((await poll(state, big)).started, false, '6 external + 4 > 9');
});

test('the head still starts first when capacity returns: it is ahead of lower-priority work', async () => {
  const { state } = freshEnv();
  const head = await reservedAgedHead(state);
  const low = ticket('low1', { priorityRequested: 'low' });
  await enqueue(state, low);
  assert.equal((await poll(state, low, cfg(), 0)).started, false, 'low work is not admitted ahead of the head');
  assert.equal((await poll(state, head, cfg(), 0)).started, true, 'the head runs once it fits');
});

test('the express per-minute rate bounds the yield', async () => {
  const { state } = freshEnv();
  await reservedAgedHead(state);
  const c = cfg({ expressPerMinute: 1 });
  const a = ticket('a', { priorityRequested: 'high' });
  const b = ticket('b', { priorityRequested: 'high' });
  await enqueue(state, a);
  await enqueue(state, b);
  assert.equal((await poll(state, a, c)).started, true);
  assert.equal((await poll(state, b, c)).started, false);
});
