import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { paths, atomicWriteJson } from '../src/state.js';
import { enqueue } from '../src/scheduler.js';
import { advanceHwm, readHwm } from '../src/priority-clock.js';
import { collectStatus, renderStatusText } from '../src/status.js';

// BRAIN-380 slice 1: `lane status` shows each queued ticket's tier and (when aged) its effective rank. The broker is in
// legacy mode, so the queue stays FIFO, says so, and status only ever READS the priority clock.

const MIN = 60_000;
const T0 = 1_700_000_000_000;

async function withStatusEnv(fn) {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE, now: Date.now };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  const clock = { now: T0 };
  Date.now = () => clock.now;
  try {
    return await fn({ state, clock });
  } finally {
    Date.now = prev.now;
    for (const [name, value] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

const ticket = (id, tier) => ({ id, key: `r:${id}`, weight: 1, supervisorPid: process.pid, supervisorStart: null, priorityRequested: tier });

test('legacy mode: status lists the queue in FIFO order with each ticket\'s tier, and says priority is inactive', async () => {
  await withStatusEnv(async ({ state }) => {
    await enqueue(state, ticket('first-low', 'low'));
    await enqueue(state, ticket('second-high', 'high'));
    const status = await collectStatus();
    assert.deepEqual(status.queued.map((q) => [q.id, q.position, q.priority]), [['first-low', 1, 'low'], ['second-high', 2, 'high']], 'FIFO, not score order');
    assert.deepEqual(status.priority, { active: false, mode: 'legacy', nowEff: T0 });
    const text = renderStatusText(status);
    assert.match(text, /^priority: inactive \(legacy scheduler; run lane migrate-scheduler\)$/m);
    assert.match(text, /QUEUE \(FIFO\):\n {2}#1 first-low .*priority=low\n {2}#2 second-high .*priority=high$/m);
    assert.equal(status.queued[0].effectiveRank, undefined, 'a fresh ticket shows no aged rank');
  });
});

test('an aged ticket shows its effective rank; a ticket that has not aged a full step does not', async () => {
  await withStatusEnv(async ({ state, clock }) => {
    await enqueue(state, ticket('old-low', 'low'));
    await enqueue(state, ticket('young-low', 'low'));
    clock.now = T0 + 9 * MIN;
    advanceHwm(state);
    // a ticket created 9 minutes in, so it is 1 minute short of one step at T0 + 10
    const ids = ['old-low', 'young-low'];
    const queueDir = paths(state).queue;
    const young = fs.readdirSync(queueDir).find((n) => n.endsWith('-young-low.json'));
    const record = JSON.parse(fs.readFileSync(`${queueDir}/${young}`, 'utf8'));
    atomicWriteJson(`${queueDir}/${young}`, { ...record, prioOriginAt: T0 + 9 * MIN });
    clock.now = T0 + 10 * MIN;
    const status = await collectStatus();
    const byId = Object.fromEntries(status.queued.map((q) => [q.id, q]));
    assert.deepEqual(ids.map((id) => byId[id].priority), ['low', 'low']);
    assert.equal(byId['old-low'].effectiveRank, 'medium', 'waited 10:00, one tier gained');
    assert.equal(byId['young-low'].effectiveRank, undefined);
    assert.match(renderStatusText(status), /old-low .*priority=low \(aged to medium\)/);
    assert.doesNotMatch(renderStatusText(status), /young-low .*aged/);
  });
});

test('status reads the persisted mark but never writes it, and ages from nowEff during a clock rollback', async () => {
  await withStatusEnv(async ({ state, clock }) => {
    await enqueue(state, ticket('t', 'low'));
    clock.now = T0 + 12 * MIN;
    advanceHwm(state);
    const hwmBefore = fs.readFileSync(paths(state).hwm, 'utf8');
    clock.now = T0 - 30 * MIN; // the wall clock steps back
    const status = await collectStatus();
    assert.equal(status.priority.nowEff, T0 + 12 * MIN, 'nowEff = max(wall, persisted mark)');
    assert.equal(status.queued[0].effectiveRank, 'medium', 'the ticket keeps its 12 minutes of age');
    clock.now = T0 + 30 * MIN; // and forward: still no write
    await collectStatus();
    assert.equal(fs.readFileSync(paths(state).hwm, 'utf8'), hwmBefore, 'status never advances the mark');
    assert.equal(readHwm(state), T0 + 12 * MIN);
  });
});

test('status on a state root that has never written a mark does not create one', async () => {
  await withStatusEnv(async ({ state }) => {
    await collectStatus();
    assert.equal(fs.existsSync(paths(state).hwm), false);
  });
});
