import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { enqueue } from '../src/scheduler.js';
import { collectStatus } from '../src/status.js';
import { queueFacts } from '../src/queue-facts.js';
import { loadGlobalConfig } from '../src/config.js';
import { remoteResultCommand, remoteProbeCommand } from '../src/remote-runner.js';
import { paths, atomicWriteJson, processStartTime } from '../src/state.js';
import { fenceLegacyQueue } from '../src/migrate.js';

// BRAIN-888: the runner reports advisory queue facts (position, behind another reservation owner, the owner's priority wait).

const GIB = 1024 ** 3;
const T0 = Date.now();
const MIN = 60_000;
const classes = (capCores) => ({ mode: 'active', test: { reserveCores: 0 }, sim: { capCores, maxTickets: 8, capMemoryBytes: 64 * GIB } });
const reserved = (seq) => ({ reason: 'resource', skipsCharged: 3, reserved: true, reservationSeq: seq, inScope: true, behindConflict: false, deniedAt: T0, budget: 4, externalBusy: 2.5 });
const ticket = (id, extra = {}) => ({ id, key: `r:${id}`, weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', resources: { cpuCores: 1, memoryBytes: GIB }, ...extra });
const push = (id, gitPid) => ({ pushGate: { caller: 'c', gitPid, gitStartEpoch: 1, worktree: '/wt', branch: 'b', pushArgsHash: id } });

async function withBroker(config, fn, { fence = true } = {}) {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, schedulerMode: 'active', resourceSkipLimit: 3, ...config });
  if (fence) {
    atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
    fenceLegacyQueue(state, 'test');
  }
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  Object.assign(process.env, { LANE_BROKER_HOME: home, LANE_BROKER_STATE: state });
  try {
    return await fn(state);
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    }
}

const setPrioOrigin = (state, id, prioOriginAt) => {
  const dir = paths(state).queue;
  const file = path.join(dir, fs.readdirSync(dir).find((n) => n.endsWith(`-${id}.json`)));
  atomicWriteJson(file, { ...JSON.parse(fs.readFileSync(file, 'utf8')), prioOriginAt });
};
const ownerThenOurs = async (state) => {
  await enqueue(state, ticket('owner'));
  await enqueue(state, ticket('ours'));
  setPrioOrigin(state, 'owner', Date.now() - 30 * MIN);
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: { owner: { resource: reserved(1) } } });
};
const facts = (state, id) => queueFacts(state, loadGlobalConfig(), id);

test('a queued ticket behind another reservation owner reports position, behindOtherOwner and the owner\'s priority wait', async () => {
  await withBroker({}, async (state) => {
    await ownerThenOurs(state);
    const got = facts(state, 'ours');
    assert.deepEqual([got.position, got.behindOtherOwner], [2, true]);
    assert.ok(Math.abs(got.reservationOwnerWaitedMs - 30 * MIN) < 5000, `waited ${got.reservationOwnerWaitedMs}`);
    const record = JSON.parse(fs.readFileSync(path.join(paths(state).queue, fs.readdirSync(paths(state).queue).find((n) => n.endsWith('-owner.json'))), 'utf8'));
    assert.ok(record.createdAt > record.prioOriginAt + 20 * MIN, 'createdAt and prioOriginAt differ, so the wait is the priority wait');
  });
});

test('a ticket that is itself the reservation owner is not behind another owner', async () => {
  await withBroker({}, async (state) => {
    await ownerThenOurs(state);
    const got = facts(state, 'owner');
    assert.deepEqual([got.position, got.behindOtherOwner, got.reservationOwnerWaitedMs], [1, false, undefined]);
  });
});

test('a push-held or cap-denied ticket has no position and is not reported behind an owner', async () => {
  await withBroker({ classes: classes(1) }, async (state) => {
    await enqueue(state, ticket('first', push('first', 1)));
    await enqueue(state, ticket('held', push('held', 2)));
    await enqueue(state, ticket('sim', { class: 'sim', resources: { cpuCores: 2, memoryBytes: GIB } }));
    for (const id of ['held', 'sim']) {
      const got = facts(state, id);
      assert.deepEqual([got.position, got.behindOtherOwner], [null, undefined], id);
    }
    assert.equal(facts(state, 'first').position, 1);
  });
});

test('legacy mode reports a position but never behindOtherOwner or an owner wait, in the facts, the record and the probe', async () => {
  await withBroker({}, async (state) => {
    await enqueue(state, ticket('owner'));
    await enqueue(state, ticket('ours'));
    const got = facts(state, 'ours');
    assert.deepEqual([got.position, got.behindOtherOwner, got.reservationOwnerWaitedMs, got.v2], [2, undefined, undefined, false]);
    assert.equal((await probe(state)).reservationOwner, undefined);
  }, { fence: false });
});

test('an invalid fairness store omits behindOtherOwner and the probe field', async () => {
  await withBroker({}, async (state) => {
    await ownerThenOurs(state);
    fs.writeFileSync(paths(state).fairness, '{not json');
    assert.equal(facts(state, 'ours').behindOtherOwner, undefined);
    assert.equal((await probe(state)).reservationOwner, undefined);
  });
});

test('the probe reports reservationOwner from a validated v2 read, true with an owner and false without', async () => {
  await withBroker({}, async (state) => {
    await enqueue(state, ticket('lone'));
    atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
    assert.equal((await probe(state)).reservationOwner, false);
    atomicWriteJson(paths(state).fairness, { version: 2, tickets: { lone: { resource: reserved(1) } } });
    assert.equal((await probe(state)).reservationOwner, true);
  });
});

test('a queue read error propagates from queueFacts, so the caller omits every fact', async () => {
  await withBroker({}, async (state) => {
    assert.throws(() => queueFacts(state, loadGlobalConfig(), 'ours', Date.now(), { readQueue: () => { throw new Error('EIO'); } }), /EIO/);
  });
});

async function runnerRecord(state, { laneId, readFacts } = {}) {
  const root = path.join(state, 'remote-root');
  const ticketId = crypto.randomUUID();
  const ticketDir = path.join(root, 'tickets', ticketId);
  fs.mkdirSync(ticketDir, { recursive: true });
  fs.writeFileSync(path.join(ticketDir, 'publisher.json'), JSON.stringify({ pid: process.pid, start: processStartTime(process.pid) }));
  fs.writeFileSync(path.join(ticketDir, 'remote-id'), laneId);
  return JSON.parse(await captureStdout(() => remoteResultCommand(ticketId, { root, ...(readFacts ? { readFacts } : {}) })));
}

async function captureStdout(fn) {
  const write = process.stdout.write;
  let out = '';
  process.stdout.write = (chunk) => { out += chunk; return true; };
  try {
    await fn();
  } finally {
    process.stdout.write = write;
  }
  return out;
}

const probe = async (state) => JSON.parse(await captureStdout(() => remoteProbeCommand({ root: path.join(state, 'remote-root'), measure: async () => null })));

test('remote-result for a queued ticket serves the facts, keyed by the lane id from remote-id', async () => {
  await withBroker({}, async (state) => {
    await ownerThenOurs(state);
    const record = await runnerRecord(state, { laneId: 'ours' });
    assert.deepEqual([record.state, record.position, record.behindOtherOwner], ['queued', 2, true]);
    assert.ok(record.reservationOwnerWaitedMs >= 29 * MIN);
  });
});

test('remote-result serves the plain phase record when the enrichment throws', async () => {
  await withBroker({}, async (state) => {
    await ownerThenOurs(state);
    const record = await runnerRecord(state, { laneId: 'ours', readFacts: () => { throw new Error('boom'); } });
    assert.deepEqual(record, { protocol: 1, missing: true, state: 'queued' });
  });
});

test('parity: the facts order is lane status\'s queue without push-held and cap-denied tickets, past an unreadable barrier', async () => {
  await withBroker({ classes: classes(1) }, async (state) => {
    await enqueue(state, ticket('a', push('a', 1)));
    await enqueue(state, ticket('held', push('held', 2)));
    await enqueue(state, ticket('sim', { class: 'sim', resources: { cpuCores: 2, memoryBytes: GIB } }));
    await enqueue(state, ticket('own', { resources: { cpuCores: 1, memoryBytes: GIB } }));
    await enqueue(state, ticket('z'));
    atomicWriteJson(paths(state).fairness, { version: 2, tickets: { z: { resource: reserved(4) } } });
    const status = await collectStatus();
    const expected = status.queued.filter((q) => q.id !== 'held' && !q.classDenied).map((q) => q.id);
    assert.deepEqual(queueFacts(state, loadGlobalConfig(), 'z').order, expected);
    // collectStatus itself throws on an unreadable record (status.js:278), so the barrier is added after its reading
    fs.writeFileSync(path.join(paths(state).queue, '000000000099-barrier.json'), '{broken');
    const got = queueFacts(state, loadGlobalConfig(), 'z');
    assert.deepEqual(got.order, expected);
    assert.equal(got.ownerId, 'z');
    assert.equal(got.order[0], 'z');
  });
});
