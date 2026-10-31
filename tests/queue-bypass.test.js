import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig } from '../src/config.js';
import { writeLease, listLeases, LEASE_STATE } from '../src/lease.js';
import { bootId, paths } from '../src/state.js';
import { cooldownActive } from '../src/admission.js';
import { buildBypassTable, bypassVerdict, commandFamily } from '../src/express.js';
import { historyRow } from '../src/supervisor.js';
import { collectStatus } from '../src/status.js';

/**
 * BRAIN-756: queue bypass. Fixture machine: 10 cores, reserve 1 -> CPU budget 9, gate closes at 90% busy. A sample of 9.5
 * busy cores keeps the CPU gate shut, so only the bypass can start `pnpm test` (10 s median over seeded history).
 */

const GIB = 1024 ** 3;
const MAX_MS = 60_000;
const CAP = 3;

const baseCfg = (overrides = {}) => ({
  ...DEFAULT_GLOBAL_CONFIG,
  schedulerMode: 'active',
  capacity: 6,
  loadClose: 1000,
  loadOpen: 900,
  loadOpenSamples: 1,
  cpuAdmissionPercent: 100,
  cpuReserveCores: 1,
  cpuOpenSamples: 1,
  admissionCooldownMs: 0,
  expressMaxMs: 0,
  floorOvershootCores: 0,
  queueBypassCap: CAP,
  queueBypassMaxMs: MAX_MS,
  ...overrides,
});

const sampler = (hostBusyCores = 9.5) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = (overrides = {}) => () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test', ...overrides });
const poll = (state, t, cfg = baseCfg(), busy = 9.5, mem = memory()) => tryStart(state, t, cfg, undefined, sampler(busy), undefined, mem);
const readLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

const ticket = (id, overrides = {}) => ({
  id,
  key: `r:${id}`,
  repoId: 'r',
  lane: 'quick',
  weight: 1,
  cwd: process.cwd(),
  cmd: ['pnpm', 'test'],
  command: 'pnpm test',
  cmdFingerprint: 'fp',
  supervisorPid: process.pid,
  supervisorStart: null,
  logPath: '/dev/null',
  resultPath: '/dev/null',
  ...overrides,
});

const held = (id, overrides = {}) => ({ id, key: `r:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), admittedAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING, ...overrides });
const bypassLease = (id, ageMs = 1000, overrides = {}) => held(id, { admittedAt: Date.now() - ageMs, queueBypass: { expectedMs: 10_000, source: 'command' }, ...overrides });

const seed = (state, rows) => fs.appendFileSync(paths(state).history, rows.map((r) => `${JSON.stringify(r)}\n`).join(''));
const run = (i, overrides = {}) => ({ id: `h${i}`, repo: 'r', lane: 'quick', command: 'pnpm test', cmdFingerprint: 'fp', startedAt: 1000 + i, endedAt: 1000 + i + 10_000, runMs: 10_000, exit: 0, ...overrides });
const seedRuns = (state, n = 5, overrides = {}) => seed(state, Array.from({ length: n }, (_, i) => run(i, overrides)));

async function queued(state, t) {
  await enqueue(state, t);
  return t;
}

test('a cached-pass row (no real run) leaves the strike window alone', () => {
  const longRuns = Array.from({ length: 4 }, (_, i) => run(100 + i, { runMs: 90_000, endedAt: 1100 + i + 90_000 }));
  const cachedRows = Array.from({ length: 20 }, (_, i) => run(200 + i, { runMs: 0, startedAt: 500_000 + i, endedAt: 500_000 + i, cachedFrom: 'x' }));
  const entry = Object.values(buildBypassTable([...Array.from({ length: 5 }, (_, i) => run(i)), ...longRuns, ...cachedRows], MAX_MS))[0];
  assert.equal(entry.strikes, 4);
});

test('a closed CPU gate does not hold a ticket whose command history is under 60 s; lease, log and event say why', async () => {
  const { state } = freshEnv();
  seedRuns(state);
  const t = await queued(state, ticket('a'));
  const result = await poll(state, t);
  assert.equal(result.started, true);
  assert.deepEqual(result.lease.queueBypass, { expectedMs: 10_000, source: 'command' });
  assert.deepEqual(listLeases(state).map((l) => l.queueBypass?.source), ['command']);
  assert.match(readLog(state), /candidate=a .*current=start:queue-bypass .*cpuGate=closed .* bypass=1 /);
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /event=queue-bypass headId=a candidateId=a expectedMs=10000 source=command inWindow=1\/3 cpuReason=cpu-gate-closed/);
});

test('the terminal history row carries the lease queueBypass', () => {
  const lease = bypassLease('a');
  const row = historyRow(ticket('a'), { id: 'a', exit: 0, startedAt: 1, endedAt: 2 }, { lease });
  assert.deepEqual(row.queueBypass, { expectedMs: 10_000, source: 'command' });
  assert.equal('queueBypass' in historyRow(ticket('a'), { id: 'a', exit: 0 }, { lease: held('a') }), false);
});

test('runner default is off: queueBypassCap 0 never bypasses', async () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.queueBypassCap, 0);
  const { state } = freshEnv();
  seedRuns(state);
  const t = await queued(state, ticket('a'));
  assert.equal((await poll(state, t, baseCfg({ queueBypassCap: 0 }))).started, false);
});

test('a ticket with no history, or only the lane median, is not bypassed', async () => {
  const none = freshEnv().state;
  const a = await queued(none, ticket('a'));
  assert.equal((await poll(none, a)).started, false);

  const laneOnly = freshEnv().state;
  seedRuns(laneOnly, 6, { command: undefined, cmdFingerprint: undefined });
  const b = await queued(laneOnly, ticket('b'));
  assert.equal((await poll(laneOnly, b)).started, false);
});

test('a command family with enough runs predicts a ticket whose exact command is new', () => {
  assert.equal(commandFamily('FOO=1 pnpm -r test src/a.test.js'), 'pnpm test');
  const table = { bypass: buildBypassTable(Array.from({ length: 4 }, (_, i) => run(i, { cmdFingerprint: `other${i}`, command: 'pnpm test --silent ./x' })), MAX_MS) };
  const cfg = baseCfg();
  assert.deepEqual(bypassVerdict(table, ticket('a', { cmdFingerprint: 'new' }), cfg), { ms: 10_000, source: 'family' });
  assert.equal(bypassVerdict(table, ticket('a', { cmd: ['pnpm', 'lint'], command: 'pnpm lint' }), cfg), null);
});

test('more than 3 of the last 20 runs over 60 s refuse the bypass, and failed and cancelled runs count as strikes', async () => {
  for (const strike of [{ runMs: 120_000 }, { exit: 1, runMs: 120_000 }, { cancelled: true, exit: null, runMs: 120_000 }, { signal: 'SIGTERM', exit: null, runMs: 120_000 }]) {
    const { state } = freshEnv();
    seedRuns(state, 16);
    seed(state, [0, 1, 2, 3].map((i) => run(100 + i, strike)));
    const t = await queued(state, ticket('a'));
    assert.equal((await poll(state, t)).started, false, JSON.stringify(strike));
  }
  const three = freshEnv().state;
  seedRuns(three, 17);
  seed(three, [0, 1, 2].map((i) => run(100 + i, { exit: 1, runMs: 120_000 })));
  const ok = await queued(three, ticket('a'));
  assert.equal((await poll(three, ok)).started, true, 'exactly 3 strikes is allowed');
});

test('a median at or over the threshold is not bypassed', async () => {
  const { state } = freshEnv();
  seed(state, [run(0), run(1, { runMs: 70_000 }), run(2, { runMs: 70_000 })]);
  const t = await queued(state, ticket('a'));
  assert.equal((await poll(state, t)).started, false);
});

test('a held lease on the same key, a memory denial, an exclusive head and a train lane each refuse', async () => {
  const same = freshEnv().state;
  seedRuns(same);
  writeLease(same, held('holder', { key: 'r:a' }));
  await queued(same, ticket('head', { key: 'r:head', cmdFingerprint: 'h', command: 'h' }));
  assert.equal((await poll(same, await queued(same, ticket('a')))).started, false, 'same key');

  const heavy = freshEnv().state;
  seedRuns(heavy);
  writeLease(heavy, held('holder', { weight: 6 }));
  await queued(heavy, ticket('head', { key: 'r:head', cmdFingerprint: 'h', command: 'h' }));
  assert.equal((await poll(heavy, await queued(heavy, ticket('a')))).reason, 'not-head', 'weight capacity');

  const mem = freshEnv().state;
  seedRuns(mem);
  assert.equal((await poll(mem, await queued(mem, ticket('a')), baseCfg(), 9.5, memory({ availableBytes: 1 }))).started, false, 'memory');

  const exHead = freshEnv().state;
  seedRuns(exHead);
  await queued(exHead, ticket('x', { exclusive: true, key: 'r:x', cmdFingerprint: 'other', command: 'other' }));
  assert.equal((await poll(exHead, await queued(exHead, ticket('a')))).started, false, 'exclusive head');

  const exHeld = freshEnv().state;
  seedRuns(exHeld);
  writeLease(exHeld, held('holder', { exclusive: true, weight: 6 }));
  assert.equal((await poll(exHeld, await queued(exHeld, ticket('a')))).started, false, 'exclusive held');

  const exBehindHead = freshEnv().state;
  seedRuns(exBehindHead);
  await queued(exBehindHead, ticket('head', { key: 'r:head', weight: 4, cmdFingerprint: 'h', command: 'h' }));
  await queued(exBehindHead, ticket('x', { exclusive: true, key: 'r:x', cmdFingerprint: 'other', command: 'other' }));
  assert.equal((await poll(exBehindHead, await queued(exBehindHead, ticket('a')))).started, false, 'exclusive queued ahead');

  const train = freshEnv().state;
  seedRuns(train, 5, { lane: 'train' });
  assert.equal((await poll(train, await queued(train, ticket('a', { lane: 'train' })))).started, false, 'train lane');
});

test('an unavailable CPU sample never lifts the denial', async () => {
  const { state } = freshEnv();
  seedRuns(state);
  writeLease(state, held('other'));
  const t = await queued(state, ticket('a'));
  const result = await tryStart(state, t, baseCfg(), undefined, () => null, undefined, memory());
  assert.equal(result.started, false);
  assert.equal(result.cpuReason, 'sample-unavailable-held');
});

test('the cap holds: in-window leases at the cap, or overran leases at the cap, refuse; past-window leases free the slot', async () => {
  const full = freshEnv().state;
  seedRuns(full);
  for (let i = 0; i < CAP; i += 1) writeLease(full, bypassLease(`b${i}`));
  assert.equal((await poll(full, await queued(full, ticket('a')))).started, false, 'in-window == cap');

  const overran = freshEnv().state;
  seedRuns(overran);
  for (let i = 0; i < CAP; i += 1) writeLease(overran, bypassLease(`b${i}`, MAX_MS + 5000));
  assert.equal((await poll(overran, await queued(overran, ticket('a')))).started, false, 'overran == cap');

  const aged = freshEnv().state;
  seedRuns(aged);
  writeLease(aged, bypassLease('b0'));
  for (let i = 1; i < CAP; i += 1) writeLease(aged, bypassLease(`b${i}`, MAX_MS + 5000));
  assert.equal((await poll(aged, await queued(aged, ticket('a')))).started, true, 'two aged out: a slot is free');
});

test('staggered ageing never holds more than 2 x cap - 1 bypass leases', async () => {
  const cap = 2;
  const cfg = baseCfg({ queueBypassCap: cap });
  const { state } = freshEnv();
  seedRuns(state);
  writeLease(state, bypassLease('old', MAX_MS + 5000));
  writeLease(state, bypassLease('fresh'));
  assert.equal((await poll(state, await queued(state, ticket('a')), cfg)).started, true);
  assert.equal(listLeases(state).length, 2 * cap - 1);
  assert.equal((await poll(state, await queued(state, ticket('b')), cfg)).started, false, 'in-window at cap');
  writeLease(state, bypassLease('fresh', MAX_MS + 5000));
  assert.equal((await poll(state, await queued(state, ticket('c')), cfg)).started, false, 'overran at cap');
  assert.equal(listLeases(state).length, 2 * cap - 1);
});

test('concurrent polls never exceed the cap', async () => {
  const { state } = freshEnv();
  seedRuns(state);
  const tickets = [];
  for (let i = 0; i < 8; i += 1) tickets.push(await queued(state, ticket(`t${i}`)));
  await Promise.all(tickets.map((t) => poll(state, t)));
  assert.equal(listLeases(state).filter((l) => l.queueBypass).length, CAP);
});

test('a bypass lease does not start the admission cooldown for ordinary tickets', async () => {
  const cfg = baseCfg({ admissionCooldownMs: 60_000 });
  assert.equal(cooldownActive([bypassLease('b')], cfg), false);
  assert.equal(cooldownActive([held('o')], cfg), true);
  const { state } = freshEnv();
  writeLease(state, bypassLease('b', 100));
  const ordinary = await queued(state, ticket('plain', { cmdFingerprint: 'none', command: 'none' }));
  assert.equal((await poll(state, ordinary, cfg, 1)).started, true);
});

test('a dequeue that fails refuses the start', async () => {
  const { state } = freshEnv();
  seedRuns(state);
  const t = await queued(state, ticket('a'));
  fs.chmodSync(paths(state).queue, 0o555);
  try {
    const result = await poll(state, t);
    assert.equal(result.started, false);
    assert.equal(result.reason, 'not-head');
    assert.deepEqual(listLeases(state), []);
  } finally {
    fs.chmodSync(paths(state).queue, 0o755);
  }
});

test('a bypass is not an express start: no express counter is written', async () => {
  const { state } = freshEnv();
  seedRuns(state);
  writeLease(state, held('holder', { key: 'r:sim' }));
  await queued(state, ticket('head', { key: 'r:head', lane: 'default', conflicts: ['r:sim'], cmdFingerprint: 'h', command: 'h' }));
  const t = await queued(state, ticket('a'));
  const cfg = baseCfg({ expressMaxMs: 120_000, conflictSafeBackfill: false });
  const result = await poll(state, t, cfg);
  assert.equal(result.started, true);
  assert.equal(fs.existsSync(paths(state).expressAdmissions), false);
  assert.doesNotMatch(readLog(state), /lane-broker-express/);
});

test('a real push gate waiting for the floor slot keeps it: the bypass is refused until the floor is off', async () => {
  const push = ticket('push', { lane: 'prepush', cmdFingerprint: 'p', command: 'p', pushGate: { caller: 'push', gitPid: 4242, gitStartEpoch: 1, worktree: '/wt/push', branch: 'push', pushArgsHash: 'push' } });
  const cfg = baseCfg({ floorOvershootCores: 3 });
  const { state } = freshEnv();
  seedRuns(state);
  await queued(state, ticket('head', { key: 'r:head', cmdFingerprint: 'h', command: 'h' }));
  await queued(state, push);
  const t = await queued(state, ticket('a'));
  assert.equal((await poll(state, t, cfg)).started, false, 'the floor winner is the push gate');
  assert.equal((await poll(state, t, baseCfg())).started, true, 'floor slot off: the bypass proceeds');
});

const agedHead = (extra = {}) => ticket('head', { key: 'r:head', cmdFingerprint: 'h', command: 'h', resources: { cpuCores: 4 }, prioOriginAt: Date.now() - 2 * 900_000, ...extra });

test('an aged head held back only by bypass leases is not delayed further; external load alone does not refuse', async () => {
  const observed = (cores) => bypassLease('b0', 1000, { observedCpuCores: cores, observedAt: Date.now() });

  const drained = freshEnv().state;
  seedRuns(drained);
  writeLease(drained, observed(6));
  await queued(drained, agedHead());
  const a = await queued(drained, ticket('a'));
  assert.equal((await poll(drained, a)).started, false, '9.5 busy of which 6 is the bypass lease: the head would start without it');

  const external = freshEnv().state;
  seedRuns(external);
  writeLease(external, observed(1));
  await queued(external, agedHead());
  const b = await queued(external, ticket('b'));
  assert.equal((await poll(external, b)).started, true, '8.5 external cores keep the gate shut whatever the bypass does');

  const shut = freshEnv().state;
  seedRuns(shut);
  writeLease(shut, observed(1.5));
  await queued(shut, agedHead({ resources: { cpuCores: 0.5 } }));
  const d = await queued(shut, ticket('d'));
  assert.equal((await poll(shut, d)).started, true, '8 external cores of 10 keep the gate shut although the head would fit');

  const young = freshEnv().state;
  seedRuns(young);
  writeLease(young, observed(6));
  await queued(young, agedHead({ prioOriginAt: Date.now() }));
  const c = await queued(young, ticket('c'));
  assert.equal((await poll(young, c)).started, true, 'a head that has not waited long enough is not protected');
});

const withEnv = async ({ home, state }, fn) => {
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  Object.assign(process.env, { LANE_BROKER_HOME: home, LANE_BROKER_STATE: state });
  try {
    return await fn();
  } finally {
    process.env.LANE_BROKER_HOME = prev.home;
    process.env.LANE_BROKER_STATE = prev.state;
  }
};

test('an aged head: a bypass on a key it conflicts with refuses; an unobserved bypass lease and an elastic floor both count as drainable', async () => {
  const rotating = freshEnv().state;
  seedRuns(rotating);
  writeLease(rotating, bypassLease('b0', 1000, { key: 'r:b' }));
  await queued(rotating, agedHead({ conflicts: ['r:b', 'r:a'] }));
  assert.equal((await poll(rotating, await queued(rotating, ticket('a')))).started, false, 'a replacement on a key the aged head conflicts with');

  const unobserved = freshEnv().state;
  seedRuns(unobserved);
  writeLease(unobserved, bypassLease('b0', 1000, { weight: 6, resources: { cpuCores: 6 } }));
  await queued(unobserved, agedHead());
  const u = await queued(unobserved, ticket('u'));
  assert.equal((await poll(unobserved, u, baseCfg({ capacity: 12 }))).started, false, 'its charge leaves externalBusy when drained');

  const elastic = freshEnv().state;
  seedRuns(elastic);
  writeLease(elastic, held('ordinary', { weight: 1, resources: { cpuCores: 4 }, observedCpuCores: 4, observedAt: Date.now() }));
  writeLease(elastic, bypassLease('b0', 1000, { observedCpuCores: 4.5, observedAt: Date.now() }));
  await queued(elastic, agedHead({ resources: { cpuCores: 8, minCpuCores: 1 } }));
  const e = await queued(elastic, ticket('e'));
  assert.equal((await poll(elastic, e, baseCfg(), 8.5)).started, false, 'the head fits at its floor once drained');
});

test('lane status tags a held in-window bypass lease', async () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { version: 1, schedulerMode: 'active', queueBypassCap: CAP });
  writeLease(env.state, bypassLease('b0'));
  writeLease(env.state, bypassLease('b1', MAX_MS + 5000));
  const status = await withEnv(env, () => collectStatus());
  assert.deepEqual(status.running.map((r) => [r.id, r.queueBypass === true]).sort(), [['b0', true], ['b1', false]]);
});

test('queueBypassCap and queueBypassMaxMs must be non-negative integers', async () => {
  const env = freshEnv();
  await withEnv(env, () => {
    for (const name of ['queueBypassCap', 'queueBypassMaxMs']) {
      writeGlobalConfig(env.home, { version: 1, [name]: 0 });
      assert.equal(loadGlobalConfig()[name], 0);
      for (const bad of [-1, 1.5, '60']) {
        writeGlobalConfig(env.home, { version: 1, [name]: bad });
        assert.throws(() => loadGlobalConfig(), new RegExp(`"${name}" must be a non-negative integer`));
      }
    }
  });
});
