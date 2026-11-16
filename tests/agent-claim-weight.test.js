import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { makeStubPg } from './helpers/stub-pg.js';
import { LdClient } from '../src/agent/ld-client.js';
import { realSrc, assertCanaryFails } from './helpers/mutant-module.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig } from '../src/config.js';
import { enqueue } from '../src/scheduler.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';

const REQ = '44444444-4444-4444-8444-444444444444';
const CLAIMS = 'agent/claims.js';
const GIB = 2 ** 30;
const CFG = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 'auto', cpuReserveCores: 1, cpuAdmissionPercent: 100, memoryReserveBytes: GIB, maxLaneWeight: 4 };
const HOST = { cpuCores: 9, memoryBytes: 16 * GIB };
const OFFER = { freeCpu: 8, freeMem: 1e9, budgetCpu: 8, freeWeight: 3, weightLimit: 4 };
const WEIGHTING = { head_reserve: true, claim_weight: true };

const record = (subphase, weight, execId = `${subphase}-${weight}`) => ({ execId, subphase, claim: { laneKey: `lk-${execId}`, header: { weight } } });
const queued = (weight, id = `q${weight}`) => ({ id, weight });

async function sentArgs(capacity) {
  const stub = makeStubPg({ answers: { ld_claim: () => ({ status: 'none' }) } });
  const client = new LdClient({ pg: {}, version: 't', connect: stub.connect, backoff: { minMs: 5, maxMs: 20 } });
  try {
    await client.claim(REQ, 8, 1e9, ['k'], null, undefined, capacity);
    return stub.sqlLog.find((c) => c.name === 'ld_claim').args;
  } finally {
    await client.close();
  }
}

test('ld_claim carries the capacity, then the free weight and the broker limit, only when the agent has them', async () => {
  assert.deepEqual(await sentArgs(null), [REQ, 8, '1000000000', ['k']]);
  assert.deepEqual(await sentArgs({ cpu: 12, mem: 2 ** 40 }), [REQ, 8, '1000000000', ['k'], [], 12, String(2 ** 40)], 'a 1099 queue gets the seven-argument call');
  assert.deepEqual(await sentArgs({ cpu: 12, mem: 2 ** 40, freeWeight: 3, weightLimit: 4 }), [REQ, 8, '1000000000', ['k'], [], 12, String(2 ** 40), 3, 4]);
});

test('weights join the capacity only when the queue advertises claim_weight', async () => {
  const { claimCapacity } = await realSrc(CLAIMS);
  assert.deepEqual(claimCapacity(OFFER, [], WEIGHTING), { cpu: 8, mem: 1e9, freeWeight: 3, weightLimit: 4 });
  assert.deepEqual(claimCapacity(OFFER, [], { head_reserve: true }), { cpu: 8, mem: 1e9 }, 'a queue at schema 9 takes no weight');
  assert.equal(claimCapacity(OFFER, [], { host: 'wintop' }), null);
});

test('canary: weights sent to a queue that does not advertise claim_weight fail the advert test', () => assertCanaryFails(CLAIMS,
  ['registered.claim_weight === true && ', ''], async (load) => {
    const { claimCapacity } = await load(CLAIMS);
    assert.deepEqual(claimCapacity(OFFER, [], { head_reserve: true }), { cpu: 8, mem: 1e9 });
  }));

test('shadow mode offers the static budget as free, the whole weight capacity free, and the broker weight limit', async () => {
  const { claimFigures } = await realSrc(CLAIMS);
  assert.deepEqual(await claimFigures({ cfg: CFG, host: HOST }), { freeCpu: 8, freeMem: 15 * GIB, budgetCpu: 8, freeWeight: 8, weightLimit: 4 });
});

const FREE_WEIGHT_CASES = [
  ['nothing held, queued or fetching leaves the whole capacity', { used: 0, queued: [], pendingWeights: [] }, 8],
  ['held leases are charged', { used: 5, queued: [], pendingWeights: [] }, 3],
  ['a ticket queued in the local broker is charged', { used: 2, queued: [queued(2)], pendingWeights: [] }, 4],
  ['an unreadable queue record is skipped', { used: 0, queued: [null, queued(1)], pendingWeights: [] }, 7],
  ['an oversized queued ticket is charged at the broker limit', { used: 0, queued: [queued(7)], pendingWeights: [] }, 4],
  ['pending executions are charged', { used: 1, queued: [], pendingWeights: [2, 1] }, 4],
  ['an oversized pending execution is charged at the broker limit', { used: 0, queued: [], pendingWeights: [7] }, 4],
  ['it never goes below zero', { used: 8, queued: [queued(2)], pendingWeights: [3] }, 0],
];

const freeWeightCase = (name) => async (load) => {
  const [, input, expected] = FREE_WEIGHT_CASES.find(([n]) => n === name);
  const { freeWeightOf } = await load(CLAIMS);
  assert.equal(freeWeightOf({ capacity: 8, cfg: CFG, ...input }), expected);
};

for (const [name] of FREE_WEIGHT_CASES) test(`free weight: ${name}`, () => freeWeightCase(name)(realSrc));

const EXECUTIONS = [{ weight: 2, laneId: null }, { weight: 3, laneId: 'prep' }, { weight: 4, laneId: 'queued' }, { weight: 5, laneId: 'leased' }];

const unadmittedScenario = async (load) => {
  const { unadmittedWeights } = await load(CLAIMS);
  assert.deepEqual(unadmittedWeights(EXECUTIONS, new Set(['queued', 'leased'])), [2, 3], 'no lane yet, and a lane that is neither queued nor leased, count; queued and leased lanes do not');
};

test('an execution counts as unadmitted weight until its lane has a queue record or a lease, matched by lane id', () => unadmittedScenario(realSrc));

test('canary: counting an execution by its subphase instead of its lane fails the unadmitted test', () => assertCanaryFails(CLAIMS,
  ['e.laneId === null || !knownLaneIds.has(e.laneId)', 'e.laneId === null'], unadmittedScenario));

const liveScenario = async (load) => {
  const { liveExecutions } = await load(CLAIMS);
  const root = makeTmpDir('live-executions-');
  fs.mkdirSync(path.join(root, 'tickets', 'RUNNING-4'), { recursive: true });
  fs.writeFileSync(path.join(root, 'tickets', 'RUNNING-4', 'remote-id'), 'lane-4\n');
  const records = [record('FETCHING', 2), record('STARTING', 1), record('SPAWNING', 3), record('RUNNING', 4), record('SETTLING', 9), record('OUTBOX', 5), record('DONE', 6), { execId: 'x', subphase: 'FETCHING', claim: { header: {} } }];
  assert.deepEqual(liveExecutions(records, root), [
    { weight: 2, laneId: null }, { weight: 1, laneId: null }, { weight: 3, laneId: null }, { weight: 4, laneId: 'lane-4' }, { weight: 0, laneId: null },
  ], 'RUNNING counts, with the lane id from remote-id; settling and later subphases do not');
};

test('live executions are those before settling, each with the lane id its remote-id file holds', () => liveScenario(realSrc));

test('canary: leaving RUNNING out of the live executions fails the live-executions test', () => assertCanaryFails(CLAIMS,
  ["new Set(['FETCHING', 'STARTING', 'SPAWNING', 'RUNNING'])", "new Set(['FETCHING', 'STARTING', 'SPAWNING'])"], liveScenario));

async function activeScenario(load) {
  const { claimFigures } = await load(CLAIMS);
  const { state, home } = freshEnv();
  const saved = [process.env.LANE_BROKER_HOME, process.env.LANE_BROKER_STATE];
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeGlobalConfig(home, { schedulerMode: 'active', capacity: 10, maxLaneWeight: 4, cpuAdmissionPercent: 100, cpuReserveCores: 1, memoryReserveBytes: 0 });
    writeLease(state, { id: 'leased', key: 'r:leased', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING });
    await enqueue(state, { id: 'queued', key: 'r:queued', repoId: 'queued', weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' });
    const executions = [{ weight: 2, laneId: 'preparing' }, { weight: 9, laneId: null }, { weight: 1, laneId: 'queued' }, { weight: 1, laneId: 'leased' }];
    const offered = await claimFigures({ cfg: loadGlobalConfig({ home }), executions });
    assert.notEqual(offered, null, 'headroom could be measured');
    assert.deepEqual([offered.freeWeight, offered.weightLimit], [10 - 1 - 1 - 2 - 4, 4], 'capacity 10 less the lease, the queued ticket, the preparing lane and the unstarted one at the limit of 4');
  } finally {
    for (const [key, value] of [['LANE_BROKER_HOME', saved[0]], ['LANE_BROKER_STATE', saved[1]]]) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test('active mode reads the broker: free weight is capacity less leases, the local queue and the executions with no lane in either', () => activeScenario(realSrc));

test('canary: active mode that ignores the local queue fails the active-mode test', () => assertCanaryFails(CLAIMS,
  ['const known = new Set([...leases, ...queued.filter(Boolean)].map((t) => t.id));', 'const known = new Set(leases.map((t) => t.id));'], activeScenario));

test('canary: active mode that ignores the leases fails the active-mode test', () => assertCanaryFails(CLAIMS,
  ['const known = new Set([...leases, ...queued.filter(Boolean)].map((t) => t.id));', 'const known = new Set(queued.filter(Boolean).map((t) => t.id));'], activeScenario));

async function claimedWith(load, { figures, table = [], registered = WEIGHTING, log }) {
  const { claimOnce } = await load(CLAIMS);
  const calls = [];
  const core = { table: { all: () => table, get: () => null, put() {} }, ld: { registered, claim: async (...args) => { calls.push(args); return { status: 'none' }; } } };
  const base = makeTmpDir('claim-weight-');
  const answer = await claimOnce({ core, remoteRoot: path.join(base, 'remote'), figures, block: () => null, log, tools: () => [] });
  return { calls, answer };
}

async function planScenario(load) {
  const seen = [];
  const { calls } = await claimedWith(load, { figures: (ask) => { seen.push(ask); return OFFER; }, table: [record('FETCHING', 2), record('RUNNING', 4), record('DONE', 6)] });
  assert.deepEqual(seen, [{ executions: [{ weight: 2, laneId: null }, { weight: 4, laneId: null }] }], 'the figures are asked with the live executions');
  assert.deepEqual(calls[0][6], { cpu: 8, mem: 1e9, freeWeight: 3, weightLimit: 4 }, 'the offer reaches ld_claim as one capacity');
}

test('claimOnce asks the figures with the live executions and passes the weights inside the capacity', () => planScenario(realSrc));

test('canary: claimOnce that never asks with the live executions fails the plan test', () => assertCanaryFails(CLAIMS,
  ['await figures({ executions: liveExecutions(core.table.all(), remoteRoot) })', 'await figures()'], planScenario));

async function fallbackScenario(load) {
  const logs = [];
  const old = await claimedWith(load, { figures: () => OFFER, registered: { head_reserve: true }, log: (line) => logs.push(line) });
  assert.deepEqual(old.answer, { status: 'none', fallback: true }, 'a queue below migration 010 is a fallback claim');
  await claimedWith(load, { figures: () => OFFER, registered: { head_reserve: true }, log: (line) => logs.push(line) });
  assert.equal(logs.length, 1, 'logged once per process');
  const current = await claimedWith(load, { figures: () => OFFER, registered: WEIGHTING });
  assert.deepEqual(current.answer, { status: 'none' }, 'a queue with the weight figures is not');
}

test('claimOnce flags a claim sent without weight figures because the queue predates migration 010, and logs it once', () => fallbackScenario(realSrc));

test('canary: claimOnce that never flags the fallback fails the fallback test', () => assertCanaryFails(CLAIMS,
  ["capacity?.freeWeight === undefined && core.ld.registered?.claim_weight !== true", 'false'], fallbackScenario));

test('canary: claimOnce that logs the fallback every time fails the fallback test', () => assertCanaryFails(CLAIMS,
  ['fallbackLogged = true;', ''], fallbackScenario));

test('free weight: canaries on the weight arithmetic', async () => {
  await assertCanaryFails(CLAIMS, ['const queuedWeight = queued.filter(Boolean)', 'const queuedWeight = [].filter(Boolean)'], freeWeightCase('a ticket queued in the local broker is charged'));
  await assertCanaryFails(CLAIMS, ['(Number(clampQueuedWeight(t, cfg, capacity).weight) || 0)', '(Number(t.weight) || 0)'], freeWeightCase('an oversized queued ticket is charged at the broker limit'));
  await assertCanaryFails(CLAIMS, ['sum + Math.min(w, limit)', 'sum + w'], freeWeightCase('an oversized pending execution is charged at the broker limit'));
});
