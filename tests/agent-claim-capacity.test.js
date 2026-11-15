import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { makeStubPg, sqlError } from './helpers/stub-pg.js';
import { LdClient } from '../src/agent/ld-client.js';
import { realSrc, assertCanaryFails } from './helpers/mutant-module.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig } from '../src/config.js';
import { enqueue } from '../src/scheduler.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';

const REQ = '44444444-4444-4444-8444-444444444444';
const CLAIMS = 'agent/claims.js';
const CFG = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 'auto', cpuReserveCores: 1, cpuAdmissionPercent: 100, memoryReserveBytes: 2 ** 30, maxLaneWeight: 4 };
const HOST = { cpuCores: 9, memoryBytes: 2 ** 34 };
const OFFER = { freeCpu: 8, freeMem: 1e9, freeWeight: 3, totalCpu: 8, totalMem: 1e9, weightLimit: 4 };

const record = (subphase, weight, execId = `${subphase}-${weight}`) => ({ execId, subphase, claim: { laneKey: `lk-${execId}`, header: { weight } } });
const queued = (weight, id = `q${weight}`) => ({ id, weight });

async function sentCalls(runClaims) {
  const stub = makeStubPg({ answers: { ld_claim: () => ({ status: 'none' }) } });
  const logs = [];
  const client = new LdClient({ pg: {}, version: 't', connect: stub.connect, backoff: { minMs: 5, maxMs: 20 }, log: (line) => logs.push(line) });
  try {
    await runClaims(client, stub);
    return { sent: stub.sqlLog.filter((c) => c.name === 'ld_claim').map((c) => c.args), logs };
  } finally {
    await client.close();
  }
}

test('ld_claim carries the capacity figures after the held keys, memory as a decimal string, and only when the agent offers them', async () => {
  const { sent: bare } = await sentCalls((client) => client.claim(REQ, 8, 1e9, ['k']));
  assert.deepEqual(bare, [[REQ, 8, '1000000000', ['k']]], 'no capacity: the four-argument call an older queue answers');
  const { sent } = await sentCalls((client) => client.claim(REQ, 8, 1e9, ['k'], undefined, { totalCpu: 16, totalMem: 2 ** 40, freeWeight: 3, weightLimit: 4 }));
  assert.deepEqual(sent, [[REQ, 8, '1000000000', ['k'], 16, String(2 ** 40), 3, 4]]);
});

const legacyScenario = async (load) => {
  const { LdClient: Client } = await load('agent/ld-client.js');
  const stub = makeStubPg({ answers: { ld_claim: (args) => { if (args.length > 4) throw sqlError('42883', 'function lane_dispatch.ld_claim does not exist'); return { status: 'none' }; } } });
  const logs = [];
  const client = new Client({ pg: {}, version: 't', connect: stub.connect, backoff: { minMs: 5, maxMs: 20 }, log: (line) => logs.push(line) });
  try {
    const capacity = { totalCpu: 8, totalMem: 1e9, freeWeight: 3, weightLimit: 4 };
    assert.deepEqual(await client.claim(REQ, 8, 1e9, [], undefined, capacity), { status: 'none' });
    assert.deepEqual(await client.claim(REQ, 8, 1e9, [], undefined, capacity), { status: 'none' });
    assert.deepEqual(stub.sqlLog.filter((c) => c.name === 'ld_claim').map((c) => c.args.length), [8, 4, 4], 'one failed eight-argument call, then only four-argument calls');
    assert.equal(logs.length, 1, 'the downgrade is logged once');
  } finally {
    await client.close();
  }
};

test('a queue without migration 008 (42883 on the eight-argument claim) is claimed against in four-argument form, logged once', () => legacyScenario(realSrc));

test('canary: an agent that does not fall back on 42883 fails the legacy-queue test', () => assertCanaryFails('agent/ld-client.js',
  ["if (trailing.length === 0 || err.code !== '42883') throw err;", 'throw err;'], legacyScenario, /42883|does not exist/));

test('canary: an agent that keeps sending the capacity after a 42883 fails the legacy-queue test', () => assertCanaryFails('agent/ld-client.js',
  ['this.#legacyQueue = true;', ''], legacyScenario));

test('shadow mode offers the static budget as both free and total, the whole weight capacity free, and the broker weight limit', async () => {
  const { claimFigures } = await realSrc(CLAIMS);
  assert.deepEqual(await claimFigures({ cfg: CFG, host: HOST }), { freeCpu: 8, freeMem: 2 ** 34 - 2 ** 30, freeWeight: 8, totalCpu: 8, totalMem: 2 ** 34 - 2 ** 30, weightLimit: 4 });
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
  const withLane = (execId, laneId) => {
    fs.mkdirSync(path.join(root, 'tickets', execId), { recursive: true });
    fs.writeFileSync(path.join(root, 'tickets', execId, 'remote-id'), `${laneId}\n`);
  };
  withLane('RUNNING-4', 'lane-4');
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
    const held = { id: 'leased', key: 'r:leased', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING };
    writeLease(state, held);
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

async function claimedWith(load, { figures, table = [] }) {
  const { claimOnce } = await load(CLAIMS);
  const calls = [];
  const core = { table: { all: () => table, get: () => null, put() {} }, ld: { claim: async (...args) => { calls.push(args); return { status: 'none' }; } } };
  const base = makeTmpDir('claim-capacity-');
  await claimOnce({ core, remoteRoot: path.join(base, 'remote'), figures, block: () => null });
  return calls;
}

async function planScenario(load) {
  const seen = [];
  const calls = await claimedWith(load, { figures: (ask) => { seen.push(ask); return OFFER; }, table: [record('FETCHING', 2), record('RUNNING', 4), record('DONE', 6)] });
  assert.deepEqual(seen, [{ executions: [{ weight: 2, laneId: null }, { weight: 4, laneId: null }] }], 'the figures are asked with the live executions');
  assert.deepEqual(calls[0].slice(1, 4), [8, 1e9, ['lk-FETCHING-2', 'lk-RUNNING-4']]);
  assert.deepEqual(calls[0][5], OFFER, 'the whole offer reaches ld_claim as its capacity');
  const bare = await claimedWith(load, { figures: () => ({ freeCpu: 8, freeMem: 1e9 }) });
  assert.equal(bare[0][5], undefined, 'figures without totals send the four-argument call');
}

test('claimOnce asks the figures with the live executions and passes the offer to ld_claim as the capacity', () => planScenario(realSrc));

test('canary: claimOnce that never asks with the live executions fails the plan test', () => assertCanaryFails(CLAIMS,
  ['await figures({ executions: liveExecutions(core.table.all(), remoteRoot) })', 'await figures()'], planScenario));

test('canary: claimOnce that never passes the capacity fails the plan test', () => assertCanaryFails(CLAIMS, ['  }, capacity);', '  });'], planScenario));

test('canary: free weight that ignores the local queue fails its case', () => assertCanaryFails(CLAIMS,
  ['const queuedWeight = queued.filter(Boolean)', 'const queuedWeight = [].filter(Boolean)'], freeWeightCase('a ticket queued in the local broker is charged')));

test('canary: free weight without the broker limit on a queued ticket fails its case', () => assertCanaryFails(CLAIMS,
  ['(Number(clampQueuedWeight(t, cfg, capacity).weight) || 0)', '(Number(t.weight) || 0)'], freeWeightCase('an oversized queued ticket is charged at the broker limit')));

test('canary: free weight without the broker limit on a pending execution fails its case', () => assertCanaryFails(CLAIMS,
  ['sum + Math.min(w, limit)', 'sum + w'], freeWeightCase('an oversized pending execution is charged at the broker limit')));

async function fallbackFlagScenario(load) {
  const { claimOnce } = await load(CLAIMS);
  const { LdClient: Client } = await load('agent/ld-client.js');
  const run = async (answers) => {
    const stub = makeStubPg({ answers });
    const client = new Client({ pg: {}, version: 't', connect: stub.connect, backoff: { minMs: 5, maxMs: 20 } });
    try {
      const core = { table: { all: () => [], get: () => null, put() {} }, ld: client };
      return await claimOnce({ core, remoteRoot: path.join(makeTmpDir('claim-fallback-'), 'remote'), figures: () => OFFER, block: () => null });
    } finally {
      await client.close();
    }
  };
  assert.deepEqual(await run({ ld_claim: () => ({ status: 'none' }) }), { status: 'none' }, 'a queue with migration 008 is not a fallback');
  assert.deepEqual(await run({ ld_claim: (args) => { if (args.length > 4) throw sqlError('42883'); return { status: 'none' }; } }), { status: 'none', fallback: true }, 'the agent counts the fallback claim');
}

test('claimOnce reports a claim that went out in four-argument form because the queue predates migration 008', () => fallbackFlagScenario(realSrc));

test('canary: claimOnce that never reports the fallback fails the fallback-flag test', () => assertCanaryFails(CLAIMS,
  ['const fallback = capacity !== undefined && core.ld.legacyQueue === true;', 'const fallback = false;'], fallbackFlagScenario));
