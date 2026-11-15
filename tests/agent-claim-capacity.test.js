import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { makeStubPg } from './helpers/stub-pg.js';
import { LdClient } from '../src/agent/ld-client.js';
import { realSrc, assertCanaryFails } from './helpers/mutant-module.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

const REQ = '44444444-4444-4444-8444-444444444444';
const CLAIMS = 'agent/claims.js';
const CFG = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 'auto', cpuReserveCores: 1, cpuAdmissionPercent: 100, memoryReserveBytes: 2 ** 30, maxLaneWeight: 4 };
const HOST = { cpuCores: 9, memoryBytes: 2 ** 34 };

const record = (subphase, weight) => ({ subphase, claim: { laneKey: `lk-${subphase}-${weight}`, header: { weight } } });
const queued = (weight, exclusive = false) => ({ id: `q${weight}`, weight, ...(exclusive ? { exclusive: true } : {}) });

async function sentArgs(claimArgs) {
  const stub = makeStubPg();
  const client = new LdClient({ pg: {}, version: 't', connect: stub.connect, backoff: { minMs: 5, maxMs: 20 } });
  try {
    await client.claim(REQ, ...claimArgs);
    return stub.sqlLog.find((c) => c.name === 'ld_claim').args;
  } finally {
    await client.close();
  }
}

test('ld_claim carries the capacity figures after the held keys, memory as a decimal string, and only when the agent offers them', async () => {
  assert.deepEqual(await sentArgs([8, 1e9, ['k']]), [REQ, 8, '1000000000', ['k']], 'no capacity: the four-argument call an older queue answers');
  const capacity = { totalCpu: 16, totalMem: 2 ** 40, freeWeight: 3, totalWeight: 8 };
  assert.deepEqual(await sentArgs([8, 1e9, ['k'], undefined, capacity]), [REQ, 8, '1000000000', ['k'], 16, String(2 ** 40), 3, 8]);
});

test('shadow mode offers the static budget as both free and total, with the whole weight capacity free', async () => {
  const { claimFigures } = await realSrc(CLAIMS);
  const offered = await claimFigures({ cfg: CFG, host: HOST });
  assert.deepEqual(offered, { freeCpu: 8, freeMem: 2 ** 34 - 2 ** 30, freeWeight: 8, totalCpu: 8, totalMem: 2 ** 34 - 2 ** 30, totalWeight: 8 });
});

const FREE_WEIGHT_CASES = [
  ['nothing held, queued or fetching leaves the whole capacity', { used: 0, queued: [], pendingWeights: [] }, 8],
  ['held leases are charged', { used: 5, queued: [], pendingWeights: [] }, 3],
  ['a ticket queued in the local broker is charged', { used: 2, queued: [queued(2)], pendingWeights: [] }, 4],
  ['an unreadable queue record is skipped', { used: 0, queued: [null, queued(1)], pendingWeights: [] }, 7],
  ['an oversized queued ticket is charged at the weight admission clamps it to', { used: 0, queued: [queued(7)], pendingWeights: [] }, 4],
  ['claims still being fetched are charged', { used: 1, queued: [], pendingWeights: [2, 1] }, 4],
  ['it never goes below zero', { used: 8, queued: [queued(2)], pendingWeights: [3] }, 0],
];

const freeWeightCase = (name) => async (load) => {
  const [, input, expected] = FREE_WEIGHT_CASES.find(([n]) => n === name);
  const { freeWeightOf } = await load(CLAIMS);
  assert.equal(freeWeightOf({ capacity: 8, cfg: CFG, ...input }), expected);
};

for (const [name] of FREE_WEIGHT_CASES) test(`free weight: ${name}`, () => freeWeightCase(name)(realSrc));

const unadmittedScenario = async (load) => {
  const { unadmittedWeights } = await load(CLAIMS);
  const records = [record('FETCHING', 2), record('STARTING', 1), record('SPAWNING', 3), record('RUNNING', 4), record('OUTBOX', 5), record('DONE', 6), { subphase: 'FETCHING', claim: { header: {} } }];
  assert.deepEqual(unadmittedWeights(records), [2, 1, 3, 0]);
};

test('only executions the local broker has not seen yet count as unadmitted weight', () => unadmittedScenario(realSrc));

async function claimedWith(load, { figures, table = [] }) {
  const { claimOnce } = await load(CLAIMS);
  const calls = [];
  const core = { table: { all: () => table, get: () => null, put() {} }, ld: { claim: async (...args) => { calls.push(args); return { status: 'none' }; } } };
  const base = makeTmpDir('claim-capacity-');
  const answer = await claimOnce({ core, remoteRoot: path.join(base, 'remote'), figures, block: () => null });
  return { calls, answer };
}

async function weightScenario(load) {
  const seen = [];
  const figures = (pending) => { seen.push(pending); return { freeCpu: 8, freeMem: 1e9, freeWeight: 3, totalCpu: 8, totalMem: 1e9, totalWeight: 8 }; };
  const { calls } = await claimedWith(load, { figures, table: [record('FETCHING', 2), record('RUNNING', 4)] });
  assert.deepEqual(seen, [{ pendingWeights: [2] }], 'the figures are asked with the weight of claims still being fetched');
  assert.deepEqual(calls[0].slice(1, 4), [8, 1e9, ['lk-FETCHING-2', 'lk-RUNNING-4']]);
  assert.deepEqual(calls[0][5], { freeCpu: 8, freeMem: 1e9, freeWeight: 3, totalCpu: 8, totalMem: 1e9, totalWeight: 8 }, 'the whole offer reaches ld_claim as its capacity');
  const bare = await claimedWith(load, { figures: () => ({ freeCpu: 8, freeMem: 1e9 }) });
  assert.equal(bare.calls[0][5], undefined, 'figures without totals send the four-argument call');
}

test('claimOnce asks the figures with the unadmitted weight and passes the offer to ld_claim as the capacity', () => weightScenario(realSrc));

test('canary: claimOnce without the unadmitted weight fails the capacity test', () => assertCanaryFails(CLAIMS,
  ['await figures({ pendingWeights: unadmittedWeights(core.table.all()) })', 'await figures()'], weightScenario, null, /pending|figures/));

test('canary: claimOnce that never passes the capacity fails the capacity test', () => assertCanaryFails(CLAIMS, ['  }, capacity);', '  });'], weightScenario));

test('canary: free weight that ignores the local queue fails its case', () => assertCanaryFails(CLAIMS,
  ['const queuedWeight = queued.filter(Boolean)', 'const queuedWeight = [].filter(Boolean)'], freeWeightCase('a ticket queued in the local broker is charged')));

test('canary: free weight without the admission clamp fails its case', () => assertCanaryFails(CLAIMS,
  ['(Number(clampQueuedWeight(t, cfg, capacity).weight) || 0)', '(Number(t.weight) || 0)'], freeWeightCase('an oversized queued ticket is charged at the weight admission clamps it to')));

test('canary: free weight that ignores claims still being fetched fails its case', () => assertCanaryFails(CLAIMS,
  ['pendingWeights.reduce((sum, w) => sum + w, 0)', '0'], freeWeightCase('claims still being fetched are charged')));

test('canary: an unadmitted filter that counts a RUNNING record fails', () => assertCanaryFails(CLAIMS,
  ["new Set(['FETCHING', 'STARTING', 'SPAWNING'])", "new Set(['FETCHING', 'STARTING', 'SPAWNING', 'RUNNING'])"], unadmittedScenario));
