import assert from 'node:assert/strict';
import test from 'node:test';
import { realSrc, assertCanaryFails } from './helpers/mutant-module.js';

const GIB = 1024 ** 3;
const LEASE = { id: 'L1', resources: { cpuCores: 2, memoryBytes: 4 * GIB } };

async function run(load, { sampleMs, samples, lease = LEASE }) {
  const { overBudgetShadow } = await load('over-budget.js');
  let current = lease;
  const lines = [];
  for (const [at, cores, memoryBytes] of samples) {
    const { fields, line } = overBudgetShadow(current, { cores, memoryBytes, now: at, sampleMs });
    current = { ...current, ...fields };
    if (line) lines.push(line);
  }
  return { lease: current, lines };
}

const held = (sampleMs, forMs, memoryBytes, cores = 1) => Array.from({ length: forMs / sampleMs + 1 }, (_, i) => [1_000_000 + i * sampleMs, cores, memoryBytes]);

async function sustainedScenario(load) {
  const { lease, lines } = await run(load, { sampleMs: 20_000, samples: held(20_000, 80_000, 9 * GIB) });
  assert.match(lease.overBudgetWouldKill ?? '', /over budget mem/, 'a lease at over twice its memory for 60 s is a would-kill, even sampled every 20 s');
  assert.deepEqual(lines, ['lane: over-budget (shadow): would kill L1: over budget mem 9G/4G for 60s\n'], 'logged once per streak, when it first reaches kill');
}

test('memory held over twice the reservation for 60 s is logged once as a would-kill, at the host sampleMs', () => sustainedScenario(realSrc));

test('canary: the default 15 s maxGap breaks a 20 s-sampled streak and fails the sustained test', () => assertCanaryFails('over-budget.js',
  ['{ ...OVER_BUDGET_POLICY, maxGapMs: 2 * sampleMs }', 'OVER_BUDGET_POLICY'], sustainedScenario));

test('a spike shorter than 60 s, a gap of more than two samples, or an unknown grant never reaches kill', async () => {
  const short = await run(realSrc, { sampleMs: 5_000, samples: held(5_000, 40_000, 9 * GIB) });
  assert.equal(short.lease.overBudgetWouldKill, null);
  const gapped = held(5_000, 75_000, 9 * GIB).filter(([at]) => at !== 1_030_000 && at !== 1_035_000 && at !== 1_040_000);
  assert.equal((await run(realSrc, { sampleMs: 5_000, samples: gapped })).lease.overBudgetWouldKill, null, 'a 20 s gap at 5 s sampling ends the streak');
  const noGrant = await run(realSrc, { sampleMs: 5_000, samples: held(5_000, 75_000, 9 * GIB), lease: { id: 'L2', resources: {} } });
  assert.equal(noGrant.lease.overBudgetWouldKill, null);
  assert.deepEqual(noGrant.lines, []);
});

test('a verdict that throws leaves the lease untouched and logs nothing', async () => {
  const { overBudgetShadow } = await realSrc('over-budget.js');
  assert.deepEqual(overBudgetShadow(LEASE, { cores: 1, memoryBytes: GIB, now: 1, sampleMs: 5_000 }, () => { throw new Error('boom'); }), { fields: {}, line: null });
});
