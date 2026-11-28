import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import test from 'node:test';
import { MAX_USAGE_ROOTS, requestSocket } from '@andrewpopov/bosun/module';
import { createFakeBudget } from '@andrewpopov/bosun/testing/budget';
import { capByGrant, openBudgetShadow, rootsReader, standaloneBudget, USAGE_HEARTBEAT_MS } from '../src/agent/budget.js';
import { claimFigures } from '../src/agent/claims.js';
import { renderAgentMetrics } from '../src/agent/metrics.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { waitFor } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { processStartTime, processStartTimes } from '../src/process-liveness.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';

const GIB = 1024 ** 3;
const HOST = { cpuCores: 8, memoryBytes: 16 * GIB };
const CFG = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow' };
const GRANT = { cpu: { free: 3.5, ceiling: 6.5 }, mem: { free: 8 * GIB, ceiling: 12 * GIB } };
const STANDALONE = standaloneBudget({ cfg: CFG, host: HOST });

function scheduler(start = 1_000_000) {
  let clock = start;
  const timers = new Set();
  return {
    now: () => clock,
    setTimer(ms, fn) { const timer = { due: clock + ms, fn }; timers.add(timer); return timer; },
    clear(timer) { timers.delete(timer); },
    advance(ms) {
      clock += ms;
      for (const timer of [...timers]) if (timer.due <= clock) { timers.delete(timer); timer.fn(); }
    },
  };
}

const bosunIdentities = async (pids) => new Map(pids.filter((pid) => pid !== 99).map((pid) => [pid, `linux:boot-1:${pid * 10}`]));
const ROOTS = { startsOf: bosunIdentities, lstartsOf: async () => new Map(), pidAlive: () => true };

const record = (execId, extra) => ({ execId, subphase: 'RUNNING', claim: { cpuCores: 2, header: { memoryBytes: GIB } }, ...extra });

async function harness(t, { bosun = true, records = () => [] } = {}) {
  const clock = scheduler();
  const socketPath = join(makeTmpDir('lb-budget-', t), 'b.sock');
  const fake = await createFakeBudget({ socketPath, elastic: ['lane'], token: 'tok', timers: clock, budget: GRANT });
  const env = bosun ? { BOSUN_SOCKET: socketPath, BOSUN_MODULE: 'lane', BOSUN_TOKEN: 'tok', BOSUN_INCARNATION: '7' } : {};
  const logs = [];
  const puts = [];
  const call = (socket, options) => {
    if (options.method !== 'PUT') return requestSocket(socket, options);
    puts.push(options);
    return Promise.resolve({ status: 204 });
  };
  const shadow = openBudgetShadow({ readConfig: () => CFG, host: () => HOST, records, log: (line) => logs.push(line), now: clock.now, env, call, rootOptions: ROOTS });
  t.after(async () => { await shadow.close(); await fake.close(); });
  return { shadow, clock, fake, logs, puts };
}

const series = (text, figure) => text.split('\n').find((line) => line.startsWith(`lane_agent_budget_shadow{figure="${figure}"`));

test('bosun answers: the shadow figures are its grant, source=bosun, and the metric carries both labels', async (t) => {
  const { shadow } = await harness(t);
  await waitFor(() => shadow.shadow().source === 'bosun');
  const sample = shadow.shadow();
  assert.deepEqual(sample.figures, { cpu_free: 3.5, cpu_ceiling: 6.5, mem_free: 8 * GIB, mem_ceiling: 12 * GIB, items: 6 });
  const text = renderAgentMetrics({ now: 0, records: [], claimed: 0, claimAttempts: {}, version: '1', budgetShadow: sample });
  assert.equal(series(text, 'cpu_ceiling'), 'lane_agent_budget_shadow{figure="cpu_ceiling",source="bosun"} 6.5');
  assert.equal(series(text, 'items'), 'lane_agent_budget_shadow{figure="items",source="bosun"} 6');
});

test('bosun absent: standalone figures from the config keys, source=standalone, nothing sent, and claimFigures is what it always was', async (t) => {
  const { shadow, puts } = await harness(t, { bosun: false, records: () => [record('a', { pid: 10, pidStart: 'x' })] });
  await shadow.heartbeat();
  assert.deepEqual(puts, []);
  const sample = shadow.shadow();
  assert.equal(sample.source, 'standalone');
  assert.deepEqual(sample.figures, { cpu_free: STANDALONE.cpu.free, cpu_ceiling: STANDALONE.cpu.ceiling, mem_free: STANDALONE.mem.free, mem_ceiling: STANDALONE.mem.ceiling, items: Math.floor(STANDALONE.cpu.ceiling) });
  const offered = await claimFigures({ cfg: CFG, host: HOST });
  assert.deepEqual(offered, { freeCpu: STANDALONE.cpu.free, freeMem: STANDALONE.mem.free, budgetCpu: STANDALONE.cpu.ceiling, freeWeight: offered.freeWeight, weightLimit: offered.weightLimit });
  const text = renderAgentMetrics({ now: 0, records: [], claimed: 0, claimAttempts: {}, version: '1', budgetShadow: sample });
  assert.match(series(text, 'cpu_ceiling'), /source="standalone"/);
});

test('a grant that expires without a renewal falls back to the standalone budget once it expires', async (t) => {
  const { shadow, clock, fake, logs } = await harness(t);
  await waitFor(() => shadow.shadow().source === 'bosun');
  await fake.close();
  await waitFor(() => logs.filter((line) => line.startsWith('budget: bosun grants')).length === 2);
  clock.advance(59_000);
  assert.equal(shadow.shadow().source, 'bosun', 'kept until expiresAt');
  clock.advance(2_000);
  const sample = shadow.shadow();
  assert.equal(sample.source, 'standalone');
  assert.equal(sample.figures.cpu_ceiling, STANDALONE.cpu.ceiling);
  assert.ok(logs.includes('budget: now standalone'), logs.join('\n'));
});

test('the usage heartbeat declares what the live lanes hold and their roots, at most once per interval', async (t) => {
  const lanes = [record('run', { pid: 41, pidStart: 'Mon Nov 24 10:00:00 2026' }), record('fetch', { subphase: 'FETCHING' }), record('gone', { subphase: 'DONE', pid: 42, pidStart: 'Mon Nov 24 10:00:01 2026' })];
  const { shadow, clock, puts } = await harness(t, { records: () => lanes });
  await shadow.heartbeat();
  assert.equal(puts.length, 1);
  assert.equal(puts[0].path, '/v1/modules/lane/usage');
  assert.equal(puts[0].authorization, 'Bearer tok');
  assert.deepEqual(puts[0].body, {
    incarnation: 7,
    held: { cpu: 4, mem: 2 * GIB, items: 2 },
    demand: { cpu: 4, mem: 2 * GIB },
    roots: [{ pid: 41, start: 'linux:boot-1:410' }],
  });
  await shadow.heartbeat();
  assert.equal(puts.length, 1, 'not again within the interval');
  clock.advance(USAGE_HEARTBEAT_MS);
  await shadow.heartbeat();
  assert.equal(puts.length, 2);
});

test('a heartbeat declares at most MAX_USAGE_ROOTS roots and says once that the rest were left out', async (t) => {
  const lanes = Array.from({ length: MAX_USAGE_ROOTS + 1 }, (_, i) => record(`run${i}`, { pid: 100 + i, pidStart: `Mon Nov 24 10:00:${i} 2026` }));
  const { shadow, clock, logs, puts } = await harness(t, { records: () => lanes });
  await shadow.heartbeat();
  clock.advance(USAGE_HEARTBEAT_MS);
  await shadow.heartbeat();
  assert.deepEqual(puts.map((put) => put.body.roots.length), [MAX_USAGE_ROOTS, MAX_USAGE_ROOTS]);
  assert.equal(puts[0].body.held.items, MAX_USAGE_ROOTS + 1, 'held still counts every live lane');
  assert.deepEqual(logs.filter((line) => /heartbeat/.test(line)), [`budget: usage heartbeat declares ${MAX_USAGE_ROOTS} of ${MAX_USAGE_ROOTS + 1} lane roots (bosun's limit)`]);
});

test('a refused or failed heartbeat is logged once and never throws', async (t) => {
  const clock = scheduler();
  const logs = [];
  const call = async () => { throw new Error('ECONNREFUSED'); };
  const env = { BOSUN_SOCKET: '/nope', BOSUN_MODULE: 'lane', BOSUN_TOKEN: 'tok', BOSUN_INCARNATION: '7' };
  const shadow = openBudgetShadow({ readConfig: () => CFG, host: () => HOST, records: () => [], log: (line) => logs.push(line), now: clock.now, env, call });
  t.after(() => shadow.close());
  await shadow.heartbeat();
  clock.advance(USAGE_HEARTBEAT_MS);
  await shadow.heartbeat();
  assert.deepEqual(logs.filter((line) => /heartbeat/.test(line)), ['budget: usage heartbeat failed: ECONNREFUSED']);
  assert.equal(shadow.shadow().source, 'standalone');
});

test('a root declares the identity bosun reads for the pid, once per lane, and skips a reused, unreadable or vanished pid', async () => {
  const reads = [];
  const lstartsOf = async (pids) => new Map(pids.filter((pid) => pid !== 43).map((pid) => [pid, pid === 42 ? 'a later start' : 'Mon Nov 24 10:00:00 2026']));
  const roots = rootsReader({ startsOf: async (pids) => { reads.push(...pids); return bosunIdentities(pids); }, lstartsOf, pidAlive: (pid) => pid !== 44 });
  const lanes = [41, 42, 43, 44, 99].map((pid) => record(`l${pid}`, { pid, pidStart: 'Mon Nov 24 10:00:00 2026' }));
  assert.deepEqual((await roots(lanes)).roots, [{ pid: 41, start: 'linux:boot-1:410' }, { pid: 43, start: 'linux:boot-1:430' }], '42 was reused, 44 is gone, 99 bosun cannot read; 43, which ps did not list though kill(0) sees it, stays');
  await roots(lanes);
  assert.deepEqual(reads, [41, 43, 99, 99]);
});

test('a probe that cannot complete keeps every live lane, as isProcessAlive fails closed', async () => {
  const roots = rootsReader({ startsOf: bosunIdentities, lstartsOf: async () => undefined, pidAlive: () => true });
  assert.deepEqual((await roots([record('a', { pid: 41, pidStart: 'x' })])).roots, [{ pid: 41, start: 'linux:boot-1:410' }]);
});

test('processStartTimes reads this process and a missing pid in one probe, matching processStartTime on every platform', async () => {
  const missing = spawnSync('true').pid;
  const starts = await processStartTimes([process.pid, missing]);
  assert.equal(starts.get(process.pid), processStartTime(process.pid));
  assert.equal(starts.has(missing), false);
  assert.equal(await processStartTimes([process.pid], { signal: AbortSignal.abort() }), undefined, 'an aborted probe is indeterminate');
});

const ENFORCE = { ...CFG, lanePull: { ...CFG.lanePull, budgetEnforce: true } };
const OFFERED = { freeCpu: 5, freeMem: 10 * GIB, budgetCpu: 7, freeWeight: 4, weightLimit: 4 };

async function enforcementScenario(load) {
  const { capByGrant: cap } = await load('agent/budget.js');
  assert.deepEqual(cap(OFFERED, { source: 'bosun', ...GRANT }), { ...OFFERED, freeCpu: 3.5, freeMem: 8 * GIB, budgetCpu: 6.5 }, 'a grant below admission caps every figure');
  const roomy = { source: 'bosun', cpu: { free: 9, ceiling: 9 }, mem: { free: 20 * GIB, ceiling: 20 * GIB } };
  assert.deepEqual(cap(OFFERED, roomy), OFFERED, 'a grant above admission changes nothing');
  assert.deepEqual(cap(OFFERED, { source: 'bosun', cpu: { free: -1, ceiling: 2 }, mem: { free: -GIB, ceiling: GIB } }), { ...OFFERED, freeCpu: 0, freeMem: 0, budgetCpu: 2 }, 'an overdrawn grant offers nothing, never a negative');
  assert.deepEqual(cap(OFFERED, { source: 'standalone', ...GRANT }), OFFERED, 'a standalone budget never caps');
  assert.equal(cap(null, { source: 'bosun', ...GRANT }), null, 'an unmeasurable claim stays unmeasurable');
  const fullGrant = { source: 'bosun', cpu: { free: 2, ceiling: 2 }, mem: { free: 4 * GIB, ceiling: 4 * GIB } };
  const holding = [{ subphase: 'FETCHING', claim: { cpuCores: 2, header: { memoryBytes: 4 * GIB } } }, { subphase: 'DONE', claim: { cpuCores: 2, header: { memoryBytes: 4 * GIB } } }];
  assert.deepEqual(cap(OFFERED, fullGrant, holding), { ...OFFERED, freeCpu: 0, freeMem: 0, budgetCpu: 2 }, 'a claim made since the last heartbeat is charged against the grant at once; a DONE one is not');
}

test('an enforced bosun grant caps what a claim offers, and a standalone budget leaves it alone', () => enforcementScenario(realSrc));

test('canary: a cap that trusts the heartbeat-stale free fails the enforcement test', () => assertCanaryFails('agent/budget.js',
  ['Math.min(offered.freeCpu, budget.cpu.free, budget.cpu.ceiling - held.cpu)', 'Math.min(offered.freeCpu, budget.cpu.free)'], enforcementScenario));

test('canary: a claim the grant does not cap fails the enforcement test', () => assertCanaryFails('agent/budget.js',
  ["if (!offered || budget?.source !== 'bosun') return offered;", 'return offered;'], enforcementScenario));

test('the grant is enforced only while lanePull.budgetEnforce is set, and each switch is logged once', async (t) => {
  const { shadow, logs } = await harness(t);
  await waitFor(() => shadow.shadow().source === 'bosun');
  assert.equal(shadow.enforced(CFG), null, 'off by default');
  assert.deepEqual(shadow.enforced(ENFORCE), { source: 'bosun', ...GRANT, gates: [] });
  shadow.enforced(ENFORCE);
  assert.equal(shadow.enforced(CFG), null);
  assert.deepEqual(logs.filter((line) => line.startsWith('budget: claims')), ['budget: claims capped by the bosun grant', 'budget: claims no longer capped by bosun']);
});

test('an unreadable budget under enforcement is logged and leaves the claim uncapped', async (t) => {
  const logs = [];
  const shadow = openBudgetShadow({ readConfig: () => CFG, host: () => { throw new Error('no host'); }, records: () => [], log: (line) => logs.push(line), env: {}, rootOptions: ROOTS });
  t.after(() => shadow.close());
  assert.equal(shadow.enforced(ENFORCE), null);
  assert.ok(logs.includes('budget: budget unreadable: no host'), logs.join('\n'));
});
