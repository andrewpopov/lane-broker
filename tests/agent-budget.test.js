import assert from 'node:assert/strict';
import { join } from 'node:path';
import test from 'node:test';
import { MAX_USAGE_ROOTS, requestSocket } from '@andrewpopov/bosun/module';
import { createFakeBudget } from '@andrewpopov/bosun/testing/budget';
import { openBudgetShadow, rootsReader, standaloneBudget, USAGE_HEARTBEAT_MS } from '../src/agent/budget.js';
import { claimFigures } from '../src/agent/claims.js';
import { renderAgentMetrics } from '../src/agent/metrics.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { waitFor } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';

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

const bosunIdentity = (pid) => {
  if (pid === 99) throw Object.assign(new Error('gone'), { code: 'ESRCH' });
  return `linux:boot-1:${pid * 10}`;
};

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
  const shadow = openBudgetShadow({ readConfig: () => CFG, host: () => HOST, records, log: (line) => logs.push(line), now: clock.now, env, call, rootStartOf: bosunIdentity, rootAlive: () => true });
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

test('a grant that expires without a renewal falls back to the standalone budget, and a new grant takes over again', async (t) => {
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

test('a heartbeat declares at most MAX_USAGE_ROOTS roots, which bosun accepts, and says once that the rest were left out', async (t) => {
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

test('a root declares the identity bosun reads for the pid, once per lane, and skips a reused or vanished pid', () => {
  const reads = [];
  const alive = (pid, start) => start !== 'reused';
  const roots = rootsReader({ startOf: (pid) => { reads.push(pid); return bosunIdentity(pid); }, alive });
  const lanes = [record('a', { pid: 41, pidStart: 'Mon Nov 24 10:00:00 2026' }), record('b', { pid: 42, pidStart: 'reused' }), record('c', { pid: 99, pidStart: 'Mon Nov 24 10:00:02 2026' })];
  assert.deepEqual(roots(lanes).roots, [{ pid: 41, start: 'linux:boot-1:410' }]);
  roots(lanes);
  assert.deepEqual(reads, [41, 99, 99]);
});
