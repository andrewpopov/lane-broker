import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, waitFor } from './helpers.js';
import { evaluateCpuAdmission, formatAdmissionLog } from '../src/admission.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig, ConfigError } from '../src/config.js';
import { resolveLaneNice } from '../src/priority.js';
import { paths } from '../src/state.js';
import { readLease, writeLease, LEASE_STATE } from '../src/lease.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { bootId } from '../src/state.js';

// BRAIN-627: a tier sets the nice a lane runs at, and admission trusts the kernel to run a higher tier ahead of lower-tier leases.

const cfg = { ...DEFAULT_GLOBAL_CONFIG, cpuAdmissionPercent: 100, cpuReserveCores: 0, cpuOpenPercent: 99, cpuClosePercent: 100 };
const sample = { hostBusyCores: 0, cores: 8, stale: false, preemptibleBusyCores: 0 };
const lease = (id, tier, nice) => ({ id, weight: 4, resources: { cpuCores: 4 }, priorityAdmitted: tier, ...(nice === undefined ? {} : { nice }), admittedAt: 0 });
const candidate = (tier, nice) => ({ id: 'cand', weight: 4, resources: { cpuCores: 4 }, priorityAdmitted: tier, nice });
const decide = (heldLeases, ref, over = {}) =>
  evaluateCpuAdmission({ cpuSample: sample, heldLeases, candidateWeight: ref.weight, candidateRef: ref, candidateResources: ref.resources, cpuGateState: { closed: false }, cooldownBlocked: false, cfg: { ...cfg, ...over }, now: 1_000_000 });

// two 4-core leases fill the 8-core budget exactly
const FULL = [lease('aaaaaaaa', 'medium', 10), lease('bbbbbbbb', 'low', 10)];

test('resolveLaneNice: lane nice > priorityNice[tier] > laneNice', () => {
  const c = { laneNice: 10, priorityNice: { high: 5, low: 15 } };
  assert.equal(resolveLaneNice({ niceOverride: 3 }, 'high', c), 3, 'a lane nice wins, even over a tier entry');
  assert.equal(resolveLaneNice({ niceOverride: 0 }, 'high', c), 0, 'nice 0 is an override, not absent');
  assert.equal(resolveLaneNice({}, 'high', c), 5);
  assert.equal(resolveLaneNice({}, 'low', c), 15);
  assert.equal(resolveLaneNice({}, 'medium', c), 10, 'a tier absent from the map uses laneNice');
  assert.equal(resolveLaneNice({}, 'high', { laneNice: 10, priorityNice: {} }), 10);
});

test('the default priorityNice puts only high ahead of laneNice', () => {
  assert.deepEqual(DEFAULT_GLOBAL_CONFIG.priorityNice, { high: 5 });
  assert.equal(resolveLaneNice({}, 'high', DEFAULT_GLOBAL_CONFIG), 5);
  assert.equal(resolveLaneNice({}, 'medium', DEFAULT_GLOBAL_CONFIG), DEFAULT_GLOBAL_CONFIG.laneNice);
});

test('a high candidate is admitted against medium and low leases that fill the budget; a medium candidate is denied', () => {
  const high = decide(FULL, candidate('high', 5));
  assert.equal(high.admit, true, `high: ${high.reason} projected ${high.projectedBusy} of ${high.budget}`);
  assert.ok(Math.abs(high.preemptibleLeaseCores - 2 * 4 * 0.8) < 1e-9, `got ${high.preemptibleLeaseCores}`);
  assert.ok(formatAdmissionLog({ ...high, id: 'x', admit: true }).includes('preemptibleLeaseCores=6.40'));
  const medium = decide(FULL, candidate('medium', 10));
  assert.equal(medium.admit, false);
  assert.equal(medium.reason, 'projected-over-budget');
  assert.equal(medium.preemptibleLeaseCores, 0);
});

test('only a lease of a LOWER tier is discounted: a high lease at a higher nice is charged in full', () => {
  const held = [lease('aaaaaaaa', 'high', 12), lease('bbbbbbbb', 'medium', 10)];
  const d = decide(held, candidate('high', 5));
  assert.ok(Math.abs(d.preemptibleLeaseCores - 4 * 0.8) < 1e-9, 'only the medium lease is discounted');
});

test('a lease at an equal or lower nice than the candidate is a peer, charged in full', () => {
  assert.equal(decide([lease('aaaaaaaa', 'low', 5), lease('bbbbbbbb', 'low', 4)], candidate('high', 5)).preemptibleLeaseCores, 0);
});

test('a lease with unknown nice is charged in full (fail closed)', () => {
  const held = [lease('aaaaaaaa', 'medium'), lease('bbbbbbbb', 'low')];
  const d = decide(held, candidate('high', 5));
  assert.equal(d.admit, false);
  assert.equal(d.preemptibleLeaseCores, 0);
});

test('a candidate without a recorded nice or tier discounts nothing', () => {
  assert.equal(decide(FULL, { id: 'cand', weight: 4, resources: { cpuCores: 4 } }).preemptibleLeaseCores, 0);
});

test('a lease below preemptibleNiceMin is not discounted', () => {
  const held = [lease('aaaaaaaa', 'medium', 3), lease('bbbbbbbb', 'low', 3)];
  assert.equal(decide(held, candidate('high', 0), { preemptibleNiceMin: 5 }).preemptibleLeaseCores, 0);
  assert.ok(decide(held, candidate('high', 0), { preemptibleNiceMin: 1 }).preemptibleLeaseCores > 0);
});

test('preemptibleNiceMin 0 disables the lease discount', () => {
  const d = decide(FULL, candidate('high', 5), { preemptibleNiceMin: 0 });
  assert.equal(d.admit, false);
  assert.equal(d.reason, 'projected-over-budget');
  assert.equal(d.preemptibleLeaseCores, 0);
});

const loadWith = (extra) => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, ...extra });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    return loadGlobalConfig();
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prev;
  }
};

test('priorityNice config: unknown tier and bad values are refused by name', () => {
  for (const [value, needle] of [[{ urgent: 5 }, 'priorityNice.urgent'], [{ high: 20 }, 'priorityNice.high'], [{ high: 'x' }, 'priorityNice.high'], [[5], '"priorityNice" must be an object']]) {
    assert.throws(() => loadWith({ priorityNice: value }), (err) => err instanceof ConfigError && err.message.includes(needle), JSON.stringify(value));
  }
  assert.deepEqual(loadWith({ priorityNice: {} }).priorityNice, {});
  assert.deepEqual(loadWith({}).priorityNice, { high: 5 });
});

async function niceOfFirstLease(args, globalCfg, laneCfg) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, ...globalCfg });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...laneCfg } } });
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', ...args, '--', 'sleep', '2'], { env, cwd: repoDir });
  const lease = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    const l = names[0] ? readLease(state, names[0].replace(/\.json$/, '')) : null;
    return l && l.childPgid ? l : null;
  });
  const ps = Number(execFileSync('ps', ['-o', 'nice=', '-p', String(lease.childPgid)], { encoding: 'utf8' }).trim());
  await new Promise((resolve) => child.on('exit', resolve));
  return { leaseNice: lease.nice, ps };
}

test('a real high lane runs at priorityNice.high and records it on the lease; a medium lane runs at laneNice', async () => {
  const high = await niceOfFirstLease(['--priority', 'high'], { laneNice: 12, priorityNice: { high: 4 } });
  assert.deepEqual(high, { leaseNice: 4, ps: 4 });
  const medium = await niceOfFirstLease([], { laneNice: 12, priorityNice: { high: 4 } });
  assert.deepEqual(medium, { leaseNice: 12, ps: 12 });
});

test('a lane\'s own nice beats priorityNice for a high lane', async () => {
  const r = await niceOfFirstLease(['--priority', 'high'], { laneNice: 12, priorityNice: { high: 4 } }, { nice: 8 });
  assert.deepEqual(r, { leaseNice: 8, ps: 8 });
});

// the scheduler wiring: the tier comes from the queue record, the held leases from the lease files

const schedCfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 64, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, cpuAdmissionPercent: 100, cpuReserveCores: 0 };
const fullSampler = () => ({ hostBusyCores: 0, cores: 8, stale: false, sampledAt: Date.now() });
const heldAt = (id, tier, nice) => ({ id, key: `other:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, resources: { cpuCores: 4 }, priorityAdmitted: tier, ...(nice === undefined ? {} : { nice }), state: LEASE_STATE.RUNNING });
const queued = (id, priorityRequested) => ({ id, key: `r:${id}`, weight: 1, resources: { cpuCores: 4 }, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', priorityRequested });

async function pollAgainst(held, ticket) {
  const { state } = freshEnv();
  for (const l of held) writeLease(state, l);
  await enqueue(state, ticket);
  return tryStart(state, ticket, schedCfg, undefined, fullSampler, () => schedCfg, () => null);
}

test('tryStart: a high ticket starts at nice 5 over medium/low leases that fill the budget; a medium ticket is denied', async () => {
  const held = [heldAt('m', 'medium', 10), heldAt('l', 'low', 10)];
  const high = await pollAgainst(held, queued('hi', 'high'));
  assert.equal(high.started, true, JSON.stringify(high));
  assert.equal(high.lease.nice, 5);
  const medium = await pollAgainst(held, queued('med', 'medium'));
  assert.equal(medium.started, false);
  assert.equal(medium.cpuReason, 'projected-over-budget');
});

test('tryStart: leases with no recorded nice are charged in full, so the high ticket is denied', async () => {
  const high = await pollAgainst([heldAt('m', 'medium'), heldAt('l', 'low')], queued('hi', 'high'));
  assert.equal(high.started, false);
  assert.equal(high.cpuReason, 'projected-over-budget');
});
