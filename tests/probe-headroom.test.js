import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { runnerRoom } from '../src/remote-client.js';
import { probeHeadroom, probeCpuHeadroom, remoteProbeCommand } from '../src/remote-runner.js';
import { evaluateCpuAdmission } from '../src/admission.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { paths, atomicWriteJson } from '../src/state.js';

// BRAIN-506: the probe's free CPU is admission's own projection, not a parallel estimate.
const cfg = { ...DEFAULT_GLOBAL_CONFIG, cpuAdmissionPercent: 75, cpuReserveCores: 1, laneNice: 10, preemptibleNiceMin: 1, preemptibleShare: 0.8 };
const CORES = 16;
const BUDGET = 12; // min(0.75 * 16, 16 - 1)
const row = (pid, nice, deltaSec) => ({ pid, ppid: 1, pgid: pid, nice, token: `t${pid}`, deltaSec });
const window = (...rows) => ({ rows: [row(1, 0, 0), ...rows], windowMs: 1000 });
const measuring = (hostBusyCores, procWindow = null) => async () => ({ hostBusyCores, cores: CORES, procWindow });
const never = async () => assert.fail('a fresh stored sample must not be re-measured');
const headroomBoth = async (root, held, measure, now = Date.now(), capacityCores = CORES) => probeCpuHeadroom(root, cfg, held, { now, measure, capacityCores });
const headroom = async (root, held, measure, now, capacityCores) => (await headroomBoth(root, held, measure, now, capacityCores))?.cpuCores ?? null;

test('a runner loaded with preemptible processes reports positive headroom', async () => {
  const { state } = freshEnv();
  // 14 busy cores, 11 of them niced sims (nice 15 > laneNice 10): 14 - 0.8 * 11 = 5.2 counts
  const got = await headroom(state, [], measuring(14, window(row(4242, 15, 11))));
  assert.ok(got > 0);
  assert.ok(Math.abs(got - (BUDGET - 5.2)) < 1e-9, `got ${got}`);
});

test('a runner loaded with non-preemptible processes still reports zero headroom', async () => {
  const { state } = freshEnv();
  assert.equal(await headroom(state, [], measuring(20, window(row(4242, 0, 20)))), 0);
});

test('with no CPU sample the probe falls back to loadavg', async () => {
  const { state } = freshEnv();
  assert.equal(await headroom(state, [], async () => null), null);
  const resources = { cpuBudgetCores: 14.4, reservedCpuCores: 8, availableMemoryBytes: 8e9 };
  const fallback = { memoryReserveBytes: 0 };
  assert.equal(probeHeadroom(resources, fallback, true, 20.5, null).cpuCores, 0);
  assert.ok(Math.abs(probeHeadroom(resources, fallback, true, 3, null).cpuCores - 6.4) < 1e-9);
  assert.ok(Math.abs(probeHeadroom(resources, fallback, true, 3).cpuCores - 6.4) < 1e-9);
});

test('(a) a lease reserving 2 but observing 8 gives the headroom admission computes', async () => {
  const { state } = freshEnv();
  const now = Date.now();
  const lease = { id: 'lease-a', key: 'k:a', weight: 2, resources: { cpuCores: 2 }, observedCpuCores: 8, observedAt: now };
  const got = await headroom(state, [lease], measuring(8), now);
  const decision = evaluateCpuAdmission({ cpuSample: { hostBusyCores: 8, preemptibleBusyCores: 0, cores: CORES }, heldLeases: [lease], candidateWeight: 1, candidateResources: { cpuCores: 1 }, cpuGateState: { closed: false }, cooldownBlocked: false, cfg, now });
  assert.equal(got, decision.budget - (decision.projectedBusy - decision.candidateEstimate));
  assert.equal(got, 4);
  assert.notEqual(got, BUDGET - 2, 'reserved cores (2) would show 6 phantom free cores');
  // an unobserved lease is charged its booking on top of the host busy baseline, exactly as admission charges it
  const unobserved = { id: 'lease-b', key: 'k:b', weight: 2, resources: { cpuCores: 2 } };
  const gotUnobserved = await headroom(state, [unobserved], measuring(8), now);
  const decisionUnobserved = evaluateCpuAdmission({ cpuSample: { hostBusyCores: 8, preemptibleBusyCores: 0, cores: CORES }, heldLeases: [unobserved], candidateWeight: 1, candidateResources: { cpuCores: 1 }, cpuGateState: { closed: false }, cooldownBlocked: false, cfg, now });
  assert.equal(gotUnobserved, decisionUnobserved.budget - (decisionUnobserved.projectedBusy - decisionUnobserved.candidateEstimate));
  assert.equal(gotUnobserved, 2);
});

test('(b) a partial process table never reports more room than the host counters allow', async () => {
  const { state } = freshEnv();
  // host says 10 busy; the table only showed one 2-core sim (the rest unreadable, new or exited)
  const partial = await headroom(state, [], measuring(10, window(row(4242, 15, 2))));
  assert.ok(Math.abs(partial - (BUDGET - (10 - 0.8 * 2))) < 1e-9, `got ${partial}`);
  const none = await headroom(state, [], measuring(10, window()));
  assert.equal(none, BUDGET - 10);
  const full = await headroom(state, [], measuring(10, window(row(4242, 15, 8))));
  assert.ok(partial <= full && none <= partial);
});

test('(c) a stored discount computed for another candidate nice does not leak', async () => {
  const { state } = freshEnv();
  const now = Date.now();
  // stored for a nice-0 candidate: the nice-5 sim counted as 11 preemptible cores. A default remote ticket runs at laneNice 10, so it is not preemptible.
  atomicWriteJson(paths(state).cpuSample, { at: now, cpus: [], lastValid: { hostBusyCores: 12, preemptibleBusyCores: 11, cores: CORES, at: now, procWindow: window(row(4242, 5, 11)) } });
  assert.equal(await headroom(state, [], never, now), 0);
});

test('remote-probe on an idle runner with a stale sample measures a window and reports headroom', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, schedulerMode: 'active', cpuReserveCores: 1, cpuAdmissionPercent: 75, preemptibleNiceMin: 1, preemptibleShare: 0.8 });
  const old = Date.now() - 600_000;
  atomicWriteJson(paths(state).cpuSample, { at: old, cpus: [], lastValid: { hostBusyCores: 99, cores: CORES, at: old } });
  const probe = async (measure) => {
    const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE, write: process.stdout.write };
    process.env.LANE_BROKER_HOME = home;
    process.env.LANE_BROKER_STATE = state;
    let out = '';
    process.stdout.write = (chunk) => { out += chunk; return true; };
    try {
      await remoteProbeCommand({ root: path.join(state, 'remote'), measure });
    } finally {
      process.stdout.write = prev.write;
      for (const [k, v] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
    return JSON.parse(out);
  };
  const niced = await probe(measuring(6, window(row(4242, 15, 6)))); // 6 - 0.8 * 6 = 1.2 counts
  const hot = await probe(measuring(6, window(row(4242, 0, 6)))); // all 6 count
  assert.ok(Math.abs(niced.headroom.cpuCores - (BUDGET - 1.2)) < 1e-9, `niced headroom ${niced.headroom.cpuCores}`);
  assert.ok(Math.abs(hot.headroom.cpuCores - (BUDGET - 6)) < 1e-9);
});

test('(d) a lane nice override above headroom.laneNice is judged on the undiscounted figure', async () => {
  const { state } = freshEnv();
  const both = await headroomBoth(state, [], measuring(14, window(row(4242, 15, 11)))); // discounted 6.8, undiscounted 12 - 14 -> 0
  assert.ok(Math.abs(both.cpuCores - 6.8) < 1e-9);
  assert.equal(both.cpuCoresUndiscounted, 0);
  const probe = { headroom: probeHeadroom({ cpuBudgetCores: BUDGET, reservedCpuCores: 0 }, cfg, true, 0, both) };
  assert.equal(probe.headroom.laneNice, 10);
  const need = { cpuCores: 2 };
  assert.equal(runnerRoom({ ...need, niceOverride: 10 }, probe).room, true, 'at laneNice: discounted figure');
  assert.equal(runnerRoom({ ...need, niceOverride: 0 }, probe).room, true, 'below laneNice: discounted figure');
  assert.equal(runnerRoom({ ...need, niceOverride: 15 }, probe).room, false, 'above laneNice: undiscounted figure');
  assert.equal(runnerRoom(need, probe).room, true, 'no override: the ticket runs at the runner laneNice, discounted figure');
});

test('(e) a probe from a runner without the new fields behaves as before, and an old client ignores them', () => {
  const old = { headroom: { cpuCores: 3, memoryBytes: 1e9 } };
  assert.equal(runnerRoom({ cpuCores: 2, niceOverride: 19 }, old).room, true);
  assert.equal(runnerRoom({ cpuCores: 4, niceOverride: 19 }, old).room, false);
  const fresh = probeHeadroom({ cpuBudgetCores: BUDGET, reservedCpuCores: 0, availableMemoryBytes: 1e9 }, { ...cfg, memoryReserveBytes: 0 }, true, 0, { cpuCores: 3, cpuCoresUndiscounted: 1 });
  // an old client reads only cpuCores and memoryBytes
  assert.equal(fresh.cpuCores, 3);
  assert.equal(fresh.memoryBytes, 1e9);
  assert.equal(probeHeadroom({ cpuBudgetCores: BUDGET, reservedCpuCores: 0 }, cfg, true, 0, null).laneNice, undefined, 'unmeasured CPU adds no fields');
});

test('(f) a stored sample taken at another capacity is not used', async () => {
  const { state } = freshEnv();
  const now = Date.now();
  atomicWriteJson(paths(state).cpuSample, { at: now, cpus: [], lastValid: { hostBusyCores: 12, preemptibleBusyCores: 0, cores: CORES, at: now } });
  assert.equal(await headroom(state, [], never, now, CORES), 0, 'same capacity: stored sample used');
  let measured = false;
  const got = await headroom(state, [], async () => { measured = true; return { hostBusyCores: 2, cores: 8, procWindow: null }; }, now, 8);
  assert.equal(measured, true);
  assert.equal(got, 4, 'measured at the shrunken capacity 8: budget min(6, 7) - 2');
});

test('(g) the submitter global laneNice never travels: no lane override is judged at the runner laneNice', async () => {
  const { state } = freshEnv();
  const runnerCfg = { ...cfg, laneNice: 0 }; // wintop
  const both = await probeCpuHeadroom(state, runnerCfg, [], { measure: measuring(14, window(row(4242, 15, 11))), capacityCores: CORES });
  const probe = { headroom: probeHeadroom({ cpuBudgetCores: BUDGET, reservedCpuCores: 0 }, runnerCfg, true, 0, both) };
  assert.equal(probe.headroom.laneNice, 0);
  assert.equal(runnerRoom({ cpuCores: 2 }, probe).room, true, 'no lane override (submitter nice 10 is not sent): discounted figure');
  assert.equal(runnerRoom({ cpuCores: 2, niceOverride: 15 }, probe).room, false, 'lane override 15 > runner laneNice 0: undiscounted figure');
});
