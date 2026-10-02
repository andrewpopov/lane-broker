import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeCpuBusyFile, laneRun } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, resolveTicketConfig } from '../src/config.js';
import { writeLease, readLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { childEnv } from '../src/supervisor.js';
import { leaseDemand, projectBusy } from '../src/admission.js';
import { elasticCpuClaims, resolveTicketResources, leaseCpuCores, leaseResources } from '../src/resources.js';
import { collectStatus, renderStatusText } from '../src/status.js';

/**
 * BRAIN-360: elastic CPU claims. Fixture machine: 10 cores, reserve 1, 100% -> CPU budget 9,
 * weight capacity 10. The elastic lane declares 4 cores with a floor of 2, so against ambient
 * load `ext` its full claim fits iff ext + 4 <= 9, and a claim of k fits iff ext + k <= 9.
 */

const GIB = 1024 ** 3;

function baseCfg(overrides = {}) {
  return {
    ...DEFAULT_GLOBAL_CONFIG,
    schedulerMode: 'active',
    capacity: 10,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    cpuAdmissionPercent: 100,
    cpuReserveCores: 1,
    admissionCooldownMs: 0,
    resourceSkipLimit: 3,
    resourceIdleOvershootCores: 0,
    ...overrides,
  };
}

const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = (overrides = {}) => () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test', ...overrides });
const poll = (state, t, cfg, { ext = 0, mem = memory() } = {}) => tryStart(state, t, cfg, undefined, sampler(ext), undefined, mem);
const readLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

function ticket(id, overrides = {}) {
  return {
    id,
    key: `r:${id}`,
    weight: 1,
    cwd: process.cwd(),
    cmd: ['true'],
    supervisorPid: process.pid,
    supervisorStart: null,
    logPath: '/dev/null',
    resultPath: '/dev/null',
    ...overrides,
  };
}

const elastic = (id, overrides = {}) =>
  ticket(id, { resources: { cpuCores: 4, minCpuCores: 2, memoryBytes: GIB }, ...overrides });
const plain = (id, overrides = {}) => ticket(id, { resources: { cpuCores: 4, memoryBytes: GIB }, ...overrides });

function heldLease(id, key, extra = {}) {
  return { id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING, ...extra };
}

test('elasticCpuClaims: integers below the claim, largest first, down to the floor, then a fractional floor', () => {
  assert.deepEqual(elasticCpuClaims({ cpuCores: 4, minCpuCores: 2 }), [3, 2]);
  assert.deepEqual(elasticCpuClaims({ cpuCores: 4, minCpuCores: 1.5 }), [3, 2, 1.5]);
  assert.deepEqual(elasticCpuClaims({ cpuCores: 2.5, minCpuCores: 1 }), [2, 1]);
  assert.deepEqual(elasticCpuClaims({ cpuCores: 4, minCpuCores: 4 }), []);
  assert.deepEqual(elasticCpuClaims({ cpuCores: 4 }), []);
});

test('resolveTicketResources only carries a floor that is strictly below the claim', () => {
  assert.deepEqual(resolveTicketResources({ weight: 1, cpuCores: 4, minCpuCores: 2, memoryBytes: GIB }), { cpuCores: 4, minCpuCores: 2, memoryBytes: GIB });
  assert.deepEqual(resolveTicketResources({ weight: 1, cpuCores: 4, minCpuCores: 4, memoryBytes: GIB }), { cpuCores: 4, memoryBytes: GIB });
  assert.deepEqual(resolveTicketResources({ weight: 1, cpuCores: 4, memoryBytes: GIB }), { cpuCores: 4, memoryBytes: GIB });
});

test('full fit: the full claim is granted and nothing elastic is logged', async () => {
  const { state } = freshEnv();
  const t = elastic('e');
  await enqueue(state, t);
  const result = await poll(state, t, baseCfg(), { ext: 2 });
  assert.equal(result.started, true);
  assert.equal(result.lease.grantedCpuCores, 4);
  assert.equal(leaseCpuCores(result.lease), 4);
  assert.doesNotMatch(readLog(state), /declaredCpu=|elastic-grant/);
});

test('partial fit: admitted at the LARGEST k >= min whose complete predicate passes', async () => {
  const cases = [
    { ext: 6, granted: 3 }, // 6+4 > 9, 6+3 = 9 fits
    { ext: 6.5, granted: 2 }, // 6.5+3 > 9, 6.5+2 = 8.5 fits
    { ext: 7, granted: 2 }, // 7+2 = 9 fits exactly
  ];
  for (const { ext, granted } of cases) {
    const { state } = freshEnv();
    const t = elastic('e');
    await enqueue(state, t);
    const result = await poll(state, t, baseCfg(), { ext });
    assert.equal(result.started, true, `ext ${ext}`);
    assert.equal(result.lease.grantedCpuCores, granted, `ext ${ext}`);
    assert.equal(result.lease.resources.cpuCores, 4, 'the declaration stays on the lease');
    assert.equal(result.lease.resources.memoryBytes, GIB, 'memory keeps the full declared claim');
    assert.equal(readLease(state, 'e').grantedCpuCores, granted, 'persisted on the lease file');
    const log = readLog(state);
    assert.match(log, new RegExp(`candidateCpu=${granted.toFixed(2)} declaredCpu=4.00`));
    assert.match(log, new RegExp(`event=elastic-grant candidateId=e declared=4 granted=${granted} min=2`));
  }
});

test('below the floor: waits with the same denial reason as a lane with no floor', async () => {
  const { state } = freshEnv();
  const t = elastic('e');
  await enqueue(state, t);
  const result = await poll(state, t, baseCfg(), { ext: 7.5 }); // 7.5+2 = 9.5 > 9
  const { state: state2 } = freshEnv();
  const p = plain('p');
  await enqueue(state2, p);
  const baseline = await poll(state2, p, baseCfg(), { ext: 7.5 });
  assert.equal(result.started, false);
  assert.deepEqual(result, baseline);
  assert.equal(result.reason, 'cpu-admission');
  assert.equal(result.cpuReason, 'projected-over-budget');
});

test('no minCpuCores: decisions are unchanged at every ambient load (grant == declaration, no elastic trace)', async () => {
  for (const ext of [0, 2, 5, 5.01, 6, 7.5, 8.9]) {
    const { state } = freshEnv();
    const p = plain('p');
    await enqueue(state, p);
    const result = await poll(state, p, baseCfg(), { ext });
    const fits = ext + 4 <= 9;
    assert.equal(result.started, fits, `ext ${ext}`);
    if (fits) {
      assert.equal(result.lease.grantedCpuCores, 4);
    } else {
      assert.equal(result.reason, 'cpu-admission');
      assert.equal(result.cpuReason, 'projected-over-budget');
      assert.equal(result.projectedBusy, ext + 4);
      assert.equal(result.budget, 9);
    }
    assert.doesNotMatch(readLog(state), /declaredCpu=|elastic-grant/, `ext ${ext}`);
  }
});

test('a floor equal to the claim is no floor', async () => {
  const { state } = freshEnv();
  const t = ticket('e', { resources: resolveTicketResources({ weight: 1, cpuCores: 4, minCpuCores: 4, memoryBytes: GIB }) });
  await enqueue(state, t);
  const result = await poll(state, t, baseCfg(), { ext: 6 });
  assert.equal(result.started, false);
  assert.equal(result.cpuReason, 'projected-over-budget');
});

test('a non-CPU denial is never elastic: memory', async () => {
  const { state } = freshEnv();
  const t = elastic('e');
  await enqueue(state, t);
  // CPU alone would grant 3 at ext 6; memory (1 GiB claim vs 2 GiB reserve + 1 GiB available) denies
  const tight = memory({ availableBytes: 2 * GIB + GIB / 2, totalBytes: 64 * GIB });
  const result = await poll(state, t, baseCfg({ memoryReserveBytes: 2 * GIB }), { ext: 6, mem: tight });
  assert.equal(result.started, false);
  assert.equal(result.memoryReason, 'memory-headroom');
  assert.doesNotMatch(readLog(state), /elastic-grant|declaredCpu=/);
});

test('a non-CPU denial is never elastic: cooldown', async () => {
  const { state } = freshEnv();
  writeLease(state, heldLease('holder', 'r:holder', { admittedAt: Date.now() }));
  const t = elastic('e');
  await enqueue(state, t);
  const result = await poll(state, t, baseCfg({ admissionCooldownMs: 60_000 }), { ext: 1 });
  assert.equal(result.started, false);
  assert.equal(result.cpuReason, 'cooldown');
});

test('a non-CPU denial is never elastic: weight capacity and a closed CPU gate', async () => {
  const cap = freshEnv();
  const heavy = elastic('e', { weight: 3 });
  writeLease(cap.state, heldLease('holder', 'r:holder', { weight: 8 }));
  await enqueue(cap.state, heavy);
  const capacity = await poll(cap.state, heavy, baseCfg({ capacity: 10 }), { ext: 6 });
  assert.equal(capacity.reason, 'capacity');

  const gate = freshEnv();
  const t = elastic('e');
  await enqueue(gate.state, t);
  atomicWriteJson(paths(gate.state).cpuGate, { closed: true, consecutiveUnder: 0, fingerprint: null });
  const closed = await poll(gate.state, t, baseCfg({ cpuClosePercent: 95, cpuOpenPercent: 10, cpuOpenSamples: 100 }), { ext: 6 });
  assert.equal(closed.started, false);
  assert.equal(closed.cpuReason, 'cpu-gate-closed');
});

test('shadow mode never grants elastically (it never denies either)', async () => {
  const { state } = freshEnv();
  const t = elastic('e');
  await enqueue(state, t);
  const result = await poll(state, t, baseCfg({ schedulerMode: 'shadow' }), { ext: 8 });
  assert.equal(result.started, true);
  assert.equal(result.lease.grantedCpuCores, 4);
});

test('the child env exports the GRANT, not the declaration', () => {
  const t = { id: 'x', key: 'k', resources: { cpuCores: 4, minCpuCores: 2, memoryBytes: GIB } };
  const env = childEnv(t, { PATH: '/bin' }, 3);
  assert.equal(env.LANE_BROKER_CPU_CORES, '3');
  assert.equal(env.LANE_BROKER_MEMORY_BYTES, String(GIB));
  assert.equal(childEnv(t, { PATH: '/bin' }).LANE_BROKER_CPU_CORES, '4', 'with no grant supplied the claim is exported, as before');
});

test('a held lease is charged its grant: reservedSum/leaseDemand, settled demand, status', async () => {
  const now = Date.now();
  const lease = heldLease('l', 'r:l', { resources: { cpuCores: 4, memoryBytes: GIB }, grantedCpuCores: 2 });
  assert.equal(leaseDemand(lease, now), 2);
  assert.equal(projectBusy(1, [lease], 1, now), 4, 'external 1 + grant 2 + candidate 1, not the declared 4');
  assert.equal(leaseResources(lease, baseCfg()).cpuCores, 2);

  // settled demand (BRAIN-354) clamps to the grant, never the declaration
  const cfg = baseCfg({ settledDemandEnabled: true, settledDemandSettleMs: 0, settledDemandWindowMs: 60_000, settledDemandFloorFraction: 0.5, settledDemandHeadroom: 1.25 });
  const settled = { ...lease, admittedAt: now - 120_000, observedCpuCores: 0.2, observedAt: now, observedCpuHistory: [{ at: now - 5000, cores: 5 }, { at: now - 2000, cores: 5 }] };
  assert.equal(leaseDemand(settled, now, cfg), 2, 'peak 5 * 1.25 is capped at the grant');
  const idle = { ...settled, observedCpuHistory: [{ at: now - 5000, cores: 0.1 }, { at: now - 2000, cores: 0.1 }] };
  assert.equal(leaseDemand(idle, now, cfg), 1, 'floor fraction applies to the grant: 2 * 0.5');
  // a lease with no grant (admitted before this field) is charged its declaration
  assert.equal(leaseDemand({ ...lease, grantedCpuCores: undefined }, now), 4);
});

test('status shows declared vs granted for a running elastic lease, and reserved CPU is the grant', async () => {
  const { state } = freshEnv();
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeLease(state, heldLease('l', 'r:l', { resources: { cpuCores: 4, memoryBytes: GIB }, grantedCpuCores: 2, childPgid: process.pid, startedAt: Date.now() }));
    const status = await collectStatus();
    const running = status.running.find((r) => r.id === 'l');
    assert.equal(running.declaredCpuCores, 4);
    assert.equal(running.grantedCpuCores, 2);
    assert.equal(status.resources.reservedCpuCores, 2);
    assert.match(renderStatusText(status), /cpu=2\/4 \(elastic\)/);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
  }
});

test('a reserved head is not delayed by an elastic backfill: reservation refuses it, even though a smaller claim would fit', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  const head = plain('head', { resources: { cpuCores: 8, memoryBytes: GIB }, weight: 1 });
  const small = elastic('small');
  await enqueue(state, head);
  await enqueue(state, small);
  atomicWriteJson(paths(state).resourceSkipState, { headId: 'head', count: 3, reserved: true, inScope: true, deniedAt: Date.now(), budget: 9, externalBusy: 6 });
  // ext 6: head (8) cannot fit; the elastic ticket's full claim (4) cannot either, but 3 or 2 would
  const result = await poll(state, small, cfg, { ext: 6 });
  assert.equal(result.started, false);
  assert.equal(result.reason, 'not-head');
  assert.equal(readLease(state, 'small'), null);
});

test('an elastic HEAD is admitted at a partial claim when it reaches the head', async () => {
  const { state } = freshEnv();
  const head = elastic('head');
  const behind = plain('behind');
  await enqueue(state, head);
  await enqueue(state, behind);
  const result = await poll(state, head, baseCfg(), { ext: 6 });
  assert.equal(result.started, true);
  assert.equal(result.lease.grantedCpuCores, 3);
});

test('safe backfill (BRAIN-355): an elastic candidate is judged against the blocked head\'s reservation, so it cannot delay it', async () => {
  const LIMIT = 3;
  const mk = async () => {
    const { state } = freshEnv();
    writeLease(state, heldLease('holder', 'rouge:sim'));
    const head = ticket('head', { key: 'rouge:default', weight: 3, conflicts: ['rouge:sim'], resources: { cpuCores: 4, memoryBytes: GIB } });
    await enqueue(state, head);
    atomicWriteJson(paths(state).conflictSkipState, { headId: 'head', count: LIMIT, blockedSince: Date.now(), loggedPhase: 'exhausted' });
    const cand = elastic('cand', { key: 'rouge:other', weight: 1 });
    await enqueue(state, cand);
    return { state, cand };
  };
  // holder demand 1 (cold, weight 1) + head reservation 4 + ext 2 = 7: the candidate's 4 would make 11 > 9;
  // at k=2 it is 9 <= 9 and the reservation is still honoured.
  const fits = await mk();
  const granted = await poll(fits.state, fits.cand, baseCfg({ conflictSkipLimit: LIMIT }), { ext: 2 });
  assert.equal(granted.started, true);
  assert.equal(granted.lease.grantedCpuCores, 2);
  // one more ambient core and even the floor would overshoot once the head's reservation is counted
  const over = await mk();
  const refused = await poll(over.state, over.cand, baseCfg({ conflictSkipLimit: LIMIT }), { ext: 3 });
  assert.equal(refused.started, false);
  assert.equal(refused.cpuReason, 'projected-over-budget');
});

test('config: minCpuCores is validated by name', () => {
  const { base } = freshEnv();
  const load = (lane) => {
    const dir = path.join(base, `r-${Math.random().toString(16).slice(2)}`);
    writeRepoConfig(dir, { version: 1, lanes: { default: lane } });
    return () => resolveTicketConfig({ cwd: dir, repo: 'r', lane: 'default', configRoot: dir });
  };
  assert.equal(load({ weight: 1, cpuCores: 4, minCpuCores: 2 })().minCpuCores, 2);
  assert.equal(load({ weight: 1, cpuCores: 4 })().minCpuCores, null);
  assert.equal(load({ weight: 4, minCpuCores: 4 })().minCpuCores, 4, 'a floor under weight when cpuCores is unset');
  assert.throws(load({ weight: 1, cpuCores: 4, minCpuCores: 5 }), /lane "default"\.minCpuCores \(5\) must be <= its cpuCores \(4\)/);
  assert.throws(load({ weight: 1, cpuCores: 4, minCpuCores: 0 }), /lane "default"\.minCpuCores must be a positive number/);
  assert.throws(load({ weight: 1, cpuCores: 4, minCpuCores: -1 }), /lane "default"\.minCpuCores must be a positive number/);
  assert.throws(load({ weight: 1, cpuCores: 4, minCpuCores: '2' }), /lane "default"\.minCpuCores must be a positive number/);
});

test('an undeclared lane inheriting via {"as"} inherits minCpuCores', () => {
  const { base } = freshEnv();
  const dir = path.join(base, 'r-as');
  writeRepoConfig(dir, { version: 1, lanes: { default: { weight: 1, cpuCores: 4, minCpuCores: 2 } }, undeclaredLanes: { as: 'default' } });
  const resolved = resolveTicketConfig({ cwd: dir, repo: 'r', lane: 'zirk999', configRoot: dir });
  assert.equal(resolved.cpuCores, 4);
  assert.equal(resolved.minCpuCores, 2);
});

test('end to end: a real lane is granted the largest fitting k, its child sees it, and history records it', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 100,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    schedulerMode: 'active',
    cpuAdmissionPercent: 100,
    cpuReserveCores: 1,
    admissionCooldownMs: 0,
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 4, minCpuCores: 2, memoryBytes: 1048576 } } });
  const busy = writeCpuBusyFile(base, 6.5, 10); // 6.5 + 4 > 9; 6.5 + 3 > 9; 6.5 + 2 = 8.5 fits
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'echo "grant=$LANE_BROKER_CPU_CORES"'], {
    env: { ...env, LANE_BROKER_CPU_BUSY_FILE: busy },
    cwd: repoDir,
  });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.match(result.stdout, /grant=2\b/);
  const rows = fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(rows.at(-1).grantedCpuCores, 2);
  assert.equal(rows.at(-1).resources.cpuCores, 4, 'the declaration is still recorded beside the grant');
});
