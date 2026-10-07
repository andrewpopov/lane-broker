import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeCpuBusyFile, laneRun, gitFixture, BIN } from './helpers.js';
import { tmpDir, setup, markerCmd, makeFakeSshBin, clientEnv, makeRunner } from './remote-harness.js';
import { selectRunner } from '../src/remote-client.js';
import { enqueue, tryStart, listQueueCapped } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, resolveTicketConfig } from '../src/config.js';
import { writeLease, readLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { childEnv } from '../src/supervisor.js';
import { leaseDemand, projectBusy } from '../src/admission.js';
import { elasticClaimRange, resolveTicketResources, leaseCpuCores, leaseResources, ELASTIC_CLAIMS_CAPABILITY, checkResourceBudget, cpuBudgetCores, capElasticClaim, detectResourceCapacity } from '../src/resources.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { PRIORITY_CAPABILITY } from '../src/priority.js';
import { ARTIFACTS_CAPABILITY } from '../src/remote-artifacts.js';

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

test('elasticClaimRange: integer claims only, bounded by the measured headroom, floor rounded UP', () => {
  assert.deepEqual(elasticClaimRange({ cpuCores: 4, minCpuCores: 2, headroom: 3.5 }), { hi: 3, lo: 2 });
  assert.deepEqual(elasticClaimRange({ cpuCores: 4, minCpuCores: 2, headroom: 2.9 }), { hi: 2, lo: 2 });
  assert.deepEqual(elasticClaimRange({ cpuCores: 4, minCpuCores: 1.5, headroom: 7 }), { hi: 3, lo: 2 }, 'a fractional floor of 1.5 means 2, never 1.5');
  assert.equal(elasticClaimRange({ cpuCores: 4, minCpuCores: 1.5, headroom: 1.5 }), null, '1.5 cores of headroom is below the rounded floor');
  assert.deepEqual(elasticClaimRange({ cpuCores: 2.5, minCpuCores: 1, headroom: 9 }), { hi: 2, lo: 1 });
  assert.deepEqual(elasticClaimRange({ cpuCores: 1e20, minCpuCores: 1, headroom: 2.5 }), { hi: 2, lo: 1 }, 'the headroom bounds the walk, not the declaration');
  assert.equal(elasticClaimRange({ cpuCores: 4, minCpuCores: 4, headroom: 9 }), null);
  assert.equal(elasticClaimRange({ cpuCores: 4, headroom: 9 }), null);
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

test('grants are integers: a fractional floor of 1.5 is never granted as 1.5', async () => {
  const frac = (id) => ticket(id, { resources: { cpuCores: 4, minCpuCores: 1.5, memoryBytes: GIB } });
  const denied = freshEnv();
  await enqueue(denied.state, frac('e'));
  const none = await poll(denied.state, frac('e'), baseCfg(), { ext: 7.5 }); // 1.5 cores of headroom
  assert.equal(none.started, false, 'the old walk granted 1.5 here');
  assert.equal(none.cpuReason, 'projected-over-budget');
  const ok = freshEnv();
  await enqueue(ok.state, frac('e'));
  const granted = await poll(ok.state, frac('e'), baseCfg(), { ext: 7 });
  assert.equal(granted.lease.grantedCpuCores, 2);
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

test('no minCpuCores: decisions are unchanged at every ambient load, and the lease record is exactly the pre-change shape', async () => {
  for (const ext of [0, 2, 5, 5.01, 6, 7.5, 8.9]) {
    const { state } = freshEnv();
    const p = plain('p');
    await enqueue(state, p);
    const result = await poll(state, p, baseCfg(), { ext });
    const fits = ext + 4 <= 9;
    assert.equal(result.started, fits, `ext ${ext}`);
    if (fits) {
      // the FULL pre-change lease record (what scheduler.js wrote before BRAIN-360), key for key
      const lease = readLease(state, 'p');
      const expected = {
        id: 'p',
        key: 'r:p',
        bootId: bootId(),
        supervisorPid: process.pid,
        supervisorStart: null,
        childPgid: null,
        heartbeatAt: lease.heartbeatAt,
        admittedAt: lease.admittedAt,
        cwd: p.cwd,
        cmd: p.cmd,
        weight: 1,
        resources: { cpuCores: 4, memoryBytes: GIB },
        logPath: '/dev/null',
        resultPath: '/dev/null',
        state: LEASE_STATE.RUNNING,
        // BRAIN-380 slice 4: the admission audit, the only addition since BRAIN-360
        priorityRequested: 'medium',
        priorityAdmitted: 'medium',
        priorityDemoted: false,
        effectiveRankAtStart: 1,
      };
      // the real clock: a ticket that waited a few ms has aged a hair past the medium base of 1
      const { scoreAtStart, ...rest } = lease;
      assert.ok(scoreAtStart >= 1 && scoreAtStart < 1.01, `ext ${ext}: ${scoreAtStart}`);
      assert.deepEqual(rest, expected, `ext ${ext}`);
    } else {
      assert.equal(result.reason, 'cpu-admission');
      assert.equal(result.cpuReason, 'projected-over-budget');
      assert.equal(result.projectedBusy, ext + 4);
      assert.equal(result.budget, 9);
    }
    assert.doesNotMatch(readLog(state), /declaredCpu=|elastic-grant/, `ext ${ext}`);
  }
});

test('no minCpuCores: the status entry has no elastic fields (BRAIN-361 adds bookedCpuCores/overrun, never declared/granted)', async () => {
  const { state } = freshEnv();
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeLease(state, heldLease('l', 'r:l', { resources: { cpuCores: 4, memoryBytes: GIB }, childPgid: process.pid, startedAt: Date.now() }));
    const entry = (await collectStatus()).running.find((r) => r.id === 'l');
    assert.deepEqual(Object.keys(entry), ['id', 'key', 'state', 'pid', 'elapsedMs', 'heartbeatAgeMs', 'logAgeMs', 'log', 'weight', 'maxConcurrent', 'resources', 'observedCpuCores', 'bookedCpuCores', 'overrun', 'observedMemoryBytes', 'noProgressSinceMs']); // BRAIN-431 stall signal, always present
    assert.doesNotMatch(renderStatusText(await collectStatus()), /elastic/);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
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

test('reservation isolation: a candidate whose FULL claim fits is refused while the head is reserved, and admitted when it is not', async () => {
  const run = async (reserved) => {
    const { state } = freshEnv();
    const head = plain('head', { resources: { cpuCores: 8, memoryBytes: GIB } }); // 2+8 > 9: denied
    const cand = elastic('cand'); // 2+4 = 6 <= 9: its FULL claim fits
    await enqueue(state, head);
    await enqueue(state, cand);
    atomicWriteJson(paths(state).resourceSkipState, { headId: 'head', count: reserved ? 1 : 0, reserved, inScope: true, deniedAt: Date.now(), budget: 9, externalBusy: 2 });
    return poll(state, cand, baseCfg(), { ext: 2 });
  };
  const open = await run(false);
  assert.equal(open.started, true, 'control: with an unreserved head the same candidate backfills');
  assert.equal(open.lease.grantedCpuCores, 4);
  const reserved = await run(true);
  assert.equal(reserved.started, false, 'the reservation alone is what refuses it');
  assert.equal(reserved.reason, 'not-head');
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

test('a pathological declared claim returns promptly with a bounded grant (child process, hard timeout)', () => {
  const code = `
    import { evaluateElasticAdmission } from ${JSON.stringify(new URL('../src/admission.js', import.meta.url).href)};
    import { DEFAULT_GLOBAL_CONFIG } from ${JSON.stringify(new URL('../src/config.js', import.meta.url).href)};
    const cfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'active', cpuAdmissionPercent: 100, cpuReserveCores: 1, admissionCooldownMs: 0 };
    const sample = { hostBusyCores: 5, cores: 10, stale: false };
    const ticket = { id: 't', weight: 1, resources: { cpuCores: 1e20, minCpuCores: 1, memoryBytes: 1 } };
    const full = { admit: false, cpuReason: 'projected-over-budget', memoryReason: 'ok', budget: 9, projectedBusy: 5 + 1e20, candidateCpuCores: 1e20, candidateEstimate: 1e20, cpuGateClosed: false, cooldownBlocked: false };
    const r = evaluateElasticAdmission(cfg, ticket, [], sample, null, full);
    console.log(JSON.stringify(r && r.grantedCpuCores));
  `;
  const res = spawnSync(process.execPath, ['--input-type=module', '-e', code], { encoding: 'utf8', timeout: 15_000 });
  assert.equal(res.error, undefined, `did not return in time: ${res.error}`);
  assert.equal(res.status, 0, res.stderr);
  assert.equal(res.stdout.trim(), '4', 'ambient 5 of a 9-core budget leaves exactly 4');
});

test('config refuses an absurd cpuCores / minCpuCores by name, including through an undeclaredLanes template', () => {
  const { base } = freshEnv();
  const resolve = (cfg, lane = 'default') => {
    const dir = path.join(base, `r-${Math.random().toString(16).slice(2)}`);
    writeRepoConfig(dir, { version: 1, ...cfg });
    return () => resolveTicketConfig({ cwd: dir, repo: 'r', lane, configRoot: dir });
  };
  assert.throws(resolve({ lanes: { default: { weight: 1, cpuCores: 1e20, minCpuCores: 1 } } }), /lane "default"\.cpuCores must be <= 1024/);
  assert.throws(resolve({ lanes: { default: { weight: 1, cpuCores: 1024, minCpuCores: 1025 } } }), /lane "default"\.minCpuCores/);
  assert.equal(resolve({ lanes: { default: { weight: 1, cpuCores: 1024, minCpuCores: 1024 } } })().minCpuCores, 1024);
  assert.throws(
    resolve({ lanes: { tpl: { weight: 1, cpuCores: 1e20 } }, undeclaredLanes: { as: 'tpl' } }, 'zirk1'),
    /lane "tpl"\.cpuCores must be <= 1024/,
    'an inheriting ad-hoc lane cannot reach an unvalidated template',
  );
});

function fixedProbeSsh(binDir, byDestination) {
  const sshBin = path.join(binDir, 'ssh-mixed');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
const argv = process.argv.slice(2).filter((a, i, all) => a !== '-o' && all[i - 1] !== '-o');
const payloads = ${JSON.stringify(byDestination)};
process.stdout.write(JSON.stringify(payloads[argv[0]]) + '\\n');
`,
  );
  fs.chmodSync(sshBin, 0o755);
  return sshBin;
}

test('mixed versions: a runner without elastic-claims/1 is judged on the FULL claim, one with it on the floor', async () => {
  const { binDir } = makeFakeSshBin();
  const probe = (capabilities) => ({
    protocol: 1,
    protocols: [1, 2],
    version: '0.0.0',
    paused: false,
    queued: 0,
    running: 0,
    ...(capabilities ? { capabilities } : {}),
    capacity: { weight: 100, cpuCores: 3, memoryBytes: 64_000_000_000, memoryReserveBytes: 0 },
  });
  const sshBin = fixedProbeSsh(binDir, { old: probe(null), new: probe([ELASTIC_CLAIMS_CAPABILITY]) });
  const { env } = clientEnv(binDir);
  const reservation = { weight: 1, cpuCores: 4, minCpuCores: 2, memoryBytes: 1 };
  const old = makeRunner({ ssh: 'old', name: 'old' });
  const neu = makeRunner({ ssh: 'new', name: 'new' });

  const oldOnly = await selectRunner([old], { sshBin, env, deadlineMs: 3000, reservation });
  assert.equal(oldOnly.runner, null, 'budget 3 < full claim 4: dispatching would be a terminal exit-64 refusal');
  assert.match(oldOnly.skipped[0].reason, /capacity/);

  const both = await selectRunner([old, neu], { sshBin, env, deadlineMs: 3000, reservation });
  assert.equal(both.runner.name, 'new', 'the old runner is skipped and the elastic one used');

  const neuOnly = await selectRunner([neu], { sshBin, env, deadlineMs: 3000, reservation });
  assert.equal(neuOnly.runner.name, 'new', 'floor 2 <= budget 3');
});

test('remote-probe advertises elastic-claims/1', () => {
  const { env } = freshEnv();
  const res = spawnSync(process.execPath, [BIN, 'remote-probe'], { env, encoding: 'utf8' });
  assert.equal(res.status, 0, res.stderr);
  assert.deepEqual(JSON.parse(res.stdout).capabilities, [
    ELASTIC_CLAIMS_CAPABILITY,
    PRIORITY_CAPABILITY,
    ARTIFACTS_CAPABILITY,
    'sim-safe-backfill/1',
    'lane-aging/1',
    'group-reap/1',
    'remote-withdraw/1',
    'exclusive/1',
  ]);
});

test('remote run: the runner-side grant reaches BOTH the runner history and the submitter result and history', async () => {
  const { env, home, state, repoDir, runnerHome, runnerState } = setup();
  const busy = writeCpuBusyFile(path.dirname(home), 6.5, 10);
  writeGlobalConfig(runnerHome, { version: 1, sampleMs: 50, capacity: 100, schedulerMode: 'active', cpuAdmissionPercent: 100, cpuReserveCores: 1, admissionCooldownMs: 0, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 4, minCpuCores: 2, memoryBytes: 1048576, remote: true } } });
  gitFixture(['add', '-A'], repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], repoDir);
  const marker = path.join(tmpDir('marker'), 'where');
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], {
    env: { ...env, LANE_BROKER_CPU_BUSY_FILE: busy },
    cwd: repoDir,
  });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const last = (root) => fs.readFileSync(paths(root).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l)).at(-1);
  const submitter = last(state);
  assert.equal(submitter.executor, 'remote');
  assert.equal(submitter.grantedCpuCores, 2, 'submitter history');
  assert.equal(submitter.resources.cpuCores, 4);
  const runnerRow = last(runnerState);
  assert.equal(runnerRow.grantedCpuCores, 2, 'runner history');
});

test('non-elastic: the result carries no grant field and the history row records the declaration as the grant (BRAIN-425)', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 2 } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 0, result.stderr);
  const row = JSON.parse(fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').at(-1));
  assert.equal(row.grantedCpuCores, 2, 'a non-elastic lane is charged exactly its declaration');
  assert.deepEqual(row.resources.cpuCores, 2);
  const res = JSON.parse(fs.readFileSync(path.join(paths(state).results, `${row.id}.json`), 'utf8'));
  assert.equal('grantedCpuCores' in res, false);
});

// BRAIN-362: the budget caps every elastic grant. Budget is pinned to 3 cores on THIS host (reserve = cores - 3),
// and the sampler reports the host's real core count so the sample budget agrees with it.
const HOST_CORES = detectResourceCapacity().cpuCores;
const tightCfg = (overrides = {}) => baseCfg({ cpuReserveCores: HOST_CORES - 3, ...overrides });
const hostSampler = (hostBusyCores) => () => ({ hostBusyCores, cores: HOST_CORES, stale: false, sampledAt: Date.now() });
const hugeElastic = (id) => ticket(id, { resources: { cpuCores: 16, minCpuCores: 2, memoryBytes: GIB } });

test('BRAIN-362: a cold CPU sample admits the full claim, but the grant is capped at floor(budget)', async () => {
  const { state } = freshEnv();
  const t = hugeElastic('cold');
  await enqueue(state, t, tightCfg());
  const result = await tryStart(state, t, tightCfg(), undefined, () => null, undefined, memory());
  assert.equal(result.started, true);
  assert.match(readLog(state), /sample-unavailable-empty/);
  assert.equal(result.lease.grantedCpuCores, 3);
  assert.equal(leaseCpuCores(result.lease), 3);
  assert.equal(result.lease.resources.cpuCores, 16, 'the declaration is kept beside the grant');
  assert.equal(childEnv(t, { PATH: '/bin' }, leaseCpuCores(result.lease)).LANE_BROKER_CPU_CORES, '3');
  assert.match(readLog(state), /elastic-grant/);
});

test('BRAIN-362: the idle exemption admits an over-projection elastic head, still capped at floor(budget)', async () => {
  const { state } = freshEnv();
  const t = hugeElastic('idle');
  const cfg = tightCfg({ resourceIdleOvershootCores: 100 });
  await enqueue(state, t, cfg);
  // ambient 2 + any claim >= 2 exceeds the 3-core budget, so only the idle exemption can start it
  const result = await tryStart(state, t, cfg, undefined, hostSampler(2), undefined, memory());
  assert.equal(result.started, true);
  assert.match(readLog(state), /resource-idle-exempt/);
  assert.equal(result.lease.grantedCpuCores, 3);
  assert.equal(leaseCpuCores(result.lease), 3);
});

test('BRAIN-362: end to end, a lane declaring more cpuCores than the budget runs with LANE_BROKER_CPU_CORES <= floor(budget)', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, schedulerMode: 'active',
    cpuAdmissionPercent: 100, cpuReserveCores: HOST_CORES - 3, admissionCooldownMs: 0,
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 16, minCpuCores: 2, memoryBytes: 1048576 } } });
  const busy = writeCpuBusyFile(base, 0, HOST_CORES);
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'echo "grant=$LANE_BROKER_CPU_CORES"'], {
    env: { ...env, LANE_BROKER_CPU_BUSY_FILE: busy },
    cwd: repoDir,
  });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.match(result.stdout, /grant=3\b/);
  const rows = fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(rows.at(-1).grantedCpuCores, 3);
});

test('BRAIN-362: a non-elastic claim over the budget is still refused (exit 64), not capped', () => {
  const host = { cpuCores: 4, memoryBytes: 64 * GIB };
  const globalCfg = baseCfg({ cpuReserveCores: 1 });
  const refused = checkResourceBudget({ resources: { cpuCores: 16, memoryBytes: GIB }, globalCfg, host });
  assert.equal(refused.ok, false);
  assert.equal(refused.exitCode, 64);
  assert.equal(checkResourceBudget({ resources: { cpuCores: 16, minCpuCores: 2, memoryBytes: GIB }, globalCfg, host }).ok, true, 'an elastic one is capped instead');
});

test('BRAIN-362: a fractional floor whose ceiling exceeds floor(budget) is refused immediately (exit 64)', () => {
  const host = { cpuCores: 4, memoryBytes: 64 * GIB };
  const globalCfg = baseCfg({ cpuReserveCores: 0.6 }); // budget 3.4: ceil(3.2) = 4 > floor(3.4) = 3
  assert.equal(cpuBudgetCores(host, globalCfg).toFixed(1), '3.4');
  const refused = checkResourceBudget({ resources: { cpuCores: 6, minCpuCores: 3.2, memoryBytes: GIB }, globalCfg, host });
  assert.equal(refused.ok, false);
  assert.equal(refused.exitCode, 64);
  assert.match(refused.message, /requested resources exceed this environment's budget \(4\/3\.40 CPU cores/);
  assert.equal(checkResourceBudget({ resources: { cpuCores: 6, minCpuCores: 3, memoryBytes: GIB }, globalCfg, host }).ok, true);
  assert.equal(checkResourceBudget({ resources: { cpuCores: 3.2, memoryBytes: GIB }, globalCfg, host }).ok, true, 'a non-elastic fractional claim is charged as-is and fits');
});

test('BRAIN-362: end to end, a fractional floor above floor(budget) exits 64 at once instead of queueing', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, schedulerMode: 'active',
    cpuAdmissionPercent: 100, cpuReserveCores: HOST_CORES - 3.4, admissionCooldownMs: 0,
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 6, minCpuCores: 3.2, memoryBytes: 1048576 } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 64, `stderr: ${result.stderr}`);
  assert.match(result.stderr, /exceed this environment's budget/);
});

test('BRAIN-362: a queue record written with an over-budget elastic claim is read at the capped size (loader and status)', async () => {
  const { state } = freshEnv();
  const t = hugeElastic('legacy');
  await enqueue(state, t, tightCfg({ schedulerMode: 'shadow' })); // shadow never caps: the record keeps the 16-core claim
  const fileOf = () => path.join(paths(state).queue, fs.readdirSync(paths(state).queue).find((n) => n.endsWith('.json')));
  assert.equal(JSON.parse(fs.readFileSync(fileOf(), 'utf8')).resources.cpuCores, 16);
  const [capped] = listQueueCapped(state, tightCfg());
  assert.equal(capped.resources.cpuCores, 3);
  assert.equal(capped.resources.minCpuCores, 2);
  assert.equal(listQueueCapped(state, tightCfg({ schedulerMode: 'shadow' }))[0].resources.cpuCores, 16, 'shadow mode never caps');
});

test('BRAIN-362: the cap never goes below the floor or to zero when the budget shrank after preflight', () => {
  const host = { cpuCores: 4, memoryBytes: 64 * GIB };
  const t = ticket('shrunk', { resources: { cpuCores: 16, minCpuCores: 3.2, memoryBytes: GIB } });
  const kept = capElasticClaim(t, baseCfg({ cpuReserveCores: 0.6 }), host); // budget 3.4: floor 3 < ceil(3.2) = 4
  assert.equal(kept, t, 'left as declared: it waits like any over-budget claim');
  const zero = capElasticClaim(t, baseCfg({ cpuReserveCores: 3.5 }), host); // budget 0.5: floor 0
  assert.equal(zero.resources.cpuCores, 16, 'never capped to zero');
  const ok = capElasticClaim(t, baseCfg({ cpuReserveCores: 0 }), host); // budget 4: floor 4 >= ceil(3.2)
  assert.equal(ok.resources.cpuCores, 4);
});

test('BRAIN-362: lane status reads a stale over-budget queue record at the capped size too', async () => {
  const { state, home } = freshEnv();
  const cfg = tightCfg();
  await enqueue(state, hugeElastic('legacy'), { ...cfg, schedulerMode: 'shadow' });
  writeGlobalConfig(home, { version: 1, capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, schedulerMode: 'active', cpuAdmissionPercent: 100, cpuReserveCores: cfg.cpuReserveCores });
  const prev = { state: process.env.LANE_BROKER_STATE, home: process.env.LANE_BROKER_HOME };
  process.env.LANE_BROKER_STATE = state;
  process.env.LANE_BROKER_HOME = home;
  try {
    const entry = (await collectStatus()).queued.find((q) => q.id === 'legacy');
    assert.equal(entry.resources.cpuCores, 3);
  } finally {
    for (const [k, v] of [['LANE_BROKER_STATE', prev.state], ['LANE_BROKER_HOME', prev.home]]) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
});

test('BRAIN-362: an elastic ticket whose floor no longer fits floor(budget) is denied, even on a cold sample or the idle exemption', async () => {
  const belowFloor = (id) => ticket(id, { resources: { cpuCores: 16, minCpuCores: 3.2, memoryBytes: GIB } }); // budget 3: floor 3 < ceil(3.2) = 4
  const cold = freshEnv().state;
  const c = belowFloor('cold');
  await enqueue(cold, c, tightCfg());
  const coldResult = await tryStart(cold, c, tightCfg(), undefined, () => null, undefined, memory());
  assert.equal(coldResult.started, false, 'a cold sample must not admit it at the full claim');
  assert.equal(coldResult.reason, 'elastic-below-floor');
  assert.match(readLog(cold), /elastic-below-floor/);
  assert.deepEqual(fs.readdirSync(paths(cold).queue).filter((n) => n.endsWith('.json')).length, 1, 'it keeps waiting, not dequeued');

  const idle = freshEnv().state;
  const i = belowFloor('idle');
  const cfg = tightCfg({ resourceIdleOvershootCores: 100 });
  await enqueue(idle, i, cfg);
  const idleResult = await tryStart(idle, i, cfg, undefined, hostSampler(2), undefined, memory());
  assert.equal(idleResult.started, false, 'the idle exemption must not admit it either');
  assert.equal(idleResult.reason, 'elastic-below-floor');

  const grown = await tryStart(idle, i, tightCfg({ resourceIdleOvershootCores: 100, cpuReserveCores: HOST_CORES - 4 }), undefined, hostSampler(0), undefined, memory());
  if (HOST_CORES >= 4) assert.equal(grown.started, true, 'once the budget holds the floor again it is admitted');
});

test('BRAIN-362: a below-floor elastic head does not block backfill', async () => {
  const { state } = freshEnv();
  const cfg = tightCfg();
  const head = ticket('head', { key: 'r:head', resources: { cpuCores: 16, minCpuCores: 3.2, memoryBytes: GIB } }); // budget 3: floor 3 < ceil(3.2) = 4
  const small = ticket('small', { key: 'r:small', resources: { cpuCores: 1, memoryBytes: GIB } });
  await enqueue(state, head, cfg);
  await enqueue(state, small, cfg);
  const denied = await tryStart(state, head, cfg, undefined, hostSampler(0), undefined, memory());
  assert.equal(denied.reason, 'elastic-below-floor');
  const result = await tryStart(state, small, cfg, undefined, hostSampler(0), undefined, memory());
  assert.equal(result.started, true, `the small ticket backfills past the futile head, got ${result.reason}`);
  const queued = listQueueCapped(state, cfg).map((t) => t.id);
  assert.deepEqual(queued, ['head'], 'the head is still queued');
});

test('BRAIN-362: the queue keeps the declared claim; the cap follows the current budget for the queue view and admission alike', async () => {
  const { state } = freshEnv();
  const t = hugeElastic('grow');
  const small = tightCfg({ cpuReserveCores: HOST_CORES - 2 });
  await enqueue(state, t, small);
  const file = path.join(paths(state).queue, fs.readdirSync(paths(state).queue).find((n) => n.endsWith('.json')));
  assert.equal(JSON.parse(fs.readFileSync(file, 'utf8')).resources.cpuCores, 16, 'stored as declared');
  assert.equal(listQueueCapped(state, small)[0].resources.cpuCores, 2);
  const grown = tightCfg(); // budget raised 2 -> 3
  assert.equal(listQueueCapped(state, grown)[0].resources.cpuCores, 3);
  const result = await tryStart(state, t, grown, undefined, () => null, undefined, memory());
  assert.equal(result.started, true);
  assert.equal(result.lease.grantedCpuCores, 3, 'admission agrees with the queue view');
});
