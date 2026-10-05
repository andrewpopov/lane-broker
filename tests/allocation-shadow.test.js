import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { readLastSimDemandAt } from '../src/sim-arm.js';
import { collectStatus, renderStatusText } from '../src/status.js';

/**
 * BRAIN-379 slice 2: the shadow wiring. Real tryStart against a temp state dir with the CPU sampler
 * and memory reader injected. Fixture machine: 10 cores, reserve 1, 100% -> B = 9.
 */

const GIB = 1024 ** 3;
const SHADOW_TOKEN = 'lane-broker-allocation-shadow';

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
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });

const ticket = (id, overrides = {}) => ({
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
});

const heldLease = (id, key, weight, extra = {}) => ({ id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), admittedAt: Date.now(), weight, state: LEASE_STATE.RUNNING, ...extra });

const poll = (state, t, cfg, { ext = 0, evaluator } = {}) => tryStart(state, t, cfg, undefined, sampler(ext), undefined, memory, undefined, evaluator);

const readLog = (state) => {
  try {
    return fs.readFileSync(paths(state).admissionLog, 'utf8');
  } catch {
    return '';
  }
};
const shadowLines = (state) => readLog(state).split('\n').filter((l) => l.startsWith(`${SHADOW_TOKEN} `));
const liveLines = (state) => readLog(state).split('\n').filter((l) => l && !l.startsWith(SHADOW_TOKEN));
const field = (line, name) => new RegExp(`(?:^| )${name}=(\\S*)`).exec(line)?.[1];

// ---- the hard invariant -------------------------------------------------------------------------

/**
 * A scripted run on a frozen, scripted clock so every persisted timestamp is reproducible. Returns
 * everything live admission produced: each poll's result, the skip/reservation/lease/queue files raw,
 * and the live admission log lines.
 */
async function runScript(script, allocationShadow) {
  const { state } = freshEnv();
  let clock = 1_700_000_000_000;
  const dateNow = mock.method(Date, 'now', () => clock);
  try {
    const cfg = baseCfg({ allocationShadow });
    const results = [];
    for (const step of script) {
      clock += 1000;
      if (step.enqueue) await enqueue(state, step.enqueue);
      else if (step.lease) writeLease(state, { ...step.lease, heartbeatAt: clock, admittedAt: clock });
      else if (step.release) fs.unlinkSync(path.join(paths(state).leases, `${step.release}.json`));
      else {
        const r = await poll(state, step.poll, cfg, { ext: step.ext ?? 0 });
        results.push({ poll: step.poll.id, started: r.started, reason: r.reason ?? null, cpuReason: r.cpuReason ?? null });
      }
    }
    const raw = (dir, name) => {
      try {
        return fs.readFileSync(path.join(dir, name), 'utf8');
      } catch {
        return null;
      }
    };
    const p = paths(state);
    const dirContents = (dir) => Object.fromEntries(fs.readdirSync(dir).sort().map((n) => [n, fs.readFileSync(path.join(dir, n), 'utf8')]));
    return {
      results,
      conflictSkip: raw(p.root, 'conflict-skip-state.json'),
      capacitySkip: raw(p.root, 'capacity-skip-state.json'),
      resourceSkip: raw(p.root, 'resource-skip-state.json'),
      leases: dirContents(p.leases),
      queue: dirContents(p.queue),
      liveLog: liveLines(state),
      shadowLines: shadowLines(state),
    };
  } finally {
    dateNow.mock.restore();
  }
}

const sim = (id, weight = 1, extra = {}) => ticket(id, { class: 'sim', weight, ...extra });

// A resource-denied head backfilled past (resource skip counter + reservation latch), mixed classes.
const RESOURCE_SCRIPT = [
  { enqueue: ticket('head', { weight: 4 }) },
  { enqueue: sim('s1') },
  { enqueue: ticket('t1') },
  { enqueue: sim('s2', 2) },
  { enqueue: ticket('t2', { weight: 2 }) },
  { poll: sim('s1'), ext: 5.4 },
  { poll: ticket('head', { weight: 4 }), ext: 5.4 },
  { poll: sim('s1'), ext: 5.4 },
  { poll: ticket('t1'), ext: 5.4 },
  { poll: ticket('head', { weight: 4 }), ext: 5.4 },
  { poll: sim('s2', 2), ext: 5.4 },
  { poll: ticket('t2', { weight: 2 }), ext: 5.4 },
  { poll: ticket('head', { weight: 4 }), ext: 5.4 },
  { release: 's1' },
  { release: 't1' },
  { poll: ticket('head', { weight: 4 }), ext: 0 },
];

// A conflict-blocked head that is skipped (conflict skip counter) until its allowance is exhausted, with a sim queued.
const CONFLICT_SCRIPT = [
  { lease: heldLease('holder', 'r:lock', 1) },
  { enqueue: ticket('chead', { key: 'r:lock', conflicts: [] }) },
  { enqueue: sim('cs1') },
  { enqueue: ticket('ct1') },
  { enqueue: sim('cs2') },
  { enqueue: ticket('ct2') },
  { poll: ticket('chead', { key: 'r:lock', conflicts: [] }) },
  { poll: sim('cs1') },
  { poll: ticket('ct1') },
  { poll: sim('cs2') },
  { poll: ticket('ct2') },
  { poll: ticket('chead', { key: 'r:lock', conflicts: [] }) },
];

// A head that does not fit weight capacity (capacity skip counter), tests holding most of B so sim locks bite.
const CAPACITY_SCRIPT = [
  { lease: heldLease('big', 'r:big', 5, { resources: { cpuCores: 5, memoryBytes: GIB } }) },
  { enqueue: ticket('khead', { weight: 6 }) },
  { enqueue: sim('ks1') },
  { enqueue: ticket('kt1', { weight: 2 }) },
  { poll: ticket('khead', { weight: 6 }) },
  { poll: sim('ks1') },
  { poll: ticket('kt1', { weight: 2 }) },
  { poll: ticket('khead', { weight: 6 }) },
];

// What proves each script really drove its skip machinery (the head's start clears the resource record, so that one shows in the live log).
const EXERCISED = {
  resource: (r) => r.liveLog.some((l) => /event=resource-backfill-start/.test(l)),
  conflict: (r) => r.conflictSkip !== null,
  capacity: (r) => r.capacitySkip !== null,
};

for (const [name, script, exercised] of [
  ['resource backfill + reservation', RESOURCE_SCRIPT, 'resource'],
  ['conflict skip + exhausted allowance', CONFLICT_SCRIPT, 'conflict'],
  ['capacity skip', CAPACITY_SCRIPT, 'capacity'],
]) {
  test(`live selection is byte-identical with shadow on and off (${name}): admission sequence, skip/reservation files, leases, queue, live log`, async () => {
    const off = await runScript(script, false);
    const on = await runScript(script, true);
    assert.deepEqual(on.results, off.results, 'identical admission sequence');
    for (const file of ['conflictSkip', 'capacitySkip', 'resourceSkip']) assert.equal(on[file], off[file], `${file} state file is byte-identical`);
    assert.deepEqual(on.leases, off.leases, 'identical lease files');
    assert.deepEqual(on.queue, off.queue, 'identical queue files');
    assert.deepEqual(on.liveLog, off.liveLog, 'identical live admission log lines');
    assert.equal(off.shadowLines.length, 0, 'shadow off writes no shadow record');
    assert.ok(on.shadowLines.length >= 3, `shadow on records evaluations (got ${on.shadowLines.length})`);
    assert.ok(EXERCISED[exercised](off), `the script really exercised the ${exercised} skip path (otherwise this proves nothing)`);
    assert.ok(off.results.some((r) => r.started) && off.results.some((r) => !r.started), 'script mixes starts and denials');
  });
}

test('the shadow path never writes a skip counter or a reservation record, even when it would consume skip budget', async () => {
  // Live: the test head starts (CPU fits). Shadow: sims are armed by the queued sim, the head's claim
  // breaches the sim lock, so the hypothetical selection is a BACKFILL that consumes one skip.
  const { state } = freshEnv();
  const cfg = baseCfg({ allocationShadow: true });
  writeLease(state, heldLease('held', 'r:held', 5, { resources: { cpuCores: 5, memoryBytes: GIB } }));
  await enqueue(state, ticket('head', { weight: 3 }));
  await enqueue(state, sim('s1'));
  const result = await poll(state, ticket('head', { weight: 3 }), cfg, { ext: 0 });
  assert.equal(result.started, true, 'live admits the head');
  const [line] = shadowLines(state);
  assert.equal(field(line, 'select'), 's1', 'shadow would have picked the sim instead');
  assert.equal(field(line, 'selectReason'), 'backfill');
  assert.match(line, / skip=none:0\+1\/3 /, 'hypothetical consumption is reported');
  const p = paths(state);
  for (const file of [p.conflictSkipState, p.capacitySkipState, p.resourceSkipState]) assert.equal(fs.existsSync(file), false, `${path.basename(file)} must not exist`);
});

test('a throw inside the shadow evaluation is logged as a shadow error and leaves the live decision intact', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ allocationShadow: true });
  await enqueue(state, ticket('head'));
  const result = await poll(state, ticket('head'), cfg, {
    evaluator: () => {
      throw new Error('injected shadow failure');
    },
  });
  assert.equal(result.started, true, 'live decision unaffected');
  assert.ok(fs.existsSync(path.join(paths(state).leases, 'head.json')), 'lease was written');
  assert.equal(shadowLines(state).length, 0, 'no shadow record');
  assert.match(readLog(state), /lane-broker-allocation-shadow-error poller=head error=injected_shadow_failure/);
  assert.equal(liveLines(state).filter((l) => l.startsWith('lane-broker-admission ')).length, 1, 'the live admission line is still written');
});

// ---- the record --------------------------------------------------------------------------------

test('a shadow record is written for a blocked head evaluation, carrying the actual live outcome', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ allocationShadow: true });
  await enqueue(state, ticket('head', { weight: 4 }));
  const denied = await poll(state, ticket('head', { weight: 4 }), cfg, { ext: 5.4 });
  assert.equal(denied.reason, 'cpu-admission');
  const [line] = shadowLines(state);
  assert.equal(field(line, 'poller'), 'head');
  assert.equal(field(line, 'actual'), 'cpu-admission');
  assert.equal(field(line, 'budgetSource'), 'sample');
  assert.equal(field(line, 'B'), '9');
  assert.equal(field(line, 'externalBusy'), '5.4');
});

test('5 queued tickets polling through one cycle with no start write exactly 1 shadow line (the head\'s)', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ allocationShadow: true });
  const ids = ['q1', 'q2', 'q3', 'q4', 'q5'];
  for (const id of ids) await enqueue(state, ticket(id, { weight: 4 }));
  for (const id of ids) {
    const r = await poll(state, ticket(id, { weight: 4 }), cfg, { ext: 8 });
    assert.equal(r.started, false);
  }
  const lines = shadowLines(state);
  assert.equal(lines.length, 1);
  assert.equal(field(lines[0], 'poller'), 'q1');
});

test('a non-head backfill start writes a shadow record with actual=started', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ allocationShadow: true });
  await enqueue(state, ticket('head', { weight: 4 }));
  await enqueue(state, ticket('small'));
  await poll(state, ticket('head', { weight: 4 }), cfg, { ext: 5.4 });
  const started = await poll(state, ticket('small'), cfg, { ext: 5.4 });
  assert.equal(started.started, true);
  const lines = shadowLines(state);
  assert.equal(lines.length, 2);
  assert.equal(field(lines[1], 'poller'), 'small');
  assert.equal(field(lines[1], 'head'), 'head');
  assert.equal(field(lines[1], 'actual'), 'started');
});

test('the shadow record format: B, usage, locks, arm state, selection, actual outcome, per-candidate verdicts', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ allocationShadow: true });
  writeLease(state, heldLease('held', 'r:held', 5, { class: 'test', resources: { cpuCores: 5, memoryBytes: GIB } }));
  await enqueue(state, ticket('headaaaa-1111', { weight: 3 }));
  await enqueue(state, sim('simbbbbb-2222'));
  await poll(state, ticket('headaaaa-1111', { weight: 3 }), cfg);
  const [line] = shadowLines(state);
  const stamp = readLastSimDemandAt(state);
  assert.ok(Number.isFinite(stamp));
  assert.equal(
    line,
    `${SHADOW_TOKEN} poller=headaaaa head=headaaaa actual=started select=simbbbbb selectReason=backfill B=9 budgetSource=sample externalBusy=0 ` +
      `used_t=5 used_s=0 L_t=1.35 L_s=2.7 armed=true lastSimDemandAt=${stamp} skip=none:0+1/3 reserved=false ` +
      'candidates=headaaaa:test:3:class-lock,simbbbbb:sim:1:ok',
  );
});

// ---- the arm stamp -----------------------------------------------------------------------------

test('the arm stamp is written on a sim enqueue, not on a test enqueue', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticket('t1'));
  assert.equal(readLastSimDemandAt(state), null);
  assert.equal(fs.existsSync(paths(state).simArm), false);
  const before = Date.now();
  await enqueue(state, sim('s1'));
  assert.ok(readLastSimDemandAt(state) >= before);
});

test('the arm stamp is reconciled from queue and lease state at a locked evaluation', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  await enqueue(state, ticket('head'));
  await enqueue(state, sim('s1'));
  fs.unlinkSync(paths(state).simArm);
  await poll(state, ticket('head'), cfg);
  assert.ok(readLastSimDemandAt(state) !== null, 'a queued sim re-stamps a lost file');

  // a charged sim lease alone (empty queue of sims) also re-stamps
  const second = freshEnv();
  writeLease(second.state, heldLease('simrun', 'r:simrun', 1, { class: 'sim' }));
  await enqueue(second.state, ticket('t'));
  await poll(second.state, ticket('t'), cfg);
  assert.ok(readLastSimDemandAt(second.state) !== null, 'a charged sim lease stamps');

  // no sim anywhere: nothing is written
  const third = freshEnv();
  await enqueue(third.state, ticket('t'));
  await poll(third.state, ticket('t'), cfg);
  assert.equal(fs.existsSync(paths(third.state).simArm), false);
});

test('a corrupt or missing arm file means unarmed with no history, never a crash', async () => {
  for (const content of [null, '{not json', '"a string"', '{"lastSimDemandAt":"soon"}', 'null']) {
    const { state } = freshEnv();
    if (content !== null) fs.writeFileSync(paths(state).simArm, content);
    assert.equal(readLastSimDemandAt(state), null);
    const cfg = baseCfg({ allocationShadow: true });
    writeLease(state, heldLease('held', 'r:held', 5, { resources: { cpuCores: 5, memoryBytes: GIB } }));
    await enqueue(state, ticket('head', { weight: 2 }));
    const result = await poll(state, ticket('head', { weight: 2 }), cfg);
    assert.equal(result.started, true);
    const [line] = shadowLines(state);
    assert.equal(field(line, 'armed'), 'false', `content ${JSON.stringify(content)}`);
    assert.equal(field(line, 'lastSimDemandAt'), 'none');
    assert.equal(field(line, 'L_s'), '0');
  }
});

test('a recent arm stamp arms the sim lock with no sim queued or running, and an old one does not', async () => {
  for (const [ageMs, armed] of [[60_000, 'true'], [10 * 60_000, 'false']]) {
    const { state } = freshEnv();
    const cfg = baseCfg({ allocationShadow: true });
    atomicWriteJson(paths(state).simArm, { lastSimDemandAt: Date.now() - ageMs });
    await enqueue(state, ticket('head'));
    await poll(state, ticket('head'), cfg);
    assert.equal(field(shadowLines(state)[0], 'armed'), armed, `stamp ${ageMs}ms old`);
  }
});

test('the arm stamp is touched on admission of a sim, and the lease carries its class', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  await enqueue(state, sim('s1'));
  fs.unlinkSync(paths(state).simArm);
  const result = await poll(state, sim('s1'), cfg);
  assert.equal(result.started, true);
  assert.equal(result.lease.class, 'sim', 'the lease record carries the class');
  assert.ok(readLastSimDemandAt(state) !== null);
  assert.equal(JSON.parse(fs.readFileSync(path.join(paths(state).leases, 's1.json'), 'utf8')).class, 'sim');
});

test('the arm stamp is touched on a queued sim cancel and on a reaped sim lease', async () => {
  const { state } = freshEnv();
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    await enqueue(state, sim('s1'));
    fs.unlinkSync(paths(state).simArm);
    const { cancelCommand } = await import('../src/cancel.js');
    assert.equal((await cancelCommand('s1')).exitCode, 0);
    assert.ok(readLastSimDemandAt(state) !== null, 'queued cancel stamps');

    fs.unlinkSync(paths(state).simArm);
    writeLease(state, heldLease('dead', 'r:dead', 1, { class: 'sim', supervisorPid: 2 ** 22 + 12345, childPgid: 2 ** 22 + 54321 }));
    const { reapAll } = await import('../src/lease.js');
    assert.deepEqual(reapAll(state, bootId()), [{ id: 'dead', action: 'reaped' }]);
    assert.ok(readLastSimDemandAt(state) !== null, 'orphan reap stamps');
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
  }
});

// ---- status ------------------------------------------------------------------------------------

test('lane status shows the allocation line and object only when allocationShadow is on', async () => {
  const { state, home } = freshEnv();
  const prevState = process.env.LANE_BROKER_STATE;
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = state;
  process.env.LANE_BROKER_HOME = home;
  try {
    writeLease(state, heldLease('held', 'r:held', 2, { class: 'test', resources: { cpuCores: 2, memoryBytes: GIB } }));
    await enqueue(state, sim('s1', 1));
    const off = await collectStatus();
    assert.equal('allocation' in off, false);
    assert.doesNotMatch(renderStatusText(off), /allocation \(shadow\)/);

    fs.writeFileSync(path.join(home, 'config.json'), JSON.stringify({ version: 1, allocationShadow: true, schedulerMode: 'shadow' }));
    const on = await collectStatus();
    assert.equal(on.allocation.shadow, true);
    assert.equal(on.allocation.armed, true, 'a queued sim arms');
    assert.equal(on.allocation.test.used, 2);
    assert.equal(on.allocation.sim.queued, 1);
    assert.ok(on.allocation.sim.target > 0);
    const text = renderStatusText(on);
    assert.match(text, /^allocation \(shadow\): test used 2\.00\/target [\d.]+ queued 0\.00; sim used 0\.00\/target [\d.]+ queued 1\.00; sims armed$/m);
  } finally {
    if (prevState === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prevState;
    if (prevHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prevHome;
  }
});
