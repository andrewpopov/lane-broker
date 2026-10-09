import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, BIN } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, resolveTicketConfig, ConfigError, loadGlobalConfig, reloadGlobalConfig, validateClasses, ClassesConfigError } from '../src/config.js';
import { writeLease, readLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { fenceLegacyQueue } from '../src/migrate.js';
import { detectResourceCapacity } from '../src/resources.js';
import { orderQueue } from '../src/priority.js';
import { childEnv, remoteSelectOptions } from '../src/supervisor.js';
import { createAttempt, fallbackToLocal, readAttempt } from '../src/attempts.js';
import { selectRunner } from '../src/remote-client.js';
import { makeFakeSshBin, makeRunner, clientEnv } from './remote-harness.js';
import { CLASS_ENFORCEMENT_ENV, resolveClasses } from '../src/classes.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { loadModules, scenarios, runScript, liveTrace } from './allocation-scenarios.js';

/**
 * BRAIN-321 S1: per-machine, per-class reservation caps. Real tryStart against a temp state dir. Caps are counted
 * against BOOKED grants (live sim leases' claims), so every fixture lease carries its own claim. The CPU budget B is
 * POOL cores (see below), so reserve arithmetic is derived from it.
 */

const GIB = 1024 ** 3;
const HOST_CORES = detectResourceCapacity().cpuCores;
// The CPU budget B is pinned to POOL cores on any host with at least that many (reserve = host - POOL, 100%), and every
// sample is injected, so no outcome here depends on the real core count above POOL or on the machine's load.
const POOL = 4;

const classes = (mode, sim = {}, test = {}) => ({ mode, test: { reserveCores: 0, ...test }, sim: { capCores: 4, maxTickets: 8, capMemoryBytes: 64 * GIB, ...sim } });

function cfgWith(classesBlock, overrides = {}) {
  return {
    ...DEFAULT_GLOBAL_CONFIG,
    schedulerMode: 'active',
    capacity: 64,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    cpuAdmissionPercent: 100,
    cpuReserveCores: HOST_CORES - POOL,
    admissionCooldownMs: 0,
    ...(classesBlock ? { classes: classesBlock } : {}),
    ...overrides,
  };
}

const sampler = () => ({ hostBusyCores: 0, cores: HOST_CORES, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t, cfg, reload) => tryStart(state, t, cfg, undefined, sampler, reload, memory);

function ticket(id, extra = {}) {
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
    ...extra,
  };
}

const simTicket = (id, cpuCores = 1, extra = {}) => ticket(id, { class: 'sim', resources: { cpuCores, memoryBytes: GIB }, ...extra });
const testTicket = (id, cpuCores = 1, extra = {}) => ticket(id, { resources: { cpuCores, memoryBytes: GIB }, ...extra });

function heldSim(state, id, cpuCores, { memoryBytes = GIB, granted } = {}) {
  writeLease(state, {
    id,
    key: `r:${id}`,
    class: 'sim',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    admittedAt: Date.now() - 60_000,
    weight: 1,
    resources: { cpuCores, memoryBytes },
    ...(granted !== undefined ? { grantedCpuCores: granted } : {}),
    state: LEASE_STATE.RUNNING,
  });
}

async function admit(state, t, cfg) {
  await enqueue(state, t);
  return poll(state, t, cfg);
}

test('HOST has the cores these fixtures assume', () => {
  assert.ok(HOST_CORES >= POOL, `class-caps fixtures need >= ${POOL} host cores (got ${HOST_CORES})`);
});

test('a sim beyond capCores is denied class-cap; one that fits the booked cap starts', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('active', { capCores: 4 }));
  heldSim(state, 'h1', 3);
  const over = simTicket('over', 2);
  const denied = await admit(state, over, cfg);
  assert.equal(denied.started, false);
  assert.equal(denied.reason, 'class-cap');
  assert.equal(denied.capReason, 'cap-cores');
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /current=deny:class-cap /);
  const fits = simTicket('fits', 1);
  assert.equal((await admit(state, fits, cfg)).started, true, '3 booked + 1 = 4 is exactly the cap');
});

test('a sim at maxTickets is denied class-cap', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('active', { maxTickets: 2, capCores: 100 }));
  heldSim(state, 'h1', 1);
  heldSim(state, 'h2', 1);
  const denied = await admit(state, simTicket('third', 1), cfg);
  assert.equal(denied.reason, 'class-cap');
  assert.equal(denied.capReason, 'max-tickets');
});

test('a sim beyond capMemoryBytes is denied class-cap', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('active', { capMemoryBytes: 2 * GIB, capCores: 100 }));
  heldSim(state, 'h1', 1, { memoryBytes: GIB + GIB / 2 });
  const denied = await admit(state, simTicket('big', 1), cfg);
  assert.equal(denied.reason, 'class-cap');
  assert.equal(denied.capReason, 'cap-memory');
});

test('sims never book into test.reserveCores', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('active', { capCores: 1000, maxTickets: 100 }, { reserveCores: POOL - 2 }));
  heldSim(state, 'h1', 1);
  const denied = await admit(state, simTicket('into-reserve', 2), cfg);
  assert.equal(denied.reason, 'class-cap');
  assert.equal(denied.capReason, 'test-reserve');
  assert.equal((await admit(state, simTicket('inside', 1), cfg)).started, true, 'booked 1 + 1 = B - reserve is allowed');
});

test('a class-cap sim head never blocks a test behind it, and a cap-denied sim is not a skip', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('active', { capCores: 0 }));
  const blockedSim = simTicket('sim-head', 1);
  await enqueue(state, blockedSim);
  const t = testTicket('behind', 1);
  await enqueue(state, t);
  assert.equal((await poll(state, blockedSim, cfg)).reason, 'class-cap');
  const result = await poll(state, t, cfg);
  assert.equal(result.started, true, `the test starts past a cap-denied sim head (got ${JSON.stringify(result)})`);
  assert.equal(fs.existsSync(paths(state).conflictSkipState), false, 'no skip was recorded');
  assert.equal(fs.existsSync(paths(state).capacitySkipState), false);
});

test('an invalid classes config grants no sim at all, but a test still runs', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, schedulerMode: 'active', classes: { mode: 'active', sim: { capCores: -1, maxTickets: 1, capMemoryBytes: 1 }, test: { reserveCores: 0 } } });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const cfg = loadGlobalConfig();
    assert.equal(cfg.classes, undefined, 'the broken block is set aside, never read as caps');
    assert.match(cfg.classesInvalid, /sim\.capCores/);
    const denied = await admit(state, simTicket('s', 1), cfg);
    assert.equal(denied.started, false);
    assert.equal(denied.reason, 'class-config-invalid');
    assert.equal((await admit(state, testTicket('t', 1), cfg)).started, true);
    assert.equal(readLease(state, 's'), null);
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});

test('a failed reload while caps were in force fails the sim class closed, never to stale caps or defaults', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, schedulerMode: 'active', classes: classes('active') });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const good = loadGlobalConfig();
    assert.equal(resolveClasses(good, state).mode, 'active');
    fs.writeFileSync(path.join(home, 'config.json'), '{ half written');
    const reloaded = reloadGlobalConfig(good);
    assert.equal(resolveClasses(reloaded, state).valid, false);
    assert.equal((await admit(state, simTicket('s', 1), reloaded)).reason, 'class-config-invalid');
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});

test('validateClasses refuses a typo, a bad mode and a missing cap in active mode', () => {
  assert.throws(() => validateClasses({ mode: 'on' }, 'f'), ClassesConfigError);
  assert.throws(() => validateClasses({ ...classes('active'), simm: {} }, 'f'), /unknown key "simm"/);
  assert.throws(() => validateClasses({ mode: 'active', test: { reserveCores: 0 }, sim: { capCores: 1, maxTickets: 1 } }, 'f'), /capMemoryBytes/);
  assert.throws(() => validateClasses(classes('active', { maxTickets: 1.5 }), 'f'), /maxTickets/);
  assert.doesNotThrow(() => validateClasses({ mode: 'off' }, 'f'));
});

test('classEnforcement is recorded on a sim lease in active mode only, and the child sees it', async () => {
  for (const mode of ['off', 'shadow']) {
    const { state } = freshEnv();
    const started = await admit(state, simTicket(`s-${mode}`, 1), cfgWith(classes(mode)));
    assert.equal(started.started, true, mode);
    assert.equal(started.lease.classEnforcement, undefined, `${mode} mode grants no classEnforcement`);
    assert.equal(childEnv(started.lease, {}, 1, started.lease.classEnforcement)[CLASS_ENFORCEMENT_ENV], undefined);
  }
  const { state } = freshEnv();
  const cfg = cfgWith(classes('active'));
  const started = await admit(state, simTicket('s-active', 1), cfg);
  assert.equal(started.started, true);
  const expected = resolveClasses(cfg, state);
  assert.deepEqual(started.lease.classEnforcement, { mode: 'active', generation: started.lease.classEnforcement.generation, stateRoot: state, configHash: expected.configHash });
  assert.match(started.lease.classEnforcement.generation, /^(pkg-\d|sched-v2@\d)/);
  assert.deepEqual(readLease(state, 's-active').classEnforcement, started.lease.classEnforcement, 'persisted on the lease file');
  assert.deepEqual(JSON.parse(childEnv(started.lease, {}, 1, started.lease.classEnforcement)[CLASS_ENFORCEMENT_ENV]), started.lease.classEnforcement);
  const testStarted = await admit(state, testTicket('t-active', 1), cfg);
  assert.equal(testStarted.lease.classEnforcement, undefined, 'a test lease never carries it');
  assert.equal(childEnv(testStarted.lease, { [CLASS_ENFORCEMENT_ENV]: 'inherited' }, 1)[CLASS_ENFORCEMENT_ENV], undefined, 'never inherited from the caller');
});

test('shadow mode logs what active would deny and still grants', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('shadow', { capCores: 2 }));
  heldSim(state, 'h1', 2);
  const result = await admit(state, simTicket('over', 2), cfg);
  assert.equal(result.started, true, 'shadow never enforces');
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /classCap=class-cap\(shadow\):cap-cores/);
});

test('with active caps, at equal effective score a test goes before a sim; with none configured the order is plain sequence order', () => {
  const now = 1_700_000_000_000;
  const mk = (id, seq, extra) => ({ id, seq, priorityAdmitted: 'medium', priorityRequested: 'medium', prioOriginAt: now, ...extra });
  const queue = [mk('sim-first', 1, { class: 'sim' }), mk('test-second', 2, {}), mk('sim-third', 3, { class: 'sim' }), mk('test-fourth', 4, {})];
  assert.deepEqual(orderQueue(queue, now, DEFAULT_GLOBAL_CONFIG, true).map((t) => t.id), ['test-second', 'test-fourth', 'sim-first', 'sim-third']);
  assert.deepEqual(orderQueue(queue, now, DEFAULT_GLOBAL_CONFIG).map((t) => t.id), ['sim-first', 'test-second', 'sim-third', 'test-fourth'], '0.28.2 order: seq only');
});

test('a fully aged sim is exempt from the tests-before-sims tie-break, so newer tests cannot overtake it forever', () => {
  const now = 1_700_000_000_000;
  const cfg = DEFAULT_GLOBAL_CONFIG;
  const mk = (id, seq, origin, extra) => ({ id, seq, priorityAdmitted: 'high', priorityRequested: 'high', prioOriginAt: origin, ...extra });
  const fresh = orderQueue([mk('sim', 1, now, { class: 'sim' }), mk('newer-test', 2, now)], now, cfg, true);
  assert.deepEqual(fresh.map((t) => t.id), ['newer-test', 'sim'], 'a young sim still yields to a test of equal score');
  const aged = orderQueue([mk('sim', 1, now - cfg.priorityAgeMaxMs, { class: 'sim' }), mk('newer-test', 2, now)], now, cfg, true);
  assert.deepEqual(aged.map((t) => t.id), ['sim', 'newer-test'], 'once fully aged the older sim goes first by seq');
});

test('P1: an exclusive sim is judged at its WHOLE-BUDGET lease, not its declared claim', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('active', { capCores: 2 }));
  const excl = simTicket('xs', 1, { exclusive: true, priorityRequested: 'high' });
  const denied = await admit(state, excl, cfg);
  assert.equal(denied.started, false);
  assert.equal(denied.reason, 'class-cap', `declared 1 core fits a 2-core cap but the exclusive lease claims the whole budget (got ${JSON.stringify(denied)})`);
  assert.equal(readLease(state, 'xs'), null);
  const roomy = cfgWith(classes('active', { capCores: 1000, maxTickets: 8, capMemoryBytes: 1024 ** 5 }));
  const ok = await poll(state, excl, roomy);
  assert.equal(ok.started, true, JSON.stringify(ok));
  assert.equal(readLease(state, 'xs').exclusive, true);
});

test('P1: one ownership check: a remote attempt owns its id against a local enqueue, except for its own fallback', async () => {
  const { state } = freshEnv();
  const id = '11111111-1111-4111-8111-111111111111';
  await createAttempt(state, id, { runner: null });
  const dup = await enqueue(state, testTicket(id));
  assert.equal(dup.existing, true);
  assert.equal(dup.state, 'attempt');
  assert.deepEqual(fs.existsSync(paths(state).queue) ? fs.readdirSync(paths(state).queue) : [], [], 'nothing was queued: no second execution');
  const stranger = await enqueue(state, testTicket(id), DEFAULT_GLOBAL_CONFIG, { ownerGeneration: 7 });
  assert.equal(stranger.existing, true, 'a different generation does not own the attempt');
  const fb = await fallbackToLocal(state, id, 'no-runner');
  assert.equal(fb.ok, true);
  const own = await enqueue(state, testTicket(id), DEFAULT_GLOBAL_CONFIG, { ownerGeneration: fb.attempt.generation });
  assert.equal(own.existing, undefined, "the owning attempt's own local fallback proceeds");
  assert.equal(readAttempt(state, id).generation, fb.attempt.generation);
  assert.equal(fs.readdirSync(paths(state).queue).length, 1);
});

test('P1: a ticket that requires class enforcement only goes to a runner advertising classes/1', async () => {
  const { binDir } = makeFakeSshBin();
  const probe = (capabilities) => ({ protocol: 1, protocols: [1, 2], version: '0.0.0', paused: false, queued: 0, running: 0, capabilities });
  const ssh = (name, payload) => {
    const bin = path.join(binDir, name);
    fs.writeFileSync(bin, `#!/usr/bin/env node\nprocess.stdout.write(${JSON.stringify(JSON.stringify(payload))} + '\\n');\nprocess.exit(0);\n`);
    fs.chmodSync(bin, 0o755);
    return bin;
  };
  const { env } = clientEnv(binDir);
  const runners = [makeRunner({ ssh: 'normal' })];
  const cfg = DEFAULT_GLOBAL_CONFIG;
  const enriched = (extra) => ({ weight: 1, resources: { cpuCores: 1, memoryBytes: GIB }, ...extra });
  const old = await selectRunner(runners, { ...remoteSelectOptions(enriched({ classEnforcement: 'required' }), cfg), sshBin: ssh('ssh-old', probe(['elastic-claims/1'])), env, deadlineMs: 3000 });
  assert.equal(old.runner, null);
  assert.match(old.skipped[0].reason, /classes\/1/);
  const newer = await selectRunner(runners, { ...remoteSelectOptions(enriched({ classEnforcement: 'required' }), cfg), sshBin: ssh('ssh-new', probe(['classes/1'])), env, deadlineMs: 3000 });
  assert.equal(newer.runner.name, runners[0].name);
  const plain = await selectRunner(runners, { ...remoteSelectOptions(enriched({}), cfg), sshBin: ssh('ssh-old2', probe(['elastic-claims/1'])), env, deadlineMs: 3000 });
  assert.equal(plain.runner.name, runners[0].name, 'a ticket without classEnforcement is unchanged on an old runner');
});

test('P2: only an ABSENT global config means no classes; unreadable or vanished-after-use fails the sim class closed', () => {
  const { home, state } = freshEnv();
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  Object.assign(process.env, { LANE_BROKER_HOME: home, LANE_BROKER_STATE: state });
  const file = path.join(home, 'config.json');
  try {
    assert.equal(resolveClasses(loadGlobalConfig(), state).valid, true, 'never configured: no classes, valid');
    fs.writeFileSync(file, JSON.stringify({ version: 1, classes: classes('active') }));
    assert.equal(resolveClasses(loadGlobalConfig(), state).mode, 'active');
    assert.equal(fs.existsSync(paths(state).classesConfigured), true, 'the fence is durable in the state dir');
    fs.rmSync(file);
    const gone = resolveClasses(loadGlobalConfig(), state);
    assert.equal(gone.valid, false, 'the file with the caps vanished: keep the fence');
    assert.match(gone.error, /classes block was in force/);
    fs.writeFileSync(file, JSON.stringify({ version: 1, classes: classes('active') }));
    assert.equal(resolveClasses(loadGlobalConfig(), state).valid, true, 'a valid file returns');
    fs.writeFileSync(file, JSON.stringify({ version: 1 }));
    assert.equal(resolveClasses(loadGlobalConfig(), state).valid, true);
    assert.equal(fs.existsSync(paths(state).classesConfigured), false, 'a valid file without classes clears the fence');
    fs.rmSync(file);
    assert.equal(resolveClasses(loadGlobalConfig(), state).valid, true, 'absent again, nothing was in force');
    fs.writeFileSync(file, JSON.stringify({ version: 1 }));
    fs.chmodSync(home, 0o000);
    try {
      const blocked = resolveClasses(loadGlobalConfig(), state);
      assert.equal(blocked.valid, false, 'an inaccessible parent is not an absent file');
      assert.match(blocked.error, /unreadable/);
    } finally {
      fs.chmodSync(home, 0o755);
    }
  } finally {
    fs.chmodSync(home, 0o755);
    process.env.LANE_BROKER_HOME = prev.home;
    process.env.LANE_BROKER_STATE = prev.state;
  }
});

test('P2: lane status reports the same reservation owner and head as admission, with the cap-denied sim listed last', async () => {
  const T0 = Date.now();
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, schedulerMode: 'active', resourceSkipLimit: 3, classes: classes('active', { capCores: 1 }) });
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
  fenceLegacyQueue(state, 'test');
  const reserved = (seq) => ({ reason: 'resource', skipsCharged: 3, reserved: true, reservationSeq: seq, inScope: true, behindConflict: false, deniedAt: T0, budget: POOL, externalBusy: 2.5 });
  await enqueue(state, simTicket('S', 2, { priorityRequested: 'high' }));
  await enqueue(state, testTicket('T', POOL));
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: { S: { resource: reserved(3) }, T: { resource: reserved(5) } } });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  Object.assign(process.env, { LANE_BROKER_HOME: home, LANE_BROKER_STATE: state });
  try {
    const status = await collectStatus();
    assert.deepEqual(status.queued.map((q) => q.id), ['T', 'S'], 'the test is the head; the cap-denied sim is not');
    assert.equal(status.queued[0].reservationOwner, true);
    assert.equal(status.queued[1].reservationOwner, undefined);
    assert.equal(status.queued[1].classDenied, 'cap-cores');
  } finally {
    process.env.LANE_BROKER_HOME = prev.home;
    process.env.LANE_BROKER_STATE = prev.state;
  }
});

test('enqueueing the same client id twice gives one ticket and one sequence', async () => {
  const { state } = freshEnv();
  const first = await enqueue(state, testTicket('same-id'));
  assert.equal(first.existing, undefined);
  const again = await enqueue(state, testTicket('same-id', 3));
  assert.equal(again.existing, true);
  assert.equal(again.state, 'queued');
  assert.equal(again.seq, first.seq);
  assert.equal(fs.readdirSync(paths(state).queue).length, 1);
  assert.equal(JSON.parse(fs.readFileSync(paths(state).seq, 'utf8')).n, 1, 'no new sequence was allocated');
  await enqueue(state, testTicket('another'));
  assert.equal(JSON.parse(fs.readFileSync(paths(state).seq, 'utf8')).n, 2);
});

test('an id that is already leased or finished is also returned as existing, with no new sequence', async () => {
  const { state } = freshEnv();
  heldSim(state, 'leased-id', 1);
  const leased = await enqueue(state, simTicket('leased-id'));
  assert.equal(leased.existing, true);
  assert.equal(leased.state, 'leased');
  fs.mkdirSync(paths(state).results, { recursive: true });
  fs.writeFileSync(path.join(paths(state).results, 'done-id.json'), JSON.stringify({ id: 'done-id', exit: 0 }));
  const done = await enqueue(state, testTicket('done-id'));
  assert.equal(done.existing, true);
  assert.equal(done.state, 'finished');
  assert.equal(fs.existsSync(paths(state).seq), false, 'neither allocated a sequence');
  assert.equal(fs.existsSync(paths(state).queue) ? fs.readdirSync(paths(state).queue).length : 0, 0);
});

test('lane run --detach --id twice submits one ticket; --id without --detach is refused', () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  fs.mkdirSync(path.join(base, 'repo'), { recursive: true });
  const bad = spawnSync(process.execPath, [BIN, 'run', '--id', 'x1', '--', 'true'], { env, cwd: base, encoding: 'utf8' });
  assert.equal(bad.status, 2);
  assert.match(bad.stderr, /--id requires --detach/);
  const badId = spawnSync(process.execPath, [BIN, 'run', '--detach', '--id', '../x', '--', 'true'], { env, cwd: base, encoding: 'utf8' });
  assert.equal(badId.status, 2);
});

test('lane capabilities --json reports the classes block and classes/1', () => {
  const { home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, classes: classes('active') });
  const out = JSON.parse(spawnSync(process.execPath, [BIN, 'capabilities', '--json'], { env, encoding: 'utf8' }).stdout);
  assert.ok(out.capabilities.includes('classes/1'));
  assert.equal(out.classes.mode, 'active');
  assert.equal(out.classes.valid, true);
  assert.match(out.classes.configHash, /^[0-9a-f]{16}$/);
  assert.equal(out.classes.stateRoot, state);
  writeGlobalConfig(home, { version: 1, classes: { mode: 'active' } });
  const broken = JSON.parse(spawnSync(process.execPath, [BIN, 'capabilities', '--json'], { env, encoding: 'utf8' }).stdout);
  assert.equal(broken.classes.valid, false);
  writeGlobalConfig(home, { version: 1 });
  const absent = JSON.parse(spawnSync(process.execPath, [BIN, 'capabilities', '--json'], { env, encoding: 'utf8' }).stdout);
  assert.deepEqual(absent.classes, { mode: 'off', configHash: null, stateRoot: state, valid: true });
});

test('lane status shows booked/cap per class in active mode and nothing when classes are absent', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, classes: classes('active', { capCores: 4 }) });
  heldSim(state, 'h1', 3);
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  Object.assign(process.env, { LANE_BROKER_HOME: home, LANE_BROKER_STATE: state });
  try {
    const status = await collectStatus();
    assert.equal(status.classes.mode, 'active');
    assert.equal(status.classes.sim.bookedCores, 3);
    assert.equal(status.classes.sim.capCores, 4);
    assert.equal(status.classes.sim.tickets, 1);
    assert.match(renderStatusText(status), /classes \(active\): sim 3\/4 cores, 1\/8 tickets/);
    writeGlobalConfig(home, { version: 1 });
    assert.equal((await collectStatus()).classes, null);
  } finally {
    process.env.LANE_BROKER_HOME = prev.home;
    process.env.LANE_BROKER_STATE = prev.state;
  }
});

test('an elastic sim is capped at the final grant, never below its floor; an immutable one over the cap is denied, not shrunk', async () => {
  const cfg = cfgWith(classes('active', { capCores: 4 }));
  const a = freshEnv().state;
  heldSim(a, 'h1', 1);
  const elastic = simTicket('elastic', 4, { resources: { cpuCores: 4, minCpuCores: 2, memoryBytes: GIB } });
  const granted = await admit(a, elastic, cfg);
  assert.equal(granted.started, true, JSON.stringify(granted));
  assert.equal(granted.lease.grantedCpuCores, 3, '1 booked leaves 3 under capCores 4: granted 3, declared 4');
  const b = freshEnv().state;
  heldSim(b, 'h1', 3);
  const belowFloor = simTicket('elastic2', 4, { resources: { cpuCores: 4, minCpuCores: 2, memoryBytes: GIB } });
  assert.equal((await admit(b, belowFloor, cfg)).reason, 'class-cap', 'only 1 core left, below the floor of 2');
  const c = freshEnv().state;
  heldSim(c, 'h1', 1);
  const immutable = simTicket('immutable', 4);
  const denied = await admit(c, immutable, cfg);
  assert.equal(denied.reason, 'class-cap');
  assert.equal(readLease(c, 'immutable'), null, 'never shrunk into a grant');
});

test('classes absent (or off) keeps the allocation golden trace byte-identical', async () => {
  const golden = JSON.parse(fs.readFileSync(new URL('./fixtures/allocation-golden-trace.json', import.meta.url), 'utf8'));
  const m = await loadModules(fileURLToPath(new URL('../src', import.meta.url)));
  for (const name of Object.keys(scenarios(m))) {
    assert.deepEqual(liveTrace(await runScript(m, scenarios(m)[name], {})), golden[name], `${name}: classes absent`);
    assert.deepEqual(liveTrace(await runScript(m, scenarios(m)[name], { cfgOverrides: { classes: { mode: 'off' } } })), golden[name], `${name}: classes off`);
  }
});

test('a cap-ineligible sim that earlier earned a head reservation neither owns the reservation nor blocks the test queued behind it', async () => {
  const T0 = 1_700_000_000_000;
  const realNow = Date.now;
  Date.now = () => T0;
  try {
    const { state } = freshEnv();
    atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
    fenceLegacyQueue(state, 'test');
    const reserved = (seq, skipsCharged) => ({ reason: 'resource', skipsCharged, reserved: true, reservationSeq: seq, inScope: true, behindConflict: false, deniedAt: T0, budget: POOL, externalBusy: 2.5 });
    // S: a sim (high, so first in order) that earned the OLDEST reservation, then the cap was lowered under it.
    // T: a heavy test head, denied on CPU by ambient load, holding the next reservation. U: a light test behind it.
    const cfg = cfgWith(classes('active', { capCores: 1 }), { resourceSkipLimit: 3, resourceIdleOvershootCores: 0, conflictSafeBackfill: false });
    const sim = simTicket('S', 2, { priorityRequested: 'high' });
    const heavy = testTicket('T', POOL);
    const light = testTicket('U', 1);
    for (const t of [sim, heavy, light]) await enqueue(state, t);
    atomicWriteJson(paths(state).fairness, { version: 2, tickets: { S: { resource: reserved(3, 3) }, T: { resource: reserved(5, 1) } } });
    const busy = (ext) => () => ({ hostBusyCores: ext, cores: HOST_CORES, stale: false, sampledAt: Date.now() });
    const at = (t, ext) => tryStart(state, t, cfg, undefined, busy(ext), undefined, memory);
    assert.equal((await at(sim, 0)).reason, 'class-cap', 'S is cap-ineligible');
    const behind = await at(light, 2.5);
    assert.equal(behind.reason, 'not-head', `T's own reservation is the active one and holds U back (got ${JSON.stringify(behind)})`);
    const admitted = await at(heavy, 0);
    assert.equal(admitted.started, true, `the test head is admitted past the cap-denied sim (got ${JSON.stringify(admitted)})`);
  } finally {
    Date.now = realNow;
  }
});

test('lane run --detach --id X twice: one ticket, one sequence, and the second exits 0 with the same id', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  fs.mkdirSync(repoDir, { recursive: true });
  const holder = spawnSync(process.execPath, [BIN, 'run', '--detach', '--id', 'holder-1', '--', 'sleep', '30'], { env, cwd: repoDir, encoding: 'utf8' });
  assert.equal(holder.status, 0, holder.stderr);
  const first = spawnSync(process.execPath, [BIN, 'run', '--detach', '--id', 'dup-1', '--', 'sleep', '30'], { env, cwd: repoDir, encoding: 'utf8' });
  const second = spawnSync(process.execPath, [BIN, 'run', '--detach', '--id', 'dup-1', '--', 'sleep', '30'], { env, cwd: repoDir, encoding: 'utf8' });
  try {
    assert.equal(first.status, 0, first.stderr);
    assert.equal(second.status, 0, second.stderr);
    assert.equal(first.stdout.trim(), 'dup-1');
    assert.equal(second.stdout.trim(), 'dup-1');
    const seqOf = () => {
      try {
        return JSON.parse(fs.readFileSync(paths(state).seq, 'utf8')).n;
      } catch {
        return null;
      }
    };
    const { waitFor } = await import('./helpers.js');
    await waitFor(() => seqOf() === 2 && fs.readdirSync(paths(state).queue).some((n) => n.endsWith('-dup-1.json')), { timeoutMs: 10_000 });
    await new Promise((r) => setTimeout(r, 1000)); // the duplicate supervisor has had time to enqueue (and be refused)
    assert.equal(seqOf(), 2, 'holder-1 and dup-1 took one sequence each; the retry took none');
    assert.equal(fs.readdirSync(paths(state).queue).filter((n) => n.endsWith('-dup-1.json')).length, 1);
  } finally {
    for (const id of ['dup-1', 'holder-1']) spawnSync(process.execPath, [BIN, 'cancel', id], { env, encoding: 'utf8' });
  }
});

test('an id is matched exactly after the sequence prefix: `job` is a new ticket while `prefix-job` is queued', async () => {
  const { state } = freshEnv();
  const first = await enqueue(state, testTicket('prefix-job'));
  const second = await enqueue(state, testTicket('job'));
  assert.equal(second.existing, undefined, '`job` is not `prefix-job`');
  assert.equal(second.seq, first.seq + 1);
  assert.equal(fs.readdirSync(paths(state).queue).length, 2);
  assert.equal((await enqueue(state, testTicket('job'))).existing, true, 'but the same id still dedups');
});

test('a fresh supervisor on a malformed global config has an unknown class policy: sims are denied, a test still runs', async () => {
  const { home, state } = freshEnv();
  fs.writeFileSync(path.join(home, 'config.json'), '{ half written');
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const cfg = { ...reloadGlobalConfig(undefined), schedulerMode: 'active', capacity: 64, cpuAdmissionPercent: 100, cpuReserveCores: HOST_CORES - POOL, admissionCooldownMs: 0, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 };
    assert.equal(resolveClasses(cfg, state).valid, false);
    assert.equal((await admit(state, simTicket('s', 1), cfg)).reason, 'class-config-invalid');
    assert.equal((await admit(state, testTicket('t', 1), cfg)).started, true);
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});

const required = (id, extra = {}) => simTicket(id, 1, { classEnforcement: 'required', ...extra });

test('classEnforcement "required" is validated and resolved from the lane', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { fleet: { weight: 1, class: 'sim', classEnforcement: 'required' }, plain: { weight: 1, class: 'sim' } } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'fleet' }).classEnforcement, 'required');
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'plain' }).classEnforcement, undefined);
  writeRepoConfig(repoDir, { version: 1, lanes: { fleet: { weight: 1, class: 'sim', classEnforcement: 'yes' } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'fleet' }), ConfigError);
});

test('a classEnforcement-required ticket is granted only under active, valid caps', async () => {
  const denied = async (name, cfg) => {
    const { state } = freshEnv();
    const result = await admit(state, required(`req-${name}`), cfg);
    assert.equal(result.started, false, name);
    assert.equal(result.reason, 'class-enforcement-unavailable', name);
    assert.equal(readLease(state, `req-${name}`), null, name);
  };
  await denied('absent', cfgWith(null));
  await denied('off', cfgWith(classes('off')));
  await denied('shadow', cfgWith(classes('shadow')));
  await denied('invalid', { ...cfgWith(classes('active')), classesInvalid: 'broken' });

  const { state } = freshEnv();
  const granted = await admit(state, required('req-active'), cfgWith(classes('active')));
  assert.equal(granted.started, true);
  assert.equal(granted.lease.classEnforcement.mode, 'active');
  assert.deepEqual(readLease(state, 'req-active').classEnforcement, granted.lease.classEnforcement);
});

test('a sim lane without classEnforcement keeps today\'s behaviour in off and shadow mode', async () => {
  for (const cfg of [cfgWith(null), cfgWith(classes('off')), cfgWith(classes('shadow'))]) {
    const { state } = freshEnv();
    const result = await admit(state, simTicket('plain', 1), cfg);
    assert.equal(result.started, true);
    assert.equal(result.lease.classEnforcement, undefined);
  }
});

test('an unavailable required ticket is skip-free: it never heads the queue or blocks a test behind it', async () => {
  const { state } = freshEnv();
  const cfg = cfgWith(classes('shadow'));
  const head = required('req-head');
  await enqueue(state, head);
  const t = testTicket('behind', 1);
  await enqueue(state, t);
  assert.equal((await poll(state, head, cfg)).reason, 'class-enforcement-unavailable');
  assert.equal((await poll(state, t, cfg)).started, true);
});
