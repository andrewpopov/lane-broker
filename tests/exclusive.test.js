import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { freshEnv, writeGlobalConfig, writeRepoConfig, gitFixture } from './helpers.js';
import { enqueue, tryStart, couldAdmitNow, listQueue } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, resolveTicketConfig, resolvePriority, ConfigError } from '../src/config.js';
import { writeLease, removeLease, listLeases, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson, writeCancelMarkerFile, writeWithdrawMarkerFile } from '../src/state.js';
import { fenceLegacyQueue } from '../src/migrate.js';
import { score, effectiveRank } from '../src/priority.js';
import { detectResourceCapacity, cpuBudgetCores } from '../src/resources.js';
import { readGateState } from '../src/load.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { runCommand } from '../src/run.js';
import { CAPABILITIES } from '../src/capabilities.js';
import { exclusiveDeny, withoutBackfillExclusives } from '../src/exclusive.js';

/**
 * BRAIN-403 v1: stateless, hookless exclusive lanes. Real tryStart against a temp state dir. Fixture machine for
 * the scheduler tests: weight capacity 10, CPU budget capped by cpuReserveCores 1 / cpuAdmissionPercent 100.
 */

const GIB = 1024 ** 3;
const MIN = 60_000;
const T0 = 1_700_000_000_000;
const LIMIT = 3;
const MODES = ['legacy', 'priority'];

const baseCfg = (overrides = {}) => ({
  ...DEFAULT_GLOBAL_CONFIG,
  schedulerMode: 'active',
  capacity: 10,
  loadClose: 1000,
  loadOpen: 900,
  loadOpenSamples: 1,
  cpuAdmissionPercent: 100,
  cpuReserveCores: 1,
  admissionCooldownMs: 0,
  conflictSkipLimit: LIMIT,
  resourceSkipLimit: 3,
  resourceIdleOvershootCores: 0,
  ...overrides,
});

const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = (macPressure = 'normal') => () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure, source: 'test' });
const poll = (state, t, cfg, ext = 1, loadSampler, mem = memory()) => tryStart(state, t, cfg, loadSampler, sampler(ext), undefined, mem);

const ticket = (id, overrides = {}) => ({
  id,
  key: `r:${id}`,
  repoId: id,
  weight: 1,
  cwd: process.cwd(),
  cmd: ['true'],
  supervisorPid: process.pid,
  supervisorStart: null,
  logPath: '/dev/null',
  resultPath: '/dev/null',
  ...overrides,
});
const exclusiveTicket = (id, overrides = {}) => ticket(id, { exclusive: true, priorityRequested: 'high', ...overrides });
const heldLease = (id, key, weight = 1, extra = {}) => ({ id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight, state: LEASE_STATE.RUNNING, ...extra });

function stateFor(mode) {
  const { state } = freshEnv();
  if (mode === 'priority') {
    atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
    fenceLegacyQueue(state, 'test');
    atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
  }
  return state;
}

async function withClock(fn) {
  const realNow = Date.now;
  Date.now = () => T0;
  try {
    return await fn();
  } finally {
    Date.now = realNow;
  }
}

// ---- backfill scenarios: each arranges a state where a ticket BEHIND the head starts when the head is ordinary ----

const SCENARIOS = {
  'conflict skip': {
    cfg: baseCfg(),
    arrange: async (state, mk) => {
      writeLease(state, heldLease('holder', 'r:holder'));
      const head = mk('head', { key: 'r:head', weight: 3, conflicts: ['r:holder'] });
      await enqueue(state, head);
      return { head, behind: mk('behind', {}), ext: 1 };
    },
  },
  'capacity skip': {
    cfg: baseCfg(),
    arrange: async (state, mk) => {
      writeLease(state, heldLease('holder', 'r:holder', 5));
      const head = mk('head', { weight: 8 });
      await enqueue(state, head);
      return { head, behind: mk('behind', {}), ext: 1 };
    },
  },
  'resource backfill': {
    cfg: baseCfg(),
    arrange: async (state, mk, cfg) => {
      writeLease(state, heldLease('lane-load', 'lane:load', 2));
      const head = mk('head', { weight: 4 });
      await enqueue(state, head);
      return { head, behind: mk('behind', {}), ext: 3.4, prepare: async (behind) => { await enqueue(state, behind); if (!head.exclusive) assert.equal((await poll(state, head, cfg, 3.4)).cpuReason, 'projected-over-budget'); } };
    },
  },
  'conflict safe backfill': {
    cfg: baseCfg(),
    arrange: async (state, mk) => {
      writeLease(state, heldLease('holder', 'r:holder'));
      const head = mk('head', { key: 'r:head', weight: 3, conflicts: ['r:holder'] });
      await enqueue(state, head);
      atomicWriteJson(paths(state).conflictSkipState, { headId: 'head', count: LIMIT, blockedSince: Date.now(), loggedPhase: 'exhausted' });
      return { head, behind: mk('behind', {}), ext: 1 };
    },
  },
  'sim safe backfill': {
    cfg: baseCfg(),
    arrange: async (state, mk) => {
      writeLease(state, heldLease('holder', 'r:holder', 3));
      const head = mk('head', { weight: 2 });
      await enqueue(state, head);
      return { head, behind: mk('behind', { class: 'sim' }), ext: 1 };
    },
  },
};

async function runScenario(name, mode, { exclusiveHead, exclusiveBehind }) {
  const { cfg, arrange } = SCENARIOS[name];
  const state = stateFor(mode);
  const mkHead = (id, o) => (id === 'head' ? ticket(id, { ...o, ...(exclusiveHead ? { exclusive: true, priorityRequested: 'high' } : {}) }) : ticket(id, { ...o, ...(exclusiveBehind ? { exclusive: true } : {}) }));
  const arranged = await arrange(state, mkHead, cfg);
  if (arranged.prepare) await arranged.prepare(arranged.behind);
  else await enqueue(state, arranged.behind);
  return { state, head: arranged.head, behind: arranged.behind, result: await poll(state, arranged.behind, cfg, arranged.ext) };
}

for (const mode of MODES) {
  for (const name of Object.keys(SCENARIOS)) {
    test(`AC1 [${mode}] ${name}: control, an ordinary head lets the ticket behind it backfill`, async () => {
      const { result } = await runScenario(name, mode, { exclusiveHead: false, exclusiveBehind: false });
      assert.equal(result.started, true, JSON.stringify(result));
    });

    test(`AC1 [${mode}] ${name}: an exclusive head denies the ticket behind it (no backfill of any kind)`, async () => {
      const { result, head, state } = await runScenario(name, mode, { exclusiveHead: true, exclusiveBehind: false });
      assert.equal(result.started, false, JSON.stringify(result));
      assert.equal(result.reason, 'exclusive-head');
      assert.equal(result.holder, 'head');
      assert.equal(listQueue(state).some((t) => t?.id === head.id), true, 'the exclusive head stays queued');
    });

    test(`walk exclusion [${mode}] ${name}: an exclusive ticket behind an ordinary head is never a backfill candidate`, async () => {
      const { result } = await runScenario(name, mode, { exclusiveHead: false, exclusiveBehind: true });
      assert.equal(result.started, false, JSON.stringify(result));
      assert.equal(result.reason, 'not-head');
    });
  }
}

// ---- AC1: newer tickets of every priority ----

for (const mode of MODES) {
  for (const tier of ['low', 'medium', 'high']) {
    test(`AC1 [${mode}]: a newer ${tier} ticket is denied while an exclusive is the head, and the exclusive starts once the lanes drain`, async () => {
      const state = stateFor(mode);
      const cfg = baseCfg({ schedulerMode: 'shadow' });
      writeLease(state, heldLease('running', 'r:running', 2));
      const excl = exclusiveTicket('excl', { resources: { cpuCores: 2, memoryBytes: GIB } });
      await enqueue(state, excl);
      const newer = ticket('newer', { priorityRequested: tier });
      await enqueue(state, newer);

      const denied = await poll(state, newer, cfg);
      assert.equal(denied.started, false);
      assert.equal(denied.reason, 'exclusive-head');
      const draining = await poll(state, excl, cfg);
      assert.equal(draining.reason, 'exclusive-draining');
      assert.equal(draining.running, 1);

      removeLease(state, 'running');
      assert.equal((await poll(state, newer, cfg)).reason, 'exclusive-head', 'still denied with nothing running');
      const started = await poll(state, excl, cfg);
      assert.equal(started.started, true, JSON.stringify(started));
      const lease = started.lease;
      assert.equal(lease.exclusive, true);
      assert.equal(lease.weight, cfg.capacity, 'the lease claims the whole weight capacity');
      assert.equal(lease.resources.cpuCores, cpuBudgetCores(detectResourceCapacity(), cfg));
      assert.equal(lease.resources.memoryBytes, Math.max(0, detectResourceCapacity().memoryBytes - cfg.memoryReserveBytes));
      assert.deepEqual(lease.declaredResources, excl.resources);
      assert.equal(listLeases(state).find((l) => l.id === 'excl').exclusive, true, 'persisted on the lease');
    });
  }
}

test('AC1: while the exclusive runs, a non-conflicting ticket of any weight is denied', async () => {
  const state = stateFor('legacy');
  const cfg = baseCfg();
  const started = await poll(state, await enqueue(state, exclusiveTicket('excl')), cfg);
  assert.equal(started.started, true, JSON.stringify(started));
  const other = ticket('other');
  await enqueue(state, other);
  const denied = await poll(state, other, cfg);
  assert.equal(denied.started, false);
  assert.equal(denied.reason, 'exclusive-held');
});

for (const schedulerMode of ['shadow', 'active']) {
  test(`a 1e-20-weight non-conflicting candidate is denied beside a held exclusive lease (${schedulerMode} mode)`, async () => {
    const state = stateFor('legacy');
    const cfg = baseCfg({ schedulerMode });
    const started = await poll(state, await enqueue(state, exclusiveTicket('excl')), cfg);
    assert.equal(started.started, true, JSON.stringify(started));
    assert.equal(started.lease.weight, 10);
    assert.equal(10 + 1e-20 > 10, false, 'the capacity arithmetic alone would admit it');
    const tiny = ticket('tiny', { weight: 1e-20 });
    await enqueue(state, tiny);
    const denied = await poll(state, tiny, cfg);
    assert.equal(denied.started, false, JSON.stringify(denied));
    assert.equal(denied.reason, 'exclusive-held');
    assert.deepEqual(listLeases(state).map((l) => l.id), ['excl']);
  });
}

// ---- A1 ----

test('A1: an exclusive earns no age credit: score and rank equal a fresh ticket of the same tier', () => {
  const cfg = baseCfg();
  const aged = { priorityAdmitted: 'low', prioOriginAt: T0 - 60 * MIN };
  assert.equal(score({ ...aged, exclusive: true }, T0, cfg), 0);
  assert.equal(effectiveRank({ ...aged, exclusive: true }, T0, cfg), 0);
  assert.equal(score(aged, T0, cfg), 2, 'control: an ordinary aged low reaches the ceiling');
});

test('A1: an aged low exclusive never precedes a fresh medium ticket; the ordinary aged low does', async () => {
  await withClock(async () => {
    for (const [exclusive, agedStarts] of [[true, false], [false, true]]) {
      const state = stateFor('priority');
      const cfg = baseCfg({ schedulerMode: 'shadow', conflictSafeBackfill: false });
      const aged = ticket('aged-low', { priorityRequested: 'low', prioOriginAt: T0 - 60 * MIN, ...(exclusive ? { exclusive: true } : {}) });
      const fresh = ticket('fresh', { priorityRequested: 'medium' });
      await enqueue(state, aged);
      await enqueue(state, fresh);
      assert.equal((await poll(state, aged, cfg)).started, agedStarts, `exclusive=${exclusive}`);
      if (!agedStarts) assert.equal((await poll(state, fresh, cfg)).started, true);
    }
  });
});

test('a newer high ticket cannot displace a latched high exclusive (equal score, older seq wins)', async () => {
  await withClock(async () => {
    const state = stateFor('priority');
    const cfg = baseCfg({ schedulerMode: 'shadow' });
    await enqueue(state, exclusiveTicket('excl'));
    const newer = ticket('newer', { priorityRequested: 'high' });
    await enqueue(state, newer);
    assert.equal((await poll(state, newer, cfg)).reason, 'exclusive-head');
  });
});

// ---- AC3 ----

test('AC3: with ambient CPU over budget and the load gate closed, an exclusive on an empty broker still admits; a normal ticket does not', async () => {
  const cfg = baseCfg({ loadClose: 1, loadOpen: 0.5, admissionLoadGate: true });
  const overBudget = 50;
  const busy = () => 1000;

  const normalState = stateFor('legacy');
  const normal = ticket('normal');
  await enqueue(normalState, normal);
  const control = await poll(normalState, normal, cfg, overBudget, busy);
  assert.equal(control.started, false, 'control');
  assert.equal(control.reason, 'cpu-admission');

  const state = stateFor('legacy');
  const excl = exclusiveTicket('excl');
  await enqueue(state, excl);
  const started = await poll(state, excl, cfg, overBudget, busy);
  assert.equal(started.started, true, JSON.stringify(started));
  assert.equal(readGateState(state).closed, true, 'the load gate was closed');
});

test('an exclusive still honours a critical memory reading (no acquire hook exists to clear it in v1)', async () => {
  const state = stateFor('legacy');
  const excl = exclusiveTicket('excl');
  await enqueue(state, excl);
  const denied = await poll(state, excl, baseCfg(), 1, undefined, memory('critical'));
  assert.equal(denied.started, false);
  assert.equal(denied.reason, 'memory-critical');
});

test('an exclusive head still reports paused and a pending cancel, and stays queued', async () => {
  const state = stateFor('legacy');
  const cfg = baseCfg();
  const excl = exclusiveTicket('excl');
  await enqueue(state, excl);
  fs.writeFileSync(paths(state).pause, 'maintenance');
  const paused = await poll(state, excl, cfg);
  assert.equal(paused.reason, 'paused');
  fs.rmSync(paths(state).pause);
  writeCancelMarkerFile(state, 'excl');
  const cancelled = await poll(state, excl, cfg);
  assert.equal(cancelled.started, false);
  assert.equal(cancelled.reason, 'cancelled', 'the final cancel re-check runs before the dequeue and lease write');
  assert.equal(listLeases(state).length, 0);
  assert.equal(listQueue(state).some((t) => t?.id === 'excl'), true);
});

// ---- terminal checks keep their own outcome behind an exclusive head ----

test('a non-holder behind an exclusive head still gets its own queue-timeout and withdrawal result', async () => {
  const state = stateFor('legacy');
  const cfg = baseCfg();
  await enqueue(state, exclusiveTicket('excl'));
  const expiring = ticket('expiring', { startDeadline: Date.now() - 1 });
  const withdrawn = ticket('withdrawn');
  const waiting = ticket('waiting');
  for (const t of [expiring, withdrawn, waiting]) await enqueue(state, t);
  writeWithdrawMarkerFile(state, 'withdrawn');
  assert.equal((await poll(state, expiring, cfg)).reason, 'queue-timeout');
  assert.equal((await poll(state, withdrawn, cfg)).reason, 'withdrawn');
  assert.equal((await poll(state, waiting, cfg)).reason, 'exclusive-head');
});

// ---- couldAdmitNow ----

test('couldAdmitNow denies while an exclusive is queued or held', async () => {
  const cfg = baseCfg();
  const queued = stateFor('legacy');
  await enqueue(queued, exclusiveTicket('excl'));
  assert.deepEqual(await couldAdmitNow(queued, cfg, ticket('new'), memory()), { admit: false, reason: 'queue-ahead' });

  const held = stateFor('legacy');
  writeLease(held, heldLease('excl', 'r:excl', 10, { exclusive: true }));
  assert.deepEqual(await couldAdmitNow(held, cfg, ticket('new', { weight: 1e-20 }), memory()), { admit: false, reason: 'exclusive-held' });

  const ordinary = stateFor('legacy');
  writeLease(ordinary, heldLease('plain', 'r:plain', 1));
  assert.equal((await couldAdmitNow(ordinary, cfg, ticket('new'), memory())).admit, true, 'control');
});

// ---- pure predicates ----

test('exclusiveDeny / withoutBackfillExclusives: the head gate, draining, and the unreadable-record barrier', () => {
  const excl = { id: 'e', exclusive: true };
  const plain = { id: 'p' };
  assert.equal(exclusiveDeny([plain, excl], excl, []), null, 'an exclusive behind an ordinary head is not gated here (the walks skip it)');
  assert.equal(exclusiveDeny([excl, plain], excl, []), null);
  assert.deepEqual(exclusiveDeny([excl, plain], excl, [{ id: 'x' }]), { reason: 'exclusive-draining', running: 1 });
  assert.deepEqual(exclusiveDeny([excl, plain], plain, []), { reason: 'exclusive-head', holder: 'e' });
  assert.deepEqual(exclusiveDeny([plain], plain, [{ id: 'h', exclusive: true }]), { reason: 'exclusive-held', holder: 'h' });
  assert.deepEqual(withoutBackfillExclusives([plain, excl, null, excl, plain]), [plain, null, plain]);
  assert.deepEqual(withoutBackfillExclusives([excl, plain]), [excl, plain], 'the head stays');
});

// ---- status ----

async function withStatusEnv(fn) {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await fn({ state });
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

test('status: a draining exclusive head shows the HOLD line, and a running exclusive is marked [exclusive]', async () => {
  await withStatusEnv(async ({ state }) => {
    writeLease(state, heldLease('running', 'r:running', 2));
    await enqueue(state, exclusiveTicket('excl'));
    const draining = await collectStatus();
    assert.deepEqual(draining.exclusiveHold, { id: 'excl', key: 'r:excl', waitingFor: 1 });
    assert.equal(draining.queued[0].exclusive, true);
    const text = renderStatusText(draining);
    assert.match(text, /^HOLD \(exclusive\) excl \(r:excl\) waiting for 1 running lane\(s\)$/m);
    assert.match(text, /#1 excl .*\[exclusive\]/);

    removeLease(state, 'running');
    writeLease(state, heldLease('excl', 'r:excl', 10, { exclusive: true }));
    for (const f of fs.readdirSync(paths(state).queue)) fs.rmSync(path.join(paths(state).queue, f));
    const running = await collectStatus();
    assert.equal(running.exclusiveHold, null);
    assert.equal(running.running[0].exclusive, true);
    assert.match(renderStatusText(running), /RUNNING:\n {2}excl .*\[exclusive\]/);
  });
});

// ---- `lane run` ----

const RUNNERS = [{ name: 'skybox', ssh: 'skybox-runner' }];

/** Run `runCommand` against a fake supervisor and hand back the decoded ticket (same seam as remote-eligibility). */
async function submit({ env, cwd, lane = 'default', extraEnv = {}, ...options }) {
  let ticket = null;
  let spawnCalled = false;
  const spawnSupervisor = (execPath, args, opts) => {
    spawnCalled = true;
    ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
    atomicWriteJson(ticket.resultPath, { id: ticket.id, exit: 0, signal: null, startedAt: Date.now(), endedAt: Date.now(), waitedMs: 0 });
    const silent = () => new PassThrough().end();
    return Object.assign(new EventEmitter(), { pid: process.pid, unref() {}, stdout: silent(), stderr: silent() });
  };
  const names = ['LANE_BROKER_HOME', 'LANE_BROKER_STATE', 'LANE_BROKER_LEASE', 'LANE_BROKER_KEY', 'LANE_BROKER_LOCAL', 'LANE_BROKER_PRIORITY'];
  const prev = Object.fromEntries(names.map((n) => [n, process.env[n]]));
  for (const n of names) delete process.env[n];
  process.env.LANE_BROKER_HOME = env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = env.LANE_BROKER_STATE;
  Object.assign(process.env, extraEnv);
  let stderr = '';
  const origWrite = process.stderr.write.bind(process.stderr);
  process.stderr.write = (chunk, ...rest) => {
    stderr += chunk;
    return origWrite(chunk, ...rest);
  };
  try {
    const result = await runCommand({ repo: 'r', lane, cmd: ['true'], cwd, spawnSupervisor, ...options });
    return { result, ticket, spawnCalled, stderr };
  } finally {
    process.stderr.write = origWrite;
    for (const n of names) {
      if (prev[n] === undefined) delete process.env[n];
      else process.env[n] = prev[n];
    }
  }
}

function setup({ lane = {}, repo = {}, host = {} } = {}) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, ...host });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane }, plain: { weight: 1 } }, ...repo });
  gitFixture(['init', '-q'], repoDir);
  return { base, home, state, env, repoDir };
}

test('lane run --exclusive: the ticket is exclusive, defaults to high, and is never remote even with a remote lane and runners', async () => {
  const { env, repoDir } = setup({ lane: { remote: true }, host: { runners: RUNNERS } });
  const { result, ticket: t, stderr } = await submit({ env, cwd: repoDir, exclusive: true });
  assert.equal(result.exitCode, 0, stderr);
  assert.equal(t.exclusive, true);
  assert.equal(t.priorityRequested, 'high');
  assert.equal(Object.hasOwn(t, 'remote'), false, 'no remote payload');
  assert.equal(t.localReason, 'exclusive');
  assert.equal(t.resources.minCpuCores, undefined);

  const control = await submit({ env, cwd: repoDir });
  assert.equal(Object.hasOwn(control.ticket, 'exclusive'), false, 'an ordinary ticket carries no exclusive key');
  assert.equal(control.ticket.priorityRequested, 'medium');
  assert.ok(control.ticket.remote, 'control: the same lane without --exclusive is remote-eligible');
});

test('lanes.<n>.exclusive makes the lane exclusive; an explicit priority (cli, lane config) still wins over the high default', async () => {
  const { env, repoDir } = setup({ lane: { exclusive: true } });
  assert.equal((await submit({ env, cwd: repoDir })).ticket.priorityRequested, 'high');
  assert.equal((await submit({ env, cwd: repoDir })).ticket.exclusive, true);
  assert.equal((await submit({ env, cwd: repoDir, priority: 'low' })).ticket.priorityRequested, 'low');
  const configured = setup({ lane: { exclusive: true, priority: 'medium' } });
  assert.equal((await submit({ env: configured.env, cwd: configured.repoDir })).ticket.priorityRequested, 'medium');
  assert.equal(resolvePriority({ exclusive: true }), 'high');
  assert.equal(resolvePriority({ exclusive: false }), 'medium');
  assert.equal(resolvePriority({ env: 'low', exclusive: true }), 'low');
});

test('lane config: exclusive must be a boolean and is not inherited through an undeclaredLanes template', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  const resolve = (lane) => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane });
  writeRepoConfig(repoDir, { version: 1, lanes: { big: { weight: 1, exclusive: true } }, undeclaredLanes: { as: 'big' } });
  assert.equal(resolve('big').exclusive, true);
  assert.equal(resolve('big-adhoc').exclusive, false, 'the template does not pass exclusive on');
  for (const bad of ['true', 1, null]) {
    writeRepoConfig(repoDir, { version: 1, lanes: { big: { weight: 1, exclusive: bad } } });
    assert.throws(() => resolve('big'), (err) => err instanceof ConfigError && /lane "big"\.exclusive must be a boolean/.test(err.message), JSON.stringify(bad));
  }
});

test('runner intake (configRoot set) suppresses a lane-config or flag exclusive, with a note', async () => {
  const { env, repoDir } = setup({ lane: { exclusive: true } });
  for (const options of [{}, { exclusive: true }]) {
    const { result, ticket: t, stderr } = await submit({ env, cwd: repoDir, configRoot: repoDir, ...options });
    assert.equal(result.exitCode, 0, stderr);
    assert.equal(Object.hasOwn(t, 'exclusive'), false);
    assert.equal(t.priorityRequested, 'medium', 'the suppressed lane does not take the exclusive high default');
    assert.match(stderr, /exclusive ignored on runner intake/);
  }
});

test('an exclusive nested under another lane exits 64 before enqueue; a same-key nested run is reentrant as today', async () => {
  const { env, repoDir, state } = setup({ lane: { exclusive: true } });
  const ownKey = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).key;
  const parent = (key) => writeLease(state, heldLease('parent', key, 4, { resources: { cpuCores: 4, memoryBytes: GIB } }));

  parent('some:other-lane');
  const nested = await submit({ env, cwd: repoDir, extraEnv: { LANE_BROKER_LEASE: 'parent', LANE_BROKER_KEY: 'some:other-lane' } });
  assert.equal(nested.result.exitCode, 64);
  assert.equal(nested.spawnCalled, false);
  assert.match(nested.stderr, /refusing an exclusive lane nested under "some:other-lane"/);

  // prepush under the same repo is normally allowed to nest; an exclusive prepush is not
  const prepushRepo = setup({ lane: { exclusive: true }, repo: {} });
  writeRepoConfig(prepushRepo.repoDir, { version: 1, lanes: { default: { weight: 1 }, prepush: { weight: 1, exclusive: true } } });
  const repoId = resolveTicketConfig({ cwd: prepushRepo.repoDir, repo: 'r', lane: 'prepush' }).repoId;
  writeLease(prepushRepo.state, heldLease('parent', `${repoId}:default`, 4, { resources: { cpuCores: 4, memoryBytes: GIB } }));
  const viaPrepush = await submit({ env: prepushRepo.env, cwd: prepushRepo.repoDir, lane: 'prepush', extraEnv: { LANE_BROKER_LEASE: 'parent', LANE_BROKER_KEY: `${repoId}:default` } });
  assert.equal(viaPrepush.result.exitCode, 64, viaPrepush.stderr);

  parent(ownKey);
  const reentrant = await submit({ env, cwd: repoDir, extraEnv: { LANE_BROKER_LEASE: 'parent', LANE_BROKER_KEY: ownKey } });
  assert.equal(reentrant.result.exitCode, 0, reentrant.stderr);
  assert.equal(reentrant.spawnCalled, false, 'same-key nesting runs directly under the inherited lease');
});

test('host config carrying exclusiveHooks (or acquire/release) is refused at lane run submission, not swallowed by the reload fallback', async () => {
  for (const hooks of [{ exclusiveHooks: { acquire: ['/bin/true'], release: ['/bin/true'] } }, { acquire: ['/bin/true'] }, { release: ['/bin/true'] }]) {
    const { env, repoDir } = setup({ host: hooks });
    const { result, spawnCalled, stderr } = await submit({ env, cwd: repoDir, lane: 'plain' });
    assert.equal(result.exitCode, 64, `${JSON.stringify(hooks)}: ${stderr}`);
    assert.equal(spawnCalled, false);
    assert.match(stderr, /not supported yet \(BRAIN-403 follow-up\)/);
    assert.match(stderr, new RegExp(Object.keys(hooks)[0]));
  }
  const clean = setup();
  assert.equal((await submit({ env: clean.env, cwd: clean.repoDir, lane: 'plain' })).result.exitCode, 0, 'control: no hook keys, no refusal');
});

test('repo config carrying exclusiveHooks (top level or on a lane) is refused through the exit-64 path', async () => {
  for (const repo of [{ exclusiveHooks: { acquire: ['/bin/true'] } }, { lanes: { default: { weight: 1, acquire: ['/bin/true'] } } }, { lanes: { default: { weight: 1, exclusiveHooks: {} } } }]) {
    const { env, repoDir } = setup({ repo });
    const { result, spawnCalled, stderr } = await submit({ env, cwd: repoDir });
    assert.equal(result.exitCode, 64, `${JSON.stringify(repo)}: ${stderr}`);
    assert.equal(spawnCalled, false);
    assert.match(stderr, /not supported yet \(BRAIN-403 follow-up\)/);
  }
});

// ---- surface ----

test('the exclusive/1 capability is advertised and --exclusive is in the lane usage text', () => {
  assert.ok(CAPABILITIES.includes('exclusive/1'));
  const lane = fs.readFileSync(new URL('../bin/lane.js', import.meta.url), 'utf8');
  assert.match(lane, /\[--priority high\|medium\|low\] \[--exclusive\]/);
  assert.match(lane, /case '--exclusive':/);
});
