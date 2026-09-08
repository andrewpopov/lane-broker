import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { freshEnv } from './helpers.js';
import { paths, atomicWriteJson, readJsonSafe, fingerprintOf, bootId } from '../src/state.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

const srcDir = fileURLToPath(new URL('../src', import.meta.url));

function makeCfg(overrides) {
  return { ...DEFAULT_GLOBAL_CONFIG, ...overrides };
}

function persistClosedGate(state, cfg, consecutiveUnder) {
  const fingerprint = fingerprintOf(cfg.loadClose, cfg.loadOpen, cfg.loadOpenSamples);
  atomicWriteJson(paths(state).loadGate, {
    closed: true,
    consecutiveUnder,
    fingerprint,
    lastLoad: 20,
    lastSampleAt: Date.now(),
  });
}

async function enqueueTicket(state, overrides = {}) {
  const ticket = {
    id: 'ticket-1',
    key: 'repo/lane',
    weight: 1,
    supervisorPid: process.pid,
    supervisorStart: null,
    cwd: '/tmp',
    cmd: ['true'],
    logPath: '/tmp/log',
    resultPath: '/tmp/result',
    ...overrides,
  };
  await enqueue(state, ticket);
  return ticket;
}

/**
 * BRAIN-197: the load gate no longer blocks an idle broker, so every test
 * in this file that means to exercise the gate itself (not the idle
 * exemption) needs a live, NON-CONFLICTING holder lease with capacity to
 * spare — otherwise `tryStart` never even reaches the config-under-lock
 * codepaths these tests exist to pin, it just falls through the exemption.
 * Different key from the ticket under test, fresh heartbeat, and this test
 * process's own (alive) pid so it survives the `reapAll` pass tryStart runs
 * on every poll.
 */
function holdLease(state, overrides = {}) {
  const lease = {
    id: 'holder-1',
    key: 'repo/holder-lane',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    admittedAt: Date.now(),
    cwd: '/tmp',
    cmd: ['sleep', '999'],
    weight: 1,
    logPath: '/tmp/holder-log',
    resultPath: '/tmp/holder-result',
    state: LEASE_STATE.RUNNING,
    ...overrides,
  };
  writeLease(state, lease);
  return lease;
}

test('tryStart re-reads config INSIDE the lock: a stale outer cfg cannot make an already-superseded gate transition authoritative', async () => {
  const { state } = freshEnv();

  const cfgA = makeCfg({ loadOpenSamples: 1, loadClose: 10, loadOpen: 5 });
  const cfgB = makeCfg({ loadOpenSamples: 3, loadClose: 10, loadOpen: 5 });

  // BRAIN-197: without a held lease the broker reads idle and the gate is
  // exempted outright, which would make this test pass without ever
  // reaching the reloadCfg codepath it exists to pin.
  holdLease(state);
  const ticket = await enqueueTicket(state);
  // Gate is CLOSED under cfgA, no consecutive-under samples yet.
  persistClosedGate(state, cfgA, 0);

  const loadSampler = () => 2; // under loadOpen (5): a candidate "under" sample

  // globalCfg is the STALE snapshot (cfgA); reloadCfg (called inside the
  // lock) returns the NEWER cfgB, which needs 3 consecutive under-samples
  // to reopen, not 1.
  const result = await tryStart(state, ticket, cfgA, loadSampler, undefined, () => cfgB);

  assert.equal(result.started, false, 'must not start: cfgB requires 3 consecutive under-samples, this is only the 1st');
  assert.equal(result.reason, 'load-gate-closed');

  const gateState = readJsonSafe(paths(state).loadGate);
  const expectedFingerprint = fingerprintOf(cfgB.loadClose, cfgB.loadOpen, cfgB.loadOpenSamples);
  assert.equal(gateState.fingerprint, expectedFingerprint, 'persisted gate state must be fingerprinted against cfgB, the config actually used');
  assert.equal(gateState.consecutiveUnder, 1, 'one under-sample recorded under cfgB');
  assert.equal(gateState.closed, true, 'still closed: cfgB needs loadOpenSamples=3');
});

test('control: when reloadCfg returns the SAME stale cfg, one under-sample is enough to reopen (proves the harness, not just the fix)', async () => {
  const { state } = freshEnv();

  const cfgA = makeCfg({ loadOpenSamples: 1, loadClose: 10, loadOpen: 5 });

  // Same reasoning as the test above: a held lease keeps the gate genuinely
  // in play, so this control proves the harness reopens the gate on its
  // own merits, not because the broker happened to be idle.
  holdLease(state);
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfgA, 0);

  const loadSampler = () => 2;

  const result = await tryStart(state, ticket, cfgA, loadSampler, undefined, () => cfgA);

  assert.equal(result.started, true, 'cfgA only needs 1 under-sample to reopen, and capacity/gates otherwise allow it');
});

test('MUTATION: tryStart ignoring reloadCfg (using globalCfg instead) makes the first assertion fail by name', async () => {
  // Copy the whole src/ tree so relative imports keep working, then mutate
  // ONLY the copy's scheduler.js to ignore reloadCfg — proving the guard in
  // the real scheduler.js is load-bearing, not decoration.
  const mutantSrcDir = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'lane-broker-mutant-src-'));
  fs.cpSync(srcDir, mutantSrcDir, { recursive: true });

  const schedulerPath = path.join(mutantSrcDir, 'scheduler.js');
  const source = fs.readFileSync(schedulerPath, 'utf8');
  const guarded = 'const cfg = reloadCfg() || globalCfg;';
  assert.ok(source.includes(guarded), 'expected the reloadCfg call site in scheduler.js; test needs updating');
  const mutated = source.replace(guarded, 'const cfg = globalCfg; // MUTATED: reloadCfg ignored');
  assert.notEqual(mutated, source, 'mutation must actually change the source');
  fs.writeFileSync(schedulerPath, mutated);

  try {
    const { enqueue: mutantEnqueue, tryStart: mutantTryStart } = await import(
      pathToFileURL(schedulerPath).href
    );

    const { state } = freshEnv();
    const cfgA = makeCfg({ loadOpenSamples: 1, loadClose: 10, loadOpen: 5 });
    const cfgB = makeCfg({ loadOpenSamples: 3, loadClose: 10, loadOpen: 5 });

    const ticket = {
      id: 'ticket-1',
      key: 'repo/lane',
      weight: 1,
      supervisorPid: process.pid,
      supervisorStart: null,
      cwd: '/tmp',
      cmd: ['true'],
      logPath: '/tmp/log',
      resultPath: '/tmp/result',
    };
    // BRAIN-197: this is the dangerous case. Under the idle exemption BOTH
    // the real scheduler and this mutant would start an idle ticket for the
    // same (wrong) reason — an empty `held`, not a bypassed reloadCfg — so
    // the assertion below would throw either way and the test would keep
    // passing without proving anything about the BRAIN-182 guard. A live,
    // non-conflicting holder keeps the gate genuinely applying to both the
    // real and the mutant scheduler, so only the mutant's ignored reloadCfg
    // makes it start.
    holdLease(state);
    await mutantEnqueue(state, ticket);
    persistClosedGate(state, cfgA, 0);

    const loadSampler = () => 2;
    const result = await mutantTryStart(state, ticket, cfgA, loadSampler, undefined, () => cfgB);

    // With reloadCfg ignored, the mutant evaluates everything against the
    // stale cfgA (loadOpenSamples: 1), so it wrongly reopens on the first
    // under-sample — exactly the bug BRAIN-182 exists to fix.
    assert.throws(
      () => assert.equal(result.started, false, 'must not start: cfgB requires 3 consecutive under-samples, this is only the 1st'),
      /must not start: cfgB requires 3 consecutive under-samples/,
      'the primary assertion must fail by name against the mutant (started === true here, using stale cfgA)',
    );
  } finally {
    fs.rmSync(mutantSrcDir, { recursive: true, force: true });
  }
});

test('supervisor retains a successful inner reload: a later poll never falls back to an older snapshot (BRAIN-182 P2)', async () => {
  // Reproduces src/supervisor.js's own closure shape (`reloadCfg` mutating
  // the outer `globalCfg` and returning it) directly, since main() reads
  // its ticket from an env var and drives real process lifecycle end to
  // end — exercising it through supervisor.js itself would mean spawning a
  // whole supervisor process rather than testing the retention logic in
  // isolation. This models exactly the pattern supervisor.js uses so the
  // callback under test is the same shape as the fix.
  const { state } = freshEnv();

  const cfgOld = makeCfg({ capacity: 2 });
  const cfgNew = makeCfg({ capacity: 3 });
  const loadSampler = () => 0; // always under: load gate never blocks this test

  // Poll 1 (a different ticket, weight 1): the inner reload succeeds and
  // returns cfgNew. Per the fix, the supervisor's closure must ASSIGN this
  // back to its own outer globalCfg, not just return it to tryStart.
  let globalCfg = cfgOld;
  const ticket1 = await enqueueTicket(state, { id: 'ticket-1', weight: 1 });
  const started1 = await tryStart(state, ticket1, globalCfg, loadSampler, undefined, () => {
    globalCfg = cfgNew;
    return globalCfg;
  });
  assert.equal(started1.started, true, 'ticket1 (weight 1) must start under either cfg');
  assert.deepEqual(globalCfg, cfgNew, 'a successful inner reload must be retained in the outer snapshot');

  // Poll 2 (a second ticket, weight 2): the inner reload now FAILS (e.g. the
  // file went invalid), so the callback falls back to the caller's own
  // globalCfg exactly as reloadGlobalConfig(globalCfg, { onError }) does.
  // Running weight is now 1 (ticket1). cfgOld's capacity (2) would deny
  // ticket2 (1 + 2 = 3 > 2); only the RETAINED cfgNew (capacity 3) allows
  // it (1 + 2 = 3 <= 3) — so a pass here proves the fallback read the
  // retained config, not the original stale snapshot.
  const ticket2 = await enqueueTicket(state, { id: 'ticket-2', weight: 2, key: 'repo/other-lane' });
  const started2 = await tryStart(state, ticket2, globalCfg, loadSampler, undefined, () => globalCfg);

  assert.equal(
    started2.started,
    true,
    `must start: retained cfgNew (capacity 3) allows running weight 1 + ticket weight 2; got ${JSON.stringify(started2)}`,
  );
});
