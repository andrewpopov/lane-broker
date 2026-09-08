import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { paths, atomicWriteJson, readJsonSafe, fingerprintOf, bootId } from '../src/state.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

// BRAIN-207: with admissionLoadGate: false (the default), the load gate is
// still sampled and logged every poll, but it never denies a start -- not
// for an idle broker (the old BRAIN-197 exemption), and not for one holding
// a non-conflicting lease with spare capacity (a case the old exemption
// explicitly did NOT cover -- see idle-gate-exemption.test.js's "one held
// lease... suppresses the idle exemption").

function makeCfg(overrides) {
  return { ...DEFAULT_GLOBAL_CONFIG, admissionLoadGate: false, ...overrides };
}

function persistClosedGate(state, cfg, consecutiveUnder = 0) {
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
    id: 'candidate-1',
    key: 'repo/candidate',
    weight: 1,
    conflicts: [],
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

/** A live, non-conflicting holder lease: different key, spare capacity, and
 *  survives reapAll (fresh heartbeat, this test process's own pid, matching
 *  bootId). */
function holdLease(state, overrides = {}) {
  const lease = {
    id: 'holder-1',
    key: 'repo/holder',
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

test('flag-false + closed gate + idle broker: starts, and the gate never denies it', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfg, 0);
  const loadSampler = () => 20; // stays closed

  const result = await tryStart(state, ticket, cfg, loadSampler);
  assert.equal(result.started, true, 'a closed gate must never deny with admissionLoadGate: false');

  const gateState = readJsonSafe(paths(state).loadGate);
  assert.equal(gateState.closed, true, 'the gate itself is still sampled and still reads closed -- ignored, not reopened');
});

test('flag-false + closed gate + one held non-conflicting lease + spare capacity: starts (the case idle-exempt never covered)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  holdLease(state, { key: 'repo/holder', weight: 1 });
  const ticket = await enqueueTicket(state, { key: 'repo/candidate', weight: 1 });
  persistClosedGate(state, cfg, 0);
  const loadSampler = () => 20;

  const result = await tryStart(state, ticket, cfg, loadSampler);
  assert.equal(result.started, true, 'BRAIN-197 idle-exempt would have refused this (broker is not idle); the flag now admits it anyway');
});

test('flag-false + over-capacity: still denied capacity (the hard bound is unaffected)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 1, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state, { weight: 2 }); // exceeds capacity even with nothing else held
  persistClosedGate(state, cfg, 0);
  const loadSampler = () => 20;

  const result = await tryStart(state, ticket, cfg, loadSampler);
  assert.equal(result.started, false, 'capacity must still refuse regardless of the load-gate flag');
  assert.equal(result.reason, 'capacity');
});

test('flag-false + conflict: still denied (conflicts are unaffected by the load-gate flag)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  holdLease(state, { key: 'repo/candidate', weight: 1 }); // SAME key as the ticket below -> conflict
  const ticket = await enqueueTicket(state, { key: 'repo/candidate', weight: 1 });

  const result = await tryStart(state, ticket, cfg, () => 0);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'conflict');
});

test('flag-false + paused: still denied (pause is checked before the gate either way)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfg, 0);
  fs.writeFileSync(paths(state).pause, 'operator paused');

  const result = await tryStart(state, ticket, cfg, () => 20);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'paused');
});

test('the admission log carries loadGateIgnored=true when a closed gate would otherwise have mattered', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfg, 0);

  const result = await tryStart(state, ticket, cfg, () => 20);
  assert.equal(result.started, true);

  const logged = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(logged, /loadGateIgnored=true/, 'the closed gate was ignored, and telemetry must say so');
  assert.match(logged, /current=start:ok/, 'with the flag false, a normal start is "ok", never "idle-exempt"');
});

test('the admission log carries loadGateIgnored=false when the gate is open (nothing to ignore)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  // No persisted closed gate: sampleAndUpdateGate will see a low load and stay open.

  const result = await tryStart(state, ticket, cfg, () => 0);
  assert.equal(result.started, true);

  const logged = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(logged, /loadGateIgnored=false/);
});

test('memory brake: an injected critical reading denies, even for an idle broker', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  const memoryReader = () => ({ macPressure: 'critical', availableBytes: 0 });

  const result = await tryStart(state, ticket, cfg, () => 0, undefined, undefined, memoryReader);
  assert.equal(result.started, false, 'critical memory pressure must deny even an otherwise-idle broker');
  assert.equal(result.reason, 'memory-critical');
});

test('memory brake: a "warn" reading never denies', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  const memoryReader = () => ({ macPressure: 'warn', availableBytes: 0 });

  const result = await tryStart(state, ticket, cfg, () => 0, undefined, undefined, memoryReader);
  assert.equal(result.started, true, 'only "critical" denies; "warn" must admit');
});

test('memory brake: a null macPressure reading never denies', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  const memoryReader = () => ({ macPressure: null, availableBytes: 0 });

  const result = await tryStart(state, ticket, cfg, () => 0, undefined, undefined, memoryReader);
  assert.equal(result.started, true, 'a missing/unknown reading must never deny');
});

test('memory brake: a throwing reader degrades to admit, never crashes tryStart', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  const memoryReader = () => {
    throw new Error('probe exploded');
  };

  const result = await tryStart(state, ticket, cfg, () => 0, undefined, undefined, memoryReader);
  assert.equal(result.started, true, 'a throwing memory reader must fail open, not abort admission');
});
