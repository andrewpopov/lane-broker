import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import {
  coldStartEstimate,
  leaseDemand,
  updateCpuGateState,
  evaluateCpuAdmission,
  evaluateNewAdmission,
  sampleCpuSafe,
  cooldownActive,
  sampleAndUpdateCpuGate,
  formatAdmissionLog,
  KNOWN_BIAS_NOTE,
} from '../src/admission.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig } from '../src/config.js';
import { writeLease, readLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';

const CPU_CFG = { cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 3 };

test('coldStartEstimate reads weight directly as an approximate core count', () => {
  assert.equal(coldStartEstimate(2), 2);
  assert.equal(coldStartEstimate(0), 0);
  assert.equal(coldStartEstimate(-1), 0, 'never negative');
});

test('leaseDemand falls back to the cold-start estimate when there is no observed measurement', () => {
  assert.equal(leaseDemand({ weight: 2 }), 2);
});

test('leaseDemand takes the max of a fresh observed measurement and the cold-start estimate', () => {
  assert.equal(leaseDemand({ weight: 2, observedCpuCores: 3.5 }), 3.5);
  assert.equal(leaseDemand({ weight: 2, observedCpuCores: 0.1 }), 2, 'cold-start wins when observed is lower');
});

test('an old lease file with no observedCpuCores field still loads and leaseDemand uses the cold-start branch', () => {
  const { state } = freshEnv();
  const lease = {
    id: 'legacy',
    key: 'r:default',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    weight: 2,
    state: LEASE_STATE.RUNNING,
  };
  writeLease(state, lease);
  const loaded = readLease(state, 'legacy');
  assert.equal(loaded.observedCpuCores, undefined);
  assert.equal(leaseDemand(loaded), 2);
});

test('an old global config file with none of the phase-1 scheduler keys still loads, with their defaults filled in', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const cfg = loadGlobalConfig();
    assert.equal(cfg.schedulerMode, 'shadow');
    assert.equal(cfg.cpuAdmissionPercent, 75);
    assert.equal(cfg.cpuClosePercent, 90);
    assert.equal(cfg.cpuOpenPercent, 70);
    assert.equal(cfg.cpuOpenSamples, 3);
    assert.equal(cfg.admissionCooldownMs, 5000);
    assert.equal(cfg.memoryCloseBytes, 4294967296);
    assert.equal(cfg.memoryOpenBytes, 8589934592);
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});

test('evaluateCpuAdmission computes projectedBusy = externalBusy + reservedSum + candidateEstimate and denies over budget', () => {
  const cpuSample = { hostBusyCores: 2, cores: 8, stale: false };
  const heldLeases = [{ id: 'a', weight: 1 }, { id: 'b', weight: 2 }];
  const result = evaluateCpuAdmission({
    cpuSample,
    heldLeases,
    candidateWeight: 2,
    cpuGateState: { closed: false },
    cooldownBlocked: false,
    cfg: { cpuAdmissionPercent: 75 },
  });
  // externalBusy = max(0, 2-0) = 2; reservedSum = 1+2 = 3; candidateEstimate = 2 -> projected 7
  // budget = 75% * 8 = 6
  assert.equal(result.externalBusy, 2);
  assert.equal(result.projectedBusy, 7);
  assert.equal(result.budget, 6);
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'projected-over-budget');
});

test('evaluateCpuAdmission admits when the same arithmetic fits under budget', () => {
  const cpuSample = { hostBusyCores: 1, cores: 8, stale: false };
  const heldLeases = [{ id: 'a', weight: 1 }];
  const result = evaluateCpuAdmission({
    cpuSample,
    heldLeases,
    candidateWeight: 1,
    cpuGateState: { closed: false },
    cooldownBlocked: false,
    cfg: { cpuAdmissionPercent: 75 },
  });
  // externalBusy=1, reservedSum=1, candidateEstimate=1 -> projected=3; budget=6
  assert.equal(result.projectedBusy, 3);
  assert.equal(result.admit, true);
  assert.equal(result.reason, 'ok');
});

test('evaluateCpuAdmission floors externalBusy at zero when broker-observed CPU exceeds the raw host sample', () => {
  const cpuSample = { hostBusyCores: 1.0, cores: 4, stale: false };
  // A held lease reporting more observed CPU than the whole host sample shows
  // (measurement noise/timing skew) must never drive externalBusy negative.
  const heldLeases = [{ id: 'a', weight: 2, observedCpuCores: 3 }];
  const result = evaluateCpuAdmission({
    cpuSample,
    heldLeases,
    candidateWeight: 1,
    cpuGateState: { closed: false },
    cooldownBlocked: false,
    cfg: { cpuAdmissionPercent: 75 },
  });
  assert.equal(result.externalBusy, 0);
  // reservedSum = leaseDemand(max(3, cold 2)) = 3; candidateEstimate = 1 -> projected = 0+3+1 = 4
  assert.equal(result.projectedBusy, 4);
});

test('evaluateCpuAdmission denies when the CPU gate is closed, regardless of the budget arithmetic', () => {
  const result = evaluateCpuAdmission({
    cpuSample: { hostBusyCores: 0, cores: 8, stale: false },
    heldLeases: [],
    candidateWeight: 1,
    cpuGateState: { closed: true },
    cooldownBlocked: false,
    cfg: { cpuAdmissionPercent: 75 },
  });
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'cpu-gate-closed');
});

test('evaluateCpuAdmission denies when the cooldown is blocking, regardless of the budget arithmetic', () => {
  const result = evaluateCpuAdmission({
    cpuSample: { hostBusyCores: 0, cores: 8, stale: false },
    heldLeases: [],
    candidateWeight: 1,
    cpuGateState: { closed: false },
    cooldownBlocked: true,
    cfg: { cpuAdmissionPercent: 75 },
  });
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'cooldown');
});

test('a missing/stale CPU sample allows exactly one admission through when nothing is held', () => {
  const result = evaluateCpuAdmission({
    cpuSample: null,
    heldLeases: [],
    candidateWeight: 1,
    cpuGateState: { closed: false },
    cooldownBlocked: false,
    cfg: {},
  });
  assert.equal(result.admit, true);
  assert.equal(result.reason, 'sample-unavailable-empty');
});

test('a missing/stale CPU sample denies until a valid sample arrives once anything is held', () => {
  const result = evaluateCpuAdmission({
    cpuSample: { stale: true },
    heldLeases: [{ id: 'a', weight: 1 }],
    candidateWeight: 1,
    cpuGateState: { closed: false },
    cooldownBlocked: false,
    cfg: {},
  });
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'sample-unavailable-held');
});

test('cpu gate closes immediately at or above cpuClosePercent', () => {
  const s = updateCpuGateState(null, 95, CPU_CFG);
  assert.equal(s.closed, true);
});

test('cpu gate stays open below cpuClosePercent when never closed', () => {
  const s = updateCpuGateState(null, 80, CPU_CFG);
  assert.equal(s.closed, false);
});

test('cpu gate requires cpuOpenSamples consecutive samples under cpuOpenPercent to reopen', () => {
  let s = updateCpuGateState(null, 95, CPU_CFG); // closes
  assert.equal(s.closed, true);
  s = updateCpuGateState(s, 50, CPU_CFG); // 1 of 3
  assert.equal(s.closed, true);
  s = updateCpuGateState(s, 50, CPU_CFG); // 2 of 3
  assert.equal(s.closed, true);
  s = updateCpuGateState(s, 50, CPU_CFG); // 3 of 3 -> reopen
  assert.equal(s.closed, false);
});

test('cpu gate: a sample between cpuOpenPercent and cpuClosePercent resets the consecutive-under counter', () => {
  let s = updateCpuGateState(null, 95, CPU_CFG); // closed
  s = updateCpuGateState(s, 50, CPU_CFG); // 1 of 3 under
  s = updateCpuGateState(s, 80, CPU_CFG); // between open/close -> reset, stays closed
  assert.equal(s.closed, true);
  assert.equal(s.consecutiveUnder, 0);
});

test('cpu gate: a spike back to cpuClosePercent while reopening re-closes and resets the counter', () => {
  let s = updateCpuGateState(null, 95, CPU_CFG);
  s = updateCpuGateState(s, 50, CPU_CFG);
  s = updateCpuGateState(s, 50, CPU_CFG);
  s = updateCpuGateState(s, 95, CPU_CFG); // spike re-closes
  assert.equal(s.closed, true);
  assert.equal(s.consecutiveUnder, 0);
});

// --- Fingerprint stamping (the peer session's src/load.js fix, applied here
// per Codex's "your CPU gate is a structural copy, it inherits both bugs by
// construction" finding). Mirrors tests/load-gate.test.js's two fingerprint
// tests exactly, translated to CPU-gate units. ---

test('cpu gate: a threshold change mid-countdown restarts the count under the new thresholds rather than blending them', () => {
  let s = updateCpuGateState(null, 95, CPU_CFG); // closes
  s = updateCpuGateState(s, 50, CPU_CFG); // 1 of 3 under the original cpuOpenPercent (70)
  s = updateCpuGateState(s, 50, CPU_CFG); // 2 of 3
  assert.equal(s.consecutiveUnder, 2);
  // Another supervisor reloads a raised cpuOpenPercent mid-countdown. A
  // stale supervisor still using the old fingerprint must not get to treat
  // this sample as "3 of 3" and reopen the gate under a config nobody
  // installed.
  const RAISED = { cpuClosePercent: 98, cpuOpenPercent: 90, cpuOpenSamples: 3 };
  s = updateCpuGateState(s, 50, RAISED);
  assert.equal(s.consecutiveUnder, 1, 'the countdown must restart at 1, not continue to 3');
  assert.equal(s.closed, true, 'a threshold change alone must never reopen the gate');
});

test('cpu gate: a threshold change does not reopen a gate on its own even when the counter had already reached the sample count', () => {
  let s = updateCpuGateState(null, 95, CPU_CFG); // closes
  s = updateCpuGateState(s, 50, CPU_CFG);
  s = updateCpuGateState(s, 50, CPU_CFG);
  s = updateCpuGateState(s, 50, CPU_CFG); // 3 of 3 -> reopens under CPU_CFG
  assert.equal(s.closed, false);
  // Re-close it, then verify a bare threshold change (no new sample beyond
  // the reset) cannot itself flip `closed`.
  s = updateCpuGateState(s, 99, CPU_CFG); // closes again
  const RAISED = { cpuClosePercent: 98, cpuOpenPercent: 90, cpuOpenSamples: 3 };
  s = updateCpuGateState(s, 80, RAISED); // between old and new open thresholds
  assert.equal(s.closed, true, 'closed must reflect the busy% sample and countdown, never the fingerprint change alone');
  assert.equal(s.consecutiveUnder, 1);
});

// cooldownActive is now derived directly from the held leases' own
// `admittedAt` field (stamped by scheduler.js on the same write that admits
// a lease) rather than a separate admission-state.json file and its own
// write inside the lock — see the doc comment on cooldownActive.

test('admission cooldown blocks right after an admission and clears once admissionCooldownMs has elapsed', () => {
  const cfg = { admissionCooldownMs: 1000 };
  const held = [{ id: 'lease-1', admittedAt: 1_000_000 }];
  assert.equal(cooldownActive(held, cfg, 1_000_500), true);
  assert.equal(cooldownActive(held, cfg, 1_002_000), false);
});

test('admission cooldown clears early once the previously admitted lease is no longer held, even mid-window', () => {
  const cfg = { admissionCooldownMs: 60_000 };
  // Still well inside the cooldown window by elapsed time, but lease-1 has
  // already finished, so it's simply absent from heldLeases -> clears early.
  assert.equal(cooldownActive([], cfg, 1_000_500), false);
});

test('with no prior admission recorded, the cooldown never blocks', () => {
  assert.equal(cooldownActive([], { admissionCooldownMs: 60_000 }), false);
});

test('an old held lease with no admittedAt field never counts toward the cooldown', () => {
  const held = [{ id: 'legacy', weight: 1 }]; // predates this field entirely
  assert.equal(cooldownActive(held, { admissionCooldownMs: 60_000 }, 1_000_000), false);
});

function baseTicket(id, overrides = {}) {
  return {
    id,
    key: 'r:default',
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

test('shadow mode: schedulerMode=shadow never changes what starts, even when the new predicate would deny, and the decision is readable from disk afterward', async () => {
  // Codex review finding #2: production supervisors are spawned with
  // stdio: 'ignore' (run.js), so stderr-only logging is silently discarded
  // on every real run. Verify against the on-disk log file rather than a
  // stubbed stream, since that stub is exactly what made the tests pass
  // while the real behavior was worthless.
  const { state } = freshEnv();
  const globalCfg = {
    ...DEFAULT_GLOBAL_CONFIG,
    capacity: 4,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    schedulerMode: 'shadow',
    cpuAdmissionPercent: 1, // tiny budget -> the new rule will want to deny
  };
  const ticket = baseTicket('shadow-cand');
  await enqueue(state, ticket);
  const cpuSampler = () => ({ hostBusyCores: 1, cores: 8, stale: false, sampledAt: Date.now() });

  const result = await tryStart(state, ticket, globalCfg, undefined, cpuSampler);
  assert.equal(result.started, true, 'shadow mode must let the current rule\'s decision stand');

  const logged = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(logged, /lane-broker-admission/);
  assert.match(logged, /candidate=shadow-cand/);
  assert.match(logged, /mode=shadow/);
  assert.match(logged, /current=start:ok/);
  assert.match(logged, /new=deny:projected-over-budget/);
  assert.match(logged, new RegExp(`bias=${KNOWN_BIAS_NOTE}`), 'the known double-count bias must be loudly noted on every line');
});

test('active mode: the new predicate can additionally deny a start the current rule would otherwise allow', async () => {
  const { state } = freshEnv();
  const globalCfg = {
    ...DEFAULT_GLOBAL_CONFIG,
    capacity: 4,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    schedulerMode: 'active',
    cpuAdmissionPercent: 1,
  };
  const ticket = baseTicket('active-cand');
  await enqueue(state, ticket);
  const cpuSampler = () => ({ hostBusyCores: 1, cores: 8, stale: false, sampledAt: Date.now() });
  const result = await tryStart(state, ticket, globalCfg, undefined, cpuSampler);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'cpu-admission');
  assert.equal(result.cpuReason, 'projected-over-budget');
});

test('active mode still admits when the new predicate agrees there is room', async () => {
  const { state } = freshEnv();
  const globalCfg = {
    ...DEFAULT_GLOBAL_CONFIG,
    capacity: 4,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    schedulerMode: 'active',
    cpuAdmissionPercent: 75,
  };
  const ticket = baseTicket('active-ok');
  await enqueue(state, ticket);
  const cpuSampler = () => ({ hostBusyCores: 0.5, cores: 8, stale: false, sampledAt: Date.now() });
  const result = await tryStart(state, ticket, globalCfg, undefined, cpuSampler);
  assert.equal(result.started, true);
});

// --- Codex review findings 1 and 5: the whole phase-1 CPU path must never
// throw, and corrupt persisted state must degrade to a safe default.
//
// Sampling itself (and its own never-throw guarantee) now lives in
// sampleCpuSafe, called BEFORE the lock (see scheduler.js); evaluateNewAdmission
// takes an already-sampled cpuSample value directly rather than a sampler
// function, since it only runs the part that must stay atomic with the
// admission decision (the CPU gate's counter). ---

test('sampleCpuSafe never throws when the CPU sampler itself throws, and returns null (treated identically to a missing sample)', () => {
  const { state } = freshEnv();
  const throwingSampler = () => {
    throw new Error('boom: simulated sampler failure');
  };
  let result;
  assert.doesNotThrow(() => {
    result = sampleCpuSafe(state, throwingSampler);
  });
  assert.equal(result, null);
});

test('sampleCpuSafe never throws when the sampler returns an unexpected shape (e.g. undefined)', () => {
  const { state } = freshEnv();
  const weirdSampler = () => undefined;
  let result;
  assert.doesNotThrow(() => {
    result = sampleCpuSafe(state, weirdSampler);
  });
  assert.equal(result, null);
});

test('evaluateNewAdmission degrades to the missing-sample fail-safe when cpuSample is null and nothing is held', () => {
  const { state } = freshEnv();
  const result = evaluateNewAdmission(
    state,
    { cpuAdmissionPercent: 75, cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 3, admissionCooldownMs: 5000 },
    { weight: 1 },
    [],
    null,
  );
  assert.equal(result.admit, true, 'nothing held -> fail open, same as a missing sample');
  assert.equal(result.reason, 'sample-unavailable-empty');
  assert.equal(result.hostBusyCores, null);
  assert.equal(result.cores, null);
  assert.equal(result.sampleStale, true);
});

test('evaluateNewAdmission denies (fails closed) with a null cpuSample when something is already held', () => {
  const { state } = freshEnv();
  const result = evaluateNewAdmission(
    state,
    { cpuAdmissionPercent: 75, cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 3, admissionCooldownMs: 5000 },
    { weight: 1 },
    [{ id: 'held-1', weight: 1 }],
    null,
  );
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'sample-unavailable-held');
});

test('evaluateNewAdmission falls back to the generic admission-error shape if something beyond the sample itself misbehaves', () => {
  const { state } = freshEnv();
  // A missing ticket (null) still must not throw — this proves the outer
  // belt-and-suspenders catch is wired up and produces the documented shape.
  const cpuSample = { hostBusyCores: 1, cores: 8, stale: false };
  let result;
  assert.doesNotThrow(() => {
    result = evaluateNewAdmission(state, { cpuAdmissionPercent: 75, cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 3, admissionCooldownMs: 5000 }, null, [], cpuSample);
  });
  assert.equal(result.reason, 'admission-error');
  assert.equal(result.admit, true, 'nothing held -> fail open');
});

test('sampleAndUpdateCpuGate sanitizes a corrupt persisted gate file instead of NaN-ing the hysteresis shut forever', () => {
  const { state } = freshEnv();
  // Simulate corruption: valid JSON, malformed fields.
  atomicWriteJson(paths(state).cpuGate, { closed: 'not-a-boolean', consecutiveUnder: 'not-a-number' });
  const cfg = { cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 3 };
  // A low, healthy sample: with sanitized state (closed:false, consecutiveUnder:0)
  // this must simply stay open, not get stuck evaluating NaN comparisons.
  const gate = sampleAndUpdateCpuGate(state, cfg, { hostBusyCores: 1, cores: 8, stale: false });
  assert.equal(gate.closed, false);
  assert.equal(gate.consecutiveUnder, 0);
});

test('sampleAndUpdateCpuGate treats a non-finite or zero core count as no usable sample', () => {
  const { state } = freshEnv();
  const cfg = { cpuClosePercent: 90, cpuOpenPercent: 70, cpuOpenSamples: 3 };
  const gate = sampleAndUpdateCpuGate(state, cfg, { hostBusyCores: 1, cores: 0, stale: false });
  assert.equal(gate.closed, false); // falls back to the default rather than dividing by zero
});

test('formatAdmissionLog always carries the known double-count bias note', () => {
  const line = formatAdmissionLog({
    candidateId: 'x',
    mode: 'shadow',
    currentDecision: 'start',
    currentReason: 'ok',
    admit: true,
    reason: 'ok',
    sampleStale: false,
    hostBusyCores: 1,
    cores: 8,
    externalBusy: 1,
    projectedBusy: 2,
    budget: 6,
    cpuGateClosed: false,
    cooldownBlocked: false,
  });
  assert.match(line, new RegExp(`bias=${KNOWN_BIAS_NOTE}`));
});

// --- Lock-hold-time fix: the decision log and the CPU sample must not
// lengthen the critical section, and the cooldown must be derivable from
// the lease itself rather than a separate file written inside the lock. ---

test('a successful admission stamps admittedAt on the written lease, so the cooldown needs no separate file', async () => {
  const { state } = freshEnv();
  const globalCfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 };
  const ticket = baseTicket('admits-cleanly');
  await enqueue(state, ticket);
  const result = await tryStart(state, ticket, globalCfg);
  assert.equal(result.started, true);
  assert.ok(Number.isFinite(result.lease.admittedAt), 'the lease must carry an admittedAt timestamp');
});

test('the admission decision log is written on a deny path too (capacity), not only on a successful admission', async () => {
  const { state } = freshEnv();
  const globalCfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 };
  writeLease(state, {
    id: 'holder',
    key: 'other:key',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    weight: 1,
    state: LEASE_STATE.RUNNING,
  });
  const ticket = baseTicket('denied-by-capacity', { key: 'ticket:key', weight: 1 });
  await enqueue(state, ticket);
  const result = await tryStart(state, ticket, globalCfg);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'capacity');
  const logged = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(logged, /candidate=denied-by-capacity/);
  assert.match(logged, /current=deny:capacity/);
});
