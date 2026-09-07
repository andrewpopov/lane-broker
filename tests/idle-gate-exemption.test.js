import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { freshEnv } from './helpers.js';
import { paths, atomicWriteJson, readJsonSafe, fingerprintOf, bootId } from '../src/state.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

// BRAIN-197: a load gate closed by CPU load the broker never generated must
// not starve a broker that is holding nothing. Every held-lease fixture in
// this file uses a DIFFERENT key from the candidate and leaves capacity to
// spare, so a "held lease suppresses the exemption" assertion can only pass
// because of the gate, never because of an unrelated conflict/capacity
// refusal — and a fresh heartbeat + a live supervisorPid, so it survives the
// reapAll pass tryStart runs on every poll.

function makeCfg(overrides) {
  return { ...DEFAULT_GLOBAL_CONFIG, ...overrides };
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

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

/** A pgid that WAS a real process group leader and is now confirmed gone —
 *  for a lease meant to be fully reaped (removed), not just marked
 *  ORPHANED. `childPgid: null` does NOT do this: `isGroupAlive` calls
 *  `process.kill(-pgid, 0)`, and `-null` coerces to `-0`, which POSIX (and
 *  Node) treats as "signal my OWN process group" — a no-op success, not a
 *  thrown ESRCH — so a null pgid reads as alive forever and the lease would
 *  wrongly persist as ORPHANED instead of being removed. */
async function deadGroupPgid() {
  const child = spawn('node', ['-e', 'process.exit(0)'], { detached: true });
  const pgid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pgid;
}

test('idle broker: a closed load gate does not block a broker holding nothing (BRAIN-197)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfg, 0);

  const loadSampler = () => 20; // stays above loadClose: the gate is genuinely still closed, not reopening on its own

  const result = await tryStart(state, ticket, cfg, loadSampler);
  assert.equal(result.started, true, 'nothing is held, so the closed gate must not block this admission');

  const gateState = readJsonSafe(paths(state).loadGate);
  assert.equal(gateState.closed, true, 'the gate itself must still read closed: the exemption bypasses it, it does not reopen it');

  const logged = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(logged, /current=start:idle-exempt/, 'telemetry must distinguish an idle-exempt start from an ordinary "ok" admission');
});

test('one held lease (different key, spare capacity) suppresses the idle exemption', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  holdLease(state, { key: 'repo/holder', weight: 1 });
  const ticket = await enqueueTicket(state, { key: 'repo/candidate', weight: 1 });
  persistClosedGate(state, cfg, 0);

  const loadSampler = () => 20;
  const result = await tryStart(state, ticket, cfg, loadSampler);

  assert.equal(result.started, false, 'the broker is not idle: the gate must apply exactly as before this change');
  assert.equal(result.reason, 'load-gate-closed');
});

test('an ORPHANED holder also suppresses the exemption (still real, possibly-running work)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });

  const dead = await deadPid();
  const group = spawn('sleep', ['5'], { detached: true, stdio: 'ignore' });
  group.unref();
  try {
    // supervisorPid dead but the child process group is alive: reapIfStale
    // marks this ORPHANED rather than removing it (same fixture shape as
    // scheduler-orphaned.test.js).
    writeLease(state, {
      id: 'orphaned-holder',
      key: 'repo/holder',
      bootId: bootId(),
      supervisorPid: dead,
      supervisorStart: null,
      childPgid: group.pid,
      heartbeatAt: Date.now(),
      weight: 1,
      state: LEASE_STATE.RUNNING,
    });

    const ticket = await enqueueTicket(state, { key: 'repo/candidate', weight: 1 });
    persistClosedGate(state, cfg, 0);
    const loadSampler = () => 20;

    const result = await tryStart(state, ticket, cfg, loadSampler);
    assert.equal(result.started, false, 'an ORPHANED lease means work may still be running: not idle');
    assert.equal(result.reason, 'load-gate-closed');
  } finally {
    try {
      process.kill(-group.pid, 'SIGKILL');
    } catch {
      // already gone
    }
  }
});

test('admission happens once the last holder is reaped', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 4, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const holder = holdLease(state, { key: 'repo/holder', weight: 1 });
  const ticket = await enqueueTicket(state, { key: 'repo/candidate', weight: 1 });
  persistClosedGate(state, cfg, 0);
  const loadSampler = () => 20;

  const before = await tryStart(state, ticket, cfg, loadSampler);
  assert.equal(before.started, false, 'still one live holder: the gate must still apply');
  assert.equal(before.reason, 'load-gate-closed');

  // Kill the holder's supervisor with no live child group behind it, so the
  // next tryStart's own reapAll pass removes the lease outright (not just
  // ORPHANED).
  const dead = await deadPid();
  const deadGroup = await deadGroupPgid();
  writeLease(state, { ...holder, supervisorPid: dead, childPgid: deadGroup });

  const after = await tryStart(state, ticket, cfg, loadSampler);
  assert.equal(after.started, true, 'the last holder is gone: the broker is idle again and the exemption applies');
});

test('a closed gate does not exempt a paused broker: pause is checked before the gate either way', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfg, 0);
  fs.writeFileSync(paths(state).pause, 'operator paused');

  const loadSampler = () => 20;
  const result = await tryStart(state, ticket, cfg, loadSampler);

  assert.equal(result.started, false, 'idle or not, a paused broker never starts anything');
  assert.equal(result.reason, 'paused');
});

test('a closed gate does not exempt an over-capacity candidate: capacity is still enforced after the exemption', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 1, loadClose: 10, loadOpen: 5, loadOpenSamples: 1 });
  const ticket = await enqueueTicket(state, { weight: 2 }); // exceeds capacity even with nothing else held
  persistClosedGate(state, cfg, 0);

  const loadSampler = () => 20;
  const result = await tryStart(state, ticket, cfg, loadSampler);

  assert.equal(result.started, false, 'the idle exemption only skips the gate check, never the capacity check');
  assert.equal(result.reason, 'capacity');
});

test('active mode: the CPU admission predicate can still veto an idle, gate-exempt start', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({
    capacity: 4,
    loadClose: 10,
    loadOpen: 5,
    loadOpenSamples: 1,
    schedulerMode: 'active',
    cpuAdmissionPercent: 1, // tiny budget: the CPU predicate will want to deny
  });
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfg, 0);
  const loadSampler = () => 20;
  const cpuSampler = () => ({ hostBusyCores: 1, cores: 8, stale: false, sampledAt: Date.now() });

  const result = await tryStart(state, ticket, cfg, loadSampler, cpuSampler);

  assert.equal(result.started, false, 'the load gate is exempted, but active-mode CPU admission is a separate, still-live veto');
  assert.equal(result.reason, 'cpu-admission');

  const logged = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(
    logged,
    /current=start:idle-exempt/,
    'telemetry must record that the CURRENT rule (gate + capacity) would have started this idle-exempt admission, even though the separate CPU predicate then vetoed it',
  );
});

test('shadow mode: a closed gate does not block an idle broker (the default scheduler mode)', async () => {
  const { state } = freshEnv();
  const cfg = makeCfg({ capacity: 2, loadClose: 10, loadOpen: 5, loadOpenSamples: 1, schedulerMode: 'shadow' });
  const ticket = await enqueueTicket(state);
  persistClosedGate(state, cfg, 0);
  const loadSampler = () => 20;

  const result = await tryStart(state, ticket, cfg, loadSampler);
  assert.equal(result.started, true);
});
