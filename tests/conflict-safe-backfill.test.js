import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, readSkipState } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';

/**
 * BRAIN-355: an exhausted conflict-blocked head (BRAIN-249) still admits a backfill ticket that
 * cannot delay it. Fixture machine: 10 cores, reserve 1, 100% -> CPU budget 9, weight capacity 10.
 * The head (rouge:default, weight 3) is blocked by a running rouge:sim (weight 1) lease.
 */

const GIB = 1024 ** 3;
const LIMIT = 3;

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
    conflictSkipLimit: LIMIT,
    ...overrides,
  };
}

const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t, cfg, ext = 1) => tryStart(state, t, cfg, undefined, sampler(ext), undefined, memory);

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

function heldLease(id, key, weight = 1) {
  return { id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight, state: LEASE_STATE.RUNNING };
}

async function exhaustedHead(state, { skipCount = LIMIT, headWeight = 3 } = {}) {
  writeLease(state, heldLease('holder', 'rouge:sim'));
  const head = ticket('head', { key: 'rouge:default', weight: headWeight, conflicts: ['rouge:sim'] });
  await enqueue(state, head);
  atomicWriteJson(paths(state).conflictSkipState, { headId: 'head', count: skipCount, blockedSince: Date.now(), loggedPhase: skipCount >= LIMIT ? 'exhausted' : 'blocked' });
  return head;
}

const readLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

test('1: an exhausted conflict-blocked head admits a non-conflicting ticket that fits alongside it, without counting a skip', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  await exhaustedHead(state);
  const jun = ticket('jun', { key: 'jun:default', weight: 2 });
  await enqueue(state, jun);

  const result = await poll(state, jun, cfg);
  assert.equal(result.started, true);
  assert.equal(readSkipState(state).count, LIMIT, 'a safe backfill cannot delay the head, so it is not a skip');
  assert.match(readLog(state), /lane-broker-head-block event=safe-backfill headId=head candidate=jun /);
});

test('2: a ticket conflicting with the head in either direction is refused while exhausted', async () => {
  const cfg = baseCfg();

  // head -> candidate: the head declares a conflict with the candidate's key
  const a = freshEnv().state;
  writeLease(a, heldLease('holder', 'rouge:sim'));
  await enqueue(a, ticket('head', { key: 'rouge:default', conflicts: ['rouge:sim', 'rouge:lint'] }));
  atomicWriteJson(paths(a).conflictSkipState, { headId: 'head', count: LIMIT, blockedSince: Date.now(), loggedPhase: 'exhausted' });
  const lint = ticket('lint', { key: 'rouge:lint' });
  await enqueue(a, lint);
  const forward = await poll(a, lint, cfg);
  assert.equal(forward.started, false, 'the head declares a conflict with this key');
  assert.equal(forward.reason, 'not-head');

  // candidate -> head: only the candidate declares the conflict ("*" resolves to every other key)
  const b = freshEnv().state;
  writeLease(b, heldLease('holder', 'rouge:sim'));
  await enqueue(b, ticket('head', { key: 'rouge:default', conflicts: ['rouge:sim'] }));
  atomicWriteJson(paths(b).conflictSkipState, { headId: 'head', count: LIMIT, blockedSince: Date.now(), loggedPhase: 'exhausted' });
  const sim2 = ticket('sim2', { key: 'rouge:sim2', conflicts: ['rouge:default', 'rouge:sim'] });
  await enqueue(b, sim2);
  const reverse = await poll(b, sim2, cfg);
  assert.equal(reverse.started, false, 'the candidate declares a conflict with the head key');
  assert.equal(reverse.reason, 'not-head');
});

test('3a: a ticket that fits CPU alone but not with the head reservation is refused', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  await exhaustedHead(state, { headWeight: 3 });
  // ext 3 + holder 1 + candidate 3 = 7 <= 9 alone; + head 3 = 10 > 9 together
  const jun = ticket('jun', { key: 'jun:default', weight: 3 });
  await enqueue(state, jun);
  const result = await poll(state, jun, cfg, 3);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'cpu-admission');
  assert.equal(result.cpuReason, 'projected-over-budget');
});

test('3b: a ticket that fits weight capacity alone but not with the head is refused', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ capacity: 6 });
  await exhaustedHead(state, { headWeight: 3 });
  // running 1 + candidate 3 = 4 <= 6 alone; + head 3 = 7 > 6 together
  const jun = ticket('jun', { key: 'jun:default', weight: 3 });
  await enqueue(state, jun);
  const result = await poll(state, jun, cfg);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'not-head');
});

test('4: conflictSafeBackfill=false keeps the BRAIN-249 refusal', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg({ conflictSafeBackfill: false });
  await exhaustedHead(state);
  const jun = ticket('jun', { key: 'jun:default', weight: 2 });
  await enqueue(state, jun);
  const result = await poll(state, jun, cfg);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'not-head');
});

test('5: while the skip allowance remains (phase blocked) backfill is unchanged and still counts a skip', async () => {
  const { state } = freshEnv();
  const cfg = baseCfg();
  await exhaustedHead(state, { skipCount: 1 });
  const jun = ticket('jun', { key: 'jun:default', weight: 2 });
  await enqueue(state, jun);
  const result = await poll(state, jun, cfg);
  assert.equal(result.started, true);
  assert.equal(readSkipState(state).count, 2, 'the ordinary BRAIN-249 skip is counted');
  assert.doesNotMatch(readLog(state), /safe-backfill/);
});
