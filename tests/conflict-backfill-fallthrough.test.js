import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, readSkipState, readResourceSkipState } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import fs from 'node:fs';
import path from 'node:path';
import { bootId, paths } from '../src/state.js';

/**
 * BRAIN-365: a conflict-blocked head (skip allowance NOT yet used) whose first non-conflicting
 * backfill candidate is denied projected-over-budget used to pin the whole queue. Fixture machine:
 * 10 cores, reserve 1 -> budget 9. Ambient load 2, a running rouge:sim lease holds 4 cores, and the
 * head (rouge:default, 1 core) is blocked by it.
 */

const GIB = 1024 ** 3;

const cfg = {
  ...DEFAULT_GLOBAL_CONFIG,
  schedulerMode: 'active',
  capacity: 10,
  loadClose: 1000,
  loadOpen: 900,
  loadOpenSamples: 1,
  cpuAdmissionPercent: 100,
  cpuReserveCores: 1,
  admissionCooldownMs: 0,
};

const sampler = () => ({ hostBusyCores: 2, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t) => tryStart(state, t, cfg, undefined, sampler, undefined, memory);

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

async function setup(...behind) {
  const { state } = freshEnv();
  writeLease(state, { id: 'sim', key: 'rouge:sim', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 4, state: LEASE_STATE.RUNNING });
  await enqueue(state, ticket('head', { key: 'rouge:default', conflicts: ['rouge:sim'] }));
  for (const t of behind) await enqueue(state, t);
  return state;
}

test('a 4-core over-budget candidate does not pin 1-core tickets behind it', async () => {
  const big = ticket('big', { key: 'jun:prepush', weight: 4 });
  const small = ticket('small', { key: 'mobile:test', weight: 1 });
  const small2 = ticket('small2', { key: 'jun:117', weight: 1 });
  const state = await setup(big, small, small2);

  const denied = await poll(state, big);
  assert.equal(denied.started, false);
  assert.equal(denied.cpuReason, 'projected-over-budget');
  assert.equal(readResourceSkipState(state).behindConflict, true, 'the denial is recorded for the walk');

  assert.equal((await poll(state, small2)).reason, 'not-head', 'a tie on claim goes to the earlier ticket');
  const result = await poll(state, small);
  assert.equal(result.started, true, 'the smallest fitting ticket is admitted');
  assert.equal(readSkipState(state).count, 1, 'it still counts toward the head conflict skip allowance');
});

test('a fitting ticket that would consume the head reservation is not admitted', async () => {
  // alone: 2 + 4 + 3 = 9 <= 9, but with the head's 1 core reserved: 10 > 9
  const big = ticket('big', { key: 'jun:prepush', weight: 4 });
  const mid = ticket('mid', { key: 'mobile:test', weight: 3 });
  const state = await setup(big, mid);

  await poll(state, big);
  const result = await poll(state, mid);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'not-head');
  assert.equal(readSkipState(state).count, 0);
});

test('a ticket that conflicts with the head is skipped by the walk', async () => {
  const big = ticket('big', { key: 'jun:prepush', weight: 4 });
  const clash = ticket('clash', { key: 'rouge:other', weight: 1, conflicts: ['rouge:default'] });
  const small = ticket('small', { key: 'mobile:test', weight: 1 });
  const state = await setup(big, clash, small);

  await poll(state, big);
  assert.equal((await poll(state, clash)).reason, 'not-head');
  assert.equal((await poll(state, small)).started, true);
});

test('an unreadable ticket stops the walk', async () => {
  const big = ticket('big', { key: 'jun:prepush', weight: 4 });
  const small = ticket('small', { key: 'mobile:test', weight: 1 });
  const corrupt = ticket('corrupt', { key: 'x:y' });
  const state = await setup(big, corrupt, small);
  const dir = paths(state).queue;
  fs.writeFileSync(path.join(dir, fs.readdirSync(dir).find((n) => n.endsWith('-corrupt.json'))), '{not json');

  await poll(state, big);
  assert.equal((await poll(state, small)).started, false, 'a ticket behind an unreadable record is never selected');
});

test('an elastic ticket is picked when its floor fits', async () => {
  const big = ticket('big', { key: 'jun:prepush', weight: 4 });
  const elastic = ticket('elastic', { key: 'mobile:test', weight: 4, resources: { cpuCores: 4, minCpuCores: 1 } });
  const state = await setup(big, elastic);

  await poll(state, big);
  const result = await poll(state, elastic);
  assert.equal(result.started, true);
  assert.ok(result.lease.grantedCpuCores < 4, 'granted below its declared claim');
});
