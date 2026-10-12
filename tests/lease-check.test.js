import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, BIN } from './helpers.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, processStartTime } from '../src/state.js';
import { CAPABILITIES } from '../src/capabilities.js';

// BRAIN-553: a pre-push hook trusted any non-empty LANE_BROKER_LEASE as "already inside a lane", so a made-up value
// skipped the queue and ran the whole suite bare. `lane lease-check` is what a hook asks instead.

const leaseCheck = (env, args) => spawnSync(process.execPath, [BIN, 'lease-check', ...args], { env, encoding: 'utf8' });

/** A lease record shaped like the scheduler's, held by `supervisorPid` (this test process unless overridden). */
function plantLease(state, overrides = {}) {
  const id = crypto.randomUUID();
  writeLease(state, {
    id,
    key: 'r:default',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: processStartTime(process.pid),
    childPgid: process.pid,
    weight: 1,
    heartbeatAt: Date.now(),
    state: LEASE_STATE.RUNNING,
    ...overrides,
  });
  return id;
}

async function deadPid() {
  const child = spawn(process.execPath, ['-e', 'process.exit(0)']);
  await new Promise((resolve) => child.on('exit', resolve));
  return child.pid;
}

function assertRefused(res, id, reason) {
  assert.equal(res.status, 1, res.stderr);
  assert.equal(res.stdout, '');
  assert.equal(res.stderr, `lane lease-check: ${id || '""'} is not a live lease on this host (${reason})\n`);
}

test('lease-check: a RUNNING lease whose supervisor is alive exits 0 and prints nothing', () => {
  const { state, env } = freshEnv();
  const res = leaseCheck(env, [plantLease(state)]);
  assert.equal(res.status, 0, res.stderr);
  assert.equal(res.stdout, '');
  assert.equal(res.stderr, '');
});

test('lease-check: an id with no lease record exits 1 (the forged LANE_BROKER_LEASE case)', () => {
  const { env } = freshEnv();
  assertRefused(leaseCheck(env, ['inline-user-skip-queue']), 'inline-user-skip-queue', 'no lease record');
});

test('lease-check: a lease whose supervisor pid is dead exits 1', async () => {
  const { state, env } = freshEnv();
  const id = plantLease(state, { supervisorPid: await deadPid(), supervisorStart: null });
  assertRefused(leaseCheck(env, [id]), id, 'its supervisor has exited');
});

test('lease-check: a live pid with a different recorded start time (pid reuse) exits 1', () => {
  const { state, env } = freshEnv();
  const id = plantLease(state, { supervisorStart: 'Thu Jan  1 00:00:00 1970' });
  assertRefused(leaseCheck(env, [id]), id, 'its supervisor has exited');
});

test('lease-check: an ORPHANED lease exits 1 even with its pid still alive', () => {
  const { state, env } = freshEnv();
  const id = plantLease(state, { state: LEASE_STATE.ORPHANED });
  assertRefused(leaseCheck(env, [id]), id, 'lease is ORPHANED');
});

test('lease-check: a lease recorded before this boot exits 1', () => {
  const { state, env } = freshEnv();
  const id = plantLease(state, { bootId: 'an-earlier-boot' });
  assertRefused(leaseCheck(env, [id]), id, 'recorded before this boot');
});

test('lease-check: an empty id, a path-shaped id and a missing or extra argument all exit 1', () => {
  const { state, env } = freshEnv();
  const live = plantLease(state);
  assertRefused(leaseCheck(env, ['']), '', 'not a ticket id');
  assertRefused(leaseCheck(env, [`../leases/${live}`]), `../leases/${live}`, 'not a ticket id');
  assertRefused(leaseCheck(env, []), '', 'expected exactly one <id>');
  assertRefused(leaseCheck(env, [live, 'extra']), '', 'expected exactly one <id>');
});

test('lease-check: the lease a real `lane run` child runs under, and a nested reentrant run under it, both exit 0', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  // What a hook does: hand the inherited LANE_BROKER_LEASE to `lane lease-check` and act on its exit code.
  const check = [
    process.execPath,
    '-e',
    "const r = require('child_process').spawnSync(process.execPath, [process.argv[1], 'lease-check', process.env.LANE_BROKER_LEASE || ''], { stdio: 'inherit' }); process.exit(r.status ?? 1)",
    BIN,
  ];
  const direct = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...check], { env, cwd: repoDir });
  assert.equal(direct.code, 0, direct.stderr);
  const nested = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, BIN, 'run', '--repo', 'r', '--lane', 'default', '--', ...check], { env, cwd: repoDir });
  assert.equal(nested.code, 0, nested.stderr);
});

test('lane capabilities advertises lease-check/1', () => {
  assert.ok(CAPABILITIES.includes('lease-check/1'));
});
