import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { freshEnv } from './helpers.js';
import { writeLease, readLease, reapAll, reapIfStale, LEASE_STATE, processStartTime } from '../src/lease.js';
import { bootId } from '../src/state.js';

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

test('a lease with a dead supervisor and a dead process group is reaped (removed)', async () => {
  const { state } = freshEnv();
  const pid = await deadPid();
  writeLease(state, {
    id: 'dead-both',
    key: 'r:default',
    bootId: bootId(),
    supervisorPid: pid,
    supervisorStart: null,
    childPgid: pid,
    heartbeatAt: Date.now(),
    weight: 2,
    state: LEASE_STATE.RUNNING,
  });
  const results = reapAll(state, bootId());
  assert.equal(results.find((r) => r.id === 'dead-both').action, 'reaped');
  assert.equal(readLease(state, 'dead-both'), null);
});

test('a lease with a dead supervisor but a live process group is ORPHANED, never auto-reaped', async () => {
  const { state } = freshEnv();
  const dead = await deadPid();
  const group = spawn('sleep', ['5'], { detached: true, stdio: 'ignore' });
  group.unref();
  try {
    writeLease(state, {
      id: 'orphan-me',
      key: 'r:default',
      bootId: bootId(),
      supervisorPid: dead,
      supervisorStart: null,
      childPgid: group.pid,
      heartbeatAt: Date.now(),
      weight: 2,
      state: LEASE_STATE.RUNNING,
    });
    let results = reapAll(state, bootId());
    assert.equal(results.find((r) => r.id === 'orphan-me').action, 'orphaned');
    assert.equal(readLease(state, 'orphan-me').state, LEASE_STATE.ORPHANED);

    // A second pass must not reap an already-ORPHANED lease.
    results = reapAll(state, bootId());
    assert.equal(results.find((r) => r.id === 'orphan-me').action, 'orphaned');
    assert.notEqual(readLease(state, 'orphan-me'), null);
  } finally {
    try {
      process.kill(-group.pid, 'SIGKILL');
    } catch {
      // already gone
    }
  }
});

test('PID reuse: a lease whose stored start time no longer matches the live pid is treated as dead', async () => {
  const { state } = freshEnv();
  const dead = await deadPid();
  // Use our own live pid but with a bogus recorded start time -> must not be
  // mistaken for a live supervisor, even though process.kill(pid, 0) succeeds.
  const bogusLease = {
    id: 'pid-reuse',
    key: 'r:default',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: 'Thu Jan  1 00:00:00 1970',
    childPgid: dead, // group also dead -> should fully reap
    heartbeatAt: Date.now(),
    weight: 2,
    state: LEASE_STATE.RUNNING,
  };
  writeLease(state, bogusLease);
  const action = reapIfStale(state, bogusLease, bootId());
  assert.equal(action, 'reaped', 'a mismatched start time must be treated as a dead supervisor, not a live one');
});

test('a lease with the correct recorded start time for a live supervisor is kept', () => {
  const { state } = freshEnv();
  const realStart = processStartTime(process.pid);
  const lease = {
    id: 'still-alive',
    key: 'r:default',
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: realStart,
    childPgid: process.pid,
    heartbeatAt: Date.now(),
    weight: 2,
    state: LEASE_STATE.RUNNING,
  };
  writeLease(state, lease);
  const action = reapIfStale(state, lease, bootId());
  assert.equal(action, 'kept');
});

test('a boot id change reaps a lease even if the supervisor pid still looks alive', () => {
  const { state } = freshEnv();
  const realStart = processStartTime(process.pid);
  const lease = {
    id: 'old-boot',
    key: 'r:default',
    bootId: 'a-previous-boot-id',
    supervisorPid: process.pid,
    supervisorStart: realStart,
    childPgid: process.pid,
    heartbeatAt: Date.now(),
    weight: 2,
    state: LEASE_STATE.RUNNING,
  };
  writeLease(state, lease);
  const action = reapIfStale(state, lease, bootId());
  assert.equal(action, 'reaped');
});
