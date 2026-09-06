import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { writeLease, LEASE_STATE, reapAll } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { collectStatus } from '../src/status.js';

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

test('lane status counts an ORPHANED lease against used capacity, same as the scheduler does', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;

  const dead = await deadPid();
  const group = spawn('sleep', ['5'], { detached: true, stdio: 'ignore' });
  group.unref();
  try {
    writeLease(state, {
      id: 'orphaned-holder',
      key: 'rouge:default',
      bootId: bootId(),
      supervisorPid: dead,
      supervisorStart: null,
      childPgid: group.pid,
      heartbeatAt: Date.now(),
      weight: 2,
      state: LEASE_STATE.RUNNING,
    });

    // Move the lease to ORPHANED, like the real broker would.
    reapAll(state, bootId());

    const status = await collectStatus();
    assert.equal(status.used, 2, 'an ORPHANED lease must count toward used capacity, matching the scheduler\'s HELD_STATES');
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
    try {
      process.kill(-group.pid, 'SIGKILL');
    } catch {
      // already gone
    }
  }
});
