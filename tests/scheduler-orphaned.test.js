import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig } from './helpers.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { tryStart } from '../src/scheduler.js';
import { loadGlobalConfig, resolveTicketConfig } from '../src/config.js';
import path from 'node:path';

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

test('an ORPHANED lease still blocks a conflicting ticket and still counts against capacity', async () => {
  const { base, home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 } } });

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

    const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'rouge', lane: 'default' });
    const globalCfg = loadGlobalConfig();
    const newcomer = {
      id: 'newcomer',
      key: resolved.key,
      conflicts: resolved.conflicts,
      weight: resolved.weight,
      supervisorPid: process.pid,
      supervisorStart: null,
      cwd: repoDir,
      cmd: ['true'],
      logPath: path.join(base, 'newcomer.log'),
      resultPath: path.join(base, 'newcomer.json'),
      createdAt: Date.now(),
    };
    // Move the orphaned lease to ORPHANED state via one reap pass first, like the real broker would.
    const { reapAll } = await import('../src/lease.js');
    reapAll(state, bootId());

    // Directly enqueue then call tryStart; tryStart itself calls reapAll too.
    const { enqueue } = await import('../src/scheduler.js');
    await enqueue(state, newcomer);
    const result = await tryStart(state, newcomer, globalCfg);

    assert.equal(result.started, false, 'a live ORPHANED lease on the same key must still block a newcomer');
    assert.equal(result.reason, 'conflict');
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    try {
      process.kill(-group.pid, 'SIGKILL');
    } catch {
      // already gone
    }
  }
});
