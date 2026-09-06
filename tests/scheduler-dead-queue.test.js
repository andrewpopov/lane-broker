import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig } from './helpers.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { loadGlobalConfig, resolveTicketConfig } from '../src/config.js';

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

test('a queued ticket whose supervisor is dead is dequeued so a live ticket behind it can start', async () => {
  const { base, home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  try {
    const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
    const globalCfg = loadGlobalConfig();
    const dead = await deadPid();

    const deadHeadTicket = {
      id: 'dead-head',
      key: resolved.key,
      conflicts: resolved.conflicts,
      weight: resolved.weight,
      supervisorPid: dead,
      supervisorStart: null,
      cwd: repoDir,
      cmd: ['true'],
      logPath: path.join(base, 'dead-head.log'),
      resultPath: path.join(base, 'dead-head.json'),
      createdAt: Date.now(),
    };
    const liveTicket = {
      id: 'live-second',
      key: resolved.key,
      conflicts: resolved.conflicts,
      weight: resolved.weight,
      supervisorPid: process.pid,
      supervisorStart: null,
      cwd: repoDir,
      cmd: ['true'],
      logPath: path.join(base, 'live-second.log'),
      resultPath: path.join(base, 'live-second.json'),
      createdAt: Date.now() + 1,
    };

    await enqueue(state, deadHeadTicket);
    await enqueue(state, liveTicket);
    assert.equal(listQueue(state).length, 2);

    const result = await tryStart(state, liveTicket, globalCfg);
    assert.equal(result.started, true, 'the live ticket must start once the dead-supervisor head is reaped from the queue');
    assert.equal(listQueue(state).find((t) => t && t.id === 'dead-head'), undefined, 'the dead ticket must be dequeued');
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});
