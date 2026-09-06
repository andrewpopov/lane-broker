import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { processStartTime, isSupervisorAlive } from '../src/lease.js';
import { freshEnv, writeGlobalConfig, writeRepoConfig } from './helpers.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { loadGlobalConfig, resolveTicketConfig } from '../src/config.js';

/** Break PATH so `ps` cannot be found: execFileSync then fails at the spawn
 *  level (ENOENT) with `status: null`, not a numeric non-zero exit — the
 *  same shape as EAGAIN under load. This must read as indeterminate, never
 *  as "confirmed gone". */
function withBrokenPs(fn) {
  const prevPath = process.env.PATH;
  process.env.PATH = '/lane-broker-test-empty-dir-that-does-not-exist';
  try {
    return fn();
  } finally {
    process.env.PATH = prevPath;
  }
}

test('a failed probe spawn (status: null, e.g. ENOENT/EAGAIN) is indeterminate, not confirmed-gone', () => {
  withBrokenPs(() => {
    const result = processStartTime(process.pid);
    assert.equal(result, undefined, 'a spawn-level probe failure must return undefined (indeterminate), never null (confirmed gone)');
  });
});

test('isSupervisorAlive fails closed when the liveness probe itself cannot run', () => {
  const lease = { supervisorPid: process.pid, supervisorStart: 'some-captured-start-time' };
  withBrokenPs(() => {
    assert.equal(isSupervisorAlive(lease), true, 'an indeterminate probe must keep the lease, never declare the supervisor dead');
  });
});

test('a live-but-unprobeable head ticket is not dequeued while a second ticket checks the queue', async () => {
  const { base, home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  try {
    const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
    const globalCfg = loadGlobalConfig();

    // The head ticket's supervisor is genuinely alive (our own pid) and has
    // a captured start time, so isSupervisorAlive must consult processStartTime
    // when tryStart's dead-queue scan checks it (it is not the ticket calling
    // tryStart, so it is not skipped as "self").
    const headTicket = {
      id: 'live-head',
      key: resolved.key,
      conflicts: resolved.conflicts,
      weight: resolved.weight,
      supervisorPid: process.pid,
      supervisorStart: 'some-captured-start-time',
      cwd: repoDir,
      cmd: ['true'],
      logPath: path.join(base, 'live-head.log'),
      resultPath: path.join(base, 'live-head.json'),
      createdAt: Date.now(),
    };
    const secondTicket = {
      id: 'second',
      key: resolved.key,
      conflicts: resolved.conflicts,
      weight: resolved.weight,
      supervisorPid: process.pid,
      supervisorStart: null,
      cwd: repoDir,
      cmd: ['true'],
      logPath: path.join(base, 'second.log'),
      resultPath: path.join(base, 'second.json'),
      createdAt: Date.now() + 1,
    };

    await enqueue(state, headTicket);
    await enqueue(state, secondTicket);
    assert.equal(listQueue(state).length, 2);

    await withBrokenPs(async () => {
      const result = await tryStart(state, secondTicket, globalCfg);
      assert.equal(result.started, false, 'the second ticket must not start while the (still-live) head ticket occupies the FIFO head');
      assert.equal(result.reason, 'not-head');
    });

    assert.notEqual(
      listQueue(state).find((t) => t && t.id === 'live-head'),
      undefined,
      'a live supervisor must never be dequeued just because the liveness probe failed to spawn',
    );
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});
