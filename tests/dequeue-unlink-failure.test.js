import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig } from './helpers.js';
import { enqueue, tryStart, listQueue, dequeueSync } from '../src/scheduler.js';
import { loadGlobalConfig, resolveTicketConfig } from '../src/config.js';
import { paths } from '../src/state.js';

/**
 * Codex review, correctness bug #2: `dequeueSync` used to swallow every
 * unlink failure identically to "already gone" (ENOENT), so its caller in
 * tryStart's dead-supervisor sweep appended a `dequeuedDeadSupervisor`
 * history record unconditionally -- even when the queue file was NOT
 * actually removed. That both lies (history says dequeued; the ticket is
 * still queued) and repeats: the same false event gets appended again on
 * every subsequent poll, since the ticket never leaves the head.
 *
 * These tests simulate a genuine unlink failure (not ENOENT) by
 * monkey-patching fs.unlinkSync for the duration of one test only.
 */

function readHistory(state) {
  const file = paths(state).history;
  if (!fs.existsSync(file)) return [];
  return fs
    .readFileSync(file, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l));
}

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

test('dequeueSync returns false, and leaves the file on disk, when unlink fails for a reason other than "already gone"', () => {
  const { state } = freshEnv();
  const origUnlinkSync = fs.unlinkSync;
  try {
    fs.unlinkSync = () => {
      const err = new Error('EPERM: operation not permitted');
      err.code = 'EPERM';
      throw err;
    };
    const dir = paths(state).queue;
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, '000000000001-some-id.json');
    fs.writeFileSync(file, JSON.stringify({ id: 'some-id' }));

    const removed = dequeueSync(state, 'some-id');
    assert.equal(removed, false, 'a genuine unlink failure must be reported as not-removed');
    assert.ok(fs.existsSync(file), 'the queue file must still be on disk after a failed unlink');
  } finally {
    fs.unlinkSync = origUnlinkSync;
  }
});

test('dequeueSync returns true for a file that is already gone (ENOENT) or never existed', () => {
  const { state } = freshEnv();
  assert.equal(dequeueSync(state, 'never-existed'), true);
});

test(
  'a dead-supervisor queue record whose removal fails is NOT recorded in history, and stays queued, instead of ' +
    'falsely claiming a dequeue that never happened',
  async () => {
    const { base, home, state } = freshEnv();
    writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
    const prevHome = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    const repoDir = path.join(base, 'repo');
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

    const origUnlinkSync = fs.unlinkSync;
    try {
      const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
      const globalCfg = loadGlobalConfig();
      const dead = await deadPid();

      const deadHeadTicket = {
        id: 'dead-head-2',
        key: resolved.key,
        conflicts: resolved.conflicts,
        weight: resolved.weight,
        supervisorPid: dead,
        supervisorStart: null,
        cwd: repoDir,
        cmd: ['true'],
        logPath: path.join(base, 'dead-head-2.log'),
        resultPath: path.join(base, 'dead-head-2.json'),
        createdAt: Date.now(),
      };
      const liveTicket = {
        id: 'live-second-2',
        key: resolved.key,
        conflicts: resolved.conflicts,
        weight: resolved.weight,
        supervisorPid: process.pid,
        supervisorStart: null,
        cwd: repoDir,
        cmd: ['true'],
        logPath: path.join(base, 'live-second-2.log'),
        resultPath: path.join(base, 'live-second-2.json'),
        createdAt: Date.now() + 1,
      };

      await enqueue(state, deadHeadTicket);
      await enqueue(state, liveTicket);

      // Unlink fails for the dead ticket's own queue file only (matched by
      // id in the filename), so the enqueue writes above -- which don't go
      // through unlinkSync -- are unaffected.
      fs.unlinkSync = (file) => {
        if (String(file).includes('dead-head-2')) {
          const err = new Error('EPERM: simulated unlink failure');
          err.code = 'EPERM';
          throw err;
        }
        return origUnlinkSync(file);
      };

      const result = await tryStart(state, liveTicket, globalCfg);
      assert.equal(result.started, false, 'the live ticket must not start: the dead-supervisor record could not be removed, so it still occupies the head');

      assert.ok(
        listQueue(state).some((t) => t && t.id === 'dead-head-2'),
        'the dead ticket must remain queued when its record could not be removed',
      );

      const record = readHistory(state).find((r) => r.id === 'dead-head-2');
      assert.equal(record, undefined, 'no dequeuedDeadSupervisor record may be written when the dequeue itself failed');
    } finally {
      fs.unlinkSync = origUnlinkSync;
      process.env.LANE_BROKER_HOME = prevHome;
    }
  },
);
