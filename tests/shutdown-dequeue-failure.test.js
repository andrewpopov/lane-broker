import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { enqueue } from '../src/scheduler.js';
import { remoteShutdownCommand } from '../src/remote-runner.js';

/** BRAIN-1035 S2a review: `shutdown-unstarted` ("the sweep dequeued it") is written only when the dequeue really happened. */

test('a queued ticket whose queue file cannot be unlinked is not marked shutdown-unstarted', async () => {
  const { base, home, state } = freshEnv();
  writeGlobalConfig(home, {});
  const remoteRoot = makeTmpDir('shutdown-dequeue-');
  const execId = crypto.randomUUID();
  const laneId = crypto.randomUUID();
  const ticketDir = path.join(remoteRoot, 'tickets', execId);
  fs.mkdirSync(ticketDir, { recursive: true });
  fs.writeFileSync(path.join(ticketDir, 'remote-id'), laneId);
  await enqueue(state, { id: laneId, key: 'k', conflicts: [], weight: 1, supervisorPid: process.pid, supervisorStart: null, cwd: base, cmd: ['true'], logPath: path.join(base, 'l.log'), resultPath: path.join(base, 'r.json'), createdAt: Date.now() });

  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  const origUnlink = fs.unlinkSync;
  try {
    fs.unlinkSync = (file, ...rest) => {
      if (String(file).includes(`${path.sep}queue${path.sep}`)) throw Object.assign(new Error('EPERM'), { code: 'EPERM' });
      return origUnlink(file, ...rest);
    };
    await remoteShutdownCommand({ root: remoteRoot, waitMs: 200 });
  } finally {
    fs.unlinkSync = origUnlink;
    for (const [k, v] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
  assert.equal(fs.existsSync(path.join(ticketDir, 'shutdown')), true, 'marked');
  assert.equal(fs.existsSync(path.join(ticketDir, 'shutdown-unstarted')), false, 'the unlink failed: nothing was dequeued');
});
