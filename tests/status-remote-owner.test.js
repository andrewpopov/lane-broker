import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { enqueue } from '../src/scheduler.js';
import { collectStatus } from '../src/status.js';
import { tmpDir } from './remote-harness.js';

// BRAIN-518: `lane status --json` names the submitter of a runner-side remote-exec ticket (`remoteOwner`), else null.

const ticket = (id) => ({ id, key: `r:${id}`, weight: 1, supervisorPid: process.pid, supervisorStart: null });

async function withStatusEnv(fn) {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await fn({ state, remoteRoot: tmpDir('owner-remote') });
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

const writeRemoteId = (remoteRoot, submitter, brokerId) => {
  fs.mkdirSync(path.join(remoteRoot, 'tickets', submitter), { recursive: true });
  fs.writeFileSync(path.join(remoteRoot, 'tickets', submitter, 'remote-id'), `${brokerId}\n`);
};

test('a queued remote-exec ticket reports its submitter; a local ticket reports null', async () => {
  await withStatusEnv(async ({ state, remoteRoot }) => {
    await enqueue(state, ticket('remote-t'));
    await enqueue(state, ticket('local-t'));
    writeRemoteId(remoteRoot, 'submitter-7', 'remote-t');
    const status = await collectStatus({ remoteRoot });
    assert.deepEqual(status.queued.map((q) => [q.id, q.remoteOwner]), [['remote-t', 'submitter-7'], ['local-t', null]]);
    assert.ok(JSON.parse(JSON.stringify(status)).queued.every((q) => 'remoteOwner' in q), 'the key survives JSON');
  });
});

test('an unreadable remote-id, or no remote root at all, yields null and status still succeeds', async () => {
  await withStatusEnv(async ({ state, remoteRoot }) => {
    await enqueue(state, ticket('remote-t'));
    fs.mkdirSync(path.join(remoteRoot, 'tickets', 'broken', 'remote-id'), { recursive: true });
    writeRemoteId(remoteRoot, 'good', 'other-id');
    let status = await collectStatus({ remoteRoot });
    assert.equal(status.queued[0].remoteOwner, null);
    status = await collectStatus({ remoteRoot: path.join(remoteRoot, 'missing') });
    assert.equal(status.queued[0].remoteOwner, null);
  });
});
