import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';
import { openAgentCore } from '../src/agent/agent.js';
import { ExecutionTable, agentPaths, ensureAgentDirs, newExecutionRecord } from '../src/agent/store.js';

const ID = '11111111-1111-4111-8111-111111111111';
const TOKEN = '22222222-2222-4222-8222-222222222222';
const claim = { id: ID, epoch: 1, token: TOKEN, repo_key: 'repoA', lane: 'test', lane_key: 'lk', binding: {}, header: {}, snapshot: {} };

/** A pg client that answers the gate's calls, so the order of local fsyncs against the first ld_* call is observable without a database. */
const stubConnect = async () => ({
  on() {},
  end: async () => {},
  escapeIdentifier: (name) => `"${name}"`,
  query: async (sql) => {
    if (sql.startsWith('LISTEN')) return { rows: [] };
    return { rows: [{ v: { host: 'a1', max_claims: 2, protocol_min: 1 } }] };
  },
});

function withTmpRoot(t) {
  const base = fs.realpathSync(makeTmpDir('agent-durability-'));
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = path.join(base, 'state');
  t.after(() => { if (prev === undefined) delete process.env.LANE_BROKER_STATE; else process.env.LANE_BROKER_STATE = prev; });
  return { base, remoteRoot: path.join(base, 'remote') };
}

test('T23 every component of the agent directories is fsynced with its parent before the first ld_* call, including one that already existed', async (t) => {
  const { base, remoteRoot } = withTmpRoot(t);
  fs.mkdirSync(path.join(remoteRoot, 'agent'), { recursive: true });
  const recorder = recordFsyncs();
  let core;
  try {
    core = await openAgentCore({ remoteRoot, config: { pg: {}, schedule: null }, version: 'test', connect: stubConnect, onCall: (name) => recorder.events.push({ op: 'call', path: name }) });
  } finally {
    recorder.stop();
  }
  await core.close();
  const p = agentPaths(remoteRoot);
  const firstCall = recorder.events.findIndex((e) => e.op === 'call');
  assert.equal(recorder.events[firstCall].path, 'ld_register');
  for (const dir of [base, remoteRoot, p.root, p.executions, p.out, p.spool]) {
    const at = recorder.indexOf('fsync', dir);
    assert.ok(at >= 0 && at < firstCall, `${dir} fsynced before the first call (at ${at}, call at ${firstCall})`);
  }
  const ownerWrittenAt = recorder.indexOf('rename', p.owner);
  assert.ok(ownerWrittenAt > recorder.indexOf('fsync', p.executions) && ownerWrittenAt < firstCall, 'owner.json is written after the directories are durable and before the first call');
});

test('T23 a record is fsynced before its rename and its directory after it; a removal fsyncs the directory', (t) => {
  const { remoteRoot } = withTmpRoot(t);
  const { executions } = ensureAgentDirs(remoteRoot);
  const table = new ExecutionTable(remoteRoot);
  const record = newExecutionRecord(claim);
  const target = path.join(executions, `${record.execId}.json`);

  let recorder = recordFsyncs();
  try {
    table.put(record);
  } finally {
    recorder.stop();
  }
  const renamedAt = recorder.indexOf('rename', target);
  const tmpFsyncAt = recorder.events.findIndex((e) => e.op === 'fsync' && e.path?.startsWith(path.join(executions, '.tmp-')));
  assert.ok(tmpFsyncAt >= 0 && tmpFsyncAt < renamedAt, 'the file is fsynced before the rename');
  assert.ok(recorder.indexOf('fsync', executions) > renamedAt, 'the directory is fsynced after the rename');

  recorder = recordFsyncs();
  try {
    table.remove(record.execId);
  } finally {
    recorder.stop();
  }
  assert.equal(fs.existsSync(target), false);
  assert.ok(recorder.fsyncedPaths().includes(executions), 'the unlink is made durable');
});

test('T23 memory only changes after the record is on disk', { skip: process.getuid() === 0 }, (t) => {
  const { remoteRoot } = withTmpRoot(t);
  const { executions } = ensureAgentDirs(remoteRoot);
  const table = new ExecutionTable(remoteRoot);
  const record = newExecutionRecord(claim);
  fs.chmodSync(executions, 0o500);
  try {
    assert.throws(() => table.put(record), { code: 'EACCES' });
  } finally {
    fs.chmodSync(executions, 0o700);
  }
  assert.equal(table.get(record.execId), undefined);
  assert.deepEqual(fs.readdirSync(executions), []);
});
