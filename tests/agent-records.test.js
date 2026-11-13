import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { AgentStoreError, ExecutionTable, ensureAgentDirs, newExecutionRecord } from '../src/agent/store.js';

const ID = '11111111-1111-4111-8111-111111111111';
const TOKEN = '22222222-2222-4222-8222-222222222222';
const claim = { id: ID, epoch: 2, token: TOKEN, repo_key: 'repoA', lane: 'test', lane_key: 'lk', binding: { protocol: 1 }, header: { cmd: ['true'] }, snapshot: { key: 'snap' } };

const at = (subphase, outbox) => ({ ...newExecutionRecord(claim), subphase, ...(outbox && { outbox }) });
const freshRoot = () => path.join(makeTmpDir('agent-records-'), 'remote');

test('a new record carries the claim, the execution id and a zero log cursor', () => {
  const record = newExecutionRecord(claim, { now: 1234 });
  assert.deepEqual(record, {
    id: ID, epoch: 2, token: TOKEN, execId: `${ID}.e2`, claim: { repoKey: 'repoA', lane: 'test', laneKey: 'lk', binding: { protocol: 1 }, header: { cmd: ['true'] }, snapshot: { key: 'snap' } },
    subphase: 'AWAIT_URLS', claimedAt: 1234, kernelBootId: null, logCursor: { seq: 0, offset: 0, len: 0 },
  });
});

test('the table keeps records on disk and reloads them', () => {
  const root = freshRoot();
  ensureAgentDirs(root);
  const table = new ExecutionTable(root);
  const live = at('RUNNING');
  const other = { ...at('KILLING'), id: '33333333-3333-4333-8333-333333333333', execId: '33333333-3333-4333-8333-333333333333.e2' };
  table.put(live);
  table.put(other);
  const reloaded = new ExecutionTable(root);
  assert.deepEqual(reloaded.all(), [live, other].sort((a, b) => a.execId.localeCompare(b.execId)));
  const handed = table.get(live.execId);
  handed.subphase = 'DONE';
  live.subphase = 'DONE';
  assert.equal(table.get(live.execId).subphase, 'RUNNING', 'what put stored and what get returned are copies, so memory cannot run ahead of disk');
  reloaded.remove(live.execId);
  assert.deepEqual(new ExecutionTable(root).all().map((r) => r.execId), [other.execId]);
});

test('a record file that cannot be trusted stops the agent instead of being skipped', () => {
  const bad = {
    'not JSON': (file) => fs.writeFileSync(file, '{"id":'),
    'a subphase that does not exist': (file) => fs.writeFileSync(file, JSON.stringify({ ...at('RUNNING'), subphase: 'LIMBO' })),
    'an outbox with no kind': (file) => fs.writeFileSync(file, JSON.stringify({ ...at('OUTBOX'), outbox: {} })),
    'an execId that disagrees with id and epoch': (file) => fs.writeFileSync(file, JSON.stringify({ ...at('RUNNING'), epoch: 5 })),
  };
  for (const [why, write] of Object.entries(bad)) {
    const root = freshRoot();
    const { executions } = ensureAgentDirs(root);
    write(path.join(executions, `${ID}.e2.json`));
    assert.throws(() => new ExecutionTable(root), AgentStoreError, why);
  }
  const root = freshRoot();
  const { executions } = ensureAgentDirs(root);
  fs.writeFileSync(path.join(executions, `${ID}.e9.json`), JSON.stringify(at('RUNNING')));
  assert.throws(() => new ExecutionTable(root), /holds .*\.e2/, 'a file named for another execution');
});
