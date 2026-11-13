import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { AgentStoreError, ExecutionTable, SUBPHASES, ensureAgentDirs, newExecutionRecord, reassertPhase, reassertVerdict } from '../src/agent/store.js';

const ID = '11111111-1111-4111-8111-111111111111';
const TOKEN = '22222222-2222-4222-8222-222222222222';
const claim = { id: ID, epoch: 2, token: TOKEN, repo_key: 'repoA', lane: 'test', lane_key: 'lk', binding: { protocol: 1 }, header: { cmd: ['true'] }, snapshot: { key: 'snap' } };

const at = (subphase, outbox) => ({ ...newExecutionRecord(claim), subphase, ...(outbox && { outbox }) });
const freshRoot = () => path.join(makeTmpDir('agent-records-'), 'remote');

const PHASES = {
  AWAIT_URLS: 'pre-start', FETCHING: 'pre-start', VERIFIED: 'pre-start', STARTING: 'pre-start',
  SPAWNING: 'running', RUNNING: 'running', SETTLING: 'running', HOLD: 'running', KILLING: null, DONE: null,
};

test('a new record carries the claim, the execution id and a zero log cursor', () => {
  const record = newExecutionRecord(claim, { now: 1234 });
  assert.deepEqual(record, {
    id: ID, epoch: 2, token: TOKEN, execId: `${ID}.e2`, claim: { repoKey: 'repoA', lane: 'test', laneKey: 'lk', binding: { protocol: 1 }, header: { cmd: ['true'] }, snapshot: { key: 'snap' } },
    subphase: 'AWAIT_URLS', claimedAt: 1234, kernelBootId: null, logCursor: { seq: 0, offset: 0, len: 0 },
  });
});

test('the phase a record is reasserted in follows the phase map: a release outbox never reasserts as running, KILLING and DONE not at all', () => {
  for (const [subphase, phase] of Object.entries(PHASES)) assert.equal(reassertPhase(at(subphase)), phase, subphase);
  assert.equal(reassertPhase(at('OUTBOX', { kind: 'finish' })), 'running');
  assert.equal(reassertPhase(at('OUTBOX', { kind: 'release' })), 'pre-start');
  assert.deepEqual(Object.keys(PHASES).concat('OUTBOX').sort(), [...SUBPHASES].sort(), 'the phase map covers every subphase');
});

test('reassertVerdict: ok resumes, fenced or unknown ends a pre-start record and kills a running one', () => {
  for (const result of ['fenced', 'unknown']) {
    for (const subphase of ['AWAIT_URLS', 'FETCHING', 'VERIFIED']) assert.deepEqual(reassertVerdict(at(subphase), { id: ID, result }), { next: 'done', anomaly: false, tombstone: false }, subphase);
    assert.deepEqual(reassertVerdict(at('STARTING'), { id: ID, result }), { next: 'done', anomaly: false, tombstone: true }, 'STARTING also tombstones the execution id');
    for (const subphase of ['SPAWNING', 'RUNNING', 'SETTLING', 'HOLD']) assert.deepEqual(reassertVerdict(at(subphase), { id: ID, result }), { next: 'killing', anomaly: false }, subphase);
    assert.equal(reassertVerdict(at('OUTBOX', { kind: 'finish' }), { id: ID, result }).next, 'done');
    assert.equal(reassertVerdict(at('OUTBOX', { kind: 'release' }), { id: ID, result }).next, 'done');
  }
  for (const subphase of ['AWAIT_URLS', 'RUNNING', 'SETTLING']) assert.deepEqual(reassertVerdict(at(subphase), { id: ID, result: 'ok', state: 'claimed' }).next, subphase === 'AWAIT_URLS' ? 'resume' : 'killing');
  assert.equal(reassertVerdict(at('RUNNING'), { id: ID, result: 'ok', state: 'running' }).next, 'resume');
  assert.throws(() => reassertVerdict(at('RUNNING'), { id: ID, result: 'maybe' }), AgentStoreError);
});

test('a running-phase item answered ok on a still-claimed row is an anomaly judged as fenced, for every running-phase record', () => {
  const answer = { id: ID, result: 'ok', state: 'claimed' };
  for (const [record, next] of [[at('SPAWNING'), 'killing'], [at('RUNNING'), 'killing'], [at('HOLD'), 'killing'], [at('OUTBOX', { kind: 'finish' }), 'done']]) {
    assert.deepEqual(reassertVerdict(record, answer), { next, anomaly: true }, record.subphase);
  }
  assert.equal(reassertVerdict(at('STARTING'), answer).anomaly, false, 'a pre-start item on a claimed row is the normal case');
});

test('the table keeps records on disk, reloads them, and reasserts only the live ones', () => {
  const root = freshRoot();
  ensureAgentDirs(root);
  const table = new ExecutionTable(root);
  const live = at('RUNNING');
  const other = { ...at('KILLING'), id: '33333333-3333-4333-8333-333333333333', execId: '33333333-3333-4333-8333-333333333333.e2' };
  table.put(live);
  table.put(other);
  assert.deepEqual(table.reassertItems(), [{ id: ID, epoch: 2, token: TOKEN, phase: 'running' }]);
  const reloaded = new ExecutionTable(root);
  assert.deepEqual(reloaded.all(), [live, other].sort((a, b) => a.execId.localeCompare(b.execId)));
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
