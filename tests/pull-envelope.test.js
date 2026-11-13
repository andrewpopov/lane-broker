import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createEnvelope, envelopeFile, pullTicketDir, readEnvelope, recordSnapshot } from '../src/pull-envelope.js';
import { freshEnv } from './helpers.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';

/** BRAIN-1036 R4: the recovery envelope is durable, with its ancestors, before anything is uploaded or sent. */

const ID = '00000000-0000-0000-0000-000000000042';
const fields = (overrides = {}) => ({
  logicalId: ID,
  binding: { protocol: 1, ticketId: ID, generation: 3, manifestHash: 'mh' },
  nonce: 'nonce-1',
  header: { protocol: 1, ticketId: ID, argv: ['true'] },
  repoKey: 'repoA',
  lane: 'test',
  laneKey: 'repoA:test',
  cpuCores: 2,
  memoryBytes: 1000,
  requiresTags: ['linux'],
  priority: 0,
  worktreeRoot: '/work/tree',
  ...overrides,
});
const SNAPSHOT = { key: `streams/${ID}-3-abc.gz`, sha256: 'abc', bytes: 10, blobs: [] };

test('create: every ancestor is fsynced first, then the file, then its directory entry', (t) => {
  const { state } = freshEnv();
  fs.mkdirSync(path.join(state, 'pull'));
  const rec = recordFsyncs();
  try {
    createEnvelope(state, fields());
  } finally {
    rec.stop();
  }
  const ticketDir = pullTicketDir(state, ID);
  const at = (pathName, from = 0) => rec.events.findIndex((e, i) => i >= from && e.op === 'fsync' && e.path === pathName);
  const [rootFsync, pullFsync, dirFsync] = [at(state), at(path.join(state, 'pull')), at(ticketDir)];
  const rename = rec.indexOf('rename', envelopeFile(state, ID));
  const fileFsync = rec.events.findIndex((e) => e.op === 'fsync' && e.path?.startsWith(path.join(ticketDir, '.tmp-')));
  assert.ok(rootFsync >= 0 && pullFsync > rootFsync && dirFsync > pullFsync, `ancestors outermost first (even a pre-existing pull/): ${[rootFsync, pullFsync, dirFsync]}`);
  assert.ok(fileFsync > dirFsync, 'the file is fsynced after its directory chain');
  assert.ok(rename > fileFsync, 'renamed into place only after the file is fsynced');
  assert.ok(at(ticketDir, rename) > rename, 'the directory entry is fsynced after the rename');
  t.diagnostic(`fsync order ${[rootFsync, pullFsync, dirFsync, fileFsync, rename]}`);
});

test('create refuses to overwrite, and a missing required field names itself', () => {
  const { state } = freshEnv();
  const created = createEnvelope(state, fields());
  assert.deepEqual(readEnvelope(state, ID), created);
  assert.throws(() => createEnvelope(state, fields({ nonce: 'other' })), { code: 'EEXIST' });
  assert.equal(readEnvelope(state, ID).nonce, 'nonce-1');
  const { nonce, ...withoutNonce } = fields();
  assert.throws(() => createEnvelope(state, withoutNonce), /lacks nonce/);
  assert.throws(() => createEnvelope(state, fields({ binding: { protocol: 1 } })), /binding\.generation, binding\.manifestHash/);
  assert.throws(() => createEnvelope(state, fields({ logicalId: '../escape' })), /ticket uuid/);
});

test('read: absent is null, but an envelope that exists and cannot be read is never "nothing prepared"', () => {
  const { state } = freshEnv();
  assert.equal(readEnvelope(state, ID), null);
  createEnvelope(state, fields());
  fs.writeFileSync(envelopeFile(state, ID), '{"v":1,');
  assert.throws(() => readEnvelope(state, ID), SyntaxError);
  fs.writeFileSync(envelopeFile(state, ID), JSON.stringify({ ...fields(), v: 2 }));
  assert.throws(() => readEnvelope(state, ID), /version 2/);
});

test('recordSnapshot: durable, repeatable, and never replaced by a different descriptor', () => {
  const { state } = freshEnv();
  createEnvelope(state, fields());
  const rec = recordFsyncs();
  try {
    recordSnapshot(state, ID, SNAPSHOT);
  } finally {
    rec.stop();
  }
  assert.ok(rec.indexOf('rename', envelopeFile(state, ID)) >= 0 && rec.events.some((e) => e.op === 'fsync' && e.path?.includes('.tmp-')), 'the update is fsynced and renamed');
  assert.deepEqual(readEnvelope(state, ID).snapshot, SNAPSHOT);
  assert.deepEqual(recordSnapshot(state, ID, { ...SNAPSHOT }).snapshot, SNAPSHOT);
  assert.throws(() => recordSnapshot(state, ID, { ...SNAPSHOT, sha256: 'def' }), /different snapshot/);
  assert.deepEqual(readEnvelope(state, ID).snapshot, SNAPSHOT);
  assert.throws(() => recordSnapshot(state, '00000000-0000-0000-0000-000000000043', SNAPSHOT), /no pull envelope/);
});
