import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { startStore, fakeClock, snapshotOf, publish, sha } from './store-harness.js';
import { readJournal } from '../src/store/journal.js';
import { Replicator } from '../src/store/replicate.js';
import { sweep } from '../src/store/retention.js';
import { blobRelPath, manifestRelPath } from '../src/store/ids.js';
import { lockMode } from '../src/store/lock.js';

// The fence guards the macOS file-lock fallback only; on Linux the kernel-held lock has no path to displace.
const fileLockOnly = { skip: lockMode() === 'file' ? false : 'macOS file-lock fallback; Linux uses the kernel-held lock (see the N11 (Linux) tests)' };

const DAY = 24 * 3600 * 1000;
const replicator = (p) => new Replicator({ store: p.primary.store, root: p.primary.root, replica: p.replica.replicaPeer, now: p.clock.now });

async function pair({ replicaGraceHours } = {}) {
  const clock = fakeClock();
  const primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true, ...(replicaGraceHours ? { replicaGraceHours } : {}) });
  return { clock, primary, replica, close: () => Promise.all([primary.close(), replica.close()]) };
}

// ---- N11: fence by self-check ----
test('N11 a displaced owner (its lock path now names another file) throws LockLostError on the next append and writes nothing', fileLockOnly, async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  await srv.submit.putBlob(sha('before'), Buffer.from('before'));
  const journalFile = path.join(srv.root, 'journal.log');
  const sizeBefore = fs.statSync(journalFile).size;
  const lock = path.join(srv.root, 'store.lock');
  // A/B/C: B renamed A's live lock aside, C now owns the path
  fs.renameSync(lock, `${lock}.displaced`);
  fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, token: 'C' }));
  assert.throws(() => srv.store.journal.append({ kind: 'blob', path: 'blobs/zz', createdAt: 1 }), (e) => e.name === 'LockLostError');
  assert.equal(fs.statSync(journalFile).size, sizeBefore, 'nothing was written');
  assert.equal(srv.store.fenced, true);
  await assert.rejects(srv.submit.putBlob(sha('after'), Buffer.from('after')));
  assert.equal(fs.statSync(journalFile).size, sizeBefore);
});

test('N11 a vanished lock path also fences the owner, and the replication watermark is not written', fileLockOnly, async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await p.primary.submit.putBlob(sha('x'), Buffer.from('x'));
  fs.rmSync(path.join(p.primary.root, 'store.lock'));
  await assert.rejects(replicator(p).runOnce(), (e) => e.name === 'LockLostError');
  assert.equal(fs.existsSync(path.join(p.primary.root, 'replication.json')), false, 'no watermark write from a displaced owner');
});

test('N11 the fence invokes onLockLost so the server can shut down', fileLockOnly, async (t) => {
  let lost = null;
  const srv = await startStore({ onLockLost: (e) => { lost = e; } });
  t.after(() => srv.close());
  fs.rmSync(path.join(srv.root, 'store.lock'));
  assert.throws(() => srv.store.journal.append({ kind: 'blob', path: 'blobs/zz', createdAt: 1 }));
  assert.equal(lost?.name, 'LockLostError');
});

// ---- N14: reference count ----
test('N14 two overlapping rejournals: protection lasts until the LAST one finishes', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  await srv.submit.putBlob(sha('shared'), Buffer.from('shared'));
  const rel = blobRelPath(sha('shared'));
  const real = srv.store.verifyObject.bind(srv.store);
  let calls = 0;
  let release;
  const gate = new Promise((r) => { release = r; });
  const m = mock.method(srv.store, 'verifyObject', async (r) => {
    calls += 1;
    if (calls === 2) await gate; // the second re-journal stays open
    return real(r);
  });
  const first = srv.store.rejournal([rel]);
  const second = srv.store.rejournal([rel]);
  await first;
  assert.equal(srv.store.deleteObject(rel), false, 'the second re-journal still protects it');
  release();
  await second;
  m.mock.restore();
  const kinds = readJournal(srv.root).map((r) => r.kind);
  assert.ok(!kinds.includes('delete'), kinds.join(','));
  assert.equal(srv.store.deleteObject(rel), true, 'once both are done, retention proceeds');
});

// ---- N19 ----
test('N19a the replica ages a manifest from the replicated terminal mark, not from when it registered', async (t) => {
  const p = await pair({ replicaGraceHours: 120 }); // 5 days
  t.after(() => p.close());
  await publish(p.primary, 'job-1', snapshotOf({ 'a.txt': 'long-lived input' }));
  assert.equal((await replicator(p).runOnce()).ok, true);
  p.clock.advance(6 * DAY); // registered 6 days ago, past the 5-day grace
  await p.primary.admin.json('PUT', '/jobs/job-1/terminal');
  assert.equal((await replicator(p).runOnce()).ok, true);
  sweep(p.replica.store);
  assert.ok(p.replica.store.readManifest('job-1'), 'terminal for 0 days: kept');
  p.clock.advance(5 * DAY);
  sweep(p.replica.store);
  assert.equal(p.replica.store.readManifest('job-1'), null);
});

test('N19b the replica lost a blob its retention swept; a new manifest that needs it converges with no operator action', async (t) => {
  const p = await pair({ replicaGraceHours: 24 });
  t.after(() => p.close());
  const one = snapshotOf({ 'shared.bin': 'blob B used by two jobs' });
  await publish(p.primary, 'job-1', one);
  assert.equal((await replicator(p).runOnce()).ok, true);
  await p.primary.admin.json('PUT', '/jobs/job-1/terminal');
  assert.equal((await replicator(p).runOnce()).ok, true);
  p.clock.advance(2 * DAY); // past the replica's 1-day grace, well inside the primary's 14 days
  sweep(p.replica.store);
  const [h] = one.blobs.keys();
  assert.equal(p.replica.store.hasBlob(h), false, 'the replica swept M1 and B');
  assert.equal(p.primary.store.hasBlob(h), true);
  const two = snapshotOf({ 'shared.bin': 'blob B used by two jobs', 'extra.txt': 'new' });
  for (const [bh, buf] of two.blobs) await p.primary.submit.putBlob(bh, buf);
  await p.primary.submit.putManifest('job-2', two.manifest);
  const r = await replicator(p).runOnce();
  assert.equal(r.ok, true, r.error);
  assert.ok(p.replica.store.readManifest('job-2'));
  assert.equal(p.replica.store.hasBlob(h), true, 'B was re-shipped, verified');
});

test('N19b if the blob is gone on the primary too, the path blocks as before', async (t) => {
  const p = await pair({ replicaGraceHours: 24 });
  t.after(() => p.close());
  const one = snapshotOf({ 'shared.bin': 'blob B vanishes everywhere' });
  await publish(p.primary, 'job-1', one);
  assert.equal((await replicator(p).runOnce()).ok, true);
  await p.primary.admin.json('PUT', '/jobs/job-1/terminal');
  assert.equal((await replicator(p).runOnce()).ok, true);
  p.clock.advance(2 * DAY);
  sweep(p.replica.store);
  const two = snapshotOf({ 'shared.bin': 'blob B vanishes everywhere', 'extra.txt': 'new' });
  for (const [bh, buf] of two.blobs) await p.primary.submit.putBlob(bh, buf);
  await p.primary.submit.putManifest('job-2', two.manifest);
  fs.rmSync(path.join(p.primary.root, blobRelPath([...one.blobs.keys()][0])));
  const r = await replicator(p).runOnce();
  assert.equal(r.ok, false);
  assert.match(r.error, /no longer has it/);
});

test('N11 only ObjectStore.open builds a store: a bare constructor (which would take no platform lock) is refused', async () => {
  const { ObjectStore } = await import('../src/store/objects.js');
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'lane-store-ctor-'));
  try {
    assert.throws(() => new ObjectStore(root), /ObjectStore\.open/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('N11 the file lock refuses on Linux, so a hand-made file lock cannot sit beside the kernel-held lock', { skip: process.platform === 'linux' ? false : 'Linux-only; run on wintop' }, async () => {
  const { acquireStoreLock } = await import('../src/store/lock.js');
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'lane-store-flock-'));
  try {
    assert.throws(() => acquireStoreLock(root), /fallback only/);
    assert.equal(fs.existsSync(path.join(root, 'store.lock')), false);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
