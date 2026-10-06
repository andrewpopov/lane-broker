import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import { Readable } from 'node:stream';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { startStore, fakeClock, snapshotOf, publish, sha, tmpDir } from './store-harness.js';
import { readJournal, readJournalFrom } from '../src/store/journal.js';
import { Replicator, readReplicationState } from '../src/store/replicate.js';
import { compareStores } from '../src/store/compare.js';
import { blobRelPath, manifestRelPath } from '../src/store/ids.js';
import { StoreHttpError } from '../src/store/client.js';

const BIN = fileURLToPath(new URL('../bin/lane-store.js', import.meta.url));
const put = (srv, text) => srv.submit.putBlob(sha(text), Buffer.from(text));

async function pair(opts = {}) {
  const clock = fakeClock();
  const primary = await startStore({ clock, ...opts });
  const replica = await startStore({ clock, replicaMode: true });
  return { clock, primary, replica, close: () => Promise.all([primary.close(), replica.close()]) };
}
const replicator = (p, replica = p.replica.replicaPeer) => new Replicator({ store: p.primary.store, replica, now: p.clock.now });
const sameAsReplica = async (p) => (await compareStores({ store: p.primary.store, replica: p.replica.replicaPeer, deep: true })).ok;

// ---- A ----
test('A a reader that starts during an append whose fsync fails never sees the record, and the replica ends equal to the journal', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'X');
  assert.equal((await replicator(p).runOnce()).ok, true);
  const journal = p.primary.store.journal;
  const real = fs.fsyncSync;
  let seenDuringAppend = null;
  const m = mock.method(fs, 'fsyncSync', (fd) => {
    if (fd === journal.fd && seenDuringAppend === null) {
      seenDuringAppend = journal.read(0).map((r) => r.path); // the only reader there is: the in-process one, bounded by committedOffset
      throw new Error('EIO');
    }
    return real(fd);
  });
  await assert.rejects(put(p.primary, 'Y'), (e) => e instanceof StoreHttpError && e.status >= 500);
  m.mock.restore();
  assert.deepEqual(seenDuringAppend, [blobRelPath(sha('X'))], 'the uncommitted record was invisible to the reader');
  assert.equal(journal.read(0).length, 1);
  await put(p.primary, 'Y'); // reuses the rolled-back seq and offset
  assert.equal((await replicator(p).runOnce()).ok, true);
  assert.equal(await sameAsReplica(p), true);
  assert.equal(p.replica.store.hasBlob(sha('Y')), true);
});

// ---- B ----
test('B a delete whose unlink fails journals nothing, so a later manifest can still reference the blob and replication converges', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  const snap = snapshotOf({ 'f.txt': 'survivor' });
  const [h, buf] = [...snap.blobs][0];
  await p.primary.submit.putBlob(h, buf);
  const m = mock.method(fs, 'unlinkSync', () => { throw new Error('EBUSY'); }, { times: 1 });
  assert.throws(() => p.primary.store.deleteObject(blobRelPath(h)), /EBUSY/);
  m.mock.restore();
  assert.deepEqual(readJournal(p.primary.root).map((r) => r.kind), ['blob', 'delete-intent'], 'an intent, but no delete record for an object that is still there');
  await p.primary.submit.putManifest('job-1', snap.manifest);
  const r = await replicator(p).runOnce();
  assert.equal(r.ok, true, r.error);
  assert.equal(await sameAsReplica(p), true);
  assert.ok(p.replica.store.readManifest('job-1'));
});

test('B a put whose object is gone and whose delete is committed later is not skipped: the replica is brought to the deleted state and verified, or the watermark stays', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'doomed');
  assert.equal(p.primary.store.deleteObject(blobRelPath(sha('doomed'))), true); // put (1), intent (2), committed delete (3)
  const stuck = Object.create(p.replica.replicaPeer);
  stuck.deleteObject = async () => { throw new Error('replica delete failed'); };
  const failed = await replicator(p, stuck).runOnce();
  assert.equal(failed.ok, false);
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, 0, 'never advanced past an unverified put');
  const ok = await replicator(p).runOnce();
  assert.equal(ok.ok, true, ok.error);
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, 3);
  assert.equal(p.replica.store.hasBlob(sha('doomed')), false);
});

test('B interrupted retention (intent journaled, unlinked, no delete) completes at startup and the replica converges', async (t) => {
  const clock = fakeClock();
  let primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true });
  t.after(() => replica.close());
  await put(primary, 'retired');
  const root = primary.root;
  assert.equal((await new Replicator({ store: primary.store, replica: replica.replicaPeer, now: clock.now }).runOnce()).ok, true);
  const real = primary.store.journal.append.bind(primary.store.journal);
  const m = mock.method(primary.store.journal, 'append', (f) => { if (f.kind === 'delete') throw new Error('EIO'); return real(f); });
  assert.throws(() => primary.store.deleteObject(blobRelPath(sha('retired'))), /EIO/);
  m.mock.restore();
  await primary.close(); // crash: intent durable, object unlinked, delete never journaled
  primary = await startStore({ clock, root });
  t.after(() => primary.close());
  assert.deepEqual(readJournal(root).map((r) => r.kind), ['blob', 'delete-intent', 'delete']);
  assert.deepEqual(primary.store.reconcileReport.deleted, [blobRelPath(sha('retired'))]);
  assert.equal((await new Replicator({ store: primary.store, replica: replica.replicaPeer, now: clock.now }).runOnce()).ok, true);
  assert.equal(replica.store.hasBlob(sha('retired')), false);
});

// ---- C ----
test('C a pin for a job missing on both sides with no committed manifest delete blocks; with one it converges', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await publish(p.primary, 'job-1', snapshotOf({ 'a.txt': 'a' }));
  assert.equal((await replicator(p).runOnce()).ok, true);
  await p.primary.admin.json('PUT', '/pins/job-1'); // seq 3
  const seqBefore = readReplicationState(p.primary.root).replicatedSeq;
  fs.rmSync(path.join(p.primary.root, manifestRelPath('job-1')));
  fs.rmSync(path.join(p.replica.root, manifestRelPath('job-1')));
  const r = await replicator(p).runOnce();
  assert.equal(r.ok, false);
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, seqBefore, 'watermark did not move');
});

test('C a pin whose job was legitimately expired (committed manifest delete) is confirmed absent and passes', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await publish(p.primary, 'job-1', snapshotOf({ 'a.txt': 'a' }));
  assert.equal((await replicator(p).runOnce()).ok, true);
  await p.primary.admin.json('PUT', '/pins/job-1');
  await p.primary.admin.json('DELETE', '/pins/job-1');
  await p.primary.admin.json('PUT', '/jobs/job-1/terminal');
  p.primary.store.deleteObject(manifestRelPath('job-1'));
  fs.rmSync(path.join(p.replica.root, manifestRelPath('job-1'))); // replica lost it too, before the pin records arrive
  const r = await replicator(p).runOnce();
  assert.equal(r.ok, true, r.error);
});

// ---- D ----
test('D a watermark that is not format 2 refuses to start, and --rebuild-watermark recomputes it from the journal', async (t) => {
  const clock = fakeClock();
  const first = await startStore({ clock });
  for (const x of ['a', 'b', 'c']) await put(first, x);
  const root = first.root;
  await first.close();
  fs.writeFileSync(path.join(root, 'replication.json'), JSON.stringify({ replicatedSeq: 2, failedRounds: 0 }));
  await assert.rejects(startStore({ clock, root }), /rebuild-watermark/);
  const rebuilt = spawnSync(process.execPath, [BIN, 'rebuild-watermark', '--seq', '2', '--root', root], { encoding: 'utf8' });
  assert.equal(rebuilt.status, 0, rebuilt.stderr);
  const file = JSON.parse(fs.readFileSync(path.join(root, 'replication.json'), 'utf8'));
  assert.equal(file.format, 2);
  assert.equal(file.seq, 2);
  assert.equal(file.offset, readJournal(root)[1].end);
  const again = await startStore({ clock, root });
  t.after(() => again.close());
  const replica = await startStore({ clock, replicaMode: true });
  t.after(() => replica.close());
  const r = await new Replicator({ store: again.store, replica: replica.replicaPeer, now: clock.now }).runOnce();
  assert.deepEqual({ ok: r.ok, shipped: r.shipped, seq: r.replicatedSeq }, { ok: true, shipped: 1, seq: 3 });
  assert.equal(replica.store.hasBlob(sha('c')), true);
  assert.equal(replica.store.hasBlob(sha('a')), false, 'records at or below the rebuilt watermark are not re-shipped');
});

// ---- E ----
test('E a manifest followed by a pin stays a journaled manifest: compare reports nothing and reconcile appends nothing', async (t) => {
  const clock = fakeClock();
  const primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true });
  t.after(() => replica.close());
  await publish(primary, 'job-1', snapshotOf({ 'a.txt': 'a' }));
  await primary.admin.json('PUT', '/pins/job-1');
  const root = primary.root;
  assert.equal((await new Replicator({ store: primary.store, replica: replica.replicaPeer, now: clock.now }).runOnce()).ok, true);
  const diff = await compareStores({ store: primary.store, replica: replica.replicaPeer });
  assert.deepEqual(diff.notInJournal, []);
  const before = readJournal(root).length;
  await primary.close();
  const again = await startStore({ clock, root });
  t.after(() => again.close());
  assert.equal(readJournal(root).length, before);
  assert.deepEqual(again.store.reconcileReport.journaled, []);
});

// ---- F ----
test('F an upload whose blob is deleted mid-flight never overshoots the admission threshold and is told to retry', async (t) => {
  const srv = await startStore({ capBytes: 1000 });
  t.after(() => srv.close());
  await srv.submit.putBlob(sha(Buffer.alloc(860, 9)), Buffer.alloc(860, 9));
  const d = Buffer.alloc(40, 1);
  await srv.submit.putBlob(sha(d), d); // bytes = 900
  let open;
  const gate = new Promise((r) => { open = r; });
  const slow = Readable.from((async function* () { await gate; yield d; })());
  const inflight = srv.store.putBlob(sha(d), slow, d.length); // intact duplicate at admission: no reservation
  const outcome = inflight.then((v) => ({ v }), (err) => ({ err }));
  await new Promise((r) => setTimeout(r, 50));
  srv.store.deleteObject(blobRelPath(sha(d))); // retention removes it mid-upload
  const e = Buffer.alloc(40, 2);
  await srv.submit.putBlob(sha(e), e);
  open();
  const res = await outcome;
  assert.equal(res.err?.status, 503, 'told to retry rather than acknowledged for a blob that is gone');
  assert.ok(srv.store.bytes <= 950, `bytes ${srv.store.bytes}`);
  assert.equal(srv.store.reserved, 0);
});

test('F a failure generating the temp name leaves zero reservations', async (t) => {
  const srv = await startStore({ capBytes: 1000 });
  t.after(() => srv.close());
  const m = mock.method(crypto, 'randomBytes', () => { throw new Error('entropy'); }, { times: 1 });
  await assert.rejects(put(srv, 'x'.repeat(40)));
  m.mock.restore();
  assert.equal(srv.store.reserved, 0);
});

// ---- G ----
test('G a failure after the rename (utimes) leaves no unjournaled, uncounted blob: the retry journals it', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const m = mock.method(fs, 'utimesSync', () => { throw new Error('EPERM'); }, { times: 1 });
  await assert.rejects(put(srv, 'after rename'));
  m.mock.restore();
  await put(srv, 'after rename');
  assert.deepEqual(readJournal(srv.root).map((r) => r.path), [blobRelPath(sha('after rename'))]);
  assert.equal(srv.store.bytes, Buffer.byteLength('after rename'));
});

test('G if the rollback fails too the blob is counted and tracked as unjournaled, and the retry journals it before acknowledging', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const mu = mock.method(fs, 'utimesSync', () => { throw new Error('EPERM'); }, { times: 1 });
  const ml = mock.method(fs, 'unlinkSync', () => { throw new Error('EBUSY'); }, { times: 1 });
  await assert.rejects(put(srv, 'stuck'));
  mu.mock.restore();
  ml.mock.restore();
  assert.equal(srv.store.unjournaled.has(sha('stuck')), true);
  assert.equal(srv.store.bytes, 5, 'counted in byte accounting');
  assert.deepEqual(await put(srv, 'stuck'), { stored: false });
  assert.deepEqual(readJournal(srv.root).map((r) => r.kind), ['blob']);
});

// ---- H ----
test('H a symlinked top-level directory (tmp) is refused at startup and nothing outside is touched', async () => {
  const outside = tmpDir('outside');
  fs.writeFileSync(path.join(outside, 'victim'), 'must survive');
  const root = tmpDir('root');
  fs.symlinkSync(outside, path.join(root, 'tmp'));
  await assert.rejects(startStore({ root }), /symlink|real directory/);
  assert.equal(fs.readFileSync(path.join(outside, 'victim'), 'utf8'), 'must survive');
});

test('H hashing opens with O_NOFOLLOW: a symlink leaf is refused', async () => {
  const { hashFile } = await import('../src/store/objects.js');
  assert.equal(typeof hashFile, 'function');
  const dir = tmpDir('hash');
  fs.writeFileSync(path.join(dir, 'real'), 'x');
  fs.symlinkSync(path.join(dir, 'real'), path.join(dir, 'link'));
  await assert.rejects(async () => hashFile(path.join(dir, 'link')), /ELOOP|symbolic/i);
});

// ---- I ----
test('I a replica (no downstream target) keeps no pending journal tail, however many appends', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  for (let i = 0; i < 20; i += 1) await put(p.primary, `object ${i}`);
  assert.equal(p.primary.store.journal.pending.length, 20);
  assert.equal((await replicator(p).runOnce()).ok, true);
  assert.equal(p.replica.store.journal.seq, 20);
  assert.equal(p.replica.store.journal.pending.length, 0);
});
