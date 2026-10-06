import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { startStore, fakeClock, snapshotOf, publish, sha, tmpDir } from './store-harness.js';
import { readJournal } from '../src/store/journal.js';
import { Replicator, readReplicationState } from '../src/store/replicate.js';
import { compareStores } from '../src/store/compare.js';
import { ObjectStore } from '../src/store/objects.js';
import { blobRelPath } from '../src/store/ids.js';
import { StoreHttpError } from '../src/store/client.js';

const put = (srv, text) => srv.submit.putBlob(sha(text), Buffer.from(text));
// `store` is what the in-process replicator needs; `root` keeps the same call valid against the previous (root-reading) design.
const replicator = (primary, replica, clock) => new Replicator({ store: primary.store, root: primary.root, replica: replica.replicaPeer, now: clock.now });
const compare = (primary, replica) => compareStores({ store: primary.store, root: primary.root, replica: replica.replicaPeer });

async function pair() {
  const clock = fakeClock();
  const primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true });
  return { clock, primary, replica, close: () => Promise.all([primary.close(), replica.close()]) };
}

// ---- N7: structure, not interleaving ----
test('N7 no journal marker file exists and the replicator reads the journal only through the in-process committed view', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'a');
  assert.equal(fs.existsSync(path.join(p.primary.root, 'journal.committed')), false);
  const reads = mock.method(p.primary.store.journal, 'read');
  assert.equal((await replicator(p.primary, p.replica, p.clock).runOnce()).ok, true);
  assert.ok(reads.mock.callCount() > 0, 'the replicator went through store.journal.read');
  assert.equal(fs.existsSync(path.join(p.primary.root, 'journal.committed')), false);
});

test('N7 opening a store fsyncs the journal before anything can read it', async (t) => {
  const first = await startStore();
  await put(first, 'x');
  const root = first.root;
  await first.close();
  const real = fs.fsyncSync;
  const synced = [];
  const m = mock.method(fs, 'fsyncSync', (fd) => { synced.push(fd); return real(fd); });
  const again = await startStore({ root });
  m.mock.restore();
  t.after(() => again.close());
  assert.ok(synced.includes(again.store.journal.fd), 'the journal fd was fsynced during open');
});

test('a second ObjectStore on a held root is refused, and an offline tool cannot take it either', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  assert.throws(() => new ObjectStore(srv.root), /in use/);
  const { rebuildWatermark } = await import('../src/store/replicate.js');
  assert.throws(() => rebuildWatermark(srv.root, 0), /in use/);
});

// ---- N8 ----
test('N8 a replicated blob that vanishes from the primary is a LOSS: never tombstoned, the replica copy survives, metric and compare report it', async (t) => {
  const clock = fakeClock();
  let primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true });
  t.after(() => replica.close());
  await put(primary, 'precious');
  const root = primary.root;
  assert.equal((await replicator(primary, replica, clock).runOnce()).ok, true);
  await primary.close();
  fs.rmSync(path.join(root, blobRelPath(sha('precious')))); // accidental loss: no retention intent anywhere
  primary = await startStore({ clock, root });
  t.after(() => primary.close());
  assert.deepEqual(primary.store.reconcileReport.deleted, []);
  assert.deepEqual(primary.store.reconcileReport.lost, [blobRelPath(sha('precious'))]);
  assert.ok(!readJournal(root).some((r) => r.kind === 'delete'), 'no tombstone');
  assert.equal((await replicator(primary, replica, clock).runOnce()).ok, true);
  assert.equal(replica.store.hasBlob(sha('precious')), true, 'the backup is intact');
  assert.match(await (await fetch(`${primary.url}/metrics`)).text(), /^lane_store_lost_objects 1$/m);
  assert.deepEqual((await compare(primary, replica)).lostAtPrimary, [blobRelPath(sha('precious'))]);
  // recovery: re-upload clears it
  await primary.submit.putBlob(sha('precious'), Buffer.from('precious'));
  assert.equal(primary.store.lost.size, 0);
});

test('N8 an unreplicated blob lost from the primary blocks replication of that path instead of being skipped', async (t) => {
  const clock = fakeClock();
  let primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true });
  t.after(() => replica.close());
  await put(primary, 'never shipped');
  const root = primary.root;
  await primary.close();
  fs.rmSync(path.join(root, blobRelPath(sha('never shipped'))));
  primary = await startStore({ clock, root });
  t.after(() => primary.close());
  const r = await replicator(primary, replica, clock).runOnce();
  assert.equal(r.ok, false);
  assert.equal(readReplicationState(root).replicatedSeq, 0);
  assert.ok(!readJournal(root).some((x) => x.kind === 'delete'));
});

// ---- N10 / intent ----
test('N10 a delete whose final record failed is completed by the periodic maintenance, with no further uploads', async (t) => {
  const srv = await startStore({ maintenanceIntervalMs: 40 });
  t.after(() => srv.close());
  await put(srv, 'retired');
  const real = srv.store.journal.append.bind(srv.store.journal);
  let failed = false;
  const m = mock.method(srv.store.journal, 'append', (f) => {
    if (f.kind === 'delete' && !failed) { failed = true; throw new Error('EIO'); }
    return real(f);
  });
  assert.throws(() => srv.store.deleteObject(blobRelPath(sha('retired'))), /EIO/);
  assert.equal(srv.store.pendingDeletes.size, 1);
  await new Promise((r) => setTimeout(r, 400));
  m.mock.restore();
  assert.equal(srv.store.pendingDeletes.size, 0);
  assert.deepEqual(readJournal(srv.root).map((r) => r.kind), ['blob', 'delete-intent', 'delete']);
});

test('N10 every append, including a terminal mark, flushes a pending delete first', async (t) => {
  const srv = await startStore({ maintenanceIntervalMs: 0 });
  t.after(() => srv.close());
  await publish(srv, 'job-1', snapshotOf({ 'a.txt': 'a' }));
  await put(srv, 'retired');
  const real = srv.store.journal.append.bind(srv.store.journal);
  let failed = false;
  const m = mock.method(srv.store.journal, 'append', (f) => {
    if (f.kind === 'delete' && !failed) { failed = true; throw new Error('EIO'); }
    return real(f);
  });
  assert.throws(() => srv.store.deleteObject(blobRelPath(sha('retired'))), /EIO/);
  m.mock.restore();
  await srv.admin.json('PUT', '/jobs/job-1/terminal');
  const kinds = readJournal(srv.root).map((r) => r.kind);
  assert.ok(kinds.indexOf('delete') !== -1 && kinds.indexOf('delete') < kinds.indexOf('terminal'), kinds.join(','));
});

// ---- N9 ----
test('N9 a retried upload of a blob the store already holds succeeds without reserving capacity', async (t) => {
  const srv = await startStore({ capBytes: 1000 });
  t.after(() => srv.close());
  const big = Buffer.alloc(900, 7);
  assert.deepEqual(await srv.submit.putBlob(sha(big), big), { stored: true });
  assert.deepEqual(await srv.submit.putBlob(sha(big), big), { stored: false });
  assert.equal(srv.store.reserved, 0);
  await assert.rejects(srv.submit.putBlob(sha(big), Buffer.alloc(900, 8)), (e) => e instanceof StoreHttpError && e.status === 422, 'a wrong body is still refused');
});

// ---- #10 ----
test('#10 tmp/ swapped for a symlink after startup is caught before the next upload creates a temp file', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const outside = tmpDir('outside');
  fs.rmSync(path.join(srv.root, 'tmp'), { recursive: true });
  fs.symlinkSync(outside, path.join(srv.root, 'tmp'));
  await assert.rejects(put(srv, 'x'), (e) => e instanceof StoreHttpError && e.status >= 400);
  assert.deepEqual(fs.readdirSync(outside), []);
  assert.equal(srv.store.reserved, 0);
});
