import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import net from 'node:net';
import path from 'node:path';
import { startStore, fakeClock, snapshotOf, publish, sha, token, tmpDir } from './store-harness.js';
import { Journal, readJournal } from '../src/store/journal.js';
import { Replicator, readReplicationState, replicationLag } from '../src/store/replicate.js';
import { compareStores } from '../src/store/compare.js';
import { sweep } from '../src/store/retention.js';
import { listObjects } from '../src/store/objects.js';
import { materializeSnapshot } from '../src/store/materialize.js';
import { StoreHttpError } from '../src/store/client.js';
import { blobRelPath, manifestRelPath } from '../src/store/ids.js';

const DAY = 24 * 3600 * 1000;
const put = (srv, text) => srv.submit.putBlob(sha(text), Buffer.from(text));
const rejects = (promise, status) => assert.rejects(promise, (e) => e instanceof StoreHttpError && e.status === status, `expected HTTP ${status}`);
const fdCount = () => fs.readdirSync(process.platform === 'linux' ? '/proc/self/fd' : '/dev/fd').length;

async function pair(opts = {}) {
  const clock = fakeClock();
  const primary = await startStore({ clock, ...opts });
  const replica = await startStore({ clock, replicaMode: true });
  return { clock, primary, replica, close: () => Promise.all([primary.close(), replica.close()]) };
}
const replicator = (p, extra = {}) => new Replicator({ store: p.primary.store, replica: p.replica.replicaPeer, now: p.clock.now, ...extra });

// ---- #1 ----
test('#1 an object gone from the primary before it shipped blocks the watermark and the age keeps growing', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  for (const x of ['a', 'b', 'c']) await put(p.primary, x);
  fs.rmSync(path.join(p.primary.root, blobRelPath(sha('b'))));
  const result = await replicator(p).runOnce();
  assert.equal(result.ok, false);
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, 1);
  p.clock.advance(300_000);
  assert.ok(replicationLag(p.primary.store, p.clock.now()).oldestUnreplicatedAgeSeconds >= 300);
});

// ---- #2 ----
test('#2 a short write is completed (bytesWritten honoured), so the stored bytes are the whole blob', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const realOpen = fs.promises.open;
  const m = mock.method(fs.promises, 'open', async (...a) => {
    const fh = await realOpen(...a);
    const realWrite = fh.write.bind(fh);
    fh.write = async (buf, off = 0, len = buf.length - off, pos) => {
      const n = Math.max(1, len >> 1);
      return realWrite(buf, off, n, pos);
    };
    return fh;
  });
  const data = Buffer.from('a blob long enough to be split by the fault injector');
  try {
    await srv.submit.putBlob(sha(data), data);
  } finally {
    m.mock.restore();
  }
  assert.equal(fs.readFileSync(path.join(srv.root, blobRelPath(sha(data)))).toString(), data.toString());
});

test('#2 a write that lies about its length is caught by hashing what is on disk, not the incoming stream', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const realOpen = fs.promises.open;
  const m = mock.method(fs.promises, 'open', async (...a) => {
    const fh = await realOpen(...a);
    const realWrite = fh.write.bind(fh);
    fh.write = async (buf, off = 0, len = buf.length - off, pos) => {
      await realWrite(buf, off, Math.max(0, len - 1), pos); // drops the last byte but reports a full write
      return { bytesWritten: len, buffer: buf };
    };
    return fh;
  });
  const data = Buffer.from('silently truncated on disk');
  try {
    await assert.rejects(srv.submit.putBlob(sha(data), data), (e) => e instanceof StoreHttpError && e.status >= 400);
  } finally {
    m.mock.restore();
  }
  assert.equal(srv.store.hasBlob(sha(data)), false);
});

// ---- #3 ----
test('#3 a failed journal fsync neither burns nor repeats a sequence number, and leaves no torn line', () => {
  const root = tmpDir('journal');
  const j = new Journal(root);
  const rec = (n) => ({ kind: 'blob', path: `blobs/x${n}`, sha256: sha(String(n)), size: 1, createdAt: n });
  j.append(rec(1));
  const m = mock.method(fs, 'fsyncSync', () => { throw new Error('EIO'); }, { times: 1 });
  assert.throws(() => j.append(rec(2)), /EIO/);
  m.mock.restore();
  j.append(rec(3));
  j.append(rec(4));
  assert.deepEqual(readJournal(root).map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(readJournal(root).map((r) => r.createdAt), [1, 3, 4]);
  assert.equal(fs.readFileSync(path.join(root, 'journal.log'), 'utf8').split('\n').length, 4);
  j.close();
});

test('#3 a short journal write is looped to completion', () => {
  const root = tmpDir('journal');
  const j = new Journal(root);
  const real = fs.writeSync;
  const m = mock.method(fs, 'writeSync', (fd, buf, off = 0, len = buf.length - off, pos) => real(fd, buf, off, Math.min(len, 7), pos));
  j.append({ kind: 'blob', path: 'blobs/y', sha256: sha('y'), size: 1, createdAt: 1 });
  m.mock.restore();
  assert.ok(m.mock.callCount() > 1, 'the fault injector really split the write');
  assert.equal(readJournal(root).length, 1);
  j.close();
});

// ---- #4 ----
test('#4 crash after blob rename, restart, retry, manifest, replicate: the orphan is journaled before the manifest and everything ships', async (t) => {
  const clock = fakeClock();
  let primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true });
  t.after(() => replica.close());
  const snap = snapshotOf({ 'f.txt': 'orphaned by a crash' });
  const [h, buf] = [...snap.blobs][0];
  const root = primary.root;
  await primary.close();
  const orphan = path.join(root, blobRelPath(h)); // the crash: renamed into place, journal append never happened
  fs.mkdirSync(path.dirname(orphan), { recursive: true });
  fs.writeFileSync(orphan, buf);

  primary = await startStore({ clock, root });
  t.after(() => primary.close());
  assert.deepEqual(await primary.submit.putBlob(h, buf), { stored: false }, 'retry deduplicates');
  await primary.submit.putManifest('job-1', snap.manifest);
  const journal = readJournal(root);
  const blobSeq = journal.find((r) => r.path === blobRelPath(h))?.seq;
  const manifestSeq = journal.find((r) => r.path === manifestRelPath('job-1'))?.seq;
  assert.ok(blobSeq && manifestSeq && blobSeq < manifestSeq, 'blob journaled before the manifest that references it');
  const result = await new Replicator({ store: primary.store, replica: replica.replicaPeer, now: clock.now }).runOnce();
  assert.equal(result.ok, true, result.error);
  assert.equal((await compareStores({ store: primary.store, replica: replica.replicaPeer, deep: true })).ok, true);
});

test('#4 a failed journal append rolls the blob back, so the retry stores and journals it', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const m = mock.method(srv.store.journal, 'append', () => { throw new Error('EIO'); }, { times: 1 });
  await assert.rejects(put(srv, 'rolled back'), (e) => e instanceof StoreHttpError && e.status >= 500);
  m.mock.restore();
  assert.equal(srv.store.hasBlob(sha('rolled back')), false);
  assert.deepEqual(await put(srv, 'rolled back'), { stored: true });
  assert.equal(readJournal(srv.root).length, 1);
});

test('#4 an existing-but-unjournaled blob is journaled by the dedupe path, and before a manifest that references it', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const snap = snapshotOf({ 'g.txt': 'unjournaled survivor' });
  const [h, buf] = [...snap.blobs][0];
  const failAppend = mock.method(srv.store.journal, 'append', () => { throw new Error('EIO'); }, { times: 1 });
  const failUnlink = mock.method(fs, 'unlinkSync', () => { throw new Error('EBUSY'); }, { times: 1 });
  await assert.rejects(srv.submit.putBlob(h, buf));
  failAppend.mock.restore();
  failUnlink.mock.restore();
  assert.equal(srv.store.hasBlob(h), true, 'rollback failed, so the blob is on disk without a record');
  assert.equal(readJournal(srv.root).length, 0);
  await srv.submit.putManifest('job-g', snap.manifest); // registration must journal the blob first
  assert.deepEqual(readJournal(srv.root).map((r) => r.kind), ['blob', 'manifest']);
});

// ---- #5 / #9 ----
test('#5 a replica sweep between blob verification and manifest delivery deletes nothing', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  const snap = snapshotOf({ 'inflight.txt': 'acknowledged before its manifest' });
  await publish(p.primary, 'job-1', snap);
  let failManifest = true;
  const flaky = Object.create(p.replica.replicaPeer);
  flaky.putObject = (e, f) => {
    if (e.kind === 'manifest' && failManifest) throw new Error('replica unreachable');
    return p.replica.replicaPeer.putObject(e, f);
  };
  assert.equal((await replicator(p, { replica: flaky }).runOnce()).ok, false);
  p.clock.advance(2 * DAY);
  sweep(p.replica.store);
  await p.replica.admin.json('POST', '/admin/sweep');
  const [h] = snap.blobs.keys();
  assert.equal(p.replica.store.hasBlob(h), true, 'the replica never runs its own unreferenced-blob sweep');
  failManifest = false;
  assert.equal((await replicator(p, { replica: flaky }).runOnce()).ok, true);
  assert.ok(p.replica.store.readManifest('job-1'));
});

test('#9 repeated complete-and-expire cycles keep replica storage bounded: its own local retention, deletions are never shipped', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  for (let i = 0; i < 4; i += 1) {
    await publish(p.primary, `job-${i}`, snapshotOf({ [`f${i}.txt`]: `payload ${i}` }));
    assert.equal((await replicator(p).runOnce()).ok, true);
    await p.primary.admin.json('PUT', `/jobs/job-${i}/terminal`);
    p.clock.advance(15 * DAY);
    sweep(p.primary.store);
    const r = await replicator(p).runOnce();
    assert.equal(r.ok, true, r.error);
    sweep(p.replica.store); // the replica's own time-based retention (7-day grace; everything here is older)
    const mine = (await listObjects(p.primary.root)).map((o) => o.path);
    const theirs = (await listObjects(p.replica.root)).map((o) => o.path);
    assert.deepEqual(theirs, mine, `cycle ${i}: both stores swept`);
    assert.ok(theirs.length <= 2, `cycle ${i}: bounded (${theirs.length} objects)`);
  }
  assert.equal(p.replica.store.bytes, p.primary.store.bytes);
});

test('#9 terminal marks and pins replicate, so a promoted replica keeps the primary\'s retention state', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await publish(p.primary, 'job-1', snapshotOf({ 'a.txt': 'a' }));
  await p.primary.admin.json('PUT', '/jobs/job-1/terminal');
  await p.primary.admin.json('PUT', '/pins/job-1');
  assert.equal((await replicator(p).runOnce()).ok, true);
  const m = p.replica.store.meta('job-1');
  assert.equal(m.pinned, true);
  assert.equal(m.terminalAt, p.primary.store.meta('job-1').terminalAt);
});

// ---- #6 ----
const bigBlob = async (srv, mb) => {
  const buf = Buffer.alloc(mb * 1024 * 1024, 5);
  const snap = { blobs: new Map([[sha(buf), buf]]), manifest: null };
  const entries = [{ path: 'big.bin', type: 'file', exec: false, size: buf.length, sha256: sha(buf) }];
  const { manifestHashOf } = await import('../src/remote-manifest.js');
  snap.manifest = { manifestHash: manifestHashOf(entries), entries };
  await publish(srv, 'big', snap);
  return sha(buf);
};

const rawGet = (srv, h, tok, { pause = false } = {}) => new Promise((resolve) => {
  const port = Number(new URL(srv.url).port);
  const s = net.connect(port, '127.0.0.1', () => {
    s.write(`GET /blobs/${h} HTTP/1.1\r\nhost: x\r\nauthorization: Bearer ${tok}\r\n\r\n`);
    if (pause) s.pause();
    resolve(s);
  });
  s.on('error', () => {});
});

test('#6 an aborted download releases its file descriptor', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const h = await bigBlob(srv, 24);
  const tok = token({ role: 'read', job: 'big' }, { clock: srv.clock });
  const baseline = fdCount();
  for (let i = 0; i < 8; i += 1) {
    const s = await rawGet(srv, h, tok);
    await new Promise((r) => s.once('data', r));
    s.destroy();
  }
  await new Promise((r) => setTimeout(r, 700));
  assert.ok(fdCount() <= baseline + 2, `fds ${fdCount()} vs baseline ${baseline}`);
});

test('#6 a stalled reader is evicted within the deadline and frees its download slot', async (t) => {
  const srv = await startStore({ maxDownloads: 1, downloadIdleMs: 300 });
  t.after(() => srv.close());
  const h = await bigBlob(srv, 64);
  const tok = token({ role: 'read', job: 'big' }, { clock: srv.clock });
  const stalled = await rawGet(srv, h, tok, { pause: true });
  t.after(() => stalled.destroy());
  await new Promise((r) => setTimeout(r, 150));
  await rejects(srv.reader('big').getBlob(h), 503);
  await new Promise((r) => setTimeout(r, 1500));
  assert.equal((await srv.reader('big').getBlob(h)).length, 64 * 1024 * 1024);
});

// ---- #7 / #8 ----
test('#7 manifest bytes count toward the cap: registering manifests alone stops at the cap', async (t) => {
  const srv = await startStore({ capBytes: 20_000 });
  t.after(() => srv.close());
  let refused = 0;
  for (let i = 0; i < 40; i += 1) {
    const snap = snapshotOf(Object.fromEntries(Array.from({ length: 4 }, (_, k) => [`dir/file-${i}-${k}.txt`, 'same'])));
    try {
      await srv.submit.putBlob(sha('same'), Buffer.from('same'));
      await srv.submit.putManifest(`job-${i}`, snap.manifest);
    } catch (e) {
      assert.equal(e.status, 507);
      refused += 1;
    }
  }
  assert.ok(refused > 0, 'admission stopped');
  const onDisk = (await listObjects(srv.root)).reduce((n, o) => n + o.size, 0);
  assert.ok(onDisk <= 20_000, `on disk ${onDisk}`);
  assert.equal(srv.store.bytes, onDisk);
});

test('#8 concurrent uploads reserve capacity atomically and never exceed the cap', async (t) => {
  const srv = await startStore({ capBytes: 1000 });
  t.after(() => srv.close());
  await srv.submit.putBlob(sha(Buffer.alloc(900, 1)), Buffer.alloc(900, 1));
  const results = await Promise.allSettled([2, 3, 4].map((n) => srv.submit.putBlob(sha(Buffer.alloc(40, n)), Buffer.alloc(40, n))));
  assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1);
  assert.ok(srv.store.bytes <= 1000);
  assert.equal(srv.store.reserved, 0, 'reservations are released');
});

test('#8 manifest fields outside the schema are rejected', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const snap = snapshotOf({ 'a.txt': 'a' });
  await put(srv, 'a');
  await rejects(srv.submit.putManifest('j1', { ...snap.manifest, padding: 'x'.repeat(1000) }), 400);
  const entries = snap.manifest.entries.map((e) => ({ ...e, note: 'extra' }));
  await rejects(srv.submit.putManifest('j2', { manifestHash: snap.manifest.manifestHash, entries }), 400);
  assert.deepEqual(await srv.submit.putManifest('j3', snap.manifest), { stored: true });
});

// ---- #10 ----
test('#10 a planted leaf symlink and a planted ancestor symlink are refused, and nothing is read or written through them', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const outside = tmpDir('outside');
  fs.writeFileSync(path.join(outside, 'secret'), 'outside the store');
  const snap = snapshotOf({ 'a.txt': 'inside' });
  await publish(srv, 'job-a', snap);
  const [h] = snap.blobs.keys();
  const leaf = path.join(srv.root, blobRelPath(h));
  fs.rmSync(leaf);
  fs.symlinkSync(path.join(outside, 'secret'), leaf);
  await assert.rejects(srv.reader('job-a').getBlob(h), (e) => e instanceof StoreHttpError && e.status >= 400);
  await assert.rejects(srv.replicaPeer.verifyObject(blobRelPath(h)));

  const evil = Buffer.from('written through an ancestor');
  const dir = path.join(srv.root, 'blobs', sha(evil).slice(0, 2));
  fs.symlinkSync(outside, dir);
  await assert.rejects(srv.submit.putBlob(sha(evil), evil), (e) => e instanceof StoreHttpError && e.status >= 400);
  assert.deepEqual(fs.readdirSync(outside), ['secret'], 'nothing written outside the store');
});

test('#10 the store root is confined by realpath at startup', async (t) => {
  const real = tmpDir('realroot');
  const link = path.join(tmpDir('linkparent'), 'root');
  fs.symlinkSync(real, link);
  const srv = await startStore({ root: link });
  t.after(() => srv.close());
  assert.equal(srv.store.root, fs.realpathSync(real));
});

// ---- #11 ----
test('#11 /metrics never reads the journal file, and the age still comes out right', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'one');
  p.clock.advance(90_000);
  const touched = [];
  const spy = (name) => {
    const real = fs[name];
    return mock.method(fs, name, (...a) => {
      if (String(a[0]).includes('journal.log')) touched.push(name);
      return real(...a);
    });
  };
  const spies = ['readFileSync', 'openSync', 'readSync', 'createReadStream'].map(spy);
  let text;
  try {
    text = await (await fetch(`${p.primary.url}/metrics`)).text();
  } finally {
    spies.forEach((s) => s.mock.restore());
  }
  assert.deepEqual(touched, []);
  assert.match(text, /^oldest_unreplicated_object_age_seconds 90$/m);
  await replicator(p).runOnce();
  assert.match(await (await fetch(`${p.primary.url}/metrics`)).text(), /^oldest_unreplicated_object_age_seconds 0$/m);
});

test('#11 the watermark records a byte offset and the replicator reads forward in bounded chunks', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  for (let i = 0; i < 25; i += 1) await put(p.primary, `object ${i}`);
  const r = await replicator(p, { chunkBytes: 600 }).runOnce();
  assert.equal(r.ok, true, r.error);
  const state = readReplicationState(p.primary.root);
  assert.equal(state.replicatedSeq, 25);
  assert.equal(state.replicatedOffset, fs.statSync(path.join(p.primary.root, 'journal.log')).size);
  await put(p.primary, 'after');
  const again = await replicator(p, { chunkBytes: 600 }).runOnce();
  assert.equal(again.shipped, 1);
});

// ---- #12 ----
test('#12 a 1.6 MB manifest registers and materializes with the default limits', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const files = {};
  for (let i = 0; i < 11_000; i += 1) files[`packages/some/deeply/nested/directory/structure/file-number-${i}.txt`] = 'same bytes';
  const snap = snapshotOf(files);
  assert.ok(JSON.stringify(snap.manifest).length > 1_600_000);
  await publish(srv, 'big-manifest', snap);
  const reader = srv.reader('big-manifest');
  const manifest = await reader.getManifest('big-manifest');
  const res = await materializeSnapshot({ manifest, destDir: path.join(tmpDir(), 'w'), readBlob: (h) => reader.getBlob(h) });
  assert.deepEqual(res, { ok: true });
});
