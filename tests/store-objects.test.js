import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { startStore, snapshotOf, publish, sha, token, SECRET } from './store-harness.js';
import { StoreHttpError } from '../src/store/client.js';
import { StoreClient } from '../src/store/client.js';
import { readJournal } from '../src/store/journal.js';
import { blobRelPath } from '../src/store/ids.js';
import { createHmacVerifier, signToken } from '../src/store/auth.js';

async function rejects(promise, status) {
  await assert.rejects(promise, (err) => err instanceof StoreHttpError && err.status === status, `expected HTTP ${status}`);
}

test('upload deduplicates: a second upload of the same blob stores nothing and adds no journal record', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const data = Buffer.from('hello world');
  assert.deepEqual(await srv.submit.putBlob(sha(data), data), { stored: true });
  assert.deepEqual(await srv.submit.putBlob(sha(data), data), { stored: false });
  assert.deepEqual(await srv.submit.has([sha(data), sha('other')]), [sha('other')]);
  assert.equal(readJournal(srv.root).length, 1);
  const h = sha(data);
  assert.ok(fs.existsSync(path.join(srv.root, 'blobs', h.slice(0, 2), h.slice(2, 4), h)), '2-level fan-out layout');
});

test('re-upload is immutable: different bytes under an existing address are refused and the stored blob is untouched', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const data = Buffer.from('original');
  await srv.submit.putBlob(sha(data), data);
  const file = path.join(srv.root, blobRelPath(sha(data)));
  const before = fs.statSync(file);
  await rejects(srv.submit.putBlob(sha(data), Buffer.from('tampered')), 422);
  assert.equal(fs.readFileSync(file, 'utf8'), 'original');
  assert.equal(fs.statSync(file).ino, before.ino);
});

test('a body whose hash does not match its address is refused and leaves nothing behind', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  await rejects(srv.submit.putBlob(sha('claimed'), Buffer.from('actual')), 422);
  assert.equal(srv.store.hasBlob(sha('claimed')), false);
  assert.equal(readJournal(srv.root).length, 0);
  assert.deepEqual(fs.readdirSync(path.join(srv.root, 'tmp')), []);
});

test('the size cap refuses an oversize blob (declared and streamed) and stores nothing', async (t) => {
  const srv = await startStore({ maxBlobBytes: 1024 });
  t.after(() => srv.close());
  const big = Buffer.alloc(2048, 7);
  await rejects(srv.submit.putBlob(sha(big), big), 413);
  assert.equal(srv.store.hasBlob(sha(big)), false);
  // streamed past the cap even though the declared length lies low is impossible (http framing), so also check the boundary:
  const ok = Buffer.alloc(1024, 7);
  assert.deepEqual(await srv.submit.putBlob(sha(ok), ok), { stored: true });
});

test('uploads are refused at 95% of the store cap', async (t) => {
  const srv = await startStore({ capBytes: 1000 });
  t.after(() => srv.close());
  const a = Buffer.alloc(900, 1);
  await srv.submit.putBlob(sha(a), a);
  const b = Buffer.alloc(100, 2);
  await rejects(srv.submit.putBlob(sha(b), b), 507);
});

test('job-scoped read: own manifest blobs are readable, another job\'s blob is refused', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const a = snapshotOf({ 'a.txt': 'job A secret' });
  const b = snapshotOf({ 'b.txt': 'job B file' });
  await publish(srv, 'job-a', a);
  await publish(srv, 'job-b', b);
  const [shaA] = a.blobs.keys();
  const [shaB] = b.blobs.keys();
  const readA = srv.reader('job-a');
  assert.equal((await readA.getBlob(shaA)).toString(), 'job A secret');
  await rejects(readA.getBlob(shaB), 403);
  await rejects(readA.getManifest('job-b'), 403);
  assert.deepEqual((await readA.getManifest('job-a')).manifestHash, a.manifest.manifestHash);
  await rejects(srv.reader('job-unregistered').getBlob(shaA), 403);
  assert.equal((await srv.replicaPeer.getBlob(shaB)).toString(), 'job B file', 'replica role reads anything');
});

test('tokens: no token, a forged token, an expired token and the wrong role are all refused', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const data = Buffer.from('x');
  await rejects(new StoreClient({ baseUrl: srv.url, token: 'nope' }).putBlob(sha(data), data), 401);
  const forged = signToken('some other secret', { role: 'admin', exp: Math.floor(Date.now() / 1000) + 600 });
  await rejects(new StoreClient({ baseUrl: srv.url, token: forged }).putBlob(sha(data), data), 401);
  const expired = signToken(SECRET, { role: 'submit', exp: Math.floor(srv.clock.now() / 1000) - 5 });
  await rejects(new StoreClient({ baseUrl: srv.url, token: expired }).putBlob(sha(data), data), 401);
  await rejects(srv.reader('j').putBlob(sha(data), data), 403);
  await rejects(srv.client({ role: 'submit', job: 'j' }).list(), 403);
});

test('a manifest must match its hash, reference only stored blobs, and is immutable per job', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const snap = snapshotOf({ 'f.txt': 'content' });
  await rejects(srv.submit.putManifest('j1', snap.manifest), 409); // blob not uploaded yet
  await publish(srv, 'j1', snap);
  assert.deepEqual(await srv.submit.putManifest('j1', snap.manifest), { stored: false });
  await rejects(srv.submit.putManifest('j1', snapshotOf({ 'other.txt': 'x' }).manifest), 409);
  await rejects(srv.submit.putManifest('j2', { ...snap.manifest, manifestHash: sha('bogus') }), 422);
  await rejects(srv.submit.putManifest('.hidden', snap.manifest), 400);
  const bad = snapshotOf({ 'ok.txt': 'x' });
  bad.manifest.entries[0].path = '../escape';
  await rejects(srv.submit.putManifest('j3', bad.manifest), 400);
});

test('the store refuses to bind a wildcard address', async () => {
  const { createStore } = await import('../src/store/server.js');
  const s = await createStore({ root: fs.mkdtempSync(path.join((await import('node:os')).tmpdir(), 'ls-')), verifier: createHmacVerifier(SECRET) });
  await assert.rejects(s.listen('0.0.0.0', 0), /wildcard/);
  await assert.rejects(s.listen('::', 0), /wildcard/);
  await s.close();
});

test('the journal survives a restart and drops a torn trailing append', async (t) => {
  const srv = await startStore();
  const data = Buffer.from('persist');
  await srv.submit.putBlob(sha(data), data);
  const root = srv.root;
  await srv.close();
  fs.appendFileSync(path.join(root, 'journal.log'), '{"seq":2,"kind":"bl'); // crash mid-append
  const again = await startStore({ root });
  t.after(() => again.close());
  const more = Buffer.from('after restart');
  await again.submit.putBlob(sha(more), more);
  assert.deepEqual(readJournal(root).map((r) => r.seq), [1, 2]);
});

test('/metrics is served and reports the replication age gauge', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const res = await fetch(`${srv.url}/metrics`);
  const text = await res.text();
  assert.match(text, /^oldest_unreplicated_object_age_seconds 0$/m);
});

test('token helper stays in sync with the verifier', async () => {
  assert.deepEqual((await createHmacVerifier(SECRET)(token({ role: 'read', job: 'x' }))).job, 'x');
});
