import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { repoIdentity } from '../src/config.js';
import { blobIndexFile, blobKey, uploadSnapshot } from '../src/pull-snapshot.js';
import { blobNamespace } from '../src/remote-blobs.js';
import { buildExecHeader } from '../src/remote-client.js';
import { buildManifest } from '../src/remote-manifest.js';
import { createS3Client, S3Error } from '../src/s3-client.js';
import { sha256Hex } from '../src/s3-presign.js';
import { freshEnv } from './helpers.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './helpers/s3-stub.js';
import { makeDispatchArgs, makeGitWorktree } from './remote-harness.js';

/** BRAIN-1036 R4: the snapshot a pull ticket points at, uploaded to the S3 stub, which verifies every signature. */

const DAY = 24 * 3_600_000;
const BIG_A = Buffer.alloc(20_000, 'a');
const BIG_B = Buffer.alloc(9_000, 'b');
const FILES = { 'big-a.bin': BIG_A, 'dup-a.bin': BIG_A, 'big-b.bin': BIG_B, 'small.txt': 'hi\n' };
const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

async function fixture(t, { stubOptions = {}, files = FILES } = {}) {
  const clock = { now: Date.parse('2026-11-12T00:00:00Z') };
  const stub = await startS3Stub({ now: () => clock.now, ...stubOptions });
  t.after(() => stub.close());
  const s3 = createS3Client({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS, now: () => clock.now });
  const { state } = freshEnv();
  const worktreeRoot = makeGitWorktree(files);
  const manifest = buildManifest(worktreeRoot);
  const args = makeDispatchArgs();
  const header = buildExecHeader({ runner: { name: 'pull' }, ...args });
  const upload = (options = {}) => uploadSnapshot({
    s3, root: state, worktreeRoot, header, manifest, ticketId: args.ticketId, generation: args.generation, blobCacheMinBytes: 0, now: () => clock.now, ...options,
  });
  return { clock, stub, s3, state, worktreeRoot, manifest, args, header, upload };
}

/** The header line and, per manifest entry, the frame and its inline body (null for a cached frame). */
function decodeStream(gz) {
  const raw = zlib.gunzipSync(gz);
  let at = 0;
  const line = () => {
    const end = raw.indexOf(10, at);
    const text = raw.subarray(at, end).toString('utf8');
    at = end + 1;
    return JSON.parse(text);
  };
  const header = line();
  const frames = [];
  for (let frame = line(); !frame.end; frame = line()) {
    let body = null;
    if (frame.type === 'file' && frame.body !== 'cached') {
      body = raw.subarray(at, at + frame.size);
      at += frame.size;
    }
    frames.push({ frame, body });
  }
  return { header, frames };
}

const blobObjects = (stub) => [...stub.objects.keys()].filter((name) => name.startsWith('lane-snap/blobs/'));
const puts = (stub, prefix) => stub.requests.filter((r) => r.method === 'PUT' && r.path.startsWith(prefix));

test('the descriptor matches the stream, and blobs lists exactly the cached frames', async (t) => {
  const { stub, worktreeRoot, args, upload } = await fixture(t);
  const { snapshot, header } = await upload();
  const ns = blobNamespace(repoIdentity(worktreeRoot));

  const stream = stub.objects.get(`lane-snap/${snapshot.key}`).body;
  assert.equal(snapshot.key, `streams/${args.ticketId}-${args.generation}-${sha256Hex(stream)}.gz`);
  assert.equal(snapshot.sha256, sha256Hex(stream));
  assert.equal(snapshot.bytes, stream.length);

  const { header: streamHeader, frames } = decodeStream(stream);
  const cachedHashes = [...new Set(frames.filter((f) => f.frame.body === 'cached').map((f) => streamHeader.manifest.entries.find((e) => e.path === f.frame.path).sha256))].sort();
  assert.deepEqual(snapshot.blobs, [sha(BIG_A), sha(BIG_B)].sort().map((hash) => ({ ns, sha256: hash, size: hash === sha(BIG_A) ? BIG_A.length : BIG_B.length })));
  assert.deepEqual(snapshot.blobs.map((b) => b.sha256), cachedHashes);
  assert.equal(frames.filter((f) => f.frame.body === 'cached').length, 3, 'every copy of a cached file travels as a cached frame');
  assert.equal(frames.find((f) => f.frame.path === 'small.txt').body.toString(), 'hi\n', 'a small file stays inline');
  assert.deepEqual(blobObjects(stub).sort(), snapshot.blobs.map((b) => `lane-snap/${blobKey(ns, b.sha256)}`).sort());
  for (const blob of snapshot.blobs) assert.equal(sha(stub.objects.get(`lane-snap/${blobKey(ns, blob.sha256)}`).body), blob.sha256);

  assert.equal(header.repoIdentity, repoIdentity(worktreeRoot));
  assert.equal(streamHeader.repoIdentity, repoIdentity(worktreeRoot), 'the stream carries the same identity the ticket header does');
  assert.ok(stub.requests.every((r) => r.problem === null), 'every request carried a valid signature');
});

test('below the cache threshold everything is inline and blobs is empty, with the identity still set', async (t) => {
  const { stub, worktreeRoot, upload } = await fixture(t);
  const { snapshot, header } = await upload({ blobCacheMinBytes: 4 * 1024 * 1024 });
  assert.deepEqual(snapshot.blobs, []);
  assert.deepEqual(blobObjects(stub), []);
  assert.equal(header.repoIdentity, repoIdentity(worktreeRoot));
  assert.ok(decodeStream(stub.objects.get(`lane-snap/${snapshot.key}`).body).frames.every((f) => f.frame.body !== 'cached'));
});

test('HEAD first: a fresh blob is skipped and a missing one is uploaded', async (t) => {
  const { stub, upload } = await fixture(t);
  const first = await upload();
  assert.equal(first.stats.blobsUploaded, 2);
  const [firstBlob] = blobObjects(stub);
  stub.objects.delete(firstBlob);
  const before = puts(stub, '/lane-snap/blobs/').length;
  const second = await upload();
  assert.deepEqual([second.stats.blobsUploaded, second.stats.blobsSkipped], [1, 1]);
  assert.equal(puts(stub, '/lane-snap/blobs/').length - before, 1);
  const third = await upload();
  assert.deepEqual([third.stats.blobsUploaded, third.stats.blobsSkipped], [0, 2]);
});

test('a blob older than 7 days is re-PUT and one younger is not', async (t) => {
  const { stub, clock, upload } = await fixture(t);
  await upload();
  const uploadedAt = clock.now;
  clock.now += 2 * DAY;
  assert.equal((await upload()).stats.blobsUploaded, 0, 'two days old: skipped');
  clock.now = uploadedAt + 8 * DAY;
  assert.equal((await upload()).stats.blobsUploaded, 2, 'eight days old: re-PUT');
  assert.ok([...stub.objects.entries()].filter(([name]) => name.includes('/blobs/')).every(([, o]) => o.lastModified === clock.now), 'the re-PUT refreshed the object');
  assert.equal((await upload()).stats.blobsUploaded, 0);
});

test('without Last-Modified the local index decides, and with neither the blob counts as old', async (t) => {
  const { stub, clock, state, upload } = await fixture(t, { stubOptions: { emitLastModified: false } });
  await upload();
  assert.ok(fs.existsSync(blobIndexFile(state)));
  const uploadedAt = clock.now;
  clock.now += 2 * DAY;
  assert.equal((await upload()).stats.blobsUploaded, 0, 'the index says two days');
  clock.now = uploadedAt + 8 * DAY;
  assert.equal((await upload()).stats.blobsUploaded, 2, 'the index says eight days');
  fs.rmSync(blobIndexFile(state));
  assert.equal((await upload()).stats.blobsUploaded, 2, 'no index entry: treated as old');
  assert.equal(blobObjects(stub).length, 2);
  const index = JSON.parse(fs.readFileSync(blobIndexFile(state), 'utf8'));
  assert.equal(Object.keys(index).length, 2);
  const stale = { ...JSON.parse(fs.readFileSync(blobIndexFile(state), 'utf8')), 'ns/forgotten': clock.now - 30 * DAY };
  fs.writeFileSync(blobIndexFile(state), JSON.stringify(stale));
  clock.now += 20 * DAY;
  await upload();
  assert.equal(Object.keys(JSON.parse(fs.readFileSync(blobIndexFile(state), 'utf8'))).length, 2, 'entries past the refresh age are dropped, not accumulated');
});

test('a blob whose stored size differs is re-PUT even when young', async (t) => {
  const { stub, upload } = await fixture(t);
  await upload();
  const [name] = blobObjects(stub);
  stub.objects.get(name).body = Buffer.from('truncated');
  const again = await upload();
  assert.equal(again.stats.blobsUploaded, 1);
  assert.equal(stub.objects.get(name).body.length > 100, true);
});

test('blob uploads run with bounded concurrency', async (t) => {
  const files = Object.fromEntries(Array.from({ length: 12 }, (_, i) => [`f${i}.bin`, Buffer.alloc(9_000 + i, `${i}`)]));
  const { s3, upload } = await fixture(t, { files });
  let inFlight = 0;
  let peak = 0;
  const slow = { ...s3, headObject: async (...args) => {
    inFlight += 1;
    peak = Math.max(peak, inFlight);
    await new Promise((resolve) => setTimeout(resolve, 25));
    inFlight -= 1;
    return s3.headObject(...args);
  } };
  const { snapshot } = await upload({ s3: slow, blobConcurrency: 3 });
  assert.equal(snapshot.blobs.length, 12);
  assert.ok(peak > 1 && peak <= 3, `peak concurrent HEADs was ${peak}`);
});

test('a file changed since the manifest is never uploaded under its old hash', async (t) => {
  const { stub, worktreeRoot, upload } = await fixture(t);
  fs.writeFileSync(path.join(worktreeRoot, 'big-b.bin'), Buffer.alloc(BIG_B.length, 'z'));
  await assert.rejects(upload(), /changed content on disk/);
  assert.equal(stub.objects.has(`lane-snap/${blobKey(blobNamespace(repoIdentity(worktreeRoot)), sha(BIG_B))}`), false);
  assert.deepEqual([...stub.objects.keys()].filter((name) => name.includes('/streams/')), []);
});

test('a stalled store fails the upload at its deadline, on a HEAD, a blob PUT and the stream PUT', { timeout: 10_000 }, async (t) => {
  const { stub, s3, upload } = await fixture(t);
  stub.hang = true;
  const started = Date.now();
  await assert.rejects(upload({ deadlineMs: 200, putDeadlineMs: 200 }), (err) => err instanceof S3Error && err.code === 'Timeout' && /HEAD/.test(err.message));
  assert.ok(Date.now() - started < 3_000);

  const stallAtPut = { ...s3, putObjectFromFile: async (...args) => {
    stub.hang = true;
    return s3.putObjectFromFile(...args);
  } };
  stub.hang = false;
  await assert.rejects(upload({ s3: stallAtPut, blobConcurrency: 1, deadlineMs: 200, putDeadlineMs: 200 }), (err) => err instanceof S3Error && err.code === 'Timeout' && /PUT .*blobs\//.test(err.message));

  stub.hang = false;
  await upload();
  await assert.rejects(upload({ s3: stallAtPut, deadlineMs: 200, putDeadlineMs: 200 }), (err) => err instanceof S3Error && err.code === 'Timeout' && /PUT .*streams\//.test(err.message));
});
