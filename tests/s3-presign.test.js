import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { presignRequest, presignUrl } from '../src/s3-presign.js';
import { createS3Client, loadS3Credentials, S3Error } from '../src/s3-client.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './helpers/s3-stub.js';
import { makeTmpDir } from './helpers/tmp.js';

/** BRAIN-1036 R2 (T17): hand-rolled SigV4, checked against AWS's published vector and against a stub that verifies signatures. */

// AWS "Authenticating Requests: Using Query Parameters" GET example (virtual-hosted style, so presignRequest, not presignUrl)
const AWS_EXAMPLE_CREDS = { accessKeyId: 'AKIAIOSFODNN7EXAMPLE', secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' };
const AWS_EXAMPLE_SIGNATURE = 'aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404';

test('T17 presign reproduces the AWS documented GET example signature', () => {
  const signed = presignRequest({ method: 'GET', host: 'examplebucket.s3.amazonaws.com', path: '/test.txt', expiresSec: 86400, creds: AWS_EXAMPLE_CREDS, region: 'us-east-1', now: Date.UTC(2013, 4, 24) });
  assert.equal(
    signed.query,
    `X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=${AWS_EXAMPLE_SIGNATURE}`,
  );
});

test('T17 a path-style presigned URL is accepted by the verifying stub, and only for its own method, key and lifetime', async (t) => {
  let clock = Date.parse('2026-11-12T00:00:00Z');
  const stub = await startS3Stub({ now: () => clock });
  t.after(() => stub.close());
  const s3 = createS3Client({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS, now: () => clock });
  const dir = makeTmpDir('presign-', t);
  fs.writeFileSync(path.join(dir, 'obj'), 'payload bytes');
  await s3.putObjectFromFile('lane-out', 'lane-out/t 1/log', path.join(dir, 'obj'));

  const sign = (over = {}) => presignUrl({ method: 'GET', bucket: 'lane-out', key: 'lane-out/t 1/log', expiresSec: 900, creds: STUB_CREDS, endpoint: stub.endpoint, region: STUB_REGION, now: clock, ...over });
  const ok = await fetch(sign());
  assert.equal(ok.status, 200);
  assert.equal(await ok.text(), 'payload bytes');
  assert.equal((await fetch(`${sign()}0`)).status, 403, 'a tampered signature is refused');
  assert.equal((await fetch(sign({ method: 'PUT' }))).status, 403, 'a URL signed for PUT does not serve a GET');
  assert.equal((await fetch(sign().replace('log', 'lag'))).status, 403, 'a URL does not serve another key');
  const stale = sign();
  clock += 901_000;
  assert.equal((await fetch(stale)).status, 403, 'an expired URL is refused');
});

test('presign encodes keys per RFC 3986 and keeps slashes', () => {
  const url = presignUrl({ method: 'PUT', bucket: 'b', key: "a b/c+d'(e)*", expiresSec: 60, creds: STUB_CREDS, endpoint: 'http://h:9000', region: STUB_REGION, now: 0 });
  assert.match(url, /^http:\/\/h:9000\/b\/a%20b\/c%2Bd%27%28e%29%2A\?/);
});

test('presign includes the session token as a signed query parameter when given', () => {
  const url = presignUrl({ method: 'GET', bucket: 'b', key: 'k', expiresSec: 60, creds: { ...STUB_CREDS, sessionToken: 'tok/en' }, endpoint: 'http://h:9000', region: STUB_REGION, now: 0 });
  assert.match(url, /X-Amz-Security-Token=tok%2Fen/);
});

async function clientFor(t, options = {}) {
  const stub = await startS3Stub(options.stub);
  t.after(() => stub.close());
  const s3 = createS3Client({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS, ...options.client });
  return { stub, s3, dir: makeTmpDir('s3c-', t) };
}

test('PUT from a file signs the real payload sha256 and returns it with the size; HEAD and DELETE round-trip', async (t) => {
  const { stub, s3, dir } = await clientFor(t);
  const file = path.join(dir, 'blob');
  fs.writeFileSync(file, Buffer.alloc(300_000, 7));
  const put = await s3.putObjectFromFile('lane-snap', 'blobs/ns/abc', file);
  assert.equal(put.size, 300_000);
  assert.match(put.sha256, /^[0-9a-f]{64}$/);
  assert.equal(stub.objects.get('lane-snap/blobs/ns/abc').body.length, 300_000);
  const head = await s3.headObject('lane-snap', 'blobs/ns/abc');
  assert.equal(head.exists, true);
  assert.equal(head.size, 300_000);
  assert.ok(Number.isFinite(head.lastModified));
  assert.deepEqual(await s3.headObject('lane-snap', 'blobs/ns/missing'), { exists: false });
  await s3.deleteObject('lane-snap', 'blobs/ns/abc');
  assert.equal((await s3.headObject('lane-snap', 'blobs/ns/abc')).exists, false);
  await s3.deleteObject('lane-snap', 'blobs/ns/abc');
  assert.deepEqual(stub.requests.filter((r) => r.problem), [], 'every request verified');
});

test('listObjects pages with continuation tokens and filters by prefix', async (t) => {
  const { stub, s3, dir } = await clientFor(t);
  const file = path.join(dir, 'x');
  fs.writeFileSync(file, 'x');
  const keys = ['streams/a b', 'streams/c&d', 'streams/e', 'streams/f', 'streams/g', 'blobs/h'];
  for (const key of keys) await s3.putObjectFromFile('lane-snap', key, file);
  const listed = await s3.listObjects('lane-snap', { prefix: 'streams/', pageSize: 2 });
  assert.deepEqual(listed.map((o) => o.key), ['streams/a b', 'streams/c&d', 'streams/e', 'streams/f', 'streams/g']);
  assert.ok(listed.every((o) => o.size === 1 && Number.isFinite(o.lastModified)));
  assert.equal(stub.requests.filter((r) => r.method === 'GET').length, 3, 'five keys at two per page is three requests');
});

test('getBucketLifecycle: none configured is no rules, rules are read, an unreadable configuration throws', async (t) => {
  const { stub, s3 } = await clientFor(t);
  assert.deepEqual(await s3.getBucketLifecycle('lane-snap'), { rules: [] });
  stub.lifecycle.set('lane-snap', [{ id: 'expire-streams', prefix: 'streams/' }, { id: 'old', prefix: '', status: 'Disabled' }]);
  assert.deepEqual(await s3.getBucketLifecycle('lane-snap'), {
    rules: [{ id: 'expire-streams', status: 'Enabled', prefix: 'streams/' }, { id: 'old', status: 'Disabled', prefix: '' }],
  });
  stub.lifecycle.set('lane-snap', 'unreadable');
  await assert.rejects(s3.getBucketLifecycle('lane-snap'), (err) => err instanceof S3Error && err.status === 501);
});

test('every operation has a deadline: a server that never answers fails with Timeout', { timeout: 5000 }, async (t) => {
  const { stub, s3, dir } = await clientFor(t, { client: { deadlineMs: 150 } });
  const file = path.join(dir, 'x');
  fs.writeFileSync(file, 'x');
  stub.hang = true;
  const calls = [s3.headObject('b', 'k'), s3.deleteObject('b', 'k'), s3.listObjects('b'), s3.getBucketLifecycle('b'), s3.putObjectFromFile('b', 'k', file, { deadlineMs: 150 })];
  for (const call of calls) await assert.rejects(call, (err) => err instanceof S3Error && err.code === 'Timeout');
});

test('a wrong secret is refused by the verifying stub (a signature that cannot verify fails)', async (t) => {
  const { s3 } = await clientFor(t, { client: { creds: { ...STUB_CREDS, secretAccessKey: 'wrong' } } });
  await assert.rejects(s3.headObject('b', 'k'), (err) => err instanceof S3Error && err.status === 403);
});

test('credentials come from the env file: both variables required, mode 0600, owned by the user', (t) => {
  const dir = makeTmpDir('creds-', t);
  const file = path.join(dir, 'rustfs.env');
  fs.writeFileSync(file, '# comment\nexport AWS_ACCESS_KEY_ID="AKIA1"\nAWS_SECRET_ACCESS_KEY=sec/ret\n', { mode: 0o600 });
  assert.deepEqual(loadS3Credentials(file), { accessKeyId: 'AKIA1', secretAccessKey: 'sec/ret' });
  fs.chmodSync(file, 0o644);
  assert.throws(() => loadS3Credentials(file), /mode 0600/);
  fs.chmodSync(file, 0o600);
  fs.writeFileSync(file, 'AWS_ACCESS_KEY_ID=AKIA1\n');
  assert.throws(() => loadS3Credentials(file), /must define/);
  assert.throws(() => loadS3Credentials(path.join(dir, 'absent')), /unreadable/);
});
