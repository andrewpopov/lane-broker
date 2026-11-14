import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { canonicalQueryString, credentialScope, signatureFor, UNSIGNED_PAYLOAD } from '../src/s3-presign.js';
import { createS3Client, loadS3Credentials, S3Error } from '../src/s3-client.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './helpers/s3-stub.js';
import { assertCanaryFails } from './helpers/mutant-module.js';
import { makeTmpDir } from './helpers/tmp.js';

/** BRAIN-1036 R2 (T17): hand-rolled SigV4, checked against AWS's published vector and against a stub that verifies signatures. Runners sign their own requests, so nothing here presigns. */

// AWS "Authenticating Requests: Using Query Parameters" GET example: the published anchor for the one signer every request here goes through
const AWS_EXAMPLE_SECRET = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY';
const AWS_EXAMPLE_SIGNATURE = 'aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404';

test('T17 signatureFor reproduces the AWS documented GET example signature', () => {
  const stamp = '20130524T000000Z';
  const query = canonicalQueryString({
    'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
    'X-Amz-Credential': `AKIAIOSFODNN7EXAMPLE/${credentialScope(stamp, 'us-east-1')}`,
    'X-Amz-Date': stamp,
    'X-Amz-Expires': 86400,
    'X-Amz-SignedHeaders': 'host',
  });
  const canonicalRequest = ['GET', '/test.txt', query, 'host:examplebucket.s3.amazonaws.com\n', 'host', UNSIGNED_PAYLOAD].join('\n');
  assert.equal(signatureFor({ secretAccessKey: AWS_EXAMPLE_SECRET, stamp, region: 'us-east-1', canonicalRequest }), AWS_EXAMPLE_SIGNATURE);
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

test('canonicalQueryString sorts by encoded name and encodes values, whatever order the params arrive in (hard-coded expectation)', () => {
  assert.equal(canonicalQueryString({ 'max-keys': 2, 'list-type': 2, prefix: 'a b/' }), 'list-type=2&max-keys=2&prefix=a%20b%2F');
  assert.equal(canonicalQueryString({ b: 1, a: 'z y', 'X-Amz-Date': 'd', Zed: 1 }), 'X-Amz-Date=d&Zed=1&a=z%20y&b=1');
});

async function maxBytesScenario(load, t) {
  const stub = await startS3Stub();
  t.after(() => stub.close());
  const { createS3Client: create } = await load('s3-client.js');
  const s3 = create({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS });
  const dir = makeTmpDir('s3-max-', t);
  stub.objects.set('lane-out/big', { body: Buffer.alloc(100_000, 1), lastModified: Date.now() });
  const refused = await s3.getObjectToFile('lane-out', 'big', path.join(dir, 'big'), { maxBytes: 99_999 });
  assert.deepEqual(refused, { exists: true, tooLarge: true }, 'an object one byte past the bound is abandoned');
  assert.deepEqual(fs.readdirSync(dir), [], 'nothing of it is kept, not even a partial file');
  const whole = await s3.getObjectToFile('lane-out', 'big', path.join(dir, 'big'), { maxBytes: 100_000 });
  assert.equal(whole.size, 100_000, 'an object exactly at the bound is read whole');
}

test('getObjectToFile stops reading an object past maxBytes and keeps nothing; the bound itself is allowed', async (t) => {
  await maxBytesScenario((file) => import(`../src/${file}`), t);
  await assertCanaryFails('s3-client.js', ['done(size > maxBytes ? new OverSize() : null, chunk);', 'done(null, chunk);'], (load) => maxBytesScenario(load, t));
});
