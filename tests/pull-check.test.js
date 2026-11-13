import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pullCheck } from '../src/pull-check.js';
import { createS3Client } from '../src/s3-client.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './helpers/s3-stub.js';
import { freshEnv, laneRun, writeGlobalConfig } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';

/** BRAIN-1036 R2: `lane pull check` refuses on any enabled lifecycle rule or an unreadable one, and probes where object age comes from. */

const CFG = DEFAULT_GLOBAL_CONFIG;

const fixtureClient = async (stub) => createS3Client({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS });

async function fixture(t, stubOptions = {}) {
  let clock = Date.parse('2026-11-12T00:00:00Z');
  const stub = await startS3Stub({ now: () => clock, ...stubOptions });
  t.after(() => stub.close());
  const s3 = createS3Client({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS, now: () => clock });
  const check = () => pullCheck({ cfg: CFG, s3, settleMs: 1100, sleep: async (ms) => { clock += ms; } });
  return { stub, check };
}

test('no lifecycle rule on either bucket: ready, blobGc on, and the probe object is gone afterwards', async (t) => {
  const { stub, check } = await fixture(t);
  const report = await check();
  assert.equal(report.ok, true);
  assert.deepEqual(report.refusals, []);
  assert.equal(report.blobGc.verdict, 'on');
  assert.deepEqual([...stub.objects.keys()], []);
});

test('any enabled lifecycle rule on either bucket refuses, naming the bucket', async (t) => {
  for (const [bucket, rule] of [
    ['lane-out', { id: 'out-1d', prefix: '' }],
    ['lane-snap', { id: 'snap-all', prefix: '' }],
    ['lane-snap', { id: 'streams-1d', prefix: 'streams/' }],
    ['lane-snap', { id: 'blobs-30d', prefix: 'blobs/' }],
  ]) {
    const { stub, check } = await fixture(t);
    stub.lifecycle.set(bucket, [rule]);
    const report = await check();
    assert.equal(report.ok, false, `${bucket} ${rule.id}`);
    assert.equal(report.refusals.length, 1);
    assert.match(report.refusals[0], new RegExp(`bucket ${bucket} has an enabled lifecycle rule \\(${rule.id}`));
  }
});

test('a disabled rule does not refuse; both buckets with rules refuse twice', async (t) => {
  const first = await fixture(t);
  first.stub.lifecycle.set('lane-out', [{ id: 'paused', prefix: '', status: 'Disabled' }]);
  assert.equal((await first.check()).ok, true);
  const second = await fixture(t);
  second.stub.lifecycle.set('lane-out', [{ id: 'a' }]);
  second.stub.lifecycle.set('lane-snap', [{ id: 'b' }]);
  assert.equal((await second.check()).refusals.length, 2);
});

test('an unreadable lifecycle configuration refuses for that bucket', async (t) => {
  for (const bucket of ['lane-snap', 'lane-out']) {
    const { stub, check } = await fixture(t);
    stub.lifecycle.set(bucket, 'unreadable');
    const report = await check();
    assert.equal(report.ok, false);
    assert.match(report.refusals.join('\n'), new RegExp(`bucket ${bucket} lifecycle configuration is unreadable`));
    assert.equal(report.lifecycle[bucket], null);
  }
});

test('blobGc is off, and the check still passes, when HEAD carries no Last-Modified', async (t) => {
  const { check } = await fixture(t, { emitLastModified: false });
  const report = await check();
  assert.equal(report.ok, true);
  assert.deepEqual(report.blobGc, { verdict: 'off', reason: 'HEAD returns no Last-Modified' });
});

test('blobGc is off when a re-PUT does not refresh Last-Modified', async (t) => {
  const { stub, check } = await fixture(t, { putRefreshesLastModified: false });
  const report = await check();
  assert.equal(report.ok, true);
  assert.deepEqual(report.blobGc, { verdict: 'off', reason: 'a re-PUT does not refresh Last-Modified' });
  assert.deepEqual([...stub.objects.keys()], [], 'the probe object is removed either way');
});

test('HEAD must report the object with a finite Last-Modified, both times, or blobGc is off with the reason', async (t) => {
  const { stub } = await fixture(t);
  let heads = 0;
  const report = await pullCheck({
    cfg: CFG,
    s3: { ...(await fixtureClient(stub)), headObject: async () => (++heads === 1 ? { exists: true, size: 1, lastModified: 1_700_000_000_000 } : { exists: true, size: 1, lastModified: NaN }) },
    settleMs: 0,
    sleep: async () => {},
  });
  assert.deepEqual(report.blobGc, { verdict: 'off', reason: 'a re-PUT does not refresh Last-Modified' });
  const missing = await pullCheck({ cfg: CFG, s3: { ...(await fixtureClient(stub)), headObject: async () => ({ exists: false }) }, settleMs: 0, sleep: async () => {} });
  assert.deepEqual(missing.blobGc, { verdict: 'off', reason: 'HEAD returns no Last-Modified' });
});

test('a probe object that cannot be deleted fails the check and names bucket/key; leftovers from an earlier run are removed first', async (t) => {
  const { stub, check } = await fixture(t);
  stub.objects.set('lane-snap/probes/age-source-leftover', { body: Buffer.from('x'), lastModified: 1 });
  assert.equal((await check()).ok, true);
  assert.deepEqual([...stub.objects.keys()], [], 'the leftover and the new probe are both gone');
  stub.denyDelete = true;
  const report = await check();
  assert.equal(report.ok, false);
  assert.match(report.refusals.join('\n'), /probe object lane-snap\/probes\/age-source-[0-9a-f]{16} could not be deleted/);
});

test('an unreachable store refuses: neither lifecycle nor the age probe can be read', async (t) => {
  const { stub, check } = await fixture(t);
  await stub.close();
  const report = await check();
  assert.equal(report.ok, false);
  assert.equal(report.refusals.length, 3);
  assert.equal(report.blobGc.verdict, 'off');
});

function cliEnv(t, stub, { blobGc = true } = {}) {
  const { home, env } = freshEnv();
  const credentialsEnvFile = path.join(makeTmpDir('pull-check-creds-', t), 'rustfs.env');
  fs.writeFileSync(credentialsEnvFile, `AWS_ACCESS_KEY_ID=${STUB_CREDS.accessKeyId}\nAWS_SECRET_ACCESS_KEY=${STUB_CREDS.secretAccessKey}\n`, { mode: 0o600 });
  writeGlobalConfig(home, { version: 1, lanePull: { blobGc, rustfs: { endpoint: stub.endpoint, credentialsEnvFile } } });
  return env;
}

test('lane pull check: exit 0 and a blobGc verdict when the storage is clean', async (t) => {
  const stub = await startS3Stub();
  t.after(() => stub.close());
  const res = await laneRun(['pull', 'check'], { env: cliEnv(t, stub) });
  assert.equal(res.code, 0, res.stderr + res.stdout);
  assert.match(res.stdout, /blobGc on: /);
  assert.match(res.stdout, /ready/);
});

test('lane pull check: exit 1 with a lifecycle rule, and --json carries the refusal', async (t) => {
  const stub = await startS3Stub();
  t.after(() => stub.close());
  stub.lifecycle.set('lane-out', [{ id: 'out-1d' }]);
  const res = await laneRun(['pull', 'check', '--json'], { env: cliEnv(t, stub) });
  assert.equal(res.code, 1);
  const report = JSON.parse(res.stdout);
  assert.equal(report.ok, false);
  assert.match(report.refusals[0], /lane-out/);
});

test('lane pull check: an off verdict against lanePull.blobGc true says to set it false', async (t) => {
  const stub = await startS3Stub({ emitLastModified: false });
  t.after(() => stub.close());
  const res = await laneRun(['pull', 'check'], { env: cliEnv(t, stub) });
  assert.equal(res.code, 0, res.stderr);
  assert.match(res.stdout, /blobGc off: HEAD returns no Last-Modified \(config has lanePull\.blobGc true: set it to false\)/);
});

test('lane pull check: missing storage config, an unknown argument and an unknown subcommand are usage or config errors', async (t) => {
  const { home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1 });
  const missing = await laneRun(['pull', 'check'], { env });
  assert.equal(missing.code, 1);
  assert.match(missing.stderr, /"lanePull\.rustfs\.endpoint" and "lanePull\.rustfs\.credentialsEnvFile"/);
  assert.equal((await laneRun(['pull', 'check', '--bogus'], { env })).code, 2);
  assert.equal((await laneRun(['pull', 'sweep'], { env })).code, 2);
  assert.equal((await laneRun(['pull'], { env })).code, 2);
});
