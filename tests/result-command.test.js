import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { freshEnv, laneRun } from './helpers.js';
import { paths } from '../src/state.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { ledgerAccept } from '../src/pull-ledger.js';
import { resultCommand, defaultReaders } from '../src/result.js';

// BRAIN-1198: `lane result <id> --json` is the published, read-only way to read a finished job's result record.

function plantResult(state, record) {
  const id = crypto.randomUUID();
  const dir = paths(state).results;
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${id}.json`), JSON.stringify(record));
  return id;
}

test('result: a finished job prints its record verbatim and exits 0', async () => {
  const { state, env } = freshEnv();
  const id = plantResult(state, { exit: 3, durationMs: 12 });
  const res = await laneRun(['result', id, '--json'], { env });
  assert.equal(res.code, 0, res.stderr);
  assert.deepEqual(JSON.parse(res.stdout), { exit: 3, durationMs: 12 });
  assert.equal(res.stdout.trim().split('\n').length, 1);
});

test('result: a signalled job passes its signal through', async () => {
  const { state, env } = freshEnv();
  const id = plantResult(state, { signal: 'SIGKILL' });
  const res = await laneRun(['result', '--json', id], { env });
  assert.equal(res.code, 0, res.stderr);
  assert.deepEqual(JSON.parse(res.stdout), { signal: 'SIGKILL' });
});

test('result: a reused result keeps its reused marker', async () => {
  const { state, env } = freshEnv();
  const id = plantResult(state, { exit: 0, reused: { of: 'abc' } });
  const res = await laneRun(['result', id, '--json'], { env });
  assert.equal(res.code, 0, res.stderr);
  assert.deepEqual(JSON.parse(res.stdout).reused, { of: 'abc' });
});

test('result: without --json prints one human line', async () => {
  const { state, env } = freshEnv();
  const id = plantResult(state, { exit: 0 });
  const res = await laneRun(['result', id], { env });
  assert.equal(res.code, 0, res.stderr);
  assert.equal(res.stdout, `lane result: ${id} exited 0\n`);
});

test('result: a known job with no result yet exits 82', async () => {
  const { state, env } = freshEnv();
  const id = crypto.randomUUID();
  writeLease(state, { id, key: 'r:default', bootId: 'b', supervisorPid: process.pid, weight: 1, heartbeatAt: Date.now(), state: LEASE_STATE.RUNNING });
  const res = await laneRun(['result', id, '--json'], { env });
  assert.equal(res.code, 82, res.stderr);
  assert.equal(res.stdout, '');
  assert.match(res.stderr, /has not finished yet/);
});

test('result: an unknown id exits 83', async () => {
  const { env } = freshEnv();
  const res = await laneRun(['result', crypto.randomUUID(), '--json'], { env });
  assert.equal(res.code, 83, res.stderr);
  assert.equal(res.stdout, '');
  assert.match(res.stderr, /not a known job/);
});

test('result: a malformed id is a usage error before any file is read', async () => {
  const { state, env } = freshEnv();
  fs.writeFileSync(path.join(path.dirname(paths(state).results), 'secret.json'), JSON.stringify({ exit: 0 }));
  for (const bad of [['../secret'], ['a/b'], [''], [], ['x', 'y'], ['--bogus']]) {
    const res = await laneRun(['result', ...bad, '--json'], { env });
    assert.equal(res.code, 2, `${JSON.stringify(bad)}: ${res.stderr}`);
    assert.equal(res.stdout, '');
  }
});

test('result: reading creates no state', async () => {
  const { state, env } = freshEnv();
  fs.rmSync(state, { recursive: true });
  const res = await laneRun(['result', crypto.randomUUID()], { env });
  assert.equal(res.code, 83, res.stderr);
  assert.equal(fs.existsSync(state), false);
});

function runInProcess(args, state, readers) {
  const out = [];
  const realOut = process.stdout.write;
  const realErr = process.stderr.write;
  process.stdout.write = (chunk) => out.push(String(chunk)) > 0;
  process.stderr.write = () => true;
  try {
    return { ...resultCommand(args, state, readers), stdout: out.join('') };
  } finally {
    process.stdout.write = realOut;
    process.stderr.write = realErr;
  }
}

const noPause = { pause: () => {} };

test('result: a result that lands between the result read and the live check is printed, not 83', () => {
  const { state } = freshEnv();
  const id = crypto.randomUUID();
  let resultReads = 0;
  const res = runInProcess([id, '--json'], state, { ...defaultReaders, ...noPause, result: () => (++resultReads === 1 ? null : { exit: 7 }), live: () => false });
  assert.equal(res.exitCode, 0);
  assert.deepEqual(JSON.parse(res.stdout), { exit: 7 });
});

test('result: a ticket that is in neither place on one pass is found on the next', () => {
  const { state } = freshEnv();
  const id = crypto.randomUUID();
  let resultReads = 0;
  const res = runInProcess([id, '--json'], state, { ...defaultReaders, ...noPause, result: () => (++resultReads >= 3 ? { exit: 0 } : null), live: () => false });
  assert.equal(res.exitCode, 0);
  assert.equal(resultReads, 3);
});

test('result: a result that only appears after the last live check is still printed', () => {
  const { state } = freshEnv();
  const id = crypto.randomUUID();
  let liveChecks = 0;
  const res = runInProcess([id], state, { ...defaultReaders, ...noPause, result: () => (liveChecks >= 3 ? { exit: 0 } : null), live: () => (liveChecks += 1) < 0 });
  assert.equal(res.exitCode, 0);
});

test('result: a job live on a retry pass is 82, and one that never appears stays 83', () => {
  const { state } = freshEnv();
  const id = crypto.randomUUID();
  let checks = 0;
  assert.equal(runInProcess([id], state, { ...defaultReaders, ...noPause, result: () => null, live: () => ++checks === 2 }).exitCode, 82);
  assert.equal(runInProcess([id], state, { ...defaultReaders, ...noPause, result: () => null, live: () => false }).exitCode, 83);
});

test('result: a malformed result file exits 84 naming the file', async () => {
  const { state, env } = freshEnv();
  const id = plantResult(state, {});
  const file = path.join(paths(state).results, `${id}.json`);
  fs.writeFileSync(file, '{not json');
  const res = await laneRun(['result', id, '--json'], { env });
  assert.equal(res.code, 84, res.stderr);
  assert.equal(res.stdout, '');
  assert.ok(res.stderr.includes(file), res.stderr);
});

test('result: an unreadable result file exits 84, not 83', { skip: process.getuid?.() === 0 }, async () => {
  const { state, env } = freshEnv();
  const id = plantResult(state, { exit: 0 });
  const file = path.join(paths(state).results, `${id}.json`);
  fs.chmodSync(file, 0);
  try {
    const res = await laneRun(['result', id, '--json'], { env });
    assert.equal(res.code, 84, res.stderr);
    assert.ok(res.stderr.includes(file), res.stderr);
  } finally {
    fs.chmodSync(file, 0o600);
  }
});

test('result: a malformed lease file exits 84, not 83', async () => {
  const { state, env } = freshEnv();
  const id = crypto.randomUUID();
  writeLease(state, { id, key: 'r:default', bootId: 'b', supervisorPid: process.pid, weight: 1, heartbeatAt: Date.now(), state: LEASE_STATE.RUNNING });
  fs.writeFileSync(path.join(paths(state).leases, `${id}.json`), '{not json');
  const res = await laneRun(['result', id], { env });
  assert.equal(res.code, 84, res.stderr);
});

function snapshotTree(dir) {
  return fs.readdirSync(dir, { recursive: true }).sort();
}

test('result: a ledger-only ticket is 82 and the lookup creates nothing and leaves the ledger directory untouched', async () => {
  const { state, env } = freshEnv();
  const id = crypto.randomUUID();
  ledgerAccept(state, { logicalId: id, epoch: 1, decision: { kind: 'result' } });
  const ledgerDir = paths(state).pullLedger;
  const before = { tree: snapshotTree(state), mtime: fs.statSync(ledgerDir).mtimeMs };
  await new Promise((resolve) => setTimeout(resolve, 20));
  const res = await laneRun(['result', id, '--json'], { env });
  assert.equal(res.code, 82, res.stderr);
  assert.deepEqual(snapshotTree(state), before.tree);
  assert.equal(fs.statSync(ledgerDir).mtimeMs, before.mtime);
});
