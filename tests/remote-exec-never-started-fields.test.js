import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { laneRun, waitFor } from './helpers.js';
import { paths, readJsonSafe, withLock } from '../src/state.js';
import { listQueue, reapOrphanedQueue } from '../src/scheduler.js';
import { makeTmpDir } from './helpers/tmp.js';
import { reapSpawned, setupRunner, makeHeader, execRemote, readExecResult, ticketPath, leaseOf, whenRunning, SLEEP } from './helpers/remote-exec-fixture.js';

/**
 * BRAIN-1035 S2a (T36): which `remote-exec` results carry `neverStarted` / `stopProven`. The flag comes only from explicit pre-spawn
 * provenance; a null `startedAt` (an orphan-lease cancel) never produces it. Both fields appear only when true.
 */

afterEach(reapSpawned);

const fieldsOf = (result) => ({ neverStarted: result.neverStarted, stopProven: result.stopProven, shutdownProof: result.shutdownProof });
const deadPid = () => Number(spawnSync(process.execPath, ['-e', 'process.stdout.write(String(process.pid))']).stdout.toString());

function setupQueuedRunner() {
  const ctx = setupRunner({}, { schedulerMode: 'shadow', sampleMs: 30 });
  fs.writeFileSync(path.join(ctx.state, 'PAUSE'), 'kept busy for the test');
  return ctx;
}

test('rejected before the command (a header refused by validation) and refused at start (a shutdown drain) carry neverStarted', async () => {
  const ctx = setupRunner();
  const rejected = makeHeader('process.exit(0)', { runnerSpeedFactor: 9 });
  await execRemote(ctx, rejected).closed;
  assert.equal(readExecResult(ctx, rejected.ticketId).kind, 'rejected');
  assert.deepEqual(fieldsOf(readExecResult(ctx, rejected.ticketId)), { neverStarted: true, stopProven: undefined, shutdownProof: undefined });

  assert.equal((await laneRun(['drain', '--shutdown', 'test', '--root', ctx.root, '--wait-ms', '500'], { env: ctx.env })).code, 0);
  const refused = makeHeader('process.exit(0)');
  await execRemote(ctx, refused).closed;
  assert.deepEqual(fieldsOf(readExecResult(ctx, refused.ticketId)), { neverStarted: true, stopProven: undefined, shutdownProof: 'refused-at-start' });
});

test('a completed run carries neither field', async () => {
  const ctx = setupRunner();
  const h = makeHeader('process.exit(0)');
  await execRemote(ctx, h).closed;
  const result = readExecResult(ctx, h.ticketId);
  assert.equal(result.kind, 'completed');
  assert.deepEqual(fieldsOf(result), { neverStarted: undefined, stopProven: undefined, shutdownProof: undefined });
});

test('a queue-timeout and a dead-queued-supervisor record carry neverStarted', async () => {
  const timeout = setupQueuedRunner();
  const h = makeHeader('process.exit(0)', { protocol: 2, queueTimeoutMs: 300 });
  await execRemote(timeout, h).closed;
  const expired = readExecResult(timeout, h.ticketId);
  assert.equal(expired.reason, 'queue-timeout');
  assert.equal(expired.neverStarted, true);

  const ctx = setupQueuedRunner();
  const d = makeHeader('process.exit(0)', { protocol: 2, queueTimeoutMs: 120_000 });
  const { closed } = execRemote(ctx, d);
  await waitFor(() => fs.existsSync(ticketPath(ctx, d.ticketId, 'remote-id')));
  const laneId = fs.readFileSync(ticketPath(ctx, d.ticketId, 'remote-id'), 'utf8').trim();
  await waitFor(() => listQueue(ctx.state).some((t) => t.id === laneId && Number.isInteger(t.supervisorPid)));
  await withLock(ctx.state, () => {
    const dir = paths(ctx.state).queue;
    const file = fs.readdirSync(dir).map((n) => path.join(dir, n)).find((f) => readJsonSafe(f)?.id === laneId);
    fs.writeFileSync(file, JSON.stringify({ ...readJsonSafe(file), supervisorPid: deadPid(), supervisorStart: null }));
    reapOrphanedQueue(ctx.state);
  });
  assert.equal((await closed).code, 0);
  const dead = readExecResult(ctx, d.ticketId);
  assert.equal(dead.reason, 'supervisor-died-while-queued');
  assert.equal(dead.neverStarted, true);
});

test('a cancel with a full reap carries stopProven and not neverStarted', async () => {
  const ctx = setupRunner();
  const h = makeHeader(SLEEP);
  const { closed } = execRemote(ctx, h);
  await whenRunning(ctx, h.ticketId);
  assert.equal((await laneRun(['remote-cancel', h.ticketId, '--root', ctx.root], { env: ctx.env })).code, 0);
  await closed;
  const result = readExecResult(ctx, h.ticketId);
  assert.equal(result.kind, 'cancelled');
  assert.deepEqual(fieldsOf(result), { neverStarted: undefined, stopProven: true, shutdownProof: undefined });
});

test('an orphan plain-lease cancel (supervisor SIGKILLed, then lane cancel) has a null startedAt but carries no neverStarted and no stopProven', async () => {
  const gate = makeTmpDir('fields-hold-');
  const hold = { LANE_BROKER_TEST_HOLD_AT: 'remote-exec-before-result', LANE_BROKER_TEST_HOLD_READY: path.join(gate, 'ready'), LANE_BROKER_TEST_HOLD_GO: path.join(gate, 'go') };
  const ctx = setupRunner();
  const h = makeHeader(SLEEP);
  const { closed } = execRemote(ctx, h, { env: { ...ctx.env, ...hold } });
  await whenRunning(ctx, h.ticketId);
  const lease = leaseOf(ctx);
  process.kill(lease.supervisorPid, 'SIGKILL');
  await waitFor(() => fs.existsSync(hold.LANE_BROKER_TEST_HOLD_READY), { timeoutMs: 30_000 });
  const cancel = await laneRun(['cancel', lease.id], { env: ctx.env });
  assert.equal(cancel.code, 0, cancel.stderr);
  const brokerResult = readJsonSafe(path.join(paths(ctx.state).results, `${lease.id}.json`));
  assert.equal(brokerResult.startedAt, null);
  assert.equal(brokerResult.cancelled, true);
  fs.writeFileSync(hold.LANE_BROKER_TEST_HOLD_GO, 'go');
  await closed;
  assert.deepEqual(fieldsOf(readExecResult(ctx, h.ticketId)), { neverStarted: undefined, stopProven: undefined, shutdownProof: undefined });
});
