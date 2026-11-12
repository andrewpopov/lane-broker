import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, waitFor } from './helpers.js';
import { paths, atomicWriteJson } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';
import { reapSpawned, setupRunner, makeHeader, execRemote, readExecResult, ticketPath, hasLease } from './helpers/remote-exec-fixture.js';

/**
 * BRAIN-1035 S2a (T35 runner half, T35b): a missing lease proves nothing about a shutdown. `runner-shutdown` needs a named proof,
 * and the pre-enqueue window is proven by the supervisor's published `neverStarted`, not by the sweep.
 */

afterEach(reapSpawned);

const shutdown = (ctx) => laneRun(['drain', '--shutdown', 'test shutdown', '--root', ctx.root, '--wait-ms', '1500'], { env: ctx.env });
const remoteIdOf = (ctx, execId) => fs.readFileSync(ticketPath(ctx, execId, 'remote-id'), 'utf8').trim();

test('T35: shutdown 2 meets a ticket whose lease is gone and whose cancel result has no stopProven: runner-shutdown-unproven, no proof, no shutdown-unstarted', async () => {
  const gate = makeTmpDir('proof-hold-');
  const hold = { LANE_BROKER_TEST_HOLD_AT: 'remote-exec-before-result', LANE_BROKER_TEST_HOLD_READY: path.join(gate, 'ready'), LANE_BROKER_TEST_HOLD_GO: path.join(gate, 'go') };
  const ctx = setupRunner();
  const h = makeHeader('process.exit(0)');
  const { closed } = execRemote(ctx, h, { env: { ...ctx.env, ...hold } });
  await waitFor(() => fs.existsSync(hold.LANE_BROKER_TEST_HOLD_READY), { timeoutMs: 30_000 });
  await waitFor(() => !hasLease(ctx), { timeoutMs: 10_000 });
  const laneId = remoteIdOf(ctx, h.ticketId);
  atomicWriteJson(path.join(paths(ctx.state).results, `${laneId}.json`), { id: laneId, exit: 130, signal: null, startedAt: 1, endedAt: Date.now(), waitedMs: 0, cancelled: true });

  assert.equal((await shutdown(ctx)).code, 0);
  assert.equal(fs.existsSync(ticketPath(ctx, h.ticketId, 'shutdown')), true, 'shutdown 2 marked the ticket');
  assert.equal(fs.existsSync(ticketPath(ctx, h.ticketId, 'shutdown-unstarted')), false, 'a missing lease is not a dequeue');
  fs.writeFileSync(hold.LANE_BROKER_TEST_HOLD_GO, 'go');
  assert.equal((await closed).code, 0);

  const result = readExecResult(ctx, h.ticketId);
  assert.equal(result.kind, 'unfinished');
  assert.equal(result.reason, 'runner-shutdown-unproven');
  assert.equal('shutdownProof' in result, false);
  assert.equal(typeof result.shutdownBootId === 'string' || result.shutdownBootId === null, true);
});

test('T35b: shutdown lands after the ticket-local check and before the supervisor enqueues: runner-shutdown with shutdownProof never-started, and the command never runs', async () => {
  const gate = makeTmpDir('proof-pause-');
  const pauseFile = path.join(gate, 'resume');
  const sentinel = path.join(gate, 'ran');
  const ctx = setupRunner({ LANE_BROKER_TEST_PAUSE_AFTER_TICKET_ID: pauseFile });
  const h = makeHeader(`require('fs').writeFileSync(${JSON.stringify(sentinel)}, '1')`);
  const { closed } = execRemote(ctx, h);
  await waitFor(() => fs.existsSync(ticketPath(ctx, h.ticketId, 'remote-id')), { timeoutMs: 30_000 });

  assert.equal((await shutdown(ctx)).code, 0);
  assert.equal(fs.existsSync(ticketPath(ctx, h.ticketId, 'shutdown-unstarted')), false, 'not queued yet: nothing for the sweep to dequeue');
  fs.writeFileSync(pauseFile, 'go');
  assert.equal((await closed).code, 0);

  const result = readExecResult(ctx, h.ticketId);
  assert.equal(result.kind, 'unfinished');
  assert.equal(result.reason, 'runner-shutdown');
  assert.equal(result.shutdownProof, 'never-started');
  assert.equal(result.neverStarted, true);
  assert.equal(fs.existsSync(sentinel), false);
});
