import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { readLease } from '../src/lease.js';
import { writeCancelMarkerFile } from '../src/state.js';

/**
 * BRAIN-319: deterministic, timing-free proof that `tryStart` re-checks the
 * cancel marker INSIDE its own admission lock, right before a ticket would
 * otherwise be admitted -- not just via the caller's own (unlocked, only
 * polled once per sampleMs) `cancelRequested` check in supervisor.js. A
 * ticket that would plainly be admitted (sole head, nothing else running, no
 * gate closed) must instead be refused, with no lease written and no
 * dequeue, the instant its cancel marker exists -- exactly the same
 * SELECTION-level test shape as no-backfill.test.js/conflict-skip.test.js,
 * calling `tryStart` directly rather than racing real subprocesses.
 */

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 8, ...overrides };
}

function baseTicket(id, overrides = {}) {
  return {
    id,
    key: `r:${id}`,
    weight: 1,
    cwd: process.cwd(),
    cmd: ['true'],
    supervisorPid: process.pid,
    supervisorStart: null,
    logPath: '/dev/null',
    resultPath: '/dev/null',
    ...overrides,
  };
}

test('a cancel marker written for the head, with nothing else in its way, refuses admission: no lease, ticket stays queued', async () => {
  const globalCfg = baseCfg();

  // Sanity, on its own isolated state: with no marker, this exact ticket
  // shape (sole head, nothing running, no gate closed) IS admitted under
  // this config -- proves the refusal below comes from the marker, not from
  // some other gate this config happens to trip.
  const control = freshEnv();
  const controlTicket = baseTicket('will-be-cancelled');
  await enqueue(control.state, controlTicket);
  const controlResult = await tryStart(control.state, controlTicket, globalCfg);
  assert.equal(controlResult.started, true, 'control ticket (no marker) is admitted under this config');

  const { state } = freshEnv();
  const ticket = baseTicket('will-be-cancelled');
  await enqueue(state, ticket);
  writeCancelMarkerFile(state, ticket.id);

  const result = await tryStart(state, ticket, globalCfg);
  assert.equal(result.started, false);
  assert.equal(result.reason, 'cancelled');

  assert.equal(readLease(state, ticket.id), null, 'a cancelled ticket must never get a lease written');
  const stillQueued = listQueue(state).some((t) => t && t.id === ticket.id);
  assert.equal(stillQueued, true, 'tryStart must leave the ticket queued -- dequeue/finalize is the supervisor poll loop\'s job');
});
