import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { readLease, removeLease, LEASE_STATE } from '../src/lease.js';

/**
 * This is a SELECTION test, not an end-to-end one: it drives `enqueue` /
 * `tryStart` directly, the same way tests/conflict-skip.test.js does, rather
 * than spawning real `lane run` subprocesses and waiting on a real detached
 * supervisor to reach admission. That used to be a real end-to-end test, but
 * it was blocking the pre-push hook — waiting on a real supervisor spawn is
 * inherently at the mercy of OS scheduling, and on a machine already busy
 * with unrelated work (verified via `ps`: dozens of concurrent, unrelated
 * `lane run`/supervisor processes from other sessions, load average ~5-6 on
 * 12 cores) the time from spawn to lease-file-on-disk measured 42s in one
 * observed run and 60s+ in another, against ~400ms in isolation — a
 * genuine external-load problem no code change here can bound, since it is
 * time-to-first-scheduled-quantum for a brand new process, not anything
 * tryStart computes.
 *
 * No coverage is lost by moving this in-process: the exact scenario this
 * test asserts — a ticket that already holds the full capacity blocks a
 * later, lighter, non-conflicting ticket from starting even though nothing
 * about it looks like a conflict — is the same shape already pinned by
 * conflict-skip.test.js's "a ticket is NOT skipped merely for not fitting
 * capacity" (the same capacity arithmetic, phrased as an enqueued head
 * rather than an already-running lease; tryStart treats both identically,
 * since it recomputes runningWeight from currently-held leases regardless
 * of how they got there). The end-to-end path — that a real `lane run`
 * actually goes through tryStart and enforces this under real concurrent
 * subprocesses — is independently covered by
 * scheduler-integration.test.js's "capacity holds" test, which uses
 * `laneRun` awaited to completion instead of polling for an intermediate
 * lease file, so it does not share this test's failure mode.
 */

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, ...overrides };
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

test('strict FIFO: a heavy ticket at the head blocks a lighter, non-conflicting ticket behind it (no backfill)', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();

  // Capacity 2: the heavy (weight 2) ticket uses the whole thing once
  // running, so a weight-1 ticket behind it does not fit under capacity
  // either -- this pins the "no backfill" property through the full
  // enqueue -> start -> a second ticket denied -> release -> second ticket
  // starts lifecycle, without depending on real subprocess scheduling.
  const heavy = baseTicket('heavy', { key: 'r:heavy', weight: 2 });
  const light = baseTicket('light', { key: 'r:light', weight: 1 });
  await enqueue(state, heavy);
  await enqueue(state, light);

  const heavyResult = await tryStart(state, heavy, globalCfg);
  assert.equal(heavyResult.started, true, 'the heavy ticket is the head and fits capacity, so it starts');

  // While heavy still holds the lease, light must not backfill ahead of it,
  // even though weight 1 alone would trivially fit if evaluated in
  // isolation.
  const lightResult = await tryStart(state, light, globalCfg);
  assert.equal(lightResult.started, false, 'the light ticket must not backfill ahead of the heavy head');
  assert.equal(lightResult.reason, 'capacity');

  const heavyLease = readLease(state, heavy.id);
  assert.ok(heavyLease && heavyLease.state === LEASE_STATE.RUNNING, 'heavy should hold a running lease');
  removeLease(state, heavy.id);

  const lightResultAfterRelease = await tryStart(state, light, globalCfg);
  assert.equal(lightResultAfterRelease.started, true, 'the light ticket must start only after the heavy one releases');
});
