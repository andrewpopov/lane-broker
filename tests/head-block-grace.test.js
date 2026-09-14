import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';

/**
 * BRAIN-249: once a conflict-blocked head's skip allowance is exhausted
 * (see conflict-skip.test.js's starvation-bound tests), nothing previously
 * bounded how long the resulting refusal could last. These tests pin the
 * live incident directly: a head blocked by a still-held, genuinely
 * long-running lease, with conflictSkipLimit already exhausted, and a
 * later, non-conflicting ticket sitting behind it. Before headBlockGraceMs
 * elapses, that later ticket stays blocked (today's behavior, unchanged).
 * Once it elapses, backfill resumes despite the exhausted count.
 *
 * Time is driven, never slept: conflict-skip-state.json's `blockedSince` is
 * written directly (the same file scheduler.js's resolveHeadBlock reads and
 * heals), so a multi-minute grace period is exercised in milliseconds.
 */

function baseCfg(overrides = {}) {
  return {
    ...DEFAULT_GLOBAL_CONFIG,
    schedulerMode: 'shadow',
    capacity: 10,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    ...overrides,
  };
}

function baseTicket(id, overrides = {}) {
  return {
    id,
    key: 'r:default',
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

function heldLease(id, key, weight = 1) {
  return {
    id,
    key,
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    weight,
    state: LEASE_STATE.RUNNING,
  };
}

test('a skip-exhausted head stays blocked within headBlockGraceMs, then backfill resumes once it elapses', async () => {
  const { state } = freshEnv();
  const limit = 3;
  const graceMs = 600_000;
  const globalCfg = baseCfg({ conflictSkipLimit: limit, headBlockGraceMs: graceMs });

  // A long-held lease that never releases within this test (standing in for
  // the real incident's 4.8h `rouge:sim` run) -- the whole point of BRAIN-249
  // is that this lease is legitimate and must NOT be reclaimed by time.
  writeLease(state, heldLease('holder', 'rouge:sim'));

  const head = baseTicket('head', { key: 'rouge:sim' });
  const other = baseTicket('other', { key: 'savoro:default' }); // conflicts with nothing
  await enqueue(state, head);
  await enqueue(state, other);

  // Pre-seed the skip state as already exhausted, blocked recently (well
  // inside the grace period).
  atomicWriteJson(paths(state).conflictSkipState, {
    headId: head.id,
    count: limit,
    blockedSince: Date.now(),
    loggedPhase: 'exhausted',
  });

  const withinGrace = await tryStart(state, other, globalCfg);
  assert.equal(withinGrace.started, false, 'within headBlockGraceMs, the exhausted head still refuses backfill (today\'s behavior)');
  assert.equal(withinGrace.reason, 'not-head');

  // Drive the clock: rewrite blockedSince as if the head had been sitting
  // blocked for longer than headBlockGraceMs, instead of sleeping.
  atomicWriteJson(paths(state).conflictSkipState, {
    headId: head.id,
    count: limit,
    blockedSince: Date.now() - (graceMs + 1000),
    loggedPhase: 'exhausted',
  });

  const pastGrace = await tryStart(state, other, globalCfg);
  assert.equal(pastGrace.started, true, 'past headBlockGraceMs, backfill resumes despite the exhausted skip count');

  // The still-conflicted head itself must still be refused -- the grace
  // period only lets OTHER tickets back in, it never starts the head over
  // its own unresolved conflict.
  const headStillBlocked = await tryStart(state, head, globalCfg);
  assert.equal(headStillBlocked.started, false);
  assert.equal(headStillBlocked.reason, 'conflict', 'the head itself never starts merely because the grace period lapsed');
});

test('a legacy skip-state file with no blockedSince does not crash and is not treated as infinitely old', async () => {
  const { state } = freshEnv();
  const limit = 2;
  const globalCfg = baseCfg({ conflictSkipLimit: limit, headBlockGraceMs: 600_000 });

  writeLease(state, heldLease('holder', 'rouge:sim'));

  const head = baseTicket('head', { key: 'rouge:sim' });
  const other = baseTicket('other', { key: 'savoro:default' });
  await enqueue(state, head);
  await enqueue(state, other);

  // Pre-BRAIN-249 shape: headId/count only, no blockedSince/loggedPhase.
  atomicWriteJson(paths(state).conflictSkipState, { headId: head.id, count: limit });

  const result = await tryStart(state, other, globalCfg);
  assert.equal(result.started, false, 'a missing timestamp must be treated as "starting now", not as already expired');
  assert.equal(result.reason, 'not-head');

  const healed = JSON.parse(fs.readFileSync(paths(state).conflictSkipState, 'utf8'));
  assert.equal(typeof healed.blockedSince, 'number', 'the missing timestamp must be healed and persisted, not recomputed forever');
  assert.ok(Date.now() - healed.blockedSince < 5000, 'the healed timestamp must be "now", not some other fixed value');
});
