import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';

/**
 * BRAIN-202: a hung lane held its key for 9h; its own queued head conflicted
 * with it; once conflictSkipLimit was spent, the OLD `candidate = null` on
 * every poll meant nothing behind the head could EVER start again, no
 * matter how much capacity sat free or how unrelated the queued work was.
 * See scheduler.js's selectBackfillCandidate/tryStart doc comments for the
 * restricted-backfill rule this file exercises.
 */

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, capacity: 8, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, ...overrides };
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

test('incident replay: once the hung lane exhausts the skip limit, unrelated queued work from other repos still runs', async () => {
  const { state } = freshEnv();
  const limit = 3;
  const globalCfg = baseCfg({ conflictSkipLimit: limit });

  // The hung lane: a real held lease that NEVER releases, on the same key
  // as its own queued head -- exactly the incident (rouge:e2e held for 9h,
  // its own queued rouge:e2e head conflicting with it, no extra declared
  // conflicts).
  writeLease(state, heldLease('hung-lane', 'rouge:e2e'));
  const head = baseTicket('queued-head', { key: 'rouge:e2e' });
  await enqueue(state, head);

  // Drive the skip counter all the way to the limit with ordinary
  // (pre-exhaustion) backfill -- one throwaway ticket per cycle, each
  // staying held afterward, same as the real incident's other in-flight
  // work never releasing either.
  for (let i = 0; i < limit; i += 1) {
    const filler = baseTicket(`filler-${i}`, { key: `filler-key-${i}` });
    await enqueue(state, filler);
    const headResult = await tryStart(state, head, globalCfg);
    assert.equal(headResult.started, false);
    assert.equal(headResult.reason, 'not-head', `cycle ${i}: still under the skip limit`);
    const fillerResult = await tryStart(state, filler, globalCfg);
    assert.equal(fillerResult.started, true, `cycle ${i}: ordinary backfill still runs before exhaustion`);
  }

  // The allowance is now genuinely exhausted -- the head itself must report
  // it accurately (BRAIN-202's diagnostic fix), not merely 'not-head'.
  const exhaustedResult = await tryStart(state, head, globalCfg);
  assert.equal(exhaustedResult.started, false);
  assert.equal(exhaustedResult.reason, 'conflict', 'the skip limit is genuinely reached');

  // Two completely unrelated repos queue up behind the still-hung lane.
  // Under the OLD hard-stop behavior neither would EVER run again, no
  // matter how long the hang lasted (the actual incident ran 9h). Neither
  // key is rouge:e2e nor in its conflicts list (it declares none), so BOTH
  // must be admitted -- this is what must fail on unfixed code.
  const sanoOs = baseTicket('sano-os-ticket', { key: 'sano-os:default' });
  const laneBroker = baseTicket('lane-broker-ticket', { key: 'lane-broker:b201' });
  await enqueue(state, sanoOs);
  await enqueue(state, laneBroker);

  const sanoResult = await tryStart(state, sanoOs, globalCfg);
  assert.equal(sanoResult.started, true, 'unrelated repo work must not be starved by a hung, unrelated lane');

  const laneBrokerResult = await tryStart(state, laneBroker, globalCfg);
  assert.equal(laneBrokerResult.started, true, 'a second unrelated repo must also be admitted, not just the first');
});

test('reservation holds: once exhausted, a backfill candidate that would leave no capacity for the head is refused', async () => {
  const { state } = freshEnv();
  const limit = 1;
  const globalCfg = baseCfg({ capacity: 3, conflictSkipLimit: limit });

  writeLease(state, heldLease('hung', 'rouge:e2e', 1));
  const head = baseTicket('head', { key: 'rouge:e2e', weight: 1 });
  await enqueue(state, head);

  // One ordinary skip reaches the limit.
  const filler = baseTicket('filler', { key: 'filler-key', weight: 1 });
  await enqueue(state, filler);
  const fillerResult = await tryStart(state, filler, globalCfg);
  assert.equal(fillerResult.started, true);

  // capacity=3, held = hung(1) + filler(1) = 2. A weight-1 backfill
  // candidate that cannot possibly block the head (condition a) still must
  // leave room for the head's own weight (condition b): 2 + 1 + 1 = 4 > 3.
  const heavy = baseTicket('heavy', { key: 'unrelated-key', weight: 1 });
  await enqueue(state, heavy);

  const heavyResult = await tryStart(state, heavy, globalCfg);
  assert.equal(heavyResult.started, false);
  assert.equal(heavyResult.reason, 'capacity', 'a candidate must not be admitted if it would leave no room for the head to ever start');
});
