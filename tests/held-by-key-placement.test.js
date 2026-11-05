import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, couldAdmitNow, sameKeyHolder } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { chooseSpeedPlacement } from '../src/speed-placement.js';

/**
 * BRAIN-839: a ticket whose LOCAL admission is blocked by a same-key holder (the key's maxConcurrent reached) carries that holder
 * from placement (`couldAdmitNow`) and from every non-start `tryStart` verdict (head or not), and speed placement then goes remote.
 */
const cfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 };
const ticket = (id, overrides = {}) => ({ id, key: 'r:default', weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', ...overrides });
const lease = (id, key, state) => ({ id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING, ...state });

test('sameKeyHolder: the same-key lease at maxConcurrent; below the ceiling and unrelated keys give null', () => {
  const held = [lease('a', 'r:default'), lease('b', 'r:other')];
  assert.equal(sameKeyHolder(held, ticket('t')).id, 'a');
  assert.equal(sameKeyHolder(held, ticket('t', { maxConcurrent: 2 })), null, 'below maxConcurrent');
  assert.equal(sameKeyHolder([...held, lease('c', 'r:default')], ticket('t', { maxConcurrent: 2 })).key, 'r:default', 'at maxConcurrent');
  assert.equal(sameKeyHolder([lease('b', 'r:other')], ticket('t', { conflicts: ['r:other'] })), null, 'a declared conflict on another key is not same-key');
});

test('couldAdmitNow: reports the same-key holder, also under queue-ahead; nothing once the holder is released', async () => {
  const { state } = freshEnv();
  writeLease(state, lease('holder', 'r:default'));
  const t = ticket('t1');
  await enqueue(state, t);
  const blocked = await couldAdmitNow(state, cfg, t);
  assert.equal(blocked.admit, false);
  assert.equal(blocked.heldByKey, 'holder');
  await enqueue(state, ticket('ahead'));
  assert.deepEqual([(await couldAdmitNow(state, cfg, t)).reason, (await couldAdmitNow(state, cfg, t)).heldByKey], ['queue-ahead', 'holder']);
  removeLease(state, 'holder');
  assert.equal((await couldAdmitNow(state, cfg, t)).heldByKey, undefined);
});

test('couldAdmitNow: a ceiling of 2 with one holder sets no flag; an unrelated-key holder sets none', async () => {
  const { state } = freshEnv();
  writeLease(state, lease('holder', 'r:default'));
  writeLease(state, lease('unrelated', 'r:other'));
  assert.equal((await couldAdmitNow(state, cfg, ticket('below', { maxConcurrent: 2 }))).heldByKey, undefined);
  removeLease(state, 'holder');
  assert.equal((await couldAdmitNow(state, cfg, ticket('unrelated-only', { conflicts: ['r:other'] }))).heldByKey, undefined);
});

test('tryStart: a conflicted head and a not-head both carry localHeldByKey; a started ticket (beside an unrelated-key lease) does not', async () => {
  const { state } = freshEnv();
  writeLease(state, lease('holder', 'r:default'));
  const head = ticket('head');
  const third = ticket('third');
  for (const t of [head, third]) await enqueue(state, t);
  const headVerdict = await tryStart(state, head, cfg);
  assert.equal(headVerdict.reason, 'conflict');
  assert.equal(headVerdict.localHeldByKey, 'holder');
  const nonHead = await tryStart(state, third, cfg);
  assert.equal(nonHead.reason, 'not-head');
  assert.equal(nonHead.localHeldByKey, 'holder', 'a not-head result carries the evidence too');
  removeLease(state, 'holder');
  writeLease(state, lease('unrelated', 'r:x'));
  const unrelated = await tryStart(state, head, cfg);
  assert.equal(unrelated.started, true);
  assert.equal(unrelated.localHeldByKey, undefined);
});

test('chooseSpeedPlacement localHeldByKey: remote on a tie, but not when admitted now, without a runner, or when the flag is off', () => {
  const base = { localAdmitsNow: false, localWaitMs: 0, localHeldByKey: true, estimateMs: 100_000, runnerRatio: 1, overheadMs: 0 };
  assert.deepEqual(chooseSpeedPlacement(base), { choice: 'remote', localFinishMs: null, remoteFinishMs: 100_000 });
  assert.equal(chooseSpeedPlacement({ ...base, localHeldByKey: false }).choice, 'local', 'an ordinary tie stays local');
  assert.equal(chooseSpeedPlacement({ ...base, localAdmitsNow: true }).choice, 'local', 'admitted now stays local');
  assert.equal(chooseSpeedPlacement({ ...base, runnerRatio: null }).choice, 'local', 'no runner -> local');
});
