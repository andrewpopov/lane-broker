import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeRepoConfig, writeGlobalConfig, laneRun, waitFor } from './helpers.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, resolveTicketConfig, ConfigError } from '../src/config.js';
import { score, effectiveRank } from '../src/priority.js';
import { atomicWriteJson, paths } from '../src/state.js';
import { fenceLegacyQueue } from '../src/migrate.js';

/**
 * ROG-2181: a lane may declare `"aging": false`, so its tickets never accrue priority age and an aged low
 * ticket can never tie, let alone precede, a fresh medium or high one. Every other lane ages as before.
 */

const MIN = 60_000;
const T0 = 1_700_000_000_000;

const cfg = () => ({ ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, conflictSafeBackfill: false });

const ticket = (id, overrides = {}) => ({
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
});

async function withClock(fn) {
  const realNow = Date.now;
  Date.now = () => T0;
  try {
    return await fn();
  } finally {
    Date.now = realNow;
  }
}

function fencedState() {
  const { state } = freshEnv();
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
  fenceLegacyQueue(state, 'test');
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
  return state;
}

// the aged low ticket is enqueued FIRST, so a tie at the ceiling would go to it on seq
async function agedLowVersus(freshTier, agedOverrides) {
  const state = fencedState();
  const aged = ticket('aged-low', { priorityRequested: 'low', prioOriginAt: T0 - 60 * MIN, ...agedOverrides });
  const fresh = ticket('fresh', { priorityRequested: freshTier });
  await enqueue(state, aged);
  await enqueue(state, fresh);
  return { state, aged, fresh };
}

for (const tier of ['medium', 'high']) {
  test(`an aged low ticket on an aging:false lane never precedes a fresh ${tier} ticket`, async () => {
    await withClock(async () => {
      const { state, aged, fresh } = await agedLowVersus(tier, { aging: false });
      assert.equal((await tryStart(state, aged, cfg())).reason, 'not-head', 'no age credit: the low ticket waits');
      assert.equal((await tryStart(state, fresh, cfg())).started, true);
    });
  });
}

test('control: an aged low ticket on an ordinary aging lane still ages past a fresh medium, and ties a fresh high once it has waited the 60-minute starvation horizon', async () => {
  await withClock(async () => {
    const medium = await agedLowVersus('medium', {});
    assert.equal((await tryStart(medium.state, medium.aged, cfg())).started, true, 'aged low beats a fresh medium');
    const high = await agedLowVersus('high', { aging: true });
    assert.equal((await tryStart(high.state, high.aged, cfg())).started, true, 'at the horizon tie the older seq wins');
  });
});

test('score and effective rank credit no age to an aging:false ticket', () => {
  const aged = { priorityAdmitted: 'low', prioOriginAt: T0 - 60 * MIN };
  assert.equal(score({ ...aged, aging: false }, T0, cfg()), 0);
  assert.equal(effectiveRank({ ...aged, aging: false }, T0, cfg()), 0);
  assert.equal(score(aged, T0, cfg()), 2, 'an aging lane (field absent) is at the ceiling once past the starvation horizon');
  assert.equal(effectiveRank(aged, T0, cfg()), 2);
});

test('lane config: aging resolves (default true), an undeclared lane inherits it, a non-boolean is rejected', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  const resolve = (lane) => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane });
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 }, fleet: { weight: 1, aging: false } }, undeclaredLanes: { as: 'fleet' } });
  assert.equal(resolve('default').aging, true);
  assert.equal(resolve('fleet').aging, false);
  assert.equal(resolve('fleet-adhoc').aging, false);
  for (const bad of [0, 'false', null]) {
    writeRepoConfig(repoDir, { version: 1, lanes: { fleet: { weight: 1, aging: bad } } });
    assert.throws(() => resolve('fleet'), (err) => err instanceof ConfigError && /lane "fleet"\.aging/.test(err.message), JSON.stringify(bad));
  }
});

test('aging:false survives into the persisted queue ticket built by `lane run`; an ordinary lane persists aging:true', async () => {
  const { env, home, base } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 }, fleet: { weight: 1, class: 'sim', aging: false } } });
  const state = env.LANE_BROKER_STATE;
  assert.equal((await laneRun(['pause', 'aging test'], { env })).code, 0);
  const queued = async (lane) => {
    const detached = await laneRun(['run', '--repo', 'r', '--lane', lane, '--detach', '--', 'true'], { env, cwd: repoDir });
    assert.equal(detached.code, 0, detached.stderr);
    const id = detached.stdout.trim();
    return { id, record: await waitFor(() => listQueue(state).find((t) => t?.id === id)) };
  };
  const fleet = await queued('fleet');
  const ordinary = await queued('default');
  try {
    assert.equal(fleet.record.aging, false);
    assert.equal(ordinary.record.aging, true);
  } finally {
    await laneRun(['cancel', fleet.id], { env });
    await laneRun(['cancel', ordinary.id], { env });
  }
});
