import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { loadGlobalConfig, resolveTicketConfig, resolvePriority, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { childEnv } from '../src/supervisor.js';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, waitFor, BIN } from './helpers.js';

// BRAIN-380 slice 1: priority config, the --priority / env / .lane-broker.json precedence, exit 64, and the child env export.

const QUIET = { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 };

function withHome(home, fn) {
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    return fn();
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prev;
  }
}

const loadWith = (extra) => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { ...extra });
  return withHome(home, () => loadGlobalConfig());
};

// ---- global config bounds ----------------------------------------------------------------------

test('priority defaults: 10-minute aging, 20-minute age cap, weights 2/2/0, one queued high per repo', () => {
  const cfg = loadWith({});
  assert.equal(cfg.priorityAgingMs, 600_000);
  assert.equal(cfg.priorityAgeMaxMs, 1_200_000);
  assert.deepEqual(cfg.priorityWeights, { tier: 2, age: 2, fairshare: 0 });
  assert.equal(cfg.maxQueuedHighPerRepo, 1);
  assert.deepEqual(DEFAULT_GLOBAL_CONFIG.priorityWeights, { tier: 2, age: 2, fairshare: 0 });
});

test('the age cap follows a configured aging step unless it is set explicitly', () => {
  assert.equal(loadWith({ priorityAgingMs: 1_800_000 }).priorityAgeMaxMs, 3_600_000);
  assert.equal(loadWith({ priorityAgingMs: 1_800_000, priorityAgeMaxMs: 2_000_000 }).priorityAgeMaxMs, 2_000_000);
});

test('a partial priorityWeights overrides only the weights it names', () => {
  assert.deepEqual(loadWith({ priorityWeights: { age: 5 } }).priorityWeights, { tier: 2, age: 5, fairshare: 0 });
});

const REJECTED = [
  ['priorityAgingMs below the floor', { priorityAgingMs: 59_999 }, /priorityAgingMs/],
  ['priorityAgingMs above the ceiling', { priorityAgingMs: 3_600_001, priorityAgeMaxMs: 7_200_000 }, /priorityAgingMs/],
  ['priorityAgingMs not an integer', { priorityAgingMs: 600_000.5 }, /priorityAgingMs/],
  ['priorityAgingMs a string', { priorityAgingMs: '600000' }, /priorityAgingMs/],
  ['priorityAgeMaxMs below priorityAgingMs', { priorityAgingMs: 600_000, priorityAgeMaxMs: 599_999 }, /priorityAgeMaxMs/],
  ['priorityAgeMaxMs above one day', { priorityAgeMaxMs: 86_400_001 }, /priorityAgeMaxMs/],
  ['priorityAgeMaxMs not an integer', { priorityAgeMaxMs: 1_200_000.5 }, /priorityAgeMaxMs/],
  ['weights not an object', { priorityWeights: 2 }, /priorityWeights/],
  ['weights an array', { priorityWeights: [2, 2, 0] }, /priorityWeights/],
  ['weights null', { priorityWeights: null }, /priorityWeights/],
  ['tier weight zero', { priorityWeights: { tier: 0 } }, /priorityWeights\.tier/],
  ['tier weight below 0.01', { priorityWeights: { tier: 0.001 } }, /priorityWeights\.tier/],
  ['tier weight negative', { priorityWeights: { tier: -1 } }, /priorityWeights\.tier/],
  ['tier weight above 100', { priorityWeights: { tier: 100.5, age: 100.5 } }, /priorityWeights\.tier/],
  ['tier weight not finite', { priorityWeights: { tier: 'x' } }, /priorityWeights\.tier/],
  ['age weight above 100', { priorityWeights: { age: 101 } }, /priorityWeights\.age/],
  ['age weight zero', { priorityWeights: { age: 0 } }, /priorityWeights\.age/],
  ['age weight below tier weight', { priorityWeights: { tier: 3, age: 2 } }, /priorityWeights\.age/],
  ['fairshare weight nonzero', { priorityWeights: { fairshare: 0.1 } }, /priorityWeights\.fairshare/],
  ['fairshare weight a string', { priorityWeights: { fairshare: '0' } }, /priorityWeights\.fairshare/],
  ['maxQueuedHighPerRepo negative', { maxQueuedHighPerRepo: -1 }, /maxQueuedHighPerRepo/],
  ['maxQueuedHighPerRepo fractional', { maxQueuedHighPerRepo: 1.5 }, /maxQueuedHighPerRepo/],
];
for (const [name, extra, pattern] of REJECTED) {
  test(`global config rejects: ${name}`, () => {
    assert.throws(() => loadWith(extra), (err) => err instanceof ConfigError && pattern.test(err.message));
  });
}

test('global config accepts a tier weight of exactly 0.01', () => {
  assert.equal(loadWith({ priorityWeights: { tier: 0.01 } }).priorityWeights.tier, 0.01);
});

test('global config accepts the exact bounds, and maxQueuedHighPerRepo 0 (high disabled)', () => {
  const cfg = loadWith({
    priorityAgingMs: 60_000,
    priorityAgeMaxMs: 60_000,
    priorityWeights: { tier: 100, age: 100, fairshare: 0 },
    maxQueuedHighPerRepo: 0,
  });
  assert.equal(cfg.priorityAgingMs, 60_000);
  assert.equal(cfg.maxQueuedHighPerRepo, 0);
  assert.equal(loadWith({ priorityAgingMs: 3_600_000, priorityAgeMaxMs: 86_400_000 }).priorityAgeMaxMs, 86_400_000);
});

// ---- precedence --------------------------------------------------------------------------------

test('resolvePriority: --priority beats env, env beats config, config beats the medium default', () => {
  assert.equal(resolvePriority({ cli: 'high', env: 'low', configTier: 'medium' }), 'high');
  assert.equal(resolvePriority({ env: 'low', configTier: 'high' }), 'low');
  assert.equal(resolvePriority({ configTier: 'high' }), 'high');
  assert.equal(resolvePriority({}), 'medium');
  assert.equal(resolvePriority({ configTier: null }), 'medium');
});

test('resolvePriority validates only the source that applies', () => {
  assert.equal(resolvePriority({ cli: 'low', env: 'bogus' }), 'low', 'a lower-precedence bad value does not matter once overridden');
  for (const bad of ['urgent', 'HIGH', '', ' high']) {
    assert.throws(() => resolvePriority({ cli: bad }), ConfigError, `cli ${JSON.stringify(bad)}`);
    assert.throws(() => resolvePriority({ env: bad }), ConfigError, `env ${JSON.stringify(bad)}`);
  }
});

const repoWith = (base, lanes, extra = {}) => {
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes, ...extra });
  return repoDir;
};

test('a lane\'s .lane-broker.json priority resolves, and defaults to null (medium downstream)', () => {
  const { base } = freshEnv();
  const repoDir = repoWith(base, { default: { weight: 1 }, hot: { weight: 1, priority: 'high' }, bulk: { weight: 1, priority: 'low' } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).priority, null);
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'hot' }).priority, 'high');
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'bulk' }).priority, 'low');
});

test('the undeclaredLanes template passes its priority to an ad-hoc lane (explicit field copy)', () => {
  const { base } = freshEnv();
  const repoDir = repoWith(base, { default: { weight: 1 }, prepush: { weight: 2, priority: 'high' } }, { undeclaredLanes: { as: 'prepush' } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' }).priority, 'high');
});

test('an invalid lane priority in .lane-broker.json is a ConfigError naming the lane', () => {
  for (const bad of ['urgent', '', 2, null, true]) {
    const { base } = freshEnv();
    const repoDir = repoWith(base, { default: { weight: 1, priority: bad } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), (err) => err instanceof ConfigError && /lane "default"\.priority/.test(err.message), JSON.stringify(bad));
  }
});

// ---- exit 64 through the real CLI --------------------------------------------------------------

function setup(lanes = { default: { weight: 1 } }, extra = {}) {
  const f = freshEnv();
  writeGlobalConfig(f.home, QUIET);
  f.repoDir = repoWith(f.base, lanes, extra);
  return f;
}
const probe = ['sh', '-c', 'echo "prio=$LANE_BROKER_PRIORITY"'];

test('lane run --priority with an invalid or missing value exits 64 and enqueues nothing', async () => {
  const { env, repoDir } = setup();
  for (const args of [['--priority', 'urgent'], ['--priority', 'HIGH'], ['--priority', ''], ['--priority']]) {
    const result = await laneRun(['run', '--repo', 'r', ...args, '--', 'true'], { env, cwd: repoDir });
    assert.equal(result.code, 64, `${JSON.stringify(args)}: ${result.stderr}`);
    assert.match(result.stderr, /--priority must be one of low, medium, high/);
  }
});

test('an invalid LANE_BROKER_PRIORITY exits 64; --priority overrides it', async () => {
  const { env, repoDir } = setup();
  const bad = { ...env, LANE_BROKER_PRIORITY: 'urgent' };
  const refused = await laneRun(['run', '--repo', 'r', '--', ...probe], { env: bad, cwd: repoDir });
  assert.equal(refused.code, 64, refused.stderr);
  assert.match(refused.stderr, /LANE_BROKER_PRIORITY must be one of/);
  const overridden = await laneRun(['run', '--repo', 'r', '--priority', 'low', '--', ...probe], { env: bad, cwd: repoDir });
  assert.equal(overridden.code, 0, overridden.stderr);
  assert.match(overridden.stdout, /prio=low\b/);
});

test('an invalid .lane-broker.json priority exits 64', async () => {
  const { env, repoDir } = setup({ default: { weight: 1, priority: 'urgent' } });
  const result = await laneRun(['run', '--repo', 'r', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 64, result.stderr);
  assert.match(result.stderr, /lane "default"\.priority/);
});

test('the invalid value exits 64 even on the reentrant path (validated before the reentrancy check)', async () => {
  const { env, repoDir } = setup();
  const nested = [process.execPath, BIN, 'run', '--repo', 'r', '--priority', 'urgent', '--', 'true'];
  const result = await laneRun(['run', '--repo', 'r', '--', ...nested], { env, cwd: repoDir });
  assert.equal(result.code, 64, `the outer run surfaces the nested exit code; stderr: ${result.stderr}`);
  assert.match(result.stderr, /--priority must be one of/);
});

// ---- precedence and child env, end to end ------------------------------------------------------

test('a lane child sees its resolved tier in LANE_BROKER_PRIORITY: cli > env > config > medium', async () => {
  const { env, repoDir } = setup({ default: { weight: 1 }, hot: { weight: 1, priority: 'high' } });
  const run = async (args, extraEnv = {}) => {
    const result = await laneRun(['run', '--repo', 'r', ...args, '--', ...probe], { env: { ...env, ...extraEnv }, cwd: repoDir });
    assert.equal(result.code, 0, result.stderr);
    return /prio=(\S*)/.exec(result.stdout)?.[1];
  };
  assert.equal(await run([]), 'medium', 'default');
  assert.equal(await run(['--lane', 'hot']), 'high', 'config');
  assert.equal(await run(['--lane', 'hot'], { LANE_BROKER_PRIORITY: 'low' }), 'low', 'env beats config');
  assert.equal(await run(['--lane', 'hot', '--priority', 'medium'], { LANE_BROKER_PRIORITY: 'low' }), 'medium', 'cli beats env');
});

test('the queued ticket records requested and admitted tier, an origin, and schedVersion 2', async () => {
  const { env, repoDir, state } = setup();
  const { listQueue } = await import('../src/scheduler.js');
  assert.equal((await laneRun(['pause', 'priority test'], { env })).code, 0);
  const before = Date.now();
  const detached = await laneRun(['run', '--repo', 'r', '--priority', 'high', '--detach', '--', 'true'], { env, cwd: repoDir });
  assert.equal(detached.code, 0, detached.stderr);
  const id = detached.stdout.trim();
  try {
    const record = await waitFor(() => listQueue(state).find((t) => t?.id === id));
    assert.equal(record.priorityRequested, 'high');
    assert.equal(record.priorityAdmitted, 'high');
    assert.equal(record.schedVersion, 2);
    assert.ok(record.prioOriginAt >= before && record.prioOriginAt <= Date.now(), 'origin is the priority clock at creation');
    assert.ok(record.prioOriginAt <= record.createdAt + 1000);
  } finally {
    await laneRun(['cancel', id], { env });
  }
});

test('a nested lane run inherits the tier through both child paths: supervisor child, then the reentrant direct spawn', async () => {
  const { env, repoDir } = setup();
  const innerProbe = ['sh', '-c', 'echo "inner=$LANE_BROKER_PRIORITY"'];
  const nested = [process.execPath, BIN, 'run', '--repo', 'r', '--', ...innerProbe];
  const outer = await laneRun(['run', '--repo', 'r', '--priority', 'low', '--', ...nested], { env, cwd: repoDir });
  assert.equal(outer.code, 0, outer.stderr);
  assert.match(outer.stdout, /inner=low\b/, 'the reentrant run (direct spawn) re-exports the tier it inherited from the supervisor child');
  const nestedHigh = [process.execPath, BIN, 'run', '--repo', 'r', '--priority', 'high', '--', ...innerProbe];
  const outer2 = await laneRun(['run', '--repo', 'r', '--priority', 'low', '--', ...nestedHigh], { env, cwd: repoDir });
  assert.match(outer2.stdout, /inner=high\b/, 'an explicit --priority on the reentrant run is what its direct-spawn child sees');
});

test('childEnv exports the admitted tier, and never an inherited one it did not set', () => {
  assert.equal(childEnv({ id: 'x', key: 'k', priorityAdmitted: 'high' }, { LANE_BROKER_PRIORITY: 'low' }).LANE_BROKER_PRIORITY, 'high');
  assert.equal(childEnv({ id: 'x', key: 'k', priorityRequested: 'high', priorityAdmitted: 'medium' }, {}).LANE_BROKER_PRIORITY, 'medium', 'the ADMITTED tier, not the requested one');
  assert.equal(childEnv({ id: 'x', key: 'k' }, { LANE_BROKER_PRIORITY: 'high' }).LANE_BROKER_PRIORITY, undefined);
  assert.equal(childEnv({ id: 'x', key: 'k', priorityAdmitted: 'bogus' }, { LANE_BROKER_PRIORITY: 'high' }).LANE_BROKER_PRIORITY, undefined);
});
