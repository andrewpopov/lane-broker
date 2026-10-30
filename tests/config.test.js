import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { sanitizeKey, expandConflicts, resolveTicketConfig, ConfigError, loadGlobalConfig, reloadGlobalConfig, DEFAULT_GLOBAL_CONFIG, brokerHome } from '../src/config.js';
import { freshEnv, writeRepoConfig, writeGlobalConfig } from './helpers.js';

test('sanitizeKey strips characters outside [A-Za-z0-9_.-]', () => {
  assert.equal(sanitizeKey('rouge/feature branch!'), 'rouge_feature_branch_');
  assert.equal(sanitizeKey(''), '_');
});

test('expandConflicts turns a wildcard pair into a full adjacency map', () => {
  const lanes = ['default', 'sim', 'lint'];
  const adj = expandConflicts([['sim', '*']], lanes);
  assert.deepEqual([...adj.get('sim')].sort(), ['default', 'lint']);
  assert.ok(adj.get('default').has('sim'));
  assert.ok(adj.get('lint').has('sim'));
  assert.equal(adj.get('default').has('lint'), false);
});

test('resolveTicketConfig resolves weight, resources, key, and conflicts from a repo config file', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: {
      default: { weight: 2 },
      sim: { weight: 2, cpuCores: 1.5, memoryBytes: 3221225472, localRefused: true },
      lint: { weight: 1 },
    },
    conflicts: [['sim', '*']],
  });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'rouge', lane: 'sim' });
  assert.equal(resolved.key, 'rouge:sim');
  assert.equal(resolved.weight, 2);
  assert.equal(resolved.cpuCores, 1.5);
  assert.equal(resolved.memoryBytes, 3221225472);
  assert.equal(resolved.localRefused, true);
  assert.deepEqual(resolved.conflicts.sort(), ['rouge:default', 'rouge:lint']);
});

test('resolveTicketConfig defaults to a single "default" lane of weight 2 when no config file exists', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'bare-repo');
  fs.mkdirSync(repoDir, { recursive: true });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' });
  assert.equal(resolved.key, 'x:default');
  assert.equal(resolved.weight, 2);
  assert.equal(resolved.localRefused, false);
});

test('an invalid global config throws ConfigError with a clear message', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 5, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    assert.throws(() => loadGlobalConfig(), ConfigError);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('reloadGlobalConfig picks up a changed value from disk', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 40, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const first = loadGlobalConfig();
    assert.equal(first.loadOpen, 11);
    writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 40, loadOpen: 30, loadOpenSamples: 3, sampleMs: 5000 });
    const reloaded = reloadGlobalConfig(first);
    assert.equal(reloaded.loadOpen, 30);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('reloadGlobalConfig returns previous when the file becomes invalid JSON', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const first = loadGlobalConfig();
    fs.writeFileSync(path.join(home, 'config.json'), '{ not valid json');
    const reloaded = reloadGlobalConfig(first);
    assert.deepEqual(reloaded, first);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('reloadGlobalConfig returns previous when the file fails validation', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const first = loadGlobalConfig();
    // loadOpen >= loadClose is rejected by validateGlobalConfig.
    writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 20, loadOpenSamples: 3, sampleMs: 5000 });
    const reloaded = reloadGlobalConfig(first);
    assert.deepEqual(reloaded, first);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('reloadGlobalConfig falls back to defaults when there is no previous', () => {
  const { home } = freshEnv();
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 20, loadOpenSamples: 3, sampleMs: 5000 }); // invalid
    const reloaded = reloadGlobalConfig(undefined);
    assert.deepEqual(reloaded, DEFAULT_GLOBAL_CONFIG);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

// ---- BRAIN-320 S1a: remoteDeps / remoteSetup / remoteQueueTimeoutMs ----

test('resolveTicketConfig exposes remoteDeps and remoteSetup for a lane that declares them', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-remote-deps');
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: {
      default: { weight: 2, remote: true, remoteDeps: ['.', 'web'], remoteSetup: [['npm', 'run', 'build:test']] },
    },
  });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' });
  assert.deepEqual(resolved.remoteDeps, ['.', 'web']);
  assert.deepEqual(resolved.remoteSetup, [['npm', 'run', 'build:test']]);
});

test('resolveTicketConfig leaves remoteDeps/remoteSetup null for a lane that declares neither (I6)', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-no-remote-deps');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 } } });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' });
  assert.equal(resolved.remoteDeps, null);
  assert.equal(resolved.remoteSetup, null);
});

test('remoteDeps must be a non-empty array', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-empty-remote-deps');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2, remoteDeps: [] } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' }), ConfigError);
});

test('remoteDeps rejects a non-canonical dir', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-bad-remote-deps');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2, remoteDeps: ['../escape'] } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' }), ConfigError);
});

test('remoteDeps rejects a duplicate entry', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-dup-remote-deps');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2, remoteDeps: ['web', 'web'] } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' }), ConfigError);
});

test('remoteDeps accepts "." as a canonical entry', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-dot-remote-deps');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2, remoteDeps: ['.'] } } });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' });
  assert.deepEqual(resolved.remoteDeps, ['.']);
});

test('remoteSetup must be a non-empty array of non-empty string argv arrays', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-bad-remote-setup');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2, remoteSetup: [[]] } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' }), ConfigError);
});

test('remoteSetup rejects a non-string argv element', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-bad-remote-setup-2');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2, remoteSetup: [['npm', 1]] } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' }), ConfigError);
});

test('remoteSetup must itself be a non-empty array', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-empty-remote-setup');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2, remoteSetup: [] } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' }), ConfigError);
});

test('remoteQueueTimeoutMs is absent by default and validated as a positive integer when set', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const cfg = loadGlobalConfig();
    assert.equal(cfg.remoteQueueTimeoutMs, undefined);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('remoteQueueTimeoutMs accepts a valid positive integer', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 2,
    loadClose: 15,
    loadOpen: 11,
    loadOpenSamples: 3,
    sampleMs: 5000,
    remoteQueueTimeoutMs: 120000,
  });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    const cfg = loadGlobalConfig();
    assert.equal(cfg.remoteQueueTimeoutMs, 120000);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('remoteQueueTimeoutMs rejects zero/negative/non-integer', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 2,
    loadClose: 15,
    loadOpen: 11,
    loadOpenSamples: 3,
    sampleMs: 5000,
    remoteQueueTimeoutMs: 0,
  });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    assert.throws(() => loadGlobalConfig(), ConfigError);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('remoteResultWaitMs (BRAIN-339) accepts a positive integer and rejects zero', () => {
  const base = { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 };
  const prevHome = process.env.LANE_BROKER_HOME;
  try {
    const ok = freshEnv();
    writeGlobalConfig(ok.home, { ...base, remoteResultWaitMs: 60000 });
    process.env.LANE_BROKER_HOME = ok.home;
    assert.equal(loadGlobalConfig().remoteResultWaitMs, 60000);
    const bad = freshEnv();
    writeGlobalConfig(bad.home, { ...base, remoteResultWaitMs: 0 });
    process.env.LANE_BROKER_HOME = bad.home;
    assert.throws(() => loadGlobalConfig(), ConfigError);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('brokerHome respects LANE_BROKER_HOME', () => {
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = '/tmp/wherever';
  try {
    assert.equal(brokerHome(), '/tmp/wherever');
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});

test('BRAIN-532: hung and idle-credit keys default sensibly and reject bad values', () => {
  const base = { version: 1, capacity: 2, loadClose: 40, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000 };
  const load = (extra) => {
    const { home } = freshEnv();
    writeGlobalConfig(home, { ...base, ...extra });
    const prevHome = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      process.env.LANE_BROKER_HOME = prevHome;
    }
  };
  const cfg = load({});
  assert.deepEqual([cfg.hungAfterMs, cfg.hungCpuCores, cfg.idleCreditBelowFraction, cfg.idleCreditAfterMs, cfg.idleCreditMode], [1_800_000, 0.05, 0.1, 600_000, 'shadow']);
  assert.equal(load({ idleCreditMode: 'off', hungAfterMs: 60_000 }).idleCreditMode, 'off');
  for (const bad of [{ hungAfterMs: 0 }, { hungAfterMs: 1.5 }, { hungCpuCores: -1 }, { idleCreditBelowFraction: 0 }, { idleCreditBelowFraction: 1.5 }, { idleCreditMode: 'on' }, { idleCreditMode: 'active' }, { idleCreditAfterMs: 0 }]) {
    assert.throws(() => load(bad), ConfigError, JSON.stringify(bad));
  }
});

// ---- BRAIN-629: localWindow validation ----

function loadWithGlobal(cfg) {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, ...cfg });
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    return loadGlobalConfig();
  } finally {
    if (prevHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prevHome;
  }
}
const goodSchedule = { timezone: 'America/Los_Angeles', available: [{ days: ['mon', 'tue'], from: '01:00', to: '06:00' }] };

test('localWindow defaults to null and accepts a valid schedule with lane names', () => {
  assert.equal(loadWithGlobal({}).localWindow, null);
  const cfg = loadWithGlobal({ localWindow: { schedule: goodSchedule, lanes: ['train', 'e2e'] } });
  assert.deepEqual(cfg.localWindow.lanes, ['train', 'e2e']);
});

test('localWindow rejects a bad schedule, empty or non-string lanes, and unknown keys', () => {
  const bad = [
    [{ schedule: { ...goodSchedule, timezone: 'Nowhere/Land' }, lanes: ['e2e'] }, /localWindow.*timezone/],
    [{ schedule: { ...goodSchedule, available: [] }, lanes: ['e2e'] }, /localWindow.*available/],
    [{ lanes: ['e2e'] }, /localWindow.*schedule/],
    [{ schedule: goodSchedule, lanes: [] }, /localWindow\.lanes/],
    [{ schedule: goodSchedule, lanes: [7] }, /localWindow\.lanes/],
    [{ schedule: goodSchedule }, /localWindow\.lanes/],
    [{ schedule: goodSchedule, lanes: ['e2e'], extra: 1 }, /unknown key "extra"/],
    ['always', /localWindow/],
  ];
  for (const [localWindow, pattern] of bad) {
    assert.throws(() => loadWithGlobal({ localWindow }), (err) => err instanceof ConfigError && pattern.test(err.message), JSON.stringify(localWindow));
  }
});


test('maxExecutionCpuCores is optional, inherited and independent of CPU reservation/floor', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'execution-cap');
  writeRepoConfig(repoDir, { version: 1, undeclaredLanes: { as: 'capped' }, lanes: {
    default: { weight: 2 }, capped: { weight: 2, cpuCores: 8, minCpuCores: 1, maxExecutionCpuCores: 3 },
  } });
  const uncapped = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
  assert.equal(Object.hasOwn(uncapped, 'maxExecutionCpuCores'), false);
  for (const lane of ['capped', 'ad-hoc']) {
    const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane });
    assert.equal(resolved.maxExecutionCpuCores, 3);
    assert.equal(resolved.cpuCores, 8);
    assert.equal(resolved.minCpuCores, 1);
  }
});

test('maxExecutionCpuCores rejects nonpositive, fractional, nonfinite and nonnumeric declarations', () => {
  for (const maxExecutionCpuCores of [0, -1, 1.5, Number.NaN, Infinity, null, '3']) {
    const { base } = freshEnv();
    const repoDir = path.join(base, 'invalid-execution-cap');
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, maxExecutionCpuCores } } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }),
      /maxExecutionCpuCores must be a positive integer/);
  }
});

test('cpuCores and minCpuCores below 0.01 core are rejected naming the key (a 1% quota would exceed the grant)', () => {
  for (const key of ['cpuCores', 'minCpuCores']) {
    for (const value of [0.001, 0.0099]) {
      const { base } = freshEnv();
      const repoDir = path.join(base, 'tiny-cpu');
      writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 1, [key]: value } } });
      assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), new RegExp(`lane "default"\\.${key} must be >= 0\\.01`));
    }
  }
});

test('a lane weight below 0.01 is rejected naming weight when cpuCores is unset, and allowed when cpuCores carries the claim', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'tiny-weight');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 0.001 } } });
  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), /lane "default"\.weight must be >= 0\.01/);
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 0.001, cpuCores: 1 } } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).weight, 0.001);
});

test('PKG-277 remoteDepsCacheKeyFiles resolves, is inherited through an undeclared-lane template, and null when absent', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'key-files');
  writeRepoConfig(repoDir, { version: 1, undeclaredLanes: { as: 'keyed' }, lanes: {
    default: { weight: 1 },
    keyed: { weight: 1, remoteDeps: ['.'], remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles: ['prisma/schema.prisma', 'scripts/tools'] },
  } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).remoteDepsCacheKeyFiles, null);
  for (const lane of ['keyed', 'ad-hoc']) {
    const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane });
    assert.deepEqual(resolved.remoteDepsCacheKeyFiles, ['prisma/schema.prisma', 'scripts/tools'], lane);
    assert.equal(resolved.remoteDepsCacheRootScriptsSafe, true, lane);
  }
});

test('PKG-277 remoteDepsCacheKeyFiles rejects bad shapes, a lane without the safe flag and a key file the lane excludes', () => {
  const bad = {
    'not an array': 'a.txt',
    empty: [],
    'empty string': [''],
    absolute: ['/etc/passwd'],
    parent: ['../a.txt'],
    'inner parent': ['a/../b.txt'],
    dot: ['.'],
    'dot segment': ['./a.txt'],
    'empty segment': ['a//b.txt'],
    backslash: ['a\\b.txt'],
    'trailing slash': ['dir/'],
    duplicate: ['a.txt', 'a.txt'],
    'not a string': [3],
    'inside node_modules': ['node_modules/x/package.json'],
    'inside .git': ['.git/config'],
  };
  for (const [name, remoteDepsCacheKeyFiles] of Object.entries(bad)) {
    const { base } = freshEnv();
    const repoDir = path.join(base, 'bad-key-files');
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles } } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), /remoteDepsCacheKeyFiles must be a non-empty array/, name);
  }
  const rejected = (lane, pattern) => {
    const { base } = freshEnv();
    const repoDir = path.join(base, 'bad-key-files');
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane } } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), pattern);
  };
  rejected({ remoteDepsCacheKeyFiles: ['a.txt'] }, /requires remoteDepsCacheRootScriptsSafe: true/);
  rejected({ remoteDepsCacheRootScriptsSafe: false, remoteDepsCacheKeyFiles: ['a.txt'] }, /requires remoteDepsCacheRootScriptsSafe: true/);
  rejected({ remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles: ['prisma/schema.prisma'], remoteExclude: ['prisma/'] }, /path "prisma\/schema.prisma" is excluded by remoteExclude/);
  rejected({ remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles: ['a.txt'], remoteExclude: ['*.txt'] }, /excluded by remoteExclude/);
  const { base } = freshEnv();
  const repoDir = path.join(base, 'ok-key-files');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles: ['a.txt'], remoteExclude: ['docs/'] } } });
  assert.deepEqual(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).remoteDepsCacheKeyFiles, ['a.txt']);
});
