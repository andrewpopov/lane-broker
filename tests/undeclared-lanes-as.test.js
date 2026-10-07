// BRAIN-325: `undeclaredLanes: {"as": "<declared lane>"}` -- an ad-hoc,
// undeclared lane name inherits sizing/remote settings from the named
// declared lane while keeping its own key and name.
import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { resolveTicketConfig, ConfigError } from '../src/config.js';
import { freshEnv, writeRepoConfig } from './helpers.js';

function templateRepo(base, overrides = {}) {
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'prepush' },
    lanes: {
      default: { weight: 2 },
      prepush: {
        weight: 4,
        cpuCores: 2,
        memoryBytes: 1073741824,
        nice: 5,
        remote: true,
        remoteDeps: ['.', 'web'],
        remoteSetup: [['npm', 'run', 'build:test']],
        ...overrides,
      },
    },
    conflicts: [['prepush', '*']],
  });
  return repoDir;
}

test('an undeclared lane inherits weight/cpuCores/memoryBytes/nice/remote/remoteDeps/remoteSetup from the named template', () => {
  const { base } = freshEnv();
  const repoDir = templateRepo(base);
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' });
  assert.equal(resolved.weight, 4);
  assert.equal(resolved.cpuCores, 2);
  assert.equal(resolved.memoryBytes, 1073741824);
  assert.equal(resolved.nice, 5);
  assert.equal(resolved.remote, true);
  assert.deepEqual(resolved.remoteDeps, ['.', 'web']);
  assert.deepEqual(resolved.remoteSetup, [['npm', 'run', 'build:test']]);
});

test('an undeclared lane resolved via "as" keeps its own key and name', () => {
  const { base } = freshEnv();
  const repoDir = templateRepo(base);
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' });
  assert.equal(resolved.lane, 'zirk812');
  assert.equal(resolved.key, 'r:zirk812');
});

test('a declared lane\'s ["*", other] conflict still reaches an "as"-resolved undeclared lane', () => {
  const { base } = freshEnv();
  const repoDir = templateRepo(base);
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' });
  // conflicts: [['prepush', '*']] only creates edges between "prepush" and
  // every OTHER lane, not a full mesh, so an undeclared lane sees "prepush"
  // in its conflict set (via the wildcard) but not "default".
  assert.deepEqual(resolved.conflicts.sort(), ['r:prepush']);
});

test('an "as"-resolved undeclared lane does NOT inherit the template\'s own named conflicts', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'prepush' },
    lanes: {
      default: { weight: 2 },
      lint: { weight: 1 },
      prepush: { weight: 4, remote: true },
    },
    // named conflict on the template, NOT a wildcard
    conflicts: [['prepush', 'lint']],
  });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' });
  assert.deepEqual(resolved.conflicts, []);
});

test('an "as"-resolved undeclared lane\'s maxConcurrent stays 1 even when the template declares more', () => {
  const { base } = freshEnv();
  const repoDir = templateRepo(base, { maxConcurrent: 4 });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' });
  assert.equal(resolved.maxConcurrent, 1);
  // the template itself still gets its own declared maxConcurrent
  const templateResolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'prepush' });
  assert.equal(templateResolved.maxConcurrent, 4);
});

test('a declared lane\'s own resolution is unaffected by "undeclaredLanes": {"as": ...}', () => {
  const { base } = freshEnv();
  const repoDir = templateRepo(base);
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
  assert.equal(resolved.weight, 2);
  assert.equal(resolved.remote, false);
});

test('"undeclaredLanes": {"as": ...} rejects an unknown template lane name', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'nope' },
    lanes: { default: { weight: 2 } },
  });
  assert.throws(
    () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' }),
    (err) => {
      assert.ok(err instanceof ConfigError);
      assert.match(err.message, /"undeclaredLanes\.as" must name a declared lane/);
      return true;
    },
  );
});

test('"undeclaredLanes": {"as": ...} rejects extra keys on the object', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'default', extra: true },
    lanes: { default: { weight: 2 } },
  });
  assert.throws(
    () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' }),
    (err) => {
      assert.ok(err instanceof ConfigError);
      assert.match(err.message, /must have exactly the key "as"/);
      return true;
    },
  );
});

test('"undeclaredLanes": {"as": ...} rejects a non-string "as" value', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 5 },
    lanes: { default: { weight: 2 } },
  });
  assert.throws(
    () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' }),
    (err) => {
      assert.ok(err instanceof ConfigError);
      assert.match(err.message, /"undeclaredLanes\.as" must be a non-empty string/);
      return true;
    },
  );
});

test('"undeclaredLanes": {"as": ...} rejects a template lane that is localRefused', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'sim' },
    lanes: { default: { weight: 2 }, sim: { weight: 2, localRefused: true } },
  });
  assert.throws(
    () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk812' }),
    (err) => {
      assert.ok(err instanceof ConfigError);
      assert.match(err.message, /must not be localRefused/);
      return true;
    },
  );
});

// BRAIN-448: the built-in `default` lane must not bypass the template.
function templateRepoNoDefault(base, extra = {}) {
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'prepush' },
    lanes: {
      prepush: { weight: 4, cpuCores: 2, memoryBytes: 1073741824, remote: true, remoteDeps: ['.', 'web'] },
      ...extra,
    },
  });
  return repoDir;
}

for (const [label, lane] of [['an explicit default', 'default'], ['an omitted lane', undefined]]) {
  test(`${label} inherits the template when the file does not declare default`, () => {
    const { base } = freshEnv();
    const repoDir = templateRepoNoDefault(base);
    const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane });
    assert.equal(resolved.weight, 4);
    assert.equal(resolved.cpuCores, 2);
    assert.equal(resolved.memoryBytes, 1073741824);
    assert.equal(resolved.remote, true);
    assert.deepEqual(resolved.remoteDeps, ['.', 'web']);
    assert.equal(resolved.lane, 'default');
    assert.equal(resolved.key, 'r:default');
    assert.equal(resolved.configLane, 'prepush');
  });
}

test('a file that declares default uses its own declaration, not the template', () => {
  const { base } = freshEnv();
  const repoDir = templateRepoNoDefault(base, { default: { weight: 3 } });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
  assert.equal(resolved.weight, 3);
  assert.equal(resolved.remote, false);
  assert.equal(resolved.configLane, undefined);
});

test('without a template, default stays the built-in (weight 2, not remote)', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { other: { weight: 5, remote: true } } });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
  assert.equal(resolved.weight, 2);
  assert.equal(resolved.remote, false);
  assert.equal(resolved.configLane, undefined);
});

test('with no config file, default is the built-in', () => {
  const { base } = freshEnv();
  const resolved = resolveTicketConfig({ cwd: path.join(base, 'norepo'), repo: 'r', lane: 'default' });
  assert.equal(resolved.weight, 2);
  assert.equal(resolved.remote, false);
  assert.equal(resolved.key, 'r:default');
});

for (const [label, lane] of [['an explicit default', 'default'], ['an omitted lane', undefined]]) {
  test(`${label} under "as": "default" with no file-declared default resolves exactly as before`, () => {
    const { base } = freshEnv();
    const repoDir = path.join(base, 'repo');
    writeRepoConfig(repoDir, { version: 1, undeclaredLanes: { as: 'default' }, lanes: { other: { weight: 5, remote: true } } });
    const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane });
    assert.equal(resolved.weight, 2);
    assert.equal(resolved.remote, false);
    assert.equal(resolved.key, 'r:default');
    assert.equal(resolved.configLane, undefined);
  });
}
