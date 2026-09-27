import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { resolveTicketConfig, ConfigError } from '../src/config.js';
import { freshEnv, writeRepoConfig } from './helpers.js';

test('a lane name not declared in a repo config that declares lanes is refused', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 }, sim: { weight: 2 }, lint: { weight: 1 } } });

  assert.throws(
    () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'defualt' }),
    (err) => {
      assert.ok(err instanceof ConfigError);
      assert.match(err.message, /unknown lane "defualt"/);
      assert.match(err.message, /default/);
      assert.match(err.message, /sim/);
      assert.match(err.message, /lint/);
      return true;
    },
  );
});

test('an undeclared lane is still fine when no repo config file exists at all', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'bare-repo');
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'anything' });
  assert.equal(resolved.lane, 'anything');
});

test('"undeclaredLanes": "refuse" (explicit) still refuses an undeclared lane', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: 'refuse',
    lanes: { default: { weight: 2 }, prepush: { weight: 2, remote: true } },
  });

  assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk788' }), ConfigError);
});

test('"undeclaredLanes": "allow" resolves an undeclared lane exactly like the no-config-file case', () => {
  const { base: bareBase } = freshEnv();
  const bareRepoDir = path.join(bareBase, 'bare-repo');
  const noConfigResolved = resolveTicketConfig({ cwd: bareRepoDir, repo: 'r', lane: 'zirk788' });

  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: 'allow',
    lanes: { default: { weight: 2 }, prepush: { weight: 2, remote: true } },
  });
  const allowedResolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk788' });

  assert.deepEqual(allowedResolved, noConfigResolved);
});

test('an invalid "undeclaredLanes" value is a ConfigError', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, undeclaredLanes: 'sometimes', lanes: { default: { weight: 2 } } });

  assert.throws(
    () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }),
    (err) => {
      assert.ok(err instanceof ConfigError);
      assert.match(err.message, /"undeclaredLanes" must be "allow" or "refuse"/);
      return true;
    },
  );
});

test('a declared lane\'s ["*", other] conflict still reaches an allowed undeclared lane', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: 'allow',
    lanes: { default: { weight: 2 }, sim: { weight: 2 } },
    conflicts: [['sim', '*']],
  });

  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk788' });
  assert.deepEqual(resolved.conflicts.sort(), ['r:sim']);
});
