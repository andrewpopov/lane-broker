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
