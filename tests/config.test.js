import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { sanitizeKey, expandConflicts, resolveTicketConfig, ConfigError, loadGlobalConfig, brokerHome } from '../src/config.js';
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

test('resolveTicketConfig resolves weight, key, and conflicts from a repo config file', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: { default: { weight: 2 }, sim: { weight: 2, localRefused: true }, lint: { weight: 1 } },
    conflicts: [['sim', '*']],
  });
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'rouge', lane: 'sim' });
  assert.equal(resolved.key, 'rouge:sim');
  assert.equal(resolved.weight, 2);
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

test('brokerHome respects LANE_BROKER_HOME', () => {
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = '/tmp/wherever';
  try {
    assert.equal(brokerHome(), '/tmp/wherever');
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});
