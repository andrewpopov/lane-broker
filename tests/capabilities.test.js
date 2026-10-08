import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, BIN } from './helpers.js';
import { SCHEDULER_V2_VERSION } from '../src/fairness.js';
import { paths, atomicWriteJson } from '../src/state.js';

const capabilities = (env, args = ['capabilities', '--json']) => spawnSync(process.execPath, [BIN, ...args], { env, encoding: 'utf8' });

test('lane capabilities --json: legacy scheduler, shadow admission by default fixture', () => {
  const { home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1 });
  const res = capabilities(env);
  assert.equal(res.status, 0, res.stderr);
  const out = JSON.parse(res.stdout);
  assert.match(out.version, /^\d+\.\d+\.\d+$/);
  for (const c of ['elastic-claims/1', 'priority/1', 'artifacts/1', 'sim-safe-backfill/1', 'lane-aging/1', 'group-reap/1', 'remote-withdraw/1', 'exclusive/1']) {
    assert.ok(out.capabilities.includes(c), `missing ${c}`);
  }
  assert.equal(out.schedulerMode, 'legacy');
  assert.equal(out.admissionMode, 'shadow');
});

test('lane capabilities --json: a valid sched-v2 fence reports priority; active admission reports active', () => {
  const { home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, schedulerMode: 'active' });
  atomicWriteJson(paths(state).schedFence, { version: SCHEDULER_V2_VERSION, migratedAt: Date.now() });
  const out = JSON.parse(capabilities(env).stdout);
  assert.equal(out.schedulerMode, 'priority');
  assert.equal(out.admissionMode, 'active');
});

test('lane capabilities --json: an invalid fence is legacy; the flag is required', () => {
  const { home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1 });
  atomicWriteJson(paths(state).schedFence, { version: 99, migratedAt: Date.now() });
  assert.equal(JSON.parse(capabilities(env).stdout).schedulerMode, 'legacy');
  const bare = capabilities(env, ['capabilities']);
  assert.equal(bare.status, 2);
});
