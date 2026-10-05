import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { resolveTicketConfig, ConfigError, loadGlobalConfig, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { freshEnv, writeRepoConfig, writeGlobalConfig } from './helpers.js';

// BRAIN-379 slice 2: the lane `class` field and the allocationShadow / simArmWindowMs knobs.

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

const resolveLane = (lanes, lane, extra = {}) => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes, ...extra });
  return resolveTicketConfig({ cwd: repoDir, repo: 'r', lane });
};

test('a lane resolves class "test" by default and "sim" when declared', () => {
  const lanes = { default: { weight: 1 }, sims: { weight: 1, class: 'sim' }, unit: { weight: 1, class: 'test' } };
  assert.equal(resolveLane(lanes, 'default').class, 'test');
  assert.equal(resolveLane(lanes, 'sims').class, 'sim');
  assert.equal(resolveLane(lanes, 'unit').class, 'test');
});

test('an invalid lane class is a config error naming the lane', () => {
  for (const bad of ['simulator', '', 3, null, true]) {
    assert.throws(() => resolveLane({ default: { weight: 1, class: bad } }, 'default'), (err) => err instanceof ConfigError && /lane "default"\.class must be "test" or "sim"/.test(err.message), `class ${JSON.stringify(bad)}`);
  }
});

test('an undeclared lane resolved via "as" inherits the template lane\'s class', () => {
  const lanes = { default: { weight: 1 }, sims: { weight: 2, class: 'sim' } };
  assert.equal(resolveLane(lanes, 'adhoc', { undeclaredLanes: { as: 'sims' } }).class, 'sim');
  assert.equal(resolveLane(lanes, 'adhoc', { undeclaredLanes: { as: 'default' } }).class, 'test');
});

test('allocationShadow defaults false and simArmWindowMs defaults to 300000', () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.allocationShadow, false);
  assert.equal(DEFAULT_GLOBAL_CONFIG.simArmWindowMs, 300_000);
  const { home } = freshEnv();
  withHome(home, () => {
    const cfg = loadGlobalConfig();
    assert.equal(cfg.allocationShadow, false);
    assert.equal(cfg.simArmWindowMs, 300_000);
  });
});

test('allocationShadow must be a boolean and simArmWindowMs a positive integer', () => {
  const load = (override) => {
    const { home } = freshEnv();
    writeGlobalConfig(home, { version: 1, ...override });
    return withHome(home, () => loadGlobalConfig());
  };
  assert.equal(load({ allocationShadow: true, simArmWindowMs: 60_000 }).allocationShadow, true);
  for (const bad of ['yes', 1, null]) assert.throws(() => load({ allocationShadow: bad }), (e) => e instanceof ConfigError && /"allocationShadow" must be a boolean/.test(e.message), `allocationShadow ${JSON.stringify(bad)}`);
  for (const bad of [0, -5, 1.5, '300000', null]) assert.throws(() => load({ simArmWindowMs: bad }), (e) => e instanceof ConfigError && /"simArmWindowMs" must be a positive integer/.test(e.message), `simArmWindowMs ${JSON.stringify(bad)}`);
});

test('a sim lane whose resolved CPU claim exceeds 2 cores is rejected, whether it comes from cpuCores or the weight fallback', () => {
  const rejected = (lane) => assert.throws(() => resolveLane({ default: { weight: 1 }, sims: lane }, 'sims'), (e) => e instanceof ConfigError && /class "sim", so its CPU claim \(\d+\) must be <= 2 cores/.test(e.message), JSON.stringify(lane));
  rejected({ class: 'sim', weight: 8 });
  rejected({ class: 'sim', weight: 1, cpuCores: 8 });
  rejected({ class: 'sim', weight: 1, cpuCores: 3 });
  assert.equal(resolveLane({ default: { weight: 1 }, sims: { class: 'sim', weight: 2 } }, 'sims').class, 'sim');
  assert.equal(resolveLane({ default: { weight: 1 }, sims: { class: 'sim', weight: 8, cpuCores: 2 } }, 'sims').class, 'sim', 'cpuCores overrides weight for the claim');
  assert.equal(resolveLane({ default: { weight: 1 }, heavy: { class: 'test', weight: 8 } }, 'heavy').class, 'test', 'the ceiling is for sims only');
});
