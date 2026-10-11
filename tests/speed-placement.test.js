import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig, freshEnv } from './helpers.js';
import { setup, tmpDir, probeCount, markerCmd, resultOf } from './remote-harness.js';
import { paths, bootId, appendHistory } from '../src/state.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { loadGlobalConfig, ConfigError } from '../src/config.js';
import { speedRatio, localRunEstimateMs, localWaitMs, chooseSpeedPlacement } from '../src/speed-placement.js';

const TIMEOUT = { timeout: 120_000 };

const row = (executor, fingerprint, runMs, extra = {}) => ({
  id: `${executor}-${fingerprint}-${runMs}-${Math.random()}`, repo: 'r', lane: 'default', cmdFingerprint: fingerprint, executor, exit: 0,
  startedAt: 1000, endedAt: 1000 + runMs, runMs, ...extra,
});

/** `n` commands, each run locally in `localMs` and on `runner` in `localMs * factor(i)`: differing sides, like production. */
const pairs = (n, runner, localMs, factor) =>
  Array.from({ length: n }, (_, i) => [row('local', `fp${i}`, localMs), row('remote', `fp${i}`, localMs * factor(i), { runner })]).flat();

test('speedRatio: median over fingerprints of remote run / local run on that runner', () => {
  const rows = pairs(5, 'skybox', 10_000, (i) => [2, 3, 3, 4, 9][i]);
  assert.equal(speedRatio(rows, { name: 'skybox' }), 3);
  assert.equal(speedRatio([...rows, ...pairs(5, 'wintop', 10_000, () => 5)], { name: 'skybox' }), 3, 'another runner\'s rows do not count');
});

test('speedRatio: under 5 paired commands falls back to the configured ratio, else 1', () => {
  const rows = pairs(4, 'skybox', 10_000, () => 3);
  assert.equal(speedRatio(rows, { name: 'skybox', speedRatio: 2.5 }), 2.5);
  assert.equal(speedRatio(rows, { name: 'skybox' }), 1);
  assert.equal(speedRatio([], { name: 'skybox', speedRatio: 0.5 }), 1, 'a configured ratio below 1 is not trusted');
});

test('speedRatio: a command with only a local or only a remote run is not a pair; failed runs do not count', () => {
  const rows = [...pairs(4, 'skybox', 10_000, () => 3), row('local', 'solo', 5000), row('remote', 'other', 9000, { runner: 'skybox' }), row('local', 'fp4', 10_000), row('remote', 'fp4', 90_000, { runner: 'skybox', exit: 1 })];
  assert.equal(speedRatio(rows, { name: 'skybox' }), 1);
});

test('localRunEstimateMs: the p50 of local runs of that repo and lane; null without one', () => {
  const rows = [...[1, 2, 3].map((i) => row('local', `f${i}`, 20_000)), row('remote', 'a', 999_000, { runner: 'skybox' })];
  assert.equal(localRunEstimateMs(rows, 'r', 'default'), 20_000);
  assert.equal(localRunEstimateMs(rows, 'r', 'other'), null);
  assert.equal(localRunEstimateMs([row('remote', 'a', 5000, { runner: 'skybox' })], 'r', 'default'), null);
});

test('chooseSpeedPlacement: admits now -> local even with a 3x runner; no runner -> local', () => {
  assert.equal(chooseSpeedPlacement({ localAdmitsNow: true, estimateMs: 100_000, runnerRatio: 3 }).choice, 'local');
  assert.equal(chooseSpeedPlacement({ localAdmitsNow: false, localWaitMs: 1e9, estimateMs: 100_000, runnerRatio: null }).choice, 'local');
});

test('chooseSpeedPlacement: remote only once local wait + E exceeds ratio*E + overhead', () => {
  const E = 100_000;
  const at = (wait) => chooseSpeedPlacement({ localAdmitsNow: false, localWaitMs: wait, estimateMs: E, runnerRatio: 3, overheadMs: 60_000 });
  assert.equal(at(2 * E + 60_000).choice, 'local', 'a tie stays local');
  const long = at(2 * E + 60_001);
  assert.deepEqual([long.choice, long.localFinishMs, long.remoteFinishMs], ['remote', 360_001, 360_000]);
});

test('localWaitMs: queued-ahead estimates plus one in-flight run, over the slots', () => {
  assert.equal(localWaitMs({ aheadEstimatesMs: [100, 200], estimateMs: 100, slots: 2 }), 200);
  assert.equal(localWaitMs({ aheadEstimatesMs: [], estimateMs: 100, slots: 0 }), 100);
});

test('config: runners[].speedRatio must be a number >= 1; remotePolicy accepts speed-aware', () => {
  const { home } = freshEnv();
  const load = (patch) => {
    fs.writeFileSync(path.join(home, 'config.json'), JSON.stringify({ version: 1, runners: [{ name: 'a', ssh: 'a', ...patch }] }));
    const prev = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      process.env.LANE_BROKER_HOME = prev;
    }
  };
  assert.equal(load({ speedRatio: 2.9 }).runners[0].speedRatio, 2.9);
  for (const bad of [0.9, 'fast', null]) assert.throws(() => load({ speedRatio: bad }), ConfigError, String(bad));
});

/** The harness runner is idle with ample headroom; `speedRatio` is set on it. History is seeded from a first local run. */
async function speedSetup({ speedRatioCfg, localMs }) {
  const ctx = setup({ ssh: 'normal' });
  const globalPath = path.join(ctx.home, 'config.json');
  const cfg = JSON.parse(fs.readFileSync(globalPath, 'utf8'));
  cfg.remoteRebindIntervalMs = 200;
  cfg.runners = cfg.runners.map((r) => ({ ...r, speedRatio: speedRatioCfg }));
  fs.writeFileSync(globalPath, JSON.stringify(cfg));
  // learn this repo's id from one local run (local-first runs here when it can), then restore the default policy
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remotePolicy: 'local-first' } } });
  const first = await laneRun(['run', '--lane', 'default', '--', ...markerCmd(path.join(tmpDir('sp-first'), 'm'))], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(first.code, 0, first.stderr);
  const seed = fs.readFileSync(paths(ctx.state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l))[0];
  fs.writeFileSync(paths(ctx.state).history, ''); // the probe run's own time must not skew the seeded estimate
  for (let i = 0; i < 5; i += 1) appendHistory(ctx.state, { ...seed, id: `seed-${i}`, executor: 'local', exit: 0, startedAt: 1000, endedAt: 1000 + localMs, runMs: undefined });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true } } });
  return ctx;
}

const holdLocal = (ctx) =>
  writeLease(ctx.state, { id: 'held', key: 'x:held', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 4, state: LEASE_STATE.RUNNING });
const admissionLog = (ctx) => fs.readFileSync(paths(ctx.state).admissionLog, 'utf8');

async function runDefault(ctx) {
  const marker = path.join(tmpDir('sp-marker'), 'm');
  const started = await laneRun(['run', '--detach', '--lane', 'default', '--', ...markerCmd(marker)], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  const waited = await laneRun(['wait', id, '--timeout', '60s'], { env: ctx.env });
  assert.equal(waited.code, 0, waited.stderr);
  return { id, result: resultOf(ctx.state, id) };
}

test('speed-aware: this machine admits now and a 3x runner exists -> runs locally, no runner probed', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  const probesBefore = probeCount(ctx.probeLogPath);
  const { result } = await runDefault(ctx);
  assert.equal(result.executor ?? 'local', 'local');
  assert.equal(probeCount(ctx.probeLogPath), probesBefore);
  assert.match(admissionLog(ctx), /speed-placement: ts=\d{4}-\d\d-\d\dT[\d:.]+Z \S+: local: admits now, local estimate 10s/);
});

test('speed-aware: local queue wait + E beats the runner by too much -> goes remote, both estimates logged', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 1, localMs: 3_600_000 });
  holdLocal(ctx);
  try {
    const { result } = await runDefault(ctx);
    assert.equal(result.executor, 'remote');
    assert.match(admissionLog(ctx), /speed-placement: ts=\S+ \S+: remote: local finish 4500s \(estimate 3600s\), skybox finish 3660s \(ratio 1\.00\)/);
  } finally {
    removeLease(ctx.state, 'held');
  }
});

test('speed-aware: a 3x runner while this machine is only briefly blocked -> waits and runs locally', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  holdLocal(ctx);
  setTimeout(() => removeLease(ctx.state, 'held'), 2500);
  const { result } = await runDefault(ctx);
  assert.notEqual(result.executor, 'remote');
  assert.match(admissionLog(ctx), /speed-placement: ts=\S+ \S+: local: local finish 13s \(estimate 10s\), skybox finish 90s \(ratio 3\.00\)/);
});

test('speed-aware: no history for the lane -> today\'s remote-first behaviour', TIMEOUT, async () => {
  const ctx = setup({ ssh: 'normal' });
  const { result } = await runDefault(ctx);
  assert.equal(result.executor, 'remote');
  assert.doesNotMatch(fs.existsSync(paths(ctx.state).admissionLog) ? admissionLog(ctx) : '', /speed-placement/);
});

test('remotePolicy: "remote-first" forces today\'s behaviour even when local admits and a runner is 3x slower', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remotePolicy: 'remote-first' } } });
  const { result } = await runDefault(ctx);
  assert.equal(result.executor, 'remote');
});
