import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig, freshEnv } from './helpers.js';
import { setup, tmpDir, probeCount, markerCmd, resultOf } from './remote-harness.js';
import { paths, bootId, appendHistory } from '../src/state.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { loadGlobalConfig, ConfigError } from '../src/config.js';
import { detectResourceCapacity } from '../src/resources.js';
import { missingLocalDeps, speedRatio, pairingCommand, externalCpuSlots, localRunEstimateMs, localWaitMs, chooseSpeedPlacement } from '../src/speed-placement.js';

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
  assert.equal(speedRatio(rows, { name: 'skybox' }).ratio, 3);
  assert.equal(speedRatio([...rows, ...pairs(5, 'wintop', 10_000, () => 5)], { name: 'skybox' }).ratio, 3, 'another runner\'s rows do not count');
});

test('speedRatio: under 5 paired commands falls back to the configured ratio, else 1', () => {
  const rows = pairs(4, 'skybox', 10_000, () => 3);
  assert.equal(speedRatio(rows, { name: 'skybox', speedRatio: 2.5 }).ratio, 2.5);
  assert.equal(speedRatio(rows, { name: 'skybox' }).ratio, 1);
  assert.equal(speedRatio([], { name: 'skybox', speedRatio: 0.5 }).ratio, 1, 'a configured ratio below 1 is not trusted');
});

test('speedRatio: a command with only a local or only a remote run is not a pair; failed runs do not count', () => {
  const rows = [...pairs(4, 'skybox', 10_000, () => 3), row('local', 'solo', 5000), row('remote', 'other', 9000, { runner: 'skybox' }), row('local', 'fp4', 10_000), row('remote', 'fp4', 90_000, { runner: 'skybox', exit: 1 })];
  assert.equal(speedRatio(rows, { name: 'skybox' }).ratio, 1);
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

test('localWaitMs waitedMs floor (BRAIN-556): a head ticket stays local until it has visibly waited longer than the runner takes', () => {
  const place = (waitedMs) => {
    const wait = localWaitMs({ aheadEstimatesMs: [], estimateMs: 61_000, slots: 1, waitedMs });
    return chooseSpeedPlacement({ localAdmitsNow: false, localWaitMs: wait, estimateMs: 61_000, runnerRatio: 5, overheadMs: 60_000 });
  };
  assert.equal(place(0).choice, 'local');
  assert.equal(place(10 * 60_000).choice, 'remote');
});

test('localWaitMs waitedMs floor never lowers the modelled wait', () => {
  assert.equal(localWaitMs({ aheadEstimatesMs: [100, 200], estimateMs: 100, slots: 2, waitedMs: 50 }), 200);
  assert.equal(localWaitMs({ aheadEstimatesMs: [100, 200], estimateMs: 100, slots: 2, waitedMs: 0 }), 200);
  assert.equal(localWaitMs({ aheadEstimatesMs: [100, 200], estimateMs: 100, slots: 2, waitedMs: 900 }), 900);
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

/** BRAIN-663: a tiny dependency-free npm package in the repo, so `remoteDeps: ["."]` is eligible and `npm ci` on a runner succeeds. */
function addLockedPackage(repoDir) {
  fs.writeFileSync(path.join(repoDir, 'package.json'), JSON.stringify({ name: 'fixture', version: '1.0.0', private: true }));
  fs.writeFileSync(path.join(repoDir, 'package-lock.json'), JSON.stringify({ name: 'fixture', version: '1.0.0', lockfileVersion: 3, requires: true, packages: { '': { name: 'fixture', version: '1.0.0' } } }));
}
const writeDefaultLane = (ctx, extra) => writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, ...extra } } });

test('speed-aware (BRAIN-663): remoteDeps lane with a lockfile but no node_modules is not placed local, the reason is logged', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  addLockedPackage(ctx.repoDir);
  writeDefaultLane(ctx, { remoteDeps: ['.'] });
  const { result } = await runDefault(ctx);
  assert.equal(result.executor, 'remote');
  assert.match(admissionLog(ctx), /speed-placement: ts=\S+ \S+: remote: speed-aware: local skipped \(no node_modules for remoteDeps \.\)/);
});

test('speed-aware (BRAIN-663): the same lane with node_modules present still runs locally', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  addLockedPackage(ctx.repoDir);
  fs.mkdirSync(path.join(ctx.repoDir, 'node_modules'));
  writeDefaultLane(ctx, { remoteDeps: ['.'] });
  const probesBefore = probeCount(ctx.probeLogPath);
  const { result } = await runDefault(ctx);
  assert.equal(result.executor ?? 'local', 'local');
  assert.equal(probeCount(ctx.probeLogPath), probesBefore);
  assert.match(admissionLog(ctx), /local: admits now/);
});

test('speed-aware (BRAIN-663): a lane without remoteDeps is unaffected by a missing node_modules', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  addLockedPackage(ctx.repoDir);
  const { result } = await runDefault(ctx);
  assert.equal(result.executor ?? 'local', 'local');
  assert.doesNotMatch(admissionLog(ctx), /local skipped/);
});

test('missingLocalDeps: only remoteDeps dirs with a lockfile and no node_modules directory count', () => {
  const root = tmpDir('mld');
  for (const d of ['a', 'b', 'c', 'd']) fs.mkdirSync(path.join(root, d));
  fs.writeFileSync(path.join(root, 'a', 'pnpm-lock.yaml'), '');
  fs.writeFileSync(path.join(root, 'b', 'yarn.lock'), '');
  fs.mkdirSync(path.join(root, 'b', 'node_modules'));
  fs.writeFileSync(path.join(root, 'c', 'package-lock.json'), '{}');
  fs.writeFileSync(path.join(root, 'c', 'node_modules'), 'a file, not a directory');
  assert.deepEqual(missingLocalDeps(root, ['a', 'b', 'c', 'd']), ['a', 'c']);
  assert.deepEqual(missingLocalDeps(root, null), []);
});

test('speed-aware: local queue wait + E beats the runner by too much -> goes remote, both estimates logged', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 1, localMs: 3_600_000 });
  holdLocal(ctx);
  try {
    const { result } = await runDefault(ctx);
    assert.equal(result.executor, 'remote');
    assert.match(admissionLog(ctx), /speed-placement: ts=\S+ \S+: remote: local finish 4500s \(estimate 3600s\), skybox finish 3660s \(ratio 1\.00, configured n=0\)/);
  } finally {
    removeLease(ctx.state, 'held');
  }
});

test('speed-aware: a 3x runner while this machine is only briefly blocked -> waits and runs locally', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  holdLocal(ctx);
  // an idle host, pinned: a loaded dev machine would otherwise leave no CPU slot and rightly send the ticket to the runner (BRAIN-727)
  const { cpuCores } = detectResourceCapacity();
  const busyFile = path.join(tmpDir('sp-busy'), 'busy');
  fs.writeFileSync(busyFile, `0,${cpuCores}\n`);
  ctx.env.LANE_BROKER_CPU_BUSY_FILE = busyFile;
  fs.writeFileSync(paths(ctx.state).cpuSample, JSON.stringify({ lastValid: { at: Date.now(), hostBusyCores: 0, cores: cpuCores } }));
  setTimeout(() => removeLease(ctx.state, 'held'), 2500);
  const { result } = await runDefault(ctx);
  assert.notEqual(result.executor, 'remote');
  assert.match(admissionLog(ctx), /speed-placement: ts=\S+ \S+: local: local finish 13s \(estimate 10s\), skybox finish 90s \(ratio 3\.00, configured n=0\)/);
});

test('speed-aware: host CPU gate closed by load outside any lease -> goes remote even though the 3x runner loses on a brief block', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 10_000 });
  holdLocal(ctx);
  const { cpuCores } = detectResourceCapacity();
  fs.writeFileSync(paths(ctx.state).cpuSample, JSON.stringify({ lastValid: { at: Date.now(), hostBusyCores: cpuCores, cores: cpuCores } }));
  fs.writeFileSync(paths(ctx.state).cpuGate, JSON.stringify({ closed: true, consecutiveUnder: 0 }));
  setTimeout(() => removeLease(ctx.state, 'held'), 2500);
  const { result } = await runDefault(ctx);
  assert.equal(result.executor, 'remote');
  assert.match(admissionLog(ctx), /speed-placement: ts=\S+ \S+: remote: local finish unbounded \(estimate 10s\), skybox finish 90s \(ratio 3\.00, configured n=0, local blocked by host CPU load\)/);
});

test('speed-aware: the supervisor judges the runner on this lane\'s own pairs, not the configured ratio', TIMEOUT, async () => {
  const ctx = await speedSetup({ speedRatioCfg: 3, localMs: 3_600_000 });
  const seed = JSON.parse(fs.readFileSync(paths(ctx.state).history, 'utf8').trim().split('\n')[0]);
  for (let i = 0; i < 5; i += 1) {
    const base = { ...seed, command: `pair-${i}`, cmdFingerprint: `pair-${i}`, exit: 0, startedAt: 1000, runMs: undefined };
    appendHistory(ctx.state, { ...base, id: `pl-${i}`, executor: 'local', endedAt: 1000 + 3_600_000 });
    appendHistory(ctx.state, { ...base, id: `pr-${i}`, executor: 'remote', runner: 'skybox', endedAt: 1000 + 1_800_000 });
  }
  holdLocal(ctx);
  try {
    const { result } = await runDefault(ctx);
    assert.equal(result.executor, 'remote');
    assert.match(admissionLog(ctx), /remote: local finish 4500s \(estimate 3600s\), skybox finish 1860s \(ratio 0\.50, repo-lane n=5\)/);
  } finally {
    removeLease(ctx.state, 'held');
  }
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

const TRAIN = (sha) => `env TRAIN_BASE_SHA=${sha} TRAIN_TS=1760000000000 sh .githooks/pre-push-gate.sh verify-full`;
const cmdRow = (executor, command, runMs, extra = {}) => row(executor, `fp-${command}`, runMs, { command, ...extra });
const SHA = (c) => c.repeat(40);
const job = (command, estimateMs, lane = 'default') => ({ command, repo: 'r', lane, estimateMs });
const shortPairs = (n, factor, localMs = 100_000, lane = 'quick') => pairs(n, 'wintop', localMs, () => factor).map((r) => ({ ...r, lane }));

test('speedRatio tiers: a long job gets the long-band ratio, not the short-job global median', () => {
  const longLocal = [1_000_000, 1_200_000, 1_400_000, 900_000, 1_100_000];
  const rows = [...shortPairs(30, 5), ...longLocal.flatMap((ms, i) => [row('local', `long${i}`, ms), row('remote', `long${i}`, ms * 2.5, { runner: 'wintop' })])];
  assert.equal(speedRatio(rows, { name: 'wintop' }).ratio, 5, 'global median is the short-job ratio');
  const long = speedRatio(rows, { name: 'wintop' }, { job: job('other', 1_100_000, 'adhoc') });
  assert.deepEqual([long.ratio, long.tier], [2.5, 'size-band']);
  assert.deepEqual(speedRatio(rows, { name: 'wintop' }, { job: job('other', 100_000, 'adhoc') }).tier, 'size-band');
});

test('speedRatio tiers: repo-lane beats size-band, exact beats repo-lane, thin evidence falls through to global then configured', () => {
  const rows = [...shortPairs(5, 4), ...pairs(5, 'wintop', 100_000, () => 4).map((r) => ({ ...r, repo: 'other', cmdFingerprint: `o-${r.cmdFingerprint}`, ...(r.executor === 'remote' ? { endedAt: r.startedAt + 900_000 } : {}) }))];
  assert.equal(speedRatio(rows, { name: 'wintop' }, { job: job('x', 100_000, 'quick') }).tier, 'repo-lane');
  const exact = [...rows, ...[1, 2].flatMap((i) => [cmdRow('local', 'run-me', 200_000), cmdRow('remote', 'run-me', 100_000, { runner: 'wintop' })])];
  const hit = speedRatio(exact, { name: 'wintop' }, { job: job('run-me', 200_000) });
  assert.deepEqual([hit.ratio, hit.tier], [0.5, 'exact']);
  const single = [cmdRow('local', 'run-me', 200_000), cmdRow('remote', 'run-me', 100_000, { runner: 'wintop' }), cmdRow('local', 'run-me', 200_000)];
  assert.notEqual(speedRatio([...rows, ...single], { name: 'wintop' }, { job: job('run-me', 200_000) }).tier, 'exact', 'one remote run is not exact evidence');
  assert.equal(speedRatio(shortPairs(4, 4), { name: 'wintop', speedRatio: 2.5 }, { job: job('x', 100_000) }).tier, 'configured');
  assert.equal(speedRatio(shortPairs(5, 4), { name: 'wintop' }, { job: { command: 'x', repo: 'zzz', lane: 'q', estimateMs: 9_000_000 } }).tier, 'global');
});

test('pairingCommand strips per-run sha / numeric-id env assignments only', () => {
  assert.equal(pairingCommand(TRAIN(SHA('a'))), pairingCommand(TRAIN(SHA('b'))));
  assert.equal(pairingCommand('env CI=1 WORKERS=4 FOO=bar abcdef1 X=abcdef1'), 'env CI=1 WORKERS=4 FOO=bar abcdef1');
  assert.equal(pairingCommand('A=abc123 B=1234567 C=deadbeef npm test'), 'A=abc123 npm test');
});

test('pairingCommand strips separated date-like ids (20261015-2, epoch ms, ISO minutes) but not CI=1, WORKERS=4, E2E_ONLY=foo.test.js', () => {
  assert.equal(pairingCommand('env TRAIN_RELEASE_ID=20261015-2 sh x'), pairingCommand('env TRAIN_RELEASE_ID=20261016-1 sh x'));
  assert.equal(pairingCommand('A=1792094137109 B=2026-10-15T12:00 sh x'), 'sh x');
  assert.equal(pairingCommand('env CI=1 WORKERS=4 E2E_ONLY=foo.test.js X=12-3 sh x'), 'env CI=1 WORKERS=4 E2E_ONLY=foo.test.js X=12-3 sh x');
});

test('pairingCommand drops an `env` left bare by the stripping, but keeps env with options or assignments', () => {
  assert.equal(pairingCommand(`env TRAIN_BASE_SHA=${SHA('a')} sh x`), 'sh x');
  assert.equal(pairingCommand(`env -i TRAIN_BASE_SHA=${SHA('a')} sh x`), 'env -i sh x');
  assert.equal(pairingCommand(`env CI=1 TRAIN_BASE_SHA=${SHA('a')} sh x`), 'env CI=1 sh x');
});

test('speedRatio: two commands differing only in TRAIN_BASE_SHA pair together (exact tier)', () => {
  const rows = [
    cmdRow('local', TRAIN(SHA('a')), 2_760_000), cmdRow('local', TRAIN(SHA('b')), 2_760_000),
    cmdRow('remote', TRAIN(SHA('c')), 1_702_000, { runner: 'wintop' }), cmdRow('remote', TRAIN(SHA('d')), 1_702_000, { runner: 'wintop' }),
  ];
  const got = speedRatio(rows, { name: 'wintop' }, { job: job(TRAIN(SHA('e')), 2_206_000) });
  assert.equal(got.tier, 'exact');
  assert.ok(Math.abs(got.ratio - 0.617) < 0.001, String(got.ratio));
});

test('externalCpuSlots: a closed gate admits nothing; otherwise what fits beside the ambient load', () => {
  assert.equal(externalCpuSlots({ gateClosed: true, budget: 10.2, externalBusy: 0, claimCores: 1 }), 0);
  assert.equal(externalCpuSlots({ gateClosed: false, budget: 10.2, externalBusy: 9.4, claimCores: 4 }), 0);
  assert.equal(externalCpuSlots({ gateClosed: false, budget: 10.2, externalBusy: 2, claimCores: 4 }), 2);
});

test('chooseSpeedPlacement localBlocked: host load admits nothing here -> remote despite a short queue; no runner -> local', () => {
  const base = { localAdmitsNow: false, localWaitMs: 0, localBlocked: true, estimateMs: 100_000, runnerRatio: 3 };
  assert.deepEqual(chooseSpeedPlacement(base), { choice: 'remote', localFinishMs: null, remoteFinishMs: 360_000 });
  assert.equal(chooseSpeedPlacement({ ...base, localBlocked: false }).choice, 'local');
  assert.equal(chooseSpeedPlacement({ ...base, runnerRatio: null }).choice, 'local');
});

test('2026-10-15 numbers: a train run stuck behind an external-load gate goes to wintop, not 11376s there', () => {
  const rows = [
    ...shortPairs(60, 5.13, 90_000),
    cmdRow('local', TRAIN(SHA('a')), 2_760_000), cmdRow('local', TRAIN(SHA('b')), 2_760_000),
    cmdRow('remote', TRAIN(SHA('c')), 1_702_000, { runner: 'wintop' }), cmdRow('remote', TRAIN(SHA('d')), 1_702_000, { runner: 'wintop' }),
  ];
  const E = 2_206_000;
  const old = speedRatio(rows, { name: 'wintop' });
  assert.ok(Math.abs(old.ratio - 5.13) < 0.01);
  const evidence = speedRatio(rows, { name: 'wintop' }, { job: job(TRAIN(SHA('e')), E) });
  const wait = localWaitMs({ aheadEstimatesMs: [], estimateMs: E, slots: 1, waitedMs: 1_497_000 });
  const place = (ratio, localBlocked) => chooseSpeedPlacement({ localAdmitsNow: false, localWaitMs: wait, localBlocked, estimateMs: E, runnerRatio: ratio });
  assert.equal(place(old.ratio, false).choice, 'local', 'the global median kept it queued (local 3701s vs 11376s)');
  assert.equal(place(evidence.ratio, false).choice, 'remote');
  assert.equal(place(old.ratio, true).choice, 'remote', 'a gate with no admissible slots sends it out even on the pessimistic ratio');
});
