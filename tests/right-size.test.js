import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { rightSizedCpuCores } from '../src/right-size.js';
import { childEnv } from '../src/supervisor.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';

// BRAIN-550: a lane's CPU request follows what its finished runs used, within [floor, declared].

const NOW = 1_800_000_000_000;
const row = (mean, i, over = {}) => ({
  repo: 'r',
  lane: 'default',
  startedAt: NOW - 400_000 - i * 1000,
  endedAt: NOW - 100_000 - i * 1000,
  runMs: 300_000,
  exit: 0,
  observedCpu: { peak: mean + 1, mean, samples: 30 },
  resources: { cpuCores: 4 },
  grantedCpuCores: 4,
  ...over,
});
const rows = (means, over) => means.map((m, i) => row(m, i, over));
const size = (history, over = {}) => rightSizedCpuCores(history, { repoId: 'r', lane: 'default', declared: 4, now: NOW, ...over });

test('5+ samples with a p90 mean of 1.5 on a declared 4 request 2', () => {
  assert.deepEqual(size(rows([1.5, 1.5, 1.5, 1.5, 1.5, 1.0])), { cpuCores: 2, runs: 6 });
});

test('fewer than 5 samples: no right-size (the declaration stands)', () => {
  assert.equal(size(rows([1.5, 1.5, 1.5, 1.5])), null);
});

test('a target above the declaration is clamped to the declaration', () => {
  assert.equal(size(rows([4.5, 4.5, 4.5, 4.5, 4.5])).cpuCores, 4);
});

test('a target below the floor is clamped to the floor, and to 1 without one', () => {
  const tiny = rows([0.1, 0.1, 0.1, 0.1, 0.1]);
  assert.equal(size(tiny, { floor: 2 }).cpuCores, 2);
  assert.equal(size(tiny).cpuCores, 1);
});

test('runs under two minutes, other lanes and other repos are not samples', () => {
  const noise = [...rows([1.5, 1.5, 1.5, 1.5], { runMs: 30_000 }), ...rows([1.5], { lane: 'other' }), ...rows([1.5], { repo: 'x' }), ...rows([1.5, 1.5, 1.5, 1.5])];
  assert.equal(size(noise), null);
});

test('a run granted less than it requested counts at its requested-equivalent', () => {
  // mean 1.0 on a grant of 2 out of a request of 4: it was throttled, so it needed ~2 (x1.2 -> 3)
  assert.equal(size(rows([1, 1, 1, 1, 1], { grantedCpuCores: 2 })).cpuCores, 3);
});

test('an ad-hoc lane name is sized from its declared lane', () => {
  const history = rows([1.5, 1.5, 1.5, 1.5, 1.5], { lane: 'zirk812', configLane: 'default' });
  assert.equal(size(history).cpuCores, 2);
});

test('the env export is the grant the lease carries', () => {
  const env = childEnv({ id: 'l', key: 'k', resources: { cpuCores: 2, memoryBytes: 1 } }, { PATH: '/bin' });
  assert.equal(env.LANE_BROKER_CPU_CORES, '2');
});

test('config: rightSizeFromHistory defaults on', () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.rightSizeFromHistory, true);
});

/** Run once to learn the repo id the broker resolves for this fixture, then seed history under it. */
async function fixture({ lane = { weight: 1, cpuCores: 4 }, global = {}, means = [1.5, 1.5, 1.5, 1.5, 1.5, 1.0] } = {}) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 8, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, ...global });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: lane } });
  const args = ['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'echo "grant=$LANE_BROKER_CPU_CORES"'];
  const probe = await laneRun(args, { env, cwd: repoDir });
  assert.equal(probe.code, 0, probe.stderr);
  const history = path.join(state, 'history.jsonl');
  const repoId = JSON.parse(fs.readFileSync(history, 'utf8').trim().split('\n').pop()).repo;
  const now = Date.now();
  const seeded = means.map((m, i) => JSON.stringify(row(m, i, { repo: repoId, endedAt: now - 1000 - i, startedAt: now - 400_000 - i })));
  fs.writeFileSync(history, `${seeded.join('\n')}\n`);
  return { args, env, repoDir, history, state };
}
const lastRow = (history) => JSON.parse(fs.readFileSync(history, 'utf8').trim().split('\n').pop());

test('lane run: a right-sized lane is granted 2 of a declared 4, exported, and recorded', async () => {
  const f = await fixture();
  const r = await laneRun(f.args, { env: f.env, cwd: f.repoDir });
  assert.equal(r.code, 0, r.stderr);
  assert.match(r.stdout, /grant=2\b/);
  const h = lastRow(f.history);
  assert.equal(h.requestedCpuCores, 2);
  assert.equal(h.grantedCpuCores, 2);
  assert.equal(h.declaredCpuCores, 4);
});

test('lane run: fewer than 5 samples keeps the declaration', async () => {
  const f = await fixture({ means: [1.5, 1.5, 1.5, 1.5] });
  const r = await laneRun(f.args, { env: f.env, cwd: f.repoDir });
  assert.match(r.stdout, /grant=4\b/);
  assert.equal(lastRow(f.history).declaredCpuCores, undefined);
});

test('lane run: a lane with rightSize false keeps the declaration', async () => {
  const f = await fixture({ lane: { weight: 1, cpuCores: 4, rightSize: false } });
  const r = await laneRun(f.args, { env: f.env, cwd: f.repoDir });
  assert.match(r.stdout, /grant=4\b/);
});

test('lane run: rightSizeFromHistory false keeps the declaration everywhere', async () => {
  const f = await fixture({ global: { rightSizeFromHistory: false } });
  const r = await laneRun(f.args, { env: f.env, cwd: f.repoDir });
  assert.match(r.stdout, /grant=4\b/);
});

test('lane run: an explicit --cpu is never right-sized', async () => {
  const f = await fixture();
  const r = await laneRun([...f.args.slice(0, 5), '--cpu', '3', ...f.args.slice(5)], { env: f.env, cwd: f.repoDir });
  assert.match(r.stdout, /grant=3\b/);
});

test('lane run: a right-sized lane with a floor can still be granted down to its floor, never below it', async () => {
  const f = await fixture({ lane: { weight: 1, cpuCores: 4, minCpuCores: 2 }, means: [0.1, 0.1, 0.1, 0.1, 0.1] });
  const r = await laneRun(f.args, { env: f.env, cwd: f.repoDir });
  assert.match(r.stdout, /grant=2\b/);
});

test('lane status shows declared to granted for a right-sized lease', async () => {
  const { state } = freshEnv();
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeLease(state, {
      id: 'l', key: 'r:l', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: process.pid, startedAt: Date.now(), heartbeatAt: Date.now(),
      weight: 1, state: LEASE_STATE.RUNNING, resources: { cpuCores: 2, memoryBytes: 1024 ** 3 }, declaredCpuCores: 4,
    });
    const status = await collectStatus();
    const running = status.running.find((r) => r.id === 'l');
    assert.equal(running.declaredCpuCores, 4);
    assert.equal(running.grantedCpuCores, 2);
    assert.match(renderStatusText(status), /cpu 4\u21922 \(right-sized\)/);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
  }
});
