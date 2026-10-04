import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, gitFixture, BIN } from './helpers.js';
import { tmpDir, setup } from './remote-harness.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths } from '../src/state.js';
import { applyHeartbeatObservation } from '../src/supervisor.js';
import { leaseDemand, projectBusy } from '../src/admission.js';
import { foldObservedCpu, summarizeObservedCpu, sanitizeObservedCpu, sanitizeRssPeak, leaseOverrun, OVERRUN_MIN_MS } from '../src/observed.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { buildSuggestions, renderSuggestText } from '../src/suggest.js';

/** BRAIN-361: observed usage in history, the OVERRUN flag, and `lane suggest`. */

const GIB = 1024 ** 3;
const MIN = 60_000;

/** Replay `readings` ([offsetMs, cores]) through the real heartbeat fold. */
function replay(lease, readings, t0 = 1_000_000) {
  let l = lease;
  for (const [offset, cores] of readings) l = applyHeartbeatObservation(l, cores, t0 + offset);
  return l;
}
const lease4 = () => ({ id: 'l', weight: 1, resources: { cpuCores: 4, memoryBytes: GIB } });
const every10s = (from, to, cores) => {
  const out = [];
  for (let t = from; t <= to; t += 10_000) out.push([t, cores]);
  return out;
};

test('summarizeObservedCpu: peak, TIME-WEIGHTED mean, sample count', () => {
  let s;
  for (const [at, cores] of [[0, 1], [1000, 3], [4000, 0]]) s = foldObservedCpu(s, cores, at);
  // 1 core held 1000ms, 3 cores held 3000ms over a 4000ms span: (1000 + 9000) / 4000 = 2.5; a simple mean would say 1.333
  assert.deepEqual(summarizeObservedCpu(s), { peak: 3, mean: 2.5, samples: 3 });
  assert.deepEqual(summarizeObservedCpu(foldObservedCpu(undefined, 2, 5)), { peak: 2, mean: 2, samples: 1 });
  assert.equal(summarizeObservedCpu(undefined), null);
});

test('heartbeat keeps whole-run stats past the 64-reading history cap, and an RSS peak', () => {
  let l = lease4();
  for (let i = 0; i < 200; i += 1) l = applyHeartbeatObservation(l, i === 3 ? 9 : 1, 1000 + i * 100, i === 5 ? 7 * GIB : GIB);
  assert.equal(l.observedCpuHistory.length, 64);
  assert.equal(summarizeObservedCpu(l.observedCpuStats).peak, 9, 'the peak at reading 3 is long out of the capped history');
  assert.equal(summarizeObservedCpu(l.observedCpuStats).samples, 200);
  assert.equal(l.observedRssPeakBytes, 7 * GIB);
});

test('OVERRUN: sustained > 1.25x for 2 minutes flags; before 2 minutes it does not', () => {
  const t0 = 1_000_000;
  const before = replay(lease4(), every10s(0, 110_000, 6), t0); // 6 > 5 (1.25 x 4), 110s so far
  assert.equal(leaseOverrun(before), null, 'under two minutes');
  const after = replay(lease4(), every10s(0, 120_000, 6), t0);
  assert.deepEqual(leaseOverrun(after), { sinceMs: 120_000, observedPeak: 6 });
});

test('no OVERRUN on a single spike, even when it is the latest reading', () => {
  const spike = replay(lease4(), [...every10s(0, 100_000, 1), [110_000, 20], ...every10s(120_000, 300_000, 1)]);
  assert.equal(leaseOverrun(spike), null);
  const spikeLast = replay(lease4(), [...every10s(0, 300_000, 1), [310_000, 20]]);
  assert.equal(leaseOverrun(spikeLast), null);
  // a dip back under the threshold resets the clock: 100s over, 1 reading under, 100s over
  const reset = replay(lease4(), [...every10s(0, 100_000, 6), [110_000, 1], ...every10s(120_000, 220_000, 6)]);
  assert.equal(leaseOverrun(reset), null);
});

test('no OVERRUN at or below 1.25x the booking, for any duration', () => {
  const l = replay(lease4(), every10s(0, 10 * MIN, 5)); // exactly 1.25 x 4
  assert.equal(leaseOverrun(l), null);
});

test('an elastic lease is judged against its GRANT, not its declaration', () => {
  const elastic = { ...lease4(), resources: { cpuCores: 4, minCpuCores: 2, memoryBytes: GIB }, grantedCpuCores: 2 };
  const l = replay(elastic, every10s(0, 2 * MIN, 3.5)); // > 2.5 (1.25 x grant 2) but < 5 (1.25 x declared 4)
  assert.deepEqual(leaseOverrun(l), { sinceMs: 2 * MIN, observedPeak: 3.5 });
  assert.equal(leaseOverrun(replay(lease4(), every10s(0, 2 * MIN, 3.5))), null, 'the same load on a 4-core booking is fine');
});

test('a failed probe resets the overrun streak: no instant 10-minute overrun after a gap', () => {
  const t0 = 1_000_000;
  const over = replay(lease4(), every10s(0, 60_000, 6), t0);
  assert.ok(Number.isFinite(over.overrunSince));
  const probeFailed = applyHeartbeatObservation(over, null, t0 + 70_000);
  assert.equal('overrunSince' in probeFailed, false);
  const later = applyHeartbeatObservation(probeFailed, 6, t0 + 10 * MIN);
  assert.equal(leaseOverrun(later), null, 'one reading 10 minutes later is a new streak of length 0');
  assert.equal(OVERRUN_MIN_MS, 2 * MIN);
});

test('a gap of more than two heartbeats between good readings also resets the streak (stalled heartbeat, clock jump)', () => {
  const t0 = 1_000_000;
  const over = replay(lease4(), every10s(0, 60_000, 6), t0);
  const gap = applyHeartbeatObservation(over, 6, t0 + 10 * MIN, null, 10_000);
  assert.equal(gap.overrunSince, t0 + 10 * MIN, 'streak restarts at the new reading');
  assert.equal(leaseOverrun(gap), null);
  const bridged = applyHeartbeatObservation(over, 6, t0 + 60_000 + 10_000, null, 10_000);
  assert.equal(bridged.overrunSince, over.overrunSince, 'a normal 2-heartbeat gap keeps the streak');
});

test('a clock rollback restarts the overrun streak instead of bridging the retained boundary', () => {
  const l = replay(lease4(), [[0, 6], [-200_000, 6], [5_000, 6]], 1_000_000);
  assert.equal(l.overrunSince, 1_005_000, 'the 5s-later reading is not a continuation of the pre-rollback streak');
  assert.equal(leaseOverrun(l), null);
  const equal = replay(lease4(), [[0, 6], [0, 6]], 1_000_000);
  assert.equal(equal.overrunSince, 1_000_000, 'an equal timestamp restarts too');
});

test('malformed persisted stats never throw out of the heartbeat step; they are discarded and restarted', () => {
  const evil = { valueOf: 0, toString: 0 };
  const bad = [
    { peak: evil, area: 0, spanMs: 0, samples: 1, lastAt: 5, lastCores: 1 },
    { peak: 1, area: '0', spanMs: 0, samples: 1, lastAt: 5, lastCores: 1 },
    { peak: 1, area: 0, spanMs: 0, samples: evil, lastAt: 5, lastCores: 1 },
    { peak: 1, area: 0, spanMs: 0, samples: 1, lastAt: 5, lastCores: null },
    'garbage',
    [],
  ];
  for (const stats of bad) {
    const l = applyHeartbeatObservation({ ...lease4(), observedCpuStats: stats, overrunSince: evil, overrunPeak: evil, observedRssPeakBytes: evil }, 2, 1_000_000, GIB);
    assert.deepEqual(summarizeObservedCpu(l.observedCpuStats), { peak: 2, mean: 2, samples: 1 }, JSON.stringify(stats));
    assert.equal(l.observedRssPeakBytes, GIB);
    assert.equal(l.heartbeatAt, 1_000_000, 'the heartbeat itself always advances');
  }
  assert.equal(summarizeObservedCpu({ peak: evil, area: 0, spanMs: 0, samples: 1, lastAt: 1, lastCores: 1 }), null);
  // a lease whose resources are hostile must not throw either
  const hostile = applyHeartbeatObservation({ id: 'x', resources: { cpuCores: evil }, grantedCpuCores: evil, observedCpuStats: bad[0] }, 3, 5);
  assert.equal(hostile.heartbeatAt, 5);
});

test('relayed usage is validated at both remote sites: malformed fields are dropped, well-formed ones kept', () => {
  assert.deepEqual(sanitizeObservedCpu({ peak: 2, mean: 1, samples: 4, extra: 'x' }), { peak: 2, mean: 1, samples: 4 });
  for (const o of [null, 'x', { peak: 2 }, { peak: 2, mean: '1', samples: 4 }, { peak: { valueOf: 0 }, mean: 1, samples: 1 }, { peak: 1, mean: NaN, samples: 1 }]) {
    assert.equal(sanitizeObservedCpu(o), null, JSON.stringify(o));
  }
  assert.equal(sanitizeRssPeak('7'), undefined);
  assert.equal(sanitizeRssPeak(7), 7);
});

test('a backward clock jump never rewinds lastAt, so no interval is counted twice', () => {
  let s;
  for (const at of [1000, 2000, 1000, 2000]) s = foldObservedCpu(s, 1, at);
  assert.equal(s.spanMs, 1000, 'span is 1000..2000 once, not 2000');
  assert.equal(s.lastAt, 2000);
  assert.equal(s.samples, 4);
  assert.equal(summarizeObservedCpu(s).mean, 1);
});

test('REPORT-ONLY: an overrunning lease changes no admission decision', async () => {
  const now = Date.now();
  const plainLease = { id: 'h', key: 'r:h', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: now, weight: 1, state: LEASE_STATE.RUNNING, resources: { cpuCores: 2, memoryBytes: GIB }, admittedAt: now - 10 * MIN, observedCpuCores: 7, observedAt: now, observedCpuHistory: [{ at: now - 2000, cores: 7 }] };
  const flagged = { ...plainLease, overrunSince: now - 5 * MIN, overrunPeak: 7, observedCpuStats: { peak: 7, area: 1, spanMs: 1, samples: 9, lastAt: now, lastCores: 7 }, observedRssPeakBytes: GIB };
  assert.equal(leaseDemand(flagged, now), leaseDemand(plainLease, now));
  assert.equal(projectBusy(1, [flagged], 2, now), projectBusy(1, [plainLease], 2, now));
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'active', capacity: 10, cpuAdmissionPercent: 100, cpuReserveCores: 1, admissionCooldownMs: 0, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 };
  const sampler = () => ({ hostBusyCores: 3, cores: 10, stale: false, sampledAt: Date.now() });
  const mem = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
  const decide = async (held) => {
    const { state } = freshEnv();
    writeLease(state, held);
    const t = { id: 'c', key: 'r:c', weight: 1, resources: { cpuCores: 3, memoryBytes: GIB }, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' };
    await enqueue(state, t);
    const r = await tryStart(state, t, cfg, undefined, sampler, undefined, mem);
    return { started: r.started, cpuReason: r.cpuReason, memoryReason: r.memoryReason, projectedBusy: r.projectedBusy };
  };
  assert.deepEqual(await decide(flagged), await decide(plainLease));
});

test('lane status: an overrunning RUNNING lease is marked, with observed/booked and the --json shape', async () => {
  const { state } = freshEnv();
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    const now = Date.now();
    const base = { bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: process.pid, heartbeatAt: now, weight: 1, state: LEASE_STATE.RUNNING, startedAt: now - 10 * MIN, resources: { cpuCores: 4, memoryBytes: GIB }, observedAt: now };
    writeLease(state, { ...base, id: 'hot', key: 'r:hot', observedCpuCores: 7.8, overrunSince: now - 3 * MIN, overrunPeak: 8.1 });
    writeLease(state, { ...base, id: 'calm', key: 'r:calm', observedCpuCores: 1.5 });
    const status = await collectStatus();
    const hot = status.running.find((r) => r.id === 'hot');
    assert.equal(hot.bookedCpuCores, 4);
    assert.deepEqual(hot.overrun, { sinceMs: 3 * MIN, observedPeak: 8.1 });
    assert.equal(status.running.find((r) => r.id === 'calm').overrun, null);
    const lines = renderStatusText(status).split('\n');
    const hotLine = lines.find((l) => l.trim().startsWith('hot'));
    assert.match(hotLine, /\[OVERRUN peak 8\.1 for 3m0s\]/);
    assert.match(hotLine, /cpu 7\.80\/4/);
    const calmLine = lines.find((l) => l.trim().startsWith('calm'));
    assert.doesNotMatch(calmLine, /OVERRUN/);
    assert.match(calmLine, /cpu 1\.50\/4/);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
  }
});

const busyCmd = (ms) => [process.execPath, '-e', `const end=Date.now()+${ms};while(Date.now()<end){}`];
const lastRow = (root) => fs.readFileSync(paths(root).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l)).at(-1);

test('end to end (local): the history row carries observedCpu and observedRssPeakBytes', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 2 } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...busyCmd(1500)], { env, cwd: repoDir });
  assert.equal(result.code, 0, result.stderr);
  const row = lastRow(state);
  assert.ok(row.observedCpu.samples >= 2, JSON.stringify(row.observedCpu));
  assert.ok(row.observedCpu.peak > 0.3, `a busy loop must read as CPU: ${JSON.stringify(row.observedCpu)}`);
  assert.ok(row.observedCpu.mean > 0 && row.observedCpu.mean <= row.observedCpu.peak);
  assert.ok(row.observedRssPeakBytes > 1_000_000);
});

test('end to end (remote): runner history AND the submitter history carry observedCpu', async () => {
  const { env, repoDir, state, runnerState } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 2, memoryBytes: 1048576, remote: true } } });
  gitFixture(['add', '-A'], repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], repoDir);
  const marker = path.join(tmpDir('marker'), 'where');
  const cmd = [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(marker)}, process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local'); const end=Date.now()+1500;while(Date.now()<end){}`];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...cmd], { env, cwd: repoDir });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const runnerRow = lastRow(runnerState);
  assert.ok(runnerRow.observedCpu.peak > 0.3, `runner: ${JSON.stringify(runnerRow.observedCpu)}`);
  const submitter = lastRow(state);
  assert.equal(submitter.executor, 'remote');
  assert.deepEqual(submitter.observedCpu, runnerRow.observedCpu);
  assert.equal(submitter.observedRssPeakBytes, runnerRow.observedRssPeakBytes);
});

// ---- lane suggest ----

const NOW = Date.UTC(2026, 9, 2);
const SUSTAINED_MS = 5 * 60_000;
const row = (lane, peak, { mean = peak / 2, declared = 4, repo = 'r', ageDays = 1, observed = true, extra = {} } = {}) => ({
  id: `${lane}-${peak}-${Math.random()}`,
  repo,
  lane,
  weight: 1,
  resources: { cpuCores: declared, memoryBytes: GIB },
  startedAt: NOW - ageDays * 86_400_000,
  endedAt: NOW - ageDays * 86_400_000 + SUSTAINED_MS,
  exit: 0,
  ...(observed ? { observedCpu: { peak, mean, samples: 5 } } : {}),
  ...extra,
});
const many = (lane, peaks, opts) => peaks.map((p) => row(lane, p, opts));

function fixtureRows() {
  return [
    // under-booked: declared 3, peaks 1..10 (mean = peak/2) -> mean p50 2.5, p90 4.5 > 1.25 x 3, suggested 5. Peak p90 9 would say 9.
    ...many('hot', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], { declared: 3 }),
    // over-booked: declared 8, peaks ~0.3-1.5 -> mean p90 0.6, suggested 1 <= 4
    ...many('idle', [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.5], { declared: 8 }),
    // fine: declared 4, mean p90 3.5 -> suggested 4
    ...many('ok', [4, 5, 6, 6, 6.4, 6.6, 6.8, 7, 7, 7], { declared: 4 }),
    // too few runs
    ...many('rare', [9, 9, 9, 9], { declared: 1 }),
    // older history without observedCpu, a stale row outside the window, another repo, a row that never started
    ...many('hot', [0, 0, 0], { observed: false }),
    row('hot', 99, { ageDays: 30 }),
    row('hot', 99, { repo: 'other' }),
    row('hot', 99, { extra: { startedAt: undefined } }),
  ];
}

test('suggest math: p50/p90 of MEAN (peak shown), ceil, under/over-booked, too-few-runs, and missing observedCpu counted', () => {
  const report = buildSuggestions(fixtureRows(), { repo: 'r', days: 7, now: NOW });
  const by = Object.fromEntries(report.groups.map((g) => [g.lane, g]));
  assert.deepEqual(by.hot, { repo: 'r', lane: 'hot', grouping: 'lane', foldedLaneNames: 0, shortRunsExcluded: 0, unfinishedRuns: 0, malformedRuns: 0, elasticRuns: 0, grantedBelowDeclaredPct: 0, runs: 10, declaredCpuCores: 3, meanP50: 2.5, meanP90: 4.5, p50Peak: 5, p90Peak: 9, suggestedCpuCores: 5, verdict: 'under-booked' });
  assert.equal(by.idle.suggestedCpuCores, 1);
  assert.equal(by.idle.verdict, 'over-booked');
  assert.equal(by.ok.suggestedCpuCores, 4);
  assert.equal(by.ok.verdict, 'ok');
  assert.equal(by.rare, undefined);
  assert.deepEqual(report.tooFewRuns, [{ repo: 'r', lane: '(by resources)', runs: 4 }], 'a too-small legacy lane is pooled by resources, still short');
  assert.equal(report.skippedNoObservation, 3, 'the three in-window rows without observedCpu');
  assert.deepEqual(report.groups.map((g) => g.lane), ['hot', 'idle', 'ok'], 'under-booked first');
  const all = buildSuggestions(fixtureRows(), { days: 7, now: NOW });
  assert.equal(all.groups.find((g) => g.lane === 'hot').runs, 10, 'another repo\'s lane of the same name is its own group');
  assert.ok(all.tooFewRuns.some((g) => g.repo === 'other'), 'no --repo includes the other repo');
  const text = renderSuggestText(report);
  assert.match(text, /r:hot .*declared=3 .*mean p50\/p90=2.5\/4.5 {2}peak p50\/p90=5\/9 {2}suggested=5 {2}UNDER-BOOKED/);
  assert.match(text, /3 finished run\(s\) without observedCpu/);
});

test('suggest: sizes from the sustained MEAN, not brief peaks (jun prepush shape: booked 4, mean ~2, peak ~8)', () => {
  const means = [1.9, 1.9, 2, 2, 2, 2.1, 2.1, 2.2, 2.3, 2.3];
  const peaks = [7, 7.5, 7.5, 7.5, 8, 8, 8.5, 8.9, 8.9, 9];
  const rows = means.map((m, i) => row('prepush', peaks[i], { mean: m, declared: 4 }));
  const g = buildSuggestions(rows, { repo: 'r', days: 7, now: NOW }).groups[0];
  assert.equal(g.suggestedCpuCores, 3);
  assert.equal(g.verdict, 'ok', 'ceil(p90 mean) 3 > 0.5 x 4, so not over-booked, and 3 <= 4 so not under-booked; on peak it was under-booked at 9');
  assert.ok(g.p90Peak > 8, 'the peak is still reported');
  assert.match(renderSuggestText({ days: 7, minRuns: 5, groups: [g], tooFewRuns: [], skippedNoObservation: 0 }), /suggested=3$/m);
});

test('suggest: ad-hoc lanes group under the configLane they inherited and report how many names were folded', () => {
  const rows = [
    // 12 per-ticket names, 1 run each, all inheriting declared lane "default" (4 cores); plus the declared lane's own runs
    ...Array.from({ length: 12 }, (_, i) => row(`zirk${800 + i}`, 7, { declared: 4, extra: { configLane: 'default' } })),
    ...many('default', [1, 1], { declared: 4 }),
  ];
  const report = buildSuggestions(rows, { repo: 'r', days: 7, now: NOW });
  assert.equal(report.groups.length, 1, `grouped by literal lane name this would be 13 groups of 1-2: ${JSON.stringify(report.groups)}`);
  assert.deepEqual(report.groups[0], { repo: 'r', lane: 'default', grouping: 'lane', runs: 14, shortRunsExcluded: 0, unfinishedRuns: 0, malformedRuns: 0, elasticRuns: 0, grantedBelowDeclaredPct: 0, foldedLaneNames: 12, declaredCpuCores: 4, meanP50: 3.5, meanP90: 3.5, p50Peak: 7, p90Peak: 7, suggestedCpuCores: 4, verdict: 'ok' });
  assert.match(renderSuggestText(report), /runs=14 \(12 ad-hoc lane names folded\)/);
});

test('suggest: rows from before configLane existed pool by identical resources, labelled (by resources)', () => {
  const legacy = (lane, peak, declared, repo = 'r') => row(lane, peak, { declared, repo });
  const rows = [
    ...Array.from({ length: 6 }, (_, i) => legacy(`jun${100 + i}`, 3, 2)), // 6 names, 1 run each, same signature
    legacy('jun200', 9, 8), // different signature: stays short
    legacy('jun300', 3, 2, 'other'), // same signature, other repo: never pooled across repos
  ];
  const report = buildSuggestions(rows, { days: 7, now: NOW });
  assert.equal(report.groups.length, 1);
  assert.deepEqual(report.groups[0], { repo: 'r', lane: '(by resources)', grouping: 'resources', runs: 6, shortRunsExcluded: 0, unfinishedRuns: 0, malformedRuns: 0, elasticRuns: 0, grantedBelowDeclaredPct: 0, foldedLaneNames: 6, declaredCpuCores: 2, meanP50: 1.5, meanP90: 1.5, p50Peak: 3, p90Peak: 3, suggestedCpuCores: 2, verdict: 'ok' });
  assert.equal(report.tooFewRuns.length, 2);
  assert.match(renderSuggestText(report), /r:\(by resources\) /);
});

test('configLane is recorded on a history row only for an ad-hoc lane inheriting a template', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, undeclaredLanes: { as: 'default' }, lanes: { default: { weight: 1, cpuCores: 2 } } });
  for (const lane of ['default', 'zirk812']) {
    const result = await laneRun(['run', '--repo', 'r', '--lane', lane, '--', 'true'], { env, cwd: repoDir });
    assert.equal(result.code, 0, result.stderr);
  }
  const rows = fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  const byLane = Object.fromEntries(rows.map((r) => [r.lane, r]));
  assert.equal(byLane.zirk812.configLane, 'default');
  assert.equal('configLane' in byLane.default, false, 'a declared lane row stays byte-identical');
});

test('suggest: the declaration is the NEWEST row by time, even when ad-hoc rows interleave with the template', () => {
  const at = (i) => ({ startedAt: NOW - (100 - i) * 60_000, endedAt: NOW - (100 - i) * 60_000 + SUSTAINED_MS });
  const rows = [
    row('default', 2, { declared: 4, extra: at(1) }),
    row('zirk1', 2, { declared: 4, extra: { ...at(2), configLane: 'default' } }),
    row('default', 2, { declared: 8, extra: at(3) }),
    row('default', 2, { declared: 8, extra: at(4) }),
    row('default', 2, { declared: 8, extra: at(5) }),
  ];
  // lane-order grouping puts the three declared-lane rows after the ad-hoc one; the old code read the last of those
  const report = buildSuggestions([rows[0], rows[2], rows[3], rows[4], rows[1]], { days: 7, now: NOW });
  assert.equal(report.groups[0].declaredCpuCores, 8);
  assert.equal(buildSuggestions(rows, { days: 7, now: NOW }).groups[0].declaredCpuCores, 8);
});

test('suggest: the suggestion is floored at 1 core but the label uses the unclamped ceil(p90 mean)', () => {
  const idle = buildSuggestions(many('idle', [0, 0, 0, 0, 0], { declared: 0.5 }), { days: 7, now: NOW }).groups[0];
  assert.equal(idle.suggestedCpuCores, 1);
  assert.notEqual(idle.verdict, 'under-booked', 'an idle lane booked at 0.5 is not under-booked');
  assert.equal(idle.verdict, 'over-booked');
  const busy = buildSuggestions(many('busy', [1.6, 1.6, 1.6, 1.6, 1.6], { declared: 0.5 }), { days: 7, now: NOW }).groups[0];
  assert.equal(busy.suggestedCpuCores, 1);
  assert.equal(busy.verdict, 'under-booked', 'p90 0.8 > 1.25 x 0.5 is genuinely under-booked');
});

test('suggest: the resources fallback never pools rows whose memory or minCpuCores differ', () => {
  const legacy = (lane, resources) => ({ ...row(lane, 3, { declared: 2 }), resources });
  const sig = (memoryBytes, minCpuCores) => ({ cpuCores: 2, memoryBytes, ...(minCpuCores ? { minCpuCores } : {}) });
  const rows = [
    ...Array.from({ length: 5 }, (_, i) => legacy(`a${i}`, sig(GIB))),
    ...Array.from({ length: 5 }, (_, i) => legacy(`b${i}`, sig(2 * GIB))),
    ...Array.from({ length: 5 }, (_, i) => legacy(`c${i}`, sig(GIB, 1))),
  ];
  const report = buildSuggestions(rows, { days: 7, now: NOW });
  assert.deepEqual(report.groups.map((g) => [g.lane, g.runs]), [['(by resources)', 5], ['(by resources)', 5], ['(by resources)', 5]], 'three signatures, three groups, none merged into 15');
  assert.equal(report.tooFewRuns.length, 0);
});

test('suggest: UNDER-BOOKED needs the p90 to exceed OVERRUN_FACTOR x booking; ceil rounding alone is ok', () => {
  const at = (mean) => buildSuggestions(many('sim', [6, 6, 6, 6, 6, 6], { mean, declared: 4 }), { days: 7, now: NOW }).groups[0];
  const within = at(4.15);
  assert.equal(within.suggestedCpuCores, 5, 'suggested is still ceil(p90)');
  assert.equal(within.verdict, 'ok', '4.15 on a booking of 4 is 104%, inside the 1.25x overrun tolerance');
  assert.doesNotMatch(renderSuggestText({ days: 7, minRuns: 5, groups: [within], tooFewRuns: [], skippedNoObservation: 0 }), /UNDER-BOOKED/);
  assert.equal(at(5.2).verdict, 'under-booked', '5.2 > 1.25 x 4');
});

test('suggest: malformed rows are excluded and counted separately; no NaN or null reaches a numeric field', () => {
  const unfinished = row('w', 4, { mean: 9, extra: { endedAt: undefined } });
  const noDeclared = row('w', 4, { mean: 9, extra: { grantedCpuCores: 2, resources: { memoryBytes: GIB }, weight: undefined } });
  const good = many('w', [4, 4, 4, 4, 4, 4], { mean: 2, declared: 4 });
  const g = buildSuggestions([unfinished, noDeclared, ...good], { days: 7, now: NOW }).groups[0];
  assert.equal(g.unfinishedRuns, 1);
  assert.equal(g.malformedRuns, 1);
  assert.equal(g.shortRunsExcluded, 0, 'an unfinished run is not a short one');
  assert.equal(g.runs, 6);
  assert.equal(g.meanP90, 2, 'the mean-9 malformed rows never reach the percentile');
  // insufficient path: elastic stats come from its sustained rows, and every numeric field stays a number
  const few = [unfinished, noDeclared, ...many('v', [4, 4, 4], { mean: 2, declared: 4, extra: { grantedCpuCores: 2 } }), row('v', 4, { extra: { endedAt: NOW - 86_400_000 + 5000 } })];
  const fewG = buildSuggestions(few.map((r, i) => (i < 2 ? { ...r, lane: 'v' } : r)), { days: 7, now: NOW }).groups[0];
  assert.equal(fewG.verdict, 'insufficient');
  assert.equal(fewG.elasticRuns, 3);
  assert.equal(fewG.grantedBelowDeclaredPct, 100);
  for (const f of [g, fewG]) {
    for (const k of ['runs', 'shortRunsExcluded', 'unfinishedRuns', 'malformedRuns', 'elasticRuns', 'grantedBelowDeclaredPct', 'declaredCpuCores']) assert.ok(Number.isFinite(f[k]), `${k} is finite`);
  }
  assert.doesNotMatch(JSON.stringify(g), /null|NaN/);
});

test('suggest: runs under 2 minutes are excluded and counted; too few sustained runs means no suggestion', () => {
  const short = { startedAt: NOW - 86_400_000, endedAt: NOW - 86_400_000 + 5000 };
  const mixed = [...many('focused', [8, 8, 8, 8, 8, 8], { mean: 4, extra: short }), ...many('focused', [2, 2, 2, 2, 2], { mean: 1 })];
  const g = buildSuggestions(mixed, { days: 7, now: NOW }).groups[0];
  assert.equal(g.shortRunsExcluded, 6);
  assert.equal(g.runs, 5);
  assert.equal(g.meanP90, 1, 'the startup-dominated means of the short runs never reach the percentile');
  const allShort = buildSuggestions(many('focused', [8, 8, 8, 8, 8, 8], { mean: 4, extra: short }), { days: 7, now: NOW });
  assert.equal(allShort.groups[0].suggestedCpuCores, null);
  assert.equal(allShort.groups[0].verdict, 'insufficient');
  assert.equal(allShort.groups[0].shortRunsExcluded, 6);
  assert.match(renderSuggestText(allShort), /r:focused .*insufficient sustained runs \(6 short excluded\)/);
});

test('suggest: an elastic lane granted below declared is sized from declared-equivalent need, not the raw mean', () => {
  const elastic = many('sim', [4, 4, 4, 4, 4, 4], { mean: 2, declared: 4, extra: { grantedCpuCores: 2 } });
  const report = buildSuggestions(elastic, { days: 7, now: NOW });
  const g = report.groups[0];
  assert.equal(g.suggestedCpuCores, 4, 'mean 2 on a grant of 2 is a full declared 4, not 2');
  assert.equal(g.verdict, 'ok');
  assert.equal(g.elasticRuns, 6);
  assert.equal(g.grantedBelowDeclaredPct, 100);
  assert.equal(g.p90Peak, 4, 'peak stays unscaled');
  assert.match(renderSuggestText(report), /elastic: 100% of runs granted below declared/);
});

test('suggest: a non-elastic group is unchanged and prints no elastic note', () => {
  const report = buildSuggestions(many('plain', [4, 4, 4, 4, 4, 4], { mean: 2, declared: 4 }), { days: 7, now: NOW });
  assert.equal(report.groups[0].suggestedCpuCores, 2);
  assert.equal(report.groups[0].elasticRuns, 0);
  assert.equal(report.groups[0].shortRunsExcluded, 0);
  assert.doesNotMatch(renderSuggestText(report), /elastic/);
});

test('lane suggest CLI: reads history.jsonl, --json shape, --repo / --days', () => {
  const { state, env } = freshEnv();
  fs.writeFileSync(paths(state).history, `${fixtureRows().map((r) => JSON.stringify(r)).join('\n')}\n{torn`);
  const run = (...args) => spawnSync(process.execPath, [BIN, 'suggest', ...args], { env, encoding: 'utf8' });
  const res = run('--json', '--days', '100000');
  assert.equal(res.status, 0, res.stderr);
  const report = JSON.parse(res.stdout);
  assert.deepEqual(Object.keys(report).sort(), ['days', 'groups', 'minRuns', 'skippedNoObservation', 'tooFewRuns']);
  assert.deepEqual(Object.keys(report.groups[0]).sort(), ['declaredCpuCores', 'elasticRuns', 'foldedLaneNames', 'grantedBelowDeclaredPct', 'grouping', 'lane', 'malformedRuns', 'meanP50', 'meanP90', 'p50Peak', 'p90Peak', 'repo', 'runs', 'shortRunsExcluded', 'suggestedCpuCores', 'unfinishedRuns', 'verdict']);
  assert.equal(run('--repo', 'nonexistent').stdout.includes('no lane has enough observed runs'), true);
  assert.equal(run('--days', 'x').status, 2);
});
