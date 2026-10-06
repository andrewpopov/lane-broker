import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { evaluateCpuAdmission, leaseDemand, leaseDemandBasis, ticketCpuEstimateBasis, formatAdmissionLog } from '../src/admission.js';
import { computeCpuEstimates, refreshCpuEstimates, peekCpuEstimates, REFRESH_MS } from '../src/cpu-estimates.js';

// BRAIN-433: admission charges a lane's history-informed CPU, not its declaration.
const NOW = 2_000_000_000_000;
const KEY = '_Volumes_Lexar_proj_cairn_.git:qwen-552a';
const run = (key, mean, over = {}) => ({ key, repo: key.split(':')[0], lane: key.split(':')[1], resources: { cpuCores: 4 }, exit: 0, startedAt: NOW - 70_000, endedAt: NOW - 10_000, runMs: 60_000, observedCpu: { mean, peak: mean * 2, samples: 10 }, ...over });
const runs = (key, n, mean) => Array.from({ length: n }, (_, i) => run(key, mean, { endedAt: NOW - 10_000 - i * 1000 }));
const sourceOf = (rows) => (now, minRuns) => computeCpuEstimates(rows, { now, minRuns });
const cfgWith = (rows, over = {}) => ({ ...DEFAULT_GLOBAL_CONFIG, cpuEstimateSource: sourceOf(rows), ...over });

test('history present: 5+ runs at ~0.5 cores on a 4-core declaration is charged ~0.5', () => {
  const b = ticketCpuEstimateBasis({ key: KEY, weight: 4 }, cfgWith(runs(KEY, 6, 0.5)), NOW);
  assert.deepEqual(b, { cores: 0.5, source: 'history:exact' });
});

test('history present: the estimate is capped at the declaration and floored at 0.25', () => {
  assert.equal(ticketCpuEstimateBasis({ key: KEY, weight: 2 }, cfgWith(runs(KEY, 6, 3.5)), NOW).cores, 2);
  assert.equal(ticketCpuEstimateBasis({ key: KEY, weight: 4 }, cfgWith(runs(KEY, 6, 0.02)), NOW).cores, 0.25);
});

test('no history: fewer than 5 runs is charged the declaration', () => {
  assert.deepEqual(ticketCpuEstimateBasis({ key: KEY, weight: 4 }, cfgWith(runs(KEY, 4, 0.5)), NOW), { cores: 4, source: 'declared' });
});

test('disabled switch charges the declaration', () => {
  const cfg = cfgWith(runs(KEY, 6, 0.5), { historyDemandEnabled: false });
  assert.deepEqual(ticketCpuEstimateBasis({ key: KEY, weight: 4 }, cfg, NOW), { cores: 4, source: 'declared' });
});

test('estimator: p90 of per-run mean, cpuSeconds/wall preferred, failed/old runs and other keys ignored', () => {
  const rows = [
    ...[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0].map((m, i) => run(KEY, 9, { cpuSeconds: m * 60, endedAt: NOW - 1000 * (i + 1) })),
    run(KEY, 50, { exit: 1 }),
    run(KEY, 50, { endedAt: NOW - 15 * 24 * 3600 * 1000 }),
    ...runs('other:lane', 5, 7),
  ];
  const est = computeCpuEstimates(rows, { now: NOW, minRuns: 5 });
  const exact = (k) => est.get(`exact\0${k}`);
  assert.equal(exact(KEY).n, 10);
  assert.ok(Math.abs(exact(KEY).p90 - 0.9) < 1e-9);
  assert.equal(exact('other:lane').p90, 7);
});

test('estimator: only the last 50 runs count', () => {
  const rows = [...runs(KEY, 60, 0.5).map((r, i) => ({ ...r, endedAt: NOW - 1000 - i })), ...Array.from({ length: 30 }, (_, i) => run(KEY, 8, { endedAt: NOW - 100_000 - i }))];
  assert.equal(computeCpuEstimates(rows, { now: NOW, minRuns: 5 }).get(`exact\0${KEY}`).p90, 0.5);
});

test('observed above the estimate: a lease observed above its history estimate is charged observed', () => {
  const cfg = cfgWith(runs(KEY, 6, 0.5));
  const lease = { id: 'abcdef123456', key: KEY, weight: 4, admittedAt: NOW - 10_000, observedCpuCores: 2.5, observedAt: NOW };
  assert.equal(leaseDemand(lease, NOW, cfg), 2.5);
  assert.equal(leaseDemand({ ...lease, observedCpuCores: undefined, observedAt: undefined }, NOW, cfg), 0.5, 'cold demand of a just-admitted lease is the estimate');
});

test('settled floor: floorFraction x the estimate, not x declared', () => {
  const cfg = cfgWith(runs(KEY, 6, 1.0), { settledDemandFloorFraction: 0.5, settledDemandHeadroom: 1.25 });
  const lease = {
    id: 'abcdef123456', key: KEY, weight: 4, admittedAt: NOW - 300_000, observedCpuCores: 0.1, observedAt: NOW,
    observedCpuHistory: [{ at: NOW - 10_000, cores: 0.1 }, { at: NOW - 5_000, cores: 0.1 }],
  };
  const d = leaseDemandBasis(lease, NOW, cfg);
  assert.equal(d.basis, 'settled');
  assert.equal(d.demand, 0.5, 'declared-based floor would be 2');
});

const CPU_SAMPLE = { hostBusyCores: 3 + 4.75, cores: 10, stale: false };
function replay(cfg) {
  // 14:26: externalBusy 3; a cold lease charged 2.00; a settled lease charged 2.75; candidate declares 4; budget 9
  const cold = { id: 'cold00000000', key: 'x:cold', weight: 2, admittedAt: NOW - 10_000 };
  const settled = {
    id: 'settled00000', key: 'x:settled', weight: 4, admittedAt: NOW - 300_000, observedCpuCores: 2.2, observedAt: NOW,
    observedCpuHistory: [{ at: NOW - 10_000, cores: 2.2 }, { at: NOW - 5_000, cores: 2.2 }],
  };
  return evaluateCpuAdmission({
    cpuSample: { ...CPU_SAMPLE, hostBusyCores: 3 + 2.2 },
    heldLeases: [cold, settled],
    candidateWeight: 4,
    candidateRef: { key: KEY },
    candidateResources: { cpuCores: 4, memoryBytes: 1 },
    cpuGateState: { closed: false },
    cooldownBlocked: false,
    cfg: { ...cfg, cpuAdmissionPercent: 90, cpuReserveCores: 1 },
    now: NOW,
  });
}

test('integration/replay: denied at declared 4 (11.75 > 9), admitted when the lane history says ~0.3', () => {
  const declared = replay(cfgWith([]));
  assert.equal(declared.admit, false);
  assert.equal(declared.reason, 'projected-over-budget');
  assert.ok(Math.abs(declared.projectedBusy - 11.75) < 1e-9, String(declared.projectedBusy));
  assert.equal(declared.candidateEstimateSource, 'declared');

  const history = replay(cfgWith(runs(KEY, 8, 0.3)));
  assert.equal(history.admit, true);
  assert.equal(history.candidateEstimate, 0.3);
  assert.equal(history.candidateEstimateSource, 'history:exact');
  assert.ok(Math.abs(history.projectedBusy - 8.05) < 1e-9, String(history.projectedBusy));
});

test('admission log line shows candidateEstimate and its source', () => {
  const line = formatAdmissionLog({ candidateId: 'c', candidateEstimate: 0.5, candidateEstimateSource: 'history:exact' });
  assert.match(line, /candidateEstimate=0\.50\(history:exact\)/);
  assert.match(formatAdmissionLog({ candidateId: 'c', candidateEstimate: 4, candidateEstimateSource: 'declared' }), /candidateEstimate=4\.00\(declared\)/);
});

const REPO = '_Volumes_Lexar_proj_cairn_.git';
const ADHOC = `${REPO}:qwen-999z`;

test('ad-hoc lane with no exact history falls back to configLane, then to repo, then to declared', () => {
  const sib = (lane, mean, n, over = {}) => runs(`${REPO}:${lane}`, n, mean).map((r) => ({ ...r, ...over }));
  const ticket = { key: ADHOC, configLane: 'default', weight: 4, resources: { cpuCores: 4 } };
  // (b) sibling ad-hoc runs that resolved to the same declared lane, plus the declared lane's own runs
  const viaConfigLane = [...sib('qwen-111a', 0.5, 3, { configLane: 'default' }), ...sib('default', 0.5, 3)];
  assert.deepEqual(ticketCpuEstimateBasis(ticket, cfgWith(viaConfigLane), NOW), { cores: 0.5, source: 'history:configLane' });
  // (c) other lanes in the repo that declared the same cores; a different declaration is not pooled
  const viaRepo = [...sib('a', 0.4, 3), ...sib('b', 0.4, 3, { configLane: 'other' }), ...sib('c', 0.1, 9, { resources: { cpuCores: 1 } })];
  assert.deepEqual(ticketCpuEstimateBasis(ticket, cfgWith(viaRepo), NOW), { cores: 0.4, source: 'history:repo' });
  // exact wins over both
  assert.equal(ticketCpuEstimateBasis(ticket, cfgWith([...viaRepo, ...sib('qwen-999z', 0.3, 5)]), NOW).source, 'history:exact');
  // nothing enough anywhere
  assert.deepEqual(ticketCpuEstimateBasis(ticket, cfgWith(sib('a', 0.4, 4)), NOW), { cores: 4, source: 'declared' });
});

test('a lease carrying configLane is charged the same hierarchical estimate', () => {
  const rows = runs(`${REPO}:default`, 6, 0.5);
  const lease = { id: 'abcdef123456', key: ADHOC, configLane: 'default', weight: 4, resources: { cpuCores: 4 }, admittedAt: NOW - 10_000 };
  assert.equal(leaseDemand(lease, NOW, cfgWith(rows)), 0.5);
});

test('refresh reads history.jsonl (torn line tolerated) at most once a REFRESH_MS; peek never reads', () => {
  const root = makeTmpDir('history-demand-');
  const file = path.join(root, 'history.jsonl');
  const cfg = { historyDemandEnabled: true, historyDemandMinRuns: 5 };
  fs.writeFileSync(file, `${runs(KEY, 6, 0.5).map((r) => JSON.stringify(r)).join('\n')}\n{torn`);
  assert.equal(peekCpuEstimates(root).size, 0, 'peek before any refresh reads nothing');
  refreshCpuEstimates(cfg, NOW, root);
  assert.equal(peekCpuEstimates(root).get(`exact\0${KEY}`).p90, 0.5);
  fs.writeFileSync(file, '');
  refreshCpuEstimates(cfg, NOW + 1000, root);
  assert.equal(peekCpuEstimates(root).get(`exact\0${KEY}`).p90, 0.5, 'still cached');
  refreshCpuEstimates(cfg, NOW + REFRESH_MS + 1, root);
  assert.equal(peekCpuEstimates(root).has(`exact\0${KEY}`), false, 'refreshed');
});
