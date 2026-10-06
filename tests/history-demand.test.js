import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { evaluateNewAdmission, leaseDemand, leaseDemandBasis, ticketCpuEstimateBasis, formatAdmissionLog, freshObservedCores, coldStartEstimate } from '../src/admission.js';
import { leaseCpuCores } from '../src/resources.js';
import { computeCpuEstimates, refreshCpuEstimates, peekCpuEstimates, commandFingerprint, REFRESH_MS, SNAPSHOT_MAX_AGE_MS } from '../src/cpu-estimates.js';

// BRAIN-433: admission charges a workload's history-informed PEAK CPU, not its declaration. Every test here goes through
// the production signatures: a real history.jsonl in a state root, refreshCpuEstimates, then the real admission functions.
const REPO = '_Volumes_Lexar_proj_cairn_.git';
const KEY = `${REPO}:qwen-552a`;
const CMD = 'npm run verify';
const baseCfg = (over = {}) => ({ ...DEFAULT_GLOBAL_CONFIG, cpuAdmissionPercent: 90, cpuReserveCores: 1, admissionCooldownMs: 0, ...over });
const now0 = Date.now();

function row(key, peak, over = {}) {
  const [repo, lane] = [key.slice(0, key.lastIndexOf(':')), key.slice(key.lastIndexOf(':') + 1)];
  return { key, repo, lane, command: CMD, executor: 'local', resources: { cpuCores: 4 }, exit: 0, startedAt: now0 - 70_000, endedAt: now0 - 10_000, runMs: 60_000, observedCpu: { peak, mean: peak / 4, samples: 10 }, ...over };
}
const rows = (key, n, peak, over = {}) => Array.from({ length: n }, (_, i) => row(key, peak, { endedAt: now0 - 10_000 - i * 1000, ...over }));

/** A fresh state root holding these history rows, selected the way production selects it (LANE_BROKER_STATE), refreshed for real. */
function primed(history, cfg = baseCfg()) {
  const root = makeTmpDir('history-demand-');
  process.env.LANE_BROKER_STATE = root;
  fs.writeFileSync(path.join(root, 'history.jsonl'), history.map((r) => JSON.stringify(r)).join('\n') + '\n');
  refreshCpuEstimates(cfg, now0);
  return root;
}
const ticket = (over = {}) => ({ id: 'cand0000', key: KEY, command: CMD, weight: 4, resources: { cpuCores: 4, memoryBytes: 1 }, ...over });

test('history present: 5+ runs peaking ~0.8 on a 4-core declaration is charged ~0.8', () => {
  primed(rows(KEY, 6, 0.8));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), baseCfg(), now0), { cores: 0.8, source: 'history:exact' });
});

test('cap at the declaration, floor at 0.5', () => {
  primed(rows(KEY, 6, 7));
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0).cores, 4);
  primed(rows(KEY, 6, 0.05));
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0).cores, 0.5);
});

test('no history: fewer than 5 runs, or the switch off, is charged the declaration', () => {
  primed(rows(KEY, 4, 0.8));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), baseCfg(), now0), { cores: 4, source: 'declared' });
  primed(rows(KEY, 6, 0.8), baseCfg({ historyDemandEnabled: false }));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), baseCfg({ historyDemandEnabled: false }), now0), { cores: 4, source: 'declared' });
});

test('a bursty lane (peak 4, mean 1) is charged ~4, not its mean', () => {
  primed(rows(KEY, 8, 4, { observedCpu: { peak: 4, mean: 1, samples: 10 } }));
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0).cores, 4);
});

test('a different command under the same lane gets no relief', () => {
  primed(rows(KEY, 8, 0.8));
  assert.deepEqual(ticketCpuEstimateBasis(ticket({ command: 'npm run e2e' }), baseCfg(), now0), { cores: 4, source: 'declared' });
});

test('there is no repo-level pool: other lanes of the repo with the same declaration and command give no relief', () => {
  primed([...rows(`${REPO}:a`, 6, 0.8), ...rows(`${REPO}:b`, 6, 0.8, { configLane: 'other' })]);
  assert.deepEqual(ticketCpuEstimateBasis(ticket({ key: `${REPO}:qwen-999z`, configLane: 'default' }), baseCfg(), now0), { cores: 4, source: 'declared' });
});

test('configLane level: sibling ad-hoc runs of the same declared lane AND the same command; exact wins', () => {
  const sib = (lane, n, peak, over = {}) => rows(`${REPO}:${lane}`, n, peak, over);
  primed([...sib('qwen-111a', 3, 0.8, { configLane: 'default' }), ...sib('default', 3, 0.8)]);
  const adhoc = ticket({ key: `${REPO}:qwen-999z`, configLane: 'default' });
  assert.deepEqual(ticketCpuEstimateBasis(adhoc, baseCfg(), now0), { cores: 0.8, source: 'history:configLane' });
  primed([...sib('qwen-111a', 3, 0.8, { configLane: 'default', command: 'other cmd' }), ...sib('default', 3, 0.8)]);
  assert.equal(ticketCpuEstimateBasis(adhoc, baseCfg(), now0).source, 'declared', 'siblings that ran another command do not count');
  primed([...sib('qwen-111a', 3, 0.8, { configLane: 'default' }), ...sib('default', 3, 0.8), ...sib('qwen-999z', 5, 0.6)]);
  assert.equal(ticketCpuEstimateBasis(adhoc, baseCfg(), now0).source, 'history:exact');
});

test('only same-executor, full-grant runs count: remote and elastic-reduced runs are excluded', () => {
  primed([...rows(KEY, 4, 0.8), ...rows(KEY, 10, 0.8, { executor: 'remote' }), ...rows(KEY, 10, 0.8, { grantedCpuCores: 2, resources: { cpuCores: 4, minCpuCores: 2 } })]);
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0).source, 'declared');
  primed([...rows(KEY, 4, 0.8), ...rows(KEY, 1, 0.8, { grantedCpuCores: 4 })]);
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0).source, 'history:exact', 'a granted == declared run counts');
});

test('estimator: p90 of per-run peak, failed/old runs ignored, only the last 50 runs count', () => {
  const peaks = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
  const est = computeCpuEstimates(
    [...peaks.map((p, i) => row(KEY, p, { endedAt: now0 - 1000 * (i + 1) })), row(KEY, 50, { exit: 1 }), row(KEY, 50, { endedAt: now0 - 15 * 24 * 3600 * 1000 })],
    { now: now0, minRuns: 5 },
  );
  const [id] = [...est.keys()];
  assert.equal(est.get(id).n, 10);
  assert.equal(est.get(id).p90, 0.9);
  const many = [...rows(KEY, 60, 0.5).map((r, i) => ({ ...r, endedAt: now0 - 1000 - i })), ...rows(KEY, 30, 8, { endedAt: now0 - 100_000 })];
  assert.equal([...computeCpuEstimates(many, { now: now0, minRuns: 5 }).values()][0].p90, 0.5);
});

test('a stale or config-mismatched snapshot charges declared', () => {
  primed(rows(KEY, 6, 0.8));
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0).source, 'history:exact');
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0 + SNAPSHOT_MAX_AGE_MS + 1).source, 'declared', 'older than 5 minutes');
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg({ historyDemandMinRuns: 7 }), now0).source, 'declared', 'built under another minRuns');
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg({ historyDemandEnabled: false }), now0).source, 'declared', 'disabled by the locked config');
});

test('refresh reads history.jsonl (torn line tolerated) at most once a REFRESH_MS; peek never reads', () => {
  const root = makeTmpDir('history-demand-');
  process.env.LANE_BROKER_STATE = root;
  const file = path.join(root, 'history.jsonl');
  const cfg = baseCfg();
  fs.writeFileSync(file, `${rows(KEY, 6, 0.5).map((r) => JSON.stringify(r)).join('\n')}\n{torn`);
  assert.equal(peekCpuEstimates(cfg, now0).size, 0, 'peek before any refresh reads nothing');
  refreshCpuEstimates(cfg, now0);
  assert.equal(peekCpuEstimates(cfg, now0).size, 2);
  fs.writeFileSync(file, '');
  refreshCpuEstimates(cfg, now0 + 1000);
  assert.equal(peekCpuEstimates(cfg, now0 + 1000).size, 2, 'still cached');
  refreshCpuEstimates(cfg, now0 + REFRESH_MS + 1);
  assert.equal(peekCpuEstimates(cfg, now0 + REFRESH_MS + 1).size, 0, 'refreshed');
});

test('fingerprint: whitespace-normalized, argv or string alike, null when empty', () => {
  assert.equal(commandFingerprint(['npm', 'run', 'verify']), commandFingerprint('npm  run   verify'));
  assert.notEqual(commandFingerprint('npm run verify'), commandFingerprint('npm run e2e'));
  assert.equal(commandFingerprint(''), null);
});

// the 14:26 situation: external 3, a cold lease (2.00), a settled lease (2.75), a 4-core candidate, budget 9
const settledLease = () => ({
  id: 'settled00000', key: 'x:settled', weight: 4, admittedAt: now0 - 300_000, observedCpuCores: 2.2, observedAt: now0,
  observedCpuHistory: [{ at: now0 - 10_000, cores: 2.2 }, { at: now0 - 5_000, cores: 2.2 }],
});
const coldLease = () => ({ id: 'cold00000000', key: 'x:cold', weight: 2, admittedAt: now0 - 10_000 });

test('end-to-end: a REAL refresh then evaluateNewAdmission denies at declared 4 and admits on history (the inert-lookup regression)', () => {
  const root = primed([]);
  const cpuSample = { hostBusyCores: 3 + 2.2, cores: 10, stale: false };
  const run = () => evaluateNewAdmission(root, baseCfg(), ticket(), [coldLease(), settledLease()], cpuSample, null);
  const declared = run();
  assert.equal(declared.admit, false);
  assert.equal(declared.reason, 'projected-over-budget');
  assert.ok(Math.abs(declared.projectedBusy - 11.75) < 1e-9, String(declared.projectedBusy));
  assert.equal(declared.candidateEstimateSource, 'declared');

  const root2 = primed(rows(KEY, 8, 0.8)); // a real file read by the production refresh; admission below uses the real clock
  const history = evaluateNewAdmission(root2, baseCfg(), ticket(), [coldLease(), settledLease()], cpuSample, null);
  assert.equal(history.candidateEstimateSource, 'history:exact');
  assert.equal(history.candidateEstimate, 0.8);
  assert.equal(history.admit, true, JSON.stringify(history));
  assert.ok(Math.abs(history.projectedBusy - 8.55) < 1e-9, String(history.projectedBusy));
});

test('admission log line shows candidateEstimate and its source', () => {
  assert.match(formatAdmissionLog({ candidateId: 'c', candidateEstimate: 0.5, candidateEstimateSource: 'history:exact' }), /candidateEstimate=0\.50\(history:exact\)/);
  assert.match(formatAdmissionLog({ candidateId: 'c', candidateEstimate: 4, candidateEstimateSource: 'declared' }), /candidateEstimate=4\.00\(declared\)/);
});

test('a just-admitted lease is charged its estimate cold; observed above it is charged observed', () => {
  primed(rows(KEY, 6, 0.8));
  const lease = { id: 'abcdef123456', key: KEY, cmd: ['npm', 'run', 'verify'], weight: 4, resources: { cpuCores: 4 }, admittedAt: now0 - 10_000 };
  assert.equal(leaseDemand(lease, now0, baseCfg()), 0.8);
  assert.equal(leaseDemand({ ...lease, observedCpuCores: 2.5, observedAt: now0 }, now0, baseCfg()), 2.5);
});

// main's leaseDemandBasis at e10ab8e, verbatim apart from the helper imports
function mainLeaseDemandBasis(lease, now, cfg) {
  const cold = coldStartEstimate(leaseCpuCores(lease) ?? lease.weight);
  const observed = freshObservedCores(lease, now);
  if (observed === null) return { demand: cold, basis: 'cold' };
  const unsettled = { demand: Math.max(observed, cold), basis: 'cold' };
  if (cfg?.settledDemandEnabled !== true) return unsettled;
  if (!Number.isFinite(lease.admittedAt) || now - lease.admittedAt < cfg.settledDemandSettleMs) return unsettled;
  const h = Array.isArray(lease.observedCpuHistory) ? lease.observedCpuHistory : [];
  const inWindow = h.filter((e) => e && Number.isFinite(e.at) && Number.isFinite(e.cores) && e.cores >= 0 && now - e.at <= cfg.settledDemandWindowMs);
  if (inWindow.length < 2) return unsettled;
  const peak = Math.max(observed, ...inWindow.map((e) => e.cores));
  const padded = Math.min(cold, Math.max(cold * cfg.settledDemandFloorFraction, peak * cfg.settledDemandHeadroom));
  return { demand: Math.max(observed, padded), basis: 'settled' };
}

test('settled demand is main\'s, uncapped by an estimate: golden grid against main\'s leaseDemandBasis', () => {
  const cfg = baseCfg();
  const leases = [];
  for (const weight of [1, 2, 4]) {
    for (const obs of [undefined, 0.1, 1, 2.2, 3.9, 6]) {
      for (const agoMs of [10_000, 300_000]) {
        for (const hist of [[], [[10_000, 3.8], [5_000, 0.2]], [[10_000, 0.2], [5_000, 0.2]], [[200_000, 3.8], [5_000, 0.2], [4_000, 0.2]]]) {
          leases.push({
            id: 'g', weight, admittedAt: now0 - agoMs, ...(obs === undefined ? {} : { observedCpuCores: obs, observedAt: now0 }),
            observedCpuHistory: hist.map(([a, c]) => ({ at: now0 - a, cores: c })),
          });
        }
      }
    }
  }
  primed([]);
  for (const l of leases) assert.deepEqual(leaseDemandBasis(l, now0, cfg), mainLeaseDemandBasis(l, now0, cfg), JSON.stringify(l));
  // with a LOW estimate present for the very same workload, a settled lease is still charged main's trailing-peak allowance
  primed(rows(KEY, 8, 0.5));
  const keyed = leases.map((l) => ({ ...l, key: KEY, cmd: CMD }));
  for (const l of keyed) {
    const got = leaseDemandBasis(l, now0, cfg);
    const main = mainLeaseDemandBasis(l, now0, cfg);
    if (main.basis === 'settled') assert.deepEqual(got, main, JSON.stringify(l));
  }
});

test('fingerprint: the same script from two worktrees, the primary checkout and a remote work dir is ONE workload', () => {
  const fp = (root) => commandFingerprint(`bash ${root}/.githooks/pre-push --gate-body`);
  const one = fp('/Volumes/Lexar/worktrees/agent_brain/librarian-sweep-355');
  assert.equal(one, fp('/Volumes/Lexar/worktrees/agent_brain/budget-answers'));
  assert.equal(one, fp('/Users/andrew/proj/agent_brain'));
  assert.equal(one, fp('/Volumes/Lexar/proj/agent_brain'));
  assert.equal(one, fp('/Users/andrew/proj/zirkbot/.worktree/zirk-1'));
  assert.equal(one, fp('/Users/andrew/.cache/lane-broker/remote/tickets/0b5e3c1e-5a4f-4f3a-9c1d-2a7e3f4d5b6c/work'));
  assert.equal(one, fp('~/.cache/lane-broker/remote/tickets/0b5e3c1e-5a4f-4f3a-9c1d-2a7e3f4d5b6c/work'));
});

test('fingerprint: different test-file args, flags, and numbers stay different workloads', () => {
  const wt = '/Volumes/Lexar/worktrees/zirkbot/zirk1';
  const fp = (args) => commandFingerprint(`npx vitest run ${args} --root ${wt}`);
  assert.notEqual(fp('src/a.test.ts'), fp('src/b.test.ts'));
  assert.notEqual(fp('src/a.test.ts'), fp(''));
  assert.notEqual(fp('src/a.test.ts --retry 1'), fp('src/a.test.ts --retry 2'));
  assert.notEqual(commandFingerprint('git checkout 9afe0c19'), commandFingerprint('git checkout f2e7b325'));
});

test('fingerprint: a path under the run\'s TMPDIR is normalised, the rest of the name is not', () => {
  const fp = (t) => commandFingerprint(`node run.js --out ${t}/lane-out`);
  assert.equal(fp('/private/var/folders/ab/cd1234/T'), fp('/var/folders/zz/yy9999/T'));
  assert.equal(fp('/private/var/folders/ab/cd1234/T'), fp('/tmp'));
  assert.notEqual(commandFingerprint('node run.js --out /tmp/a'), commandFingerprint('node run.js --out /tmp/b'));
});

test('history from different worktrees pools into one workload for the estimate', () => {
  const at = (slug) => (i) => row(KEY, 0.8, { command: `bash /Volumes/Lexar/worktrees/cairn/${slug}/scripts/verify.sh`, endedAt: now0 - 10_000 - i });
  primed([0, 1, 2].map(at('a')).concat([3, 4, 5].map(at('b'))));
  const t = ticket({ command: 'bash /Users/andrew/proj/cairn/scripts/verify.sh' });
  assert.deepEqual(ticketCpuEstimateBasis(t, baseCfg(), now0), { cores: 0.8, source: 'history:exact' });
});
