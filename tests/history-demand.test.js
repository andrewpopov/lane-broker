import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, gitFixture } from './helpers.js';
import { paths } from '../src/state.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { evaluateNewAdmission, leaseDemand, leaseDemandBasis, ticketCpuEstimateBasis, formatAdmissionLog, freshObservedCores, coldStartEstimate } from '../src/admission.js';
import { leaseCpuCores } from '../src/resources.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { computeCpuEstimates, refreshCpuEstimates, peekCpuEstimates, argvFingerprint, REFRESH_MS, SNAPSHOT_MAX_AGE_MS } from '../src/cpu-estimates.js';

// BRAIN-433: admission charges a workload's history-informed PEAK CPU, not its declaration. Every test here goes through
// the production signatures: a real history.jsonl in a state root, refreshCpuEstimates, then the real admission functions.
const REPO = '_Volumes_Lexar_proj_cairn_.git';
const KEY = `${REPO}:qwen-552a`;
const fpOf = (argv) => argvFingerprint(argv, { root: '/w/root', tmp: '/w/tmp' });
const FP = fpOf(['npm', 'run', 'verify']);
const baseCfg = (over = {}) => ({ ...DEFAULT_GLOBAL_CONFIG, cpuAdmissionPercent: 90, cpuReserveCores: 1, admissionCooldownMs: 0, ...over });
const now0 = Date.now();

function row(key, peak, over = {}) {
  const [repo, lane] = [key.slice(0, key.lastIndexOf(':')), key.slice(key.lastIndexOf(':') + 1)];
  return { key, repo, lane, cmdFingerprint: FP, executor: 'local', resources: { cpuCores: 4 }, exit: 0, startedAt: now0 - 70_000, endedAt: now0 - 10_000, runMs: 60_000, observedCpu: { peak, mean: peak / 4, samples: 10 }, ...over };
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
const ticket = (over = {}) => ({ id: 'cand0000', key: KEY, cmdFingerprint: FP, weight: 4, resources: { cpuCores: 4, memoryBytes: 1 }, ...over });

test('history present: 5+ runs peaking ~0.8 on a 4-core declaration is charged ~0.8', () => {
  primed(rows(KEY, 6, 0.8));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), baseCfg(), now0), { cores: 0.8, source: 'history:exact' });
});

test('historyDemandRaise off: cap at the declaration, floor at 0.5', () => {
  primed(rows(KEY, 6, 7));
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg({ historyDemandRaise: false }), now0, 9).cores, 4);
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg({ historyDemandRaise: false }), now0, 9).source, 'history:exact');
  primed(rows(KEY, 6, 0.05));
  assert.equal(ticketCpuEstimateBasis(ticket(), baseCfg(), now0).cores, 0.5);
});

// BRAIN-454: history may RAISE a charge for an under-declared lane, capped at the budget the admission predicate uses.
test('BRAIN-454: an under-declared lane whose p90 exceeds its declaration is charged the p90 (history:raised)', () => {
  primed(rows(KEY, 6, 7));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), baseCfg(), now0, 9), { cores: 7, source: 'history:raised' });
  assert.equal(leaseDemandBasis({ ...coldLease(), key: KEY, cmdFingerprint: FP, weight: 4, resources: { cpuCores: 4, memoryBytes: 1 } }, now0, baseCfg(), 9).demand, 7, 'a lease cold charge uses the same raised value');
});

test('BRAIN-454: the raised charge is capped at the budget, so it can never be unadmittable', () => {
  primed(rows(KEY, 6, 12));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), baseCfg(), now0, 9), { cores: 9, source: 'history:raised' });
  const cpuSample = { hostBusyCores: 0, cores: 10, stale: false }; // budget = min(0.9 * 10, 10 - 1) = 9
  const decision = evaluateNewAdmission(primed(rows(KEY, 6, 12)), baseCfg(), ticket(), [{ ...coldLease(), observedCpuCores: 0, observedAt: now0 }], cpuSample, null);
  assert.equal(decision.candidateEstimate, 9, 'charged the budget, not the 12-core p90');
  assert.equal(decision.budget, 9);
});

test('BRAIN-454: with NO held lease the raise never applies, so persistent ambient load cannot make a lane unadmittable', () => {
  const cpuSample = { hostBusyCores: 2, cores: 10, stale: false }; // budget 9, ambient 2
  const root = primed(rows(KEY, 6, 12)); // declared 4, p90 12
  const idle = evaluateNewAdmission(root, baseCfg(), ticket(), [], cpuSample, null);
  assert.equal(idle.candidateEstimate, 4, 'declared, exactly as before 454');
  assert.equal(idle.candidateEstimateSource, 'history:exact');
  assert.equal(idle.projectedBusy, 6);
  assert.equal(idle.admit, true, JSON.stringify(idle));
  const concurrent = evaluateNewAdmission(root, baseCfg(), ticket(), [coldLease()], cpuSample, null);
  assert.equal(concurrent.candidateEstimateSource, 'history:raised', 'with another lease held the raise applies');
});

test('BRAIN-454: backfill selection charges an elastic ticket as admission does, so a ticket that cannot fit never keeps winning', async () => {
  const { selectResourceCandidate } = await import('../src/scheduler.js');
  const KEY_E = `${REPO}:elastic`;
  primed([...rows(KEY_E, 6, 9, { resources: { cpuCores: 3 } }), ...rows(KEY_E, 6, 9, { resources: { cpuCores: 2 } })]);
  const cfg = baseCfg();
  const budget = 9;
  const held = [{ id: 'held00000000', key: 'x:held', weight: 3, admittedAt: now0 }]; // charged 3: 3 external + 3 held = 6, so 3 cores free
  const head = { id: 'head', key: 'x:head', weight: 1, resources: { cpuCores: 8, memoryBytes: 1 } };
  const elastic = ticket({ id: 'elastic', key: KEY_E, weight: 1, resources: { cpuCores: 3, minCpuCores: 2, memoryBytes: 1 } });
  const ordinary = ticket({ id: 'ordinary', key: 'x:ordinary', cmdFingerprint: undefined, weight: 3, resources: { cpuCores: 3, memoryBytes: 1 } });
  const record = { headId: 'head', budget, externalBusy: 3 };
  const picked = selectResourceCandidate([head, elastic, ordinary], held, 3, 10, record, cfg, now0);
  assert.equal(picked?.id, 'ordinary', 'p90 9 at every grant means the elastic ticket is charged 9 and cannot fit 3 free cores');
});

test('BRAIN-454: selection ranks an elastic ticket only over the grants admission tries (capped at floor(headroom))', async () => {
  const { selectResourceCandidate } = await import('../src/scheduler.js');
  const KEY_E = `${REPO}:elastic`;
  const at = (cores, peak) => rows(KEY_E, 6, peak, { resources: { cpuCores: cores } });
  primed([...at(6, 9), ...at(3, 9), ...at(2, 9), ...at(4, 1)]);
  const held = [{ id: 'held00000000', key: 'x:held', weight: 3, admittedAt: now0 }]; // headroom = 9 - (3 external + 3 held) = 3
  const head = { id: 'head', key: 'x:head', weight: 1, resources: { cpuCores: 8, memoryBytes: 1 } };
  const elastic = ticket({ id: 'elastic', key: KEY_E, weight: 1, resources: { cpuCores: 6, minCpuCores: 2, memoryBytes: 1 } });
  const ordinary = ticket({ id: 'ordinary', key: 'x:ordinary', cmdFingerprint: undefined, weight: 3, resources: { cpuCores: 3, memoryBytes: 1 } });
  const picked = selectResourceCandidate([head, elastic, ordinary], held, 3, 10, { headId: 'head', budget: 9, externalBusy: 3 }, baseCfg(), now0);
  assert.equal(picked?.id, 'ordinary', 'grant 4 (charge 1) is above floor(headroom) = 3, so admission never tries it');
});

test('BRAIN-454: with historyDemandRaise false the charge is byte-identical to the lowering-only behaviour', () => {
  primed(rows(KEY, 6, 7));
  const off = baseCfg({ historyDemandRaise: false });
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), off, now0, 9), { cores: 4, source: 'history:exact' });
  primed(rows(KEY, 6, 0.8));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), off, now0, 9), { cores: 0.8, source: 'history:exact' });
});

test('BRAIN-454: lane status shows the raised value and source for a queued ticket', async () => {
  const { collectStatus, renderStatusText } = await import('../src/status.js');
  const { enqueue } = await import('../src/scheduler.js');
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, cpuAdmissionPercent: 100, cpuReserveCores: 0, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  process.env.LANE_BROKER_HOME = home;
  const root = primed(rows(KEY, 6, 1.5, { resources: { cpuCores: 1 } }));
  writeLease(root, { id: 'h454', key: 'x:held', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING });
  await enqueue(root, { id: 'q454', key: KEY, cmdFingerprint: FP, weight: 1, resources: { cpuCores: 1, memoryBytes: 1 }, supervisorPid: process.pid, supervisorStart: null });
  const status = await collectStatus();
  assert.deepEqual(status.queued[0].cpuEstimate, { cores: 1.5, source: 'history:raised' });
  assert.match(renderStatusText(status), /cpu~1\.50\(history:raised\)/);
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
  assert.deepEqual(ticketCpuEstimateBasis(ticket({ cmdFingerprint: fpOf(['npm', 'run', 'e2e']) }), baseCfg(), now0), { cores: 4, source: 'declared' });
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
  primed([...sib('qwen-111a', 3, 0.8, { configLane: 'default', cmdFingerprint: fpOf(['other']) }), ...sib('default', 3, 0.8)]);
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
  const lease = { id: 'abcdef123456', key: KEY, cmdFingerprint: FP, weight: 4, resources: { cpuCores: 4 }, admittedAt: now0 - 10_000 };
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
  const keyed = leases.map((l) => ({ ...l, key: KEY, cmdFingerprint: FP }));
  for (const l of keyed) {
    const got = leaseDemandBasis(l, now0, cfg);
    const main = mainLeaseDemandBasis(l, now0, cfg);
    if (main.basis === 'settled') assert.deepEqual(got, main, JSON.stringify(l));
  }
});

const fpAt = (argv, root, tmp = '/var/folders/ab/cd/T') => argvFingerprint(argv, { root, tmp });

test('fingerprint: the same command from two worktrees of one repo is ONE workload (each run normalises its own root)', () => {
  const script = (root) => ['bash', `${root}/.githooks/pre-push`, '--gate-body', `--root=${root}`];
  const a = '/Volumes/Lexar/worktrees/agent_brain/slug-a';
  const b = '/Volumes/Lexar/worktrees/agent_brain/slug-b';
  assert.equal(fpAt(script(a), a), fpAt(script(b), b));
  assert.equal(fpAt(script(a), a), fpAt(script('/Users/andrew/proj/agent_brain'), '/Users/andrew/proj/agent_brain'));
  assert.notEqual(fpAt(script(a), a), fpAt(script(a), '/elsewhere'), 'a root that is not the run\'s own is not rewritten');
});

test('fingerprint: another repo\'s path in argv is not normalised, so small and large differ', () => {
  const root = '/Users/andrew/proj/zirkbot';
  assert.notEqual(fpAt(['node', '/Users/andrew/proj/small/scripts/verify.js'], root), fpAt(['node', '/Users/andrew/proj/large/scripts/verify.js'], root));
  assert.notEqual(fpAt(['node', '/Users/andrew/proj/zirkbot-other/x.js'], root), fpAt(['node', '<repo>-other/x.js'], root), 'a sibling that merely shares the prefix text is not the root');
});

test('fingerprint: argv boundaries survive, and a long common prefix with a different suffix differs', () => {
  assert.notEqual(fpAt(['bash', '-c', 'true', ';', 'node', 'heavy.js'], '/r'), fpAt(['bash', '-c', 'true ; node heavy.js'], '/r'));
  const prefix = 'x'.repeat(400);
  assert.notEqual(fpAt(['vitest', prefix, 'a.test.ts'], '/r'), fpAt(['vitest', prefix, 'b.test.ts'], '/r'));
  assert.notEqual(fpAt(['run', '--workers', '1'], '/r'), fpAt(['run', '--workers', '8'], '/r'));
});

test('fingerprint: test-file args and numbers stay distinct; the run\'s own TMPDIR is normalised', () => {
  assert.notEqual(fpAt(['vitest', 'src/a.test.ts'], '/r'), fpAt(['vitest', 'src/b.test.ts'], '/r'));
  assert.notEqual(fpAt(['vitest'], '/r'), fpAt(['vitest', 'src/a.test.ts'], '/r'));
  assert.equal(fpAt(['node', 'x.js', '--out', '/var/folders/ab/cd/T/out'], '/r', '/var/folders/ab/cd/T'), fpAt(['node', 'x.js', '--out', '/tmp/q/out'], '/r', '/tmp/q'));
  assert.equal(fpAt([], '/r'), null);
});

test('grant is part of the key: 1-core history gives an 8-core candidate no relief', () => {
  primed(rows(KEY, 8, 0.8, { resources: { cpuCores: 1 } }));
  assert.deepEqual(ticketCpuEstimateBasis(ticket({ resources: { cpuCores: 8, memoryBytes: 1 }, weight: 8 }), baseCfg(), now0), { cores: 8, source: 'declared' });
  assert.deepEqual(ticketCpuEstimateBasis(ticket({ resources: { cpuCores: 1, memoryBytes: 1 }, weight: 1 }), baseCfg(), now0), { cores: 0.8, source: 'history:exact' });
});

test('a legacy row without cmdFingerprint is ignored', () => {
  primed(rows(KEY, 8, 0.8).map(({ cmdFingerprint, ...legacy }) => ({ ...legacy, command: 'npm run verify' })));
  assert.deepEqual(ticketCpuEstimateBasis(ticket(), baseCfg(), now0), { cores: 4, source: 'declared' });
  assert.deepEqual(ticketCpuEstimateBasis({ key: KEY, weight: 4, resources: { cpuCores: 4 } }, baseCfg(), now0), { cores: 4, source: 'declared' }, 'a ticket without one never matches');
});

test('history from different worktrees pools: rows fingerprinted by their own root match a ticket from another worktree', () => {
  const run = (root) => (i) => row(KEY, 0.8, { cmdFingerprint: fpAt(['bash', `${root}/scripts/verify.sh`], root), endedAt: now0 - 10_000 - i });
  primed([0, 1, 2].map(run('/Volumes/Lexar/worktrees/cairn/a')).concat([3, 4, 5].map(run('/Users/andrew/proj/cairn'))));
  const t = ticket({ cmdFingerprint: fpAt(['bash', '/Volumes/Lexar/worktrees/cairn/c/scripts/verify.sh'], '/Volumes/Lexar/worktrees/cairn/c') });
  assert.deepEqual(ticketCpuEstimateBasis(t, baseCfg(), now0), { cores: 0.8, source: 'history:exact' });
});

test('end to end: a real `lane run` records the persisted fingerprint, normalised against its own checkout root', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  fs.mkdirSync(repoDir, { recursive: true });
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  gitFixture(['init', '-q'], repoDir);
  gitFixture(['add', '-A'], repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], repoDir);
  const root = fs.realpathSync(repoDir);
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'true', `${root}/x.sh`], { env, cwd: repoDir });
  assert.equal(result.code, 0, result.stderr);
  const [hist] = fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(hist.cmdFingerprint, argvFingerprint(['sh', '-c', 'true', `${root}/x.sh`], { root, tmp: env.TMPDIR ?? '/tmp' }));
  assert.match(hist.cmdFingerprint, /^[0-9a-f]{64}$/);
});

test('end to end: a run whose checkout root is unknown (not a git repo) records no fingerprint, so it is charged declared', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const dir = path.join(base, 'not-a-repo', 'packages', 'heavy');
  fs.mkdirSync(dir, { recursive: true });
  writeRepoConfig(dir, { version: 1, lanes: { default: { weight: 1 } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'true'], { env, cwd: dir });
  assert.equal(result.code, 0, result.stderr);
  const [hist] = fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(hist.cmdFingerprint, undefined);
});

test('fingerprint: the execution directory is part of the workload (packages/small vs packages/heavy), pooled across worktrees', () => {
  const a = '/Volumes/Lexar/worktrees/mono/slug-a';
  const b = '/Volumes/Lexar/worktrees/mono/slug-b';
  const fp = (root, rel) => argvFingerprint(['npm', 'test'], { root, cwd: rel ? `${root}/${rel}` : root });
  assert.notEqual(fp(a, 'packages/small'), fp(a, 'packages/heavy'));
  assert.notEqual(fp(a, 'packages/small'), fp(a, ''));
  assert.equal(fp(a, 'packages/small'), fp(b, 'packages/small'));
  assert.notEqual(argvFingerprint(['npm', 'test'], { root: a, cwd: '/elsewhere/x' }), argvFingerprint(['npm', 'test'], { root: a, cwd: '/elsewhere/y' }), 'outside the root keeps its path');
});
