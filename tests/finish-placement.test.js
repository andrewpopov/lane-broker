import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeRunner, tmpDir } from './remote-harness.js';
import { selectRunner, waitSlots } from '../src/remote-client.js';
import { DEFAULT_REMOTE_OVERHEAD_MS, jobClass, pairingCommand, RANKED_WAIT_RESIDUAL, runnerRunEstimateMs, runnerRunEstimatesMs, runnerWaitMs } from '../src/speed-placement.js';

// ---- runnerRunEstimateMs: the tiers, the donor and its scaling ----

const REPO = 'lane-broker';
const LANE = 'pr';
const CMD = 'sh scripts/test-run.sh tests/a.test.js';
const SKYBOX = { name: 'skybox', speedFactor: 1 };
const WINTOP = { name: 'wintop', speedFactor: 0.5 };
const BIGPI = { name: 'bigpi' };
const CONFIGURED = [SKYBOX, WINTOP];
const remoteRow = (runner, runMs, extra = {}) => ({
  repo: REPO, lane: LANE, command: CMD, executor: 'remote', runner, exit: 0, startedAt: 1000, endedAt: 1000 + runMs, runMs, ...extra,
});
const job = (extra = {}) => ({ repo: REPO, lane: LANE, command: CMD, ...extra });
const rowsOf = (runner, ...runMs) => runMs.map((ms) => remoteRow(runner, ms));

test('runnerRunEstimateMs: the exact tier is the median of this command\'s successful runs on that runner', () => {
  assert.deepEqual(runnerRunEstimateMs(rowsOf('skybox', 180_000, 200_000, 260_000), SKYBOX, job(), CONFIGURED), { ms: 200_000, tier: 'exact', n: 3 });
  assert.deepEqual(runnerRunEstimateMs(rowsOf('skybox', 180_000, 220_000), SKYBOX, job(), CONFIGURED), { ms: 200_000, tier: 'exact', n: 2 }, 'two runs are enough');
  assert.deepEqual(runnerRunEstimateMs(rowsOf('wintop', 400_000, 500_000), WINTOP, job(), CONFIGURED), { ms: 450_000, tier: 'exact', n: 2 }, 'another runner\'s rows are not this one\'s evidence');
});

test('runnerRunEstimateMs: shards of one lane keep their own durations through the exact tier', () => {
  const shard = (i, ms) => remoteRow('skybox', ms, { command: `env SHARD=${i} sh scripts/test-run.sh` });
  const rows = [shard(1, 100_000), shard(1, 120_000), shard(2, 400_000), shard(2, 500_000)];
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, job({ command: 'env SHARD=1 sh scripts/test-run.sh' }), CONFIGURED), { ms: 110_000, tier: 'exact', n: 2 });
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, job({ command: 'env SHARD=2 sh scripts/test-run.sh' }), CONFIGURED), { ms: 450_000, tier: 'exact', n: 2 });
});

test('runnerRunEstimateMs: two runs differing only in a per-run sha are one exact key', () => {
  const cmd = (c) => `env TRAIN_BASE_SHA=${c.repeat(40)} sh .githooks/pre-push-gate.sh verify-full`;
  const rows = [remoteRow('skybox', 1_700_000, { command: cmd('a') }), remoteRow('skybox', 1_800_000, { command: cmd('b') })];
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, job({ command: cmd('c') }), CONFIGURED), { ms: 1_750_000, tier: 'exact', n: 2 });
  assert.equal(pairingCommand(cmd('a')), pairingCommand(cmd('b')), 'the pairing key is what makes them one tier');
});

test('runnerRunEstimateMs: the exact tier beats the repo-lane median, which needs three runs', () => {
  const broad = rowsOf('skybox', 10_000, 20_000, 30_000).map((r) => ({ ...r, command: 'sh other.sh' }));
  const exact = rowsOf('skybox', 500_000, 600_000);
  assert.deepEqual(runnerRunEstimateMs([...broad, ...exact], SKYBOX, job(), CONFIGURED), { ms: 550_000, tier: 'exact', n: 2 });
  assert.deepEqual(runnerRunEstimateMs([...broad, ...exact.slice(0, 1)], SKYBOX, job(), CONFIGURED), { ms: 25_000, tier: 'repo-lane', n: 4 });
  assert.equal(runnerRunEstimateMs(broad.slice(0, 2), SKYBOX, job(), CONFIGURED), null, 'two repo-lane runs are not evidence');
});

test('runnerRunEstimateMs: a runner with no rows of its own takes the donor median scaled by their speedFactors', () => {
  const rows = rowsOf('skybox', 200_000, 220_000);
  assert.deepEqual(runnerRunEstimateMs(rows, WINTOP, job(), CONFIGURED), { ms: 420_000, tier: 'exact', n: 2, donor: 'skybox' });
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, job(), CONFIGURED), { ms: 210_000, tier: 'exact', n: 2 }, 'the donor\'s own rows are never scaled');
  assert.deepEqual(runnerRunEstimateMs(rows, BIGPI, job(), [SKYBOX, BIGPI]), { ms: 210_000, tier: 'exact', n: 2, donor: 'skybox' }, 'an unset speedFactor is 1');
});

test('runnerRunEstimateMs: the donor is the runner with the most matching rows, ties going to config order', () => {
  const rows = [...rowsOf('skybox', 100_000, 100_000, 100_000), ...rowsOf('wintop', 300_000, 300_000)];
  assert.deepEqual(runnerRunEstimateMs(rows, BIGPI, job(), [SKYBOX, WINTOP, BIGPI]), { ms: 100_000, tier: 'exact', n: 3, donor: 'skybox' });
  const tied = [...rowsOf('skybox', 100_000, 100_000), ...rowsOf('wintop', 300_000, 300_000)];
  assert.deepEqual(runnerRunEstimateMs(tied, BIGPI, job(), [SKYBOX, WINTOP, BIGPI]).donor, 'skybox');
  assert.deepEqual(runnerRunEstimateMs(tied, BIGPI, job(), [WINTOP, SKYBOX, BIGPI]).donor, 'wintop');
});

test('runnerRunEstimateMs: own evidence beats donor evidence, and a donor exact tier beats its repo-lane one', () => {
  const own = rowsOf('wintop', 900_000, 900_000, 900_000);
  const donor = rowsOf('skybox', 100_000, 100_000);
  const got = runnerRunEstimateMs([...own, ...donor], WINTOP, job(), CONFIGURED);
  assert.deepEqual([got.ms, got.donor], [900_000, undefined], 'three repo-lane runs on the runner itself beat an exact donor');
  const mixed = [...donor, ...rowsOf('skybox', 40_000, 50_000, 60_000).map((r) => ({ ...r, command: 'sh other.sh' }))];
  assert.deepEqual(runnerRunEstimateMs(mixed, BIGPI, job(), [SKYBOX, BIGPI]), { ms: 100_000, tier: 'exact', n: 2, donor: 'skybox' });
});

test('runnerRunEstimateMs: null with no rows, failed exits, non-finite durations, or no command to pair on', () => {
  const rows = rowsOf('skybox', 200_000, 210_000, 220_000);
  assert.equal(runnerRunEstimateMs([], SKYBOX, job(), CONFIGURED), null);
  assert.equal(runnerRunEstimateMs(rows.map((r) => ({ ...r, exit: 1 })), SKYBOX, job(), CONFIGURED), null, 'a failed run says nothing about duration');
  for (const runMs of [undefined, null, NaN, Infinity, 0, -5, '200000']) {
    assert.equal(runnerRunEstimateMs(rows.map((r) => ({ ...r, runMs })), SKYBOX, job(), CONFIGURED), null, String(runMs));
  }
  assert.equal(runnerRunEstimateMs(rows.map((r) => ({ ...r, executor: 'local', runner: undefined })), SKYBOX, job(), CONFIGURED), null);
  assert.equal(runnerRunEstimateMs(rows.slice(0, 2), SKYBOX, { repo: REPO, lane: LANE }, CONFIGURED), null, 'without a command only the repo-lane tier can hit, and it has no three rows');
  assert.equal(runnerRunEstimateMs(rows, SKYBOX, null, CONFIGURED), null);
});

test('runnerRunEstimateMs: the same command in another repo or lane never matches', () => {
  const elsewhere = rowsOf('skybox', 50_000, 60_000, 70_000).map((r) => ({ ...r, repo: 'rouge' }));
  assert.equal(runnerRunEstimateMs(elsewhere, SKYBOX, job(), CONFIGURED), null);
  const otherLane = rowsOf('skybox', 50_000, 60_000, 70_000).map((r) => ({ ...r, lane: 'train' }));
  assert.equal(runnerRunEstimateMs(otherLane, SKYBOX, job(), CONFIGURED), null);
  const mixed = [...elsewhere, ...rowsOf('skybox', 900_000, 910_000, 920_000).map((r) => ({ ...r, command: 'sh other.sh' }))];
  assert.deepEqual(runnerRunEstimateMs(mixed, SKYBOX, job(), CONFIGURED), { ms: 910_000, tier: 'repo-lane', n: 3 }, 'only this repo and lane count');
});

// ---- runnerRunEstimateMs: the BRAIN-899 B2 recency of a tier median ----

/** `n` runs of this exact command on `runner`, started one second apart from `firstStartedAt`, in the order given. */
const runsFrom = (runner, firstStartedAt, ...runMs) =>
  runMs.map((ms, i) => remoteRow(runner, ms, { startedAt: firstStartedAt + i * 1000, endedAt: firstStartedAt + i * 1000 + ms }));

test('runnerRunEstimateMs: the median covers the newest 20 matching runs by startedAt, not the append order', () => {
  const stale = runsFrom('skybox', 1000, 600_000, 600_000, 600_000, 600_000, 600_000);
  const recent = runsFrom('skybox', 100_000, ...Array.from({ length: 20 }, () => 100_000));
  const appendedLast = runnerRunEstimateMs([...recent, ...stale], SKYBOX, job(), CONFIGURED);
  assert.deepEqual(appendedLast, { ms: 100_000, tier: 'exact', n: 20 }, 'the five stale runs were appended last but started first');
  assert.deepEqual(runnerRunEstimateMs([...stale, ...recent], SKYBOX, job(), CONFIGURED), appendedLast, 'append order decides nothing');
  assert.deepEqual(runnerRunEstimateMs(runsFrom('skybox', 1000, 100_000, 600_000, 200_000), SKYBOX, job(), CONFIGURED), { ms: 200_000, tier: 'exact', n: 3 }, 'under 20 runs, every one still counts');
});

test('runnerRunEstimateMs: a run without startedAt sorts oldest, so the newest-20 drops it first', () => {
  const recent = runsFrom('skybox', 100_000, ...Array.from({ length: 20 }, () => 100_000));
  const undated = [remoteRow('skybox', 600_000, { startedAt: undefined })];
  assert.deepEqual(runnerRunEstimateMs([...undated, ...recent], SKYBOX, job(), CONFIGURED), { ms: 100_000, tier: 'exact', n: 20 });
  assert.deepEqual(runnerRunEstimateMs(runsFrom('skybox', 1000, 100_000, 200_000).concat(undated), SKYBOX, job(), CONFIGURED), { ms: 200_000, tier: 'exact', n: 3 }, 'under 20 it still counts, as a row without startedAt always has');
});

// ---- runnerRunEstimateMs: the BRAIN-916 class tier ----

const TRAIN_REPO = 'durvivors';
const TRAIN_LANE = 'train';
const IDS = 'abcdef0123456789';
const trainCommand = ({ shard = null, tier = 'pr', id, files = null }) =>
  `env ${shard === null ? '' : `TRAIN_SHARD=${shard} TRAIN_SHARDS=3 `}env TRAIN_TIER=${tier} TRAIN_BASE=${id.repeat(40)} TRAIN_TIP=${id.repeat(40)} TRAIN_CHANGED_FILES_PATH=.train-changed-files TRAIN_CHANGED_FILES=${files ?? `src/pages/${id}.html\nsrc/lib/${id}.js`} sh .githooks/pre-push-gate.sh verify-full`;
const trainRow = (runner, runMs, parts) => ({ repo: TRAIN_REPO, lane: TRAIN_LANE, command: trainCommand(parts), executor: 'remote', runner, exit: 0, startedAt: 1000, endedAt: 1000 + runMs, runMs });
const trainJob = (parts) => ({ repo: TRAIN_REPO, lane: TRAIN_LANE, command: trainCommand(parts) });
const trainRows = (runner) => [
  ...['1/3', '2/3', '3/3'].flatMap((shard, s) => [0, 1, 2].map((i) => trainRow(runner, 400_000 + s * 100_000 + i * 20_000, { shard, id: IDS[s * 3 + i] }))),
  trainRow(runner, 3_000_000, { id: '3' }),
  trainRow(runner, 3_200_000, { id: '4' }),
  trainRow(runner, 5_000_000, { tier: 'master', id: '5' }),
];
const REPO_LANE_MEDIAN = 570_000;

test('jobClass: the sorted low-cardinality assignments, without the per-run ones', () => {
  const rows = trainRows('skybox');
  assert.equal(jobClass(trainCommand({ shard: '1/3', id: 'ff' }), rows), 'TRAIN_CHANGED_FILES_PATH=.train-changed-files TRAIN_SHARD=1/3 TRAIN_SHARDS=3 TRAIN_TIER=pr', 'the shas and the 12-value changed-file list are gone');
  assert.equal(jobClass(trainCommand({ id: 'ff' }), rows), 'TRAIN_CHANGED_FILES_PATH=.train-changed-files TRAIN_TIER=pr');
  assert.equal(jobClass('sh scripts/test-run.sh', rows), null);
  assert.equal(jobClass(trainCommand({ shard: '1/3', id: 'ff' }), []), null, 'with no lane history no name is low-cardinality');
});

test('runnerRunEstimateMs: a train lane\'s shards and tiers keep their own medians through the class tier', () => {
  const rows = trainRows('skybox');
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, trainJob({ shard: '1/3', id: 'ff' }), CONFIGURED), { ms: 420_000, tier: 'class', n: 3 }, `not the ${REPO_LANE_MEDIAN / 1000}s repo-lane median that mixes 400s shards with 3000s unsharded runs`);
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, trainJob({ shard: '2/3', id: 'ff' }), CONFIGURED), { ms: 520_000, tier: 'class', n: 3 });
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, trainJob({ shard: '3/3', id: 'ff' }), CONFIGURED), { ms: 620_000, tier: 'class', n: 3 });
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, trainJob({ id: 'ff' }), CONFIGURED), { ms: 3_100_000, tier: 'class', n: 2 }, 'an unsharded pr run is a class of its own');
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, trainJob({ tier: 'master', id: 'ff' }), CONFIGURED), { ms: REPO_LANE_MEDIAN, tier: 'repo-lane', n: 12 }, 'one run is no class evidence, so the master tier falls back');
});

test('runnerRunEstimateMs: the exact tier still beats the class one', () => {
  const same = { shard: '1/3', files: 'src/pages/classic.html\nsrc/lib/same.js' };
  const rows = [...trainRows('skybox'), trainRow('skybox', 700_000, { ...same, id: 'aa' }), trainRow('skybox', 900_000, { ...same, id: 'bb' })];
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, trainJob({ ...same, id: 'cc' }), CONFIGURED), { ms: 800_000, tier: 'exact', n: 2 }, 'two runs of this very command beat the 420s class median of its shard');
});

test('runnerRunEstimateMs: the class tier scales from a donor, chosen by its class rows', () => {
  const rows = trainRows('skybox');
  assert.deepEqual(runnerRunEstimateMs(rows, WINTOP, trainJob({ shard: '2/3', id: 'ff' }), CONFIGURED), { ms: 1_040_000, tier: 'class', n: 3, donor: 'skybox' }, '520s at speedFactor 1 is 1040s at 0.5, not the 1140s a repo-lane donor would give');
  const both = [...rows, trainRow('wintop', 2_000_000, { shard: '2/3', id: 'aa' }), trainRow('wintop', 2_200_000, { shard: '2/3', id: 'bb' })];
  assert.deepEqual(runnerRunEstimateMs(both, BIGPI, trainJob({ shard: '2/3', id: 'ff' }), [SKYBOX, WINTOP, BIGPI]), { ms: 520_000, tier: 'class', n: 3, donor: 'skybox' }, 'three class rows beat two, whatever the rest of the lane says');
});

test('runnerRunEstimateMs: a repeating short changed-file value joins the class only while the lane keeps repeating it', () => {
  const classic = (runMs, id) => trainRow('skybox', runMs, { shard: '1/3', id, files: `classic.html\nsrc/lib/${id}.js` });
  const other = (runMs, id) => trainRow('skybox', runMs, { shard: '1/3', id });
  const wanted = trainJob({ shard: '1/3', id: 'ff', files: 'classic.html\nsrc/lib/ff.js' });
  const few = [classic(100_000, 'a'), classic(120_000, 'b'), other(900_000, 'c'), other(980_000, 'd')];
  assert.deepEqual(runnerRunEstimateMs(few, SKYBOX, wanted, CONFIGURED), { ms: 110_000, tier: 'class', n: 2 }, 'TRAIN_CHANGED_FILES takes 3 distinct values here, so it stays in the class key and only the classic.html rows match');
  const many = [...few, ...[0, 1, 2, 3, 4, 5, 6].map((i) => other(900_000 + i * 10_000, IDS[6 + i]))];
  assert.deepEqual(runnerRunEstimateMs(many, SKYBOX, wanted, CONFIGURED), { ms: 920_000, tier: 'class', n: 11 }, 'at 10 distinct values the name is dropped, so the classic.html runs pool with the rest of their shard class');
});

test('runnerRunEstimateMs: no kept assignment leaves the class tier out, as today', () => {
  const rows = trainRows('skybox');
  const plain = { repo: TRAIN_REPO, lane: TRAIN_LANE, command: 'sh .githooks/pre-push-gate.sh verify-full' };
  assert.deepEqual(runnerRunEstimateMs(rows, SKYBOX, plain, CONFIGURED), { ms: REPO_LANE_MEDIAN, tier: 'repo-lane', n: 12 });
  assert.equal(runnerRunEstimateMs(rows, SKYBOX, { ...plain, command: 'env BRAND_NEW=1 sh .githooks/pre-push-gate.sh verify-full' }, CONFIGURED).tier, 'repo-lane', 'a name this lane has never seen is not a class');
  assert.equal(runnerRunEstimateMs(rows, SKYBOX, { ...plain, command: 'env TRAIN_SHARD=9/9 sh .githooks/pre-push-gate.sh verify-full' }, CONFIGURED).tier, 'repo-lane', 'nor is a class no row shares');
});

// ---- runnerRunEstimatesMs: BRAIN-899 B3, the whole field at ONE tier ----

test('runnerRunEstimatesMs: a mixed-evidence pair settles on the tier every candidate reaches, donor-scaled included', () => {
  const rows = [...rowsOf('skybox', 200_000, 220_000, 240_000), ...rowsOf('wintop', 900_000, 900_000, 900_000).map((r) => ({ ...r, command: 'sh other.sh' }))];
  const all = runnerRunEstimatesMs(rows, [SKYBOX, WINTOP], job(), CONFIGURED);
  assert.deepEqual(all.get('skybox'), { ms: 220_000, tier: 'exact', n: 3 });
  assert.deepEqual(all.get('wintop'), { ms: 440_000, tier: 'exact', n: 3, donor: 'skybox' }, 'exact through the donor, not its own 900s repo-lane median');
  assert.deepEqual(runnerRunEstimateMs(rows, WINTOP, job(), CONFIGURED), { ms: 900_000, tier: 'repo-lane', n: 3 }, 'per-runner, the field would have been compared at two tiers');
});

test('runnerRunEstimatesMs: a class tier is the common one when every candidate reaches it', () => {
  const shard = trainJob({ shard: '2/3', id: 'ff' });
  const rows = [...trainRows('skybox'), ...[700_000, 800_000, 900_000].map((ms, i) => trainRow('wintop', ms, { shard: '9/9', id: IDS[i] }))];
  const all = runnerRunEstimatesMs(rows, [SKYBOX, WINTOP], shard, CONFIGURED);
  assert.deepEqual(all.get('skybox'), { ms: 520_000, tier: 'class', n: 3 });
  assert.deepEqual(all.get('wintop'), { ms: 1_040_000, tier: 'class', n: 3, donor: 'skybox' }, 'its own 3 runs are of another class, so the common tier comes from the donor');
  assert.deepEqual(runnerRunEstimateMs(rows, WINTOP, shard, CONFIGURED), { ms: 800_000, tier: 'repo-lane', n: 3 }, 'per-runner it would have mixed a repo-lane 800s against a class 520s');
});

test('runnerRunEstimatesMs: a candidate nothing can estimate is null, and it does not downgrade the others', () => {
  const rows = rowsOf('skybox', 200_000, 220_000, 240_000);
  const noDonor = runnerRunEstimatesMs(rows, [SKYBOX, WINTOP], job(), []);
  assert.deepEqual(noDonor.get('skybox'), { ms: 220_000, tier: 'exact', n: 3 }, 'no tier reaches every candidate, so each keeps its own most specific one');
  assert.equal(noDonor.get('wintop'), null, 'the one nothing can estimate, which is what leaves selectRunner unranked');
  assert.deepEqual([...runnerRunEstimatesMs([], [SKYBOX, WINTOP], job(), CONFIGURED).values()], [null, null], 'no history estimates nobody');
  assert.equal(runnerRunEstimatesMs(rows, [], job(), CONFIGURED).size, 0, 'no candidates, no estimates');
  assert.deepEqual(runnerRunEstimatesMs(rows, null, job(), CONFIGURED).size, 0);
});

// ---- runnerWaitMs ----

test('runnerWaitMs: 0 on an idle runner with room, one run per ticket ahead plus one otherwise', () => {
  assert.equal(runnerWaitMs({ probe: { queued: 0, running: 0 }, estimateMs: 200_000, room: true }), 0);
  assert.equal(runnerWaitMs({ probe: { queued: 0, running: 2 }, estimateMs: 200_000, room: false }), 200_000, 'a busy runner waits one full run whatever it reports as running');
  assert.equal(runnerWaitMs({ probe: { queued: 2, running: 1 }, estimateMs: 200_000, room: false }), 600_000);
  assert.equal(runnerWaitMs({ probe: { queued: 2, running: 8 }, estimateMs: 200_000, room: true }), 600_000, 'a queue with room still waits, and running never shortens it');
});

test('runnerWaitMs: a queued that is not a positive integer counts as 0', () => {
  for (const queued of [undefined, null, -1, 1.5, NaN, '2']) {
    assert.equal(runnerWaitMs({ probe: { queued }, estimateMs: 200_000, room: true }), 0, String(queued));
    assert.equal(runnerWaitMs({ probe: { queued }, estimateMs: 200_000, room: false }), 200_000, String(queued));
  }
  assert.equal(runnerWaitMs({ probe: null, estimateMs: 200_000, room: false }), 200_000);
});

// ---- BRAIN-899 A1: the slot-aware ranking wait ----

test('runnerWaitMs: slots divide the work ahead, and a sub-one slot count never inflates the wait', () => {
  assert.equal(runnerWaitMs({ probe: { queued: 3 }, estimateMs: 200_000, room: false, slots: 2, residual: 0.5 }), 350_000, '(3 + 0.5) x 200s / 2');
  assert.equal(runnerWaitMs({ probe: { queued: 0 }, estimateMs: 200_000, room: false, slots: 4, residual: 0.5 }), 25_000, 'a busy runner with an empty queue still charges the residual');
  assert.equal(runnerWaitMs({ probe: { queued: 0 }, estimateMs: 200_000, room: true, slots: 4, residual: 0.5 }), 0, 'idle with room is 0 however many slots it has');
  for (const slots of [0, -1, 0.5, NaN, null, undefined]) {
    assert.equal(runnerWaitMs({ probe: { queued: 1 }, estimateMs: 200_000, room: false, slots, residual: 0.5 }), 300_000, String(slots));
  }
});

test('runnerWaitMs: the residual defaults to the whole run the BRAIN-888 rebalance gate has always charged', () => {
  assert.equal(runnerWaitMs({ probe: { queued: 2 }, estimateMs: 200_000, room: false }), 600_000);
  assert.equal(runnerWaitMs({ probe: { queued: 2 }, estimateMs: 200_000, room: false, residual: RANKED_WAIT_RESIDUAL }), 500_000);
});

test('waitSlots: the CPU budget over the claim, floored at 1, and 1 whenever either figure is unusable', () => {
  const at = (cpuCores, claim = 4) => waitSlots({ weight: 1, cpuCores: claim }, { capacity: { cpuCores } });
  assert.equal(at(12), 3);
  assert.equal(at(12, 5), 2, 'a fraction of a slot is no slot');
  assert.equal(at(4), 1);
  assert.equal(at(2), 1, 'a budget smaller than the claim is still one slot, never a longer wait');
  for (const cpuCores of [null, undefined, NaN, 0, -8, Infinity, '12']) assert.equal(at(cpuCores), 1, String(cpuCores));
  for (const cpuCores of [undefined, null, NaN, 0, -1, '4']) assert.equal(waitSlots({ weight: 1, cpuCores }, { capacity: { cpuCores: 12 } }), 1, String(cpuCores));
  assert.equal(waitSlots(undefined, undefined), 1);
  assert.equal(waitSlots({ weight: 1, cpuCores: 4 }, { capacity: {} }), 1, 'an unenforced runner reports no budget');
});

// ---- selectRunner: ranking the eligible candidates by predicted finish ----

/** A fake ssh that answers `remote-probe` per destination from `probes` (name -> payload), the tests/remote-queue-choice.test.js pattern. */
function makeProbeMapSsh(probes) {
  const binDir = tmpDir('finish-placement-ssh');
  const sshBin = path.join(binDir, 'ssh');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
const probes = ${JSON.stringify(probes)};
const rest = [];
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i += 1) { if (argv[i] === '-o') { i += 1; continue; } rest.push(argv[i]); }
const p = probes[rest[0]];
if (!p) process.exit(255);
process.stdout.write(JSON.stringify(p) + '\\n');
`,
  );
  fs.chmodSync(sshBin, 0o755);
  return sshBin;
}

const probe = (queued, extra = {}) => ({ protocol: 1, protocols: [1, 2], version: '0.0.0', paused: false, queued, running: 0, capacity: {}, ...extra });
const RESERVATION = { weight: 1, cpuCores: 4, memoryBytes: 1e9 };
const ROOM = { headroom: { cpuCores: 8, memoryBytes: 1e12 } };
const LESS_ROOM = { headroom: { cpuCores: 5, memoryBytes: 1e12 } };
const NO_CPU = { headroom: { cpuCores: 0, memoryBytes: 1e12 } };
const NO_MEMORY = { headroom: { cpuCores: 8, memoryBytes: 1 } };
const runnersOf = (...names) => names.map((n) => makeRunner({ ssh: n }));
const est = (ms, extra = {}) => ({ ms, tier: 'repo-lane', n: 3, ...extra });
const modelOf = (byName) => ({ estimateFor: (runner) => byName[runner.name] ?? null });
const finishOf = (waitMs, runMs) => waitMs + runMs + DEFAULT_REMOTE_OVERHEAD_MS;
const select = (probes, runners, opts = {}) =>
  selectRunner(runners, { sshBin: makeProbeMapSsh(probes), deadlineMs: 3000, reservation: RESERVATION, maxRemoteQueue: 2, ...opts });
const names = (res) => res.scores.map((s) => s.name);

test('selectRunner finish: a busy fast runner beats an idle slow one on the place path, as a queuedChoice', async () => {
  const res = await select(
    { fast: probe(0, NO_CPU), slow: probe(0, ROOM) },
    runnersOf('fast', 'slow'),
    { busyQueueable: true, finishModel: modelOf({ fast: est(200_000, { tier: 'exact', n: 9 }), slow: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'fast');
  assert.equal(res.queuedChoice, true);
  assert.equal(res.probe.queued, 0, 'the winner\'s probe is the busy one the BRAIN-338 branch records as name(0)');
  assert.deepEqual(res.scores, [
    { name: 'fast', finishMs: finishOf(100_000, 200_000), waitMs: 100_000, runMs: 200_000, tier: 'exact', n: 9, idle: false, queued: 0, slots: 1 },
    { name: 'slow', finishMs: finishOf(0, 500_000), waitMs: 0, runMs: 500_000, tier: 'repo-lane', n: 3, idle: true, queued: 0, slots: 1 },
  ], 'a busy runner charges the 0.5-run residual, an idle one with room nothing');
});

test('selectRunner finish: a queued fast runner loses to an idle slow one, strictly', async () => {
  const res = await select(
    { fast: probe(2, ROOM), slow: probe(0, ROOM) },
    runnersOf('fast', 'slow'),
    { finishModel: modelOf({ fast: est(200_000), slow: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'slow');
  assert.equal(res.queuedChoice, undefined);
  assert.deepEqual(res.scores.map((s) => [s.name, s.queued, s.slots, s.waitMs, s.finishMs]), [
    ['fast', 2, 1, 500_000, finishOf(500_000, 200_000)],
    ['slow', 0, 1, 0, finishOf(0, 500_000)],
  ]);
  assert.ok(res.scores[1].finishMs < res.scores[0].finishMs, 'a strict win on finish, not a tie broken by idle-first');
});

test('selectRunner finish: an exact tie goes to the idle runner, not to its freer CPU', async () => {
  const res = await select(
    { fast: probe(0, NO_MEMORY), slow: probe(0, LESS_ROOM) },
    runnersOf('fast', 'slow'),
    { busyQueueable: true, finishModel: modelOf({ fast: est(250_000), slow: est(375_000) }) },
  );
  assert.equal(res.runner.name, 'slow', 'the busy runner has 8 cores free against the idle one\'s 5 and still loses');
  assert.equal(res.queuedChoice, undefined);
  assert.deepEqual(res.scores.map((s) => [s.name, s.finishMs, s.idle]), [['fast', finishOf(125_000, 250_000), false], ['slow', finishOf(0, 375_000), true]]);
});

test('selectRunner finish: equal finishes between two queued runners go to config order, and probe.running is ignored', async () => {
  const res = await select(
    { b: probe(2, { running: 4, ...ROOM }), a: probe(2, { running: 0, ...ROOM }) },
    runnersOf('b', 'a'),
    { finishModel: modelOf({ a: est(100_000), b: est(100_000) }) },
  );
  assert.equal(res.runner.name, 'b');
  assert.equal(res.queuedChoice, true);
  assert.deepEqual(res.scores.map((s) => [s.name, s.waitMs]), [['b', 250_000], ['a', 250_000]], '4 running tickets do not halve the wait');
});

test('selectRunner finish: a deeper queue on a much faster runner still beats a shallow one', async () => {
  const res = await select(
    { fast: probe(2, ROOM), slow: probe(1, ROOM) },
    runnersOf('fast', 'slow'),
    { finishModel: modelOf({ fast: est(100_000), slow: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'fast', 'today\'s fewest-queued rule would have picked slow');
  assert.equal(res.queuedChoice, true);
  assert.deepEqual(res.scores.map((s) => [s.name, s.finishMs]), [['fast', finishOf(250_000, 100_000)], ['slow', finishOf(750_000, 500_000)]]);
});

test('selectRunner finish: a runner whose budget holds several of these tickets divides its queue by that many', async () => {
  const wide = { capacity: { cpuCores: 12 } };
  const model = modelOf({ wide: est(120_000), narrow: est(120_000) });
  const res = await select(
    { wide: probe(3, { ...ROOM, ...wide }), narrow: probe(1, ROOM) },
    runnersOf('wide', 'narrow'),
    { maxRemoteQueue: 3, finishModel: model },
  );
  assert.equal(res.runner.name, 'wide', '3 queued over 3 slots beats 1 queued over 1');
  assert.deepEqual(res.scores.map((s) => [s.name, s.queued, s.slots, s.waitMs, s.finishMs]), [
    ['wide', 3, 3, 140_000, finishOf(140_000, 120_000)],
    ['narrow', 1, 1, 180_000, finishOf(180_000, 120_000)],
  ]);
  const unenforced = await select(
    { wide: probe(3, { ...ROOM, capacity: { cpuCores: null } }), narrow: probe(1, ROOM) },
    runnersOf('wide', 'narrow'),
    { maxRemoteQueue: 3, finishModel: model },
  );
  assert.equal(unenforced.runner.name, 'narrow', 'a runner that reports no budget is ranked serially, so its deeper queue loses');
  assert.deepEqual(unenforced.scores.map((s) => [s.name, s.slots, s.waitMs]), [['wide', 1, 420_000], ['narrow', 1, 180_000]]);
});

test('selectRunner finish: between two idle runners the faster estimate wins, not the freer CPU', async () => {
  const res = await select(
    { a: probe(0, LESS_ROOM), b: probe(0, ROOM) },
    runnersOf('a', 'b'),
    { finishModel: modelOf({ a: est(100_000), b: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'a');
  assert.equal(res.queuedChoice, undefined);
});

test('selectRunner finish: one candidate without an estimate gives exactly today\'s choice, and no scores', async () => {
  const probes = { fast: probe(1, ROOM), slow: probe(0, ROOM) };
  const partial = await select(probes, runnersOf('fast', 'slow'), { finishModel: modelOf({ fast: est(200_000) }) });
  const plain = await select(probes, runnersOf('fast', 'slow'), {});
  for (const res of [partial, plain]) {
    assert.equal(res.runner.name, 'slow', 'idle beats queued whatever the fast runner would have scored');
    assert.equal(res.queuedChoice, undefined);
    assert.equal('scores' in res, false);
  }
  assert.deepEqual(partial.skipped.map((s) => s.reason), plain.skipped.map((s) => s.reason));
  const busyNull = await select(
    { fast: probe(0, NO_CPU), slow: probe(0, ROOM) },
    runnersOf('fast', 'slow'),
    { busyQueueable: true, finishModel: modelOf({ fast: est(200_000), slow: null }) },
  );
  assert.equal(busyNull.runner.name, 'slow');
  assert.equal('scores' in busyNull, false);
});

test('selectRunner finish: a capacity that would give slots changes nothing while an estimate is missing', async () => {
  const probes = { wide: probe(2, { ...ROOM, capacity: { cpuCores: 12 } }), narrow: probe(1, ROOM) };
  const partial = await select(probes, runnersOf('wide', 'narrow'), { maxRemoteQueue: 3, finishModel: modelOf({ wide: est(120_000), narrow: null }) });
  const plain = await select(probes, runnersOf('wide', 'narrow'), { maxRemoteQueue: 3 });
  for (const res of [partial, plain]) {
    assert.equal(res.runner.name, 'narrow', 'fewest queued, exactly as before the finish model');
    assert.equal(res.queuedChoice, true);
    assert.equal('scores' in res, false);
  }
  assert.deepEqual(partial.skipped, plain.skipped, 'one missing estimate leaves the whole field, slots included, on today\'s ranking');
});

test('selectRunner finish: queuedFilter still vetoes a candidate, and a vetoed one cannot veto the ranking', async () => {
  const res = await select(
    { fast: probe(1, ROOM), slow: probe(1, ROOM) },
    runnersOf('fast', 'slow'),
    { finishModel: modelOf({ slow: est(500_000) }), queuedFilter: ({ runner }) => runner.name !== 'fast' },
  );
  assert.equal(res.runner.name, 'slow');
  assert.equal(res.queuedChoice, true);
  assert.deepEqual(names(res), ['slow'], 'the vetoed runner never reached ranking, so its missing estimate does not disable scoring');
});

test('selectRunner finish: without busyQueueable a busy runner is not queueable, as today', async () => {
  const probes = { fast: probe(0, NO_CPU), slow: probe(0, ROOM) };
  const model = modelOf({ fast: est(200_000), slow: est(500_000) });
  const res = await select(probes, runnersOf('fast', 'slow'), { finishModel: model });
  assert.equal(res.runner.name, 'slow');
  assert.deepEqual(names(res), ['slow']);
  const zeroCap = await select(probes, runnersOf('fast', 'slow'), { busyQueueable: true, maxRemoteQueue: 0, finishModel: model });
  assert.equal(zeroCap.runner.name, 'slow', 'maxRemoteQueue 0 keeps a busy runner out even on the place path');
  const only = await select({ fast: probe(0, NO_CPU) }, runnersOf('fast'), { busyQueueable: true, finishModel: modelOf({ fast: est(200_000) }) });
  assert.equal(only.runner.name, 'fast', 'on the place path a lone busy runner is waited for as a queued choice');
  assert.equal(only.queuedChoice, true);
  const today = await select({ fast: probe(0, NO_CPU) }, runnersOf('fast'), { finishModel: modelOf({ fast: est(200_000) }) });
  assert.equal(today.runner, null);
  assert.equal(today.busy.runner.name, 'fast');
  assert.equal('scores' in today, false, 'nothing reached ranking, so nothing was scored');
});

// A Sunday 13:00 PDT, and a window that closes 300s later.
const NOW = Date.parse('2026-10-11T20:00:00Z');
const CLOSING_SOON = { timezone: 'America/Los_Angeles', available: [{ days: ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'], from: '06:00', to: '13:05' }] };
const scheduled = (name) => ({ ...makeRunner({ ssh: name }), schedule: CLOSING_SOON });
const reasonFor = (res, name) => res.skipped.filter((s) => s.name === name).map((s) => s.reason).join('; ');

test('selectRunner finish: a window closing during the predicted wait + run skips that runner', async () => {
  const probes = { fast: probe(1, ROOM), slow: probe(0, ROOM) };
  const model = modelOf({ fast: est(150_000), slow: est(500_000) });
  const res = await select(probes, [scheduled('fast'), makeRunner({ ssh: 'slow' })], { now: NOW, finishModel: model });
  assert.equal(res.runner.name, 'slow');
  assert.deepEqual(names(res), ['slow']);
  assert.match(reasonFor(res, 'fast'), /window closes at 2026-10-11T20:05:00\.000Z, before the predicted 375s finish \(wait 225s \+ run 150s\)/);
  const unscheduled = await select(probes, runnersOf('fast', 'slow'), { now: NOW, finishModel: model });
  assert.equal(unscheduled.runner.name, 'fast', 'the same runner wins on finish alone, so the window is what dropped it');
  assert.deepEqual(names(unscheduled), ['fast', 'slow']);
});

test('selectRunner finish: with a model, today\'s expected-run window check still skips a runner before it is probed', async () => {
  const probes = { slow: probe(0, LESS_ROOM) };
  const res = await select(probes, [scheduled('fast'), makeRunner({ ssh: 'slow' })], {
    now: NOW,
    expectedRunMs: 600_000,
    finishModel: modelOf({}),
  });
  assert.equal(res.runner.name, 'slow');
  assert.equal('scores' in res, false);
  assert.match(reasonFor(res, 'fast'), /window closes at 2026-10-11T20:05:00\.000Z, before the expected 600s run/);
  assert.doesNotMatch(reasonFor(res, 'fast'), /probe/, 'fast has no probe answer, so probing it would have shown up as a probe failure');
});

test('selectRunner finish: a runner outside its window is still skipped before it is probed', async () => {
  const off = Date.parse('2026-10-12T07:00:00Z'); // 00:00 PDT Monday: no window open
  const res = await select({ fast: probe(0, ROOM) }, [scheduled('fast')], {
    now: off,
    busyQueueable: true,
    finishModel: modelOf({ fast: est(200_000) }),
  });
  assert.equal(res.runner, null);
  assert.deepEqual(res.skipped, [{ name: 'fast', reason: 'outside schedule' }]);
  assert.equal('scores' in res, false);
});

test('selectRunner finish: exclusions still come before any scoring', async () => {
  const res = await select(
    { down: probe(0, ROOM), paused: probe(0, { paused: true, ...ROOM }), over: probe(5, ROOM), slow: probe(0, ROOM) },
    [...runnersOf('down', 'paused', 'over'), makeRunner({ ssh: 'slow' })],
    { downRunners: { down: 'unreachable for 10m' }, finishModel: modelOf({ slow: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'slow');
  assert.deepEqual(names(res), ['slow']);
  assert.deepEqual(res.skipped.map((s) => s.reason), ['unreachable for 10m', 'paused', 'queued: 5 (over maxRemoteQueue 2)']);
});

test('selectRunner finish: busyQueueable without estimates leaves a busy runner as today\'s busy fallback, not a queued choice', async () => {
  const res = await select({ fast: probe(0, NO_CPU), slow: probe(1, ROOM) }, runnersOf('fast', 'slow'), { busyQueueable: true, finishModel: modelOf({}) });
  assert.equal(res.runner.name, 'slow', 'today: the queued runner within maxRemoteQueue is the queued choice');
  assert.equal(res.queuedChoice, true);
  const lone = await select({ fast: probe(0, NO_CPU) }, runnersOf('fast'), { busyQueueable: true, finishModel: modelOf({}) });
  assert.equal(lone.runner, null);
  assert.equal(lone.busy.runner.name, 'fast');
});

test('selectRunner finish: slotAwareWait false ranks with the serial wait (the rebalance passes)', async () => {
  const probes = { fast: probe(3, { ...NO_CPU, capacity: { cpuCores: 16 } }), slow: probe(0, ROOM) };
  const model = modelOf({ fast: est(200_000), slow: est(500_000) });
  const serial = await select(probes, runnersOf('fast', 'slow'), { busyQueueable: true, maxRemoteQueue: 4, finishModel: model, slotAwareWait: false });
  assert.equal(serial.scores.find((s) => s.name === 'fast').waitMs, 800_000, '(3 + 1) x 200s, one slot');
  assert.equal(serial.runner.name, 'slow');
  const aware = await select(probes, runnersOf('fast', 'slow'), { busyQueueable: true, maxRemoteQueue: 4, finishModel: model });
  assert.equal(aware.scores.find((s) => s.name === 'fast').waitMs, 175_000, '(3 + 0.5) x 200s / 4 slots of a 4-core ticket');
  assert.equal(aware.runner.name, 'fast');
});

// ---- BRAIN-899 B3: one estimateAll call over the whole field ----

/** A model that records every field it was asked about, answering from a per-name table. */
const recordingModel = (byName, calls = []) => ({
  calls,
  estimateFor: (runner) => byName[runner.name] ?? null,
  estimateAll: (runners) => {
    calls.push(runners.map((runner) => runner.name));
    return new Map(runners.map((runner) => [runner.name, byName[runner.name] ?? null]));
  },
});

test('selectRunner: estimateAll is called ONCE, over the pool plus the busy candidates the schedule check inspects', async () => {
  const calls = [];
  const model = recordingModel({ fast: est(150_000), slow: est(500_000) }, calls);
  // `fast` is busy and not queueable here, so it never reaches the pool -- only the schedule check below it looks at it
  const res = await select({ fast: probe(0, NO_CPU), slow: probe(0, ROOM) }, runnersOf('fast', 'slow'), { finishModel: model });
  assert.deepEqual(calls, [['slow', 'fast']], 'one call, naming the busy candidate outside the pool too');
  assert.equal(res.runner.name, 'slow');
  assert.deepEqual(names(res), ['slow']);
});

test('selectRunner: a busy candidate outside the pool is estimated, so a closing window can still drop it', async () => {
  const calls = [];
  const model = recordingModel({ fast: est(400_000), slow: est(500_000) }, calls);
  const res = await select({ fast: probe(0, NO_CPU), slow: probe(0, ROOM) }, [scheduled('fast'), makeRunner({ ssh: 'slow' })], { now: NOW, finishModel: model });
  assert.deepEqual(calls, [['slow', 'fast']]);
  assert.equal(res.runner.name, 'slow');
  assert.match(reasonFor(res, 'fast'), /window closes at 2026-10-11T20:05:00\.000Z, before the predicted 600s finish \(wait 200s \+ run 400s\)/, 'the busy runner had an estimate to predict with');
});

test('selectRunner: an estimateAll answer that omits a runner is null, never undefined', async () => {
  const omitsAll = { estimateFor: () => est(200_000), estimateAll: () => new Map() };
  const res = await select({ fast: probe(0, NO_CPU), slow: probe(0, ROOM) }, [scheduled('fast'), makeRunner({ ssh: 'slow' })], { now: NOW, finishModel: omitsAll });
  assert.equal(res.runner.name, 'slow');
  assert.equal('scores' in res, false, 'a missing estimate leaves the whole field on today\'s ranking');
  assert.doesNotMatch(reasonFor(res, 'fast'), /predicted/, 'with nothing to predict, the window check stays out of it');
  const omitsBusy = { estimateFor: () => est(200_000), estimateAll: (runners) => new Map(runners.filter((runner) => runner.name === 'slow').map((runner) => [runner.name, est(200_000)])) };
  const ranked = await select({ fast: probe(0, NO_CPU), slow: probe(0, ROOM) }, [scheduled('fast'), makeRunner({ ssh: 'slow' })], { now: NOW, finishModel: omitsBusy });
  assert.equal(ranked.runner.name, 'slow');
  assert.deepEqual(names(ranked), ['slow'], 'the pool candidate it did answer for is still ranked');
});
