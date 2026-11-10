import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { tmpDir, makeRunner, IDLE } from './remote-harness.js';
import { paths, appendHistory } from '../src/state.js';
import { readRecentHistoryRows } from '../src/suggest.js';
import { REMOTE_WITHDRAW_CAPABILITY } from '../src/capabilities.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { historyIncludeFor, rebalanceFinishAllows, selectionOptionsFor } from '../src/supervisor.js';
import { chooseSpeedPlacement, DEFAULT_REMOTE_OVERHEAD_MS, RUN_ESTIMATE_WINDOW_MS } from '../src/speed-placement.js';

// BRAIN-899 slice B: how the supervisor wires the finish model into the four selection paths. Unit level -- the estimator and
// selectRunner's ranking are tests/finish-placement.test.js, the end-to-end moves are tests/rebalance-queued.test.js.

const REPO = 'lane-broker';
const LANE = 'pr';
const CMD = 'sh scripts/test-run.sh tests/a.test.js';
const SKYBOX = { name: 'skybox', speedFactor: 1 };
const WINTOP = { name: 'wintop', speedFactor: 0.5 };
const ticket = { id: 't-1', repoId: REPO, lane: LANE, command: CMD, weight: 1, resources: { cpuCores: 1 }, remote: {} };
const cfg = (runners = [SKYBOX, WINTOP]) => ({ ...DEFAULT_GLOBAL_CONFIG, runners });
const RECENT = Date.now() - 3_600_000;
const AGED = Date.now() - RUN_ESTIMATE_WINDOW_MS - 3_600_000;

const remoteRows = (runner, ...runMs) =>
  runMs.map((ms, i) => ({ id: `${runner}-${i}`, repo: REPO, lane: LANE, command: CMD, executor: 'remote', runner, exit: 0, startedAt: RECENT + i * 1000, endedAt: RECENT + i * 1000 + ms, runMs: ms }));

/** The same runs, started before the run-estimate window: a placement must not see them. */
const agedRows = (runner, ...runMs) => remoteRows(runner, ...runMs).map((row, i) => ({ ...row, startedAt: AGED + i * 1000, endedAt: AGED + i * 1000 + row.runMs }));

/** A state root whose history.jsonl holds `rows`. */
function rootWithHistory(rows, prefix = 'finish-wiring') {
  const root = tmpDir(prefix);
  if (rows.length > 0) fs.writeFileSync(paths(root).history, rows.map((r) => `${JSON.stringify(r)}\n`).join(''));
  return root;
}

const optionsFor = (kind, root, extra = {}, cfgExtra = {}) => selectionOptionsFor(kind, ticket, { ...cfg(), ...cfgExtra }, { root, ...extra });
const queuedMove = (position, name = 'skybox') => ({ position, sourceRunner: makeRunner({ ssh: name }) });

test('selectionOptionsFor: every kind carries the finish model, and only place may queue on a busy runner', () => {
  const root = rootWithHistory(remoteRows('skybox', 200_000, 210_000));
  const place = optionsFor('place', root);
  assert.equal(place.busyQueueable, true);
  assert.equal(place.maxRemoteQueue, DEFAULT_GLOBAL_CONFIG.maxRemoteQueue);
  assert.deepEqual(place.finishModel.estimateFor(SKYBOX), { ms: 205_000, tier: 'exact', n: 2 }, 'the model reads the state root\'s history');
  assert.deepEqual(place.finishModel.estimateFor(WINTOP), { ms: 410_000, tier: 'exact', n: 2, donor: 'skybox' }, 'and scales a donor by the configured speedFactors');
  const kinds = [['rebind', {}], ['rebalance', {}], ['rebalance', { queuedMove: queuedMove(2) }]];
  for (const [kind, extra] of kinds) {
    const opts = optionsFor(kind, root, extra);
    assert.equal(typeof opts.finishModel?.estimateFor, 'function', kind);
    assert.equal(opts.busyQueueable, undefined, `${kind} keeps today's eligibility`);
  }
  assert.equal(optionsFor('rebalance', root).maxRemoteQueue, 0);
  const moved = optionsFor('rebalance', root, { queuedMove: queuedMove(2) });
  assert.equal(moved.maxRemoteQueue, DEFAULT_GLOBAL_CONFIG.remoteRebalanceQueuedMax);
  assert.equal(moved.allowBusyQueue, true);
  assert.equal(typeof moved.queuedFilter, 'function');
});

test('selectionOptionsFor: rebind eligibility is exactly today\'s for every stuck/localBlocked combination', () => {
  const root = rootWithHistory(remoteRows('skybox', 200_000, 210_000));
  for (const stuckLocally of [false, true]) {
    for (const localBlocked of [false, true]) {
      const opts = optionsFor('rebind', root, { stuckLocally, localBlocked });
      assert.equal(opts.maxRemoteQueue, stuckLocally ? DEFAULT_GLOBAL_CONFIG.maxRemoteQueue : 0, `stuckLocally=${stuckLocally}`);
      assert.equal(opts.allowBusyQueue, localBlocked, `localBlocked=${localBlocked}`);
      assert.equal(opts.queuedFilter, undefined);
    }
  }
});

test('selectionOptionsFor: history too thin to estimate leaves a model that answers null, so nothing is ranked', () => {
  const root = rootWithHistory(remoteRows('skybox', 200_000));
  assert.equal(optionsFor('place', root).finishModel.estimateFor(SKYBOX), null, 'one run is neither exact nor repo-lane evidence');
  assert.equal(optionsFor('place', rootWithHistory([])).finishModel.estimateFor(SKYBOX), null, 'nor is an empty history');
});

// ---- BRAIN-899 B2: the window a placement reads ----

test('selectionOptionsFor: a run older than the window estimates nothing, and cannot move a newer median', () => {
  assert.equal(optionsFor('place', rootWithHistory(agedRows('skybox', 200_000, 210_000, 220_000))).finishModel.estimateFor(SKYBOX), null, `${RUN_ESTIMATE_WINDOW_MS / 86_400_000} days is how far back a run estimate looks`);
  const mixed = rootWithHistory([...agedRows('skybox', 900_000, 900_000, 900_000), ...remoteRows('skybox', 200_000, 210_000, 220_000)]);
  assert.deepEqual(optionsFor('place', mixed).finishModel.estimateFor(SKYBOX), { ms: 210_000, tier: 'exact', n: 3 }, 'the aged runs neither add to n nor move the median');
});

test('historyIncludeFor: the raw-line filter is spelled exactly as appendHistory writes the repo key', () => {
  const root = tmpDir('include-spelling');
  appendHistory(root, { id: 'a', repo: REPO, lane: LANE, startedAt: Date.now(), exit: 0 });
  appendHistory(root, { id: 'b', repo: `${REPO}-x`, lane: LANE, startedAt: Date.now(), exit: 0 });
  assert.equal(historyIncludeFor(REPO), '"repo":"lane-broker"');
  assert.ok(fs.readFileSync(paths(root).history, 'utf8').split('\n')[0].includes(historyIncludeFor(REPO)), 'the reader filters on the writer\'s own spelling');
  const read = readRecentHistoryRows(paths(root).history, { sinceMs: Date.now() - RUN_ESTIMATE_WINDOW_MS, include: historyIncludeFor(REPO) });
  assert.deepEqual(read.map((r) => r.id), ['a'], 'and the closing quote keeps a longer repo id out');
});

const TRAIN_IDS = 'abcdef0123456789';
/** A train-shaped command. The `\n` inside TRAIN_CHANGED_FILES splits the token, so the class value is the first path alone. */
const trainCommand = ({ id, files }) =>
  `env TRAIN_SHARD=1/3 TRAIN_SHARDS=3 TRAIN_TIER=pr TRAIN_CHANGED_FILES_PATH=.train-changed-files TRAIN_CHANGED_FILES=${files ?? `src/pages/${id}.html\nsrc/lib/${id}.js`} sh .githooks/pre-push-gate.sh verify-full`;
const trainRuns = (startedAt, ...specs) =>
  specs.map(({ runMs, id, files }, i) => ({ id: `train-${id}-${i}`, repo: REPO, lane: LANE, command: trainCommand({ id, files }), executor: 'remote', runner: 'skybox', exit: 0, startedAt: startedAt + i * 1000, endedAt: startedAt + i * 1000 + runMs, runMs }));

test('selectionOptionsFor: the job classes are the windowed rows\', so aged runs cannot drop a name from the class key', () => {
  const classic = (runMs, id) => ({ runMs, id, files: `classic.html\nsrc/lib/${id}.js` });
  const other = (runMs, id) => ({ runMs, id });
  const inWindow = trainRuns(RECENT, classic(100_000, 'a'), classic(120_000, 'b'), other(900_000, 'c'), other(980_000, 'd'));
  // seven more distinct changed-file values: nine with the window's two, so an all-history read would drop the name and pool the shard
  const aged = trainRuns(AGED, ...[0, 1, 2, 3, 4, 5, 6].map((i) => other(900_000 + i * 10_000, TRAIN_IDS[6 + i])));
  const wanted = { ...ticket, command: trainCommand({ id: 'ff', files: 'classic.html\nsrc/lib/ff.js' }) };
  const model = selectionOptionsFor('place', wanted, cfg([SKYBOX]), { root: rootWithHistory([...aged, ...inWindow]) }).finishModel;
  assert.deepEqual(model.estimateFor(SKYBOX), { ms: 110_000, tier: 'class', n: 2 }, 'only the window\'s two classic.html runs are of this class');
  assert.deepEqual(model.estimateAll([SKYBOX]).get('skybox'), { ms: 110_000, tier: 'class', n: 2 });
});

// ---- BRAIN-899 B3: the model estimates a field at one tier ----

test('selectionOptionsFor: estimateAll settles the field on one tier, and estimateFor stays the single-runner answer', () => {
  const root = rootWithHistory([...remoteRows('skybox', 200_000, 220_000, 240_000), ...remoteRows('wintop', 900_000, 900_000, 900_000).map((r) => ({ ...r, command: 'sh other.sh' }))]);
  const model = optionsFor('place', root).finishModel;
  const all = model.estimateAll([SKYBOX, WINTOP]);
  assert.deepEqual([...all.keys()], ['skybox', 'wintop']);
  assert.deepEqual(all.get('skybox'), { ms: 220_000, tier: 'exact', n: 3 });
  assert.deepEqual(all.get('wintop'), { ms: 440_000, tier: 'exact', n: 3, donor: 'skybox' }, 'exact through the donor, so both are compared alike');
  assert.deepEqual(model.estimateFor(WINTOP), { ms: 900_000, tier: 'repo-lane', n: 3 }, 'per-runner, wintop would have been compared at repo-lane');
  assert.deepEqual(model.estimateAll([SKYBOX, { name: 'ghost' }]).get('ghost'), { ms: 220_000, tier: 'exact', n: 3, donor: 'skybox' }, 'a candidate with no rows of its own is estimated through the configured donor');
  assert.deepEqual([...model.estimateAll([]).keys()], [], 'no candidates, no estimates');
  assert.deepEqual([...optionsFor('place', rootWithHistory([])).finishModel.estimateAll([SKYBOX, WINTOP]).values()], [null, null], 'no history estimates nobody, and the answer is null rather than a missing key');
});

test('selectionOptionsFor: rebalance keeps per-runner estimates, since its gain gate costs the source per runner', () => {
  const root = rootWithHistory([...remoteRows('skybox', 200_000, 220_000, 240_000), ...remoteRows('wintop', 900_000, 900_000, 900_000).map((r) => ({ ...r, command: 'sh other.sh' }))]);
  assert.equal(typeof optionsFor('rebind', root).finishModel.estimateAll, 'function', 'rebind ranks at one tier, as place does');
  const rebalance = optionsFor('rebalance', root).finishModel;
  assert.equal('estimateAll' in rebalance, false, 'so selectRunner falls back to estimateFor for every destination');
  assert.deepEqual(rebalance.estimateFor(WINTOP), { ms: 900_000, tier: 'repo-lane', n: 3 });
});

test('selectionOptionsFor: place always offers busyQueueable; selectRunner honours it only when the model ranks', () => {
  assert.equal(optionsFor('place', rootWithHistory([...remoteRows('skybox', 200_000, 210_000), ...remoteRows('wintop', 400_000, 410_000)])).busyQueueable, true);
  assert.equal(optionsFor('place', rootWithHistory([])).busyQueueable, true);
});

const gate = (over = {}) => rebalanceFinishAllows({ destScore: { finishMs: 10_000_000 }, sourceEstimateMs: 200_000, position: 2, ...over });

test('rebalanceFinishAllows: a drain, the refusal deadline, no score, no source estimate and no position keep today\'s rule', () => {
  assert.equal(gate(), false, 'the ungated shape is a refusal to move: 10000s against 0.75 x 2 x 200s');
  assert.equal(gate({ draining: true }), true, 'a drain move is an operator fact');
  assert.equal(gate({ refusalCapElapsed: true }), true, 'the refusal deadline must stay reachable');
  for (const destScore of [undefined, null]) assert.equal(gate({ destScore }), true, 'an unranked selection has no score');
  for (const sourceEstimateMs of [null, undefined, NaN, Infinity, '200000']) assert.equal(gate({ sourceEstimateMs }), true, String(sourceEstimateMs));
  for (const position of [undefined, null, 0, -1, NaN, 0.5]) assert.equal(gate({ position }), true, String(position));
});

test('rebalanceFinishAllows: the destination must finish inside 0.75 x position x the source estimate, strictly', () => {
  const at = (finishMs, position = 2) => rebalanceFinishAllows({ destScore: { finishMs }, sourceEstimateMs: 200_000, position });
  const threshold = 0.75 * 2 * 200_000;
  assert.equal(at(threshold - 1), true);
  assert.equal(at(threshold), false, 'equality is not a gain');
  assert.equal(at(threshold + 1), false);
  assert.equal(at(100_000, 1), true, 'position 1: staying put costs one run of the source estimate');
  assert.equal(at(200_000, 1), false);
  assert.equal(at(1_000_000, 8), true, 'a deep queue on the source is what makes a move pay');
});

// ---- the BRAIN-888 queued filter, in ms once both sides have estimates ----

const QUEUED_DEST = { ...IDLE, queued: 1, reservationOwner: false };
const cand = (runner, probe = QUEUED_DEST, finish = 1, idle = false) => ({ runner, probe, finish, idle, cpuFree: 0, order: 0, closesAt: null });
const wintop = makeRunner({ ssh: 'wintop' });

test('queued rebalance filter: with estimates on both sides the gain is judged in ms, not in cpu-core units', () => {
  const root = rootWithHistory([...remoteRows('skybox', 1_000_000, 1_000_000), ...remoteRows('wintop', 100_000, 100_000)]);
  const opts = optionsFor('rebalance', root, { queuedMove: queuedMove(2) });
  // wait (1 queued + 1) x 100s + run 100s + 60s overhead = 360s, against 0.75 x 2 x 1000s = 1500s
  assert.equal(opts.queuedFilter(cand(wintop, QUEUED_DEST, 99)), true, 'a finish far worse in cpu-core units still passes on ms');
  const slow = rootWithHistory([...remoteRows('skybox', 100_000, 100_000), ...remoteRows('wintop', 1_000_000, 1_000_000)]);
  const strict = optionsFor('rebalance', slow, { queuedMove: queuedMove(2) });
  // wait 2 x 1000s + run 1000s + 60s = 3060s, against 0.75 x 2 x 100s = 150s
  assert.equal(strict.queuedFilter(cand(wintop, QUEUED_DEST, 0.01)), false, 'a finish better in cpu-core units is refused on ms');
});

test('queued rebalance filter: an idle destination is not charged a wait, and queue depth still counts serially', () => {
  const root = rootWithHistory([...remoteRows('skybox', 1_000_000, 1_000_000), ...remoteRows('wintop', 400_000, 400_000)]);
  const opts = optionsFor('rebalance', root, { queuedMove: queuedMove(2) }, { remoteRebalanceQueuedMax: 3 });
  assert.equal(opts.queuedFilter(cand(wintop, { ...QUEUED_DEST, queued: 0 }, 99, true)), true, '0 + 400s + 60s < 1500s');
  assert.equal(opts.queuedFilter(cand(wintop, QUEUED_DEST, 99)), true, '(1 + 1) x 400s + 400s + 60s = 1260s < 1500s');
  assert.equal(opts.queuedFilter(cand(wintop, { ...QUEUED_DEST, queued: 3 }, 99)), false, '(3 + 1) x 400s + 400s + 60s = 2060s > 1500s');
});

test('queued rebalance filter: without estimates it keeps today\'s cpu-core comparison', () => {
  const root = rootWithHistory(remoteRows('skybox', 1_000_000));
  const opts = optionsFor('rebalance', root, { queuedMove: queuedMove(2) });
  assert.equal(opts.queuedFilter(cand(wintop, QUEUED_DEST, 1)), true, 'stayFinish is (1 + 1) cores / speed 1, so 1 < 0.75 x 2');
  assert.equal(opts.queuedFilter(cand(wintop, QUEUED_DEST, 2)), false);
});

test('queued rebalance filter: every other clause still vetoes a candidate the ms gate would take', () => {
  const root = rootWithHistory([...remoteRows('skybox', 1_000_000, 1_000_000), ...remoteRows('wintop', 100_000, 100_000)]);
  const opts = optionsFor('rebalance', root, { queuedMove: queuedMove(2) });
  const rejects = {
    'is itself a reservation owner': { ...QUEUED_DEST, reservationOwner: true },
    'reports no reservationOwner': Object.fromEntries(Object.entries(QUEUED_DEST).filter(([k]) => k !== 'reservationOwner')),
    'lacks remote-withdraw/1': { ...QUEUED_DEST, capabilities: IDLE.capabilities.filter((c) => c !== REMOTE_WITHDRAW_CAPABILITY) },
    'has unmeasured headroom': Object.fromEntries(Object.entries(QUEUED_DEST).filter(([k]) => k !== 'headroom')),
    'is queued deeper than remoteRebalanceQueuedMax': { ...QUEUED_DEST, queued: DEFAULT_GLOBAL_CONFIG.remoteRebalanceQueuedMax + 1 },
  };
  for (const [why, probe] of Object.entries(rejects)) assert.equal(opts.queuedFilter(cand(wintop, probe, 99)), false, why);
  assert.equal(opts.queuedFilter(cand(wintop, QUEUED_DEST, 99)), true, 'the same shape without the defect passes');
});

// ---- speedChoice's remote finish is the score, never the score plus the ratio arithmetic ----

const BASE = { localAdmitsNow: false, localWaitMs: 3_600_000, estimateMs: 1_800_000, runnerRatio: 3, overheadMs: DEFAULT_REMOTE_OVERHEAD_MS };
const RATIO_FINISH = 1_800_000 * 3 + DEFAULT_REMOTE_OVERHEAD_MS;

test('chooseSpeedPlacement: a finite remoteFinishMs IS the remote finish, replacing the ratio arithmetic', () => {
  assert.equal(chooseSpeedPlacement(BASE).remoteFinishMs, RATIO_FINISH, 'unscored, the ratio still decides');
  const scored = chooseSpeedPlacement({ ...BASE, remoteFinishMs: 900_000 });
  assert.deepEqual([scored.choice, scored.remoteFinishMs], ['remote', 900_000], 'the score is not added to E x ratio + overhead');
  assert.equal(chooseSpeedPlacement({ ...BASE, localWaitMs: 0, remoteFinishMs: 9_000_000 }).choice, 'local', 'a shorter local finish still beats a score');
  assert.equal(chooseSpeedPlacement({ ...BASE, localWaitMs: 0, estimateMs: 900_000, remoteFinishMs: 900_000 }).choice, 'local', 'a tie stays local');
  assert.equal(chooseSpeedPlacement({ ...BASE, remoteFinishMs: 0 }).remoteFinishMs, 0, 'a zero score is a score');
});

test('chooseSpeedPlacement: a missing, malformed or infinite remoteFinishMs falls back to the ratio arithmetic', () => {
  for (const remoteFinishMs of [undefined, null, NaN, Infinity, '900000', {}]) {
    assert.equal(chooseSpeedPlacement({ ...BASE, remoteFinishMs }).remoteFinishMs, RATIO_FINISH, String(remoteFinishMs));
  }
  assert.equal(chooseSpeedPlacement({ ...BASE, runnerRatio: null }).remoteFinishMs, null, 'no runner at all is still no remote finish');
});

test('chooseSpeedPlacement: localBlocked and localHeldByKey report the score as the remote finish', () => {
  for (const extra of [{ localBlocked: true }, { localHeldByKey: true }]) {
    const got = chooseSpeedPlacement({ ...BASE, ...extra, remoteFinishMs: 123_456 });
    assert.deepEqual([got.choice, got.localFinishMs, got.remoteFinishMs], ['remote', null, 123_456], JSON.stringify(extra));
  }
});

test('BRAIN-899 A1: both rebalance passes keep the serial wait; place and rebind rank slot-aware', () => {
  const root = rootWithHistory([]);
  assert.equal(optionsFor('rebalance', root).slotAwareWait, false);
  assert.equal(optionsFor('rebalance', root, { queuedMove: { position: 2, sourceRunner: SKYBOX } }).slotAwareWait, false);
  assert.equal(optionsFor('place', root).slotAwareWait, undefined, 'place takes selectRunner\'s slot-aware default');
});
