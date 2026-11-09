import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeRunner, tmpDir } from './remote-harness.js';
import { selectRunner } from '../src/remote-client.js';
import { DEFAULT_REMOTE_OVERHEAD_MS, pairingCommand, runnerRunEstimateMs, runnerWaitMs } from '../src/speed-placement.js';

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
    { name: 'fast', finishMs: finishOf(200_000, 200_000), waitMs: 200_000, runMs: 200_000, tier: 'exact', n: 9, idle: false },
    { name: 'slow', finishMs: finishOf(0, 500_000), waitMs: 0, runMs: 500_000, tier: 'repo-lane', n: 3, idle: true },
  ]);
});

test('selectRunner finish: a queued fast runner loses to an idle slow one, strictly', async () => {
  const res = await select(
    { fast: probe(2, ROOM), slow: probe(0, ROOM) },
    runnersOf('fast', 'slow'),
    { finishModel: modelOf({ fast: est(200_000), slow: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'slow');
  assert.equal(res.queuedChoice, undefined);
  assert.deepEqual(res.scores.map((s) => [s.name, s.waitMs, s.finishMs]), [
    ['fast', 600_000, finishOf(600_000, 200_000)],
    ['slow', 0, finishOf(0, 500_000)],
  ]);
  assert.ok(res.scores[1].finishMs < res.scores[0].finishMs, 'a strict win on finish, not a tie broken by idle-first');
});

test('selectRunner finish: an exact tie goes to the idle runner, not to its freer CPU', async () => {
  const res = await select(
    { fast: probe(0, NO_MEMORY), slow: probe(0, LESS_ROOM) },
    runnersOf('fast', 'slow'),
    { busyQueueable: true, finishModel: modelOf({ fast: est(250_000), slow: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'slow', 'the busy runner has 8 cores free against the idle one\'s 5 and still loses');
  assert.equal(res.queuedChoice, undefined);
  assert.deepEqual(res.scores.map((s) => [s.name, s.finishMs, s.idle]), [['fast', finishOf(250_000, 250_000), false], ['slow', finishOf(0, 500_000), true]]);
});

test('selectRunner finish: equal finishes between two queued runners go to config order, and probe.running is ignored', async () => {
  const res = await select(
    { b: probe(2, { running: 4, ...ROOM }), a: probe(2, { running: 0, ...ROOM }) },
    runnersOf('b', 'a'),
    { finishModel: modelOf({ a: est(100_000), b: est(100_000) }) },
  );
  assert.equal(res.runner.name, 'b');
  assert.equal(res.queuedChoice, true);
  assert.deepEqual(res.scores.map((s) => [s.name, s.waitMs]), [['b', 300_000], ['a', 300_000]], '4 running tickets do not halve the wait');
});

test('selectRunner finish: a deeper queue on a much faster runner still beats a shallow one', async () => {
  const res = await select(
    { fast: probe(2, ROOM), slow: probe(1, ROOM) },
    runnersOf('fast', 'slow'),
    { finishModel: modelOf({ fast: est(100_000), slow: est(500_000) }) },
  );
  assert.equal(res.runner.name, 'fast', 'today\'s fewest-queued rule would have picked slow');
  assert.equal(res.queuedChoice, true);
  assert.deepEqual(res.scores.map((s) => [s.name, s.finishMs]), [['fast', finishOf(300_000, 100_000)], ['slow', finishOf(1_000_000, 500_000)]]);
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
  const model = modelOf({ fast: est(120_000), slow: est(500_000) });
  const res = await select(probes, [scheduled('fast'), makeRunner({ ssh: 'slow' })], { now: NOW, finishModel: model });
  assert.equal(res.runner.name, 'slow');
  assert.deepEqual(names(res), ['slow']);
  assert.match(reasonFor(res, 'fast'), /window closes at 2026-10-11T20:05:00\.000Z, before the predicted 360s finish \(wait 240s \+ run 120s\)/);
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
