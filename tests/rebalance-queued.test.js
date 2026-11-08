import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { sleep, waitFor, laneRun, writeRepoConfig, gitFixture } from './helpers.js';
import { tmpDir, makeRunner, resultOf, IDLE, rebalanceSetup, execArrived, T } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { paths } from '../src/state.js';
import { REMOTE_WITHDRAW_CAPABILITY } from '../src/capabilities.js';
import { selectRunner } from '../src/remote-client.js';
import { selectionOptionsFor } from '../src/supervisor.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

/**
 * BRAIN-888 part B: a ticket queued behind ANOTHER ticket's reservation on its runner may move to a short non-idle queue. The
 * runner's advisory facts (position, behindOtherOwner, reservationOwnerWaitedMs) are injected on runner `a`'s still-queued record
 * (`queuedPatch`); probes are synthetic. Runner `a` stays paused, so the ticket sits queued there until a move or the test ends.
 */

const FACTS = { position: 2, behindOtherOwner: true, reservationOwnerWaitedMs: 3_600_000 };
const NO_FACTS = { position: null, behindOtherOwner: null, reservationOwnerWaitedMs: null };
const QUEUED_B = { ...IDLE, queued: 1, reservationOwner: false };
const PARKED = { ...IDLE, paused: true };
const BUSY = { ...IDLE, queued: 0, reservationOwner: false, headroom: { cpuCores: 0, memoryBytes: 1e12 } };
const without = (probe, key) => Object.fromEntries(Object.entries(probe).filter(([k]) => k !== key));
const MIN_QUEUED = 1500;

/** Source `a` (speed 0.4) holds the ticket at position 2 behind another owner; every other runner is parked until `arm()`. */
function stuckSetup({ names = ['a', 'b'], facts = FACTS, probes = { b: QUEUED_B }, speed = 0.4, rebalance = {}, runnerOpts = {}, paused = ['a'], configOpts = {} } = {}) {
  const ctx = rebalanceSetup({
    names, paused, rebalance: { remoteRebalanceMinQueuedMs: MIN_QUEUED, ...rebalance },
    configOpts: { a: { speedFactor: speed }, ...configOpts },
    runnerOpts: { ...runnerOpts, a: { queuedPatch: facts, ...(runnerOpts.a ?? {}) } },
  });
  for (const name of names.filter((n) => n !== 'a')) ctx.fake.setProbe(name, PARKED);
  ctx.arm = () => { for (const [name, probe] of Object.entries(probes)) ctx.fake.setProbe(name, probe); };
  return ctx;
}

const admissionLog = (ctx) => fs.readFileSync(paths(ctx.state).admissionLog, 'utf8');

async function assertStays(ctx, id, { settleMs = MIN_QUEUED + 2500 } = {}) {
  await sleep(settleMs);
  assert.equal(ctx.fake.count('withdraw a'), 0, `no withdraw\n${ctx.fake.calls().join(',')}`);
  assert.equal(readAttempt(ctx.state, id)?.moving, undefined);
  ctx.unpause('a');
  assert.equal((await ctx.finish(id)).code, 0);
  assert.deepEqual(ctx.runs(), ['a'], 'ran once, on a');
}

async function assertMovesTo(ctx, id, to) {
  await waitFor(() => ctx.fake.count(`exec ${to}`) === 1, { timeoutMs: 30_000 });
  const waited = await ctx.finish(id, '60s');
  assert.equal(waited.code, 0, `${waited.stderr}\n${ctx.fake.calls().join(',')}`);
  assert.deepEqual(ctx.runs(), [to], `exactly once, on ${to}`);
  assert.equal(ctx.fake.count('withdraw a'), 1);
}

test('queued rebalance: behind another owner at position 2, the ticket moves to a runner with one queued and no owner, and runs once', T, async () => {
  const ctx = stuckSetup();
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  await assertMovesTo(ctx, id, 'b');
  assert.match(admissionLog(ctx), new RegExp(`lane: rebalance: ${id} behind another owner on a \\(pos 2\\) -> b \\(queued 1\\)`));
  assert.equal(resultOf(ctx.state, id).rebalancedFrom, 'a');
});

test('queued rebalance: a target whose headroom is measured but insufficient is still a destination, and the post-move log does not throw', T, async () => {
  const ctx = stuckSetup({ probes: { b: { ...QUEUED_B, headroom: { cpuCores: 0, memoryBytes: 1e12 } } } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  await assertMovesTo(ctx, id, 'b');
  assert.match(admissionLog(ctx), /lane: remote-rebalance: .* a -> b \(queued \d+s on a; b queued 1\)/);
  assert.match(admissionLog(ctx), /behind another owner on a \(pos 2\) -> b \(queued 1\)/);
});

test('queued rebalance: a busy runner with an empty queue and no headroom is a destination when it passes the filter', T, async () => {
  const ctx = stuckSetup({ probes: { b: BUSY } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  await assertMovesTo(ctx, id, 'b');
  assert.match(admissionLog(ctx), /behind another owner on a \(pos 2\) -> b \(queued 0\)/);
});

const STAYS = {
  'our own ticket is the owner (behindOtherOwner false)': { facts: { ...FACTS, behindOtherOwner: false } },
  'the target is itself a reservation owner': { probes: { b: { ...QUEUED_B, reservationOwner: true } } },
  'the target probe has no reservationOwner field': { probes: { b: without(QUEUED_B, 'reservationOwner') } },
  'the target lacks remote-withdraw/1': { probes: { b: { ...QUEUED_B, capabilities: IDLE.capabilities.filter((c) => c !== REMOTE_WITHDRAW_CAPABILITY) } } },
  'the target headroom is unmeasured': { probes: { b: without(QUEUED_B, 'headroom') } },
  'the target has 2 queued and remoteRebalanceQueuedMax is 1': { probes: { b: { ...QUEUED_B, queued: 2 } } },
  'the owner waited less than remoteRebalanceMinQueuedMs': { facts: { ...FACTS, reservationOwnerWaitedMs: MIN_QUEUED - 1 } },
  'the runner record has no facts (an old runner)': { facts: NO_FACTS },
  'remoteRebalanceQueuedMax is 0': { rebalance: { remoteRebalanceQueuedMax: 0 } },
  'the gain is exactly the threshold (strict)': { speed: 0.5, rebalance: { remoteRebalanceQueuedMax: 2 }, probes: { b: { ...QUEUED_B, queued: 2 } } },
};

for (const [name, setup] of Object.entries(STAYS)) {
  test(`queued rebalance: no move when ${name}`, T, async () => {
    const ctx = stuckSetup(setup);
    const id = await ctx.start();
    await execArrived(ctx, 'a');
    ctx.arm();
    await assertStays(ctx, id);
  });
}

test('queued rebalance: just under the gain threshold the same shape does move (the equality case above is the boundary)', T, async () => {
  const ctx = stuckSetup({ speed: 0.45, rebalance: { remoteRebalanceQueuedMax: 2 }, probes: { b: { ...QUEUED_B, queued: 2 } } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  await assertMovesTo(ctx, id, 'b');
});

test('queued rebalance: an idle runner is still preferred and takes the ordinary path (no non-idle log line)', T, async () => {
  const ctx = stuckSetup({ probes: { b: IDLE } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  await assertMovesTo(ctx, id, 'b');
  assert.doesNotMatch(admissionLog(ctx), /behind another owner/);
});

// ---- three runners: the preferred candidate fails each predicate in turn, another passes ----

const PREFERRED_FAILS = {
  'is a reservation owner': { ...BUSY, reservationOwner: true },
  'reports no reservationOwner field': without(BUSY, 'reservationOwner'),
  'lacks remote-withdraw/1': { ...BUSY, capabilities: IDLE.capabilities.filter((c) => c !== REMOTE_WITHDRAW_CAPABILITY) },
  'has unmeasured headroom': without(QUEUED_B, 'headroom'),
  'is queued deeper than the maximum': { ...BUSY, queued: 2 },
};

for (const [name, failing] of Object.entries(PREFERRED_FAILS)) {
  test(`queued rebalance, three runners: b (preferred, fewest queued) ${name}, so c is chosen`, T, async () => {
    const ctx = stuckSetup({ names: ['a', 'b', 'c'], probes: { b: failing, c: QUEUED_B } });
    const id = await ctx.start();
    await execArrived(ctx, 'a');
    ctx.arm();
    await assertMovesTo(ctx, id, 'c');
    assert.equal(ctx.fake.count('exec b'), 0);
  });
}

test('queued rebalance, three runners: b (preferred, fewest queued) is too slow to gain, so c is chosen', T, async () => {
  const ctx = stuckSetup({ names: ['a', 'b', 'c'], probes: { b: BUSY, c: QUEUED_B }, configOpts: { b: { speedFactor: 0.1 } } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  await assertMovesTo(ctx, id, 'c');
  assert.equal(ctx.fake.count('exec b'), 0);
});

// ---- callers that pass no filter are unchanged ----

const PROBE_BASE = { protocol: 1, protocols: [1, 2], paused: false, running: 0, capacity: {}, version: 't', capabilities: IDLE.capabilities };

/** A fake ssh answering `remote-probe` per destination, with `probes[dest]` over a base probe. */
function probeSsh(probes) {
  const bin = path.join(tmpDir('rebalance-queued-ssh'), 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const probes = ${JSON.stringify(probes)};
const argv = process.argv.slice(2);
const rest = [];
for (let i = 0; i < argv.length; i += 1) { if (argv[i] === '-o') { i += 1; continue; } rest.push(argv[i]); }
process.stdout.write(JSON.stringify({ ...${JSON.stringify(PROBE_BASE)}, ...probes[rest[0]] }) + '\\n');
`,
  );
  fs.chmodSync(bin, 0o755);
  return bin;
}

const reservation = { weight: 1, cpuCores: 1, memoryBytes: 1 };
const named = (...names) => names.map((n) => makeRunner({ ssh: n }));

test('callers without a queuedFilter treat a queued runner exactly as before (place, rebind with busy queue)', async () => {
  const sshBin = probeSsh({ q: { queued: 1, headroom: { cpuCores: 8, memoryBytes: 1e12 } }, busy: { queued: 0, headroom: { cpuCores: 0, memoryBytes: 1e12 } } });
  const place = await selectRunner(named('q'), { sshBin, reservation, maxRemoteQueue: 1 });
  assert.equal(place.queuedChoice, true, 'placement: queued runner without reservationOwner is a queued choice');
  const rebind = await selectRunner(named('q', 'busy'), { sshBin, reservation, maxRemoteQueue: 1, allowBusyQueue: true });
  assert.equal(rebind.runner.name, 'busy', 'rebind: the busy empty queue still wins on fewest queued');
  const filtered = await selectRunner(named('q', 'busy'), { sshBin, reservation, maxRemoteQueue: 1, allowBusyQueue: true, queuedFilter: (c) => c.runner.name === 'q' });
  assert.equal(filtered.runner.name, 'q', 'a filter drops the candidate it rejects before ranking');
});

test('rebalance options: drain and ordinary rebalance carry no queuedFilter and no queue; only a queuedMove adds them', () => {
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, runners: [] };
  const ticket = { id: 'x', lane: 'default', repoId: 'r', weight: 1, resources: { cpuCores: 1 }, remote: {} };
  const root = tmpDir('rebalance-queued-options');
  for (const draining of [false, true]) {
    const opts = selectionOptionsFor('rebalance', ticket, cfg, { root, draining });
    assert.equal(opts.queuedFilter, undefined, `draining=${draining}`);
    assert.equal(opts.maxRemoteQueue, 0);
    assert.equal(opts.allowBusyQueue, undefined);
  }
  for (const kind of ['place', 'rebind']) assert.equal(selectionOptionsFor(kind, ticket, cfg, { root }).queuedFilter, undefined, kind);
  const moved = selectionOptionsFor('rebalance', ticket, cfg, { root, queuedMove: { position: 2, sourceRunner: makeRunner({ ssh: 'a' }) } });
  assert.equal(moved.maxRemoteQueue, cfg.remoteRebalanceQueuedMax);
  assert.equal(typeof moved.queuedFilter, 'function');
});

test('config: remoteRebalanceQueuedMax defaults to 1', () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.remoteRebalanceQueuedMax, 1);
});

// ---- BRAIN-770 refusal precedence and the move races, on the new branch ----

function refusedStuckSetup({ cap = 1000, ...rest } = {}) {
  const ctx = stuckSetup({ rebalance: { remoteRefusedMaxQueuedMs: cap, remoteRebalanceMinQueuedMs: 300 }, ...rest });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, localRefused: true } } });
  return ctx;
}

test('A1: once the refusal cap has elapsed, a queued destination no longer blocks the refusal (changed tree: exit 76, zero executions)', T, async () => {
  const ctx = refusedStuckSetup({ names: ['a', 'b'] });
  gitFixture(['add', '-f', '.lane-broker.json'], ctx.repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], ctx.repoDir);
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  fs.appendFileSync(path.join(ctx.repoDir, '.lane-broker.json'), '\n');
  ctx.arm();
  const waited = await ctx.finish(id, '45s');
  assert.equal(waited.code, 76, `${waited.stderr}\n${ctx.fake.calls().join(',')}`);
  assert.ok(ctx.fake.count('withdraw a') >= 1, 'a confirmed withdraw');
  assert.equal(ctx.fake.count('exec b'), 0);
  assert.deepEqual(ctx.runs(), [], 'nothing ran anywhere');
  assert.match(admissionLog(ctx), /remote-refused: .*withdrawn from a/);
});

test('queued rebalance: a lost withdraw reply is MOVE-INTERRUPTED (74), never a second run', T, async () => {
  const ctx = stuckSetup({ runnerOpts: { a: { withdrawReplyLost: true, resultDownAfterWithdraw: true } } });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  const waited = await ctx.finish(id, '60s');
  assert.equal(waited.code, 74, waited.stderr);
  assert.match(waited.stderr, /MOVE-INTERRUPTED/);
  assert.deepEqual(ctx.runs(), []);
  assert.equal(readAttempt(ctx.state, id).moving?.from, 'a');
});

test('queued rebalance: a cancel that lands while the destination is being chosen stops the move before any withdraw', T, async () => {
  const ctx = stuckSetup();
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  const probesBefore = ctx.fake.count('probe b');
  ctx.fake.setProbe('b', { ...QUEUED_B, delayMs: 3000 });
  await waitFor(() => ctx.fake.count('probe b') > probesBefore, { timeoutMs: 30_000 });
  const cancelled = await laneRun(['cancel', id], { env: ctx.env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  const waited = await ctx.finish(id, '30s');
  assert.equal(waited.code, 130, waited.stderr);
  await sleep(3500);
  assert.equal(ctx.fake.count('withdraw a'), 0);
  assert.deepEqual(ctx.runs(), []);
});

test('queued rebalance: the cumulative refusal cap still counts from the first queueing after the move (76 on b, never run)', T, async () => {
  const MIN = 5000;
  const CAP = 15_000;
  const ctx = refusedStuckSetup({ cap: CAP, rebalance: { remoteRebalanceMinQueuedMs: MIN, remoteRefusedMaxQueuedMs: CAP }, paused: ['a', 'b'] });
  const id = await ctx.start();
  await execArrived(ctx, 'a');
  ctx.arm();
  await waitFor(() => readAttempt(ctx.state, id)?.moves?.length === 1, { timeoutMs: 60_000 });
  const moved = readAttempt(ctx.state, id);
  assert.equal(moved.runner, 'b');
  assert.ok(moved.firstQueuedAt < moved.moves[0].at, 'stamped on a, kept across the move');
  const waited = await ctx.finish(id, '90s');
  assert.equal(waited.code, 76, `${waited.stderr}\n${ctx.fake.calls().join(',')}`);
  const { endedAt } = resultOf(ctx.state, id);
  assert.ok(endedAt - moved.firstQueuedAt >= CAP, 'refused only after the whole cap');
  assert.ok(endedAt < moved.moves[0].at + CAP, 'and not after a fresh cap on b');
  assert.deepEqual(ctx.runs(), []);
});
