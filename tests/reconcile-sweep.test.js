import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeGlobalConfig, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, resultOf } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { paths, atomicWriteJson, ensureStateDirs, bootId } from '../src/state.js';
import { processStartTime } from '../src/process-liveness.js';
import { sweepOrphans } from '../src/reconcile-sweep.js';

/**
 * BRAIN-515 step 4: the background sweep that settles ORPHANED-REMOTE attempts without a `lane cancel`. Same fake-ssh transport as
 * tests/remote-reconcile.test.js; orphans are seeded (a dead-supervisor attempt plus the runner's own ticket dir) so each runner
 * answer is exact.
 */

const DEAD_SUPERVISOR = { pid: 2 ** 22 + 7, startTime: null, bootId: 'dead' };
const MANIFEST = 'm'.repeat(64);
const NO_STATUS_SPAWN = { LANE_BROKER_TEST_NO_STATUS_RECONCILE: '1' };

function context() {
  const ctx = setup();
  const globalCfg = JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8'));
  return { ...ctx, globalCfg };
}

/** A second runner behind a different fake-ssh destination, in the client's own global config. */
function addRunner(ctx, name, ssh) {
  const root = tmpDir(`sweep-runner-${name}`);
  ctx.globalCfg = { ...ctx.globalCfg, runners: [...ctx.globalCfg.runners, { name, ssh, shell: 'sh -c', root }] };
  writeGlobalConfig(ctx.home, ctx.globalCfg);
  return root;
}

const sweep = (ctx, extra = {}) => sweepOrphans(ctx.state, { globalCfg: ctx.globalCfg, env: ctx.env, deadlineMs: 20_000, ...extra });

/** A dead-supervisor bound remote attempt on `runner`, and (optionally) the runner's own record for it. */
function seedOrphan(ctx, { runner = 'skybox', runnerRoot = ctx.runnerRoot, record, attempt = {} } = {}) {
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), {
    id,
    generation: 0,
    executor: 'remote',
    phase: 'running',
    runner,
    startedAt: Date.now() - 60_000,
    supervisor: DEAD_SUPERVISOR,
    binding: { protocol: 1, generation: 0, manifestHash: MANIFEST, runner },
    ...attempt,
  });
  if (record) {
    const dir = path.join(runnerRoot, 'tickets', id);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'result.json'), JSON.stringify({ protocol: 1, ticketId: id, generation: 0, manifestHash: MANIFEST, ...record }));
  }
  return id;
}

const completed = (exit) => ({ kind: 'completed', exit, signal: null, reason: null, runMs: 10, observedCpu: { peak: 0.5, mean: 0.25, samples: 2 }, deps: { outcome: 'hit', ms: 7 } });

const statusRemote = async (ctx) => JSON.parse((await laneRun(['status', '--json'], { env: { ...ctx.env, ...NO_STATUS_SPAWN }, cwd: ctx.repoDir })).stdout).remote;

/** An `ssh` that logs each call's destination, then delegates to the harness's fake transport. */
function loggingSsh(ctx) {
  const real = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  const log = path.join(tmpDir('sweep-ssh-log'), 'calls.log');
  const bin = path.join(path.dirname(log), 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const { spawnSync } = require('child_process');
const a = process.argv.slice(2);
require('fs').appendFileSync(${JSON.stringify(log)}, a[a.length - 2] + '\\n');
const r = spawnSync(${JSON.stringify(real)}, a, { stdio: 'inherit' });
process.exit(r.status == null ? 1 : r.status);
`,
  );
  fs.chmodSync(bin, 0o755);
  return { bin, callsTo: (dest) => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter((d) => d === dest).length : 0) };
}

test('two orphans on a reachable runner: one finished (exit 3, bound), one gone -> adopted 3 and lost 75; status lists neither', async () => {
  const ctx = context();
  const finished = seedOrphan(ctx, { record: completed(3), attempt: { startedAt: Date.now() - 120_000 } });
  const gone = seedOrphan(ctx);
  assert.equal((await statusRemote(ctx)).filter((r) => r.orphaned).length, 2);

  const out = await sweep(ctx);
  assert.equal(out.ran, true);
  assert.deepEqual(
    out.results.map(({ id, runner, state, exit }) => ({ id, runner, state, exit })),
    [
      { id: finished, runner: 'skybox', state: 'adopted', exit: 3 },
      { id: gone, runner: 'skybox', state: 'lost', exit: 75 },
    ],
    'oldest first',
  );
  assert.equal(resultOf(ctx.state, finished).exit, 3);
  assert.equal(resultOf(ctx.state, gone).reason, 'runner-lost');
  assert.deepEqual(await statusRemote(ctx), []);
});

test('runner A down, runner B up: only A\'s first attempt is asked (no ssh for the rest); B\'s are settled', async () => {
  const ctx = context();
  const aRoot = addRunner(ctx, 'a', 'down');
  const a1 = seedOrphan(ctx, { runner: 'a', runnerRoot: aRoot, record: completed(0), attempt: { startedAt: Date.now() - 300_000 } });
  const b1 = seedOrphan(ctx, { record: completed(4), attempt: { startedAt: Date.now() - 200_000 } });
  const a2 = seedOrphan(ctx, { runner: 'a', runnerRoot: aRoot, record: completed(0), attempt: { startedAt: Date.now() - 100_000 } });
  const b2 = seedOrphan(ctx);
  const ssh = loggingSsh(ctx);

  const out = await sweep(ctx, { sshBin: ssh.bin });
  assert.equal(ssh.callsTo('down'), 1, 'runner a is asked once, then skipped for the rest of the sweep');
  const stateOf = Object.fromEntries(out.results.map((r) => [r.id, r.state]));
  assert.deepEqual(stateOf, { [a1]: 'unreachable', [b1]: 'adopted', [a2]: 'skipped-runner-down', [b2]: 'lost' });
  assert.ok(readAttempt(ctx.state, a1) && readAttempt(ctx.state, a2), 'a\'s attempts are kept');
  assert.equal(resultOf(ctx.state, b1).exit, 4);
  assert.equal(resultOf(ctx.state, b2).exit, 75);
});

/** An `ssh` that logs each call's subcommand, answers like the fake runner, but fails every `remote-cancel` (exit 255). */
function cancelFailingSsh(ctx) {
  const real = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  const log = path.join(tmpDir('sweep-cancel-log'), 'calls.log');
  const bin = path.join(path.dirname(log), 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const { spawnSync } = require('child_process');
const a = process.argv.slice(2);
const sub = (/remote-(probe|exec|result|cancel|withdraw)/.exec(a[a.length - 1]) || [])[1];
require('fs').appendFileSync(${JSON.stringify(log)}, sub + '\\n');
if (sub === 'cancel') process.exit(255);
const r = spawnSync(${JSON.stringify(real)}, a, { stdio: 'inherit' });
process.exit(r.status == null ? 1 : r.status);
`,
  );
  fs.chmodSync(bin, 0o755);
  return { bin, calls: () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter(Boolean) : []) };
}

test('a runner that answers gone but cannot confirm the cancel is treated as down: its first attempt is unreachable, the rest skip with no further ssh', async () => {
  const ctx = context();
  const first = seedOrphan(ctx, { attempt: { startedAt: Date.now() - 300_000 } });
  const second = seedOrphan(ctx, { attempt: { startedAt: Date.now() - 200_000 } });
  const third = seedOrphan(ctx, { attempt: { startedAt: Date.now() - 100_000 } });
  const ssh = cancelFailingSsh(ctx);

  const out = await sweep(ctx, { sshBin: ssh.bin });
  assert.deepEqual(
    out.results.map(({ id, state }) => ({ id, state })),
    [
      { id: first, state: 'unreachable' },
      { id: second, state: 'skipped-runner-down' },
      { id: third, state: 'skipped-runner-down' },
    ],
  );
  assert.match(out.results[0].reason, /remote cancel not confirmed/);
  assert.deepEqual(ssh.calls(), ['result', 'cancel'], 'one result fetch and one cancel for the whole runner, nothing for the other two');
  for (const id of [first, second, third]) {
    assert.ok(readAttempt(ctx.state, id), 'every attempt is kept');
    assert.equal(resultOf(ctx.state, id), null);
  }
});

test('claim: concurrent in-process sweeps run exactly one; a second call within 5 min is recently swept; force bypasses', async () => {
  const ctx = context();
  const outs = await Promise.all([sweep(ctx), sweep(ctx), sweep(ctx)]);
  assert.equal(outs.filter((o) => o.ran).length, 1);
  assert.deepEqual(outs.filter((o) => !o.ran), [{ ran: false, reason: 'recently swept' }, { ran: false, reason: 'recently swept' }]);
  assert.deepEqual(await sweep(ctx), { ran: false, reason: 'recently swept' });
  assert.equal((await sweep(ctx, { force: true })).ran, true);
  assert.equal((await sweep(ctx, { now: Date.now() + 6 * 60_000 })).ran, true, 'due again after 5 minutes');
});

test('claim: concurrent sweeps in separate processes, all held at the claim, run exactly one', async () => {
  const ctx = context();
  const hold = tmpDir('sweep-claim-hold');
  const env = { ...ctx.env, LANE_BROKER_TEST_PAUSE_IN_RECONCILE_CLAIM: hold };
  const runs = [1, 2, 3].map(() => laneRun(['reconcile', '--json'], { env, cwd: ctx.repoDir }));
  await waitFor(() => fs.readdirSync(hold).some((n) => n.startsWith('ready-')), { timeoutMs: 15_000 });
  await sleep(1500); // everything that can reach the claim has: with the lock only one is inside, without it all are
  fs.writeFileSync(path.join(hold, 'go'), '');
  const outs = (await Promise.all(runs)).map((r) => {
    assert.equal(r.code, 0, r.stderr);
    return JSON.parse(r.stdout);
  });
  assert.equal(outs.filter((o) => o.ran).length, 1, JSON.stringify(outs));
  assert.deepEqual([...new Set(outs.filter((o) => !o.ran).map((o) => o.reason))], ['recently swept']);
});

test('a live-supervisor attempt and a moving attempt are not touched', async () => {
  const ctx = context();
  const live = seedOrphan(ctx, { record: completed(3), attempt: { supervisor: { pid: process.pid, startTime: processStartTime(process.pid), bootId: bootId() } } });
  const moving = seedOrphan(ctx, { record: completed(3), attempt: { moving: { from: 'skybox', to: 'other', phase: 'dispatching', at: Date.now() } } });
  const out = await sweep(ctx);
  assert.deepEqual(out, { ran: true, results: [] });
  for (const id of [live, moving]) {
    assert.ok(readAttempt(ctx.state, id));
    assert.equal(resultOf(ctx.state, id), null);
  }
});

test('an unreadable config does not claim', async () => {
  const ctx = context();
  assert.deepEqual(await sweep(ctx, { globalCfg: null }), { ran: false, reason: 'config unreadable' });
  assert.equal(fs.existsSync(paths(ctx.state).reconcileLastRun), false);
  assert.equal((await sweep(ctx)).ran, true);
});

test('lane reconcile --json prints the results; a second run within 5 min prints the swept-recently line; --now bypasses', async () => {
  const ctx = context();
  const id = seedOrphan(ctx, { record: completed(3) });
  const run = (args) => laneRun(['reconcile', ...args], { env: ctx.env, cwd: ctx.repoDir });

  const first = await run(['--json']);
  assert.equal(first.code, 0, first.stderr);
  const parsed = JSON.parse(first.stdout);
  assert.equal(parsed.ran, true);
  assert.deepEqual(parsed.results, [{ id, runner: 'skybox', state: 'adopted', exit: 3 }]);

  const again = await run([]);
  assert.equal(again.code, 0);
  assert.equal(again.stdout, 'lane reconcile: swept recently; use --now\n');

  const id2 = seedOrphan(ctx);
  const forced = await run(['--now']);
  assert.equal(forced.code, 0);
  assert.equal(forced.stdout, `${id2} skybox lost exit 75\n`);
});

test('lane status with an orphan spawns a detached sweep that settles it, and returns without waiting for the runner', async () => {
  const ctx = context();
  ctx.globalCfg = { ...ctx.globalCfg, runners: ctx.globalCfg.runners.map((r) => ({ ...r, ssh: 'slow-result' })) }; // a result answer lands ~6 s late
  writeGlobalConfig(ctx.home, ctx.globalCfg);
  const id = seedOrphan(ctx, { record: completed(3) });

  const started = Date.now();
  const out = await laneRun(['status'], { env: ctx.env, cwd: ctx.repoDir });
  const tookMs = Date.now() - started;
  assert.equal(out.code, 0, out.stderr);
  assert.ok(tookMs < 2000, `status took ${tookMs} ms; it must not wait for the sweep`);
  assert.match(out.stdout, new RegExp(`${id}.*\\[ORPHANED-REMOTE\\] — reconciles automatically when skybox answers \\(or: lane cancel ${id}\\)`));
  assert.equal(resultOf(ctx.state, id), null, 'status itself settled nothing');

  await waitFor(() => resultOf(ctx.state, id) !== null, { timeoutMs: 25_000, intervalMs: 100 });
  assert.equal(resultOf(ctx.state, id).exit, 3);
  assert.equal(resultOf(ctx.state, id).reconciled, true);
});

test('lane status --json spawns the sweep too; the test-only env disables it', async () => {
  const off = context();
  const offId = seedOrphan(off, { record: completed(3) });
  assert.equal((await laneRun(['status', '--json'], { env: { ...off.env, ...NO_STATUS_SPAWN }, cwd: off.repoDir })).code, 0);
  await sleep(2500);
  assert.equal(resultOf(off.state, offId), null, 'LANE_BROKER_TEST_NO_STATUS_RECONCILE=1 must keep status from forking a sweep');

  const on = context();
  const onId = seedOrphan(on, { record: completed(3) });
  assert.equal((await laneRun(['status', '--json'], { env: on.env, cwd: on.repoDir })).code, 0);
  await waitFor(() => resultOf(on.state, onId) !== null, { timeoutMs: 15_000, intervalMs: 100 });
  assert.equal(resultOf(on.state, onId).exit, 3);
});

test('lane status without an orphan (or with only an interrupted move) spawns no sweep', async () => {
  const ctx = context();
  seedOrphan(ctx, { record: completed(3), attempt: { moving: { from: 'skybox', to: 'other', phase: 'dispatching', at: Date.now() } } });
  const out = await laneRun(['status'], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(out.code, 0, out.stderr);
  await sleep(2000);
  assert.equal(fs.existsSync(paths(ctx.state).reconcileLastRun), false, 'no sweep claimed');
});
