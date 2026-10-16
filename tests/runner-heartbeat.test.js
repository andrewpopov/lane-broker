import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { runnerWatchOnce } from '../src/runner-watch.js';
import { readUnreachable, readHeartbeat, downRunnerReasons, collectRunnerStates, renderRunnersLines, recordContact, clock } from '../src/runner-unreachable.js';
import { selectRunner } from '../src/remote-client.js';
import { reconcileRemoteAttempt } from '../src/remote-reconcile.js';
import { remoteSelectOptions, selectionOptionsFor } from '../src/supervisor.js';
import { loadGlobalConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths, atomicWriteJson, readJsonSafe, ensureStateDirs, withLock } from '../src/state.js';
import { readAttempt } from '../src/attempts.js';
import { tmpDir, setup, resultOf, probeCount, markerCmd, detachAndWait } from './remote-harness.js';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, waitFor } from './helpers.js';

/**
 * BRAIN-597: the runner heartbeat (`lane runner watch`) on top of BRAIN-594's runner-unreachable.json: the shared clock helper,
 * the watcher tick (lock, pings, sweep), placement skipping a runner unreachable long enough, and the RUNNERS status.
 */

const MIN = 60_000;
const HOUR = 60 * MIN;
const NOW = Date.parse('2026-10-15T12:00:00Z');
const SKYBOX = { name: 'skybox', ssh: 'skybox-runner' };
const WINTOP = { name: 'wintop', ssh: 'wintop-runner' };
const CFG = { ...DEFAULT_GLOBAL_CONFIG, runners: [SKYBOX, WINTOP] };
const DEAD_PID = 2 ** 22 + 11;

/** A held `withLock` lock: a directory whose owner.json names `pid`. */
function seedLockOwner(lockDir, pid) {
  fs.mkdirSync(lockDir, { recursive: true });
  fs.writeFileSync(path.join(lockDir, 'owner.json'), JSON.stringify({ pid, token: `seed-${pid}`, start: null, at: Date.now() }));
}

/** A fake ssh: logs each call's argv, answers a probe like an idle runner, and fails every call to a destination listed in `<dir>/failing`. */
function fakeSsh() {
  const dir = tmpDir('heartbeat-ssh');
  const log = path.join(dir, 'calls.log');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const fs = require('fs');
const argv = process.argv.slice(2);
fs.appendFileSync(${JSON.stringify(log)}, argv.join(' ') + '\\n');
const failing = fs.existsSync(${JSON.stringify(path.join(dir, 'failing'))}) ? fs.readFileSync(${JSON.stringify(path.join(dir, 'failing'))}, 'utf8').split('\\n') : [];
if (failing.some((d) => d && argv.includes(d))) process.exit(255);
if (argv.join(' ').includes('remote-probe')) {
  process.stdout.write(JSON.stringify({ protocol: 1, protocols: [1, 2], paused: false, queued: 0, running: 0, capacity: {} }) + '\\n');
}
process.exit(0);
`,
  );
  fs.chmodSync(bin, 0o755);
  const calls = () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter(Boolean) : []);
  return {
    bin,
    calls,
    callsTo: (ssh) => calls().filter((c) => c.split(' ').includes(ssh)).length,
    fail: (...destinations) => fs.writeFileSync(path.join(dir, 'failing'), destinations.join('\n')),
  };
}

test('watch: a failed ping starts the clock, a second keeps its first time, an ok ping clears it; the ping is `true` with BatchMode and ConnectTimeout=5', async () => {
  const root = tmpDir('heartbeat-root');
  const ssh = fakeSsh();
  const cfg = { ...CFG };
  const tick = (now) => runnerWatchOnce(root, { cfg, sshBin: ssh.bin, now });

  await tick(NOW);
  assert.ok(ssh.calls().some((c) => /^-o BatchMode=yes -o ConnectTimeout=5 .*skybox-runner true$/.test(c)), ssh.calls().join('\n'));
  assert.deepEqual(readUnreachable(root), {});
  assert.deepEqual(readHeartbeat(root).runners.skybox, { lastOkAt: NOW, lastPingAt: NOW });

  ssh.fail('skybox-runner');
  await tick(NOW + MIN);
  await tick(NOW + 2 * MIN);
  assert.deepEqual(readUnreachable(root), { skybox: NOW + MIN }, 'the first failed contact is kept');
  assert.deepEqual(readHeartbeat(root).runners.skybox, { lastOkAt: NOW, lastPingAt: NOW + 2 * MIN });
  assert.equal(readHeartbeat(root).at, NOW + 2 * MIN);
  assert.deepEqual(readHeartbeat(root).runners.wintop, { lastOkAt: NOW + 2 * MIN, lastPingAt: NOW + 2 * MIN });

  ssh.fail();
  await tick(NOW + 3 * MIN);
  assert.deepEqual(readUnreachable(root), {});
  assert.equal(readHeartbeat(root).runners.skybox.lastOkAt, NOW + 3 * MIN);
  assert.equal(fs.existsSync(paths(root).runnerWatchLock), false, 'the tick releases its lock');
});

test('watch and reconcile share one clock: a failed ping starts it, reconcile\'s failed fetch keeps that time, and the file keeps the BRAIN-594 per-runner shape', async () => {
  const ctx = setup({ ssh: 'down' });
  const globalCfg = { ...DEFAULT_GLOBAL_CONFIG, ...JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8')) };
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), {
    id, generation: 0, executor: 'remote', phase: 'running', runner: 'skybox', startedAt: NOW - 10 * MIN,
    supervisor: { pid: DEAD_PID, startTime: null, bootId: 'dead' }, binding: { protocol: 1, generation: 0, manifestHash: 'm'.repeat(64), runner: 'skybox' },
  });
  const ssh = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  await runnerWatchOnce(ctx.state, { cfg: globalCfg, sshBin: ssh, env: ctx.env, now: NOW });
  assert.equal(readJsonSafe(paths(ctx.state).runnerUnreachable).skybox, NOW, 'a plain number, as BRAIN-594 wrote it');
  const outcome = await reconcileRemoteAttempt(ctx.state, id, { globalCfg, sshBin: ssh, env: ctx.env, now: NOW + 5 * MIN });
  assert.equal(outcome.state, 'unreachable');
  assert.equal(readUnreachable(ctx.state).skybox, NOW, 'reconcile did not restart the streak the watcher began');
  assert.ok(readAttempt(ctx.state, id));
});

test('watch: an offline runner is not pinged; a runner outside its schedule still is', async () => {
  const root = tmpDir('heartbeat-offline');
  const ssh = fakeSsh();
  const night = { ...WINTOP, schedule: { timezone: 'UTC', available: [{ days: ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'], from: '06:00', to: '07:00' }] } };
  await runnerWatchOnce(root, { cfg: { ...CFG, runners: [{ ...SKYBOX, offline: true }, night] }, sshBin: ssh.bin, now: NOW });
  assert.equal(ssh.callsTo('skybox-runner'), 0);
  assert.equal(ssh.callsTo('wintop-runner'), 1, 'NOW is outside 06:00-07:00 UTC and it is pinged anyway');
});

test('watch: a held lock exits without pinging or writing', async () => {
  const root = tmpDir('heartbeat-held');
  const ssh = fakeSsh();
  seedLockOwner(paths(root).runnerWatchLock, process.pid);
  const res = await runnerWatchOnce(root, { cfg: CFG, sshBin: ssh.bin, now: NOW });
  assert.equal(res.ran, false);
  assert.equal(ssh.calls().length, 0);
  assert.equal(fs.existsSync(paths(root).runnerUnreachable), false);
});

test('watch: a lock left by a dead process is replaced', async () => {
  const root = tmpDir('heartbeat-dead-lock');
  seedLockOwner(paths(root).runnerWatchLock, DEAD_PID);
  const res = await runnerWatchOnce(root, { cfg: { ...CFG, runners: [] }, sshBin: fakeSsh().bin, now: NOW });
  assert.equal(res.ran, true);
  assert.equal(fs.existsSync(paths(root).runnerWatchLock), false);
});

test('watch runs the sweep: an orphan on a runner unreachable past runnerLostAfterMs is settled as runner-lost by one tick, with no status or wait call', async () => {
  const ctx = setup({ ssh: 'down' });
  const globalCfg = { ...DEFAULT_GLOBAL_CONFIG, ...JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8')) };
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), {
    id, generation: 0, executor: 'remote', phase: 'running', runner: 'skybox', startedAt: NOW - 3 * HOUR,
    supervisor: { pid: DEAD_PID, startTime: null, bootId: 'dead' }, binding: { protocol: 1, generation: 0, manifestHash: 'm'.repeat(64), runner: 'skybox' },
  });
  atomicWriteJson(paths(ctx.state).runnerUnreachable, { skybox: NOW - 3 * HOUR });
  const ssh = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  const res = await runnerWatchOnce(ctx.state, { cfg: globalCfg, sshBin: ssh, env: ctx.env, now: NOW });
  assert.equal(res.sweep.ran, true);
  assert.equal(resultOf(ctx.state, id).reason, 'runner-lost');
  assert.equal(resultOf(ctx.state, id).exit, 75);
  assert.equal(readAttempt(ctx.state, id), null);
});

/** A child that records a contact for `name`, parked inside the critical section (after its read) until `<dir>/go` exists. */
function contactWorker(dir) {
  const file = path.join(dir, 'worker.mjs');
  fs.writeFileSync(
    file,
    `import { recordContact } from ${JSON.stringify(new URL('../src/runner-unreachable.js', import.meta.url).href)};
const [root, name, at] = process.argv.slice(2);
await recordContact(root, name, false, Number(at));
`,
  );
  return file;
}

test('concurrent contact records from separate processes do not lose an update (the read-modify-write is locked)', async () => {
  const root = tmpDir('heartbeat-concurrent');
  const dir = tmpDir('heartbeat-concurrent-seam');
  const worker = contactWorker(dir);
  const env = { ...process.env, LANE_BROKER_TEST_PAUSE_IN_UNREACHABLE_UPDATE: dir };
  const children = ['alpha', 'beta'].map((name, i) => {
    const child = spawn(process.execPath, [worker, root, name, String(NOW + i)], { env, stdio: 'inherit' });
    return { child, closed: new Promise((resolve) => child.on('close', resolve)) };
  });
  const ready = () => fs.readdirSync(dir).filter((n) => n.startsWith('ready-'));
  let enteredTogether;
  try {
    await waitFor(() => ready().length >= 1, { timeoutMs: 10_000 });
    await new Promise((resolve) => setTimeout(resolve, 600));
    enteredTogether = ready().length;
  } finally {
    fs.writeFileSync(path.join(dir, 'go'), ''); // always release the parked children, even when the assertion below fails
  }
  await Promise.all(children.map((c) => c.closed));
  assert.equal(enteredTogether, 1, 'the second process must wait for the lock, not enter the critical section alongside the first');
  assert.deepEqual(Object.keys(readUnreachable(root)).sort(), ['alpha', 'beta'], 'no lost update');
});

test('a reconcile whose deadline passes while the clock lock is held returns the timeout outcome instead of waiting', async () => {
  const ctx = setup({ ssh: 'down' });
  const globalCfg = { ...DEFAULT_GLOBAL_CONFIG, ...JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8')) };
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), {
    id, generation: 0, executor: 'remote', phase: 'running', runner: 'skybox', startedAt: NOW,
    supervisor: { pid: DEAD_PID, startTime: null, bootId: 'dead' }, binding: { protocol: 1, generation: 0, manifestHash: 'm'.repeat(64), runner: 'skybox' },
  });
  seedLockOwner(paths(ctx.state).runnerUnreachableLock, process.pid);
  const ssh = path.join(ctx.env.PATH.split(path.delimiter)[0], 'ssh');
  const started = Date.now();
  const outcome = await reconcileRemoteAttempt(ctx.state, id, { globalCfg, sshBin: ssh, env: ctx.env, deadlineAt: Date.now() + 1500 });
  assert.equal(outcome.timedOut, true, JSON.stringify(outcome));
  assert.ok(Date.now() - started < 4000, 'bounded by the deadline, not the 5 s lock wait');
});

test('lock order: a heartbeat lock is never taken under the broker lock, nor the broker lock under a heartbeat lock', async () => {
  const root = tmpDir('heartbeat-order');
  const heartbeat = { lockPath: paths(root).runnerUnreachableLock, group: 'heartbeat' };
  await assert.rejects(withLock(root, () => withLock(root, async () => {}, heartbeat)), /lock order violation/);
  await assert.rejects(withLock(root, () => withLock(root, async () => {}), heartbeat), /lock order violation/);
  await withLock(root, async () => {}, heartbeat);
  await withLock(root, async () => {});
});

test('lock order: a continuation created inside a heartbeat lock that runs after its release may take the broker lock', async () => {
  const root = tmpDir('heartbeat-order-late');
  let later;
  await withLock(
    root,
    async () => {
      later = new Promise((resolve, reject) => setTimeout(() => withLock(root, async () => 'broker').then(resolve, reject), 50));
    },
    { lockPath: paths(root).runnerUnreachableLock, group: 'heartbeat' },
  );
  assert.equal(await later, 'broker');
});

// ---- placement ---------------------------------------------------------------------------------------------------------

const beat = (at, runners = {}) => ({ at, runners });
const seed = (root, unreachable, heartbeat) => atomicWriteJson(paths(root).runnerUnreachable, { ...unreachable, ...(heartbeat ? { __heartbeat: heartbeat } : {}) });
const placementCfg = { ...CFG, runnerDownAfterMs: 5 * MIN };

test('downRunnerReasons: skipped at runnerDownAfterMs, not before; a stale file, an offline runner and 0 are ignored', () => {
  const root = tmpDir('heartbeat-reasons');
  seed(root, { skybox: NOW - 5 * MIN, wintop: NOW - 4 * MIN }, beat(NOW - MIN));
  assert.deepEqual(downRunnerReasons(root, placementCfg, NOW), { skybox: `unreachable since ${clock(NOW - 5 * MIN)}` });
  assert.deepEqual(downRunnerReasons(root, placementCfg, NOW + 2 * MIN), { skybox: `unreachable since ${clock(NOW - 5 * MIN)}`, wintop: `unreachable since ${clock(NOW - 4 * MIN)}` });
  assert.deepEqual(downRunnerReasons(root, placementCfg, NOW - MIN + 5 * MIN + 1), {}, 'a heartbeat older than 5 minutes is not trusted');
  assert.deepEqual(downRunnerReasons(root, { ...placementCfg, runners: [{ ...SKYBOX, offline: true }, WINTOP] }, NOW + 2 * MIN), { wintop: `unreachable since ${clock(NOW - 4 * MIN)}` });
  assert.deepEqual(downRunnerReasons(root, { ...placementCfg, runnerDownAfterMs: 0 }, NOW), {});
  const noBeat = tmpDir('heartbeat-reasons-nobeat');
  seed(noBeat, { skybox: NOW - HOUR });
  assert.deepEqual(downRunnerReasons(noBeat, placementCfg, NOW), {}, 'a clock with no watcher heartbeat is not trusted');
});

const ENRICHED = { repoId: 'r', lane: 't', remote: {}, resources: {}, weight: 1 };

for (const kind of ['place', 'rebind', 'rebalance']) {
  test(`${kind}: a runner unreachable past runnerDownAfterMs gets zero probes through the options the supervisor path uses; one under it, or behind a stale file, is probed`, async () => {
    const run = async (unreachableFor, beatAge) => {
      const root = tmpDir(`heartbeat-${kind}`);
      const now = Date.now();
      seed(root, { skybox: now - unreachableFor }, beat(now - beatAge));
      const ssh = fakeSsh();
      const options = selectionOptionsFor(kind, ENRICHED, placementCfg, { root, stuckLocally: true, draining: true });
      const res = await selectRunner(placementCfg.runners, { ...options, sshBin: ssh.bin, deadlineMs: 3000 });
      return { ssh, res };
    };
    const down = await run(10 * MIN, MIN);
    assert.equal(down.ssh.callsTo('skybox-runner'), 0, `${kind} probed the unreachable runner`);
    assert.equal(down.ssh.callsTo('wintop-runner'), 1);
    assert.match(down.res.skipped.find((x) => x.name === 'skybox').reason, /^unreachable since \d\d:\d\d$/);

    const brief = await run(2 * MIN, MIN);
    assert.equal(brief.ssh.callsTo('skybox-runner'), 1, 'unreachable for less than runnerDownAfterMs is still probed');

    const stale = await run(10 * MIN, 6 * MIN);
    assert.equal(stale.ssh.callsTo('skybox-runner'), 1, 'a file the watcher no longer keeps fresh is ignored');
  });
}

test('remoteSelectOptions carries downRunners only when a runner is down', () => {
  const root = tmpDir('heartbeat-options');
  assert.equal('downRunners' in remoteSelectOptions(ENRICHED, placementCfg, root), false);
  seed(root, { skybox: Date.now() - HOUR }, beat(Date.now()));
  assert.deepEqual(Object.keys(remoteSelectOptions(ENRICHED, placementCfg, root).downRunners), ['skybox']);
});

test('selectRunner: with every runner down nothing is probed and none is chosen', async () => {
  const ssh = fakeSsh();
  const res = await selectRunner([SKYBOX], { sshBin: ssh.bin, deadlineMs: 3000, downRunners: { skybox: 'unreachable since 11:00' } });
  assert.equal(res.runner, null);
  assert.equal(ssh.calls().length, 0);
});

test('the real placement path: a persisted unreachable clock means the supervisor never probes that runner and runs the lane locally', async () => {
  const run = async (seedClock) => {
    const ctx = setup();
    writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remotePolicy: 'remote-first' } } });
    if (seedClock) seed(ctx.state, { skybox: Date.now() - 10 * MIN }, beat(Date.now()));
    const marker = path.join(tmpDir('marker'), 'where');
    const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], ctx.env, ctx.repoDir);
    assert.equal(waited.code, 0, waited.stderr);
    return { where: fs.readFileSync(marker, 'utf8'), probes: probeCount(ctx.probeLogPath) };
  };
  const down = await run(true);
  assert.deepEqual(down, { where: 'local', probes: 0 });
  const control = await run(false);
  assert.equal(control.where, 'remote');
  assert.ok(control.probes >= 1, 'without the clock the same setup does probe the runner');
});

// ---- status ------------------------------------------------------------------------------------------------------------

test('collectRunnerStates / renderRunnersLines: every state, last-ok age, lost-settle time and the stale-watcher line', () => {
  const root = tmpDir('heartbeat-states');
  const cfg = { ...placementCfg, runnerLostAfterMs: 2 * HOUR, runners: [SKYBOX, WINTOP, { name: 'old', ssh: 'o', offline: true }, { name: 'night', ssh: 'n', schedule: { timezone: 'UTC', available: [{ days: ['mon'], from: '06:00', to: '07:00' }] } }, { name: 'ok', ssh: 'k' }] };
  seed(root, { skybox: NOW - 10 * MIN, wintop: NOW - 2 * MIN }, beat(NOW - 30_000, { skybox: { lastOkAt: NOW - 12 * MIN, lastPingAt: NOW - 30_000 }, ok: { lastOkAt: NOW - 30_000, lastPingAt: NOW - 30_000 } }));
  const states = collectRunnerStates(root, cfg, NOW);
  assert.deepEqual(states.map((s) => [s.name, s.state]), [['skybox', 'down'], ['wintop', 'unreachable'], ['old', 'offline'], ['night', 'off-schedule'], ['ok', 'up']]);
  assert.equal(states[0].lostSettleAt, NOW - 10 * MIN + 2 * HOUR);
  assert.equal(states[2].lostSettleAt, null);
  assert.equal(states[0].watcherRunning, true);
  const text = renderRunnersLines(states, NOW).join('\n');
  assert.match(text, new RegExp(`^RUNNERS:\\n {2}skybox {2}down \\(placement skips; unreachable since ${clock(NOW - 10 * MIN)}\\) {2}last-ok 12m ago {2}lost-settle ${clock(NOW - 10 * MIN + 2 * HOUR)}`));
  assert.match(text, new RegExp(`wintop {2}unreachable since ${clock(NOW - 2 * MIN)} {2}last-ok never`));
  assert.match(text, / {2}old {2}offline /);
  assert.match(text, / {2}night {2}off-schedule /);
  assert.match(text, / {2}ok {2}up {2}last-ok 30s ago/);
  assert.doesNotMatch(text, /watcher not running/);

  seed(root, { skybox: NOW - 10 * MIN }, beat(NOW - 12 * MIN));
  const stale = renderRunnersLines(collectRunnerStates(root, cfg, NOW), NOW).join('\n');
  assert.match(stale, /^RUNNERS:\n {2}watcher not running \(state 12m old\)/);
  assert.match(stale, /skybox {2}unreachable since/, 'a stale file never reads as down');
  assert.deepEqual(collectRunnerStates(tmpDir('heartbeat-none'), DEFAULT_GLOBAL_CONFIG), []);
});

test('lane status and lane runner: RUNNERS in text and JSON', async () => {
  const { home, state, env } = freshEnv({ LANE_BROKER_TEST_NO_STATUS_RECONCILE: '1' });
  writeGlobalConfig(home, { runners: [SKYBOX], runnerLostAfterMs: 2 * HOUR });
  const now = Date.now();
  seed(state, { skybox: now - 10 * MIN }, beat(now, { skybox: { lastOkAt: now - 12 * MIN, lastPingAt: now } }));
  const text = await laneRun(['status'], { env });
  assert.match(text.stdout, /RUNNERS:\n {2}skybox {2}down \(placement skips; unreachable since \d\d:\d\d\) {2}last-ok 12m ago {2}lost-settle \d\d:\d\d/);
  const json = JSON.parse((await laneRun(['status', '--json'], { env })).stdout);
  assert.equal(json.runners.length, 1);
  assert.equal(json.runners[0].name, 'skybox');
  assert.equal(json.runners[0].state, 'down');
  assert.equal(json.runners[0].unreachableSince, now - 10 * MIN);
  assert.equal(json.runners[0].lostSettleAt, now - 10 * MIN + 2 * HOUR);
  const viaRunner = JSON.parse((await laneRun(['runner', '--json'], { env })).stdout);
  assert.deepEqual(viaRunner.map((r) => [r.name, r.state]), [['skybox', 'down']]);
  assert.match((await laneRun(['runner', 'status'], { env })).stdout, /skybox {2}down/);
  assert.equal((await laneRun(['runner', 'bogus'], { env })).code, 2);
});

test('config: runnerDownAfterMs defaults to 5 min, accepts 0, rejects negatives and non-integers', () => {
  const { home } = freshEnv();
  const load = (patch) => {
    writeGlobalConfig(home, { version: 1, ...patch });
    const prev = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      if (prev === undefined) delete process.env.LANE_BROKER_HOME;
      else process.env.LANE_BROKER_HOME = prev;
    }
  };
  assert.equal(load({}).runnerDownAfterMs, 5 * MIN);
  assert.equal(load({ runnerDownAfterMs: 0 }).runnerDownAfterMs, 0);
  for (const bad of [-1, 1.5, '300000']) assert.throws(() => load({ runnerDownAfterMs: bad }), ConfigError, String(bad));
});
