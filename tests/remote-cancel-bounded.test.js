import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, laneSpawn, waitFor, sleep } from './helpers.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { tmpDir, setup, resultOf, holdLaneLocally } from './remote-harness.js';
import { readAttempt, remoteCancelExitDeadlineMs, remoteCancelDeadlineMs, cancelWaitMs, POST_ABORT_JOIN_MS, CANCELLING_LOCK_TIMEOUT_MS } from '../src/attempts.js';
import { listQueue } from '../src/scheduler.js';
import { isCancelled, paths } from '../src/state.js';
import { isPidAlive } from '../src/lease.js';

/**
 * BRAIN-850 S3: a supervisor running a REMOTE ticket stops within `lane cancel`'s grace after SIGTERM. The wrapper ssh below
 * sits ahead of the harness's fake ssh and misbehaves on `remote-cancel` per its mode file: `hang-first` hangs only the first
 * call (the supervisor's) with a grandchild holding its pipes open, so even the deadline kill of the call never yields its `close`,
 * `slow-first` answers the first call (genuinely, via the real ssh) only after 2 s, `fail` exits 255 on every call, anything else
 * passes through. A `down` file makes every ssh call fail (a runner that is unreachable); a `probe-patch` file gives the runner room.
 *
 * Deadlines under the harness's 1 s cancel grace: remote cancel 6 s, supervisor exit deadline 11.5 s, `lane cancel` wait 13.5 s.
 */

const T = { timeout: 90_000 };
const ABANDONED_LINE = /cancel did not finish within \d+ms; exiting 74/;

function withCancelWrapper(env) {
  const dir = tmpDir('cancel-wrapper');
  const modeFile = path.join(dir, 'mode');
  const countFile = path.join(dir, 'cancel-calls');
  const realSsh = path.join(env.PATH.split(path.delimiter)[0], 'ssh');
  fs.writeFileSync(modeFile, '');
  fs.writeFileSync(countFile, '');
  const wrapper = path.join(dir, 'ssh');
  const cancelFilter = path.join(dir, 'cancel-filter.js');
  const holderPidFile = path.join(dir, 'holder.pid');
  fs.writeFileSync(
    cancelFilter,
    `const fs = require('fs');
const { spawn, spawnSync } = require('child_process');
const real = ${JSON.stringify(realSsh)};
const argv = process.argv.slice(2);
const passThrough = () => process.exit(spawnSync(real, argv, { stdio: 'inherit' }).status ?? 1);
if (argv.join(' ').includes('remote-probe')) {
  if (!fs.existsSync(${JSON.stringify(path.join(dir, 'probe-patch'))})) passThrough();
  const res = spawnSync(real, argv, { encoding: 'utf8' });
  const probe = JSON.parse(res.stdout.trim());
  // the fake runner shares the client's broker state, so it would report the client's own tickets as its own
  process.stdout.write(JSON.stringify({ ...probe, queued: 0, running: 0, headroom: { cpuCores: 64, memoryBytes: 1e12 } }) + '\\n');
  process.exit(0);
}
fs.appendFileSync(${JSON.stringify(countFile)}, 'x');
const mode = fs.readFileSync(${JSON.stringify(modeFile)}, 'utf8').trim();
const calls = fs.readFileSync(${JSON.stringify(countFile)}, 'utf8').length;
if (mode === 'fail') process.exit(255);
if (mode === 'hang-first' && calls === 1) {
  fs.writeFileSync(${JSON.stringify(holderPidFile)}, String(spawn('sleep', ['45'], { stdio: 'inherit' }).pid));
  setTimeout(() => process.exit(255), 45_000);
} else if (mode === 'slow-first' && calls === 1) setTimeout(passThrough, 2000);
else passThrough();
`,
  );
  // exec for everything but remote-cancel/-probe, so killing the wrapper kills the real ssh (no orphan holding the pipes open)
  fs.writeFileSync(wrapper, `#!/bin/sh\n[ -f ${JSON.stringify(path.join(dir, 'down'))} ] && exit 255\ncase "$*" in *remote-cancel*|*remote-probe*) exec ${JSON.stringify(process.execPath)} ${JSON.stringify(cancelFilter)} "$@";; esac\nexec ${JSON.stringify(realSsh)} "$@"\n`);
  fs.chmodSync(wrapper, 0o755);
  const releaseHolder = () => {
    try {
      process.kill(Number(fs.readFileSync(holderPidFile, 'utf8')), 'SIGKILL');
    } catch {
      // never started or gone
    }
  };
  return { env: { ...env, PATH: `${dir}${path.delimiter}${env.PATH}` }, setMode: (m) => fs.writeFileSync(modeFile, m), setDown: (down) => (down ? fs.writeFileSync(path.join(dir, 'down'), '') : fs.rmSync(path.join(dir, 'down'), { force: true })), patchProbe: () => fs.writeFileSync(path.join(dir, 'probe-patch'), ''), releaseHolder };
}

/** A detached remote ticket that logs each execution to `ranLog` and then sleeps; resolves once the remote command is running. */
async function startRemote(env, repoDir) {
  const ranLog = path.join(tmpDir('ran'), 'ran.log');
  const body = `require('fs').appendFileSync(${JSON.stringify(ranLog)}, (process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local') + '\\n'); setInterval(() => {}, 1000);`;
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'node', '-e', body], { env, cwd: repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  await waitFor(() => fs.existsSync(ranLog), { timeoutMs: 30_000 });
  return { id, ranLog };
}

const ran = (ranLog) => fs.readFileSync(ranLog, 'utf8').split('\n').filter(Boolean);
const brokerLog = (state) => (fs.existsSync(paths(state).admissionLog) ? fs.readFileSync(paths(state).admissionLog, 'utf8') : '');

async function reapOnFailure(state, id, env) {
  const attempt = readAttempt(state, id);
  if (attempt && isPidAlive(attempt.supervisor.pid)) process.kill(attempt.supervisor.pid, 'SIGKILL');
  await laneRun(['cancel', id], { env });
}

test('a remote supervisor whose cancel never finishes exits within the deadline and leaves its attempt (no rerun)', T, async () => {
  const base = setup();
  const { env, setMode, releaseHolder } = withCancelWrapper(base.env);
  const { state, repoDir } = base;
  const { id, ranLog } = await startRemote(env, repoDir);
  try {
    setMode('hang-first');
    const pid = readAttempt(state, id).supervisor.pid;
    const signalledAt = Date.now();
    // A supervisor gone before the cancel is a different failure from a missed deadline: name it with the evidence.
    assert.ok(isPidAlive(pid), `the supervisor exited before it was cancelled: attempt ${JSON.stringify(readAttempt(state, id))}, result ${JSON.stringify(resultOf(state, id))}, broker log tail ${brokerLog(state).slice(-2000)}`);
    process.kill(pid, 'SIGTERM');
    await waitFor(() => !isPidAlive(pid), { timeoutMs: 25_000 });
    assert.ok(Date.now() - signalledAt < cancelWaitMs(), 'exits within the cancel wait, though the remote cancel never yields its close');
    assert.match(brokerLog(state), ABANDONED_LINE);
    assert.ok(readAttempt(state, id), 'the attempt is retained');
    assert.ok(isCancelled(state, id), 'the cancel marker is retained');
    assert.equal(resultOf(state, id), null, 'no result published for an unconfirmed cancel');
    assert.deepEqual(ran(ranLog), ['remote'], 'nothing re-run locally');
  } finally {
    setMode('');
    releaseHolder();
    await reapOnFailure(state, id, env);
  }
});

test('lane cancel on a supervisor whose cancel hangs completes within its grace via the orphaned-remote path', T, async () => {
  const base = setup();
  const { env, setMode, releaseHolder } = withCancelWrapper(base.env);
  const { state, repoDir } = base;
  const { id, ranLog } = await startRemote(env, repoDir);
  try {
    setMode('hang-first');
    const startedAt = Date.now();
    const cancelled = await laneRun(['cancel', id], { env });
    assert.equal(cancelled.code, 0, `stderr: ${cancelled.stderr}`);
    assert.match(cancelled.stdout, /cancelled orphaned remote attempt/);
    assert.ok(Date.now() - startedAt < cancelWaitMs() + 5000, 'cancel returned within its wait plus the orphan path');
    assert.equal(readAttempt(state, id), null);
    assert.equal(resultOf(state, id).cancelled, true);
    assert.equal(resultOf(state, id).exit, 130);
    assert.deepEqual(ran(ranLog), ['remote']);
  } finally {
    setMode('');
    releaseHolder();
    await reapOnFailure(state, id, env);
  }
});

test('a remote cancel the runner confirms quickly still publishes exit 130', T, async () => {
  const base = setup();
  const { env } = withCancelWrapper(base.env);
  const { state, repoDir } = base;
  const { id, ranLog } = await startRemote(env, repoDir);
  try {
    const pid = readAttempt(state, id).supervisor.pid;
    process.kill(pid, 'SIGTERM');
    await waitFor(() => !isPidAlive(pid), { timeoutMs: 12_000 });
    assert.equal(readAttempt(state, id), null);
    assert.equal(resultOf(state, id).cancelled, true);
    assert.equal(resultOf(state, id).exit, 130);
    const waited = await laneRun(['wait', id, '--timeout', '15s'], { env });
    assert.equal(waited.code, 130, waited.stderr);
    assert.doesNotMatch(brokerLog(state), ABANDONED_LINE);
    assert.deepEqual(ran(ranLog), ['remote']);
  } finally {
    await reapOnFailure(state, id, env);
  }
});

test('an unconfirmed remote cancel (runner unreachable) keeps the attempt and exits without a rerun', T, async () => {
  const base = setup();
  const { env, setMode } = withCancelWrapper(base.env);
  const { state, repoDir } = base;
  const { id, ranLog } = await startRemote(env, repoDir);
  try {
    setMode('fail');
    const pid = readAttempt(state, id).supervisor.pid;
    process.kill(pid, 'SIGTERM');
    await waitFor(() => !isPidAlive(pid), { timeoutMs: 12_000 });
    await sleep(200);
    const attempt = readAttempt(state, id);
    assert.ok(attempt, 'the attempt is retained');
    assert.match(attempt.unresolved, /remote cancel on skybox was not confirmed/);
    assert.equal(resultOf(state, id), null);
    assert.deepEqual(ran(ranLog), ['remote']);
  } finally {
    setMode('');
    await reapOnFailure(state, id, env);
  }
});

test('a remote cancel the runner confirms slowly (inside its own deadline) publishes 130, not an abandoned exit 74', T, async () => {
  const base = setup();
  // A 5 s grace (remote cancel 10 s, exit deadline 15.5 s): the 2 s answer plus a real fake-ssh round trip on a loaded
  // runner outran the 1 s grace's 6 s deadline. "Slowly, inside the deadline" is the case; how tight is not.
  const { env, setMode } = withCancelWrapper({ ...base.env, LANE_BROKER_TEST_CANCEL_GRACE_MS: '5000' });
  const { state, repoDir } = base;
  const { id, ranLog } = await startRemote(env, repoDir);
  try {
    setMode('slow-first');
    const pid = readAttempt(state, id).supervisor.pid;
    process.kill(pid, 'SIGTERM');
    await waitFor(() => !isPidAlive(pid), { timeoutMs: 25_000 });
    assert.doesNotMatch(brokerLog(state), ABANDONED_LINE);
    assert.equal(readAttempt(state, id), null);
    assert.equal(resultOf(state, id).cancelled, true);
    assert.equal(resultOf(state, id).exit, 130);
    assert.deepEqual(ran(ranLog), ['remote']);
  } finally {
    setMode('');
    await reapOnFailure(state, id, env);
  }
});

test('the derived deadlines nest: remote cancel < supervisor exit deadline < lane cancel wait', () => {
  assert.ok(remoteCancelExitDeadlineMs() > remoteCancelDeadlineMs() + POST_ABORT_JOIN_MS + CANCELLING_LOCK_TIMEOUT_MS, 'the exit deadline outlasts every bounded step of the cancel it waits for');
  assert.ok(cancelWaitMs() > remoteCancelExitDeadlineMs(), 'lane cancel outwaits the supervisor deadline');
});

test('a ticket rebound from the local queue to a runner exits within the deadline when its cancel hangs, and lane cancel settles the orphan', T, async () => {
  const base = setup();
  const { env, setMode, setDown, patchProbe, releaseHolder } = withCancelWrapper(base.env);
  const { home, state, repoDir } = base;
  const configPath = path.join(home, 'config.json');
  fs.writeFileSync(configPath, JSON.stringify({ ...JSON.parse(fs.readFileSync(configPath, 'utf8')), remoteRebindIntervalMs: 300 }));
  setDown(true);
  const ctx = { env, state, repoDir };
  const hold = await holdLaneLocally(ctx);
  const ranLog = path.join(tmpDir('ran'), 'ran.log');
  const body = `require('fs').appendFileSync(${JSON.stringify(ranLog)}, (process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local') + '\\n'); setInterval(() => {}, 1000);`;
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'node', '-e', body], { env, cwd: repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  try {
    await waitFor(() => listQueue(state).some((t) => t.id === id), { timeoutMs: 30_000 });
    setDown(false);
    patchProbe();
    await waitFor(() => fs.existsSync(ranLog), { timeoutMs: 60_000 });
    assert.deepEqual(ran(ranLog), ['remote'], 'the rebound ticket runs on the runner');

    setMode('hang-first');
    const pid = readAttempt(state, id).supervisor.pid;
    const signalledAt = Date.now();
    process.kill(pid, 'SIGTERM');
    await waitFor(() => !isPidAlive(pid), { timeoutMs: 25_000 });
    assert.ok(Date.now() - signalledAt < cancelWaitMs(), 'a rebound dispatch exits within the cancel wait too');
    assert.match(brokerLog(state), ABANDONED_LINE);
    assert.ok(readAttempt(state, id), 'the attempt is retained');

    const cancelled = await laneRun(['cancel', id], { env });
    assert.equal(cancelled.code, 0, `stderr: ${cancelled.stderr}`);
    assert.match(cancelled.stdout, /cancelled orphaned remote attempt/);
    assert.equal(resultOf(state, id).exit, 130);
    assert.deepEqual(ran(ranLog), ['remote']);
  } finally {
    setMode('');
    releaseHolder();
    hold.release();
    await hold.done;
    await reapOnFailure(state, id, env);
  }
});

async function foregroundRemoteRun(load, { env, repoDir }) {
  const ranLog = path.join(tmpDir('ran'), 'ran.log');
  const body = `require('fs').appendFileSync(${JSON.stringify(ranLog)}, 'x'); setInterval(() => {}, 1000);`;
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'node', '-e', body], { env, cwd: repoDir, bin: load.laneBin });
  const exited = new Promise((resolve) => child.on('close', (code) => resolve(code)));
  child.stdout.resume();
  child.stderr.resume();
  await waitFor(() => fs.existsSync(ranLog), { timeoutMs: 30_000 });
  return { child, exited };
}

const settleExit = (exited) => Promise.race([exited, sleep(60_000).then(() => 'timeout')]);

async function cleanup(state, env, child) {
  child.kill('SIGKILL');
  for (const file of fs.existsSync(paths(state).attempts) ? fs.readdirSync(paths(state).attempts) : []) await reapOnFailure(state, file.replace('.json', ''), env);
}

async function watchdogExit(load) {
  const base = setup();
  const { env, setMode, releaseHolder } = withCancelWrapper(base.env);
  const { child, exited } = await foregroundRemoteRun(load, { env, repoDir: base.repoDir });
  try {
    setMode('hang-first');
    child.kill('SIGINT');
    const code = await settleExit(exited);
    assert.match(brokerLog(base.state), ABANDONED_LINE, 'the supervisor gave up on the cancel (exit 74, no result)');
    assert.equal(code, 1, 'an abandoned cancel is not a proven one: never 130');
  } finally {
    setMode('');
    releaseHolder();
    await cleanup(base.state, env, child);
  }
}

async function killedSupervisor(load) {
  const base = setup();
  const { env } = withCancelWrapper(base.env);
  const { child, exited } = await foregroundRemoteRun(load, { env, repoDir: base.repoDir });
  try {
    const [file] = fs.readdirSync(paths(base.state).attempts);
    process.kill(readAttempt(base.state, file.replace('.json', '')).supervisor.pid, 'SIGKILL');
    child.kill('SIGINT');
    assert.equal(await settleExit(exited), 1, 'a supervisor killed by a signal leaves no proof of a cancel, even though the caller was interrupted');
  } finally {
    await cleanup(base.state, env, child);
  }
}

async function preStartCancel(load) {
  const base = setup();
  fs.writeFileSync(path.join(base.state, 'PAUSE'), 'the local queue is closed for the test');
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'needs ssh', '--', 'true'], { env: base.env, cwd: base.repoDir, bin: load.laneBin });
  const exited = new Promise((resolve) => child.on('close', (code) => resolve(code)));
  child.stdout.resume();
  child.stderr.resume();
  try {
    await waitFor(() => listQueue(base.state).length === 1, { timeoutMs: 30_000 });
    child.kill('SIGINT');
    assert.equal(await settleExit(exited), 130, 'a ticket cancelled while still queued, which the supervisor ends cleanly with no result, is the one real cancel-before-start');
  } finally {
    child.kill('SIGKILL');
  }
}

const EXIT_RULE = ['(ended.code === 0 || ended.code === 130)', 'true'];
const SIGNAL_RULE = ['(ended.code === 0 || ended.code === 130)', '(ended.code === null || ended.code === 0 || ended.code === 130)'];

test('a foreground lane run whose cancel the supervisor abandons (watchdog exit 74) exits 1, never 130', T, async () => {
  await watchdogExit(realSrc);
  await assertCanaryFails('run.js', EXIT_RULE, watchdogExit);
});

test('a foreground lane run whose supervisor is killed by a signal exits 1, never 130, whatever the caller did', T, async () => {
  await killedSupervisor(realSrc);
  await assertCanaryFails('run.js', SIGNAL_RULE, killedSupervisor);
});

test('a ticket cancelled while still queued exits 130: the supervisor ended cleanly with no result', T, async () => {
  await preStartCancel(realSrc);
  await assertCanaryFails('run.js', ["if (cancelling && ended !== null && ", 'if (false && ended !== null && '], preStartCancel);
});
