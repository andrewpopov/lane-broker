import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, resultOf } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { isCancelled, paths } from '../src/state.js';
import { isPidAlive } from '../src/lease.js';

/**
 * BRAIN-850 S3: a supervisor running a REMOTE ticket stops within `lane cancel`'s grace after SIGTERM. The wrapper ssh below
 * sits ahead of the harness's fake ssh and misbehaves on `remote-cancel` per its mode file: `hang-first` hangs only the first
 * call (the supervisor's), `fail` exits 255 on every call, anything else passes through.
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
  fs.writeFileSync(
    cancelFilter,
    `const fs = require('fs');
const { spawnSync } = require('child_process');
fs.appendFileSync(${JSON.stringify(countFile)}, 'x');
const mode = fs.readFileSync(${JSON.stringify(modeFile)}, 'utf8').trim();
const calls = fs.readFileSync(${JSON.stringify(countFile)}, 'utf8').length;
if (mode === 'fail') process.exit(255);
if (mode === 'hang-first' && calls === 1) setTimeout(() => process.exit(255), 40_000);
else process.exit(spawnSync(${JSON.stringify(realSsh)}, process.argv.slice(2), { stdio: 'inherit' }).status ?? 1);
`,
  );
  // exec for everything but remote-cancel, so killing the wrapper kills the real ssh (no orphan holding the pipes open)
  fs.writeFileSync(wrapper, `#!/bin/sh\ncase "$*" in *remote-cancel*) exec ${JSON.stringify(process.execPath)} ${JSON.stringify(cancelFilter)} "$@";; esac\nexec ${JSON.stringify(realSsh)} "$@"\n`);
  fs.chmodSync(wrapper, 0o755);
  return { env: { ...env, PATH: `${dir}${path.delimiter}${env.PATH}` }, setMode: (m) => fs.writeFileSync(modeFile, m) };
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
  const { env, setMode } = withCancelWrapper(base.env);
  const { state, repoDir } = base;
  const { id, ranLog } = await startRemote(env, repoDir);
  try {
    setMode('hang-first');
    const pid = readAttempt(state, id).supervisor.pid;
    const signalledAt = Date.now();
    process.kill(pid, 'SIGTERM');
    await waitFor(() => !isPidAlive(pid), { timeoutMs: 12_000 });
    assert.ok(Date.now() - signalledAt < 9000, 'exits within the cancel wait, not after the 15s remote cancel deadline');
    assert.match(brokerLog(state), ABANDONED_LINE);
    assert.ok(readAttempt(state, id), 'the attempt is retained');
    assert.ok(isCancelled(state, id), 'the cancel marker is retained');
    assert.equal(resultOf(state, id), null, 'no result published for an unconfirmed cancel');
    assert.deepEqual(ran(ranLog), ['remote'], 'nothing re-run locally');
  } finally {
    setMode('');
    await reapOnFailure(state, id, env);
  }
});

test('lane cancel on a supervisor whose cancel hangs completes within its grace via the orphaned-remote path', T, async () => {
  const base = setup();
  const { env, setMode } = withCancelWrapper(base.env);
  const { state, repoDir } = base;
  const { id, ranLog } = await startRemote(env, repoDir);
  try {
    setMode('hang-first');
    const startedAt = Date.now();
    const cancelled = await laneRun(['cancel', id], { env });
    assert.equal(cancelled.code, 0, `stderr: ${cancelled.stderr}`);
    assert.match(cancelled.stdout, /cancelled orphaned remote attempt/);
    assert.ok(Date.now() - startedAt < 9000, 'cancel returned within the grace plus the orphan path');
    assert.equal(readAttempt(state, id), null);
    assert.equal(resultOf(state, id).cancelled, true);
    assert.equal(resultOf(state, id).exit, 130);
    assert.deepEqual(ran(ranLog), ['remote']);
  } finally {
    setMode('');
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
