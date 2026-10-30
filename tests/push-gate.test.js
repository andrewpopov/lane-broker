import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, gitFixture, waitFor, sleep, BIN } from './helpers.js';
import { isPidAlive } from '../src/lease.js';
import { enqueue } from '../src/scheduler.js';
import { paths, ensureStateDirs, readSupersededBy, isCancelled } from '../src/state.js';

const PUSH_REF = 'HEAD:refs/heads/feat';
const SESSION = 'sess-push-gate';
const GATE_SECONDS_APART = 1100; // git start times have one-second resolution

const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');

function setup() {
  const fx = freshEnv({ CLAUDE_CODE_SESSION_ID: SESSION, LANE_BIN: BIN });
  writeGlobalConfig(fx.home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const work = path.join(fx.base, 'work');
  const remote = path.join(fx.base, 'remote.git');
  fs.mkdirSync(work, { recursive: true });
  gitFixture(['init', '--bare', remote], fx.base);
  gitFixture(['init'], work);
  writeRepoConfig(work, { version: 1, lanes: { prepush: { weight: 1 }, default: { weight: 1 } } });
  gitFixture(['add', '.'], work);
  gitFixture(['commit', '-m', 'init'], work);
  gitFixture(['remote', 'add', 'origin', remote], work);
  const hook = path.join(work, '.git', 'hooks', 'pre-push');
  fs.writeFileSync(hook, '#!/bin/sh\ncat >/dev/null\nnode "$LANE_BIN" run --repo r --lane prepush $GATE_ARGS -- sh -c "$GATE_CMD"\nrc=$?\necho $rc > "$GATE_RC"\nexit $rc\n', { mode: 0o755 });
  const env = { ...fx.env };
  for (const name of Object.keys(env)) if (name.startsWith('GIT_')) delete env[name];
  return { ...fx, env, work, history: paths(fx.state).history };
}

/** A real `git push`; resolves with the hook's exit (via GATE_RC) and the push's stderr. */
function push(fx, name, gateCmd, ref = PUSH_REF, gateArgs = '') {
  const rcFile = path.join(fx.base, `${name}.rc`);
  const child = spawn('git', ['push', 'origin', ref], { cwd: fx.work, env: { ...fx.env, GATE_CMD: gateCmd, GATE_ARGS: gateArgs, GATE_RC: rcFile } });
  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });
  const done = new Promise((resolve) => child.on('close', () => resolve({ rc: Number(fs.readFileSync(rcFile, 'utf8')), stderr })));
  const hookRc = async () => {
    await waitFor(() => fs.existsSync(rcFile) && fs.readFileSync(rcFile, 'utf8').trim() !== '', { timeoutMs: 60_000 });
    return Number(fs.readFileSync(rcFile, 'utf8'));
  };
  return { child, done, hookRc };
}

const historyRows = (fx) => (fs.existsSync(fx.history) ? fs.readFileSync(fx.history, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)) : []);
const gateRows = (fx) => historyRows(fx).filter((r) => r.lane === 'prepush');
const queuedRecords = (fx) => fs.readdirSync(paths(fx.state).queue).map((n) => JSON.parse(fs.readFileSync(path.join(paths(fx.state).queue, n), 'utf8')));
const hasLease = (fx) => fs.existsSync(paths(fx.state).leases) && fs.readdirSync(paths(fx.state).leases).some((n) => n.endsWith('.json'));

async function occupyCapacity(fx, seconds) {
  const blocker = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', String(seconds)], { env: fx.env, cwd: fx.work });
  await waitFor(() => hasLease(fx), { timeoutMs: 60_000 });
  return blocker;
}

function expectSupersededRow(fx, row, winnerRow) {
  assert.equal(row.exit, 130);
  assert.equal(row.cancelled, true);
  assert.equal(row.reason, 'superseded');
  assert.equal(row.supersededBy, winnerRow.id);
  assert.equal(row.worktree, fs.realpathSync(fx.work));
  assert.equal(row.branch, 'main');
  assert.equal(row.caller, SESSION);
  assert.equal(row.pushArgsHash, sha256(`origin ${PUSH_REF}`));
}

test('a repeat of the same push supersedes the older queued gate', async () => {
  const fx = setup();
  await occupyCapacity(fx, 4);
  const older = push(fx, 'older', 'true');
  await waitFor(() => fs.existsSync(paths(fx.state).queue) && queuedRecords(fx).some((r) => r.pushGate), { timeoutMs: 60_000 });
  await sleep(GATE_SECONDS_APART);
  const newer = push(fx, 'newer', 'true');
  const [olderDone, newerDone] = await Promise.all([older.done, newer.done]);
  assert.equal(olderDone.rc, 130);
  assert.match(olderDone.stderr, /lane run: superseded by [0-9a-f-]+/);
  assert.equal(newerDone.rc, 0);
  const rows = gateRows(fx);
  const winner = rows.find((r) => r.reason !== 'superseded');
  const loser = rows.find((r) => r.reason === 'superseded');
  assert.equal(rows.length, 2);
  expectSupersededRow(fx, loser, winner);
  assert.match(olderDone.stderr, new RegExp(`superseded by ${winner.id}`));
});

test('a repeat of the same push supersedes the older running gate', async () => {
  const fx = setup();
  const older = push(fx, 'older', 'sleep 60');
  await waitFor(() => hasLease(fx), { timeoutMs: 60_000 });
  await sleep(GATE_SECONDS_APART);
  const newer = push(fx, 'newer', 'true');
  const [olderDone, newerDone] = await Promise.all([older.done, newer.done]);
  assert.equal(olderDone.rc, 130);
  assert.match(olderDone.stderr, /lane run: superseded by [0-9a-f-]+/);
  assert.equal(newerDone.rc, 0);
  const rows = gateRows(fx);
  const winner = rows.find((r) => r.reason !== 'superseded');
  const loser = rows.find((r) => r.reason === 'superseded');
  expectSupersededRow(fx, loser, winner);
  assert.equal(typeof loser.startedAt, 'number', 'the older gate had started running');
});

test('two pushes of the same checkout with different args are not superseded', async () => {
  const fx = setup();
  await occupyCapacity(fx, 3);
  const first = push(fx, 'first', 'true');
  await waitFor(() => fs.existsSync(paths(fx.state).queue) && queuedRecords(fx).some((r) => r.pushGate), { timeoutMs: 60_000 });
  await sleep(GATE_SECONDS_APART);
  const second = push(fx, 'second', 'true', 'HEAD:refs/heads/other');
  const [firstDone, secondDone] = await Promise.all([first.done, second.done]);
  assert.equal(firstDone.rc, 0);
  assert.equal(secondDone.rc, 0);
  const rows = gateRows(fx);
  assert.equal(rows.length, 2);
  assert.equal(new Set(rows.map((r) => r.pushArgsHash)).size, 2);
  assert.ok(rows.every((r) => r.reason !== 'superseded' && r.cancelled !== true));
});

/** Runs `lane run` detached from this process tree (its parent is pid 1), so no real `git push` above the test runner can be found. */
function runOrphaned(fx, name, gateCmd) {
  const rcFile = path.join(fx.base, `${name}.rc`);
  const script = `(sh -c '"$NODE" "$LANE_BIN" run --repo r --lane prepush -- sh -c "$GATE_CMD" 2>/dev/null; echo $? >"$GATE_RC"' &)`;
  spawn('sh', ['-c', script], { cwd: fx.work, env: { ...fx.env, NODE: process.execPath, GATE_CMD: gateCmd, GATE_RC: rcFile } });
  const finished = () => fs.existsSync(rcFile) && fs.readFileSync(rcFile, 'utf8').trim() !== '';
  return waitFor(finished, { timeoutMs: 60_000 }).then(() => Number(fs.readFileSync(rcFile, 'utf8')));
}

test('a hand-run lane run on the prepush lane is never stamped or superseded', async () => {
  const fx = setup();
  await occupyCapacity(fx, 3);
  const first = runOrphaned(fx, 'first', 'exit 0');
  await waitFor(() => fs.existsSync(paths(fx.state).queue) && queuedRecords(fx).length === 1, { timeoutMs: 60_000 });
  await sleep(GATE_SECONDS_APART);
  const second = runOrphaned(fx, 'second', 'exit 0');
  await waitFor(() => queuedRecords(fx).length === 2, { timeoutMs: 60_000 });
  assert.ok(queuedRecords(fx).every((r) => r.pushGate === undefined));
  await waitFor(() => gateRows(fx).length === 2, { timeoutMs: 60_000 });
  await Promise.all([first, second]);
  const rows = gateRows(fx);
  assert.ok(rows.every((r) => r.exit === 0 && r.reason !== 'superseded' && r.worktree === undefined && r.caller === undefined));
});

test('git hook run pre-push is not stamped', async () => {
  const fx = setup();
  const rcFile = path.join(fx.base, 'hook.rc');
  const child = spawn('git', ['hook', 'run', 'pre-push'], { cwd: fx.work, env: { ...fx.env, GATE_CMD: 'exit 0', GATE_RC: rcFile } });
  await new Promise((resolve) => child.on('close', resolve));
  assert.equal(Number(fs.readFileSync(rcFile, 'utf8')), 0);
  const [row] = gateRows(fx);
  assert.equal(row.exit, 0);
  assert.equal(row.worktree, undefined);
  assert.equal(row.pushArgsHash, undefined);
});

test('a late-enqueued older push supersedes itself, not the newer one', async () => {
  const { state } = freshEnv();
  ensureStateDirs(state);
  const gate = (gitPid, gitStartEpoch) => ({ gitPid, gitStartEpoch, worktree: '/w', branch: 'main', pushArgsHash: 'h', caller: 'c' });
  const ticket = (id, pushGate) => ({ id, key: 'r:prepush', repoId: 'r', lane: 'prepush', weight: 1, cwd: '/w', cmd: ['true'], pushGate });
  const newer = await enqueue(state, ticket('newer', gate(200, 2_000_000)));
  assert.deepEqual(newer.supersededTargets, []);
  const older = await enqueue(state, ticket('older', gate(100, 1_000_000)));
  assert.deepEqual(older.supersededTargets, [], 'nothing else to stop: the incoming ticket is the one superseded');
  assert.equal(readSupersededBy(state, 'older'), 'newer');
  assert.equal(isCancelled(state, 'older'), true);
  assert.equal(readSupersededBy(state, 'newer'), null);
  assert.equal(isCancelled(state, 'newer'), false);
});

const queuedGate = async (fx) => {
  await waitFor(() => fs.existsSync(paths(fx.state).queue) && queuedRecords(fx).some((r) => r.pushGate), { timeoutMs: 60_000 });
  return queuedRecords(fx).find((r) => r.pushGate);
};
const queueFiles = (fx) => fs.readdirSync(paths(fx.state).queue);

async function expectAbandoned(fx, record, hook) {
  assert.equal(await hook.hookRc(), 75);
  await waitFor(() => gateRows(fx).length === 1, { timeoutMs: 30_000 });
  const [row] = gateRows(fx);
  assert.equal(row.id, record.id);
  assert.equal(row.exit, 75);
  assert.equal(row.reason, 'push-abandoned');
  assert.notEqual(row.cancelled, true);
  assert.deepEqual(queueFiles(fx), []);
  await waitFor(() => !isPidAlive(record.supervisorPid), { timeoutMs: 30_000 });
}

test('killing only the git push removes its queued gate within a cycle', async () => {
  const fx = setup();
  await occupyCapacity(fx, 30);
  const hook = push(fx, 'p', 'true');
  const record = await queuedGate(fx);
  process.kill(record.pushGate.gitPid, 'SIGKILL');
  assert.ok(isPidAlive(record.pushGate.clientPid), 'the hook and its lane client survive the git kill');
  await expectAbandoned(fx, record, hook);
});

test('killing the lane client removes its queued gate', async () => {
  const fx = setup();
  await occupyCapacity(fx, 30);
  const hook = push(fx, 'p', 'true');
  const record = await queuedGate(fx);
  process.kill(record.pushGate.clientPid, 'SIGKILL');
  await waitFor(() => !isPidAlive(record.supervisorPid), { timeoutMs: 30_000 });
  await waitFor(() => gateRows(fx).length === 1, { timeoutMs: 30_000 });
  const [row] = gateRows(fx);
  assert.equal(row.reason, 'push-abandoned');
  assert.equal(row.exit, 75);
  assert.deepEqual(queueFiles(fx), []);
  assert.equal(await hook.hookRc(), 137);
});

test('a dead push leaves a push-abandoned history row and no queue file or orphan process', async () => {
  const fx = setup();
  await occupyCapacity(fx, 30);
  const hook = push(fx, 'p', 'true');
  const record = await queuedGate(fx);
  process.kill(record.pushGate.gitPid, 'SIGKILL');
  await expectAbandoned(fx, record, hook);
  assert.equal(hasLeaseOf(fx, record.id), false);
  assert.equal(isPidAlive(record.pushGate.clientPid), false);
});

const hasLeaseOf = (fx, id) => fs.existsSync(path.join(paths(fx.state).leases, `${id}.json`));

test('a client that timed out is not reaped and lane wait still returns its result', async () => {
  const fx = setup();
  await occupyCapacity(fx, 5);
  const hook = push(fx, 'p', 'true', PUSH_REF, '--timeout 1s');
  const record = await queuedGate(fx);
  assert.equal(await hook.hookRc(), 75);
  await hook.done;
  assert.equal(isPidAlive(record.pushGate.clientPid), false);
  await sleep(1500);
  assert.equal(queueFiles(fx).length, 1, 'a released gate stays queued though its push and client are gone');
  assert.ok(isPidAlive(record.supervisorPid));
  const waited = await laneRun(['wait', record.id], { env: fx.env, cwd: fx.work });
  assert.equal(waited.code, 0);
  assert.equal(gateRows(fx)[0].exit, 0);
});

test('an orphaned superseded gate is cancelled with the superseded result, quietly', async () => {
  const fx = setup();
  const older = push(fx, 'older', 'sleep 60');
  await waitFor(() => hasLease(fx), { timeoutMs: 60_000 });
  const leaseDir = paths(fx.state).leases;
  const lease = JSON.parse(fs.readFileSync(path.join(leaseDir, fs.readdirSync(leaseDir).find((n) => n.endsWith('.json'))), 'utf8'));
  await waitFor(() => JSON.parse(fs.readFileSync(path.join(leaseDir, `${lease.id}.json`), 'utf8')).childPgid, { timeoutMs: 30_000 });
  process.kill(lease.supervisorPid, 'SIGKILL');
  await sleep(GATE_SECONDS_APART);
  const newer = push(fx, 'newer', 'true');
  const newerDone = await newer.done;
  assert.equal(newerDone.rc, 0);
  assert.doesNotMatch(newerDone.stderr, /cancelled orphaned lease/);
  await older.hookRc();
  const loser = historyRows(fx).find((r) => r.id === lease.id);
  const winner = gateRows(fx).find((r) => r.id !== lease.id);
  assert.equal(loser.exit, 130);
  assert.equal(loser.cancelled, true);
  assert.equal(loser.reason, 'superseded');
  assert.equal(loser.supersededBy, winner.id);
  assert.equal(fs.existsSync(path.join(paths(fx.state).superseded, lease.id)), false);
  assert.equal(hasLeaseOf(fx, lease.id), false);
});
