import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, gitFixture, waitFor, sleep, BIN } from './helpers.js';
import { fenceLegacyQueue } from '../src/migrate.js';
import { isPidAlive } from '../src/lease.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { stopSupersededTicket } from '../src/supervisor.js';
import { releaseUnlessSettled } from '../src/run.js';
import { reapOrphanedQueue } from '../src/scheduler.js';
import { waitLabel } from '../src/ticket-wait.js';
import { processStartTime } from '../src/lease.js';
import { pushHoldBehind } from '../src/push-gate.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeCancelMarkerFile, writeSupersededMarkerFile, isReleased, bootId, atomicWriteJson } from '../src/state.js';
import { paths, ensureStateDirs, readSupersededBy, isCancelled } from '../src/state.js';

const PUSH_REF = 'HEAD:refs/heads/feat';
const SESSION = 'sess-push-gate';
const GATE_SECONDS_APART = 1100; // git start times have one-second resolution

const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');

function addWork(fx, name) {
  const work = path.join(fx.base, name);
  const remote = path.join(fx.base, `${name}-remote.git`);
  fs.mkdirSync(work, { recursive: true });
  gitFixture(['init', '--bare', remote], fx.base);
  gitFixture(['init'], work);
  writeRepoConfig(work, { version: 1, lanes: { prepush: { weight: 1 }, default: { weight: 1 }, other: { weight: 1 } } });
  gitFixture(['add', '.'], work);
  gitFixture(['commit', '-m', 'init'], work);
  gitFixture(['remote', 'add', 'origin', remote], work);
  const hook = path.join(work, '.git', 'hooks', 'pre-push');
  fs.writeFileSync(hook, `#!/bin/sh\ncat >/dev/null\nnode "$LANE_BIN" run --repo ${name} --lane prepush $GATE_ARGS -- sh -c "$GATE_CMD"\nrc=$?\necho $rc > "$GATE_RC"\nexit $rc\n`, { mode: 0o755 });
  return work;
}

function setup(extraEnv = {}, capacity = 1) {
  const fx = freshEnv({ CLAUDE_CODE_SESSION_ID: SESSION, LANE_BIN: BIN, ...extraEnv });
  writeGlobalConfig(fx.home, { version: 1, capacity, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const work = addWork(fx, 'r');
  const env = { ...fx.env };
  for (const name of Object.keys(env)) if (name.startsWith('GIT_')) delete env[name];
  return { ...fx, env, work, history: paths(fx.state).history };
}

/** A real `git push`; resolves with the hook's exit (via GATE_RC) and the push's stderr. */
function push(fx, name, gateCmd, ref = PUSH_REF, gateArgs = '', work = fx.work, extraEnv = {}) {
  const rcFile = path.join(fx.base, `${name}.rc`);
  const child = spawn('git', ['push', 'origin', ref], { cwd: work, env: { ...fx.env, ...extraEnv, GATE_CMD: gateCmd, GATE_ARGS: gateArgs, GATE_RC: rcFile } });
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

const intervalOf = (row) => [row.startedAt, row.endedAt];

/** Two pushes of one caller to two repos on a roomy broker: the second is enqueued while the first is running. */
async function pushToTwoRepos({ firstCmd = 'sleep 3', extraEnv = {}, secondEnv = {}, unsetCaller = false } = {}) {
  const fx = setup(extraEnv, 4);
  if (unsetCaller) for (const name of ['CLAUDE_CODE_SESSION_ID', 'CODEX_THREAD_ID', 'CODEX_SESSION_ID']) delete fx.env[name];
  const second = addWork(fx, 's');
  const first = push(fx, 'first', firstCmd);
  await waitFor(() => hasLease(fx), { timeoutMs: 60_000 });
  await sleep(GATE_SECONDS_APART);
  const held = push(fx, 'second', 'true', PUSH_REF, '', second, secondEnv);
  const [firstDone, heldDone] = await Promise.all([first.done, held.done]);
  const rows = gateRows(fx);
  return { fx, firstDone, heldDone, firstRow: rows.find((r) => r.worktree === fs.realpathSync(fx.work)), heldRow: rows.find((r) => r.worktree === fs.realpathSync(second)) };
}

let sameCallerRun;
const sameCaller = () => (sameCallerRun ??= pushToTwoRepos());

test('two pushes from one caller to different repos never run concurrently', async () => {
  const { firstDone, heldDone, firstRow, heldRow } = await sameCaller();
  assert.equal(firstDone.rc, 0);
  assert.equal(heldDone.rc, 0);
  const [, firstEnd] = intervalOf(firstRow);
  const [heldStart] = intervalOf(heldRow);
  assert.ok(heldStart >= firstEnd, `the held push started at ${heldStart}, before the first ended at ${firstEnd}`);
});

test('a held push prints that it waits behind its own other push and records pushHeldMs', async () => {
  const { heldDone, firstRow, heldRow } = await sameCaller();
  assert.match(heldDone.stderr, new RegExp(`lane run: waiting behind your other push ${firstRow.id} \\(${firstRow.repo}\\)`));
  assert.equal(heldDone.stderr.match(/waiting behind your other push/g).length, 1, 'printed once');
  assert.equal(heldRow.heldBehind, firstRow.id);
  assert.ok(heldRow.pushHeldMs > 0);
  assert.equal(firstRow.heldBehind, undefined);
});

test('pushes from different callers are not held', async () => {
  const { firstDone, heldDone, firstRow, heldRow } = await pushToTwoRepos({ secondEnv: { CLAUDE_CODE_SESSION_ID: 'another-session' } });
  assert.equal(firstDone.rc, 0);
  assert.equal(heldDone.rc, 0);
  assert.equal(heldRow.heldBehind, undefined);
  assert.ok(heldRow.startedAt < firstRow.endedAt, 'the second push ran beside the first');
});

test('without a session id each worktree is its own caller, so pushes from two worktrees are not held', async () => {
  const { heldRow, firstRow } = await pushToTwoRepos({ unsetCaller: true });
  assert.match(firstRow.caller, /^worktree:/);
  assert.notEqual(firstRow.caller, heldRow.caller);
  assert.equal(heldRow.heldBehind, undefined);
  assert.ok(heldRow.startedAt < firstRow.endedAt);
});

test('a held ticket blocks no other ticket', async () => {
  const fx = setup({}, 2);
  const second = addWork(fx, 's');
  const first = push(fx, 'first', 'sleep 6');
  await waitFor(() => hasLease(fx), { timeoutMs: 60_000 });
  await sleep(GATE_SECONDS_APART);
  const held = push(fx, 'second', 'true', PUSH_REF, '', second);
  await waitFor(() => fs.existsSync(paths(fx.state).queue) && queuedRecords(fx).some((r) => r.pushGate), { timeoutMs: 60_000 });
  const unrelated = await laneRun(['run', '--repo', 'r', '--lane', 'other', '--', 'true'], { env: fx.env, cwd: fx.work });
  assert.equal(unrelated.code, 0);
  assert.equal(queuedRecords(fx).filter((r) => r.pushGate).length, 1, 'the held push is still queued');
  await Promise.all([first.done, held.done]);
});

test('killing the holder lifts the hold', async () => {
  const fx = setup({}, 4);
  const second = addWork(fx, 's');
  const first = push(fx, 'first', 'sleep 60');
  await waitFor(() => hasLease(fx), { timeoutMs: 60_000 });
  const leaseDir = paths(fx.state).leases;
  const leaseFile = path.join(leaseDir, fs.readdirSync(leaseDir).find((n) => n.endsWith('.json')));
  await waitFor(() => JSON.parse(fs.readFileSync(leaseFile, 'utf8')).childPgid, { timeoutMs: 30_000 });
  const lease = JSON.parse(fs.readFileSync(leaseFile, 'utf8'));
  await sleep(GATE_SECONDS_APART);
  const held = push(fx, 'second', 'true', PUSH_REF, '', second);
  await waitFor(() => fs.existsSync(paths(fx.state).queue) && queuedRecords(fx).some((r) => r.pushGate), { timeoutMs: 60_000 });
  const heldId = queuedRecords(fx).find((r) => r.pushGate).id;
  const waitFile = path.join(paths(fx.state).waits, `${heldId}.json`);
  await waitFor(() => fs.existsSync(waitFile) && JSON.parse(fs.readFileSync(waitFile, 'utf8')).reason === 'push-hold', { timeoutMs: 30_000 });
  process.kill(lease.supervisorPid, 'SIGKILL');
  try {
    assert.equal((await held.done).rc, 0);
  } finally {
    process.kill(-lease.childPgid, 'SIGKILL');
  }
  assert.ok(gateRows(fx).find((r) => r.worktree === fs.realpathSync(second)).pushHeldMs > 0);
  await first.hookRc();
});

const gate = (gitPid, gitStartEpoch, caller = 'c') => ({ gitPid, gitStartEpoch, worktree: `/w${gitPid}`, branch: 'main', pushArgsHash: 'h', caller });
const stamped = (id, pushGate, extra = {}) => ({ id, key: `k:${id}`, repoId: `repo${id}`, lane: 'prepush', weight: 1, cwd: '/w', cmd: ['true'], pushGate, ...extra });
const runningLease = (id, pushGate, state = LEASE_STATE.RUNNING) => ({ ...stamped(id, pushGate), bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), state });

test('a younger running push holds an older late enqueue', () => {
  const { state } = freshEnv();
  const older = stamped('older', gate(100, 1_000_000));
  const younger = runningLease('younger', gate(200, 2_000_000));
  assert.equal(pushHoldBehind(state, older, [older], [younger]), 'younger');
  assert.equal(pushHoldBehind(state, older, [older], [{ ...younger, state: LEASE_STATE.ORPHANED }]), null, 'an orphaned lease holds nothing');
  assert.equal(pushHoldBehind(state, older, [older], [runningLease('sibling', gate(100, 1_000_000))]), null, 'the same push is a sibling');
  assert.equal(pushHoldBehind(state, older, [older], [runningLease('other', gate(300, 3_000_000, 'someone else'))]), null);
  writeCancelMarkerFile(state, 'younger');
  assert.equal(pushHoldBehind(state, older, [older], [younger]), null, 'a cancelled ticket holds nothing');
});

test('among queued pushes the earlier holds and the earliest is never held', () => {
  const { state } = freshEnv();
  const a = stamped('a', gate(100, 1_000_000));
  const b = stamped('b', gate(200, 2_000_000));
  assert.equal(pushHoldBehind(state, b, [a, b], []), 'a');
  assert.equal(pushHoldBehind(state, a, [a, b], []), null);
});

test('a held ticket is never reservation owner', async () => {
  const { state } = freshEnv();
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'active', capacity: 10, maxLaneWeight: 5, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, cpuAdmissionPercent: 100, cpuReserveCores: 1, admissionCooldownMs: 0, resourceSkipLimit: 3 };
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: Date.now() });
  fenceLegacyQueue(state, 'test');
  const rec = (seq) => ({ reason: 'resource', skipsCharged: 3, reserved: true, reservationSeq: seq, inScope: true, behindConflict: false, deniedAt: Date.now(), budget: 9, externalBusy: 0 });
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: { held: { resource: rec(10) }, owner: { resource: rec(12) } } });
  writeLease(state, runningLease('running', gate(1, 1_000)));
  await enqueue(state, stamped('held', gate(2, 2_000)));
  await enqueue(state, stamped('plain', undefined));
  const owner = stamped('owner', undefined);
  await enqueue(state, owner);
  const sampler = () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() });
  const memory = () => ({ availableBytes: 64 * 1024 ** 3, totalBytes: 64 * 1024 ** 3, macPressure: 'normal', source: 'test' });
  const result = await tryStart(state, owner, cfg, undefined, sampler, undefined, memory);
  assert.equal(result.started, true, 'the next dormant owner is promoted past the held ticket and heads the queue');
});

test('a superseded ticket is signalled only when its recorded supervisor identity still matches', async () => {
  const child = spawn('sleep', ['30']);
  try {
    await stopSupersededTicket({ id: 'x', supervisorPid: child.pid, supervisorStart: 'Mon Jan  1 00:00:00 2001' });
    await sleep(200);
    assert.equal(child.signalCode, null, 'a reused pid (different start time) is not signalled');
    const exited = new Promise((resolve) => child.on('exit', resolve));
    await stopSupersededTicket({ id: 'x', supervisorPid: child.pid, supervisorStart: processStartTime(child.pid) });
    await exited;
  } finally {
    child.kill('SIGKILL');
  }
});

test('a push already cancelled or superseded cannot supersede a live older push', async () => {
  const { state } = freshEnv();
  ensureStateDirs(state);
  const ticket = (id, pushGate) => ({ id, key: 'r:prepush', repoId: 'r', lane: 'prepush', weight: 1, cwd: '/w', cmd: ['true'], pushGate });
  const sameCheckout = (gitPid, gitStartEpoch) => ({ gitPid, gitStartEpoch, worktree: '/w', branch: 'main', pushArgsHash: 'h', caller: 'c' });
  await enqueue(state, ticket('dying', sameCheckout(200, 2_000_000)));
  writeCancelMarkerFile(state, 'dying');
  const live = await enqueue(state, ticket('live', sameCheckout(100, 1_000_000)));
  assert.deepEqual(live.supersededTargets, []);
  assert.equal(readSupersededBy(state, 'live'), null);
  assert.equal(isCancelled(state, 'live'), false);
});

test('a queued superseded ticket whose supervisor died publishes the superseded result and clears its markers', async () => {
  const { state, base } = freshEnv();
  ensureStateDirs(state);
  const dead = spawn('true');
  await new Promise((resolve) => dead.on('exit', resolve));
  const resultPath = path.join(base, 'result.json');
  await enqueue(state, { ...stamped('gone', gate(100, 1_000_000)), supervisorPid: dead.pid, supervisorStart: null, resultPath });
  writeSupersededMarkerFile(state, 'gone', 'winner');
  reapOrphanedQueue(state, 'someone-else');
  const result = JSON.parse(fs.readFileSync(resultPath, 'utf8'));
  assert.deepEqual([result.exit, result.cancelled, result.reason, result.supersededBy], [130, true, 'superseded', 'winner']);
  const row = fs.readFileSync(paths(state).history, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)).find((r) => r.id === 'gone');
  assert.deepEqual([row.exit, row.cancelled, row.reason, row.supersededBy], [130, true, 'superseded', 'winner']);
  assert.equal(readSupersededBy(state, 'gone'), null);
  assert.deepEqual(fs.readdirSync(paths(state).queue), []);
});

test('a timed-out client releases its gate only when no result has landed', async () => {
  const { state, base } = freshEnv();
  ensureStateDirs(state);
  const resultPath = path.join(base, 'result.json');
  assert.equal(await releaseUnlessSettled(state, 'open', resultPath), false);
  assert.equal(isReleased(state, 'open'), true);
  fs.writeFileSync(resultPath, JSON.stringify({ id: 'done', exit: 0 }));
  assert.equal(await releaseUnlessSettled(state, 'done', resultPath), true);
  assert.equal(isReleased(state, 'done'), false);
});

test('lane status names the other push a held ticket waits behind', () => {
  assert.equal(waitLabel('push-hold', { behind: 'abc-123' }), 'your other push (abc-123)');
});
