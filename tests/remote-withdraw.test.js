import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, laneRun, sleep, waitFor, BIN } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { listLeases } from '../src/lease.js';
import { withdrawLane, remoteTicketState } from '../src/remote-runner.js';
import { encodeSnapshot } from '../src/remote-stream.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths, readJsonSafe, isWithdrawn, writeWithdrawMarkerFile, writeCancelMarkerFile, processStartTime, atomicWriteJson } from '../src/state.js';
import { CAPABILITIES } from '../src/capabilities.js';

/**
 * BRAIN-436 S1: `remote-withdraw`, the irreversible take-back of a ticket still QUEUED on a runner. The marker written
 * under the admission lock is the commit; `tryStart` refuses a marked ticket whether or not its queue file survives.
 */

const cfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 4, schedulerMode: 'shadow' };
const ticketOf = (id) => ({ id, key: `r:${id}`, weight: 1, resources: {}, conflicts: [], supervisorPid: process.pid, supervisorStart: null });
const queuedIds = (state) => listQueue(state).map((t) => t.id);

test('remote-withdraw/1 is advertised', () => {
  assert.ok(CAPABILITIES.includes('remote-withdraw/1'));
});

// ---- the decision, under the admission lock ----

test('withdraw: a queued ticket is withdrawn, leaves the queue, and is never admitted afterwards', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  assert.equal(await withdrawLane(state, 'a'), 'withdrawn');
  assert.ok(isWithdrawn(state, 'a'));
  assert.deepEqual(queuedIds(state), []);
  const started = await tryStart(state, ticketOf('a'), cfg);
  assert.equal(started.started, false);
  assert.equal(listLeases(state).length, 0);
});

test('withdraw: unknown, started and cancelled tickets are refused and leave no marker', async () => {
  const { state } = freshEnv();
  assert.equal(await withdrawLane(state, 'ghost'), 'not-queued');
  await enqueue(state, ticketOf('run'));
  assert.equal((await tryStart(state, ticketOf('run'), cfg)).started, true);
  assert.equal(await withdrawLane(state, 'run'), 'started');
  await enqueue(state, ticketOf('c'));
  writeCancelMarkerFile(state, 'c');
  assert.equal(await withdrawLane(state, 'c'), 'cancelled');
  for (const id of ['ghost', 'run', 'c']) assert.equal(isWithdrawn(state, id), false, id);
  assert.deepEqual(queuedIds(state), ['c'], 'a cancelled ticket is left for its own cancel path');
});

test('withdraw is idempotent: a retry answers withdrawn again', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  assert.equal(await withdrawLane(state, 'a'), 'withdrawn');
  assert.equal(await withdrawLane(state, 'a'), 'withdrawn');
});

test('Race (a): withdraw racing admission of the same ticket -- a lease XOR withdrawn, over 25 rounds', async () => {
  for (let round = 0; round < 25; round += 1) {
    const { state } = freshEnv();
    await enqueue(state, ticketOf('a'));
    const [started, action] = await Promise.all([tryStart(state, ticketOf('a'), cfg), withdrawLane(state, 'a')]);
    const startedWon = started.started === true;
    const withdrawnWon = action === 'withdrawn';
    assert.notEqual(startedWon, withdrawnWon, `round ${round}: started=${startedWon} action=${action}`);
    assert.equal(listLeases(state).length, startedWon ? 1 : 0, `round ${round}: a lease exists iff admission won`);
    assert.equal(isWithdrawn(state, 'a'), withdrawnWon, `round ${round}: the marker exists iff the withdraw won`);
    assert.deepEqual(queuedIds(state), [], `round ${round}: nobody leaves it queued`);
  }
});

test('Race (b): admission attempted while the withdraw holds the lock stays blocked, then finds the ticket withdrawn, with no lease', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  const dir = makeTmpDir('withdraw-hold-');
  const ready = path.join(dir, 'ready');
  const go = path.join(dir, 'go');
  const saved = { ...process.env };
  Object.assign(process.env, { LANE_BROKER_TEST_HOLD_AT: 'remote-withdraw-checked', LANE_BROKER_TEST_HOLD_READY: ready, LANE_BROKER_TEST_HOLD_GO: go });
  try {
    const withdrawing = withdrawLane(state, 'a');
    await waitFor(() => fs.existsSync(ready));
    delete process.env.LANE_BROKER_TEST_HOLD_AT;
    let settled = false;
    const starting = tryStart(state, ticketOf('a'), cfg).finally(() => {
      settled = true;
    });
    await sleep(300);
    assert.equal(settled, false, 'admission is blocked behind the lock the withdraw holds');
    assert.equal(listLeases(state).length, 0);
    fs.writeFileSync(go, '1');
    assert.equal(await withdrawing, 'withdrawn');
    const started = await starting;
    assert.equal(started.started, false);
    assert.equal(listLeases(state).length, 0, 'never admitted');
  } finally {
    for (const k of ['LANE_BROKER_TEST_HOLD_AT', 'LANE_BROKER_TEST_HOLD_READY', 'LANE_BROKER_TEST_HOLD_GO']) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  }
});

test('R1: two concurrent withdraws with the dequeue failing both answer withdrawn, and the ticket is never admitted', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  const failing = () => {
    throw Object.assign(new Error('EPERM'), { code: 'EPERM' });
  };
  const actions = await Promise.all([withdrawLane(state, 'a', { dequeue: failing }), withdrawLane(state, 'a', { dequeue: failing })]);
  assert.deepEqual(actions, ['withdrawn', 'withdrawn']);
  assert.deepEqual(queuedIds(state), ['a'], 'the queue file survived the failed unlink');
  const started = await tryStart(state, ticketOf('a'), cfg);
  assert.equal(started.started, false);
  assert.equal(started.reason, 'withdrawn');
  assert.equal(listLeases(state).length, 0);
  assert.deepEqual(queuedIds(state), ['a'], 'tryStart refuses; it is the supervisor that dequeues');
});

test('tryStart refuses a withdrawn ticket even when its queue file survives', { timeout: 10_000 }, async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  writeWithdrawMarkerFile(state, 'a');
  assert.deepEqual(queuedIds(state), ['a'], 'the queue file is still there');
  const started = await tryStart(state, ticketOf('a'), cfg);
  assert.equal(started.started, false);
  assert.equal(started.reason, 'withdrawn');
  assert.equal(listLeases(state).length, 0);
});

// ---- remoteTicketState phases ----

test('remoteTicketState: preparing, queued and admitted are told apart by remote-id, queue entry and lease', async () => {
  const { state } = freshEnv();
  const ticketDir = makeTmpDir('withdraw-ticket-');
  fs.writeFileSync(path.join(ticketDir, 'publisher.json'), JSON.stringify({ pid: process.pid, start: processStartTime(process.pid) }));
  assert.equal(remoteTicketState(ticketDir, state), 'preparing', 'no remote-id yet');
  const laneId = crypto.randomUUID();
  fs.writeFileSync(path.join(ticketDir, 'remote-id'), laneId);
  assert.equal(remoteTicketState(ticketDir, state), 'preparing', 'remote-id but not enqueued');
  await enqueue(state, ticketOf(laneId));
  assert.equal(remoteTicketState(ticketDir, state), 'queued');
  assert.equal((await tryStart(state, ticketOf(laneId), cfg)).started, true);
  assert.equal(remoteTicketState(ticketDir, state), 'admitted');
});

// ---- end to end through remote-exec and `lane remote-withdraw` ----

function startRemoteExec({ env, root, header }) {
  const src = makeTmpDir('withdraw-src-');
  fs.writeFileSync(path.join(src, 'a.txt'), 'hello');
  const entries = [{ path: 'a.txt', type: 'file', exec: false, size: 5, sha256: crypto.createHash('sha256').update('hello').digest('hex') }];
  const child = spawn(process.execPath, [BIN, 'remote-exec', '--root', root], { env });
  const closedP = new Promise((resolve) => child.on('close', resolve));
  encodeSnapshot(src, header, entries).pipe(child.stdin);
  return { child, closed: closedP };
}

const headerOf = (overrides = {}) => ({
  ticketId: crypto.randomUUID(),
  generation: 0,
  repoKey: 'remote-withdraw-test-repo',
  lane: 'default',
  argv: [process.execPath, '-e', 'process.exit(0)'],
  relCwd: '',
  ...overrides,
});

/** A runner whose broker is paused, so a remote-exec'd ticket sits queued until the test decides otherwise. */
function pausedRunner() {
  const f = freshEnv();
  writeGlobalConfig(f.home, { schedulerMode: 'shadow', sampleMs: 30 });
  fs.mkdirSync(f.state, { recursive: true });
  fs.writeFileSync(path.join(f.state, 'PAUSE'), 'keep queued');
  return { ...f, root: makeTmpDir('withdraw-root-') };
}

async function queuedLaneId({ root, state, header }) {
  const remoteId = path.join(root, 'tickets', header.ticketId, 'remote-id');
  await waitFor(() => fs.existsSync(remoteId) && queuedIds(state).includes(fs.readFileSync(remoteId, 'utf8').trim()), { timeoutMs: 20000 });
  return fs.readFileSync(remoteId, 'utf8').trim();
}

const withdrawCli = async (f, header) => {
  const res = await laneRun(['remote-withdraw', header.ticketId, '--root', f.root], { env: f.env });
  assert.equal(res.code, 0, res.stderr);
  return JSON.parse(res.stdout.trim());
};
const resultOf = (f, header) => readJsonSafe(path.join(f.root, 'tickets', header.ticketId, 'result.json'));

for (const protocol of [1, 2]) {
  test(`remote-withdraw end to end (protocol ${protocol}): a queued ticket is withdrawn, published unfinished/withdrawn, never run${protocol === 2 ? ', with no phase or deps record' : ''}`, { timeout: 60_000 }, async () => {
    const f = pausedRunner();
    const header = headerOf({ ...(protocol === 2 ? { protocol: 2 } : {}) });
    const run = startRemoteExec({ env: f.env, root: f.root, header });
    const laneId = await queuedLaneId({ ...f, header });
    assert.deepEqual(await withdrawCli(f, header), { protocol: 1, ticketId: header.ticketId, action: 'withdrawn' });
    assert.equal(await run.closed, 0);
    const result = resultOf(f, header);
    assert.equal(result.kind, 'unfinished');
    assert.equal(result.reason, 'withdrawn');
    assert.equal(listLeases(f.state).length, 0);
    assert.deepEqual(queuedIds(f.state), []);
    const structured = readJsonSafe(path.join(paths(f.state).results, `${laneId}.json`));
    assert.equal(structured.reason, 'withdrawn');
    assert.equal(structured.exit, 75);
    assert.equal(structured.cancelled, false);
    if (protocol === 2) {
      const names = fs.readdirSync(path.join(f.root, 'tickets', header.ticketId));
      assert.ok(!names.includes('phase') && !names.includes('deps.json') && !names.includes('phases.json'), names.join(','));
    }
    // idempotent after the ticket finished and its markers were cleared
    assert.equal((await withdrawCli(f, header)).action, 'withdrawn');
  });
}

test('remote-withdraw: an unknown ticket id answers no-such-ticket', { timeout: 60_000 }, async () => {
  const f = pausedRunner();
  const header = headerOf();
  // the exec for this id may still be on its way: the answer leaves a tombstone so it loses that race
  assert.deepEqual(await withdrawCli(f, header), { protocol: 1, ticketId: header.ticketId, action: 'no-such-ticket', tombstoned: true });
});

test('Race (c): a ticket with a remote-id but not yet enqueued answers not-queued, and then runs normally', { timeout: 60_000 }, async () => {
  const f = freshEnv();
  writeGlobalConfig(f.home, { schedulerMode: 'shadow', sampleMs: 30 });
  const root = makeTmpDir('withdraw-root-');
  const pauseFile = path.join(makeTmpDir('withdraw-pause-'), 'go');
  const header = headerOf();
  const run = startRemoteExec({ env: { ...f.env, LANE_BROKER_TEST_PAUSE_AFTER_TICKET_ID: pauseFile }, root, header });
  await waitFor(() => fs.existsSync(path.join(root, 'tickets', header.ticketId, 'remote-id')));
  const reply = await withdrawCli({ ...f, root }, header);
  assert.equal(reply.action, 'not-queued');
  assert.equal(isWithdrawn(f.state, fs.readFileSync(path.join(root, 'tickets', header.ticketId, 'remote-id'), 'utf8').trim()), false);
  fs.writeFileSync(pauseFile, '1');
  assert.equal(await run.closed, 0);
  assert.equal(resultOf({ root }, header).kind, 'completed');
});

test('Race (d): a cancel that is already pending wins over a withdraw: published cancelled, not withdrawn', { timeout: 60_000 }, async () => {
  const f = pausedRunner();
  const header = headerOf();
  const run = startRemoteExec({ env: f.env, root: f.root, header });
  const laneId = await queuedLaneId({ ...f, header });
  // both markers land together (the supervisor polls every 30 ms, so any gap between them would race the assertion)
  writeCancelMarkerFile(f.state, laneId);
  writeWithdrawMarkerFile(f.state, laneId);
  assert.equal(await run.closed, 0);
  const structured = readJsonSafe(path.join(paths(f.state).results, `${laneId}.json`));
  // the cancel path (not the withdraw path) finalized it: no withdrawn result anywhere, and it never ran
  assert.notEqual(structured?.reason, 'withdrawn');
  assert.notEqual(resultOf(f, header).reason, 'withdrawn');
  assert.equal(listLeases(f.state).length, 0);
});

test('a crash after the marker but before the dequeue: the runner supervisor dequeues, then publishes withdrawn', { timeout: 60_000 }, async () => {
  const f = pausedRunner();
  const header = headerOf();
  const run = startRemoteExec({ env: f.env, root: f.root, header });
  const laneId = await queuedLaneId({ ...f, header });
  writeWithdrawMarkerFile(f.state, laneId);
  assert.equal(await run.closed, 0);
  assert.deepEqual(queuedIds(f.state), []);
  assert.equal(listLeases(f.state).length, 0);
  assert.equal(resultOf(f, header).reason, 'withdrawn');
  assert.equal(isWithdrawn(f.state, laneId), false, 'finalize cleared the marker');
});

// ---- Codex S1 review fixes ----

/** Make unlinks in the runner's queue directory fail (EACCES), so a supervisor's dequeue has to retry. */
const blockQueue = (state) => fs.chmodSync(paths(state).queue, 0o555);
const unblockQueue = (state) => fs.chmodSync(paths(state).queue, 0o755);

test('a cancel landing while the supervisor retries the dequeue still wins', { timeout: 60_000 }, async () => {
  const f = pausedRunner();
  const header = headerOf();
  const run = startRemoteExec({ env: f.env, root: f.root, header });
  const laneId = await queuedLaneId({ ...f, header });
  blockQueue(f.state);
  try {
    writeWithdrawMarkerFile(f.state, laneId);
    await sleep(500);
    assert.deepEqual(queuedIds(f.state), [laneId], 'the supervisor is stuck retrying the unlink');
    writeCancelMarkerFile(f.state, laneId);
  } finally {
    unblockQueue(f.state);
  }
  assert.equal(await run.closed, 0);
  const structured = readJsonSafe(path.join(paths(f.state).results, `${laneId}.json`));
  assert.notEqual(structured?.reason, 'withdrawn');
  assert.equal(structured?.cancelled, true);
  assert.deepEqual(queuedIds(f.state), []);
});

test('cancel wins over a withdraw while the unlink fails: the queue entry is still removed and a result published', { timeout: 60_000 }, async () => {
  const f = pausedRunner();
  const header = headerOf();
  const run = startRemoteExec({ env: f.env, root: f.root, header });
  const laneId = await queuedLaneId({ ...f, header });
  blockQueue(f.state);
  try {
    writeCancelMarkerFile(f.state, laneId);
    writeWithdrawMarkerFile(f.state, laneId);
    await sleep(500);
    assert.deepEqual(queuedIds(f.state), [laneId], 'still stuck, and the withdraw marker is still holding admission off');
    assert.ok(isWithdrawn(f.state, laneId));
  } finally {
    unblockQueue(f.state);
  }
  assert.equal(await run.closed, 0);
  assert.deepEqual(queuedIds(f.state), []);
  assert.equal(listLeases(f.state).length, 0);
  const structured = readJsonSafe(path.join(paths(f.state).results, `${laneId}.json`));
  assert.equal(structured?.cancelled, true, 'a cancelled result was published');
});

test('a retry between the broker result and the remote result still answers withdrawn', async () => {
  const { state } = freshEnv();
  // the supervisor published the broker result and cleared the marker; remote-exec has not published yet
  fs.mkdirSync(paths(state).results, { recursive: true });
  atomicWriteJson(path.join(paths(state).results, 'a.json'), { id: 'a', exit: 75, reason: 'withdrawn', cancelled: false });
  assert.equal(isWithdrawn(state, 'a'), false);
  assert.equal(await withdrawLane(state, 'a'), 'withdrawn');
  atomicWriteJson(path.join(paths(state).results, 'b.json'), { id: 'b', exit: 130, cancelled: true });
  assert.equal(await withdrawLane(state, 'b'), 'not-queued', 'any other published result is not a withdrawal');
});

test('durability: the idempotent answer fsyncs the marker directory, and creating withdraw/ fsyncs its parent', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  const opened = new Map();
  const synced = [];
  const open = fs.openSync;
  mock.method(fs, 'openSync', (p, ...rest) => {
    const fd = open(p, ...rest);
    opened.set(fd, String(p));
    return fd;
  });
  mock.method(fs, 'fsyncSync', (fd) => {
    synced.push(opened.get(fd));
  });
  try {
    assert.equal(await withdrawLane(state, 'a'), 'withdrawn');
    assert.ok(synced.includes(state), `creating withdraw/ fsyncs the state root; synced: ${synced.join(', ')}`);
    synced.length = 0;
    assert.equal(await withdrawLane(state, 'a'), 'withdrawn');
    assert.deepEqual(synced, [paths(state).withdraw], 'the idempotent path fsyncs the marker directory before answering');
  } finally {
    mock.restoreAll();
  }
});
