import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { freshEnv, laneRun, laneSpawn, waitFor, writeGlobalConfig, writeRepoConfig } from './helpers.js';
import { enqueue, tryStart, dequeueSync, listQueue, retierQueued, withdrawQueued } from '../src/scheduler.js';
import { createAttempt } from '../src/attempts.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { fenceLegacyQueue } from '../src/migrate.js';
import { readLease } from '../src/lease.js';
import { effectiveRank, priorityAuditOf, score } from '../src/priority.js';
import { paths, atomicWriteJson, readJsonSafe, withLock, writeCancelMarkerFile, writeExpireMarkerFile, writeWithdrawMarkerFile } from '../src/state.js';

/**
 * BRAIN-518 slice 1: `lane priority <id> high|medium|low`. The core re-tier (`retierQueued`), the CLI verb and its exit codes
 * (plans/exit-codes.md). A refusal asserts the exact exit AND that nothing on disk moved: queue bytes, fairness and reservation
 * records, the admission log.
 */

const T0 = 1_700_000_000_000;
const HERE = new URL('.', import.meta.url).pathname;
const SRC = path.join(HERE, '..', 'src');

const ticket = (id, repoId, priority, extra = {}) => ({
  id,
  key: `${repoId}:${id}`,
  repoId,
  weight: 1,
  cwd: process.cwd(),
  cmd: ['true'],
  supervisorPid: process.pid,
  supervisorStart: null,
  logPath: '/dev/null',
  resultPath: '/dev/null',
  priorityRequested: priority,
  ...extra,
});
const cfgWith = (overrides = {}) => ({ ...DEFAULT_GLOBAL_CONFIG, ...overrides });
const tryCfg = (overrides = {}) => cfgWith({ schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, conflictSafeBackfill: false, ...overrides });
const sampler = () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * 1024 ** 3, totalBytes: 64 * 1024 ** 3, macPressure: 'normal', source: 'test' });
const poll = (state, t, cfg) => tryStart(state, t, cfg, undefined, sampler, undefined, memory);
const readLog = (state) => (fs.existsSync(paths(state).admissionLog) ? fs.readFileSync(paths(state).admissionLog, 'utf8') : '');
const queued = (state) => Object.fromEntries(listQueue(state).map((t) => [t.id, t]));
const queueNames = (state) => fs.readdirSync(paths(state).queue).sort();
const deadPid = () => spawnSync(process.execPath, ['-e', '0']).pid;

/** A fenced state root: valid sched-v2.json and an empty fairness file, the legacy queue directory fenced off. */
function fenced() {
  const fx = freshEnv();
  atomicWriteJson(paths(fx.state).schedFence, { version: 2, migratedAt: T0 });
  atomicWriteJson(paths(fx.state).fairness, { version: 2, tickets: {} });
  fenceLegacyQueue(fx.state, 'test');
  return fx;
}

/** Every byte a refused re-tier must leave alone: the queue directory, each state-root file (fairness, reservation, skip state, seq), the admission log. */
function snapshot(state) {
  const files = {};
  for (const name of fs.readdirSync(state)) {
    const full = path.join(state, name);
    if (name !== path.basename(paths(state).admissionLog) && fs.statSync(full).isFile()) files[name] = fs.readFileSync(full, 'utf8');
  }
  const queue = {};
  const dir = paths(state).queue;
  for (const name of fs.readdirSync(dir)) queue[name] = fs.readFileSync(path.join(dir, name), 'utf8');
  return { files, queue, log: readLog(state), results: fs.existsSync(paths(state).results) ? fs.readdirSync(paths(state).results).sort() : [] };
}

const priority = (env, ...args) => laneRun(['priority', ...args], { env });

/** A refusal: the exit code, the phrase it prints, and that nothing changed. */
async function assertRefusal({ state, env }, args, exit, phrase) {
  const before = snapshot(state);
  const result = await priority(env, ...args);
  assert.equal(result.code, exit, `${args.join(' ')} exited ${result.code}: ${result.stderr}`);
  assert.match(result.stderr, phrase);
  assert.deepEqual(snapshot(state), before, 'a refusal changes nothing on disk');
  return result;
}

// ---- usage and validation ----

test('usage: missing or extra arguments exit 2, an invalid tier exits 64, and neither touches the queue', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  await assertRefusal(fx, [], 2, /usage/);
  await assertRefusal(fx, ['a'], 2, /usage/);
  await assertRefusal(fx, ['a', 'high', 'extra'], 2, /usage/);
  await assertRefusal(fx, ['a', 'urgent'], 64, /invalid tier "urgent"/);
});

// ---- refusals, each with its exact exit code ----

test('refusal 89: no fence, and a fence with a missing fairness file, name the real resolution reason', async () => {
  const legacy = freshEnv();
  await enqueue(legacy.state, ticket('a', 'r1', 'medium'), cfgWith());
  await assertRefusal(legacy, ['a', 'high'], 89, /legacy scheduler \(fence-missing\)/);

  const broken = fenced();
  await enqueue(broken.state, ticket('a', 'r1', 'medium'), cfgWith());
  fs.rmSync(paths(broken.state).fairness);
  await assertRefusal(broken, ['a', 'high'], 89, /legacy scheduler \(fairness-missing\)/);
});

test('refusal 80: no such ticket', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  await assertRefusal(fx, ['nope', 'high'], 80, /no such queued ticket/);
});

test('refusal 81: a started lease, whether or not a queue record still sits beside it (A2)', async () => {
  const lone = fenced();
  atomicWriteJson(path.join(paths(lone.state).leases, 'a.json'), { id: 'a', repoId: 'r1', supervisorPid: process.pid });
  await assertRefusal(lone, ['a', 'high'], 81, /running/);

  const both = fenced();
  await enqueue(both.state, ticket('a', 'r1', 'medium'), cfgWith());
  atomicWriteJson(path.join(paths(both.state).leases, 'a.json'), { id: 'a', repoId: 'r1', supervisorPid: process.pid });
  await assertRefusal(both, ['a', 'high'], 81, /running/);
});

test('refusal 80: a published result, whether or not a queue record still sits beside it (A2)', async () => {
  const lone = fenced();
  atomicWriteJson(path.join(paths(lone.state).results, 'a.json'), { id: 'a', exit: 0 });
  await assertRefusal(lone, ['a', 'high'], 80, /already finished \(result\)/);

  const both = fenced();
  await enqueue(both.state, ticket('a', 'r1', 'medium'), cfgWith());
  atomicWriteJson(path.join(paths(both.state).results, 'a.json'), { id: 'a', exit: 0 });
  await assertRefusal(both, ['a', 'high'], 80, /already finished \(result\)/);
});

for (const [kind, write] of [
  ['cancelled', writeCancelMarkerFile],
  ['expired', writeExpireMarkerFile],
  ['withdrawn', writeWithdrawMarkerFile],
]) {
  test(`refusal 80: a queued ticket with a ${kind} marker`, async () => {
    const fx = fenced();
    await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
    write(fx.state, 'a');
    await assertRefusal(fx, ['a', 'high'], 80, new RegExp(`already finished \\(${kind}\\)`));
  });
}

test('refusal 85: the queued ticket\'s supervisor is dead', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium', { supervisorPid: deadPid() }), cfgWith());
  await assertRefusal(fx, ['a', 'high'], 85, /supervisor is gone/);
});

test('lane priority <id> --bogus is an unknown option: exit 2 and nothing changes', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  const before = snapshot(fx.state);
  const r = await priority(fx.env, 'a', '--bogus');
  assert.equal(r.code, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /unknown option "--bogus"/);
  assert.deepEqual(snapshot(fx.state), before);
});

test('refusal 81: a remote attempt with nothing queued here says to cancel and resubmit, never to run lane priority on the runner (A4)', async () => {
  const fx = fenced();
  await createAttempt(fx.state, 'a', { runner: 'r', cwd: process.cwd() });
  const result = await assertRefusal(fx, ['a', 'high'], 81, /queued on a runner; lane cancel it and resubmit with --priority/);
  assert.doesNotMatch(result.stderr, /on the runner/);
});

test('refusal 81: on a runner, a ticket owned by a submitter (remote-exec ticket dir names it) is refused', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  const remoteTickets = path.join(fx.base, 'home-dir', '.cache', 'lane-broker', 'remote', 'tickets', 'submitter-ticket');
  fs.mkdirSync(remoteTickets, { recursive: true });
  fs.writeFileSync(path.join(remoteTickets, 'remote-id'), 'a\n');
  const env = { ...fx.env, HOME: path.join(fx.base, 'home-dir') };
  await assertRefusal({ state: fx.state, env }, ['a', 'high'], 81, /owned by submitter \(remote ticket submitter-ticket\); lane cancel it and resubmit with --priority there/);
});

test('refusal 75: a ticket mid-rebind (withdrawn to a runner) is retried later, not refused for good', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  assert.ok(await withdrawQueued(fx.state, 'a'));
  await assertRefusal(fx, ['a', 'high'], 75, /being moved to a runner/);
});

test('refusal 75: a migration in progress, and a lock that cannot be taken', async () => {
  const migrating = fenced();
  await enqueue(migrating.state, ticket('a', 'r1', 'medium'), cfgWith());
  fs.writeFileSync(paths(migrating.state).migrating, 'x');
  await assertRefusal(migrating, ['a', 'high'], 75, /nothing changed/);

  const busy = fenced();
  await enqueue(busy.state, ticket('a', 'r1', 'medium'), cfgWith());
  const env = { ...busy.env, LANE_BROKER_TEST_LOCK_TIMEOUT_MS: '300' };
  await withLock(busy.state, () => assertRefusal({ state: busy.state, env }, ['a', 'high'], 75, /nothing changed/));
});

test('refusal 1: a queue record that is unreadable, carries another id, or predates schedVersion 2', async () => {
  const garbage = fenced();
  fs.writeFileSync(path.join(paths(garbage.state).queue, `${String(1).padStart(12, '0')}-a.json`), '{not json');
  await assertRefusal(garbage, ['a', 'high'], 1, /unreadable or inconsistent/);

  const mismatch = fenced();
  atomicWriteJson(path.join(paths(mismatch.state).queue, `${String(1).padStart(12, '0')}-a.json`), { ...ticket('b', 'r1', 'medium'), seq: 1, schedVersion: 2 });
  await assertRefusal(mismatch, ['a', 'high'], 1, /unreadable or inconsistent/);

  const legacyRecord = fenced();
  atomicWriteJson(path.join(paths(legacyRecord.state).queue, `${String(1).padStart(12, '0')}-a.json`), { ...ticket('a', 'r1', 'medium'), seq: 1 });
  await assertRefusal(legacyRecord, ['a', 'high'], 1, /predates the priority scheduler/);
});

test('record.id mismatch is its own reason, and writes nothing (A2)', async () => {
  const fx = fenced();
  atomicWriteJson(path.join(paths(fx.state).queue, `${String(1).padStart(12, '0')}-a.json`), { ...ticket('b', 'r1', 'medium'), seq: 1, schedVersion: 2 });
  const before = snapshot(fx.state);
  const result = await retierQueued(fx.state, 'a', 'high', cfgWith());
  assert.deepEqual([result.outcome, result.reason], ['refused', 'record-mismatch']);
  assert.deepEqual(snapshot(fx.state), before);
});

// ---- the re-tier itself ----

test('success: the tier changes in place; seq, filename, age and every other file stay byte-identical; one log line', async () => {
  const fx = fenced();
  const cfg = cfgWith();
  await enqueue(fx.state, ticket('a', 'r1', 'medium', { prioOriginAt: T0 }), cfg);
  await enqueue(fx.state, ticket('b', 'r1', 'medium', { prioOriginAt: T0 }), cfg);
  // give the fairness store a real record to protect: a capacity-blocked head
  await poll(fx.state, ticket('a', 'r1', 'medium'), tryCfg({ capacity: 0.5 }));
  const before = snapshot(fx.state);
  const names = queueNames(fx.state);
  const recordBefore = queued(fx.state).b;

  const result = await retierQueued(fx.state, 'b', 'high', cfg);
  assert.deepEqual([result.outcome, result.reason], ['retiered', null]);

  assert.deepEqual(queueNames(fx.state), names, 'same filename: the seq prefix is untouched');
  const after = queued(fx.state).b;
  assert.deepEqual([after.priorityRequested, after.priorityAdmitted, after.priorityDemoted], ['high', 'high', false]);
  assert.equal(after.priorityRetiered, true);
  assert.ok(Number.isFinite(after.priorityRetieredAt));
  for (const key of ['seq', 'prioOriginAt', 'createdAt', 'localSince', 'cmd', 'key', 'repoId']) assert.deepEqual(after[key], recordBefore[key], key);
  const { priorityRequested: _r, priorityAdmitted: _a, priorityDemoted: _d, priorityRetiered: _t, priorityRetieredAt: _at, ...rest } = after;
  const { priorityRequested: _r0, priorityAdmitted: _a0, priorityDemoted: _d0, ...restBefore } = recordBefore;
  assert.deepEqual(rest, restBefore, 'only the tier fields and the audit pair changed');

  const afterSnap = snapshot(fx.state);
  assert.deepEqual(afterSnap.files, before.files, 'fairness, reservation, skip state and seq are byte-identical');
  assert.equal(afterSnap.queue[names[0]], before.queue[names[0]], 'the other ticket\'s record is byte-identical');
  const line = afterSnap.log.slice(before.log.length);
  assert.match(line, new RegExp(`lane-broker-priority event=retier ticket=b repo=r1 seq=${recordBefore.seq} from=medium/medium requested=high admitted=high demoted=false pid=${process.pid}\\n$`));
});

test('success: a re-tiered tail ticket is admitted ahead of an earlier one, by the real admission poll', async () => {
  const fx = fenced();
  const cfg = cfgWith();
  const a = ticket('a', 'r1', 'medium');
  const b = ticket('b', 'r2', 'medium');
  await enqueue(fx.state, a, cfg);
  await enqueue(fx.state, b, cfg);
  assert.equal((await retierQueued(fx.state, 'b', 'high', cfg)).outcome, 'retiered');
  assert.equal((await poll(fx.state, a, tryCfg())).reason, 'not-head');
  assert.equal((await poll(fx.state, b, tryCfg())).started, true);
  assert.equal(readLease(fx.state, 'b').priorityAdmitted, 'high');
});

test('success: an exclusive ticket keeps exclusive and its origin when re-tiered down', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium', { exclusive: true, prioOriginAt: T0 }), cfgWith());
  assert.equal((await retierQueued(fx.state, 'a', 'low', cfgWith())).outcome, 'retiered');
  const record = queued(fx.state).a;
  assert.deepEqual([record.exclusive, record.prioOriginAt, record.priorityAdmitted], [true, T0, 'low']);
});

test('unchanged: the same tier exits 0, prints so, and writes nothing (no file, no log line)', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  const before = snapshot(fx.state);
  const result = await priority(fx.env, 'a', 'medium');
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /already medium; nothing changed/);
  assert.deepEqual(snapshot(fx.state), before);
  assert.equal((await retierQueued(fx.state, 'a', 'medium', cfgWith())).outcome, 'unchanged');
});

test('CLI success: exit 0 and the transition on stdout', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'low'), cfgWith());
  const result = await priority(fx.env, 'a', 'high');
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /lane priority: a low -> high\n/);
  assert.equal(queued(fx.state).a.priorityAdmitted, 'high');
});

// ---- the per-repo high cap ----

test('cap: the ticket itself never counts against the cap (self-exclusion), a second high is demoted, and the slot frees when the first leaves high', async () => {
  const fx = fenced();
  const cfg = cfgWith();
  await enqueue(fx.state, ticket('a', 'r1', 'high'), cfg);
  await enqueue(fx.state, ticket('b', 'r1', 'medium'), cfg);

  const demoted = await retierQueued(fx.state, 'b', 'high', cfg);
  assert.deepEqual([demoted.outcome, demoted.after.priorityAdmitted, demoted.after.priorityDemoted], ['retiered', 'medium', true]);
  assert.deepEqual([queued(fx.state).b.priorityRequested, queued(fx.state).b.priorityAdmitted], ['high', 'medium'], 'the claim is kept, the admission is capped');

  assert.equal((await retierQueued(fx.state, 'a', 'high', cfg)).outcome, 'unchanged', 'a high re-asking for high is not counted against itself');

  assert.equal((await retierQueued(fx.state, 'a', 'low', cfg)).outcome, 'retiered');
  const admitted = await retierQueued(fx.state, 'b', 'high', cfg);
  assert.deepEqual([admitted.outcome, admitted.after.priorityAdmitted, admitted.after.priorityDemoted], ['retiered', 'high', false]);
});

test('cap: a demoted ticket moved back to medium stops claiming high; maxQueuedHighPerRepo 0 demotes every re-tier to high', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'high'), cfgWith());
  const down = await retierQueued(fx.state, 'a', 'medium', cfgWith());
  assert.deepEqual([down.after.priorityRequested, down.after.priorityAdmitted, down.after.priorityDemoted], ['medium', 'medium', false]);

  const zero = fenced();
  await enqueue(zero.state, ticket('a', 'r1', 'medium'), cfgWith({ maxQueuedHighPerRepo: 0 }));
  const up = await retierQueued(zero.state, 'a', 'high', cfgWith({ maxQueuedHighPerRepo: 0 }));
  assert.deepEqual([up.after.priorityAdmitted, up.after.priorityDemoted], ['medium', true]);
});

test('cap: a re-tier and an enqueue racing from two processes admit exactly one high (A7)', async () => {
  for (let round = 0; round < 3; round += 1) {
    const fx = fenced();
    await enqueue(fx.state, ticket('x', 'r1', 'medium'), cfgWith());
    const go = path.join(fx.base, 'go');
    const script = `
      import fs from 'node:fs';
      import { enqueue, retierQueued } from ${JSON.stringify(path.join(SRC, 'scheduler.js'))};
      import { DEFAULT_GLOBAL_CONFIG } from ${JSON.stringify(path.join(SRC, 'config.js'))};
      const [mode, ready, go, parentPid] = process.argv.slice(1);
      fs.writeFileSync(ready, '1');
      while (!fs.existsSync(go)) await new Promise((r) => setTimeout(r, 1));
      const root = process.env.LANE_BROKER_STATE;
      if (mode === 'retier') {
        const r = await retierQueued(root, 'x', 'high', DEFAULT_GLOBAL_CONFIG);
        process.stdout.write(JSON.stringify([r.outcome, r.after.priorityAdmitted]));
      } else {
        const t = { id: 'y', key: 'r1:y', repoId: 'r1', weight: 1, supervisorPid: Number(parentPid), supervisorStart: null, priorityRequested: 'high' };
        const r = await enqueue(root, t, DEFAULT_GLOBAL_CONFIG);
        process.stdout.write(JSON.stringify(['enqueue', r.priorityAdmitted]));
      }
    `;
    const run = (mode) => {
      const ready = path.join(fx.base, `ready-${mode}`);
      const child = spawn(process.execPath, ['--input-type=module', '-e', script, mode, ready, go, String(process.pid)], { env: fx.env });
      let out = '';
      let err = '';
      child.stdout.on('data', (d) => (out += d));
      child.stderr.on('data', (d) => (err += d));
      return { ready, done: new Promise((resolve) => child.on('close', (code) => resolve({ code, out, err }))) };
    };
    const a = run('retier');
    const b = run('enqueue');
    await waitFor(() => fs.existsSync(a.ready) && fs.existsSync(b.ready), { timeoutMs: 10_000 });
    fs.writeFileSync(go, '1');
    const results = await Promise.all([a.done, b.done]);
    for (const r of results) assert.equal(r.code, 0, r.err);
    const highs = Object.values(queued(fx.state)).filter((t) => t.priorityAdmitted === 'high');
    assert.equal(highs.length, 1, `round ${round}: ${JSON.stringify(results.map((r) => r.out))}`);
  }
});

// ---- races with the supervisor (A1, A7) ----

test('A1: a cancel that removes the queue file between the read and the write is not resurrected', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  const before = readLog(fx.state);
  const result = await retierQueued(fx.state, 'a', 'high', cfgWith(), {
    beforeWrite: () => {
      dequeueSync(fx.state, 'a'); // what an older, lock-free supervisor's dequeue-on-cancel does
    },
  });
  assert.deepEqual([result.outcome, result.reason, result.detail], ['refused', 'finished', 'gone']);
  assert.deepEqual(queueNames(fx.state), [], 'no queue record was resurrected');
  assert.equal(readLog(fx.state), before, 'and no retier line was logged');
});

test('A7: a ticket admitted between classification and write is refused as running, and its queue record is not resurrected', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  const lease = { id: 'a', repoId: 'r1', supervisorPid: process.pid };
  const result = await retierQueued(fx.state, 'a', 'high', cfgWith(), {
    beforeWrite: () => {
      atomicWriteJson(path.join(paths(fx.state).leases, 'a.json'), lease);
      dequeueSync(fx.state, 'a');
    },
  });
  assert.deepEqual([result.outcome, result.reason], ['refused', 'running']);
  assert.deepEqual(queueNames(fx.state), []);
  assert.deepEqual(readJsonSafe(path.join(paths(fx.state).leases, 'a.json')), lease, 'the lease is untouched');
});

test('A1: a locked dequeue (the supervisor\'s) waits for the broker lock a re-tier holds', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'medium'), cfgWith());
  // The supervisor's dequeue runs `withLock(root, () => dequeueSync(...))` (supervisor.js `underLock`); end to end it is covered by
  // queued-cancel.test.js. Here the contract is the lock itself: while a re-tier holds it, that dequeue cannot interleave.
  const order = [];
  const holding = retierQueued(fx.state, 'a', 'high', cfgWith(), {
    beforeWrite: async () => {
      const dequeue = withLock(fx.state, () => {
        order.push('dequeue');
        return dequeueSync(fx.state, 'a');
      });
      await new Promise((resolve) => setTimeout(resolve, 100));
      order.push('retier-writing');
      fx.pending = dequeue;
    },
  });
  const result = await holding;
  await fx.pending;
  order.push('done');
  assert.equal(result.outcome, 'retiered');
  assert.deepEqual(order, ['retier-writing', 'dequeue', 'done']);
  assert.deepEqual(queueNames(fx.state), [], 'the dequeue then removed the re-tiered record');
});

// ---- age, exclusivity, status and audit (slice 2) ----

test('age: an aged low re-tiered to medium keeps its age, so it outranks a fresh medium by exactly the credit it earned', async () => {
  const fx = fenced();
  const cfg = cfgWith();
  const later = T0 + 10 * 60_000;
  await enqueue(fx.state, ticket('a', 'r1', 'low', { prioOriginAt: T0 }), cfg);
  const agedLow = queued(fx.state).a;
  assert.equal(effectiveRank(agedLow, later, cfg), 1, 'ten minutes: a low has aged to medium');
  assert.equal((await retierQueued(fx.state, 'a', 'medium', cfg)).outcome, 'retiered');
  const record = queued(fx.state).a;
  assert.equal(record.prioOriginAt, T0, 'the age origin did not restart');
  const freshMedium = { ...record, prioOriginAt: later };
  assert.ok(score(record, later, cfg) > score(freshMedium, later, cfg), 'the earned age still counts');
  assert.ok(effectiveRank(record, later, cfg) >= effectiveRank(freshMedium, later, cfg));
});

test('exclusive: re-tiered to low it keeps exclusive and never ages, however long it waits', async () => {
  const fx = fenced();
  const cfg = cfgWith();
  await enqueue(fx.state, ticket('a', 'r1', 'high', { exclusive: true, prioOriginAt: T0 }), cfg);
  assert.equal((await retierQueued(fx.state, 'a', 'low', cfg)).outcome, 'retiered');
  const record = queued(fx.state).a;
  assert.equal(record.exclusive, true);
  const day = T0 + 24 * 3_600_000;
  assert.equal(score(record, day, cfg), score(record, T0, cfg), 'no credit accrues');
  assert.equal(effectiveRank(record, day, cfg), 0);
});

test('lane status --json shows the new tier', async () => {
  const fx = fenced();
  await enqueue(fx.state, ticket('a', 'r1', 'low'), cfgWith());
  assert.equal((await priority(fx.env, 'a', 'high')).code, 0);
  const status = JSON.parse((await laneRun(['status', '--json'], { env: fx.env })).stdout);
  assert.equal(status.queued.find((q) => q.id === 'a').priority, 'high');
});

test('audit: priorityAuditOf carries the re-tier pair, and a lease that never was re-tiered carries neither key', () => {
  assert.deepEqual(priorityAuditOf({ priorityRequested: 'high', priorityRetiered: true, priorityRetieredAt: 7 }), { priorityRequested: 'high', priorityRetiered: true, priorityRetieredAt: 7 });
  assert.deepEqual(priorityAuditOf({ priorityRequested: 'high', priorityAdmitted: 'high', priorityDemoted: false }), { priorityRequested: 'high', priorityAdmitted: 'high', priorityDemoted: false });
});

test('audit (A3): the lease of a re-tiered ticket carries priorityRetiered, an untouched one does not', async () => {
  const fx = fenced();
  const cfg = cfgWith();
  const a = ticket('a', 'r1', 'medium');
  const b = ticket('b', 'r2', 'medium');
  await enqueue(fx.state, a, cfg);
  await enqueue(fx.state, b, cfg);
  await retierQueued(fx.state, 'b', 'high', cfg);
  const stamped = queued(fx.state).b.priorityRetieredAt;
  assert.equal((await poll(fx.state, b, tryCfg())).started, true);
  const lease = readLease(fx.state, 'b');
  assert.deepEqual([lease.priorityRetiered, lease.priorityRetieredAt], [true, stamped]);
  assert.equal((await poll(fx.state, a, tryCfg())).started, true);
  const plain = readLease(fx.state, 'a');
  assert.equal('priorityRetiered' in plain, false);
  assert.equal('priorityRetieredAt' in plain, false);
});

test('audit (A3): the history row of a ticket re-tiered while queued carries priorityRetiered; an untouched ticket\'s row does not', async () => {
  const fx = fenced();
  writeGlobalConfig(fx.home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(fx.base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const release = path.join(fx.base, 'release');
  const blocker = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', `while [ ! -f ${release} ]; do sleep 0.1; done`], { env: fx.env, cwd: repoDir });
  const blockerDone = new Promise((resolve) => blocker.on('close', resolve));
  await waitFor(() => fs.existsSync(paths(fx.state).leases) && fs.readdirSync(paths(fx.state).leases).some((n) => n.endsWith('.json')), { timeoutMs: 60_000 });
  const detach = async () => {
    const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--priority', 'low', '--', 'true'], { env: fx.env, cwd: repoDir });
    assert.equal(started.code, 0, started.stderr);
    const id = started.stdout.trim();
    await waitFor(() => fs.readdirSync(paths(fx.state).queue).some((n) => n.includes(id)), { timeoutMs: 60_000 });
    return id;
  };
  const retiered = await detach();
  const untouched = await detach();
  assert.equal((await priority(fx.env, retiered, 'high')).code, 0);
  fs.writeFileSync(release, '');
  await blockerDone;
  for (const id of [retiered, untouched]) assert.equal((await laneRun(['wait', id, '--timeout', '60s'], { env: fx.env })).code, 0);
  const rows = fs.readFileSync(paths(fx.state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  const row = rows.find((r) => r.id === retiered);
  assert.deepEqual([row.priorityRequested, row.priorityRetiered], ['high', true]);
  assert.ok(Number.isFinite(row.priorityRetieredAt));
  const other = rows.find((r) => r.id === untouched);
  assert.equal('priorityRetiered' in other, false);
});
