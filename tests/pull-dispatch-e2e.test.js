import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { after as afterAll, afterEach, before, describe, test } from 'node:test';
import { supervisorAlive } from '../src/attempts.js';
import { paths, readJsonSafe } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World } from './lane-dispatch-fixture.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { pullE2E } from './helpers/pull-e2e.js';

/**
 * BRAIN-1036 R7: the placement hook and the settle mapping, through the shipped caller path. `lane run` spawns a real supervisor
 * whose queue connections land on a disposable Postgres and whose object store is an in-process stub, so the config, the hook and
 * the exit code are all production code; a stub runner plays the S2 agent.
 */

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, timeoutMs = 60_000, detail = () => '') {
  for (const deadline = Date.now() + timeoutMs; !(await fn()); await sleep(100)) if (Date.now() > deadline) assert.fail(`timed out waiting for the ticket state ${detail()}`);
}
const DISPATCH_ONLY = { localExecution: 'refused' };
const supervisorExit = async (fx) => {
  await until(() => fx.supervisorExits().length === 1);
  return fx.supervisorExits()[0];
};
const resultOf = (fx, id) => readJsonSafe(path.join(paths(fx.state).results, `${id}.json`));
const historyRows = (fx, id) => fs.readFileSync(paths(fx.state).history, 'utf8').split('\n').filter((line) => line.includes(`"id":"${id}"`)).map((line) => JSON.parse(line));

async function reapExpiredLease(w, id) {
  const su = await w.su();
  const reap = () => su.query("SELECT lane_dispatch.ld_reap(interval '2 seconds', interval '120 seconds', interval '2 seconds')");
  await reap();
  await su.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '1 hour' WHERE id = $1", [id]);
  await reap();
}

async function recoveredLikeLive(load, w, t, settle) {
  const fx = await pullE2E(t, w, { load, localPaused: true });
  const live = fx.run();
  const liveId = await fx.enqueued();
  await fx.run1({ exit: 3, log: 'live output\n' });
  assert.equal((await live).code, 3);
  const customLog = path.join(makeTmpDir('pull-e2e-log-', t), 'custom.log');
  const id = await fx.detach(['true'], ['--log', customLog]);
  await fx.enqueued([liveId]);
  await fx.claim();
  await fx.killSupervisor(id);
  await fx.runner.start();
  await fx.runner.finish({ exit: 3, ...(await fx.runner.putOutputs({ log: 'recovered output\n' })) });
  const swept = await settle(fx, id);
  await until(() => resultOf(fx, id) !== null, 60_000, () => `${swept?.stdout ?? ''}${swept?.stderr ?? ''}`);
  const [recovered, reference] = [resultOf(fx, id), resultOf(fx, liveId)];
  assert.deepEqual(historyRows(fx, id).map((row) => [row.exit, row.logPath]), [[3, customLog]], 'exactly one history row, naming the custom log');
  assert.deepEqual(Object.keys(recovered).sort(), Object.keys(reference).sort());
  const pick = (r) => [r.exit, r.executor, r.transport, r.runner, r.remoteKind, r.remotePhase];
  assert.deepEqual(pick(recovered), pick(reference));
  assert.match(fs.readFileSync(customLog, 'utf8'), /recovered output/);
}

const within = (promise, ms, what) => Promise.race([promise, sleep(ms).then(() => assert.fail(`${what} did not happen within ${ms}ms`))]);

const SCENARIOS = {
  'a queue that stays unreachable ends `lane run` 75 with a ledgered queue-unreachable result at once, and a later sweep sends the cancel the row is owed': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, queueFail: 'ld_get', lanePull: { pollMs: 100, queueUnreachableAfterMs: 1500 } });
    const run = fx.spawnRun();
    const id = await fx.enqueued();
    assert.equal(await within(run.exited, 60_000, 'lane run ending'), 75, run.stderr());
    assert.deepEqual([resultOf(fx, id).exit, resultOf(fx, id).reason], [75, 'queue-unreachable']);
    assert.equal((await fx.ticket(id)).state, 'queued', 'the queue never heard from the supervisor again');
    assert.equal((await fx.lane(['reconcile', '--now'])).code, 0);
    assert.equal((await fx.ticket(id)).state, 'cancelled', 'the sweep sent the owed cancel');
  },

  'a cancel while the snapshot is being prepared against a store that is down publishes 130 and queues nothing': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, lanePull: { pollMs: 200 } });
    fx.stub.failMatching = (method, pathname) => (method === 'PUT' && pathname.includes('/streams/') ? 503 : 0);
    const run = fx.spawnRun();
    await until(() => fs.existsSync(paths(fx.state).attempts) && fs.readdirSync(paths(fx.state).attempts).some((f) => f.endsWith('.json')));
    const id = fs.readdirSync(paths(fx.state).attempts).find((f) => f.endsWith('.json')).replace('.json', '');
    await until(() => fx.attempt(id)?.pull?.stage === 'PREPARING');
    await sleep(1000);
    run.child.kill('SIGINT');
    assert.equal(await within(run.exited, 60_000, 'lane run ending'), 130, run.stderr());
    assert.equal(resultOf(fx, id)?.exit, 130, "the cancel is published, not just inferred by the caller");
    assert.equal((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n, 0);
  },

  'a cancel that arrives while the snapshot upload is still succeeding is acted on before anything is enqueued: 130 and no queue row': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, lanePull: { pollMs: 200 } });
    fx.stub.delayMatching = (method, pathname) => (method === 'PUT' && pathname.includes('/streams/') ? 4000 : 0);
    const run = fx.spawnRun();
    await until(() => fs.existsSync(paths(fx.state).attempts) && fs.readdirSync(paths(fx.state).attempts).some((f) => f.endsWith('.json')));
    const id = fs.readdirSync(paths(fx.state).attempts).find((f) => f.endsWith('.json')).replace('.json', '');
    await until(() => fx.attempt(id)?.pull?.stage === 'PREPARING');
    await sleep(500);
    run.child.kill('SIGINT');
    assert.equal(await within(run.exited, 60_000, 'lane run ending'), 130, run.stderr());
    assert.equal(resultOf(fx, id)?.exit, 130);
    assert.equal((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n, 0, 'the cancel was seen before the enqueue, so nothing was ever queued');
  },

  'a retried --id submission ends its supervisor with the duplicate exit (78), never 0 and never anything a caller could read as a cancel': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const id = crypto.randomUUID();
    await fx.detach(['true'], ['--id', id]);
    await fx.enqueued();
    const again = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', '--id', id, '--', 'true']);
    assert.equal(again.code, 0, 'the detach path does not wait on the supervisor: the caller still gets its id back');
    assert.equal(again.stdout.trim(), id);
    await until(() => fx.supervisorExits().length === 1);
    assert.deepEqual(fx.supervisorExits(), [78]);
  },

  'a pull supervisor killed by a signal leaves `lane run` exit 1, never 130, even after the caller was interrupted': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, lanePull: { pollMs: 200 } });
    const run = fx.spawnRun();
    const id = await fx.enqueued();
    process.kill(fx.attempt(id).supervisor.pid, 'SIGKILL');
    run.child.kill('SIGINT');
    assert.equal(await within(run.exited, 60_000, 'lane run ending'), 1, run.stderr());
  },

  'a SIGINT with no proven stop never makes `lane run` exit 130: the unconfirmed cancel is unresolved, exit 1': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, lanePull: { pollMs: 200 } });
    const run = fx.spawnRun();
    const id = await fx.enqueued();
    await fx.claim();
    await fx.runner.start();
    run.child.kill('SIGINT');
    await until(() => fx.attempt(id)?.pull?.cancel?.requestedAt !== undefined);
    await fx.runner.finishCancelled(false);
    assert.equal(await within(run.exited, 60_000, 'lane run ending'), 1, run.stderr());
    assert.match(run.stderr(), /not proven/);
  },

  'T32: no eligible principal on a dispatch-only host is a retryable refusal, exit 76, and nothing is enqueued': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, globalExtra: DISPATCH_ONLY });
    await (await w.su()).query("UPDATE lane_dispatch.principals SET max_claims = 0 WHERE kind = 'agent'");
    const res = await fx.run();
    assert.equal(res.code, 76, res.stderr);
    assert.equal((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n, 0);
  },

  'T32: a withdraw the queue committed on a dispatch-only host is a retryable refusal, exit 76': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, globalExtra: DISPATCH_ONLY, lanePull: { queuedTimeoutMs: 1500 } });
    const res = await fx.run();
    assert.equal(res.code, 76, res.stderr);
    assert.equal((await fx.ticket(await fx.enqueued())).state, 'withdrawn');
  },

  'a ticket nobody claims within its queued timeout is withdrawn and, where this host may run it, runs locally': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, lanePull: { queuedTimeoutMs: 1500 } });
    const id = await fx.detach(fx.markLocalRun);
    await until(async () => (await fx.ticket(id))?.state === 'withdrawn');
    assert.equal(fx.ranLocally(), false, 'the local queue is still closed');
    fx.unpauseLocal();
    const waited = await fx.wait(id, '25s');
    assert.equal(waited.code, 0, waited.stderr);
    assert.equal(fx.ranLocally(), true);
  },

  'T2: while the queue has not committed the withdraw the command never runs locally and the supervisor keeps waiting': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, queueFail: 'ld_withdraw', lanePull: { queuedTimeoutMs: 1000 } });
    const id = await fx.detach(fx.markLocalRun);
    await sleep(4000);
    const attempt = fx.attempt(id);
    assert.equal(fx.ranLocally(), false);
    assert.equal(supervisorAlive(attempt), true, 'the supervisor gave up waiting for a withdraw that never committed');
    assert.equal(attempt.pull.withdrawProof, undefined);
    assert.equal((await fx.ticket(id)).state, 'queued');
    assert.equal((await fx.lane(['cancel', id])).code, 0);
    assert.equal((await fx.wait(id)).code, 130);
    assert.equal(fx.ranLocally(), false);
  },

  'T14: with pull switched off a pull runner is never consulted: no queue connection, no object request (the push flow itself is pinned by the pull-push-golden recording)': async (load, w, t) => {
    const off = await pullE2E(t, w, { load, pushRunner: true, ssh: 'down', lanePull: { enabled: false } });
    const res = await off.run(off.markLocalRun);
    assert.deepEqual([res.code, off.ranLocally()], [0, true]);
    assert.deepEqual([off.queueCalls(), off.stub.requests.length], [0, 0]);
  },

  'T14: a ticket a push runner takes reaches no pull code: no queue connection, no object request, even with the queue failing': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, pushRunner: true, ssh: 'normal', queueFail: 'ld_queue_ref,ld_placeable' });
    const res = await fx.run(['node', '-e', 'process.exit(0)']);
    assert.equal(res.code, 0, res.stderr);
    assert.match(res.stderr, /running on skybox/);
    assert.deepEqual([fx.queueCalls(), fx.stub.requests.length], [0, 0]);
  },

  'a finished pull ticket exits with the job\'s own exit code, publishes one result and writes one history row': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const running = fx.run();
    const id = await fx.enqueued();
    await fx.run1({ exit: 3 });
    const res = await running;
    assert.equal(res.code, 3, res.stderr);
    assert.equal(await supervisorExit(fx), 3);
    const result = resultOf(fx, id);
    assert.deepEqual([result.exit, result.executor, result.transport, result.remoteKind], [3, 'remote', 'pull', 'completed']);
    assert.deepEqual(historyRows(fx, id).map((row) => [row.exit, row.executor]), [[3, 'remote']]);
  },

  'a proven cancel of a queued pull ticket exits 130': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const id = await fx.detach();
    await fx.enqueued();
    assert.equal((await fx.lane(['cancel', id])).code, 0);
    assert.equal((await fx.wait(id)).code, 130);
    assert.equal(await supervisorExit(fx), 130);
    assert.deepEqual([resultOf(fx, id).exit, resultOf(fx, id).remoteCancelConfirmed], [130, true]);
  },

  'after a cancel the supervisor keeps polling at its own pace: a held ticket whose runner has not yet answered is not stepped in a busy loop': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, lanePull: { pollMs: 1000 } });
    const id = await fx.detach();
    await fx.enqueued();
    await fx.claim();
    await fx.runner.start();
    const cancelling = fx.lane(['cancel', id]);
    await until(() => fx.attempt(id)?.pull?.cancel?.requestedAt !== undefined);
    const before = fx.queueCalls();
    await sleep(3000);
    const steps = fx.queueCalls() - before;
    assert.ok(steps <= 12, `${steps} queue connections in 3 s after the cancel at a 1 s poll: the supervisor is stepping without a pause`);
    await fx.runner.finish({ cancelled: true });
    await cancelling;
  },

  'a ticket whose supervisor died after the enqueue settles through the reconcile sweep exactly as the live path publishes it: one history row, the same result fields, the tail in the custom log': async (load, w, t) => recoveredLikeLive(load, w, t, (fx) => fx.lane(['reconcile', '--now'])),

  'a ticket whose supervisor died after the enqueue settles through lane wait exactly as the live path publishes it: one history row, the same result fields, the tail in the custom log': async (load, w, t) => recoveredLikeLive(load, w, t, (fx, id) => fx.wait(id)),

  'a requested priority tier reaches the queue\'s priority column: medium 4 by default, high 7': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const high = fx.run(['true'], ['--priority', 'high']);
    const first = await fx.enqueued();
    assert.equal((await fx.ticket(first)).priority, 7);
    await fx.run1();
    await high;
    const medium = fx.run();
    const second = await fx.enqueued([first]);
    assert.equal((await fx.ticket(second)).priority, 4);
    await fx.run1();
    await medium;
  },

  'a second supervisor reuses the readiness verdict the first stored: no lifecycle read, no queue login': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, globalExtra: DISPATCH_ONLY });
    await (await w.su()).query("UPDATE lane_dispatch.principals SET max_claims = 0 WHERE kind = 'agent'");
    assert.equal((await fx.run()).code, 76);
    const [stubAfterFirst, queueAfterFirst] = [fx.stub.requests.length, fx.queueCalls()];
    assert.ok(stubAfterFirst >= 2, 'the first supervisor read both buckets\' lifecycle');
    assert.equal((await fx.run()).code, 76);
    assert.deepEqual([fx.stub.requests.length, fx.queueCalls() - queueAfterFirst], [stubAfterFirst, 1], 'the second supervisor asked only ld_placeable');
  },

  'a snapshot upload that fails permanently before anything is enqueued is the never-started path: nothing is queued, a dispatch-only host exits 76, another runs it locally': async (load, w, t) => {
    const refusing = await pullE2E(t, w, { load, globalExtra: DISPATCH_ONLY });
    refusing.stub.failMatching = (method, pathname) => (method === 'PUT' && pathname.includes('/streams/') ? 403 : 0);
    const res = await refusing.run();
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /pull snapshot could not be prepared/);
    const local = await pullE2E(t, w, { load, localPaused: false });
    local.stub.failMatching = refusing.stub.failMatching;
    const ran = await local.run(local.markLocalRun);
    assert.equal(ran.code, 0, ran.stderr);
    assert.equal(local.ranLocally(), true);
    assert.equal((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n, 0);
  },

  'a snapshot upload that fails with a server error is retried and the ticket then runs to its own exit': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    let uploads = 0;
    fx.stub.failMatching = (method, pathname) => (method === 'PUT' && pathname.includes('/streams/') && ++uploads <= 2 ? 503 : 0);
    const running = fx.run();
    await fx.enqueued();
    await fx.run1({ exit: 2 });
    assert.equal((await running).code, 2);
    assert.equal(uploads, 3);
  },

  'a pull ticket the queue marks lost exits 75': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const running = fx.run();
    const id = await fx.enqueued();
    await fx.claim();
    await fx.runner.start();
    await reapExpiredLease(w, id);
    const res = await running;
    assert.equal(res.code, 75, res.stderr);
    assert.equal(await supervisorExit(fx), 75);
    assert.equal(resultOf(fx, id).reason, 'runner-lost');
  },

  'a cancel the queue records without proof leaves the pull ticket unresolved: exit 1, the attempt kept': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const running = fx.run();
    const id = await fx.enqueued();
    await fx.claim();
    await fx.runner.start();
    await (await w.su()).query(`UPDATE lane_dispatch.tickets SET state = 'cancelled', finished_at = now(), result = '{"kind":"cancelled","cancelConfirmed":false,"source":"operator","author":"queue"}'::jsonb WHERE id = $1`, [id]);
    const res = await running;
    assert.equal(res.code, 1, res.stderr);
    assert.equal(await supervisorExit(fx), 1);
    assert.equal(fx.attempt(id).unresolved, 'cancel-unconfirmed');
    assert.equal(resultOf(fx, id), null);
  },
};

const supervisor = (from, to) => ['supervisor.js', [from, to]];
const pullTicket = (from, to) => ['pull-ticket.js', [from, to]];
const placement = (from, to) => ['pull-placement.js', [from, to]];

const RECOVERED_SWEEP = 'a ticket whose supervisor died after the enqueue settles through the reconcile sweep exactly as the live path publishes it: one history row, the same result fields, the tail in the custom log';
const RECOVERED_WAIT = 'a ticket whose supervisor died after the enqueue settles through lane wait exactly as the live path publishes it: one history row, the same result fields, the tail in the custom log';
const NOT_PREPARED = 'a snapshot upload that fails permanently before anything is enqueued is the never-started path: nothing is queued, a dispatch-only host exits 76, another runs it locally';

const CANARIES = [
  ['after a cancel the supervisor keeps polling at its own pace: a held ticket whose runner has not yet answered is not stepped in a busy loop', ...pullTicket('  const timer = setTimeout(done, ms);\n  signal?.addEventListener', '  if (signal?.aborted) return resolve();\n  const timer = setTimeout(done, ms);\n  signal?.addEventListener')],
  [RECOVERED_SWEEP, 'pull-services.js', ['    ...publicationBindingsFor(root, id),\n', '']],
  [RECOVERED_WAIT, 'pull-services.js', ['    ...publicationBindingsFor(root, id),\n', '']],
  ['a queue that stays unreachable ends `lane run` 75 with a ledgered queue-unreachable result at once, and a later sweep sends the cancel the row is owed', ...pullTicket('        if (queueDownSince !== null && now() - queueDownSince >= queueUnreachableAfterMs) return abandonUnreachableQueue(ctx);\n', '')],
  ['a cancel while the snapshot is being prepared against a store that is down publishes 130 and queues nothing', ...supervisor('      cancelRequested: () => abortSignal.aborted,', '      cancelRequested: () => false,')],
  ['a cancel that arrives while the snapshot upload is still succeeding is acted on before anything is enqueued: 130 and no queue row', 'pull-enqueue.js', ['    if (intent.attempts.length === 0 && cancelRequested()) return { outcome: CANCELLED_BEFORE_ENQUEUE, settled: await settleCancelled() };\n', '']],
  ['a retried --id submission ends its supervisor with the duplicate exit (78), never 0 and never anything a caller could read as a cancel', ...supervisor('if (created.existing) process.exit(DUPLICATE_SUBMISSION_EXIT);', 'if (created.existing) process.exit(0);')],
  ['a pull supervisor killed by a signal leaves `lane run` exit 1, never 130, even after the caller was interrupted', 'run.js', ['(ended.code === 0 || ended.code === 130)', '(ended.code === null || ended.code === 0 || ended.code === 130)']],
  ['a SIGINT with no proven stop never makes `lane run` exit 130: the unconfirmed cancel is unresolved, exit 1', 'run.js', ['(ended.code === 0 || ended.code === 130)', 'true']],
  ['a requested priority tier reaches the queue\'s priority column: medium 4 by default, high 7', ...supervisor('priority: pullQueuePriority(enriched.priorityRequested)', 'priority: 0')],
  ['a second supervisor reuses the readiness verdict the first stored: no lifecycle read, no queue login', 'pull-placement.js', ['if (stored?.key === key && age >= 0 && age < ttlMs) return stored.verdict;', '']],
  [NOT_PREPARED, ...supervisor('if (!(await proveNeverEnqueued(ctx)).proof) {', 'if (true) {')],
  [NOT_PREPARED, ...pullTicket('    await persistPull(run, { neverEnqueued: true });\n', '')],

  ['T32: no eligible principal on a dispatch-only host is a retryable refusal, exit 76, and nothing is enqueued', ...supervisor('{ rebindable: true, noRunner: true, retryable: true }', '{ rebindable: true, noRunner: true, retryable: false }')],
  ['T32: a withdraw the queue committed on a dispatch-only host is a retryable refusal, exit 76', ...supervisor("'withdrawn from the pull queue', { neverStarted: true, noRunner: true, retryable: true }", "'withdrawn from the pull queue', { neverStarted: true, noRunner: true, retryable: false }")],
  ['a ticket nobody claims within its queued timeout is withdrawn and, where this host may run it, runs locally', ...pullTicket('return queuedTooLong(run, attempt) ? expireQueued(run, attempt) :', 'return false ? expireQueued(run, attempt) :')],
  ['T2: while the queue has not committed the withdraw the command never runs locally and the supervisor keeps waiting', ...pullTicket("if (!(await withdrawLocked(run)).proof) return { state: 'queued', next: run.cfg.pollMs };", 'if (false) return { state: \'queued\', next: run.cfg.pollMs };')],
  ['T14: with pull switched off a pull runner is never consulted: no queue connection, no object request (the push flow itself is pinned by the pull-push-golden recording)', ...placement("if (!pullConfigured(cfg)) return { route: 'legacy' };", '')],
  ['T14: a ticket a push runner takes reaches no pull code: no queue connection, no object request, even with the queue failing', ...placement("if (pushChosen) return { route: 'push' };", '')],
  ['a finished pull ticket exits with the job\'s own exit code, publishes one result and writes one history row', ...pullTicket("case 'confirmed':\n      return Number.isInteger(outcome.exitCode) ? outcome.exitCode : 1;", "case 'confirmed':\n      return 0;")],
  ['a proven cancel of a queued pull ticket exits 130', ...pullTicket('return CANCELLED_EXIT;', 'return 0;')],
  ['a pull ticket the queue marks lost exits 75', ...pullTicket('return Number.isInteger(outcome.exitCode) ? outcome.exitCode : LOST_EXIT;', 'return 0;')],
  ['a cancel the queue records without proof leaves the pull ticket unresolved: exit 1, the attempt kept', ...pullTicket("    case 'unresolved':\n      return 1;", "    case 'unresolved':\n      return 0;")],
];

describe('pull placement and settlement through a real supervisor', { skip: pgSkipOption, timeout: 900_000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  afterAll(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());
  for (const [name, scenario] of Object.entries(SCENARIOS)) test(name, { timeout: 120_000 }, async (t) => scenario(realSrc, await World.create(cluster), t));
  for (const [name, file, mutation] of CANARIES) {
    test(`canary: with "${mutation[0].trim().slice(0, 70)}" removed, "${name}" fails`, { timeout: 150_000 }, async (t) => {
      const world = await World.create(cluster);
      const failure = await assertCanaryFails(file, mutation, (load) => SCENARIOS[name](load, world, t));
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0].slice(0, 160)}`);
    });
  }
});
