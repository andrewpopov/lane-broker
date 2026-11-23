import assert from 'node:assert/strict';
import { readAttempt } from '../src/attempts.js';
import { readLedger } from '../src/pull-ledger.js';
import { S3Error } from '../src/s3-client.js';
import { definePullSuite, pullFixture, artifactsStream, ARTIFACT_PATH, advancing, deadQueue, prepareOver, transportError } from './helpers/pull-fixture.js';
import { tid } from './lane-dispatch-fixture.js';

/** PKG-401: the pull step's local fallback while the queue is unreachable, against a disposable queue. Its own file so its parent suite has its own time budget. */

const SCENARIOS = {};

const FALLBACK_CFG = { queueUnreachableAfterMs: 900_000, localFallbackAfterMs: 120_000 };
const admitsNow = { enabled: true, couldAdmitNow: async () => true };
// the first poll is answered (the row is observed), then the queue is gone
const answeredOnce = (fx) => {
  let answered = false;
  return { ...deadQueue(fx), get: async (id) => { if (answered) throw transportError; answered = true; return fx.queue.get(id); } };
};
const fallbackRun = async (fx, over = {}, limit = 40) => {
  const { recordQueueCall } = await over.load('queue-health.js');
  await recordQueueCall(fx.state, false, fx.clock.now);
  const polls = advancing(fx, limit);
  const outcome = await fx.run({ queue: answeredOnce(fx), localFallback: admitsNow, ...over.ctx, sleep: polls.sleep });
  return { outcome, sleeps: polls.sleeps };
};

SCENARIOS['PKG-401: a ticket whose completion was already observed (a finished row still being downloaded) never falls back to local'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load, cfg: { ...FALLBACK_CFG, queueUnreachableAfterMs: 300_000 } });
  await fx.hold();
  await fx.runner.finish(await fx.runner.putOutputs({ log: 'out\n', artifacts: artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"ok":1}' }) }));
  const finished = await fx.row();
  let calls = 0;
  const queue = { ...deadQueue(fx), get: async (id) => { if (++calls === 1) return { ...finished, state: 'queued' }; if (calls === 2) return fx.queue.get(id); throw transportError; } };
  const s3 = { ...fx.s3, getObjectToFile: async () => { throw new S3Error('s3 GET answered 503', { status: 503 }); } };
  const { outcome } = await fallbackRun(fx, { load, ctx: { queue, s3 } });
  assert.deepEqual([outcome.outcome, outcome.decision?.reason], ['lost', 'queue-unreachable'], 'the row had been seen finished, so the ticket is not run again');
  assert.notEqual(readAttempt(fx.state, fx.id)?.executor, 'local');
};

SCENARIOS['PKG-401: a death between the move to local and the ledger entry leaves a local attempt with its cancel owed, and recovery publishes nothing for the pull attempt'] = async (load, w, t) => {
  const { owedCancelIds } = await load('pull-envelope.js');
  const fx = await pullFixture(t, w, { load, cfg: FALLBACK_CFG });
  await fx.enqueue();
  const CRASH = new Error('supervisor died');
  fx.hooks.afterLocalMove = () => { throw CRASH; };
  await assert.rejects(fallbackRun(fx, { load }), CRASH);
  const attempt = readAttempt(fx.state, fx.id);
  assert.deepEqual([attempt.executor, attempt.generation], ['local', fx.generation + 1], 'the ticket already moved to local');
  assert.deepEqual(owedCancelIds(fx.state), [fx.id], 'the cancel the row is owed was written before the move');
  const recovered = await fx.step({ generation: attempt.generation });
  assert.equal(recovered.state, 'local', 'recovery leaves the local attempt alone');
  assert.equal(fx.published(), null, 'and publishes no queue-unreachable-local result for it');
  assert.equal(readLedger(fx.state, fx.id), null);
};

SCENARIOS['PKG-401: a ticket last seen queued moves to local once the host has lost the queue for localFallbackAfterMs: attempt A settles lost queue-unreachable-local, the owed cancel is written once, and the ticket continues at generation + 1'] = async (load, w, t) => {
  const { owedCancelIds, readOwedCancel } = await load('pull-envelope.js');
  const fx = await pullFixture(t, w, { load, cfg: FALLBACK_CFG });
  await fx.enqueue();
  const { outcome, sleeps } = await fallbackRun(fx, { load });
  assert.deepEqual([outcome.outcome, outcome.reason], ['local-fallback', 'pull-unreachable']);
  assert.ok(sleeps.length >= 8, `it waited the whole 120 s (${sleeps.length} polls) before moving`);
  assert.equal(readLedger(fx.state, fx.id).decision.reason, 'queue-unreachable-local', 'attempt A is ledgered lost');
  const attempt = readAttempt(fx.state, fx.id);
  assert.deepEqual([attempt.executor, attempt.generation, attempt.fallbackReason, attempt.pull.localFallback.fromGeneration], ['local', fx.generation + 1, 'pull-unreachable', fx.generation]);
  assert.equal(fx.published(), null, 'nothing is published: the caller waits for the local run');
  assert.deepEqual(owedCancelIds(fx.state), [fx.id]);
  const owed = readOwedCancel(fx.state, fx.id);
  assert.equal((await fx.row()).state, 'queued', 'the row stays until the owed cancel is sent');

  const again = await fx.run({ queue: deadQueue(fx), localFallback: admitsNow, sleep: async () => {} });
  assert.equal(again.outcome, 'stale', 'a second run finds the ticket already moved');
  assert.deepEqual(readOwedCancel(fx.state, fx.id), owed, 'the owed cancel was written once and not touched again');
  assert.deepEqual(owedCancelIds(fx.state), [fx.id]);
};

SCENARIOS['PKG-401: a ticket last seen claimed or running keeps today\'s behaviour: it never moves to local and settles lost queue-unreachable'] = async (load, w, t) => {
  let first;
  for (const [n, hold] of [[1, 'claimed'], [2, 'running']]) {
    const fx = await pullFixture(t, w, { load, id: tid(n), stub: first?.stub, cfg: { ...FALLBACK_CFG, queueUnreachableAfterMs: 300_000 } });
    first ??= fx;
    await (hold === 'running' ? fx.hold() : fx.claimed());
    const { outcome } = await fallbackRun(fx, { load });
    assert.deepEqual([outcome.outcome, outcome.decision?.reason], ['lost', 'queue-unreachable'], hold);
    assert.notEqual(readAttempt(fx.state, fx.id)?.executor, 'local', hold);
  }
};

SCENARIOS['PKG-401: a ticket stays put when the lane opted out, or this host cannot admit it locally now, and settles lost queue-unreachable'] = async (load, w, t) => {
  let first;
  const cases = [[1, { enabled: false, couldAdmitNow: async () => true }], [2, { enabled: true, couldAdmitNow: async () => false }], [3, undefined]];
  for (const [n, localFallback] of cases) {
    const fx = await pullFixture(t, w, { load, id: tid(n), stub: first?.stub, cfg: { ...FALLBACK_CFG, queueUnreachableAfterMs: 300_000 } });
    first ??= fx;
    await fx.enqueue();
    const { outcome } = await fallbackRun(fx, { load, ctx: { localFallback } });
    assert.deepEqual([outcome.outcome, outcome.decision?.reason], ['lost', 'queue-unreachable'], `case ${n}`);
  }
};

SCENARIOS['PKG-401: while the host is marked down the preparation budget is 60 s, not prepareBudgetMs, for a ticket that may fall back'] = async (load, w, t) => {
  const { recordQueueCall } = await load('queue-health.js');
  const fx = await pullFixture(t, w, { load, cfg: { prepareBudgetMs: 600_000 } });
  await recordQueueCall(fx.state, false, fx.clock.now);
  const { sleeps, sleep } = advancing(fx);
  await assert.rejects(prepareOver(fx, { sleep, localFallback: admitsNow }), /could not be prepared within the budget/);
  assert.equal(sleeps.length, 4, 'four polls of 15 s, then the shortened budget is spent');
};

const CANARIES = [
  ['PKG-401: a ticket last seen queued moves to local once the host has lost the queue for localFallbackAfterMs: attempt A settles lost queue-unreachable-local, the owed cancel is written once, and the ticket continues at generation + 1', 'pull-ticket.js', ['downFor >= afterMs && ', '']],
  ['PKG-401: a ticket last seen queued moves to local once the host has lost the queue for localFallbackAfterMs: attempt A settles lost queue-unreachable-local, the owed cancel is written once, and the ticket continues at generation + 1', 'pull-ticket.js', ["    writeOwedCancel(run.root, run.id, { generation: run.generation, at: run.now() });\n    const moved", "    const moved"]],
  ['PKG-401: a ticket last seen claimed or running keeps today\'s behaviour: it never moves to local and settles lost queue-unreachable', 'pull-ticket.js', ["lastRowState === 'queued' && ", '']],
  ['PKG-401: a ticket stays put when the lane opted out, or this host cannot admit it locally now, and settles lost queue-unreachable', 'pull-ticket.js', ['ctx.localFallback?.enabled === true && lastRowState', 'lastRowState']],
  ['PKG-401: a ticket stays put when the lane opted out, or this host cannot admit it locally now, and settles lost queue-unreachable', 'pull-ticket.js', [' && (await ctx.localFallback.couldAdmitNow())', '']],
  ['PKG-401: while the host is marked down the preparation budget is 60 s, not prepareBudgetMs, for a ticket that may fall back', 'pull-ticket.js', ['Math.min(prepareBudgetMs, FALLBACK_PREPARE_BUDGET_MS)', 'prepareBudgetMs']],
  ['PKG-401: a ticket whose completion was already observed (a finished row still being downloaded) never falls back to local', 'pull-ticket.js', ["['queued', 'claimed', 'running', 'fetching', 'withdrawn', 'unresolved']", "['queued', 'claimed', 'running', 'withdrawn', 'unresolved']"]],
  ['PKG-401: a death between the move to local and the ledger entry leaves a local attempt with its cancel owed, and recovery publishes nothing for the pull attempt', 'pull-ticket.js', [
    ["    const moved = await fallbackToLocal(run.root, run.id, PULL_UNREACHABLE_REASON);\n    if (!moved.ok) return null;\n    await hook(run, 'afterLocalMove');\n", "    accept(run, entryFor(run, null, lost('queue-unreachable-local')));\n    await hook(run, 'afterLocalMove');\n    const moved = await fallbackToLocal(run.root, run.id, PULL_UNREACHABLE_REASON);\n    if (!moved.ok) return null;\n"],
    ["    accept(run, entryFor(run, null, lost('queue-unreachable-local')));\n    return { outcome: 'local-fallback'", "    return { outcome: 'local-fallback'"],
  ]],
];

definePullSuite('pull ticket local fallback on a disposable Postgres', SCENARIOS, CANARIES);
