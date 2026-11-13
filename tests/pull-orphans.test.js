import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { readAttempt } from '../src/attempts.js';
import { paths } from '../src/state.js';
import { freshEnv } from './helpers.js';
import { ARTIFACT_PATH, artifactsStream, definePullSuite, pullFixture } from './helpers/pull-fixture.js';
import { tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R6: the orphan guards and transport branches (T10a, T10b, T26b, T37b's `lane wait` half), status, and the seam's pull adapter. */

const CRASH = new Error('killed');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const SLOW_MS = 4000;
const slowly = (fn) => async (...args) => { await sleep(SLOW_MS); return fn(...args); };
const GLOBAL_CFG = { runners: [], runnerLostAfterMs: 0 };
const ctxFor = (fx) => (ticket) => fx.ctx({ generation: ticket.generation });

async function withBrokerEnv(state, fn) {
  const { home } = freshEnv();
  const previous = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  Object.assign(process.env, { LANE_BROKER_HOME: home, LANE_BROKER_STATE: state });
  try {
    return await fn();
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', previous.home], ['LANE_BROKER_STATE', previous.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

async function silenced(fn) {
  const [out, err] = [process.stdout.write, process.stderr.write];
  process.stdout.write = () => true;
  process.stderr.write = () => true;
  try {
    return await fn();
  } finally {
    Object.assign(process.stdout, { write: out });
    Object.assign(process.stderr, { write: err });
  }
}

const counting = (fx, name) => {
  const calls = [];
  const queue = { ...fx.queue, [name]: (...args) => { calls.push(args); return fx.queue[name](...args); } };
  return { calls, ctx: (ticket, extra = {}) => fx.ctx({ generation: ticket.generation, queue, ...extra }) };
};

const SCENARIOS = {
  'T10a: a dead supervisor in PREPARING is left to pull recovery, which settles it prepare-interrupted, never never-dispatched': async (load, w, t) => {
    const { reconcileRemoteAttempt } = await load('remote-reconcile.js');
    const fx = await pullFixture(t, w, { load });
    fx.hooks.afterMarker = () => { throw CRASH; };
    await assert.rejects(fx.prepare(), CRASH);
    const attempt = readAttempt(fx.state, fx.id);
    assert.deepEqual([attempt.transport, attempt.phase, attempt.runner ?? null, attempt.binding ?? null], ['pull', 'probe', null, null]);
    assert.equal((await reconcileRemoteAttempt(fx.state, fx.id, { globalCfg: GLOBAL_CFG, pullContext: ctxFor(fx) })).state, 'skipped', 'a live preparer is never an orphan');
    await fx.killSupervisor();
    const outcome = await reconcileRemoteAttempt(fx.state, fx.id, { globalCfg: GLOBAL_CFG, pullContext: ctxFor(fx) });
    assert.deepEqual([outcome.settled, outcome.state, outcome.exit], [true, 'lost', 75]);
    assert.equal(fx.published().reason, 'prepare-interrupted');
  },

  'T10b: a queued pull attempt with a dead supervisor is left untouched by the sweep and by lane wait, which both report it still running': async (load, w, t) => {
    const { reconcileRemoteAttempt } = await load('remote-reconcile.js');
    const { waitCommand } = await load('wait.js');
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    await fx.killSupervisor();
    const before = readAttempt(fx.state, fx.id);
    assert.deepEqual([before.phase, before.runner], ['running', null]);
    assert.deepEqual(await reconcileRemoteAttempt(fx.state, fx.id, { globalCfg: GLOBAL_CFG, pullContext: ctxFor(fx) }), { settled: false, state: 'running' });
    const swept = await (await load('reconcile-sweep.js')).sweepOrphans(fx.state, { globalCfg: GLOBAL_CFG, force: true, pullContext: ctxFor(fx) });
    assert.deepEqual(swept.results.map((r) => [r.id, r.state]), [[fx.id, 'running']]);
    const waited = await withBrokerEnv(fx.state, () => silenced(() => waitCommand(fx.id, { timeoutMs: 1500, pullContext: ctxFor(fx) })));
    assert.equal(waited.exitCode, 75, 'waited, not failed');
    assert.deepEqual(readAttempt(fx.state, fx.id), before);
    assert.equal(fx.published(), null);
    assert.equal((await fx.row()).state, 'queued');
  },

  'T26b: an orphan cancel racing a ledgered finish publishes the finish once and never calls the queue': async (load, w, t) => {
    const { cancelAttempt } = await load('cancel.js');
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.finish({ exit: 0 });
    fx.hooks.afterLedger = () => { throw CRASH; };
    await assert.rejects(fx.step(), CRASH);
    delete fx.hooks.afterLedger;
    await fx.killSupervisor();
    const spy = counting(fx, 'cancel');
    const out = await silenced(() => cancelAttempt(fx.state, fx.id, readAttempt(fx.state, fx.id), { orphanOnly: true, pullContext: spy.ctx }));
    assert.equal(out.exitCode, 0);
    assert.deepEqual([fx.published().exit, fx.writes.length], [0, 1], 'the finish, not 130, once');
    assert.equal(spy.calls.length, 0, 'ledger first: nothing else is called');
  },

  'T26b: an orphan cancel of a queued row publishes 130 confirmed, and of a running row stays held until the runner proves the stop': async (load, w, t) => {
    const { cancelAttempt } = await load('cancel.js');
    const queued = await pullFixture(t, w, { load });
    await queued.enqueue();
    await queued.killSupervisor();
    const cancelled = await silenced(() => cancelAttempt(queued.state, queued.id, readAttempt(queued.state, queued.id), { orphanOnly: true, pullContext: ctxFor(queued) }));
    assert.equal(cancelled.exitCode, 0);
    assert.deepEqual([queued.published().exit, queued.published().remoteCancelConfirmed], [130, true]);
    assert.equal((await queued.row()).state, 'cancelled');

    const running = await pullFixture(t, w, { load, id: tid(2), stub: queued.stub });
    await running.hold();
    await running.killSupervisor();
    const attempt = () => readAttempt(running.state, running.id);
    const held = await silenced(() => cancelAttempt(running.state, running.id, attempt(), { orphanOnly: true, pullContext: ctxFor(running) }));
    assert.equal(held.exitCode, 1);
    assert.equal(running.published(), null);
    assert.equal(attempt().pull.cancel.answer.confirmed, false, 'the queue only recorded the request');
    await running.runner.finishCancelled(true);
    const proven = await silenced(() => cancelAttempt(running.state, running.id, attempt(), { orphanOnly: true, pullContext: ctxFor(running) }));
    assert.equal(proven.exitCode, 0);
    assert.deepEqual([running.published().exit, running.published().remoteCancelConfirmed], [130, true]);
  },

  'T37b: lane wait returns the ledgered exit when the attempt and the result are both gone': async (load, w, t) => {
    const { waitCommand } = await load('wait.js');
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.finish({ exit: 3 });
    fx.hooks.afterLedger = () => { throw CRASH; };
    await assert.rejects(fx.step(), CRASH);
    delete fx.hooks.afterLedger;
    fs.rmSync(path.join(paths(fx.state).attempts, `${fx.id}.json`));
    const waited = await withBrokerEnv(fx.state, () => silenced(() => waitCommand(fx.id, { timeoutMs: 20_000, pullContext: ctxFor(fx) })));
    assert.equal(waited.exitCode, 3, 'the ledgered exit, never the "nothing to wait for" 1');
    assert.equal(fx.published().exit, 3);
    assert.equal(fx.writes.length, 1);
  },

  'T26b: a cancel arriving while the publisher holds the ticket lock waits for it and then finds the finish already settled': async (load, w, t) => {
    const { cancelAttempt } = await load('cancel.js');
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.finish({ exit: 0 });
    let release;
    let reached;
    const gate = new Promise((resolve) => { release = resolve; });
    const atGate = new Promise((resolve) => { reached = resolve; });
    fx.hooks.afterLedger = () => { reached(); return gate; };
    const publishing = fx.step();
    publishing.catch(() => {});
    await atGate;
    const spy = counting(fx, 'cancel');
    const cancelling = cancelAttempt(fx.state, fx.id, readAttempt(fx.state, fx.id), { orphanOnly: true, pullContext: (ticket) => spy.ctx(ticket, { lockTimeoutMs: 10_000, hooks: {} }) });
    cancelling.catch(() => {});
    try {
      assert.equal(await Promise.race([cancelling, sleep(400).then(() => 'blocked')]), 'blocked', 'the cancel is queued behind the lock the publisher holds');
    } finally {
      release();
    }
    assert.equal((await publishing).state, 'settled');
    assert.equal((await cancelling).exitCode, 0);
    assert.deepEqual([fx.published().exit, fx.writes.length, spy.calls.length], [0, 1, 0], 'the finish, once, and the queue never asked to cancel');
  },

  'lane wait --timeout bounds a pull step that never answers and a ledger rebuild that never finishes': async (load, w, t) => {
    const { waitCommand } = await load('wait.js');
    const ledgered = await pullFixture(t, w, { load, id: tid(2) });
    await ledgered.hold();
    await ledgered.runner.finish(await ledgered.runner.putOutputs({ log: 'out\n', artifacts: artifactsStream(ledgered.id, { [ARTIFACT_PATH]: '{}' }) }));
    ledgered.hooks.afterLedger = () => { throw CRASH; };
    await assert.rejects(ledgered.step(), CRASH);
    fs.rmSync(path.join(paths(ledgered.state).attempts, `${ledgered.id}.json`));
    const hangs = { hooks: { beforeInstall: slowly(async () => {}) } };
    const rebuildStarted = Date.now();
    const rebuilt = await withBrokerEnv(ledgered.state, () => silenced(() => waitCommand(ledgered.id, { timeoutMs: 1500, pullContext: (ticket) => ledgered.ctx({ generation: ticket.generation, ...hangs }) })));
    assert.deepEqual(rebuilt, { exitCode: 75 });
    assert.ok(Date.now() - rebuildStarted < SLOW_MS - 500, 'the ledger rebuild is bounded by the wait deadline, not by how long it takes');
    const stuck = await pullFixture(t, w, { load, id: tid(1), stub: ledgered.stub });
    await stuck.enqueue();
    await stuck.killSupervisor();
    const hangingQueue = { ...stuck.queue, get: slowly((id) => stuck.queue.get(id)) };
    const started = Date.now();
    const reconciled = await withBrokerEnv(stuck.state, () => silenced(() => waitCommand(stuck.id, { timeoutMs: 1500, pullContext: (ticket) => stuck.ctx({ generation: ticket.generation, queue: hangingQueue }) })));
    assert.deepEqual(reconciled, { exitCode: 75 });
    assert.ok(Date.now() - started < SLOW_MS - 500, 'the pull step is bounded by the wait deadline, not by how long it takes');

  },

  'status shows a pull attempt as pull@host with no runner, and marks an orphan for pull recovery': async (load, w, t) => {
    const { collectStatus, renderStatusText } = await load('status.js');
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.step();
    await fx.killSupervisor();
    const status = await withBrokerEnv(fx.state, () => collectStatus({}));
    const row = status.remote.find((r) => r.id === fx.id);
    assert.deepEqual([row.runner, row.transport, row.pullHost, row.pullStage, row.orphaned], [null, 'pull', 'a1', 'QUEUED', true]);
    const text = renderStatusText(status);
    assert.match(text, new RegExp(`${fx.id}.*pull@a1  stage=QUEUED`));
    assert.match(text, /\[ORPHANED-PULL\]/);
  },

  'the pull transport adapter answers in the ssh shapes: a record, {missing, state}, a confirmed bool, an {action}': async (load, w, t) => {
    const { pullTransport } = await load('runner-transport.js');
    const fx = await pullFixture(t, w, { load });
    const transport = pullTransport(fx.queue);
    assert.deepEqual(await transport.fetchResult(fx.id), { missing: true, state: 'gone' }, 'no row');
    await fx.enqueue();
    assert.deepEqual(await transport.fetchResult(fx.id), { missing: true, state: 'queued' });
    await fx.runner.claim();
    assert.deepEqual(await transport.fetchResult(fx.id), { missing: true, state: 'running' });
    assert.deepEqual(await transport.withdraw(fx.id), { action: 'started' });
    await fx.runner.start();
    await fx.runner.finish();
    assert.equal((await transport.fetchResult(fx.id)).kind, 'completed', 'the runner record itself');
    assert.deepEqual(await transport.withdraw(fx.id), { action: 'finished' });

    const queued = await pullFixture(t, w, { load, id: tid(2), stub: fx.stub });
    await queued.enqueue();
    assert.equal(await transport.cancel(queued.id), true, 'a queued cancel is confirmed');
    assert.deepEqual(await transport.fetchResult(queued.id), { missing: true, state: 'gone' }, 'a queue-authored cancel is not a runner record');
    assert.deepEqual(await transport.withdraw(queued.id), { action: 'cancelled' });
    const failing = pullTransport({ get: async () => { throw CRASH; }, cancel: async () => { throw CRASH; }, withdraw: async () => { throw CRASH; } });
    assert.deepEqual([await failing.fetchResult(fx.id), await failing.cancel(fx.id), await failing.withdraw(fx.id)], [null, false, { action: 'unknown' }]);
  },
};

const RECONCILE_CANARY_TRANSPORT = "  if (attempt.transport === 'pull') {\n    const recovery = await loadPullRecovery();\n    return recovery.reconcilePullAttempt";

const CANARIES = [
  ['T10a: a dead supervisor in PREPARING is left to pull recovery, which settles it prepare-interrupted, never never-dispatched', 'remote-reconcile.js', [' && attempt.transport === undefined;', ';']],
  ['T10b: a queued pull attempt with a dead supervisor is left untouched by the sweep and by lane wait, which both report it still running', 'remote-reconcile.js', [RECONCILE_CANARY_TRANSPORT, RECONCILE_CANARY_TRANSPORT.replace("attempt.transport === 'pull'", 'false')]],
  ['T26b: an orphan cancel racing a ledgered finish publishes the finish once and never calls the queue', 'cancel.js', ["  if (attempt.transport === 'pull') {", '  if (false) {']],
  ['T26b: an orphan cancel of a queued row publishes 130 confirmed, and of a running row stays held until the runner proves the stop', 'pull-recover.js', ['cancelRequested: () => true', 'cancelRequested: () => false']],
  ['T37b: lane wait returns the ledgered exit when the attempt and the result are both gone', 'wait.js', ['if (readLedger(root, id) !== null) {', 'if (false) {']],
  ['lane wait --timeout bounds a pull step that never answers and a ledger rebuild that never finishes', 'pull-recover.js', ['return Promise.race([stepPullTicket({ ...ctx, stepBudgetMs: Math.min(ctx.stepBudgetMs ?? Infinity, remaining) }), expiry]).finally(() => clearTimeout(timer));', 'return stepPullTicket(ctx);']],
  ['status shows a pull attempt as pull@host with no runner, and marks an orphan for pull recovery', 'status.js', ["...(a.transport === 'pull' ? { transport: 'pull', pullHost: a.pull?.host ?? null, pullStage: a.pull?.stage ?? null } : {}),", '']],
  ['the pull transport adapter answers in the ssh shapes: a record, {missing, state}, a confirmed bool, an {action}', 'runner-transport.js', ["row.result && row.result.author !== 'queue' ? row.result", 'row.result ? row.result']],
  ['T26b: a cancel arriving while the publisher holds the ticket lock waits for it and then finds the finish already settled', 'pull-envelope.js', ["return withLock(root, fn, { lockPath: path.join(pullTicketDir(root, id), 'recover.lock'), group: 'broker', timeoutMs: lockTimeoutMs });", 'return fn();']],
];

definePullSuite('pull recovery surfaces: reconcile, cancel, wait, status and the transport seam', SCENARIOS, CANARIES);
