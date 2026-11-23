import assert from 'node:assert/strict';
import test from 'node:test';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { freshEnv } from './helpers.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';

/** PKG-401: queue health (hysteresis, what counts as down), the readiness cache around it, and the local-fallback predicates the supervisor and the pull step share. */

const T0 = 1_000_000;
const SPACING = 30_000;
const CFG = { ...DEFAULT_GLOBAL_CONFIG, laneQueueUrl: 'postgres://u@pitelite:5432/lane_queue?sslmode=verify-full', lanePull: { ...DEFAULT_GLOBAL_CONFIG.lanePull, localFallbackWhenUnreachable: true, rustfs: { ...DEFAULT_GLOBAL_CONFIG.lanePull.rustfs, endpoint: 'http://s', credentialsEnvFile: '/e' } } };
const transport = (load) => load('queue-client.js').then(({ QueueError }) => new QueueError('ld_get failed: reset', { code: 'ECONNRESET', cause: new Error('reset') }));

function readiness(calls, { queueRef = async () => 'ref', rules = async () => ({ rules: [] }) } = {}) {
  return {
    available: true,
    s3: { getBucketLifecycle: async (bucket) => { calls.push(`lifecycle ${bucket}`); return rules(bucket); } },
    queue: { queueRef: async () => { calls.push('queueRef'); return queueRef(); } },
  };
}

const SCENARIOS = {
  'the queue is marked down by the first failed call and cleared only by two answered calls at least 30 s apart; a failure in between starts the count over': async (load) => {
    const { recordQueueCall, readQueueHealth } = await load('queue-health.js');
    const { state } = freshEnv();
    assert.equal(readQueueHealth(state).downSince, null);
    await recordQueueCall(state, false, T0);
    await recordQueueCall(state, false, T0 + 5_000);
    assert.equal(readQueueHealth(state).downSince, T0, 'the outage keeps the time of its first failure');
    await recordQueueCall(state, true, T0 + 6_000);
    await recordQueueCall(state, true, T0 + 6_000 + SPACING - 1);
    assert.equal(readQueueHealth(state).downSince, T0, 'two answers 29.999 s apart do not clear it');
    await recordQueueCall(state, false, T0 + 40_000);
    await recordQueueCall(state, true, T0 + 41_000);
    await recordQueueCall(state, true, T0 + 60_000);
    assert.equal(readQueueHealth(state).downSince, T0, 'a failure between the answers dropped the first one');
    await recordQueueCall(state, true, T0 + 41_000 + SPACING);
    assert.equal(readQueueHealth(state).downSince, null, 'two answers 30 s apart with no failure between clear it');
    assert.equal(readQueueHealth(state).lastFailureAt, T0 + 40_000);
  },

  'a failed queue call invalidates a cached positive readiness verdict, and a negative verdict is reused for 30 s only while the host is marked down': async (load) => {
    const { createPullReadiness, PULL_READY_TTL_MS } = await load('pull-placement.js');
    const { recordQueueCall } = await load('queue-health.js');
    const { state } = freshEnv();
    const clock = { now: T0 };
    const verdictFor = (services) => createPullReadiness({ root: state, now: () => clock.now })(CFG, services);
    const first = [];
    assert.deepEqual(await verdictFor(readiness(first)), { ready: true });
    clock.now += 1_000;
    const cached = [];
    await verdictFor(readiness(cached));
    assert.deepEqual(cached, [], 'without a failure the positive verdict is reused');
    await recordQueueCall(state, false, clock.now);
    clock.now += 1_000;
    const probed = [];
    await verdictFor(readiness(probed));
    assert.ok(probed.includes('queueRef'), 'a failed call since the verdict made it stale');

    const negative = freshEnv().state;
    const verdictAt = (services) => createPullReadiness({ root: negative, now: () => clock.now })(CFG, services);
    clock.now = T0;
    const refusing = { queueRef: async () => { throw new Error('connection refused'); } };
    assert.equal((await verdictAt(readiness([], refusing))).ready, false);
    clock.now += 31_000;
    const calm = [];
    await verdictAt(readiness(calm));
    assert.deepEqual(calm, [], 'a negative verdict lasts its full TTL while the host is not marked down');

    clock.now = T0;
    const down = freshEnv().state;
    const downVerdict = (services) => createPullReadiness({ root: down, now: () => clock.now })(CFG, services);
    await recordQueueCall(down, false, clock.now - 1);
    assert.equal((await downVerdict(readiness([], refusing))).ready, false);
    clock.now += 29_000;
    const young = [];
    await downVerdict(readiness(young));
    assert.deepEqual(young, [], 'inside 30 s the negative verdict is reused');
    clock.now += 2_000;
    const old = [];
    assert.deepEqual(await downVerdict(readiness(old)), { ready: true });
    assert.ok(old.includes('queueRef'), 'past 30 s, with the host marked down, it is probed again');
    assert.ok(PULL_READY_TTL_MS > 31_000);
  },

  'a store (S3) timeout never marks the queue down, while a queue transport failure does and a server error counts as an answer': async (load) => {
    const { createPullReadiness } = await load('pull-placement.js');
    const { readQueueHealth, trackQueueHealth } = await load('queue-health.js');
    const { isQueueUnreachable, QueueError } = await load('queue-client.js');
    const { state } = freshEnv();
    const s3Timeout = Object.assign(new Error('s3 GET timed out'), { name: 'S3Error', status: null, cause: new Error('timeout') });
    const services = readiness([], { rules: async () => { throw s3Timeout; } });
    services.queue = trackQueueHealth(services.queue, state);
    const verdict = await createPullReadiness({ root: state })(CFG, services);
    assert.equal(verdict.ready, false, 'the store outage is a negative verdict');
    assert.equal(readQueueHealth(state).downSince, null, 'but it is not a queue outage');
    assert.equal(isQueueUnreachable(s3Timeout), false);

    const tracked = trackQueueHealth({ get: async () => { throw await transport(load); } }, state, { now: () => T0 });
    await assert.rejects(tracked.get(), /reset/);
    assert.equal(readQueueHealth(state).downSince, T0, 'a transport failure of a queue call marks it down');
    const sqlError = Object.assign(new Error('permission denied'), { severity: 'ERROR', code: '42501' });
    const answered = trackQueueHealth({ get: async () => { throw new QueueError('ld_get failed', { code: '42501', cause: sqlError }); } }, state, { now: () => T0 + 1_000 });
    await assert.rejects(answered.get());
    assert.equal(readQueueHealth(state).okAt, T0 + 1_000, 'a server error is an answer, so it starts the recovery');
  },

  'queue health is written under a lock: a write waits for whoever holds it, so a stale read can never erase a failure': async (load) => {
    const { recordQueueCall, readQueueHealth, queueHealthLock } = await load('queue-health.js');
    const { withLock } = await load('state.js');
    const { state } = freshEnv();
    const { paths } = await load('state.js');
    (await import('node:fs')).mkdirSync(paths(state).pull, { recursive: true });
    let release;
    const held = withLock(state, () => new Promise((resolve) => { release = resolve; }), { lockPath: queueHealthLock(state), group: 'broker' });
    await new Promise((resolve) => setTimeout(resolve, 100));
    const writing = recordQueueCall(state, false, T0);
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.equal(readQueueHealth(state).downSince, null, 'the write has not happened while the lock is held');
    release();
    await held;
    await writing;
    assert.equal(readQueueHealth(state).downSince, T0);
  },

  'a failure recorded while the readiness probe ran is not hidden: the verdict is stamped when the probe started': async (load) => {
    const { createPullReadiness } = await load('pull-placement.js');
    const { recordQueueCall } = await load('queue-health.js');
    const { state } = freshEnv();
    const clock = { now: T0 };
    const verdictFor = (services) => createPullReadiness({ root: state, now: () => clock.now })(CFG, services);
    const racing = readiness([], { queueRef: async () => { clock.now += 10; await recordQueueCall(state, false, clock.now); return 'ref'; } });
    assert.deepEqual(await verdictFor(racing), { ready: true });
    clock.now += 1;
    const again = [];
    await verdictFor(readiness(again));
    assert.ok(again.includes('queueRef'), 'the failure that landed during the probe made the verdict stale');
  },

  'a host that has not opted in is unchanged: no health file is written, a failed call does not invalidate a positive verdict and a negative one keeps its 5 minute TTL': async (load) => {
    const { createPullReadiness, PULL_READY_TTL_MS } = await load('pull-placement.js');
    const { queueWithHealth } = await load('pull-services.js');
    const { readQueueHealth, recordQueueCall } = await load('queue-health.js');
    const { QueueError } = await load('queue-client.js');
    const fs = await import('node:fs');
    const path = await import('node:path');
    const { paths } = await load('state.js');
    const plain = { ...CFG, lanePull: { ...CFG.lanePull, localFallbackWhenUnreachable: false } };
    const { state } = freshEnv();
    const queue = { get: async () => { throw new QueueError('ld_get failed', { code: 'ECONNRESET', cause: new Error('reset') }); } };
    assert.equal(queueWithHealth(queue, plain, state), queue, 'the queue client is not wrapped');
    await assert.rejects(queueWithHealth(queue, plain, state).get());
    assert.equal(fs.existsSync(path.join(paths(state).pull, 'queue-health.json')), false, 'no health file');
    assert.notEqual(queueWithHealth(queue, CFG, state), queue, 'an opted-in host is wrapped');

    const clock = { now: T0 };
    const verdictFor = (services) => createPullReadiness({ root: state, now: () => clock.now })(plain, services);
    await verdictFor(readiness([]));
    await recordQueueCall(state, false, clock.now + 1);
    clock.now += 2;
    const cached = [];
    await verdictFor(readiness(cached));
    assert.deepEqual(cached, [], 'a failure on record does not make a positive verdict stale for a host that did not opt in');

    const negative = freshEnv().state;
    const refusing = { queueRef: async () => { throw new Error('connection refused'); } };
    const negativeFor = (services) => createPullReadiness({ root: negative, now: () => clock.now })(plain, services);
    await recordQueueCall(negative, false, clock.now);
    await negativeFor(readiness([], refusing));
    clock.now += 31_000;
    const reused = [];
    await negativeFor(readiness(reused));
    assert.deepEqual(reused, [], 'the negative verdict keeps its full TTL');
    assert.ok(readQueueHealth(negative).downSince !== null && PULL_READY_TTL_MS > 31_000);
  },

  'local fallback is off by default, on by the host key, off for a lane that opted out, and never on a dispatch-only host; it is active only while the queue is marked down': async (load) => {
    const { pullLocalFallbackEnabled } = await load('config.js');
    const { localFallbackActive, recordQueueCall } = await load('queue-health.js');
    assert.equal(pullLocalFallbackEnabled(DEFAULT_GLOBAL_CONFIG, {}), false);
    assert.equal(pullLocalFallbackEnabled(CFG, {}), true);
    assert.equal(pullLocalFallbackEnabled(CFG, { pullLocalFallback: true }), true);
    assert.equal(pullLocalFallbackEnabled(CFG, { pullLocalFallback: false }), false, 'the lane opt-out holds');
    assert.equal(pullLocalFallbackEnabled({ ...CFG, localExecution: 'refused' }, {}), false, 'a dispatch-only host never falls back');
    const { state } = freshEnv();
    assert.equal(localFallbackActive(state, CFG, {}), false, 'a reachable queue changes nothing');
    await recordQueueCall(state, false, T0);
    assert.equal(localFallbackActive(state, CFG, {}), true);
    assert.equal(localFallbackActive(state, CFG, { pullLocalFallback: false }), false);
    assert.equal(localFallbackActive(state, DEFAULT_GLOBAL_CONFIG, {}), false);
  },

  'a localRefused lane (and so an out-of-window e2e or train lane) is runnable here only under the fallback; a dispatch-only host stays refused': async (load) => {
    const { localIneligibilityOf } = await load('supervisor.js');
    const ticket = { lane: 'sim', localRefused: true, allowLocalSim: false, resources: { cpuCores: 1, memoryBytes: 1_000_000 }, weight: 1 };
    assert.ok(localIneligibilityOf(ticket, DEFAULT_GLOBAL_CONFIG, null), 'refused as ever');
    assert.equal(localIneligibilityOf(ticket, DEFAULT_GLOBAL_CONFIG, null, { pullFallback: true }), null, 'run under the fallback');
    assert.ok(localIneligibilityOf(ticket, { ...DEFAULT_GLOBAL_CONFIG, localExecution: 'refused' }, null, { pullFallback: true }), 'a dispatch-only host refuses everything');
  },

  'the history row of a ticket that fell back from the pull queue says transport local and fallbackReason pull-unreachable': async (load) => {
    const { historyRow } = await load('supervisor.js');
    const ticket = { id: 'a', key: 'k', repoId: 'r', lane: 'sim', weight: 1, resources: {}, command: 'x', headTree: 't' };
    const row = historyRow(ticket, { exit: 0, executor: 'local' }, { fallbackReason: 'pull-unreachable' });
    assert.deepEqual([row.transport, row.fallbackReason, row.localReason], ['local', 'pull-unreachable', 'fallback:pull-unreachable']);
    assert.equal(historyRow(ticket, { exit: 0, executor: 'local' }, { fallbackReason: 'no runner' }).transport, undefined);
  },
};

const CANARIES = [
  ['the queue is marked down by the first failed call and cleared only by two answered calls at least 30 s apart; a failure in between starts the count over', 'queue-health.js', ['  else if (now - health.okAt >= QUEUE_RECOVERY_SPACING_MS) write(', '  else write(']],
  ['a failed queue call invalidates a cached positive readiness verdict, and a negative verdict is reused for 30 s only while the host is marked down', 'pull-placement.js', [' && !failedSince) return', ') return']],
  ['a failed queue call invalidates a cached positive readiness verdict, and a negative verdict is reused for 30 s only while the host is marked down', 'pull-placement.js', ['Math.min(ttlMs, PULL_DOWN_NEGATIVE_TTL_MS)', 'ttlMs']],
  ['a store (S3) timeout never marks the queue down, while a queue transport failure does and a server error counts as an answer', 'queue-client.js', ["  if (err?.name !== 'QueueError' || tlsFailure(err)) return false;", '  if (tlsFailure(err)) return false;']],
  ['local fallback is off by default, on by the host key, off for a lane that opted out, and never on a dispatch-only host; it is active only while the queue is marked down', 'config.js', ['ticket?.pullLocalFallback !== false && ', '']],
  ['local fallback is off by default, on by the host key, off for a lane that opted out, and never on a dispatch-only host; it is active only while the queue is marked down', 'config.js', [' && !isDispatchOnly(cfg);', ';']],
  ['a localRefused lane (and so an out-of-window e2e or train lane) is runnable here only under the fallback; a dispatch-only host stays refused', 'supervisor.js', ['if (!pullFallback && locallyRefusedNow(enriched, globalCfg))', 'if (locallyRefusedNow(enriched, globalCfg))']],
  ['the history row of a ticket that fell back from the pull queue says transport local and fallbackReason pull-unreachable', 'supervisor.js', ["    ...(fallbackReason === PULL_UNREACHABLE_REASON ? { transport: 'local', fallbackReason } : {}),\n", '']],
  ['queue health is written under a lock: a write waits for whoever holds it, so a stale read can never erase a failure', 'queue-health.js', ["  return withLock(root, () => apply(root, ok, now), { lockPath: queueHealthLock(root), group: 'broker', timeoutMs: LOCK_WAIT_MS });", '  return apply(root, ok, now);']],
  ['a failure recorded while the readiness probe ran is not hidden: the verdict is stamped when the probe started', 'pull-placement.js', ['{ key, at: probedAt, verdict }', '{ key, at: now(), verdict }']],
  ['a host that has not opted in is unchanged: no health file is written, a failed call does not invalidate a positive verdict and a negative one keeps its 5 minute TTL', 'pull-services.js', ['cfg?.lanePull?.localFallbackWhenUnreachable === true ? trackQueueHealth', 'cfg?.lanePull ? trackQueueHealth']],
  ['a host that has not opted in is unchanged: no health file is written, a failed call does not invalidate a positive verdict and a negative one keeps its 5 minute TTL', 'pull-placement.js', ['cfg.lanePull?.localFallbackWhenUnreachable === true ? readQueueHealth(root) : null', 'readQueueHealth(root)']],
];

for (const [name, scenario] of Object.entries(SCENARIOS)) test(name, () => scenario(realSrc));
for (const [name, file, mutation] of CANARIES) {
  test(`canary: with "${mutation[0].trim().slice(0, 70)}" removed, "${name}" fails`, async () => {
    await assertCanaryFails(file, mutation, SCENARIOS[name]);
  });
}
