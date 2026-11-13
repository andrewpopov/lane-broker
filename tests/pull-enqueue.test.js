import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { createAttempt, readAttempt } from '../src/attempts.js';
import { enqueueOnce, enqueueArgs, ENQUEUE_CROSSED_RESTORE } from '../src/pull-enqueue.js';
import { createEnvelope, readEnvelope, recordSnapshot } from '../src/pull-envelope.js';
import { ensureStateDirs, paths } from '../src/state.js';
import { freshEnv } from './helpers.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';
import { harnessClient } from './helpers/queue-fixture.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, claim, start, simulateRestore, tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R4: the enqueue intent protocol (rev 3 N1) against a disposable Postgres, with the restore emulated as T23 does. */

const ID = tid(1);
const SNAPSHOT = { key: `streams/${ID}-0-abc.gz`, sha256: 'abc', bytes: 10, blobs: [] };
const LOST_RESPONSE = new Error('response lost');

async function preparedTicket() {
  const { state } = freshEnv();
  ensureStateDirs(state);
  const attempt = await createAttempt(state, ID);
  createEnvelope(state, {
    logicalId: ID, binding: { protocol: 1, ticketId: ID, generation: 0, manifestHash: 'mh' }, nonce: 'n', header: { protocol: 1, ticketId: ID, argv: ['true'] },
    repoKey: 'repoA', lane: 'test', laneKey: 'repoA:test', cpuCores: 1, memoryBytes: 1000, requiresTags: [], priority: 0,
  });
  recordSnapshot(state, ID, SNAPSHOT);
  return { state, generation: attempt.generation };
}

/** Wraps a queue so a test can crash around the send: `before` runs ahead of it, `after` once the call has really committed. */
function instrumented(queue, { beforeRef, before, after } = {}) {
  const calls = { refs: 0, sends: [] };
  return {
    calls,
    queueRef: async () => {
      calls.refs += 1;
      await beforeRef?.();
      return queue.queueRef();
    },
    enqueueFenced: async (args) => {
      calls.sends.push(args);
      await before?.(args);
      const view = await queue.enqueueFenced(args);
      await after?.(view);
      return view;
    },
  };
}

const emulateRestore = async (w) => {
  await (await w.su()).query('DELETE FROM lane_dispatch.tickets WHERE id = $1', [ID]);
  await simulateRestore(w);
};
const rowCount = async (w) => (await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets WHERE id = $1', [ID])).rows[0].n;
const intent = (state) => readAttempt(state, ID).pull.enqueue;
const dropResponse = () => { throw LOST_RESPONSE; };
const startRunner = async (w) => start(await w.as(logins.a1), await claim(await w.as(logins.a1)));

const SCENARIOS = {
  'a replay after a lost response reuses the same ref and gets the same row': async (w, t) => {
    const { state, generation } = await preparedTicket();
    const queue = instrumented(harnessClient(t, w).queue, { after: dropResponse });
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation, queue }), LOST_RESPONSE);
    assert.deepEqual(intent(state).attempts.map((a) => [a.n, a.answer]), [[1, null]], 'the intent stays pending');
    assert.equal(await rowCount(w), 1, 'the send did commit');

    queue.calls.refs = 0;
    const replay = instrumented(harnessClient(t, w).queue);
    const result = await enqueueOnce({ root: state, id: ID, generation, queue: replay });
    assert.equal(result.outcome, 'queued');
    assert.equal(result.view.id, ID);
    assert.equal(replay.calls.refs, 0, 'a replay never reads a fresh ref while an attempt may have been sent');
    assert.equal(replay.calls.sends[0].expectRef, queue.calls.sends[0].expectRef);
    assert.equal(JSON.stringify(replay.calls.sends[0]), JSON.stringify(queue.calls.sends[0]), 'the replay is byte-identical');
    assert.equal(await rowCount(w), 1);
    assert.deepEqual(intent(state).attempts.map((a) => [a.n, a.answer]), [[1, null], [2, 'row']]);
    assert.ok(intent(state).seenRowAt);
    assert.equal((await enqueueOnce({ root: state, id: ID, generation, queue: replay })).view, null, 'once the row is seen nothing is sent again');
    assert.equal(replay.calls.sends.length, 1);
  },

  'a crash after the commit and before the answer is recorded, then a restore, settles lost and inserts nothing': async (w, t) => {
    const { state, generation } = await preparedTicket();
    const crashing = instrumented(harnessClient(t, w).queue, { after: dropResponse });
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation, queue: crashing }), LOST_RESPONSE);
    await startRunner(w);
    await emulateRestore(w);

    const replay = instrumented(harnessClient(t, w).queue);
    assert.deepEqual(await enqueueOnce({ root: state, id: ID, generation, queue: replay }), { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE });
    assert.equal(await rowCount(w), 0, 'no new runnable row for that id');
    assert.deepEqual(intent(state).attempts.map((a) => [a.n, a.answer]), [[1, null], [2, 'stale']]);
    assert.deepEqual(await enqueueOnce({ root: state, id: ID, generation, queue: replay }), { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE });
    assert.equal(replay.calls.sends.length, 1, 'a lost ticket is never sent again');
    assert.equal(await rowCount(w), 0);
  },

  'a crash between the pending write and the send, then a restore, also settles lost (the accepted cost)': async (w, t) => {
    const { state, generation } = await preparedTicket();
    const crashing = instrumented(harnessClient(t, w).queue, { before: dropResponse });
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation, queue: crashing }), LOST_RESPONSE);
    assert.equal(await rowCount(w), 0, 'nothing was sent');
    await simulateRestore(w);
    const replay = instrumented(harnessClient(t, w).queue);
    assert.deepEqual(await enqueueOnce({ root: state, id: ID, generation, queue: replay }), { outcome: 'lost', reason: ENQUEUE_CROSSED_RESTORE });
    assert.equal(await rowCount(w), 0);
  },

  'a crash before the pending write leaves no trace, so a restore is harmless and the replay queues on a fresh ref': async (w, t) => {
    const { state, generation } = await preparedTicket();
    const crashing = instrumented(harnessClient(t, w).queue, { beforeRef: dropResponse });
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation, queue: crashing }), LOST_RESPONSE);
    assert.equal(readAttempt(state, ID).pull, undefined, 'no intent was written');
    await simulateRestore(w);
    const replay = instrumented(harnessClient(t, w).queue);
    assert.equal((await enqueueOnce({ root: state, id: ID, generation, queue: replay })).outcome, 'queued');
    assert.equal(await rowCount(w), 1);
    assert.equal(replay.calls.refs, 1);
  },

  'a stale answer after only stale attempts refreshes the ref, then queues': async (w, t) => {
    const { state, generation } = await preparedTicket();
    const restoreBetweenRefAndSend = async () => {
      await simulateRestore(w);
      await call(await w.as(logins.s1), 'ld_get', tid(99));
    };
    const racing = instrumented(harnessClient(t, w).queue, { before: restoreBetweenRefAndSend });
    assert.deepEqual(await enqueueOnce({ root: state, id: ID, generation, queue: racing }), { outcome: 'refresh' });
    assert.equal(await rowCount(w), 0);
    const firstRef = intent(state).attempts[0].ref;

    const retry = instrumented(harnessClient(t, w).queue);
    const result = await enqueueOnce({ root: state, id: ID, generation, queue: retry });
    assert.equal(result.outcome, 'queued');
    assert.equal(retry.calls.refs, 1);
    assert.notEqual(retry.calls.sends[0].expectRef, firstRef);
    assert.deepEqual(intent(state).attempts.map((a) => [a.n, a.answer]), [[1, 'stale'], [2, 'row']]);
    assert.equal(await rowCount(w), 1);
  },

  'the pending intent is on disk and fsynced before the send leaves': async (w, t) => {
    const { state, generation } = await preparedTicket();
    const rec = recordFsyncs();
    let atSend;
    const spying = instrumented(harnessClient(t, w).queue, {
      before: () => {
        atSend = { intent: intent(state), fsynced: rec.fsyncedPaths().some((p) => p.startsWith(`${paths(state).attempts}${path.sep}.tmp-`)), renamed: rec.indexOf('rename', path.join(paths(state).attempts, `${ID}.json`)) };
      },
    });
    try {
      await enqueueOnce({ root: state, id: ID, generation, queue: spying });
    } finally {
      rec.stop();
    }
    assert.deepEqual(atSend.intent.attempts.map((a) => [a.n, a.answer]), [[1, null]]);
    assert.equal(atSend.fsynced, true);
    assert.ok(atSend.renamed >= 0);
  },
};

describe('enqueue intent against a disposable Postgres', { skip: pgSkipOption, timeout: 600000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    test(name, async (t) => scenario(await World.create(cluster), t));
  }
});

describe('enqueue intent without a database', () => {
  test('an envelope with no snapshot descriptor enqueues nothing', async () => {
    const { state } = freshEnv();
    ensureStateDirs(state);
    const { generation } = await createAttempt(state, ID);
    createEnvelope(state, {
      logicalId: ID, binding: { protocol: 1, ticketId: ID, generation: 0, manifestHash: 'mh' }, nonce: 'n', header: {}, repoKey: 'repoA', lane: 'test',
      laneKey: 'k', cpuCores: 1, memoryBytes: 1, requiresTags: [], priority: 0,
    });
    const queue = instrumented({ queueRef: assert.fail, enqueueFenced: assert.fail });
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation, queue }), /no snapshot descriptor/);
    assert.equal(queue.calls.sends.length, 0);
    assert.equal(readAttempt(state, ID).pull, undefined);
  });

  test('an intent that cannot be recorded stops the enqueue before anything is sent', async () => {
    const { state } = await preparedTicket();
    const queue = instrumented({ queueRef: async () => '1.1', enqueueFenced: assert.fail });
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation: 99, queue }), /intent was not recorded/);
    assert.equal(queue.calls.sends.length, 0);
    assert.equal(readAttempt(state, ID).pull, undefined);
  });

  test('the arguments come from the envelope alone', async () => {
    const { state } = await preparedTicket();
    const args = enqueueArgs(readEnvelope(state, ID));
    assert.deepEqual(Object.keys(args).sort(), ['binding', 'cpuCores', 'header', 'id', 'lane', 'laneKey', 'memoryBytes', 'priority', 'repoKey', 'requiresTags', 'snapshot']);
    assert.equal(args.id, ID);
    assert.deepEqual(args.snapshot, SNAPSHOT);
  });
});
