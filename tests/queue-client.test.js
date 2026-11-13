import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { createQueueClient, QueueError } from '../src/queue-client.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, claim, start, tid } from './lane-dispatch-fixture.js';
import { CA_TEXT, QUEUE_URL, harnessClient, secretFiles } from './helpers/queue-fixture.js';

/** BRAIN-1036 R4: the submitter queue client against a disposable Postgres. */

const binding = { protocol: 1 };
const snapshot = { key: 'streams/x.gz', sha256: 'x', bytes: 1, blobs: [] };
const enqueueArgs = (id, expectRef) => ({
  expectRef, id, repoKey: 'repoA', lane: 'test', laneKey: `lk-${id}`, binding, header: { cmd: ['true'] }, snapshot,
  requiresTags: [], cpuCores: 1, memoryBytes: 1000,
});

const terminateBackend = async (world, client) => {
  await (await world.su()).query('SELECT pg_terminate_backend($1)', [client.processID]);
  await new Promise((resolve) => setTimeout(resolve, 100));
};

const holdConnections = (world, n) => Promise.all(Array.from({ length: n }, () => world.cluster.client(world.db, logins.s1)));

const SCENARIOS = {
  'a svc: or pg-primary host, a second host and another database are refused at construction': async (_w, t) => {
    const files = secretFiles(t);
    for (const [bad, pattern] of [
      ['postgres://lane_submit_s1@svc:pg-primary:5432/lane_queue', /svc:/],
      ['postgres://lane_submit_s1@pg-primary.tailnet.ts.net:5432/lane_queue', /pg-primary/],
      ['postgres://lane_submit_s1@a:5432,b:5432/lane_queue', /single|one host|valid/],
      ['postgres://lane_submit_s1@queue.example:5432/other_db', /lane_queue/],
    ]) {
      assert.throws(() => createQueueClient({ url: bad, ...files }), (err) => err instanceof QueueError && pattern.test(err.message), bad);
    }
  },

  'every connection uses the password file, TLS with the configured CA, and the URL endpoint': async (w, t) => {
    const { queue, seen } = harnessClient(t, w);
    await queue.queueRef();
    const [config] = seen.configs;
    assert.equal(config.password, 'queue-pass');
    assert.deepEqual(config.ssl, { ca: CA_TEXT, rejectUnauthorized: true });
    assert.deepEqual([config.host, config.port, config.user, config.database], ['queue.example', '5432', 'lane_submit_s1', 'lane_queue']);
    assert.ok(config.connectionTimeoutMillis > 0 && config.query_timeout > 0, 'every connection and query has a deadline');
  },

  'a missing or empty password file is a QueueError, never a connection attempt': async (w, t) => {
    const files = secretFiles(t, '\n');
    const queue = createQueueClient({ url: QUEUE_URL, ...files, connectClient: async () => assert.fail('connected without a password') });
    await assert.rejects(queue.queueRef(), (err) => err instanceof QueueError && /password file .* is empty/.test(err.message));
    fs.rmSync(files.passwordFile);
    await assert.rejects(queue.queueRef(), (err) => err instanceof QueueError && /unreadable/.test(err.message));
  },

  'the wrappers return what the SQL returns': async (w, t) => {
    const { queue } = harnessClient(t, w);
    const a1 = await w.as(logins.a1);
    const ref = await queue.queueRef();
    assert.match(ref, /^\d+\.\d+$/);

    assert.deepEqual(await queue.enqueueFenced(enqueueArgs(tid(9), 'stale')), { state: 'stale-queue-ref', queue_ref: ref });
    const queued = await queue.enqueueFenced(enqueueArgs(tid(1), ref));
    assert.deepEqual(Object.keys(queued).sort(), [
      'attempt_ref', 'cancel_requested', 'created_at', 'epoch', 'execution_id', 'finished_at', 'host', 'id', 'incarnation', 'lease_until', 'result', 'started_at', 'state', 'urls',
    ]);
    assert.equal(queued.state, 'queued');
    assert.deepEqual(await queue.get(tid(1)), queued);
    assert.equal(await queue.get(tid(404)), null);
    await call(await w.as(logins.s2), 'ld_enqueue', tid(2), 'repoB', 'test', 'lk-2', binding, {}, snapshot, [], 1, 1000, 0);
    assert.equal(await queue.get(tid(2)), null, 'another submitter\'s ticket is not visible');
    assert.deepEqual((await queue.listMine()).map((row) => row.id), [tid(1)]);
    assert.deepEqual(await queue.listMine({ states: ['running'] }), []);

    assert.equal(await queue.attachUrls(tid(1), 0, { v: 1 }), false, 'a queued row takes no urls');
    const claimed = await claim(a1);
    assert.equal(claimed.id, tid(1));
    assert.equal(await queue.attachUrls(tid(1), claimed.epoch, { v: 1, epoch: claimed.epoch }), true);
    assert.deepEqual((await queue.get(tid(1))).urls, { v: 1, epoch: claimed.epoch });
    await start(a1, claimed);
    for (const seq of [0, 1, 2]) await call(a1, 'ld_log', tid(1), claimed.epoch, claimed.token, seq, Buffer.from(`line ${seq}\n`));
    const chunks = await queue.logRead(tid(1), claimed.epoch, 0);
    assert.deepEqual(chunks, [{ seq: 1, data: Buffer.from('line 1\n').toString('base64') }, { seq: 2, data: Buffer.from('line 2\n').toString('base64') }]);
    assert.equal(await queue.logRead(tid(404), claimed.epoch, -1), null);
    assert.equal(await queue.logAck(tid(1), claimed.epoch, 1), 2);

    assert.deepEqual(await queue.cancel(tid(1)), { state: 'running', cancel_requested: true, confirmed: false });
    assert.deepEqual(await queue.cancel(tid(404)), { state: 'missing', cancel_requested: false, confirmed: false });
    await queue.enqueueFenced(enqueueArgs(tid(3), ref));
    assert.equal(await queue.withdraw(tid(3)), 'withdrawn');
    assert.equal(await queue.withdraw(tid(3)), 'withdrawn');
    assert.equal(await queue.withdraw(tid(404)), 'missing');
    assert.equal(await queue.placeable('repoA', []), 3);
    assert.equal(await queue.placeable('repoA', ['fast']), 1);
    assert.equal(await queue.placeable('nowhere', ['fast']), 0);
  },

  'too many connections (53300) is retried with jitter for every call, then surfaces when it never clears': async (w, t) => {
    const held = await holdConnections(w, 3);
    const release = async () => { while (held.length) await held.pop().end(); };
    const { queue, seen } = harnessClient(t, w, { sleep: release, random: () => 0.5 });
    assert.equal(typeof (await queue.cancel(tid(404))).state, 'string', 'a non-idempotent call retries 53300: nothing was sent');
    assert.equal(seen.sleeps.length, 1);
    assert.ok(seen.sleeps[0] > 0 && seen.sleeps[0] < 2000, `jittered delay, got ${seen.sleeps[0]}`);

    await holdConnections(w, 3);
    const never = harnessClient(t, w, { random: () => 0.5 });
    await assert.rejects(never.queue.queueRef(), (err) => err instanceof QueueError && err.code === '53300');
    assert.equal(never.seen.sleeps.length, 5, 'six attempts, five waits');
  },

  'a guard kill is retried for an idempotent call and thrown for a non-idempotent one': async (w, t) => {
    const killFirst = (world) => (client, n) => (n === 1 ? terminateBackend(world, client) : undefined);
    const idempotent = harnessClient(t, w, { onConnect: killFirst(w) });
    assert.match(await idempotent.queue.queueRef(), /^\d+\.\d+$/);
    assert.equal(idempotent.seen.configs.length, 2);

    const writing = harnessClient(t, w, { onConnect: killFirst(w) });
    await assert.rejects(writing.queue.cancel(tid(404)), (err) => err instanceof QueueError);
    assert.equal(writing.seen.configs.length, 1, 'an enqueue, withdraw or cancel is never sent twice by the client');
    const enqueuing = harnessClient(t, w, { onConnect: killFirst(w) });
    await assert.rejects(enqueuing.queue.enqueueFenced(enqueueArgs(tid(5), 'ref')), (err) => err instanceof QueueError);
    assert.equal(enqueuing.seen.configs.length, 1);
  },

  'a server error is thrown with its SQLSTATE and is not retried': async (w, t) => {
    const { queue, seen } = harnessClient(t, w, { login: logins.x });
    await assert.rejects(queue.get(tid(1)), (err) => err instanceof QueueError && err.code === '42501');
    assert.equal(seen.configs.length, 1);
  },
};

describe('queue client against a disposable Postgres', { skip: pgSkipOption, timeout: 600000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    test(name, async (t) => scenario(await World.create(cluster), t));
  }
});
