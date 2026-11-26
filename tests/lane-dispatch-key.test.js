import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createQueueClient, QueueError } from '../src/queue-client.js';
import { assertCanaryFails } from './helpers/mutant-module.js';
import { harnessClient } from './helpers/queue-fixture.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, start, finish, tid, downgradeToV9, waitBlocked } from './lane-dispatch-fixture.js';

/** PKG-438: a caller-supplied dispatch key makes a submission idempotent; the SQL contract, then the queue client's side of it. */

const KEY = 'train:repoA:7:2';
const rowCount = async (w, where = 'true', args = []) => (await (await w.su()).query(`SELECT count(*)::int AS n FROM lane_dispatch.tickets WHERE ${where}`, args)).rows[0].n;
const binding = { protocol: 1 };
const enqueueArgs = (id, expectRef, extra = {}) => ({
  expectRef, id, repoKey: 'repoA', lane: 'test', laneKey: `lk-${id}`, binding, header: { cmd: ['true'] }, snapshot: { key: 'streams/x.gz', sha256: 'x', bytes: 1, blobs: [] },
  requiresTags: [], cpuCores: 1, memoryBytes: 1000, ...extra,
});

/** A submit login holding an uncommitted enqueue, and a second login blocked behind it on the key, so the race is not left to timing. */
async function raceOnKey(w, secondLogin) {
  const first = await w.cluster.client(w.db, logins.s1);
  await first.query('BEGIN');
  await enqueue(first, tid(1), { dispatchKey: KEY });
  const second = await w.cluster.client(w.db, secondLogin);
  const answer = enqueue(second, tid(2), { dispatchKey: KEY }).then((value) => ({ value }), (error) => ({ error }));
  await waitBlocked(w, second.processID);
  await first.query('COMMIT');
  return answer;
}

const GUARDS = {
  'a duplicate submit with the same key returns the same ticket, also when the second call carries a different id': async (w) => {
    const s1 = await w.as(logins.s1);
    const first = await enqueue(s1, tid(1), { dispatchKey: KEY });
    assert.equal(first.id, tid(1));
    assert.deepEqual(await enqueue(s1, tid(1), { dispatchKey: KEY }), first, 'the same id replays');
    assert.deepEqual(await enqueue(s1, tid(2), { dispatchKey: KEY }), first, 'another id still answers with the first ticket');
    assert.equal(await rowCount(w), 1);
    assert.equal((await w.ticket(tid(1))).dispatch_key, KEY);
  },

  'a key held by one of the callers live tickets wins over an id that belongs to an unkeyed ticket': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { dispatchKey: KEY });
    await enqueue(s1, tid(2));
    assert.equal((await enqueue(s1, tid(2), { dispatchKey: KEY })).id, tid(1));
    assert.equal((await w.ticket(tid(2))).dispatch_key, null);
    await assert.rejects(enqueue(s1, tid(2), { dispatchKey: 'train:other' }), (err) => err.code === '23505' && /another dispatch key/.test(err.message));
  },

  'another owner holding the key is refused with its own SQLSTATE, not the id conflict, and inserts nothing': async (w) => {
    const [s1, s2] = [await w.as(logins.s1), await w.as(logins.s2)];
    await enqueue(s1, tid(1), { dispatchKey: KEY });
    await assert.rejects(enqueue(s2, tid(2), { dispatchKey: KEY }), (err) => err.code === 'LK409' && /another owner/.test(err.message));
    assert.equal(await rowCount(w), 1);
    assert.equal(await call(s2, 'ld_ticket_by_key', KEY), null, 'a stranger cannot read the ticket through the key');
  },

  'two concurrent submits with one key produce one ticket, and the loser is answered with the winner': async (w) => {
    const { value, error } = await raceOnKey(w, logins.s1);
    assert.equal(error, undefined, error?.message);
    assert.equal(value.id, tid(1));
    assert.equal(await rowCount(w), 1);
  },

  'a concurrent submit from another owner loses the race with the distinct refusal': async (w) => {
    const { value, error } = await raceOnKey(w, logins.s2);
    assert.equal(value, undefined);
    assert.equal(error?.code, 'LK409');
    assert.equal(await rowCount(w), 1);
  },

  'a submit whose holder keeps vanishing between the violation and the lookup retries up to three times, answers with the holder it then finds, and raises only when the key never settles': async (w) => {
    const holder = await enqueue(await w.as(logins.s1), tid(1), { dispatchKey: KEY });
    // the lookup misses (the early read, then two handler reads), as it does when winners are withdrawn in between, and then sees the holder
    const lookupMisses = async (misses) => {
      await (await w.su()).query(`
        CREATE OR REPLACE FUNCTION lane_dispatch.key_holder(p_dispatch_key text) RETURNS TABLE (id uuid, owner text)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
        DECLARE n int;
        BEGIN
          n := coalesce(nullif(current_setting('lane.kh_calls', true), '')::int, 0) + 1;
          PERFORM set_config('lane.kh_calls', n::text, false);
          IF n > ${misses} THEN
            RETURN QUERY SELECT t.id, t.owner FROM lane_dispatch.tickets t WHERE t.dispatch_key = p_dispatch_key AND t.state <> 'withdrawn';
          END IF;
        END $$`);
      return w.cluster.client(w.db, logins.s1);
    };
    assert.deepEqual(await enqueue(await lookupMisses(3), tid(2), { dispatchKey: KEY }), holder);
    await assert.rejects(enqueue(await lookupMisses(4), tid(3), { dispatchKey: KEY }), (err) => err.code === '23505');
    assert.equal(await rowCount(w), 1);
  },

  'a withdrawn row frees the key for a new ticket': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { dispatchKey: KEY });
    assert.equal(await call(s1, 'ld_withdraw', tid(1)), 'withdrawn');
    const second = await enqueue(s1, tid(2), { dispatchKey: KEY });
    assert.deepEqual([second.id, second.state], [tid(2), 'queued']);
    assert.equal((await call(s1, 'ld_ticket_by_key', KEY)).id, tid(2));
    assert.equal(await rowCount(w), 2);
  },

  'a withdrawn row that is the only holder reads as no ticket by key': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { dispatchKey: KEY });
    assert.equal((await call(s1, 'ld_ticket_by_key', KEY)).id, tid(1));
    await call(s1, 'ld_withdraw', tid(1));
    assert.equal(await call(s1, 'ld_ticket_by_key', KEY), null);
  },

  'a terminal ticket keeps its key, so a resubmit returns that ticket and never runs again': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1), { dispatchKey: KEY });
    const held = await claim(a1);
    await start(a1, held);
    await finish(a1, held);
    const again = await enqueue(s1, tid(2), { dispatchKey: KEY });
    assert.deepEqual([again.id, again.state], [tid(1), 'finished']);
    assert.equal(await rowCount(w), 1);
  },

  'lane.ticket_status_v1 shows the dispatch key, and null for a ticket with none': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { dispatchKey: KEY });
    await enqueue(s1, tid(2));
    const { rows } = await (await w.su()).query('SELECT dispatch_key, ticket_id FROM lane.ticket_status_v1 ORDER BY ticket_id');
    assert.deepEqual(rows, [{ dispatch_key: KEY, ticket_id: tid(1) }, { dispatch_key: null, ticket_id: tid(2) }]);
  },

  'a key outside 1-200 characters of [A-Za-z0-9:._/-] is refused and stores nothing': async (w) => {
    const s1 = await w.as(logins.s1);
    for (const bad of ['', 'has space', 'semi;colon', 'x'.repeat(201), 'ünï']) {
      await assert.rejects(enqueue(s1, tid(1), { dispatchKey: bad }), (err) => err.code === '22023', JSON.stringify(bad));
    }
    assert.equal(await rowCount(w), 0);
    await enqueue(s1, tid(1), { dispatchKey: 'x'.repeat(200) });
    await enqueue(s1, tid(2), { dispatchKey: 'Az09:._/-' });
  },

  'the table refuses a malformed key on its own, whatever function wrote it': async (w) => {
    const su = await w.su();
    await enqueue(await w.as(logins.s1), tid(1));
    await assert.rejects(su.query("UPDATE lane_dispatch.tickets SET dispatch_key = 'has space' WHERE id = $1", [tid(1)]), (err) => err.code === '23514');
  },

  'a call that names no key stores none, and an old call shape resolves to the longer function': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2), { dispatchKey: null });
    assert.deepEqual([(await w.ticket(tid(1))).dispatch_key, (await w.ticket(tid(2))).dispatch_key], [null, null]);
    assert.equal(await (await w.su()).query("SELECT count(*)::int AS n FROM pg_proc WHERE pronamespace = 'lane_dispatch'::regnamespace AND proname = 'ld_enqueue'").then((r) => r.rows[0].n), 1, 'the 18-argument overload is gone, so no call is ambiguous');
  },

  'enqueueFenced carries the key, answers a second id with the first ticket, and ticketByKey finds it': async (w, t, createClient) => {
    const { queue } = harnessClient(t, w, { createClient });
    const ref = await queue.queueRef();
    const first = await queue.enqueueFenced(enqueueArgs(tid(1), ref, { dispatchKey: KEY }));
    assert.deepEqual(await queue.enqueueFenced(enqueueArgs(tid(2), ref, { dispatchKey: KEY, cpuCores: 8, minCpuCores: 4 })), first);
    assert.deepEqual(await queue.ticketByKey(KEY), first);
    assert.equal(await queue.ticketByKey('train:none'), null);
    assert.equal(await rowCount(w), 1);
    assert.equal((await w.ticket(tid(1))).dispatch_key, KEY);
  },

  'against a queue without the dispatch key signature a keyed enqueue is refused with the missing function, never sent without its key, and an unkeyed one still enqueues': async (w, t, createClient) => {
    await downgradeToV9(w);
    const { queue } = harnessClient(t, w, { createClient });
    const ref = await queue.queueRef();
    await assert.rejects(queue.enqueueFenced(enqueueArgs(tid(1), ref, { dispatchKey: KEY })), (err) => err instanceof QueueError && err.code === '42883' && /ld_enqueue_fenced/.test(err.message));
    assert.equal(await rowCount(w), 0, 'nothing was enqueued without the key');
    await assert.doesNotReject(queue.enqueueFenced(enqueueArgs(tid(2), ref)));
    assert.equal(await rowCount(w), 1);
  },
};

const SQL_GUARDS = Object.keys(GUARDS).filter((name) => !name.startsWith('enqueueFenced') && !name.startsWith('against a queue'));

const SQL_CANARIES = [
  ['a duplicate submit with the same key returns the same ticket, also when the second call carries a different id', "WHERE t.dispatch_key = p_dispatch_key AND t.state <> 'withdrawn';", "WHERE false AND t.dispatch_key = p_dispatch_key AND t.state <> 'withdrawn';"],
  ['two concurrent submits with one key produce one ticket, and the loser is answered with the winner', "  EXCEPTION WHEN unique_violation THEN", "  EXCEPTION WHEN no_data_found THEN"],
  ['a key held by one of the callers live tickets wins over an id that belongs to an unkeyed ticket', "    SELECT h.id, h.owner INTO v_held FROM lane_dispatch.key_holder(p_dispatch_key) h;\n    IF FOUND THEN", "    SELECT h.id, h.owner INTO v_held FROM lane_dispatch.key_holder(p_dispatch_key) h;\n    IF false THEN"],
  ['a key held by one of the callers live tickets wins over an id that belongs to an unkeyed ticket', "IF p_dispatch_key IS NOT NULL AND EXISTS (SELECT 1 FROM lane_dispatch.tickets t WHERE t.id = p_id AND t.dispatch_key IS DISTINCT FROM p_dispatch_key) THEN", 'IF false THEN'],
  ['another owner holding the key is refused with its own SQLSTATE, not the id conflict, and inserts nothing', "    IF FOUND THEN\n      IF v_held.owner <> session_user THEN", "    IF FOUND THEN\n      IF false THEN"],
  ['a concurrent submit from another owner loses the race with the distinct refusal', "      END IF;\n      IF v_held.owner <> session_user THEN", "      END IF;\n      IF false THEN"],
  ['a submit whose holder keeps vanishing between the violation and the lookup retries up to three times, answers with the holder it then finds, and raises only when the key never settles', 'IF v_try = 3 THEN RAISE; END IF;', 'RAISE;'],
  ['a withdrawn row frees the key for a new ticket', "WHERE t.dispatch_key = p_dispatch_key AND t.state <> 'withdrawn';", 'WHERE t.dispatch_key = p_dispatch_key;'],
  ['a withdrawn row frees the key for a new ticket', "WHERE dispatch_key IS NOT NULL AND state <> 'withdrawn';", 'WHERE dispatch_key IS NOT NULL;'],
  ['a withdrawn row that is the only holder reads as no ticket by key', "WHERE t.dispatch_key = p_dispatch_key AND t.state <> 'withdrawn';", 'WHERE t.dispatch_key = p_dispatch_key;'],
  ['a terminal ticket keeps its key, so a resubmit returns that ticket and never runs again', "WHERE t.dispatch_key = p_dispatch_key AND t.state <> 'withdrawn';", "WHERE t.dispatch_key = p_dispatch_key AND t.state NOT IN ('withdrawn', 'finished');"],
  ['lane.ticket_status_v1 shows the dispatch key, and null for a ticket with none', "SELECT t.dispatch_key, t.id AS ticket_id,", "SELECT NULL::text AS dispatch_key, t.id AS ticket_id,"],
  ['a key outside 1-200 characters of [A-Za-z0-9:._/-] is refused and stores nothing', "IF p_dispatch_key !~ '^[A-Za-z0-9:._/-]{1,200}$' THEN", 'IF false THEN'],
  ['the table refuses a malformed key on its own, whatever function wrote it', "CHECK (dispatch_key IS NULL OR dispatch_key ~ '^[A-Za-z0-9:._/-]{1,200}$')", ''],
];

const CLIENT_CANARIES = [
  ['enqueueFenced carries the key, answers a second id with the first ticket, and ticketByKey finds it', ['const trailing = dispatchKey !== undefined ?', 'const trailing = false ?']],
  ['against a queue without the dispatch key signature a keyed enqueue is refused with the missing function, never sent without its key, and an unkeyed one still enqueues', ['const trailing = dispatchKey !== undefined ?', 'const trailing = false ?']],
];

describe('dispatch keys on a disposable Postgres', { skip: pgSkipOption, timeout: 900000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) test(name, async (t) => scenario(await World.create(cluster), t, createQueueClient));

  for (const [name, from, to] of SQL_CANARIES) {
    test(`canary: with "${from.trim().slice(0, 60).replaceAll('\n', ' ')}" mutated, "${name}" fails`, async (t) => {
      assert.ok(SQL_GUARDS.includes(name));
      const world = await World.create(cluster, { mutate: [from, to] });
      let failure;
      await GUARDS[name](world, t, createQueueClient).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }

  for (const [name, mutation] of CLIENT_CANARIES) {
    test(`canary: with "${mutation[0]}" removed, "${name}" fails`, async (t) => {
      const world = await World.create(cluster);
      await assertCanaryFails('queue-client.js', mutation, async (load) => GUARDS[name](world, t, (await load('queue-client.js')).createQueueClient));
    });
  }
});
