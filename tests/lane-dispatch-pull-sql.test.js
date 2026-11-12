import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import {
  startDispatchCluster, World, logins, call, enqueue, claim, start, finish, reassert, asItem, simulateRestore, tid,
} from './lane-dispatch-fixture.js';

async function quarantineByFirstCall(w) {
  await simulateRestore(w);
  await call(await w.as(logins.s1), 'ld_get', tid(99));
}

const ageClaimRequests = async (w, interval, where = 'true') =>
  (await w.su()).query(`UPDATE lane_dispatch.claim_requests SET created_at = now() - $1::interval WHERE ${where}`, [interval]);

const countRequests = async (w, where = 'true') =>
  Number((await (await w.su()).query(`SELECT count(*) AS n FROM lane_dispatch.claim_requests WHERE ${where}`)).rows[0].n);

async function insertNoneRows(w, count, age) {
  await (await w.su()).query(
    "INSERT INTO lane_dispatch.claim_requests (login_role, req, created_at) SELECT $1, gen_random_uuid(), now() - $3::interval FROM generate_series(1, $2)",
    [logins.a1, count, age]);
}

/** A claimed mapping per state: t1 finished, t2 running, t3 claimed, each with the given request id. */
async function mappingsOfEachState(w, reqs) {
  const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
  for (const n of [1, 2, 3]) await enqueue(s1, tid(n));
  const t1 = await claim(a1, { req: reqs[0] });
  await finish(a1, t1);
  const t2 = await claim(a1, { req: reqs[1] });
  await start(a1, t2);
  await claim(a1, { req: reqs[2] });
}

const finishedAgo = async (w, interval) =>
  (await w.su()).query('UPDATE lane_dispatch.tickets SET finished_at = now() - $1::interval WHERE id = $2', [interval, tid(1)]);

const GUARDS = {
  'ld_finish on a quarantined row answers reassert and changes nothing': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    await enqueue(await w.as(logins.s1), tid(1));
    const got = await claim(a1);
    await start(a1, got);
    await quarantineByFirstCall(w);
    const before = await w.ticket(tid(1));
    assert.deepEqual(await finish(a1, got), { fenced: false, reassert: true, state: 'quarantined' });
    assert.deepEqual(await w.ticket(tid(1)), before);
    assert.deepEqual(await finish(a2, got), { fenced: true, state: 'quarantined' }, 'another host is still fenced');
    assert.deepEqual(await reassert(a1, [asItem(got)]), [{ id: tid(1), result: 'ok' }]);
    assert.deepEqual(await finish(a1, got), { fenced: false, state: 'finished' }, 'finish lands once the running reassert lifted quarantine');
  },

  'a pre-start reassert of a matching running row leaves it running': async (w) => {
    const a1 = await w.as(logins.a1);
    await enqueue(await w.as(logins.s1), tid(1));
    const got = await claim(a1);
    await start(a1, got);
    await (await w.su()).query("UPDATE lane_dispatch.tickets SET lease_until = now() + interval '5 seconds' WHERE id = $1", [tid(1)]);
    assert.deepEqual(await reassert(a1, [asItem(got, 'pre-start')]), [{ id: tid(1), result: 'ok', state: 'running' }]);
    const row = await w.ticket(tid(1));
    assert.equal(row.state, 'running');
    assert.ok(row.lease_until - new Date() > 100000, 'the lease is extended');
  },

  'a claimed mapping of a non-terminal ticket survives pruning at any age': async (w) => {
    const [admin, req] = [await w.as(logins.admin), [randomUUID(), randomUUID(), randomUUID()]];
    await mappingsOfEachState(w, req);
    await finishedAgo(w, '400 days');
    await ageClaimRequests(w, '400 days');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, `req IN ('${req[1]}', '${req[2]}')`), 2, 'running and claimed mappings stay');
  },

  'a terminal ticket mapping is pruned only after 24 hours': async (w) => {
    const [admin, req] = [await w.as(logins.admin), [randomUUID(), randomUUID(), randomUUID()]];
    await mappingsOfEachState(w, req);
    await ageClaimRequests(w, '400 days');
    await finishedAgo(w, '23 hours');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, `req = '${req[0]}'`), 1, 'finished 23 hours ago');
    await finishedAgo(w, '25 hours');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, `req = '${req[0]}'`), 0, 'finished 25 hours ago');
  },

  'a terminal ticket mapping younger than 24 hours is never touched, so the prune stays off the tickets table scan': async (w) => {
    const [admin, req] = [await w.as(logins.admin), [randomUUID(), randomUUID(), randomUUID()]];
    await mappingsOfEachState(w, req);
    await finishedAgo(w, '25 hours');
    await ageClaimRequests(w, '23 hours');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, `req = '${req[0]}'`), 1);
  },

  'a running reassert of a claimed row with cancel_requested leaves it claimed, and ld_start still refuses': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await call(s1, 'ld_cancel', tid(1));
    const before = await w.ticket(tid(1));
    assert.deepEqual(await reassert(a1, [asItem(got)]), [{ id: tid(1), result: 'ok', state: 'claimed' }]);
    const after = await w.ticket(tid(1));
    assert.equal(after.state, 'claimed');
    assert.equal(after.cancel_requested, true);
    assert.equal(after.started_at, before.started_at);
    assert.equal(await start(a1, got), false);
  },

  'ld_reap prunes none rows older than 10 minutes and keeps fresher ones': async (w) => {
    const admin = await w.as(logins.admin);
    await insertNoneRows(w, 3, '11 minutes');
    await insertNoneRows(w, 2, '9 minutes');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w), 2);
  },

  'ld_reap prunes at most 1000 rows per tick across both predicates': async (w) => {
    const [admin, req] = [await w.as(logins.admin), [randomUUID(), randomUUID(), randomUUID()]];
    await mappingsOfEachState(w, req);
    await finishedAgo(w, '25 hours');
    await ageClaimRequests(w, '400 days');
    await insertNoneRows(w, 1500, '1 hour');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, 'ticket IS NULL'), 500, 'the none rows take the whole budget');
    assert.equal(await countRequests(w, 'ticket IS NOT NULL'), 3, 'no budget is left for the terminal mapping');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, 'ticket IS NULL'), 0);
    assert.equal(await countRequests(w, 'ticket IS NOT NULL'), 2, 'the leftover budget prunes the terminal mapping');
  },
};

const TERMINAL_PREDICATE = "t.state IN ('finished','cancelled','withdrawn','lost') AND t.finished_at < v_now - interval '24 hours'";
const CANARIES = [
  ['a running reassert of a claimed row with cancel_requested leaves it claimed, and ld_start still refuses',
    "UPDATE lane_dispatch.tickets SET lease_until = v_now + interval '120 seconds' WHERE id = v_id;\n        v_extra", "UPDATE lane_dispatch.tickets SET state = v_state, lease_until = v_now + interval '120 seconds' WHERE id = v_id;\n        v_extra"],
  ['a terminal ticket mapping younger than 24 hours is never touched, so the prune stays off the tickets table scan', "c.created_at < v_now - interval '24 hours' AND t.state", 't.state'],
  ['ld_finish on a quarantined row answers reassert and changes nothing',
    "    IF v_t.state = 'quarantined' THEN\n      RETURN jsonb_build_object('fenced', false, 'reassert', true, 'state', 'quarantined');\n    END IF;\n", ''],
  ['a pre-start reassert of a matching running row leaves it running',
    "      IF v_t.state = 'quarantined' THEN\n        UPDATE lane_dispatch.tickets SET state = v_state", "      IF true THEN\n        UPDATE lane_dispatch.tickets SET state = v_state"],
  ['a claimed mapping of a non-terminal ticket survives pruning at any age', TERMINAL_PREDICATE, 'true'],
  ['a terminal ticket mapping is pruned only after 24 hours', "t.finished_at < v_now - interval '24 hours'", "t.finished_at < v_now - interval '0 hours'"],
  ['a terminal ticket mapping is pruned only after 24 hours', "t.state IN ('finished','cancelled','withdrawn','lost') AND", "false AND"],
  ['ld_reap prunes none rows older than 10 minutes and keeps fresher ones', "c.ticket IS NULL AND c.created_at", 'false AND c.created_at'],
  ['ld_reap prunes none rows older than 10 minutes and keeps fresher ones', "interval '10 minutes'", "interval '0 minutes'"],
  ['ld_reap prunes at most 1000 rows per tick across both predicates', 'LIMIT 1000);', 'LIMIT 100000);'],
  ['ld_reap prunes at most 1000 rows per tick across both predicates', 'LIMIT 1000 - v_pruned', 'LIMIT 100000'],
];

describe('lane_dispatch finish, reassert and pruning on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) {
    test(name, async () => scenario(await World.create(cluster)));
  }

  for (const [name, from, to] of CANARIES) {
    test(`canary: with the guard removed, "${name}" fails (${from.slice(0, 40).replaceAll('\n', ' ')})`, async (t) => {
      const world = await World.create(cluster, { mutate: [from, to] });
      let failure;
      await GUARDS[name](world).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
