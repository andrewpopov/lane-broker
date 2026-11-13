import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import {
  startDispatchCluster, World, logins, call, enqueue, claim, start, finish, tid,
} from './lane-dispatch-fixture.js';

const ageClaimRequests = async (w, interval, where = 'true') =>
  (await w.su()).query(`UPDATE lane_dispatch.claim_requests SET created_at = now() - $1::interval WHERE ${where}`, [interval]);

const countRequests = async (w, where = 'true') =>
  Number((await (await w.su()).query(`SELECT count(*) AS n FROM lane_dispatch.claim_requests WHERE ${where}`)).rows[0].n);

async function insertTerminalMappings(w, count) {
  await (await w.su()).query(
    'INSERT INTO lane_dispatch.claim_requests (login_role, req, ticket, epoch, claim_token) SELECT $1, gen_random_uuid(), $3, 1, gen_random_uuid() FROM generate_series(1, $2)',
    [logins.a1, count, tid(1)]);
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

  'ld_reap prunes at most 1000 terminal mappings per tick': async (w) => {
    const [admin, req] = [await w.as(logins.admin), [randomUUID(), randomUUID(), randomUUID()]];
    await mappingsOfEachState(w, req);
    await finishedAgo(w, '25 hours');
    await insertTerminalMappings(w, 1500);
    await ageClaimRequests(w, '400 days');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, `ticket = '${tid(1)}'`), 501, 'a tick prunes 1000 of the 1501');
    await call(admin, 'ld_reap');
    assert.equal(await countRequests(w, `ticket = '${tid(1)}'`), 0);
    assert.equal(await countRequests(w, 'ticket IS NOT NULL'), 2, 'the running and claimed mappings stay');
  },
};

const TERMINAL_PREDICATE = "t.state IN ('finished','cancelled','withdrawn','lost') AND t.finished_at < v_now - interval '24 hours'";
const CANARIES = [
  ['a terminal ticket mapping younger than 24 hours is never touched, so the prune stays off the tickets table scan', "c.created_at < v_now - interval '24 hours' AND t.state", 't.state'],
  ['a claimed mapping of a non-terminal ticket survives pruning at any age', TERMINAL_PREDICATE, 'true'],
  ['a terminal ticket mapping is pruned only after 24 hours', "t.finished_at < v_now - interval '24 hours'", "t.finished_at < v_now - interval '0 hours'"],
  ['a terminal ticket mapping is pruned only after 24 hours', "t.state IN ('finished','cancelled','withdrawn','lost') AND", "false AND"],
  ['ld_reap prunes at most 1000 terminal mappings per tick', 'LIMIT 1000);', 'LIMIT 100000);'],
];

describe('lane_dispatch claim-request pruning on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
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
