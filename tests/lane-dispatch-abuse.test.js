import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, start, tid, waitBlocked, holdLock } from './lane-dispatch-fixture.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const ADMIN_SHUTDOWN = '57P01';

/** A runner or submitter backend the guard will kill: it overrides the role statement_timeout, as any caller may, and survives its own death. */
async function victim(w, login) {
  const c = await w.cluster.client(w.db, login);
  c.on('error', () => {});
  await c.query('SET statement_timeout = 0');
  return c;
}

/** A killed idle backend is only noticed by the client on its next query, and the signal takes a moment: poll until a query is refused. */
async function assertDies(c) {
  for (let i = 0; i < 60; i++) {
    if (await c.query('SELECT 1').then(() => false, () => true)) return;
    await sleep(50);
  }
  assert.fail('the backend survived the guard');
}

const settle = (promise) => promise.then((value) => ({ value }), (error) => ({ error }));

/** The guard timer: one ld_guard call per `everyMs` until `settled` resolves. Returns how long that took, when a tick first killed something, and each tick's kills. */
async function runGuardUntil(admin, settled, { everyMs, maxMs = 15000 }) {
  const t0 = Date.now();
  const kills = [];
  let killedAt = null;
  let done = false;
  settled.then(() => { done = true; });
  while (!done && Date.now() - t0 < maxMs) {
    kills.push(await call(admin, 'ld_guard'));
    if (kills.at(-1) > 0) killedAt ??= Date.now();
    await Promise.race([settled, sleep(everyMs)]);
  }
  assert.ok(done, `the guard did not stop the call within ${maxMs} ms`);
  return { elapsedMs: Date.now() - t0, killedAt, kills };
}

const GUARDS = {
  'the guard spares a transaction younger than 5 s, a superuser and any other login prefix, and kills the rest on a lower threshold': async (w, cluster) => {
    const admin = await w.as(logins.admin);
    const su = await cluster.admin();
    await su.query("DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lane_agent_super') THEN CREATE ROLE lane_agent_super LOGIN SUPERUSER; END IF; END $do$");
    const [agent, submit, superuser, stranger] = [await victim(w, logins.x), await victim(w, logins.s1), await victim(w, 'lane_agent_super'), await cluster.client('postgres', logins.stranger)];
    stranger.on('error', () => {});
    for (const c of [agent, submit, superuser, stranger]) await c.query('BEGIN');
    assert.equal(await call(admin, 'ld_guard'), 0, 'every transaction is younger than 5 s');
    for (const c of [agent, submit]) await c.query('SELECT 1');
    assert.equal(await call(admin, 'ld_guard', '0 seconds'), 2, 'only the lane_agent_ and lane_submit_ logins that are not superusers');
    for (const c of [agent, submit]) await assertDies(c);
    for (const c of [superuser, stranger]) assert.deepEqual((await c.query('SELECT 1 AS one')).rows, [{ one: 1 }]);
  },

  'the guard spares a lane_agent_ backend in another database and still kills one in its own': async (w, cluster) => {
    const admin = await w.as(logins.admin);
    const otherDb = `other_${randomUUID().replaceAll('-', '').slice(0, 10)}`;
    await (await cluster.admin()).query(`CREATE DATABASE ${otherDb}`);
    const [foreign, own] = [await cluster.client(otherDb, logins.x), await victim(w, logins.x)];
    foreign.on('error', () => {});
    await foreign.query('SET idle_in_transaction_session_timeout = 0');
    for (const c of [foreign, own]) await c.query('BEGIN');
    for (const c of [foreign, own]) await c.query('SELECT 1');
    assert.equal(await call(admin, 'ld_guard', '0 seconds'), 1, 'only the backend in this database');
    await assertDies(own);
    assert.deepEqual((await foreign.query('SELECT 1 AS one')).rows, [{ one: 1 }]);
  },

  'the guard kills a pg_sleep(10) once its transaction passes 5 s, and not before': async (w) => {
    const [admin, runner] = [await w.as(logins.admin), await victim(w, logins.a1)];
    const sleeping = settle(runner.query('SELECT pg_sleep(10)'));
    const { elapsedMs, kills } = await runGuardUntil(admin, sleeping, { everyMs: 1000 });
    assert.equal((await sleeping).error?.code, ADMIN_SHUTDOWN, 'terminated by the administrator command');
    assert.ok(elapsedMs >= 5000 && elapsedMs < 8000, `killed after ${elapsedMs} ms`);
    assert.equal(kills.at(-1), 1);
    assert.ok(kills.slice(0, 4).every((k) => k === 0), 'no earlier tick killed it');
  },

  'a pinned claim: BEGIN; ld_claim then SELECT 1 every second holds only its own ticket, other hosts claim past it, until the guard kills it within 7 s': async (w) => {
    const [s1, a1, a2, admin] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2), await w.as(logins.admin)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const pinner = await victim(w, logins.x);
    const beganAt = Date.now();
    const [, claimed] = await pinner.query(`BEGIN; SELECT lane_dispatch.ld_claim('${randomUUID()}', 8, 1000000000, '{}'::text[]) AS v`);
    assert.equal(claimed.rows[0].v.id, tid(1), 'the pinner holds a ticket with an uncommitted claim');

    let pinnerError = null;
    const keepAlive = (async () => {
      while (!pinnerError) {
        await sleep(1000);
        await pinner.query('SELECT 1').catch((err) => { pinnerError = err; });
      }
    })();
    const started = Date.now();
    assert.equal((await claim(a2)).id, tid(2), 'another principal claims the next ticket instead of waiting');
    assert.ok(Date.now() - started < 1500, 'and is not delayed');

    const { kills, killedAt } = await runGuardUntil(admin, keepAlive, { everyMs: 2000 });
    const killedAfterMs = killedAt - beganAt;
    assert.ok(pinnerError, 'the pinner lost its connection');
    assert.ok(killedAfterMs > 5000 && killedAfterMs < 7000, `killed after ${killedAfterMs} ms`);
    assert.equal(kills.at(-1), 1);
    assert.ok(kills.slice(0, -1).every((k) => k === 0));

    const next = await claim(a1);
    assert.equal(next.status, 'claimed', 'the next claim succeeds');
    assert.equal(next.id, tid(1), 'the pinner uncommitted claim was rolled back');
    assert.equal((await w.ticket(tid(1))).epoch, 1);
  },
};

const SEMANTICS = {
  'ld_log under contention: the guard kills a call blocked on the attempts row, the retry of the same seq stores one chunk and counts log_bytes once': async (w) => {
    const [s1, a1, admin, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin), await w.su()];
    await enqueue(s1, tid(1));
    const held = await claim(a1);
    await start(a1, held);
    const hold = await holdLock(w, `SELECT 1 FROM lane_dispatch.attempts WHERE ticket = '${held.id}' AND epoch = ${held.epoch} FOR UPDATE`);
    const log = (c) => call(c, 'ld_log', held.id, held.epoch, held.token, 1, Buffer.from('hello'));

    const runner = await victim(w, logins.a1);
    const blockedCall = settle(log(runner));
    await waitBlocked(w, runner.processID);
    const { elapsedMs } = await runGuardUntil(admin, blockedCall, { everyMs: 1000 });
    assert.equal((await blockedCall).error?.code, ADMIN_SHUTDOWN);
    assert.ok(elapsedMs >= 5000, `killed after ${elapsedMs} ms`);
    await hold.release();
    assert.equal((await su.query('SELECT count(*)::int AS n FROM lane_dispatch.log_chunks')).rows[0].n, 0, 'the killed call stored nothing');

    const retrying = await victim(w, logins.a1);
    assert.equal(await log(retrying), 'ok');
    assert.equal(await log(retrying), 'ok', 'a second retry');
    const { rows } = await su.query('SELECT seq FROM lane_dispatch.log_chunks');
    assert.deepEqual(rows, [{ seq: 1 }]);
    assert.equal(Number((await su.query('SELECT log_bytes FROM lane_dispatch.attempts')).rows[0].log_bytes), 5);
  },
};

const CANARIES = [
  ['the guard spares a transaction younger than 5 s, a superuser and any other login prefix, and kills the rest on a lower threshold',
    'AND clock_timestamp() - a.xact_start > p_max_age', 'AND a.xact_start IS NOT NULL'],
  ['the guard spares a transaction younger than 5 s, a superuser and any other login prefix, and kills the rest on a lower threshold', 'AND NOT r.rolsuper', ''],
  ['the guard spares a transaction younger than 5 s, a superuser and any other login prefix, and kills the rest on a lower threshold',
    "(starts_with(a.usename, 'lane_agent_') OR starts_with(a.usename, 'lane_submit_'))\n       AND NOT r.rolsuper AND a.pid <> pg_backend_pid() AND a.datname = current_database()",
    'true AND NOT r.rolsuper AND a.pid <> pg_backend_pid()'],
  ['the guard spares a lane_agent_ backend in another database and still kills one in its own', ' AND a.datname = current_database()', ''],
  ['the guard kills a pg_sleep(10) once its transaction passes 5 s, and not before', 'AND clock_timestamp() - a.xact_start > p_max_age', 'AND a.xact_start IS NOT NULL'],
];

describe('lane_dispatch backend guard and abuse limits on a disposable Postgres (timed, real seconds)', { skip: pgSkipOption, timeout: 600000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries({ ...GUARDS, ...SEMANTICS })) {
    test(name, async () => scenario(await World.create(cluster), cluster));
  }

  for (const [name, from, to] of CANARIES) {
    test(`canary: with ${from.slice(0, 40).replaceAll('\n', ' ')} removed, "${name}" fails`, async (t) => {
      let failure;
      await GUARDS[name](await World.create(cluster, { mutate: [from, to] }), cluster).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
