import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import {
  startDispatchCluster, World, logins, call, enqueue, claim, start, heartbeat, finish, liftBackoff, tid, simulateRestore, waitBlocked, holdLock,
} from './lane-dispatch-fixture.js';

const NOT_ENTRY_CHECKED = ['ld_entry', 'ld_timeline_ok', 'ld_guard', 'ld_restore_reset'];

/** The first ld_* call after a restore, from a caller with nothing else to do. */
async function settleByFirstCall(w) {
  await simulateRestore(w);
  await call(await w.as(logins.s1), 'ld_get', tid(99));
}

/** t1 claimed by a1 (and started when asked); t2 queued. */
async function heldAndQueued(w, { started = false } = {}) {
  const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
  await enqueue(s1, tid(1));
  await enqueue(s1, tid(2));
  const t1 = await claim(a1);
  if (started) await start(a1, t1);
  return t1;
}

async function expireLease(w, id, secondsAgo) {
  await (await w.su()).query("UPDATE lane_dispatch.tickets SET lease_until = now() - ($2 || ' seconds')::interval WHERE id = $1", [id, secondsAgo]);
}

async function setLastTick(w, secondsAgo) {
  await (await w.su()).query("UPDATE lane_dispatch.cluster SET last_tick = clock_timestamp() - ($1 || ' seconds')::interval", [secondsAgo]);
}

const leaseSecondsAhead = async (w, id) =>
  Number((await (await w.su()).query('SELECT extract(epoch FROM lease_until - clock_timestamp()) AS s FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0].s);

function entryCalls(t1) {
  const id = t1.id;
  const held = [id, t1.epoch, t1.token];
  const snapshot = { key: 'snap', sha256: 'x' };
  return {
    ld_register: [logins.a1, ['v', 1, {}]],
    ld_claim: [logins.a1, [randomUUID(), 8, 1e9, []]],
    ld_start: [logins.a1, held],
    ld_heartbeat: [logins.a1, held],
    ld_finish: [logins.a1, [...held, { kind: 'ok' }]],
    ld_release: [logins.a1, [...held, 'never-started']],
    ld_log: [logins.a1, [...held, 1, Buffer.from('x')]],
    ld_enqueue: [logins.s1, [tid(9), 'repoA', 'test', 'lk9', { protocol: 1 }, { cmd: ['true'] }, snapshot, [], 1, 1000, 0]],
    ld_get: [logins.s1, [id]],
    ld_list_mine: [logins.s1, [null, 10]],
    ld_withdraw: [logins.s1, [id]],
    ld_cancel: [logins.s1, [id]],
    ld_attach_urls: [logins.s1, [id, t1.epoch, { get: 'u' }]],
    ld_log_read: [logins.s1, [id, t1.epoch, 0]],
    ld_log_ack: [logins.s1, [id, t1.epoch, 0]],
    ld_set_principal: [logins.admin, [logins.a2, 'agent', 'a2', ['linux'], ['*'], 1]],
    ld_reap: [logins.admin, []],
    ld_queue_ref: [logins.s1, []],
    ld_enqueue_fenced: [logins.s1, ['0.0', tid(8), 'repoA', 'test', 'lk8', { protocol: 1 }, { cmd: ['true'] }, snapshot, [], 1, 1000, 0]],
    ld_placeable: [logins.s1, ['repoA', []]],
    ld_prune_candidates: [logins.admin, ['1 day', '7 days', null, null, 10]],
    ld_prune: [logins.admin, [[id], '7 days']],
    ld_prune_chunks: [logins.admin, ['1 hour', 10]],
    ld_blobs_live: [logins.admin, ['[]']],
  };
}

const GUARDS = {
  'the timeline restore settles the queue from each ld_* function': async (w, makeWorld) => {
    const su = await w.su();
    const { rows } = await su.query("SELECT proname FROM pg_proc WHERE pronamespace = 'lane_dispatch'::regnamespace AND proname LIKE 'ld\\_%' ORDER BY proname");
    const probe = entryCalls({ id: tid(1), epoch: 1, token: randomUUID() });
    assert.deepEqual(Object.keys(probe).sort(), rows.map((r) => r.proname).filter((n) => !NOT_ENTRY_CHECKED.includes(n)), 'every ld_* function is in the loop');
    for (const name of Object.keys(probe)) {
      const world = await makeWorld();
      const t1 = await heldAndQueued(world);
      await simulateRestore(world);
      const [login, args] = entryCalls(t1)[name];
      await call(await world.as(login), name, ...args).catch(() => {});
      const { rows: [c] } = await (await world.su()).query(
        'SELECT c.incarnation, c.timeline = (SELECT timeline_id FROM pg_control_checkpoint()) AS synced FROM lane_dispatch.cluster c');
      assert.deepEqual(c, { incarnation: 1, synced: true }, `${name}: cluster resynced and incarnation bumped once`);
      assert.equal((await world.ticket(tid(2))).state, 'lost', `${name}: the untouched queued row is settled lost`);
      await w.cluster.closeClients();
    }
  },

  'ld_entry refuses a transaction above READ COMMITTED, so a REPEATABLE READ runner cannot exceed max_claims': async (w) => {
    const s1 = await w.as(logins.s1);
    for (let n = 1; n <= 4; n++) await enqueue(s1, tid(n));
    const [c1, c2] = [await w.cluster.client(w.db, logins.x), await w.cluster.client(w.db, logins.x)];
    await c2.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
    await c2.query('SELECT 1');
    assert.equal((await claim(c1)).status, 'claimed');
    await assert.rejects(claim(c2, { held: [`lk-${tid(1)}`] }), (err) => err.code === '25000' && /READ COMMITTED/.test(err.message));
    await c2.query('ROLLBACK');
    const { rows: [{ n }] } = await (await w.su()).query("SELECT count(*)::int AS n FROM lane_dispatch.tickets WHERE host = 'x' AND state IN ('claimed','running')");
    assert.equal(n, 1, 'max_claims holds');
  },

  'the amnesty gap: after an outage longer than twice the interval ld_reap extends every lease and settles nothing': async (w) => {
    const [admin, a1, a2, s1] = [await w.as(logins.admin), await w.as(logins.a1), await w.as(logins.a2), await w.as(logins.s1)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const claimed = await claim(a1);
    const running = await claim(a2);
    await start(a2, running);
    await expireLease(w, claimed.id, 3600);
    await expireLease(w, running.id, 3600);
    await setLastTick(w, 10);
    const first = await call(admin, 'ld_reap', '120 seconds', '2 hours', '1 second');
    assert.deepEqual(first, { amnesty: true, requeued: 0, cancelled: 0, lost: 0 });
    for (const id of [claimed.id, running.id]) assert.ok(await leaseSecondsAhead(w, id) > 100, 'lease extended by lease_ttl');
    assert.deepEqual([(await w.ticket(claimed.id)).state, (await w.ticket(running.id)).state], ['claimed', 'running']);
    await expireLease(w, claimed.id, 5);
    const second = await call(admin, 'ld_reap', '120 seconds', '2 hours', '1 second');
    assert.deepEqual(second, { amnesty: false, requeued: 1, cancelled: 0, lost: 0 }, 'no gap, so the expired claim is requeued');
    assert.equal((await w.ticket(claimed.id)).state, 'queued');
  },

  'ld_reap requeues a claimed row past its lease, never a running one on expiry alone, and settles lost only after lost_after': async (w) => {
    const [admin, a1, a2, s1] = [await w.as(logins.admin), await w.as(logins.a1), await w.as(logins.a2), await w.as(logins.s1)];
    for (const n of [1, 2, 3]) await enqueue(s1, tid(n));
    const claimed = await claim(a1);
    const cancelling = await claim(a1);
    const running = await claim(a2);
    await start(a2, running);
    await call(s1, 'ld_cancel', cancelling.id);
    await setLastTick(w, 0);
    await expireLease(w, claimed.id, 30);
    await expireLease(w, cancelling.id, 30);
    await expireLease(w, running.id, 600);
    assert.deepEqual(await call(admin, 'ld_reap'), { amnesty: false, requeued: 1, cancelled: 1, lost: 0 });
    const requeued = await w.ticket(claimed.id);
    assert.deepEqual([requeued.state, requeued.epoch, requeued.claim_token, requeued.host, requeued.lease_until], ['queued', claimed.epoch, null, null, null]);
    assert.equal((await w.ticket(cancelling.id)).state, 'cancelled', 'a cancel-requested claim is not requeued into a queue that would never take it');
    assert.equal((await w.ticket(running.id)).state, 'running', 'ten minutes past the lease is still running');
    assert.deepEqual(await claim(a1), { status: 'none' }, 'the requeued row backs off for 5 s');
    await liftBackoff(w);
    assert.equal((await claim(a1)).id, claimed.id, 'the requeued row is claimable again, one epoch later');
    await expireLease(w, running.id, 3600);
    assert.deepEqual(await call(admin, 'ld_reap'), { amnesty: false, requeued: 0, cancelled: 0, lost: 0 }, 'an hour past the lease is inside the 2 h lost_after');
    await expireLease(w, running.id, 7300);
    assert.deepEqual(await call(admin, 'ld_reap'), { amnesty: false, requeued: 0, cancelled: 0, lost: 1 });
    const lost = await w.ticket(running.id);
    assert.deepEqual([lost.state, lost.result, lost.finished_at === null], ['lost', { kind: 'lost', reason: 'runner-lost', author: 'queue', epoch: running.epoch }, false]);
  },

  'ld_log stores a repeated seq once and counts log_bytes once': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    const t1 = await heldAndQueued(w, { started: true });
    const log = (client, seq, text, token = t1.token) => call(client, 'ld_log', t1.id, t1.epoch, token, seq, Buffer.from(text));
    assert.equal(await log(a1, 1, 'hello'), 'ok');
    assert.equal(await log(a1, 1, 'hello'), 'ok', 'a retry of the same seq');
    assert.equal(await log(a1, 2, 'abc'), 'ok');
    assert.equal(await log(a1, 2, 'abc'), 'ok');
    assert.equal(await log(a1, 1, 'ignored: the first write wins'), 'ok');
    assert.equal(await log(a2, 3, 'x'), 'fenced', 'another host');
    assert.equal(await log(a1, 3, 'x', randomUUID()), 'fenced', 'a stale token');
    const su = await w.su();
    const { rows: chunks } = await su.query('SELECT seq, convert_from(bytes, \'UTF8\') AS text FROM lane_dispatch.log_chunks ORDER BY seq');
    assert.deepEqual(chunks, [{ seq: 1, text: 'hello' }, { seq: 2, text: 'abc' }]);
    assert.equal(Number((await su.query('SELECT log_bytes FROM lane_dispatch.attempts')).rows[0].log_bytes), 8);
  },

  'ld_log caps one call at 256 KiB': async (w) => {
    const a1 = await w.as(logins.a1);
    const t1 = await heldAndQueued(w, { started: true });
    const log = (seq, size) => call(a1, 'ld_log', t1.id, t1.epoch, t1.token, seq, Buffer.alloc(size, 97));
    assert.equal(await log(1, 262144), 'ok');
    assert.equal(await log(2, 262145), 'capped');
    const su = await w.su();
    assert.deepEqual((await su.query('SELECT seq FROM lane_dispatch.log_chunks')).rows, [{ seq: 1 }], 'a capped chunk is not stored');
    assert.equal(Number((await su.query('SELECT log_bytes FROM lane_dispatch.attempts')).rows[0].log_bytes), 262144);
  },

  'ld_log caps one attempt at 64 MiB': async (w) => {
    const a1 = await w.as(logins.a1);
    const t1 = await heldAndQueued(w, { started: true });
    const su = await w.su();
    await su.query('UPDATE lane_dispatch.attempts SET log_bytes = 67108864 - 10');
    const log = (seq, size) => call(a1, 'ld_log', t1.id, t1.epoch, t1.token, seq, Buffer.alloc(size, 97));
    assert.equal(await log(1, 10), 'ok', 'exactly at the cap');
    assert.equal(await log(2, 1), 'capped', 'one byte over');
    assert.equal(Number((await su.query('SELECT log_bytes FROM lane_dispatch.attempts')).rows[0].log_bytes), 67108864);
    assert.deepEqual((await su.query('SELECT seq FROM lane_dispatch.log_chunks')).rows, [{ seq: 1 }]);
  },

  'ld_log caps the whole table at 1 GiB': async (w) => {
    const a1 = await w.as(logins.a1);
    const t1 = await heldAndQueued(w, { started: true });
    const su = await w.su();
    await su.query(
      "INSERT INTO lane_dispatch.log_chunks SELECT gen_random_uuid(), 1, 1, convert_to(repeat('a', 268435000), 'UTF8') FROM generate_series(1, 4)");
    const log = (seq, size) => call(a1, 'ld_log', t1.id, t1.epoch, t1.token, seq, Buffer.alloc(size, 97));
    assert.equal(await log(1, 2000), 'capped', 'the table holds 1,824 bytes of room');
    assert.equal(await log(1, 1000), 'ok');
    assert.equal(Number((await su.query('SELECT log_bytes FROM lane_dispatch.attempts WHERE ticket = $1', [t1.id])).rows[0].log_bytes), 1000);
  },

  'a restore settles every non-terminal ticket lost with a queue-authored result and a finish time, keeps terminal rows, and clears every claim request': async (w) => {
    const [a1, a2, s1] = [await w.as(logins.a1), await w.as(logins.a2), await w.as(logins.s1)];
    for (const n of [1, 2, 3, 4]) await enqueue(s1, tid(n));
    const finished = await claim(a1);
    await start(a1, finished);
    await finish(a1, finished);
    const running = await claim(a2);
    await start(a2, running);
    const claimed = await claim(a1);
    await call(s1, 'ld_withdraw', tid(4));
    assert.ok((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.claim_requests')).rows[0].n > 0);
    await settleByFirstCall(w);
    const rows = await Promise.all([1, 2, 3, 4].map((n) => w.ticket(tid(n))));
    assert.deepEqual(rows.map((r) => r.state), ['finished', 'lost', 'lost', 'withdrawn']);
    for (const [row, held] of [[rows[1], running], [rows[2], claimed]]) {
      assert.deepEqual(row.result, { kind: 'lost', reason: 'queue-restored', author: 'queue', epoch: held.epoch });
      assert.notEqual(row.finished_at, null);
    }
    assert.deepEqual(rows[0].result, { kind: 'ok', exitCode: 0, epoch: finished.epoch }, 'a finished result is never rewritten');
    assert.equal((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.claim_requests')).rows[0].n, 0);
    assert.equal((await heartbeat(a2, running)).fenced, true, 'the runner learns on its next heartbeat');
    assert.equal(await start(a1, claimed), false);
    assert.equal((await finish(a2, running)).fenced, true);
  },

  'ld_restore_reset settles every non-terminal ticket lost on an unchanged timeline, bumps the incarnation and clears claim requests; only the admin may call it': async (w) => {
    const [a1, s1, admin] = [await w.as(logins.a1), await w.as(logins.s1), await w.as(logins.admin)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const held = await claim(a1);
    const ref = await call(s1, 'ld_queue_ref');
    await assert.rejects(call(a1, 'ld_restore_reset'), /permission denied for function/);
    await assert.rejects(call(s1, 'ld_restore_reset'), /permission denied for function/);
    assert.equal((await w.ticket(tid(2))).state, 'queued', 'a refused caller changes nothing');
    assert.equal(await call(admin, 'ld_restore_reset'), 2);
    const rows = await Promise.all([1, 2].map((n) => w.ticket(tid(n))));
    assert.deepEqual(rows.map((r) => r.state), ['lost', 'lost']);
    assert.deepEqual(rows[0].result, { kind: 'lost', reason: 'queue-restored', author: 'queue', epoch: held.epoch });
    assert.notEqual(rows[1].finished_at, null);
    assert.notEqual(await call(s1, 'ld_queue_ref'), ref, 'submitters holding the old queue ref are stale');
    assert.equal((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.claim_requests')).rows[0].n, 0);
    assert.equal(await call(admin, 'ld_restore_reset'), 0, 'nothing left to settle');
  },

  'ld_start racing the restore: either order ends lost with the same epoch, token and host': async (w) => {
    const [a1, s1] = [await w.as(logins.a1), await w.as(logins.s1)];
    const t1 = await heldAndQueued(w);
    const rowLock = await holdLock(w, `SELECT 1 FROM lane_dispatch.tickets WHERE id = '${t1.id}' FOR UPDATE`);
    const started = start(a1, t1);
    started.catch(() => {});
    await waitBlocked(w, a1.processID);
    await simulateRestore(w);
    const settler = call(s1, 'ld_get', tid(99));
    await waitBlocked(w, s1.processID);
    await rowLock.release();
    assert.equal(await started, true, 'ld_start got the row first');
    await settler;
    const row = await w.ticket(t1.id);
    assert.deepEqual([row.state, row.epoch, row.claim_token, row.host], ['lost', t1.epoch, t1.token, 'a1']);
    assert.equal(await start(a1, t1), false, 'a lost row never starts');
  },

  'concurrent callers that both find the restore settle once: one incarnation bump': async (w) => {
    const [s1, s2] = [await w.as(logins.s1), await w.as(logins.s2)];
    await heldAndQueued(w);
    await simulateRestore(w);
    const lock = await holdLock(w, 'SELECT 1 FROM lane_dispatch.cluster WHERE id = 1 FOR UPDATE');
    const calls = [call(s1, 'ld_get', tid(99)), call(s2, 'ld_get', tid(99))];
    await waitBlocked(w, s1.processID);
    await waitBlocked(w, s2.processID);
    await lock.release();
    await Promise.all(calls);
    assert.equal((await (await w.su()).query('SELECT incarnation FROM lane_dispatch.cluster')).rows[0].incarnation, 1);
  },
};

const CANARIES = [
  ['the timeline restore settles the queue from each ld_* function', 'PERFORM lane_dispatch.ld_timeline_ok();', 'NULL;'],
  ['the amnesty gap: after an outage longer than twice the interval ld_reap extends every lease and settles nothing', 'IF v_amnesty THEN', 'IF false THEN'],
  ['ld_reap requeues a claimed row past its lease, never a running one on expiry alone, and settles lost only after lost_after',
    "WHERE t.state = 'claimed' AND t.lease_until < v_now ORDER BY t.id FOR UPDATE LOOP", "WHERE t.state IN ('claimed','running') AND t.lease_until < v_now ORDER BY t.id FOR UPDATE LOOP"],
  ["ld_entry refuses a transaction above READ COMMITTED, so a REPEATABLE READ runner cannot exceed max_claims",
    "RAISE EXCEPTION 'lane_dispatch must run at READ COMMITTED, not %', current_setting('transaction_isolation') USING ERRCODE = '25000';", "NULL;"],
  ['ld_log stores a repeated seq once and counts log_bytes once', 'ON CONFLICT (ticket, epoch, seq) DO NOTHING;', ';'],
  ['ld_log caps one attempt at 64 MiB', 'IF v_logged + v_len > 67108864 THEN', 'IF false THEN'],
  ['ld_log caps one call at 256 KiB', 'IF v_len > 262144 THEN', 'IF false THEN'],
  ['ld_log caps the whole table at 1 GiB', '+ v_len > 1073741824 THEN', '+ v_len > 1073741824000 THEN'],
  ['a restore settles every non-terminal ticket lost with a queue-authored result and a finish time, keeps terminal rows, and clears every claim request',
    "   WHERE state NOT IN ('finished','cancelled','withdrawn','lost');\n  GET DIAGNOSTICS v_lost", "   WHERE false;\n  GET DIAGNOSTICS v_lost"],
  ['a restore settles every non-terminal ticket lost with a queue-authored result and a finish time, keeps terminal rows, and clears every claim request',
    "DELETE FROM lane_dispatch.claim_requests;\n  RETURN v_lost;", "RETURN v_lost;"],
  ['a restore settles every non-terminal ticket lost with a queue-authored result and a finish time, keeps terminal rows, and clears every claim request',
    "p_row_state IN ('claimed','running')", "p_row_state IN ('claimed','running','lost')"],
  ['a restore settles every non-terminal ticket lost with a queue-authored result and a finish time, keeps terminal rows, and clears every claim request',
    "lane_dispatch.lost_result('queue-restored', epoch)", "lane_dispatch.lost_result('restored', epoch)"],
  ['ld_restore_reset settles every non-terminal ticket lost on an unchanged timeline, bumps the incarnation and clears claim requests; only the admin may call it',
    "RETURN lane_dispatch.settle_restore((SELECT c.timeline_id FROM pg_control_checkpoint() c));", "RETURN 0;"],
  ['ld_restore_reset settles every non-terminal ticket lost on an unchanged timeline, bumps the incarnation and clears claim requests; only the admin may call it',
    "lane_dispatch.ld_restore_reset(), lane_dispatch.ld_guard(interval)", "lane_dispatch.ld_guard(interval)"],
  ['ld_start racing the restore: either order ends lost with the same epoch, token and host',
    "   WHERE state NOT IN ('finished','cancelled','withdrawn','lost');\n  GET DIAGNOSTICS v_lost", "   WHERE state = 'queued';\n  GET DIAGNOSTICS v_lost"],
  ['concurrent callers that both find the restore settle once: one incarnation bump',
    "IF v_timeline <> (SELECT c.timeline FROM lane_dispatch.cluster c WHERE c.id = 1) THEN\n    PERFORM lane_dispatch.settle_restore(v_timeline);\n  END IF;", "PERFORM lane_dispatch.settle_restore(v_timeline);"],
];

describe('lane_dispatch recovery on a disposable Postgres', { skip: pgSkipOption, timeout: 600000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) {
    test(name, async () => scenario(await World.create(cluster), () => World.create(cluster)));
  }

  for (const [name, from, to] of CANARIES) {
    test(`canary: with the guard removed, "${name}" fails`, async (t) => {
      const makeWorld = () => World.create(cluster, { mutate: [from, to] });
      let failure;
      await GUARDS[name](await makeWorld(), makeWorld).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
