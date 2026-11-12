import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import {
  startDispatchCluster, World, logins, call, enqueue, claim, start, heartbeat, finish, tid, reassert, asItem, simulateRestore, waitBlocked, holdLock,
} from './lane-dispatch-fixture.js';

const NOT_ENTRY_CHECKED = ['ld_entry', 'ld_timeline_ok', 'ld_guard'];

/** The first ld_* call after a restore, from a caller with nothing else to do. */
async function quarantineByFirstCall(w) {
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

const claimLock = (w) => holdLock(w, 'SELECT 1 FROM lane_dispatch.claim_lock WHERE id = 1 FOR UPDATE');

async function expireLease(w, id, secondsAgo) {
  await (await w.su()).query("UPDATE lane_dispatch.tickets SET lease_until = now() - ($2 || ' seconds')::interval WHERE id = $1", [id, secondsAgo]);
}

async function setLastTick(w, secondsAgo) {
  await (await w.su()).query("UPDATE lane_dispatch.cluster SET last_tick = clock_timestamp() - ($1 || ' seconds')::interval", [secondsAgo]);
}

const leaseSecondsAhead = async (w, id) =>
  Number((await (await w.su()).query('SELECT extract(epoch FROM lease_until - clock_timestamp()) AS s FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0].s);

async function raceReasserts(w, [loginA, itemA], [loginB, itemB]) {
  const [a, b] = [await w.as(loginA), await w.as(loginB)];
  const lock = await claimLock(w);
  const first = reassert(a, [itemA]);
  await waitBlocked(w, a.processID);
  const second = reassert(b, [itemB]);
  await waitBlocked(w, b.processID);
  await lock.release();
  return Promise.all([first, second]);
}

async function ownerlessRow(w, ticketOpts = {}) {
  await enqueue(await w.as(logins.s1), tid(1), ticketOpts);
  await quarantineByFirstCall(w);
  const row = await w.ticket(tid(1));
  assert.deepEqual([row.state, row.epoch, row.claim_token, row.host], ['quarantined', 0, null, null]);
}

const sameExecution = (row, t) => assert.deepEqual([row.epoch, row.claim_token, row.host], [t.epoch, t.token, 'a1']);

function entryCalls(t1) {
  const id = t1.id;
  const held = [id, t1.epoch, t1.token];
  const snapshot = { key: 'snap', sha256: 'x' };
  return {
    ld_register: [logins.a1, ['v', {}]],
    ld_claim: [logins.a1, [randomUUID(), 8, 1e9, []]],
    ld_start: [logins.a1, held],
    ld_heartbeat: [logins.a1, held],
    ld_finish: [logins.a1, [...held, { kind: 'ok' }]],
    ld_release: [logins.a1, [...held, 'never-started']],
    ld_log: [logins.a1, [...held, 1, Buffer.from('x')]],
    ld_reassert: [logins.a1, ['[]']],
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
    ld_cancel_quarantined: [logins.admin, [id]],
  };
}

const GUARDS = {
  'the timeline quarantine fires from each ld_* function': async (w, makeWorld) => {
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
      assert.equal((await world.ticket(tid(2))).state, 'quarantined', `${name}: the untouched queued row is quarantined`);
      await w.cluster.closeClients();
    }
  },

  'ld_reassert fences another host token, a wrong token, a lower epoch and a terminal row': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    const t1 = await heldAndQueued(w, { started: true });
    const t2 = await claim(a1);
    await start(a1, t2);
    await finish(a1, t2);
    await quarantineByFirstCall(w);
    assert.deepEqual(await reassert(a2, [asItem(t1)]), [{ id: t1.id, result: 'fenced' }], "another host's token");
    assert.deepEqual(await reassert(a1, [{ ...asItem(t1), token: randomUUID() }]), [{ id: t1.id, result: 'fenced' }], 'wrong token');
    assert.deepEqual(await reassert(a1, [{ ...asItem(t1), epoch: t1.epoch - 1 }]), [{ id: t1.id, result: 'fenced' }], 'lower epoch');
    assert.deepEqual(await reassert(a1, [asItem(t2)]), [{ id: t2.id, result: 'fenced' }], 'terminal row');
    const row = await w.ticket(tid(1));
    assert.equal(row.state, 'quarantined');
    sameExecution(row, t1);
    assert.equal((await w.ticket(tid(2))).state, 'finished');
  },

  'a heartbeat never lifts quarantine: the matching runner is told to reassert, the row is unchanged, and a running reassert or ld_finish leaves quarantine': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    const t1 = await heldAndQueued(w, { started: true });
    const t2 = await claim(a1);
    await quarantineByFirstCall(w);
    assert.deepEqual(await heartbeat(a2, t1), { fenced: true, cancel: false, urls: null });
    assert.deepEqual(await heartbeat(a1, { ...t1, token: randomUUID() }), { fenced: true, cancel: false, urls: null });
    const before = await w.ticket(t1.id);
    assert.deepEqual(await heartbeat(a1, t1), { fenced: false, reassert: true, cancel: false, urls: null });
    assert.deepEqual(await w.ticket(t1.id), before, 'a heartbeat changes nothing');
    assert.deepEqual(await reassert(a1, [asItem(t1)]), [{ id: t1.id, result: 'ok' }]);
    assert.equal((await w.ticket(t1.id)).state, 'running');
    assert.ok(await leaseSecondsAhead(w, t1.id) > 100);
    assert.deepEqual(await heartbeat(a1, t1), { fenced: false, cancel: false, urls: null });
    assert.deepEqual(await finish(a1, t2), { fenced: false, state: 'finished' });
  },

  'a quarantined claim that never started is fenced: ld_release, a pre-start reassert and ld_start leave it untouched and unclaimable': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    const t1 = await heldAndQueued(w);
    await quarantineByFirstCall(w);
    const before = await w.ticket(t1.id);
    assert.deepEqual(await call(a1, 'ld_release', t1.id, t1.epoch, t1.token, 'never-started'), { fenced: true, state: 'quarantined' });
    assert.deepEqual(await reassert(a1, [asItem(t1, 'pre-start')]), [{ id: t1.id, result: 'fenced' }]);
    assert.equal(await start(a1, t1), false);
    assert.deepEqual(await w.ticket(t1.id), before);
    assert.deepEqual(await claim(a2), { status: 'none' }, 'no other host can claim it');
  },

  'ld_cancel confirms only a queued row: a quarantined row, owned or ownerless, is never confirmed': async (w) => {
    const s1 = await w.as(logins.s1);
    const t1 = await heldAndQueued(w);
    await quarantineByFirstCall(w);
    assert.deepEqual(await call(s1, 'ld_cancel', t1.id), { state: 'cancelled', cancel_requested: true, confirmed: false }, 'owned');
    assert.deepEqual(await call(s1, 'ld_cancel', tid(2)), { state: 'cancelled', cancel_requested: true, confirmed: false }, 'ownerless');
    await enqueue(s1, tid(4));
    assert.deepEqual(await call(s1, 'ld_cancel', tid(4)), { state: 'cancelled', cancel_requested: true, confirmed: true }, 'a queued row is confirmed');
  },

  'a higher-epoch reassert from another principal, with no restore, is fenced and the owner keeps the row': async (w) => {
    const [a1, x] = [await w.as(logins.a1), await w.as(logins.x)];
    const t1 = await heldAndQueued(w, { started: true });
    assert.deepEqual(await reassert(a1, [asItem(t1)]), [{ id: t1.id, result: 'ok' }]);
    const stolenToken = randomUUID();
    assert.deepEqual(await reassert(x, [{ id: t1.id, epoch: t1.epoch + 1, token: stolenToken, phase: 'running' }]), [{ id: t1.id, result: 'fenced' }]);
    assert.deepEqual(await call(x, 'ld_finish', t1.id, t1.epoch + 1, stolenToken, { kind: 'ok', exitCode: 0 }), { fenced: true, state: 'running' });
    const row = await w.ticket(t1.id);
    assert.equal(row.state, 'running');
    sameExecution(row, t1);
    assert.equal((await heartbeat(a1, t1)).fenced, false);
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

  'two concurrent ld_reassert calls on one row serialise: the matching one is ok, the other is fenced, whatever the order or epochs': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    const [tokenA, tokenB] = [randomUUID(), randomUUID()];
    const item = (epoch, token) => ({ id: tid(1), epoch, token, phase: 'running' });
    const ok = [{ id: tid(1), result: 'ok' }];
    const fenced = [{ id: tid(1), result: 'fenced' }];
    const heartbeats = async (a, b) => [(await call(a1, 'ld_heartbeat', tid(1), a.epoch, a.token)).fenced, (await call(a2, 'ld_heartbeat', tid(1), b.epoch, b.token)).fenced];
    const rollBack = async () => (await w.su()).query("UPDATE lane_dispatch.tickets SET state = 'quarantined', claim_token = NULL, host = NULL, epoch = 0");

    await ownerlessRow(w);
    assert.deepEqual(await raceReasserts(w, [logins.a1, item(3, tokenA)], [logins.a2, item(3, tokenB)]), [ok, fenced], 'equal epochs');
    assert.deepEqual(await heartbeats({ epoch: 3, token: tokenA }, { epoch: 3, token: tokenB }), [false, true]);

    await rollBack();
    assert.deepEqual(await raceReasserts(w, [logins.a1, item(3, tokenA)], [logins.a2, item(5, tokenB)]), [ok, fenced], 'a higher epoch does not replace the first');
    assert.deepEqual(await heartbeats({ epoch: 3, token: tokenA }, { epoch: 5, token: tokenB }), [false, true]);
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.state, row.epoch, row.claim_token, row.host], ['running', 3, tokenA, 'a1']);

    await rollBack();
    assert.deepEqual(await raceReasserts(w, [logins.a2, item(5, tokenB)], [logins.a1, item(3, tokenA)]), [ok, fenced], 'lower epoch second');
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
    const first = await call(admin, 'ld_reap', '1 second', '120 seconds', '30 minutes');
    assert.deepEqual(first, { amnesty: true, requeued: 0, cancelled: 0, lost: 0 });
    for (const id of [claimed.id, running.id]) assert.ok(await leaseSecondsAhead(w, id) > 100, 'lease extended by lease_ttl');
    assert.deepEqual([(await w.ticket(claimed.id)).state, (await w.ticket(running.id)).state], ['claimed', 'running']);
    await expireLease(w, claimed.id, 5);
    const second = await call(admin, 'ld_reap', '1 second', '120 seconds', '30 minutes');
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
    assert.equal((await claim(a1)).id, claimed.id, 'the requeued row is claimable again, one epoch later');
    await expireLease(w, running.id, 3600);
    assert.deepEqual(await call(admin, 'ld_reap'), { amnesty: false, requeued: 0, cancelled: 0, lost: 1 });
    assert.equal((await w.ticket(running.id)).state, 'lost');
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
};

const SEMANTICS = {
  'quarantined rows are never claimed, requeued or settled by elapsed time, and keep their epoch, token and host': async (w) => {
    const [admin, a1, a2, s1] = [await w.as(logins.admin), await w.as(logins.a1), await w.as(logins.a2), await w.as(logins.s1)];
    for (const n of [1, 2, 3]) await enqueue(s1, tid(n));
    const claimed = await claim(a1);
    const running = await claim(a2);
    await start(a2, running);
    await quarantineByFirstCall(w);
    const before = await Promise.all([1, 2, 3].map((n) => w.ticket(tid(n))));
    assert.deepEqual(before.map((r) => r.state), ['quarantined', 'quarantined', 'quarantined']);
    sameExecution(before[0], claimed);
    assert.deepEqual([before[1].epoch, before[1].claim_token, before[1].host], [running.epoch, running.token, 'a2']);
    assert.deepEqual([before[2].epoch, before[2].claim_token, before[2].host], [0, null, null]);
    assert.deepEqual(await claim(a1), { status: 'none' });
    await setLastTick(w, 0);
    for (const n of [1, 2]) await expireLease(w, tid(n), 7200);
    assert.deepEqual(await call(admin, 'ld_reap', '2 seconds', '120 seconds', '1 second'), { amnesty: false, requeued: 0, cancelled: 0, lost: 0 });
    assert.deepEqual((await Promise.all([1, 2, 3].map((n) => w.ticket(tid(n))))).map((r) => r.state), ['quarantined', 'quarantined', 'quarantined']);
  },

  'an owner ld_cancel and the admin ld_cancel_quarantined are terminal for a quarantined row': async (w) => {
    const [admin, s1, s2] = [await w.as(logins.admin), await w.as(logins.s1), await w.as(logins.s2)];
    const t1 = await heldAndQueued(w);
    await enqueue(s1, tid(3));
    await quarantineByFirstCall(w);
    assert.deepEqual(await call(s2, 'ld_cancel', t1.id), { state: 'missing', cancel_requested: false, confirmed: false });
    assert.deepEqual(await call(s1, 'ld_cancel', t1.id), { state: 'cancelled', cancel_requested: true, confirmed: false }, 'its runner may still be running it');
    assert.deepEqual(await call(s1, 'ld_cancel', tid(2)), { state: 'cancelled', cancel_requested: true, confirmed: false }, 'quarantined is never confirmed');
    assert.equal(await call(admin, 'ld_cancel_quarantined', tid(3)), 'cancelled');
    assert.equal(await call(admin, 'ld_cancel_quarantined', tid(3)), 'cancelled', 'a repeat sees the terminal state');
    assert.equal(await call(admin, 'ld_cancel_quarantined', tid(8)), 'missing');
    assert.equal((await w.ticket(tid(3))).state, 'cancelled');
  },

  'ld_reassert adopts the caller own execution: running with the lease extended, and a claim before ld_start only while it is not quarantined': async (w) => {
    const a1 = await w.as(logins.a1);
    const t1 = await heldAndQueued(w, { started: true });
    const t2 = await claim(a1);
    await quarantineByFirstCall(w);
    await expireLease(w, t1.id, 600);
    await expireLease(w, t2.id, 600);
    assert.deepEqual(await reassert(a1, [asItem(t1), asItem(t2, 'pre-start')]), [{ id: t1.id, result: 'ok' }, { id: t2.id, result: 'fenced' }]);
    assert.deepEqual([(await w.ticket(t1.id)).state, (await w.ticket(t2.id)).state], ['running', 'quarantined']);
    assert.ok(await leaseSecondsAhead(w, t1.id) > 100);
    await (await w.su()).query("UPDATE lane_dispatch.tickets SET state = 'claimed' WHERE id = $1", [t2.id]);
    assert.deepEqual(await reassert(a1, [asItem(t2, 'pre-start')]), [{ id: t2.id, result: 'ok' }], 'a live claim before ld_start is adopted');
    assert.equal((await w.ticket(t2.id)).state, 'claimed');
    assert.ok(await leaseSecondsAhead(w, t2.id) > 100);
    await assert.rejects(reassert(a1, [{ ...asItem(t1), phase: 'sleeping' }]), /unknown reassert phase/);
  },

  'ld_reassert of a missing row is unknown': async (w) => {
    const a1 = await w.as(logins.a1);
    assert.deepEqual(await reassert(a1, [{ id: tid(7), epoch: 1, token: randomUUID(), phase: 'running' }]), [{ id: tid(7), result: 'unknown' }]);
  },

  'ld_reassert of an ownerless rolled-back row adopts it for an eligible caller and fences an ineligible one': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { repo: 'repoZ' });
    await enqueue(s1, tid(2), { tags: ['fast'] });
    await quarantineByFirstCall(w);
    const [tokenA, tokenB, tokenC] = [randomUUID(), randomUUID(), randomUUID()];
    const item = (n, epoch, token) => ({ id: tid(n), epoch, token, phase: 'running' });
    assert.deepEqual(await reassert(a1, [item(1, 2, tokenA)]), [{ id: tid(1), result: 'fenced' }], 'repoZ is outside a1 allowed_repos');
    assert.deepEqual(await reassert(a2, [item(2, 2, tokenB)]), [{ id: tid(2), result: 'fenced' }], 'a2 lacks the fast tag');
    assert.deepEqual(await reassert(a2, [item(1, 0, tokenA)]), [{ id: tid(1), result: 'fenced' }], 'the epoch must be higher than the row');
    assert.deepEqual(await reassert(a2, [{ ...item(1, 2, tokenA), phase: 'pre-start' }]), [{ id: tid(1), result: 'fenced' }], 'only a running phase adopts');
    for (const n of [1, 2]) {
      const row = await w.ticket(tid(n));
      assert.deepEqual([row.state, row.epoch, row.claim_token, row.host], ['quarantined', 0, null, null], `row ${n} untouched`);
    }
    assert.deepEqual(await reassert(a2, [item(1, 2, tokenA)]), [{ id: tid(1), result: 'ok' }]);
    assert.deepEqual(await reassert(a1, [item(2, 3, tokenC)]), [{ id: tid(2), result: 'ok' }]);
    const [r1, r2] = [await w.ticket(tid(1)), await w.ticket(tid(2))];
    assert.deepEqual([r1.state, r1.epoch, r1.claim_token, r1.host], ['running', 2, tokenA, 'a2']);
    assert.deepEqual([r2.state, r2.epoch, r2.claim_token, r2.host], ['running', 3, tokenC, 'a1']);
    const { rows } = await (await w.su()).query('SELECT ticket, epoch, host, claim_token FROM lane_dispatch.attempts ORDER BY ticket');
    assert.deepEqual(rows, [{ ticket: tid(1), epoch: 2, host: 'a2', claim_token: tokenA }, { ticket: tid(2), epoch: 3, host: 'a1', claim_token: tokenC }]);
  },

  'a higher epoch never replaces a healthy claimed row that no reassert adopted': async (w) => {
    const [a1, a2] = [await w.as(logins.a1), await w.as(logins.a2)];
    const t1 = await heldAndQueued(w, { started: true });
    assert.deepEqual(await reassert(a2, [{ id: t1.id, epoch: t1.epoch + 5, token: randomUUID(), phase: 'running' }]), [{ id: t1.id, result: 'fenced' }]);
    assert.equal((await heartbeat(a1, t1)).fenced, false);
  },

  'ld_reassert racing the quarantine: either order ends consistent': async (w) => {
    const [a1, s1] = [await w.as(logins.a1), await w.as(logins.s1)];
    const t1 = await heldAndQueued(w, { started: true });
    let lock = await claimLock(w);
    const reasserted = reassert(a1, [asItem(t1)]);
    await waitBlocked(w, a1.processID);
    await simulateRestore(w);
    const quarantiner = call(s1, 'ld_get', tid(99));
    await waitBlocked(w, s1.processID);
    await lock.release();
    assert.deepEqual(await reasserted, [{ id: t1.id, result: 'ok' }]);
    await quarantiner;
    let row = await w.ticket(t1.id);
    assert.equal(row.state, 'quarantined', 'the quarantine ran after the reassert, so it wins');
    sameExecution(row, t1);

    await (await w.su()).query("UPDATE lane_dispatch.tickets SET state = 'running' WHERE id = $1", [t1.id]);
    lock = await claimLock(w);
    await simulateRestore(w);
    const quarantinerFirst = call(s1, 'ld_get', tid(99));
    await waitBlocked(w, s1.processID);
    const reassertedSecond = reassert(a1, [asItem(t1)]);
    await waitBlocked(w, a1.processID);
    await lock.release();
    await quarantinerFirst;
    assert.deepEqual(await reassertedSecond, [{ id: t1.id, result: 'ok' }]);
    row = await w.ticket(t1.id);
    assert.equal(row.state, 'running', 'the reassert ran after the quarantine, so it adopts');
    sameExecution(row, t1);
    const { rows: [c] } = await (await w.su()).query('SELECT incarnation FROM lane_dispatch.cluster');
    assert.equal(c.incarnation, 2, 'one bump per quarantine, none for the call that found it already done');
  },

  'ld_start racing the quarantine: either order ends quarantined with the same epoch, token and host': async (w) => {
    const [a1, s1] = [await w.as(logins.a1), await w.as(logins.s1)];
    const t1 = await heldAndQueued(w);
    let rowLock = await holdLock(w, `SELECT 1 FROM lane_dispatch.tickets WHERE id = '${t1.id}' FOR UPDATE`);
    const started = start(a1, t1);
    await waitBlocked(w, a1.processID);
    await simulateRestore(w);
    const quarantiner = call(s1, 'ld_get', tid(99));
    await waitBlocked(w, s1.processID);
    await rowLock.release();
    assert.equal(await started, true, 'ld_start got the row first');
    await quarantiner;
    let row = await w.ticket(t1.id);
    assert.equal(row.state, 'quarantined');
    sameExecution(row, t1);

    await (await w.su()).query("UPDATE lane_dispatch.tickets SET state = 'claimed' WHERE id = $1", [t1.id]);
    rowLock = await holdLock(w, `SELECT 1 FROM lane_dispatch.tickets WHERE id = '${t1.id}' FOR UPDATE`);
    await simulateRestore(w);
    const quarantinerFirst = call(s1, 'ld_get', tid(99));
    await waitBlocked(w, s1.processID);
    const startedSecond = start(a1, t1);
    await waitBlocked(w, a1.processID);
    await rowLock.release();
    await quarantinerFirst;
    assert.equal(await startedSecond, false, 'the quarantine got there first, and ld_start does not leave quarantine');
    row = await w.ticket(t1.id);
    assert.equal(row.state, 'quarantined');
    sameExecution(row, t1);
  },
};

const CANARIES = [
  ['the timeline quarantine fires from each ld_* function', 'PERFORM lane_dispatch.ld_timeline_ok();', 'NULL;'],
  ['ld_reassert fences another host token, a wrong token, a lower epoch and a terminal row', 'AND p_row_host = p_host', ''],
  ['the amnesty gap: after an outage longer than twice the interval ld_reap extends every lease and settles nothing', 'IF v_amnesty THEN', 'IF false THEN'],
  ['ld_reap requeues a claimed row past its lease, never a running one on expiry alone, and settles lost only after lost_after',
    "WHERE t.state = 'claimed' AND t.lease_until < v_now RETURNING t.state)", "WHERE t.state IN ('claimed','running') AND t.lease_until < v_now RETURNING t.state)"],
  ["a heartbeat never lifts quarantine: the matching runner is told to reassert, the row is unchanged, and a running reassert or ld_finish leaves quarantine",
    "  IF v_t.state = 'quarantined' THEN\n    RETURN jsonb_build_object('fenced', false, 'reassert', true, 'cancel', false, 'urls', NULL);\n  END IF;", "  IF v_t.state = 'quarantined' THEN\n    UPDATE lane_dispatch.tickets SET state = 'running' WHERE id = p_id;\n  END IF;"],
  ["a quarantined claim that never started is fenced: ld_release, a pre-start reassert and ld_start leave it untouched and unclaimable",
    "OR v_t.state = 'quarantined' THEN\n    RETURN jsonb_build_object('fenced', true, 'state', v_t.state);", "THEN\n    RETURN jsonb_build_object('fenced', true, 'state', v_t.state);"],
  ["a quarantined claim that never started is fenced: ld_release, a pre-start reassert and ld_start leave it untouched and unclaimable",
    "AND NOT (v_t.state = 'quarantined' AND v_phase = 'pre-start'))", "AND true)"],
  ["a quarantined claim that never started is fenced: ld_release, a pre-start reassert and ld_start leave it untouched and unclaimable",
    "IF v_t.state <> 'claimed' THEN RETURN false; END IF;", "IF v_t.state NOT IN ('claimed','quarantined') THEN RETURN false; END IF;"],
  ["ld_cancel confirms only a queued row: a quarantined row, owned or ownerless, is never confirmed",
    "'confirmed', v_t.state = 'queued');", "'confirmed', v_t.claim_token IS NULL);"],
  ["a higher-epoch reassert from another principal, with no restore, is fenced and the owner keeps the row",
    "OR (v_phase = 'running' AND v_t.state = 'quarantined' AND v_t.claim_token IS NULL AND v_epoch > v_t.epoch", "OR (v_phase = 'running' AND v_t.state IN ('claimed','running','quarantined') AND v_epoch > v_t.epoch"],
  ["two concurrent ld_reassert calls on one row serialise: the matching one is ok, the other is fenced, whatever the order or epochs",
    "OR (v_phase = 'running' AND v_t.state = 'quarantined' AND v_t.claim_token IS NULL AND v_epoch > v_t.epoch", "OR (v_phase = 'running' AND v_t.state IN ('claimed','running','quarantined') AND v_epoch > v_t.epoch"],
  ["ld_entry refuses a transaction above READ COMMITTED, so a REPEATABLE READ runner cannot exceed max_claims",
    "RAISE EXCEPTION 'lane_dispatch must run at READ COMMITTED, not %', current_setting('transaction_isolation') USING ERRCODE = '25000';", "NULL;"],
  ['ld_log stores a repeated seq once and counts log_bytes once', 'ON CONFLICT (ticket, epoch, seq) DO NOTHING;', ';'],
  ['ld_log caps one attempt at 64 MiB', 'IF v_logged + v_len > 67108864 THEN', 'IF false THEN'],
  ['ld_log caps one call at 256 KiB', 'IF v_len > 262144 THEN', 'IF false THEN'],
  ['ld_log caps the whole table at 1 GiB', '+ v_len > 1073741824 THEN', '+ v_len > 1073741824000 THEN'],
];

describe('lane_dispatch recovery on a disposable Postgres', { skip: pgSkipOption, timeout: 600000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries({ ...GUARDS, ...SEMANTICS })) {
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
