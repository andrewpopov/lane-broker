import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, start, heartbeat, finish, release, tid } from './lane-dispatch-fixture.js';

const GUARDS = {
  'ld_claim refuses past max_claims': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    for (const n of [1, 2, 3]) await enqueue(s1, tid(n));
    const first = await claim(a1);
    const second = await claim(a1);
    assert.deepEqual([first.id, second.id], [tid(1), tid(2)]);
    assert.deepEqual(await claim(a1), { status: 'none' }, 'a third claim exceeds max_claims 2');
    assert.equal((await w.ticket(tid(3))).state, 'queued');
    await release(a1, first);
    assert.equal((await claim(a1)).id, tid(1), 'a released slot is claimable again');
  },

  'ld_claim never hands out a ticket whose requires_tags the principal lacks': async (w) => {
    const [s1, a1, admin] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin)];
    await enqueue(s1, tid(1), { tags: ['gpu'] });
    assert.deepEqual(await claim(a1), { status: 'none' });
    assert.equal((await w.ticket(tid(1))).state, 'queued');
    await enqueue(s1, tid(2), { tags: ['fast'] });
    assert.equal((await claim(a1)).id, tid(2), 'a subset of the admin-owned tags is claimable');
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast', 'gpu'], ['repoA'], 2);
    assert.equal((await claim(a1)).id, tid(1), 'tags come from ld_set_principal');
  },

  'ld_claim never hands out a ticket outside the principal allowed_repos': async (w) => {
    const [s1, a1, a2] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2)];
    await enqueue(s1, tid(1), { repo: 'repoZ' });
    assert.deepEqual(await claim(a1), { status: 'none' });
    assert.equal((await w.ticket(tid(1))).state, 'queued');
    assert.equal((await claim(a2)).id, tid(1), "a '*' principal may claim any repo");
  },

  'ld_start refuses a ticket with cancel_requested': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    assert.deepEqual(await call(s1, 'ld_cancel', tid(1)), { state: 'claimed', cancel_requested: true, confirmed: false });
    assert.equal(await start(a1, got), false);
    assert.equal((await w.ticket(tid(1))).state, 'claimed');
    assert.equal((await heartbeat(a1, got)).cancel, true);
  },

  'ld_start refuses an expired lease': async (w) => {
    const [s1, a1, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.su()];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await su.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '1 second'");
    assert.equal(await start(a1, got), false);
    assert.equal((await w.ticket(tid(1))).state, 'claimed');
  },

  'ld_start refuses a stale epoch, a stale token and another host': async (w) => {
    const [s1, a1, a2] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2)];
    await enqueue(s1, tid(1));
    const old = await claim(a1);
    await release(a1, old);
    const current = await claim(a1);
    assert.equal(current.epoch, old.epoch + 1);
    assert.equal(await start(a1, old), false, 'both stale');
    assert.equal(await start(a1, { ...current, epoch: old.epoch }), false, 'stale epoch');
    assert.equal(await start(a1, { ...current, token: old.token }), false, 'stale token');
    assert.equal(await start(a2, current), false, 'another host');
    assert.equal((await w.ticket(tid(1))).state, 'claimed');
    assert.equal(await start(a1, current), true);
    assert.equal(await start(a1, current), true, 'a repeated start has the same outcome');
  },

  'ld_finish is fenced unless id, epoch, token and host match a non-terminal row': async (w) => {
    const [s1, a1, a2] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await start(a1, got);
    const fenced = [
      await finish(a1, { ...got, token: randomUUID() }),
      await finish(a1, { ...got, epoch: got.epoch + 1 }),
      await finish(a2, got),
      await finish(a1, { ...got, id: tid(99) }),
    ];
    assert.ok(fenced.every((r) => r.fenced === true), JSON.stringify(fenced));
    assert.equal((await w.ticket(tid(1))).state, 'running');
    assert.deepEqual(await finish(a1, got, { kind: 'ok', exitCode: 0 }), { fenced: false, state: 'finished' });
    assert.deepEqual(await finish(a1, got, { kind: 'failed', exitCode: 1 }), { fenced: false, state: 'finished' }, 'same (id, epoch, token) repeats');
    const row = await w.ticket(tid(1));
    assert.deepEqual(row.result, { kind: 'ok', exitCode: 0, epoch: got.epoch }, 'the first accepted result stays');
    assert.equal((await finish(a1, { ...got, token: randomUUID() })).fenced, true, 'a terminal row accepts no other token');
    assert.equal((await heartbeat(a1, got)).fenced, true, 'a terminal row fences the heartbeat');
  },

  'a duplicate req returns the stored outcome and never takes a second ticket': async (w) => {
    const [s1, a1, a2] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const req = randomUUID();
    const first = await claim(a1, { req });
    assert.deepEqual(await claim(a1, { req }), first);
    assert.equal((await w.ticket(tid(2))).state, 'queued');
    assert.equal((await claim(a2, { req })).id, tid(2), 'a req is scoped to its login');
    const empty = randomUUID();
    assert.deepEqual(await claim(a2, { req: empty }), { status: 'none' });
    await enqueue(s1, tid(3));
    assert.deepEqual(await claim(a2, { req: empty }), { status: 'none' }, 'a replayed "no claim" stays no claim');
  },

  'ld_withdraw returns withdrawn only for a committed queued to withdrawn': async (w) => {
    const [s1, s2, a1] = [await w.as(logins.s1), await w.as(logins.s2), await w.as(logins.a1)];
    for (const n of [1, 2, 3, 4, 5]) await enqueue(s1, tid(n));
    const finished = await claim(a1);
    await finish(a1, finished);
    const running = await claim(a1);
    await start(a1, running);
    const claimed = await claim(a1);
    assert.deepEqual([finished.id, running.id, claimed.id], [tid(1), tid(2), tid(3)]);
    assert.equal(await call(s1, 'ld_withdraw', tid(1)), 'finished');
    assert.equal(await call(s1, 'ld_withdraw', tid(2)), 'running');
    assert.equal(await call(s1, 'ld_withdraw', tid(3)), 'claimed');
    assert.deepEqual([(await w.ticket(tid(2))).state, (await w.ticket(tid(3))).state], ['running', 'claimed']);
    assert.equal(await call(s2, 'ld_withdraw', tid(4)), 'missing', 'another owner learns nothing');
    assert.equal((await w.ticket(tid(4))).state, 'queued');
    assert.equal(await call(s1, 'ld_withdraw', tid(4)), 'withdrawn');
    assert.equal(await call(s1, 'ld_withdraw', tid(4)), 'withdrawn', 'a retry after a lost response');
    assert.equal(await call(s1, 'ld_withdraw', tid(99)), 'missing');
    assert.equal((await w.ticket(tid(4))).state, 'withdrawn');
  },
};

const SEMANTICS = {
  'a same-host re-claim after a requeue gets a new epoch, token and execution id': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const first = await claim(a1);
    assert.deepEqual([first.epoch, first.execution_id], [1, `${tid(1)}.e1`]);
    assert.deepEqual(await release(a1, first, 'runner-shutdown'), { fenced: false, state: 'queued' });
    assert.deepEqual(await release(a1, first, 'runner-shutdown'), { fenced: true, state: 'queued' });
    const second = await claim(a1);
    assert.deepEqual([second.epoch, second.execution_id], [2, `${tid(1)}.e2`]);
    assert.notEqual(second.token, first.token);
    assert.equal((await heartbeat(a1, first)).fenced, true, 'the old epoch is fenced');
    assert.equal((await heartbeat(a1, second)).fenced, false);
    const su = await w.su();
    assert.deepEqual((await su.query('SELECT epoch, host FROM lane_dispatch.attempts ORDER BY epoch')).rows, [{ epoch: 1, host: 'a1' }, { epoch: 2, host: 'a1' }]);
  },

  'a claim stamps a 120 s lease, epoch and host, and carries the pinned snapshot and header': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.state, row.epoch, row.host, row.claim_token], ['claimed', 1, 'a1', got.token]);
    const su = await w.su();
    const { rows: [{ ttl }] } = await su.query('SELECT extract(epoch FROM lease_until - now()) AS ttl FROM lane_dispatch.tickets');
    assert.ok(ttl > 110 && ttl <= 120, `lease ${ttl}`);
    assert.deepEqual([got.snapshot, got.header, got.binding], [{ key: 'snap', sha256: 'x' }, { cmd: ['true'] }, { protocol: 1 }]);
  },

  'a claim takes the highest priority, then the oldest, and only what fits the runner free cpu, memory and held keys': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1), { laneKey: 'kA' });
    await enqueue(s1, tid(2), { priority: 5 });
    await enqueue(s1, tid(3), { priority: 5, cpu: 16 });
    await enqueue(s1, tid(4), { priority: 9, mem: 1e12 });
    assert.equal((await claim(a1)).id, tid(2), 'priority 9 needs too much memory, priority 5 cpu 16 too much cpu');
    assert.deepEqual(await claim(a1, { held: ['kA'] }), { status: 'none' }, 'a held lane key is skipped');
    assert.equal((await claim(a1)).id, tid(1));
  },

  'ld_cancel: queued is cancelled at once, claimed and running only set the flag, a terminal row is unchanged, another owner sees missing': async (w) => {
    const [s1, s2, a1] = [await w.as(logins.s1), await w.as(logins.s2), await w.as(logins.a1)];
    for (const n of [1, 2, 3]) await enqueue(s1, tid(n));
    assert.deepEqual(await call(s1, 'ld_cancel', tid(1)), { state: 'cancelled', cancel_requested: true, confirmed: true });
    assert.deepEqual(await claim(a1, { cpu: 0 }), { status: 'none' });
    const running = await claim(a1);
    assert.equal(running.id, tid(2));
    await start(a1, running);
    assert.deepEqual(await call(s2, 'ld_cancel', tid(2)), { state: 'missing', cancel_requested: false, confirmed: false });
    assert.deepEqual(await call(s1, 'ld_cancel', tid(2)), { state: 'running', cancel_requested: true, confirmed: false });
    assert.equal((await heartbeat(a1, running)).cancel, true);
    assert.deepEqual(await finish(a1, running, { kind: 'cancelled', cancelConfirmed: true }), { fenced: false, state: 'cancelled' });
    assert.deepEqual(await call(s1, 'ld_cancel', tid(2)), { state: 'cancelled', cancel_requested: true, confirmed: true });
  },

  'ld_release: only a listed proof requeues, a cancel_requested row becomes cancelled, a mismatched caller is fenced': async (w) => {
    const [s1, a1, a2] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await assert.rejects(release(a1, got, 'because'), /unknown release proof/);
    assert.deepEqual(await release(a2, got), { fenced: true, state: 'claimed' });
    await call(s1, 'ld_cancel', tid(1));
    assert.deepEqual(await release(a1, got, 'cancel-confirmed'), { fenced: false, state: 'cancelled' });
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.claim_token, row.host, row.lease_until], [null, null, null]);
  },

  'ld_heartbeat extends the lease and returns the owner-attached urls; ld_attach_urls is owner-only and epoch-checked': async (w) => {
    const [s1, s2, a1, su] = [await w.as(logins.s1), await w.as(logins.s2), await w.as(logins.a1), await w.su()];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await su.query("UPDATE lane_dispatch.tickets SET lease_until = now() + interval '5 seconds'");
    assert.deepEqual(await heartbeat(a1, got), { fenced: false, cancel: false, urls: null });
    const { rows: [{ ttl }] } = await su.query('SELECT extract(epoch FROM lease_until - now()) AS ttl FROM lane_dispatch.tickets');
    assert.ok(ttl > 110, `lease ${ttl}`);
    assert.equal(await call(s2, 'ld_attach_urls', tid(1), got.epoch, { get: 'u' }), false, 'not the owner');
    assert.equal(await call(s1, 'ld_attach_urls', tid(1), got.epoch + 1, { get: 'u' }), false, 'wrong epoch');
    assert.equal(await call(s1, 'ld_attach_urls', tid(1), got.epoch, { epoch: got.epoch, get: 'u', put: 'p' }), true);
    assert.deepEqual((await heartbeat(a1, got)).urls, { epoch: got.epoch, get: 'u', put: 'p' });
    assert.equal(await call(s2, 'ld_get', tid(1)), null, 'another owner cannot read the ticket');
    assert.equal((await call(s1, 'ld_get', tid(1))).state, 'claimed');
    assert.equal('claim_token' in (await call(s1, 'ld_get', tid(1))), false, 'the fencing token never reaches the submitter');
    assert.deepEqual((await call(s1, 'ld_list_mine', ['claimed'])).map((t) => t.id), [tid(1)]);
    assert.deepEqual(await call(s2, 'ld_list_mine'), []);
  },

  'ld_enqueue is idempotent on id for its owner and refuses another owner': async (w) => {
    const [s1, s2] = [await w.as(logins.s1), await w.as(logins.s2)];
    const first = await enqueue(s1, tid(1));
    assert.deepEqual(await enqueue(s1, tid(1)), first);
    await assert.rejects(enqueue(s2, tid(1)), (err) => err.code === '23505');
  },
};

const CANARIES = [
  ['ld_claim refuses past max_claims', 'IF v_held >= v_me.max_claims THEN', 'IF false THEN'],
  ['ld_claim never hands out a ticket whose requires_tags the principal lacks', 'p_requires_tags <@ p_tags AND ', ''],
  ['ld_claim never hands out a ticket outside the principal allowed_repos', " AND (p_repo_key = ANY (p_allowed_repos) OR '*' = ANY (p_allowed_repos))", ''],
  ['ld_start refuses a ticket with cancel_requested', 'IF v_t.cancel_requested THEN RETURN false; END IF;', ''],
  ['ld_start refuses an expired lease', 'IF v_t.lease_until <= clock_timestamp() THEN RETURN false; END IF;', ''],
  ['ld_start refuses a stale epoch, a stale token and another host', 'AND p_row_epoch = p_epoch', ''],
  ['ld_finish is fenced unless id, epoch, token and host match a non-terminal row', 'AND p_row_token = p_token', ''],
  ['a duplicate req returns the stored outcome and never takes a second ticket', 'WHERE c.login_role = session_user AND c.req = p_req', 'WHERE false AND c.req = p_req'],
  ['ld_withdraw returns withdrawn only for a committed queued to withdrawn', "IF v_state = 'queued' THEN", 'IF true THEN'],
];

describe('lane_dispatch lease model on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries({ ...GUARDS, ...SEMANTICS })) {
    test(name, async () => scenario(await World.create(cluster)));
  }

  for (const [name, from, to] of CANARIES) {
    test(`canary: with the guard removed, "${name}" fails`, async (t) => {
      const world = await World.create(cluster, { mutate: [from, to] });
      let failure;
      await GUARDS[name](world).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
