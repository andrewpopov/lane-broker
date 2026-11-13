import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import {
  startDispatchCluster, World, logins, call, enqueue, claim, start, release, liftBackoff, setNow, tid,
} from './lane-dispatch-fixture.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const T0 = Date.parse('2030-01-01T00:00:00Z');
const at = (ms) => new Date(T0 + ms).toISOString();
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;

const countRequests = async (w) => Number((await (await w.su()).query('SELECT count(*) AS n FROM lane_dispatch.claim_requests')).rows[0].n);
const secondsUntilNotBefore = async (w, id) =>
  Number((await (await w.su()).query('SELECT extract(epoch FROM not_before - lane_dispatch.now()) AS s FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0].s);
const effScore = async (w, ...args) => (await (await w.su()).query('SELECT lane_dispatch.eff_score($1, $2, $3) AS v', args)).rows[0].v;

const GUARDS = {
  'a release of a running row is refused and changes nothing, unless a cancel was requested': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const [running, cancelling] = [await claim(a1), await claim(a1)];
    await start(a1, running);
    await start(a1, cancelling);
    const before = await w.ticket(tid(1));
    for (const proof of ['never-started', 'cancel-confirmed']) {
      assert.deepEqual(await release(a1, running, proof), { refused: 'started' }, proof);
    }
    assert.deepEqual(await w.ticket(tid(1)), before, 'nothing changed');
    await call(s1, 'ld_cancel', tid(2));
    assert.deepEqual(await release(a1, cancelling, 'never-started'), { refused: 'started' }, 'a cancel request alone is not proof the run stopped');
    assert.equal((await w.ticket(tid(2))).state, 'running');
    assert.deepEqual(await release(a1, cancelling, 'cancel-confirmed'), { fenced: false, state: 'cancelled' });
    assert.notEqual((await w.ticket(tid(2))).finished_at, null);
  },

  'the sixth requeue settles lost with release-cap, and each requeue backs off 5, 10, 20, 40 and 60 s': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    for (const [n, backoff] of [[1, 5], [2, 10], [3, 20], [4, 40], [5, 60]]) {
      const got = await claim(a1);
      assert.equal(got.id, tid(1), `claim ${n}`);
      assert.deepEqual(await release(a1, got), { fenced: false, state: 'queued' });
      const row = await w.ticket(tid(1));
      assert.equal(row.requeues, n);
      const wait = await secondsUntilNotBefore(w, tid(1));
      assert.ok(wait > backoff - 2 && wait <= backoff, `requeue ${n} waits ${wait} s, want ${backoff}`);
      await liftBackoff(w);
    }
    const sixth = await claim(a1);
    assert.deepEqual(await release(a1, sixth), { fenced: false, state: 'lost' });
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.state, row.claim_token, row.host], ['lost', null, null]);
    assert.deepEqual(row.result, { kind: 'lost', reason: 'release-cap', author: 'queue', epoch: sixth.epoch });
    assert.notEqual(row.finished_at, null);
    assert.deepEqual(await claim(a1), { status: 'none' }, 'a lost ticket is never claimed again');
  },

  'a lease expiry returns a claimed ticket to the queue with the same requeue count and backoff as a release': async (w) => {
    const [s1, a1, admin, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin), await w.su()];
    await enqueue(s1, tid(1));
    await claim(a1);
    await su.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '10 seconds'");
    await su.query('UPDATE lane_dispatch.cluster SET last_tick = clock_timestamp()');
    assert.equal((await call(admin, 'ld_reap')).requeued, 1);
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.state, row.requeues], ['queued', 1]);
    assert.ok(await secondsUntilNotBefore(w, tid(1)) > 3);
  },

  'not_before is honoured on a fresh ticket and on a requeued one': async (w) => {
    const [s1, a1, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.su()];
    await enqueue(s1, tid(1));
    assert.equal((await claim(a1)).id, tid(1), 'a fresh ticket is claimable at once');
    await enqueue(s1, tid(2));
    await su.query("UPDATE lane_dispatch.tickets SET not_before = now() + interval '1 hour' WHERE id = $1", [tid(2)]);
    assert.deepEqual(await claim(a1), { status: 'none' }, 'a ticket held back by not_before');
    await su.query("UPDATE lane_dispatch.tickets SET not_before = now() - interval '1 second' WHERE id = $1", [tid(2)]);
    const got = await claim(a1);
    assert.equal(got.id, tid(2), 'claimable once it has passed');
    await release(a1, got);
    await release(a1, { id: tid(1), epoch: 1, token: (await w.ticket(tid(1))).claim_token });
    assert.deepEqual(await claim(a1), { status: 'none' }, 'both requeued tickets are backing off');
    await liftBackoff(w);
    assert.equal((await claim(a1)).status, 'claimed');
  },

  'a none answer writes no claim request, whether nothing is queued or the host is full': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    assert.deepEqual(await claim(a1), { status: 'none' });
    assert.equal(await countRequests(w), 0, 'an empty queue');
    for (const n of [1, 2, 3]) await enqueue(s1, tid(n));
    await claim(a1);
    await claim(a1);
    const before = await countRequests(w);
    assert.deepEqual(await claim(a1), { status: 'none' });
    assert.equal(await countRequests(w), before, 'a full host');
  },

  'the protocol filter hides a ticket from an older runner, and a runner below protocol_min or unregistered is told to upgrade': async (w) => {
    const [s1, a1, a2, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2), await w.su()];
    await enqueue(s1, tid(1), { minProtocol: 2 });
    assert.deepEqual(await claim(a1), { status: 'none' }, 'a protocol 1 runner');
    assert.equal(await countRequests(w), 0);
    await call(a1, 'ld_register', 'test', 2, null);
    assert.equal((await claim(a1)).id, tid(1), 'a protocol 2 runner');

    await su.query("DELETE FROM lane_dispatch.runners WHERE host = 'a2'");
    assert.deepEqual(await claim(a2), { status: 'upgrade', protocol_min: 1 }, 'a host with no runners row');
    await call(a2, 'ld_register', 'test', 0, null);
    assert.deepEqual(await claim(a2), { status: 'upgrade', protocol_min: 1 }, 'a protocol below protocol_min');
    assert.equal(await countRequests(w), 1, 'only the real claim left a request row');
  },

  'two hosts claim concurrently: neither is answered busy or made to wait for the other': async (w) => {
    const [s1, a2] = [await w.as(logins.s1), await w.as(logins.a2)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const holder = await w.cluster.client(w.db, logins.x);
    await holder.query('BEGIN');
    assert.equal((await call(holder, 'ld_claim', randomUUID(), 8, 1e9, [])).id, tid(1), 'the first claim is uncommitted');
    const started = Date.now();
    const second = await claim(a2);
    assert.deepEqual([second.status, second.id], ['claimed', tid(2)]);
    assert.ok(Date.now() - started < 1500);
    await holder.query('COMMIT');
  },

  'two claims by one login serialise on its principal row, so max_claims holds': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const [first, second] = [await w.cluster.client(w.db, logins.a2), await w.cluster.client(w.db, logins.a2)];
    await first.query('BEGIN');
    assert.equal((await call(first, 'ld_claim', randomUUID(), 8, 1e9, [])).id, tid(1));
    const racing = claim(second);
    assert.equal(await Promise.race([racing, sleep(400).then(() => 'waiting')]), 'waiting', 'the second claim waits for the first login claim to commit');
    await first.query('COMMIT');
    assert.deepEqual(await racing, { status: 'none' }, 'a2 holds its one claim');
    assert.equal((await w.ticket(tid(2))).state, 'queued');
  },

  'a host takes the ticket fewer hosts can run first, within a tier': async (w) => {
    await setNow(w, at(0));
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1), { priority: 5 });
    await enqueue(s1, tid(2), { priority: 5, tags: ['fast'] });
    assert.equal((await claim(a1)).id, tid(2), 'two fresh mediums of one score: only a1 can run the fast one, so a1 takes it first');
    assert.equal((await claim(a1)).id, tid(1));
  },

  'eff_score holds an aged low and medium under a fresh high until 60 minutes, then ties it': async (w, makeWorld) => {
    const held = async (world, waited) => {
      await setNow(world, at(0));
      const s1 = await world.as(logins.s1);
      await enqueue(s1, tid(1), { priority: 2, tags: ['fast'] });
      await enqueue(s1, tid(2), { priority: 5, tags: ['fast'] });
      await setNow(world, at(waited));
      await enqueue(await world.as(logins.s1), tid(3), { priority: 8 });
      const a1 = await world.as(logins.a1);
      return [(await claim(a1)).id, (await claim(a1)).id];
    };
    assert.deepEqual(await held(w, 59 * MINUTE), [tid(3), tid(1)], 'at 59 min a fresh high goes first, then the older of the two aged tickets (both sit at the same capped score)');
    assert.deepEqual(await held(await makeWorld(), 61 * MINUTE), [tid(1), tid(2)], 'at 61 min both tie the high, and the scarcer tickets (one host can run them) go before it, oldest first');

    const probe = await makeWorld();
    assert.equal(await effScore(probe, 8, true, 0), 2);
    assert.equal(await effScore(probe, 9, true, 0), 2.01);
    assert.equal(await effScore(probe, 2, true, 59 * 60), 1.98);
    assert.equal(await effScore(probe, 5, true, 61 * 60), 2);
    assert.equal(await effScore(probe, 5, false, 61 * 60), 1, 'a ticket that does not age credits no wait');
    assert.equal(await effScore(probe, 2, true, 600), 1, 'half the 20 minute age window is half the age weight');
  },

  'an aged low ties a fresh priority 8 and wins on age, and a fresh priority 9 beats both': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { priority: 2 });
    await setNow(w, at(61 * MINUTE));
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(2), { priority: 8 });
    await enqueue(s1, tid(3), { priority: 9 });
    const a1 = await w.as(logins.a1);
    assert.deepEqual([(await claim(a1)).id, (await claim(a1)).id], [tid(3), tid(1)], 'the release candidate first, then the aged low before the fresh high');
    assert.equal((await w.ticket(tid(2))).state, 'queued');
  },

  'aged work is claimed before fresh scarcer work once it has reached the high level': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { priority: 2, tags: ['linux'] });
    await setNow(w, at(61 * MINUTE));
    const s1 = await w.as(logins.s1);
    for (const n of [2, 3, 4]) await enqueue(s1, tid(n), { priority: 8, tags: ['fast'] });
    assert.equal((await claim(await w.as(logins.a1))).id, tid(1), 'the aged low, runnable on a1 and a2, goes before fresh work only a1 can run');
  },

  'ld_claim reads the principal under its lock: a max_claims change committed while a claim waits is seen by that claim': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const a1 = await w.as(logins.a1);
    const admin = await w.cluster.client(w.db, logins.admin);
    await admin.query('BEGIN');
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast'], ['repoA'], 0);
    const racing = claim(a1);
    assert.equal(await Promise.race([racing, sleep(400).then(() => 'waiting')]), 'waiting', 'the claim waits for the open principal change');
    await admin.query('COMMIT');
    assert.deepEqual(await racing, { status: 'none' }, 'and then sees max_claims 0');
    assert.equal((await w.ticket(tid(1))).state, 'queued');
  },

  'a runner cannot store lane.test_now for itself with ALTER ROLE, in the database or cluster-wide': async (w) => {
    const a1 = await w.as(logins.a1);
    for (const sql of [`ALTER ROLE ${logins.a1} SET lane.test_now = '2000-01-01T00:00:00Z'`,
      `ALTER ROLE ${logins.a1} IN DATABASE ${w.db} SET lane.test_now = '2000-01-01T00:00:00Z'`]) {
      await assert.rejects(a1.query(sql), (err) => err.code === '42501', sql);
    }
  },

  'retention selects every queue-authored lost row: reaped, release-capped and restored': async (w) => {
    const su = await w.su();
    await setNow(w, at(0));
    const [s1, a1, a2] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2)];
    for (const n of [1, 2]) await enqueue(s1, tid(n));
    const running = await claim(a1);
    await start(a1, running);
    await su.query('UPDATE lane_dispatch.tickets SET requeues = 5 WHERE id = $1', [tid(2)]);
    await claim(a2);
    await setNow(w, at(6 * HOUR));
    await su.query('UPDATE lane_dispatch.cluster SET last_tick = $1', [at(6 * HOUR - 5000)]);
    assert.deepEqual(await call(await w.as(logins.admin), 'ld_reap'), { amnesty: false, requeued: 0, cancelled: 0, lost: 2 });
    await enqueue(await w.as(logins.s1), tid(3));
    assert.equal(await call(await w.as(logins.admin), 'ld_restore_reset'), 1);
    await setNow(w, at(10 * 24 * HOUR));
    const { items } = await call(await w.as(logins.admin), 'ld_prune_candidates', '1 day', '7 days', null, null, 10);
    assert.deepEqual(items.map((i) => i.id).sort(), [tid(1), tid(2), tid(3)], 'every lost row has a finished_at, so retention sees it');
    assert.ok(items.every((i) => i.row_due));
    const reasons = (await su.query('SELECT result->>\'reason\' AS r FROM lane_dispatch.tickets ORDER BY id')).rows.map((r) => r.r);
    assert.deepEqual(reasons, ['runner-lost', 'release-cap', 'queue-restored']);
  },

  'amnesty fires when the gap between reaper ticks passes 60 s, not at 5 s': async (w) => {
    const su = await w.su();
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1));
    const got = await claim(await w.as(logins.a1));
    await setNow(w, at(130_000));
    await su.query('UPDATE lane_dispatch.cluster SET last_tick = $1', [at(125_000)]);
    assert.deepEqual(await call(await w.as(logins.admin), 'ld_reap'), { amnesty: false, requeued: 1, cancelled: 0, lost: 0 }, 'a 5 s gap, lease expired 10 s ago');

    await liftBackoff(w);
    const again = await claim(await w.as(logins.a1));
    assert.equal(again.id, got.id);
    await setNow(w, at(400_000));
    await su.query('UPDATE lane_dispatch.cluster SET last_tick = $1', [at(400_000 - 61_000)]);
    assert.deepEqual(await call(await w.as(logins.admin), 'ld_reap'), { amnesty: true, requeued: 0, cancelled: 0, lost: 0 }, 'a 61 s gap');
    const row = await w.ticket(tid(1));
    assert.equal(row.state, 'claimed');
    assert.equal(row.lease_until.toISOString(), at(400_000 + 120_000), 'the lease is extended to a full term from now');
  },

  'a session SET of lane.test_now never moves the queue clock': async (w) => {
    const s1 = await w.as(logins.s1);
    await s1.query("SET lane.test_now = '2000-01-01T00:00:00Z'");
    await enqueue(s1, tid(1));
    assert.ok((await w.ticket(tid(1))).created_at > new Date('2020-01-01T00:00:00Z'));
  },

  'ld_enqueue and ld_enqueue_fenced store min_protocol, ages, source and est_run_s, and default them for the old call shape': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { minProtocol: 3, ages: false, source: 'rc', estRunS: 12.5 });
    await enqueue(s1, tid(2));
    const fields = (row) => [row.min_protocol, row.ages, row.source, row.est_run_s, row.reserved_host, row.reserved_at];
    assert.deepEqual(fields(await w.ticket(tid(1))), [3, false, 'rc', 12.5, null, null]);
    assert.deepEqual(fields(await w.ticket(tid(2))), [1, true, null, null, null, null]);
    const ref = await call(s1, 'ld_queue_ref');
    await call(s1, 'ld_enqueue_fenced', ref, tid(3), 'repoA', 'test', 'lk3', { protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x' }, [], 1, 1000, 0, 4, false, 'rc', 7);
    assert.deepEqual(fields(await w.ticket(tid(3))), [4, false, 'rc', 7, null, null]);
    await call(s1, 'ld_enqueue_fenced', ref, tid(4), 'repoA', 'test', 'lk4', { protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x' }, [], 1, 1000, 0);
    assert.deepEqual(fields(await w.ticket(tid(4))), [1, true, null, null, null, null], 'the 12-argument call still resolves');
  },
};

const MIGRATION_NOT_BEFORE = "ADD COLUMN not_before timestamptz NOT NULL DEFAULT '-infinity'";
const RESTORE_STAMP = "SET state = 'lost', finished_at = lane_dispatch.now(), result = lane_dispatch.lost_result('queue-restored', epoch)";
const CANARIES = [
  ['a release of a running row is refused and changes nothing, unless a cancel was requested',
    "IF NOT (v_t.cancel_requested AND p_proof = 'cancel-confirmed') THEN RETURN jsonb_build_object('refused', 'started'); END IF;", ''],
  ['a release of a running row is refused and changes nothing, unless a cancel was requested',
    "IF NOT (v_t.cancel_requested AND p_proof = 'cancel-confirmed') THEN", "IF NOT v_t.cancel_requested THEN"],
  ['the sixth requeue settles lost with release-cap, and each requeue backs off 5, 10, 20, 40 and 60 s', "WHEN v_t.requeues >= 5 THEN 'lost'", "WHEN v_t.requeues >= 500 THEN 'lost'"],
  ['the sixth requeue settles lost with release-cap, and each requeue backs off 5, 10, 20, 40 and 60 s', "interval '5 seconds' * 2 ^ requeues", "interval '5 seconds'"],
  ['the sixth requeue settles lost with release-cap, and each requeue backs off 5, 10, 20, 40 and 60 s', "THEN lane_dispatch.lost_result('release-cap', epoch)", 'THEN NULL'],
  ['a lease expiry returns a claimed ticket to the queue with the same requeue count and backoff as a release',
    'v_state := lane_dispatch.return_to_queue(v_id, v_now);', "v_state := 'queued'; UPDATE lane_dispatch.tickets SET state = 'queued', claim_token = NULL, host = NULL, lease_until = NULL WHERE id = v_id;"],
  ['not_before is honoured on a fresh ticket and on a requeued one', 'AND t.not_before <= v_now', ''],
  ['not_before is honoured on a fresh ticket and on a requeued one', MIGRATION_NOT_BEFORE, "ADD COLUMN not_before timestamptz NOT NULL DEFAULT 'infinity'"],
  ['a none answer writes no claim request, whether nothing is queued or the host is full',
    "  IF NOT FOUND THEN RETURN jsonb_build_object('status', 'none'); END IF;\n  v_token", "  IF NOT FOUND THEN INSERT INTO lane_dispatch.claim_requests (login_role, req) VALUES (session_user, p_req); RETURN jsonb_build_object('status', 'none'); END IF;\n  v_token"],
  ['a none answer writes no claim request, whether nothing is queued or the host is full',
    "IF v_held >= v_me.max_claims THEN RETURN jsonb_build_object('status', 'none'); END IF;", "IF v_held >= v_me.max_claims THEN INSERT INTO lane_dispatch.claim_requests (login_role, req) VALUES (session_user, p_req); RETURN jsonb_build_object('status', 'none'); END IF;"],
  ['the protocol filter hides a ticket from an older runner, and a runner below protocol_min or unregistered is told to upgrade', 'AND t.min_protocol <= v_protocol', ''],
  ['the protocol filter hides a ticket from an older runner, and a runner below protocol_min or unregistered is told to upgrade', 'IF NOT FOUND OR v_protocol < lane_dispatch.protocol_min() THEN', 'IF false THEN'],
  ['two hosts claim concurrently: neither is answered busy or made to wait for the other',
    'v_out := lane_dispatch.claim_replay(p_req);\n  IF v_out IS NOT NULL THEN RETURN v_out; END IF;\n  SELECT r.protocol', 'PERFORM pg_advisory_xact_lock(1);\n  v_out := lane_dispatch.claim_replay(p_req);\n  IF v_out IS NOT NULL THEN RETURN v_out; END IF;\n  SELECT r.protocol'],
  ['two claims by one login serialise on its principal row, so max_claims holds', "p.kind = 'agent' FOR UPDATE;", "p.kind = 'agent';"],
  ['ld_claim reads the principal under its lock: a max_claims change committed while a claim waits is seen by that claim', "p.kind = 'agent' FOR UPDATE;", "p.kind = 'agent';"],
  ['ld_claim reads the principal under its lock: a max_claims change committed while a claim waits is seen by that claim',
    "SELECT p.host, p.tags, p.allowed_repos, p.max_claims INTO v_me FROM lane_dispatch.principals p WHERE p.login_role = session_user AND p.kind = 'agent' FOR UPDATE;\n  IF NOT FOUND THEN\n    RAISE EXCEPTION 'no agent principal for %', session_user USING ERRCODE = '42501';\n  END IF;",
    "SELECT * INTO v_me FROM lane_dispatch.me('agent');\n  PERFORM 1 FROM lane_dispatch.principals p WHERE p.login_role = session_user FOR UPDATE;"],
  ['aged work is claimed before fresh scarcer work once it has reached the high level',
    "            CASE WHEN lane_dispatch.eff_score(t.priority, t.ages, extract(epoch FROM v_now - t.created_at)) >= 2.0 THEN t.created_at END NULLS LAST,\n", ''],
  ['a host takes the ticket fewer hosts can run first, within a tier', 'lane_dispatch.scarcity(t.requires_tags, t.repo_key), t.created_at, t.id', 't.created_at, t.id'],
  ['eff_score holds an aged low and medium under a fresh high until 60 minutes, then ties it', 'v_credit >= 3600', 'v_credit >= 360000'],
  ['eff_score holds an aged low and medium under a fresh high until 60 minutes, then ties it', 'ELSE 1.98 END', 'ELSE 2.0 END'],
  ['eff_score holds an aged low and medium under a fresh high until 60 minutes, then ties it', 'CASE WHEN p_priority >= 9 THEN 0.01 ELSE 0 END', '0'],
  ['an aged low ties a fresh priority 8 and wins on age, and a fresh priority 9 beats both', 'CASE WHEN p_priority >= 9 THEN 0.01 ELSE 0 END', '0'],
  ['an aged low ties a fresh priority 8 and wins on age, and a fresh priority 9 beats both', 'v_credit >= 3600', 'v_credit >= 360000'],
  ['an aged low ties a fresh priority 8 and wins on age, and a fresh priority 9 beats both',
    "THEN t.created_at END NULLS LAST,\n            lane_dispatch.scarcity(t.requires_tags, t.repo_key), t.created_at, t.id LIMIT 1", "THEN t.id END DESC NULLS LAST,\n            lane_dispatch.scarcity(t.requires_tags, t.repo_key), t.id DESC LIMIT 1"],
  ['eff_score holds an aged low and medium under a fresh high until 60 minutes, then ties it', 'CASE WHEN p_ages THEN greatest(p_age_s, 0) ELSE 0 END', 'greatest(p_age_s, 0)'],
  ['retention selects every queue-authored lost row: reaped, release-capped and restored', RESTORE_STAMP, "SET state = 'lost', result = lane_dispatch.lost_result('queue-restored', epoch)"],
  ['retention selects every queue-authored lost row: reaped, release-capped and restored', "finished_at = CASE WHEN v_state = 'queued' THEN NULL ELSE p_now END", 'finished_at = NULL'],
  ['retention selects every queue-authored lost row: reaped, release-capped and restored',
    "SET state = 'lost', finished_at = v_now, result = lane_dispatch.lost_result('runner-lost', t.epoch)", "SET state = 'lost', result = lane_dispatch.lost_result('runner-lost', t.epoch)"],
  ['amnesty fires when the gap between reaper ticks passes 60 s, not at 5 s', '> p_amnesty_after;', "> interval '1 second';"],
  ['amnesty fires when the gap between reaper ticks passes 60 s, not at 5 s', '> p_amnesty_after;', "> interval '1 hour';"],
  ['a session SET of lane.test_now never moves the queue clock', "AND 'lane.test_now=' || v_set = ANY (s.setconfig)", 'AND true'],
  ['ld_enqueue and ld_enqueue_fenced store min_protocol, ages, source and est_run_s, and default them for the old call shape',
    '                                  p_min_protocol, p_ages, p_source, p_est_run_s);', '                                  1, true, NULL, NULL);'],
  ['ld_enqueue and ld_enqueue_fenced store min_protocol, ages, source and est_run_s, and default them for the old call shape',
    'coalesce(p_min_protocol, 1), coalesce(p_ages, true), p_source, p_est_run_s,', '1, true, NULL, NULL,'],
];

describe('lane_dispatch pull dispatch queue SQL (rev 6) on a disposable Postgres', { skip: pgSkipOption, timeout: 900000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) {
    test(name, async () => scenario(await World.create(cluster), () => World.create(cluster)));
  }

  for (const [name, from, to] of CANARIES) {
    test(`canary: with "${from.slice(0, 50).replaceAll('\n', ' ')}" mutated, "${name}" fails`, async (t) => {
      const makeWorld = () => World.create(cluster, { mutate: [from, to] });
      let failure;
      await GUARDS[name](await makeWorld(), makeWorld).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
