// BRAIN-1106 claims fit the weight the runner's broker can admit, the aged head follows claim order with credited age, and a ticket whose supervisor died unstarted requeues.
import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import {
  startDispatchCluster, World, logins, call, enqueue, claim, start, finish, heartbeat, liftBackoff, setNow, downgradeToV9, tid,
} from './lane-dispatch-fixture.js';

const T0 = Date.parse('2030-01-01T00:00:00Z');
const at = (ms) => new Date(T0 + ms).toISOString();
const MINUTE = 60_000;
const AGED = 61 * MINUTE;
const NONE = { status: 'none' };
const DEAD = { kind: 'unfinished', reason: 'supervisor-died-while-queued', neverStarted: true };

/**
 * a1 holds ticket 1 (2 cores). Ticket 2 is the big one (7 cores), enqueued a second earlier so it is the oldest, and ticket 3 the small one (4 cores).
 * `big` and `small` override those tickets' options.
 */
async function aged(w, { big = {}, small = {} } = {}) {
  await setNow(w, at(-1000));
  await enqueue(await w.as(logins.s1), tid(2), { cpu: 7, ...big });
  await setNow(w, at(0));
  const s1 = await w.as(logins.s1);
  await enqueue(s1, tid(1), { cpu: 2, laneKey: 'lk-hold' });
  await enqueue(s1, tid(3), { cpu: 4, ...small });
  assert.equal((await claim(await w.as(logins.a1), { cpu: 2 })).id, tid(1), 'a1 takes the hold ticket');
}

const a1At = async (w, ms) => {
  await setNow(w, at(ms));
  return w.as(logins.a1);
};

const WEIGHTS = (freeWeight, weightLimit = 4) => ({ capacity: 12, freeWeight, weightLimit });

const GUARDS = {
  'a head whose weight does not fit now is reserved even though its cores and memory fit, and is claimed once the weight is free': async (w) => {
    await aged(w, { big: { cpu: 3, weight: 3 }, small: { cpu: 1, weight: 1 } });
    const a1 = await a1At(w, AGED);
    assert.deepEqual(await claim(a1, { cpu: 8, ...WEIGHTS(2) }), { status: 'none', reserved_for: tid(2) });
    assert.equal((await claim(a1, { cpu: 8, ...WEIGHTS(3) })).id, tid(2));
  },

  'a fresh priority 9 ticket outranks an aged priority 0 head: the reservation follows the claim order': async (w) => {
    await aged(w);
    await setNow(w, at(AGED));
    await enqueue(await w.as(logins.s1), tid(4), { cpu: 1, priority: 9 });
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6, ...WEIGHTS(8) })).id, tid(4));
  },

  'a ticket that does not age never reserves, however long it has waited': async (w) => {
    await aged(w, { big: { ages: false }, small: { ages: false } });
    assert.equal((await claim(await a1At(w, 10 * 60 * MINUTE), { cpu: 6, ...WEIGHTS(8) })).id, tid(3));
  },

  'a ticket heavier than the free weight is not claimed, and one that fits is': async (w) => {
    await setNow(w, at(0));
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { weight: 3 });
    await enqueue(s1, tid(2), { weight: 1 });
    const a1 = await w.as(logins.a1);
    assert.equal((await claim(a1, { freeWeight: 2 })).id, tid(2), 'the weight-3 ticket is first by order but does not fit');
    assert.deepEqual(await claim(a1, { freeWeight: 2 }), NONE);
    assert.equal((await claim(a1, { freeWeight: 3 })).id, tid(1));
  },

  'a ticket over the broker weight limit is charged at the limit, so it fits when that much is free': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1), { weight: 20 });
    assert.deepEqual(await claim(await w.as(logins.a1), { freeWeight: 3, weightLimit: 4 }), NONE);
    assert.equal((await claim(await w.as(logins.a1), { freeWeight: 4, weightLimit: 4 })).id, tid(1));
  },

  'a weight that is not a number counts as none and never breaks a claim': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1), { weight: 'heavy' });
    assert.equal((await claim(await w.as(logins.a1), { freeWeight: 0, weightLimit: 4 })).id, tid(1));
  },

  'an agent that sends no weight claims a heavy ticket as before': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1), { weight: 20 });
    assert.equal((await claim(await w.as(logins.a1))).id, tid(1));
    await enqueue(await w.as(logins.s1), tid(2), { weight: 20 });
    assert.equal((await claim(await w.as(logins.a1), { capacity: 12 })).id, tid(2), 'the seven-argument call of a 1099 agent too');
  },

  'an elastic ticket at its floor must also fit the free weight, and is granted the free cores once it does': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8, minCpu: 4, weight: 3 });
    const a1 = await a1At(w, 11 * MINUTE);
    assert.deepEqual(await claim(a1, { cpu: 6.5, freeWeight: 2, weightLimit: 4 }), NONE, 'the floor fits the cores, the weight does not');
    const got = await claim(a1, { cpu: 6.5, freeWeight: 3, weightLimit: 4 });
    assert.equal(got.id, tid(1));
    assert.equal(got.cpu_cores, 6, 'the grant is the free cores, at least the floor');
  },

  'an elastic head too heavy to fit now is reserved at its floor: the weight it needs is the clamped weight': async (w) => {
    await aged(w, { big: { cpu: 8, minCpu: 4, weight: 20 }, small: { cpu: 1, weight: 1 } });
    const a1 = await a1At(w, AGED);
    assert.deepEqual(await claim(a1, { cpu: 6, ...WEIGHTS(3) }), { status: 'none', reserved_for: tid(2) });
    const got = await claim(a1, { cpu: 6, ...WEIGHTS(4) });
    assert.deepEqual([got.id, got.cpu_cores], [tid(2), 6]);
  },

  'ld_register tells an agent the queue takes weight figures': async (w) => {
    assert.equal((await call(await w.as(logins.a1), 'ld_register', 't', 1, null)).claim_weight, true);
  },

  'a queue still at schema version 9 answers the seven-argument claim and has no nine-argument one': async (w) => {
    await downgradeToV9(w);
    await enqueue(await w.as(logins.s1), tid(1));
    const a1 = await w.as(logins.a1);
    await assert.rejects(claim(a1, { freeWeight: 4 }), { code: '42883' });
    assert.equal((await claim(a1, { capacity: 12 })).id, tid(1));
    assert.equal((await call(a1, 'ld_register', 't', 1, null)).claim_weight, undefined, 'a v9 queue does not advertise the weight figures');
  },

  'an unstarted ticket whose supervisor died goes back to the queue instead of finishing unfinished, and a replayed finish is fenced': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const a1 = await w.as(logins.a1);
    const got = await claim(a1);
    await start(a1, got);
    assert.deepEqual(await finish(a1, got, DEAD), { fenced: false, state: 'queued' });
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.state, row.requeues, row.host, row.claim_token, row.result, row.started_at, row.finished_at, row.epoch], ['queued', 1, null, null, null, null, null, got.epoch]);
    assert.deepEqual(await finish(a1, got, DEAD), { fenced: true, state: 'queued' }, 'a retry of the same finish changes nothing');
    assert.deepEqual(await claim(a1), NONE, 'the requeue backs off');
    await liftBackoff(w);
    const again = await claim(a1);
    assert.deepEqual([again.id, again.epoch], [tid(1), got.epoch + 1]);
  },

  'the third requeue is the last: a fourth dead supervisor finishes the ticket unfinished': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const a1 = await w.as(logins.a1);
    for (const n of [1, 2, 3]) {
      await liftBackoff(w);
      const got = await claim(a1);
      await start(a1, got);
      assert.deepEqual(await finish(a1, got, DEAD), { fenced: false, state: 'queued' }, `requeue ${n}`);
    }
    await liftBackoff(w);
    const last = await claim(a1);
    await start(a1, last);
    assert.deepEqual(await finish(a1, last, DEAD), { fenced: false, state: 'finished' });
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.state, row.requeues, row.result.kind, row.result.reason], ['finished', 3, 'unfinished', 'supervisor-died-while-queued']);
    assert.deepEqual([row.result.requeuesExhausted, row.result.requeues], [true, 3], 'the exhausted state is visible in the result');
  },

  'only a dead supervisor that proves nothing ran is requeued: every other result finishes the ticket': async (w) => {
    const s1 = await w.as(logins.s1);
    const a1 = await w.as(logins.a1);
    const others = [
      { ...DEAD, neverStarted: false }, { kind: 'unfinished', reason: 'supervisor-died-while-queued' }, { ...DEAD, reason: 'withdrawn' },
      { ...DEAD, kind: 'rejected' }, { kind: 'completed', exit: 0 },
    ];
    for (const [i, result] of others.entries()) {
      await enqueue(s1, tid(i + 1));
      const got = await claim(a1);
      await start(a1, got);
      assert.deepEqual(await finish(a1, got, result), { fenced: false, state: 'finished' }, JSON.stringify(result));
      const row = await w.ticket(tid(i + 1));
      assert.deepEqual([row.requeues, row.result.requeuesExhausted], [0, undefined]);
    }
  },

  'a ticket with a cancel requested finishes unfinished even when its supervisor died unstarted': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1));
    const a1 = await w.as(logins.a1);
    const got = await claim(a1);
    await start(a1, got);
    await call(s1, 'ld_cancel', tid(1));
    assert.deepEqual(await finish(a1, got, DEAD), { fenced: false, state: 'finished' });
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.result.kind, row.result.requeuesExhausted], ['unfinished', undefined]);
  },

  'a late finish, start or heartbeat from the old attempt is fenced after a supervisor-died requeue, and never overwrites the newer claim': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const a1 = await w.as(logins.a1);
    const old = await claim(a1);
    await start(a1, old);
    assert.deepEqual(await finish(a1, old, DEAD), { fenced: false, state: 'queued' });
    const late = { kind: 'completed', exit: 0, from: 'old' };
    assert.deepEqual(await finish(a1, old, late), { fenced: true, state: 'queued' }, 'fenced while queued');
    assert.equal(await start(a1, old), false);
    assert.equal((await heartbeat(a1, old)).fenced, true);
    assert.deepEqual(await claim(a1), NONE, 'the requeue backs off before the next claim');
    await liftBackoff(w);
    const current = await claim(a1);
    assert.equal(current.epoch, old.epoch + 1);
    await start(a1, current);
    assert.deepEqual(await finish(a1, old, late), { fenced: true, state: 'running' }, 'fenced against the newer claim');
    assert.equal(await start(a1, old), false);
    assert.equal((await w.ticket(tid(1))).result, null);
    assert.deepEqual(await finish(a1, current, { kind: 'completed', exit: 0, from: 'new' }), { fenced: false, state: 'finished' });
    assert.deepEqual(await finish(a1, old, late), { fenced: true, state: 'finished' });
    assert.equal((await w.ticket(tid(1))).result.from, 'new', 'the newer result stands');
  },

};

const HEAD_ORDER = "AND NOT (t.lane_key = ANY (coalesce(p_held_keys, '{}')))\n     AND t.requires <@ coalesce(p_has_tools, '{}')\n   ORDER BY lane_dispatch.eff_score(t.priority, t.ages, extract(epoch FROM v_now - t.created_at)) DESC,\n            CASE WHEN lane_dispatch.eff_score(t.priority, t.ages, extract(epoch FROM v_now - t.created_at)) >= 2.0\n                  AND extract(epoch FROM v_now - t.created_at) >= lane_dispatch.age_horizon_s() THEN t.created_at END NULLS LAST,\n            lane_dispatch.scarcity(t.requires_tags, t.repo_key), t.created_at, t.id LIMIT 1;";
const WEIGHT_FIT = "AND lane_dispatch.ticket_weight(t.header, p_weight_limit) <= coalesce(p_free_weight::numeric, 'Infinity'::numeric)";
const HEAD_WEIGHT_FIT = " AND v_head.weight <= coalesce(p_free_weight::numeric, 'Infinity'::numeric)";
const HEAD_FITS = 'a head whose weight does not fit now is reserved even though its cores and memory fit, and is claimed once the weight is free';
const FRESH = 'a fresh priority 9 ticket outranks an aged priority 0 head: the reservation follows the claim order';
const HEAVIER = 'a ticket heavier than the free weight is not claimed, and one that fits is';
const LIMIT = 'a ticket over the broker weight limit is charged at the limit, so it fits when that much is free';
const OLD = 'an agent that sends no weight claims a heavy ticket as before';
const ELASTIC = 'an elastic ticket at its floor must also fit the free weight, and is granted the free cores once it does';
const CANARIES = [
  [HEAD_FITS, HEAD_WEIGHT_FIT, ''],
  [FRESH, HEAD_ORDER, "AND NOT (t.lane_key = ANY (coalesce(p_held_keys, '{}')))\n     AND t.requires <@ coalesce(p_has_tools, '{}')\n   ORDER BY t.created_at, t.id LIMIT 1;"],
  ['a ticket that does not age never reserves, however long it has waited', "CASE WHEN t.ages THEN v_now - t.created_at ELSE interval '0' END", 'v_now - t.created_at'],
  [HEAVIER, WEIGHT_FIT, ''],
  [LIMIT, "coalesce(p_weight_limit::numeric, 'Infinity'::numeric)", "'Infinity'::numeric"],
  ['a weight that is not a number counts as none and never breaks a claim', "CASE WHEN jsonb_typeof(p_header->'weight') = 'number' THEN (p_header->>'weight')::numeric ELSE 0 END", "(p_header->>'weight')::numeric"],
  [OLD, "coalesce(p_free_weight::numeric, 'Infinity'::numeric)", 'coalesce(p_free_weight::numeric, 0)'],
  [OLD, "coalesce(p_free_weight::numeric, 'Infinity'::numeric)", 'p_free_weight::numeric'],
  [ELASTIC, WEIGHT_FIT, ''],
  ['an elastic head too heavy to fit now is reserved at its floor: the weight it needs is the clamped weight', "coalesce(p_weight_limit::numeric, 'Infinity'::numeric)", "'Infinity'::numeric"],
  ['ld_register tells an agent the queue takes weight figures', "'claim_weight', true", "'claim_weight', false"],
  ['an unstarted ticket whose supervisor died goes back to the queue instead of finishing unfinished, and a replayed finish is fenced', 'IF v_t.requeues < 3 THEN\n        v_state := lane_dispatch.return_to_queue(p_id, lane_dispatch.now());', "IF false THEN\n        v_state := lane_dispatch.return_to_queue(p_id, lane_dispatch.now());"],
  ['an unstarted ticket whose supervisor died goes back to the queue instead of finishing unfinished, and a replayed finish is fenced', 'v_state := lane_dispatch.return_to_queue(p_id, lane_dispatch.now());\n        PERFORM pg_notify', "v_state := 'queued';\n        PERFORM pg_notify"],
  ['the third requeue is the last: a fourth dead supervisor finishes the ticket unfinished', 'IF v_t.requeues < 3 THEN\n        v_state', 'IF v_t.requeues < 300 THEN\n        v_state'],
  ['the third requeue is the last: a fourth dead supervisor finishes the ticket unfinished', "p_result := p_result || jsonb_build_object('requeuesExhausted', true, 'requeues', v_t.requeues);", ''],
  ['only a dead supervisor that proves nothing ran is requeued: every other result finishes the ticket', " AND p_result->>'neverStarted' = 'true', false)", ', false)'],
  ['only a dead supervisor that proves nothing ran is requeued: every other result finishes the ticket', " AND p_result->>'reason' = 'supervisor-died-while-queued'", ''],
  ['only a dead supervisor that proves nothing ran is requeued: every other result finishes the ticket', "coalesce(p_result->>'kind' = 'unfinished' AND ", 'coalesce('],
  ['a ticket with a cancel requested finishes unfinished even when its supervisor died unstarted', 'IF v_dead AND NOT v_t.cancel_requested THEN', 'IF v_dead THEN'],
  ['a late finish, start or heartbeat from the old attempt is fenced after a supervisor-died requeue, and never overwrites the newer claim',
    "RETURN coalesce(p_row_state IN ('claimed','running') AND p_row_epoch = p_epoch AND p_row_token = p_token AND p_row_host = p_host, false);", "RETURN coalesce(p_row_state IN ('claimed','running') AND p_row_host = p_host, false);"],
];

describe('lane_dispatch claim weight, head order and supervisor-died requeue (BRAIN-1106) on a disposable Postgres', { skip: pgSkipOption, timeout: 900000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) {
    test(name, async () => scenario(await World.create(cluster)));
  }

  for (const [name, from, to] of CANARIES) {
    test(`canary: with "${from.slice(0, 50).replaceAll('\n', ' ')}" mutated, "${name}" fails`, async (t) => {
      let failure;
      await GUARDS[name](await World.create(cluster, { mutate: [from, to] })).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
