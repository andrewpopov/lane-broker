// BRAIN-1099: elastic pull claims (a ticket's floor, after elastic_floor_after_s) and the aged-head reservation in ld_claim.
import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, finish, setNow, tid } from './lane-dispatch-fixture.js';

const T0 = Date.parse('2030-01-01T00:00:00Z');
const at = (ms) => new Date(T0 + ms).toISOString();
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;

/** Enqueues `n` 4-core tickets one per `everyMs` from `fromMs`, claiming after each with `free` cores and finishing what it got; returns the claims. */
async function smallStream(w, { fromMs, n, everyMs, free, idBase }) {
  const claims = [];
  for (let i = 0; i < n; i++) {
    await setNow(w, at(fromMs + i * everyMs));
    await enqueue(await w.as(logins.s1), tid(idBase + i), { cpu: 4 });
    const got = await claim(await w.as(logins.a1), { cpu: free });
    claims.push(got);
    if (got.status === 'claimed') await finish(await w.as(logins.a1), got);
  }
  return claims;
}

const GUARDS = {
  'an 8-core min-4 ticket behind a stream of 4-core tickets on a 6-free-core runner is claimed, at 6 cores': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8, minCpu: 4 });
    const claims = await smallStream(w, { fromMs: MINUTE, n: 12, everyMs: 5 * MINUTE, free: 6, idBase: 100 });
    const big = claims.find((c) => c.id === tid(1));
    assert.ok(big, `the elastic ticket was never claimed: ${claims.map((c) => c.id).join(', ')}`);
    assert.equal(big.cpu_cores, 6, 'claimed at the largest integer grant that fits');
    assert.equal(Number((await w.ticket(tid(1))).claimed_cpu_cores), 6);
  },

  'an elastic ticket shrinks only after it has waited elastic_floor_after_s, and a full fit keeps its declared cores': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8, minCpu: 4 });
    await setNow(w, at(9 * MINUTE));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6 })).status, 'none', 'before 10 minutes it needs all 8 cores');
    await setNow(w, at(10 * MINUTE));
    const got = await claim(await w.as(logins.a1), { cpu: 6.5 });
    assert.equal(got.status, 'claimed');
    assert.equal(got.cpu_cores, 6, 'a grant is an integer');
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 8, minCpu: 4 });
    await setNow(w, at(30 * MINUTE));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 12 })).cpu_cores, 8);
  },

  'the elastic wait is the cluster elastic_floor_after_s': async (w) => {
    await (await w.su()).query('UPDATE lane_dispatch.cluster SET elastic_floor_after_s = 60');
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8, minCpu: 4 });
    await setNow(w, at(2 * MINUTE));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 4 })).cpu_cores, 4);
  },

  'a ticket stores min_cpu_cores only when it is below cpu_cores, and a ticket without one never shrinks': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8, minCpu: 4 });
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 8, minCpu: 8 });
    await enqueue(await w.as(logins.s1), tid(3), { cpu: 8 });
    assert.equal(Number((await w.ticket(tid(1))).min_cpu_cores), 4);
    assert.equal((await w.ticket(tid(2))).min_cpu_cores, null);
    assert.equal((await w.ticket(tid(3))).min_cpu_cores, null);
    await setNow(w, at(HOUR));
    const got = await claim(await w.as(logins.a1), { cpu: 6 });
    assert.equal(got.id, tid(1));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6 })).status, 'none', 'the fixed 8-core tickets need 8');
  },

  'a runner holds back for an aged head it could fit: no backfill until the head fits, then it takes the head': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(9), { cpu: 2 });
    const running = await claim(await w.as(logins.a1), { cpu: 12, capacity: 12 });
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8 });
    await setNow(w, at(61 * MINUTE));
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 4 });
    const held = await claim(await w.as(logins.a1), { cpu: 6, capacity: 12 });
    assert.deepEqual(held, { status: 'none', reserved_for: tid(1) });
    assert.equal((await w.ticket(tid(2))).state, 'queued', 'the 4-core ticket was not backfilled past the head');
    await finish(await w.as(logins.a1), running);
    const head = await claim(await w.as(logins.a1), { cpu: 8, capacity: 12 });
    assert.equal(head.id, tid(1));
  },

  'the reservation holds only on a runner that could fit the head, holds a claim, reports its budget, and only for an aged head': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(9), { cpu: 1 });
    const running = await claim(await w.as(logins.a1), { cpu: 12, capacity: 12 });
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8 });
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 4 });
    await setNow(w, at(30 * MINUTE));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6, capacity: 12 })).id, tid(2), 'a head younger than age_horizon_s holds nothing');
    await finish(await w.as(logins.a1), running);
    await enqueue(await w.as(logins.s1), tid(3), { cpu: 1 });
    await enqueue(await w.as(logins.s1), tid(4), { cpu: 4 });
    await enqueue(await w.as(logins.s1), tid(5), { cpu: 4 });
    await setNow(w, at(2 * HOUR));
    const small = await claim(await w.as(logins.a1), { cpu: 6, capacity: 6 });
    assert.equal(small.id, tid(3), 'a runner whose budget can never fit the head backfills');
    await finish(await w.as(logins.a1), small);
    assert.equal((await claim(await w.as(logins.a1), { cpu: 5 })).id, tid(4), 'an agent that sends no budget backfills (the pre-1099 call)');
  },

  'a head this runner could never fit on memory holds nothing, so the runner backfills': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(9), { cpu: 1 });
    await claim(await w.as(logins.a1), { cpu: 12, capacity: 12 });
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8, mem: 2e10 });
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 4 });
    await setNow(w, at(2 * HOUR));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6, capacity: 12, capacityMem: 1e10 })).id, tid(2));
  },

  'a head too big for this runner is passed over, and the reservation goes to the next aged ticket it could fit': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(9), { cpu: 1 });
    await claim(await w.as(logins.a1), { cpu: 12, capacity: 12 });
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 16 });
    await setNow(w, at(MINUTE));
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 8 });
    await setNow(w, at(2 * HOUR));
    await enqueue(await w.as(logins.s1), tid(3), { cpu: 4 });
    assert.deepEqual(await claim(await w.as(logins.a1), { cpu: 6, capacity: 12 }), { status: 'none', reserved_for: tid(2) });
  },

  'ld_register tells an agent the queue takes its capacity, so a new agent sends it only to a queue that has the reservation': async (w) => {
    assert.equal((await call(await w.as(logins.a1), 'ld_register', 'test', 1, null)).head_reserve, true);
  },

  'a drained runner never holds back: with no claim of its own running, it backfills': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8 });
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 4 });
    await setNow(w, at(2 * HOUR));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6, capacity: 12 })).id, tid(2));
  },

  'the reservation is per runner: another eligible runner that cannot fit the head still backfills': async (w) => {
    await call(await w.as(logins.admin), 'ld_set_principal', logins.a2, 'agent', 'a2', ['linux'], ['*'], 2);
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(9), { cpu: 1 });
    assert.equal((await claim(await w.as(logins.a1), { cpu: 12, capacity: 12 })).id, tid(9));
    await enqueue(await w.as(logins.s1), tid(8), { cpu: 1 });
    assert.equal((await claim(await w.as(logins.a2), { cpu: 12, capacity: 6 })).id, tid(8));
    await enqueue(await w.as(logins.s1), tid(1), { cpu: 8 });
    await enqueue(await w.as(logins.s1), tid(2), { cpu: 4 });
    await setNow(w, at(2 * HOUR));
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6, capacity: 12 })).reserved_for, tid(1));
    assert.equal((await claim(await w.as(logins.a2), { cpu: 6, capacity: 6 })).id, tid(2), 'a runner that can never fit the head is not held by another runner\'s reservation');
  },
};

const CLAIM_NEED = 'p_min_cpu_cores IS NOT NULL AND p_waited >= p_floor_after THEN least(ceil(p_min_cpu_cores), p_cpu_cores)';
const FIT = 'AND lane_dispatch.claim_need(t.cpu_cores, t.min_cpu_cores, v_now - t.created_at, v_floor_after) <= p_free_cpu AND t.memory_bytes <= p_free_mem';
const CAPACITY_FIT = 'lane_dispatch.claim_need(t.cpu_cores, t.min_cpu_cores, v_now - t.created_at, v_floor_after) <= p_capacity_cpu AND t.memory_bytes <= coalesce(p_capacity_mem, p_free_mem)';
const RESERVE = "      RETURN jsonb_build_object('status', 'none', 'reserved_for', v_head.id);";
const CANARIES = [
  ['an 8-core min-4 ticket behind a stream of 4-core tickets on a 6-free-core runner is claimed, at 6 cores', FIT, 'AND t.cpu_cores <= p_free_cpu AND t.memory_bytes <= p_free_mem'],
  ['an 8-core min-4 ticket behind a stream of 4-core tickets on a 6-free-core runner is claimed, at 6 cores', 'ELSE greatest(v_t.need, floor(p_free_cpu)) END', 'ELSE v_t.need END'],
  ['an 8-core min-4 ticket behind a stream of 4-core tickets on a 6-free-core runner is claimed, at 6 cores',
    "coalesce(p_requires, '{}'), CASE WHEN p_min_cpu_cores < p_cpu_cores THEN p_min_cpu_cores END,", "coalesce(p_requires, '{}'), NULL,"],
  ['an elastic ticket shrinks only after it has waited elastic_floor_after_s, and a full fit keeps its declared cores', CLAIM_NEED, 'p_min_cpu_cores IS NOT NULL THEN least(ceil(p_min_cpu_cores), p_cpu_cores)'],
  ['an elastic ticket shrinks only after it has waited elastic_floor_after_s, and a full fit keeps its declared cores', 'CASE WHEN v_t.cpu_cores <= p_free_cpu THEN v_t.cpu_cores ELSE', 'CASE WHEN false THEN v_t.cpu_cores ELSE'],
  ['an elastic ticket shrinks only after it has waited elastic_floor_after_s, and a full fit keeps its declared cores', 'ELSE greatest(v_t.need, floor(p_free_cpu)) END', 'ELSE greatest(v_t.need, p_free_cpu) END'],
  ['the elastic wait is the cluster elastic_floor_after_s', 'make_interval(secs => c.elastic_floor_after_s)', "interval '600 seconds'"],
  ['a ticket stores min_cpu_cores only when it is below cpu_cores, and a ticket without one never shrinks',
    "coalesce(p_requires, '{}'), CASE WHEN p_min_cpu_cores < p_cpu_cores THEN p_min_cpu_cores END,", "coalesce(p_requires, '{}'), p_min_cpu_cores,"],
  ['a runner holds back for an aged head it could fit: no backfill until the head fits, then it takes the head', RESERVE, ''],
  ['the reservation holds only on a runner that could fit the head, holds a claim, reports its budget, and only for an aged head',
    'IF FOUND AND extract(epoch FROM v_now - v_head.created_at) >= lane_dispatch.age_horizon_s()', 'IF FOUND'],
  ['the reservation holds only on a runner that could fit the head, holds a claim, reports its budget, and only for an aged head', CAPACITY_FIT, 'true'],
  ['a head this runner could never fit on memory holds nothing, so the runner backfills', 'AND t.memory_bytes <= coalesce(p_capacity_mem, p_free_mem)', ''],
  ['a head too big for this runner is passed over, and the reservation goes to the next aged ticket it could fit', CAPACITY_FIT, 'true AND t.memory_bytes <= coalesce(p_capacity_mem, p_free_mem)'],
  ['ld_register tells an agent the queue takes its capacity, so a new agent sends it only to a queue that has the reservation', "'head_reserve', true", "'head_reserve', false"],
  ['a drained runner never holds back: with no claim of its own running, it backfills', 'IF p_capacity_cpu IS NOT NULL AND v_held > 0 THEN', 'IF p_capacity_cpu IS NOT NULL THEN'],
];

test('the head query shares the claim query\'s eligibility and order, so the reservation never names a ticket the claim would rank differently', () => {
  const sql = fs.readFileSync(new URL('../sql/lane/functions.sql', import.meta.url), 'utf8');
  const body = sql.slice(sql.indexOf('CREATE OR REPLACE FUNCTION lane_dispatch.ld_claim('), sql.indexOf('CREATE OR REPLACE FUNCTION lane_dispatch.ld_start('));
  const queries = [...body.matchAll(/FROM lane_dispatch\.tickets t\n([\s\S]*?)LIMIT 1/g)].map((m) => m[1].split('\n').filter((line) => !/<= p_(free|capacity)_cpu/.test(line)).join('\n'));
  assert.equal(queries.length, 2);
  assert.equal(queries[1], queries[0]);
});

describe('lane_dispatch elastic pull claims and the aged-head reservation (BRAIN-1099) on a disposable Postgres', { skip: pgSkipOption, timeout: 900000 }, () => {
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
