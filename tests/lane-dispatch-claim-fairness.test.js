import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import {
  startDispatchCluster, World, logins, call, enqueue, claim, start, finish, heartbeat, liftBackoff, setNow, downgradeToV6, tid,
} from './lane-dispatch-fixture.js';

const T0 = Date.parse('2030-01-01T00:00:00Z');
const at = (ms) => new Date(T0 + ms).toISOString();
const AGED = 900_000;
const CAP = (extra = {}) => ({ totalCpu: 8, totalMem: 1e9, freeWeight: 8, weightLimit: 8, ...extra });
const DEAD = { kind: 'unfinished', reason: 'supervisor-died-while-queued', neverStarted: true };

/**
 * a1 holds ticket 1 (2 cores), ticket 2 is the big one (7 cores, enqueued a second earlier so it is the oldest) and ticket 3 the small one (4 cores).
 * `held: false` leaves a1 with nothing claimed; `hold`, `big` and `small` override those tickets' options.
 */
async function aged(w, { held = true, hold = {}, big = {}, small = {} } = {}) {
  await setNow(w, at(-1000));
  await enqueue(await w.as(logins.s1), tid(2), { cpu: 7, ...big });
  await setNow(w, at(0));
  const s1 = await w.as(logins.s1);
  await enqueue(s1, tid(1), { cpu: 2, laneKey: 'lk-hold', ...hold });
  await enqueue(s1, tid(3), { cpu: 4, ...small });
  if (held) assert.equal((await claim(await w.as(logins.a1), { cpu: 2, capacity: CAP() })).id, tid(1), 'a1 takes the hold ticket');
}

const a1At = async (w, ms) => {
  await setNow(w, at(ms));
  return w.as(logins.a1);
};

const NONE = { status: 'none' };

const GUARDS = {
  'before the oldest ticket has waited reserve_after_s a smaller ticket passes it': async (w) => {
    await aged(w);
    assert.equal((await claim(await a1At(w, AGED - 2000), { cpu: 6, capacity: CAP() })).id, tid(3));
  },

  'once the oldest ticket has waited reserve_after_s the agent takes only it: none while it does not fit, the ticket itself once it does': async (w) => {
    await aged(w);
    const a1 = await a1At(w, AGED);
    assert.deepEqual(await claim(a1, { cpu: 6, capacity: CAP() }), NONE, 'the 4-core ticket fits but the aged 7-core head is reserved');
    const got = await claim(a1, { cpu: 7, capacity: CAP() });
    assert.equal(got.id, tid(2));
    assert.equal(got.status, 'claimed');
  },

  'the reserved ticket is held back for memory and weight as well as cores': async (w) => {
    await aged(w, { hold: { mem: 2e8 }, big: { cpu: 3, mem: 5e8, weight: 3 }, small: { cpu: 1, weight: 1 } });
    const a1 = await a1At(w, AGED);
    assert.deepEqual(await claim(a1, { cpu: 8, mem: 4e8, capacity: CAP() }), NONE, 'not enough free memory');
    assert.deepEqual(await claim(a1, { cpu: 8, capacity: CAP({ freeWeight: 2 }) }), NONE, 'not enough free weight');
    assert.equal((await claim(a1, { cpu: 8, capacity: CAP({ freeWeight: 3 }) })).id, tid(2));
  },

  'a head that needs more cores than the agent could ever have is not reserved for': async (w) => {
    await aged(w);
    assert.equal((await claim(await a1At(w, AGED), { cpu: 5, capacity: CAP({ totalCpu: 6 }) })).id, tid(3), 'the 7-core head fits free plus held but not the total');
  },

  'a head that needs more memory than the agent could ever have is not reserved for': async (w) => {
    await aged(w, { hold: { mem: 2e8 }, big: { cpu: 3, mem: 5e8 } });
    assert.equal((await claim(await a1At(w, AGED), { cpu: 8, mem: 4e8, capacity: CAP({ totalMem: 4e8 }) })).id, tid(3), 'the head fits free plus held memory but not the total');
  },

  'the head is the oldest ticket this agent is eligible for: one it may not take reserves nothing': async (w) => {
    await aged(w, { big: { tags: ['gpu'] } });
    assert.equal((await claim(await a1At(w, AGED), { cpu: 6, capacity: CAP() })).id, tid(3));
  },

  'a head that conflicts with a lane key the agent holds is not reserved for': async (w) => {
    await aged(w, { big: { laneKey: 'lk-hold' } });
    assert.equal((await claim(await a1At(w, AGED), { cpu: 6, held: ['lk-hold'], capacity: CAP() })).id, tid(3));
  },

  'an agent with nothing held and a head above its free cores reserves nothing, so outside load never idles it': async (w) => {
    await aged(w, { held: false });
    assert.equal((await claim(await a1At(w, AGED), { cpu: 6, capacity: CAP() })).id, tid(1), 'the oldest of the tickets that fit goes first');
  },

  'a holding agent whose free cores plus held cores stay below the head does not reserve': async (w) => {
    await aged(w);
    assert.equal((await claim(await a1At(w, AGED), { cpu: 4, capacity: CAP() })).id, tid(3), '4 free + 2 held < 7');
  },

  'a holding agent whose free memory plus held memory stays below the head does not reserve': async (w) => {
    await aged(w, { hold: { mem: 1e8 }, big: { cpu: 3, mem: 5e8 } });
    assert.equal((await claim(await a1At(w, AGED), { cpu: 8, mem: 3e8, capacity: CAP() })).id, tid(3), '3e8 free + 1e8 held < 5e8');
  },

  'a fresh priority 9 ticket outranks an aged priority 0 head: the reservation follows the claim order': async (w) => {
    await aged(w);
    await setNow(w, at(AGED));
    await enqueue(await w.as(logins.s1), tid(4), { cpu: 1, priority: 9 });
    assert.equal((await claim(await w.as(logins.a1), { cpu: 6, capacity: CAP() })).id, tid(4));
  },

  'a ticket that does not age never reserves, however long it has waited': async (w) => {
    await aged(w, { big: { ages: false }, small: { ages: false } });
    assert.equal((await claim(await a1At(w, 10 * AGED), { cpu: 6, capacity: CAP() })).id, tid(3));
  },

  'an old agent that sends no totals never reserves': async (w) => {
    await aged(w);
    assert.equal((await claim(await a1At(w, AGED), { cpu: 6 })).id, tid(3));
  },

  'reserve_after_s is the cluster setting, and 0 turns the reservation off': async (w) => {
    await (await w.su()).query('UPDATE lane_dispatch.cluster SET reserve_after_s = 60');
    await aged(w);
    assert.deepEqual(await claim(await a1At(w, 60_000), { cpu: 6, capacity: CAP() }), NONE, 'reserved at 60 s');
  },

  'with reserve_after_s at 0 an aged head reserves nothing': async (w) => {
    await (await w.su()).query('UPDATE lane_dispatch.cluster SET reserve_after_s = 0');
    await aged(w);
    assert.equal((await claim(await a1At(w, AGED), { cpu: 6, capacity: CAP() })).id, tid(3));
  },

  'a ticket heavier than the free weight is not claimed, and one that fits is': async (w) => {
    await setNow(w, at(0));
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1), { weight: 3 });
    await enqueue(s1, tid(2), { weight: 1 });
    const a1 = await w.as(logins.a1);
    assert.equal((await claim(a1, { capacity: CAP({ freeWeight: 2 }) })).id, tid(2), 'the weight-3 ticket is first by order but does not fit');
    assert.deepEqual(await claim(a1, { capacity: CAP({ freeWeight: 2 }) }), NONE);
    assert.equal((await claim(a1, { capacity: CAP({ freeWeight: 3 }) })).id, tid(1));
  },

  'a ticket over the broker weight limit is charged at the limit, so it fits when that much is free': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1), { weight: 20 });
    assert.deepEqual(await claim(await w.as(logins.a1), { capacity: CAP({ freeWeight: 3, weightLimit: 4 }) }), NONE);
    assert.equal((await claim(await w.as(logins.a1), { capacity: CAP({ freeWeight: 4, weightLimit: 4 }) })).id, tid(1));
  },

  'a weight that is not a number counts as none and never breaks a claim': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1), { weight: 'heavy' });
    assert.equal((await claim(await w.as(logins.a1), { capacity: CAP({ freeWeight: 0 }) })).id, tid(1));
  },

  'an old agent that sends no weight claims a heavy ticket as before': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1), { weight: 20 });
    assert.equal((await claim(await w.as(logins.a1))).id, tid(1));
  },

  'a queue still at schema version 6 answers the four-argument claim and has no eight-argument one': async (w) => {
    await downgradeToV6(w);
    await call(await w.as(logins.s1), 'ld_enqueue', tid(1), 'repoA', 'test', 'lk-1', { protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x' }, [], 1, 1000);
    const a1 = await w.as(logins.a1);
    await assert.rejects(claim(a1, { capacity: CAP() }), { code: '42883' });
    assert.equal((await claim(a1)).id, tid(1));
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

  'a head heavier than the broker weight limit is charged at the limit: reserved, and claimed once that much is free': async (w) => {
    await aged(w, { big: { weight: 20 } });
    const a1 = await a1At(w, AGED);
    assert.deepEqual(await claim(a1, { cpu: 7, capacity: CAP({ freeWeight: 3, weightLimit: 4 }) }), NONE);
    assert.equal((await claim(a1, { cpu: 7, capacity: CAP({ freeWeight: 4, weightLimit: 4 }) })).id, tid(2));
  },

  'the head is limited to the repositories the agent may take': async (w) => {
    await aged(w, { big: { repo: 'repoB' } });
    assert.equal((await claim(await a1At(w, AGED), { cpu: 6, capacity: CAP() })).id, tid(3));
  },
};

const RESERVE = 'a head that needs more cores than the agent could ever have is not reserved for';
const ONCE = 'once the oldest ticket has waited reserve_after_s the agent takes only it: none while it does not fit, the ticket itself once it does';
const CANARIES = [
  [ONCE, 'WHERE hd.credited >= v_reserve_after AND', 'WHERE false AND'],
  [ONCE, 'AND (v_reserved IS NULL OR t.id = v_reserved)', ''],
  [ONCE, 'AND t.cpu_cores <= p_free_cpu AND', 'AND'],
  ['before the oldest ticket has waited reserve_after_s a smaller ticket passes it', 'WHERE hd.credited >= v_reserve_after AND', 'WHERE true AND'],
  ['the reserved ticket is held back for memory and weight as well as cores', 'AND t.cpu_cores <= p_free_cpu AND t.memory_bytes <= p_free_mem AND NOT', 'AND t.cpu_cores <= p_free_cpu AND NOT'],
  ['the reserved ticket is held back for memory and weight as well as cores', "AND lane_dispatch.ticket_weight(t.header, p_weight_limit) <= coalesce(p_free_weight::numeric, 'Infinity'::numeric)", ''],
  [RESERVE, 'h.cpu_cores <= p_total_cpu AND', ''],
  [RESERVE, 'h.cpu_cores <= p_total_cpu AND', 'h.cpu_cores <= p_total_cpu + 100 AND'],
  ['a head that needs more memory than the agent could ever have is not reserved for', 'AND h.memory_bytes <= p_total_mem AND NOT', 'AND NOT'],
  ['the head is the oldest ticket this agent is eligible for: one it may not take reserves nothing',
    'lane_dispatch.eligible(h.requires_tags, h.repo_key, v_me.tags, v_me.allowed_repos)', 'true'],
  ['a head that conflicts with a lane key the agent holds is not reserved for', "AND h.memory_bytes <= p_total_mem AND NOT (h.lane_key = ANY (coalesce(p_held_keys, '{}')))", 'AND h.memory_bytes <= p_total_mem'],
  ['an agent with nothing held and a head above its free cores reserves nothing, so outside load never idles it', ' AND hd.cpu_cores <= p_free_cpu + v_held_cpu', ''],
  ['a holding agent whose free cores plus held cores stay below the head does not reserve', ' AND hd.cpu_cores <= p_free_cpu + v_held_cpu', ''],
  ['a holding agent whose free cores plus held cores stay below the head does not reserve', 'p_free_cpu + v_held_cpu', 'p_free_cpu + v_held_cpu + 100'],
  ['a holding agent whose free memory plus held memory stays below the head does not reserve', ' AND hd.memory_bytes <= p_free_mem + v_held_mem', ''],
  ['a fresh priority 9 ticket outranks an aged priority 0 head: the reservation follows the claim order',
    'ORDER BY lane_dispatch.eff_score(h.priority, h.ages, extract(epoch FROM v_now - h.created_at)) DESC,', 'ORDER BY h.created_at,'],
  ['a ticket that does not age never reserves, however long it has waited', "CASE WHEN h.ages THEN v_now - h.created_at ELSE interval '0' END", 'v_now - h.created_at'],
  ['an old agent that sends no totals never reserves', 'h.cpu_cores <= p_total_cpu AND h.memory_bytes <= p_total_mem AND NOT',
    'h.cpu_cores <= coalesce(p_total_cpu, 1e9) AND h.memory_bytes <= coalesce(p_total_mem, 1e12) AND NOT'],
  ['reserve_after_s is the cluster setting, and 0 turns the reservation off', 'make_interval(secs => c.reserve_after_s)', "interval '900 seconds'"],
  ['with reserve_after_s at 0 an aged head reserves nothing', "IF v_reserve_after > interval '0' THEN", 'IF true THEN'],
  ['a ticket heavier than the free weight is not claimed, and one that fits is', "AND lane_dispatch.ticket_weight(t.header, p_weight_limit) <= coalesce(p_free_weight::numeric, 'Infinity'::numeric)", ''],
  ['a ticket over the broker weight limit is charged at the limit, so it fits when that much is free', "coalesce(p_weight_limit::numeric, 'Infinity'::numeric)", "'Infinity'::numeric"],
  ['a weight that is not a number counts as none and never breaks a claim', "CASE WHEN jsonb_typeof(p_header->'weight') = 'number' THEN (p_header->>'weight')::numeric ELSE 0 END", "(p_header->>'weight')::numeric"],
  ['an old agent that sends no weight claims a heavy ticket as before', "coalesce(p_free_weight::numeric, 'Infinity'::numeric)", 'coalesce(p_free_weight::numeric, 0)'],
  ['an old agent that sends no weight claims a heavy ticket as before', "coalesce(p_free_weight::numeric, 'Infinity'::numeric)", 'p_free_weight::numeric'],
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
  ['a head heavier than the broker weight limit is charged at the limit: reserved, and claimed once that much is free', "coalesce(p_weight_limit::numeric, 'Infinity'::numeric)", "'Infinity'::numeric"],
  ['the head is limited to the repositories the agent may take', 'lane_dispatch.eligible(h.requires_tags, h.repo_key, v_me.tags, v_me.allowed_repos)', 'true'],
];

describe('lane_dispatch claim fairness (BRAIN-1106) on a disposable Postgres', { skip: pgSkipOption, timeout: 900000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) {
    test(name, async () => scenario(await World.create(cluster), () => World.create(cluster)));
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
