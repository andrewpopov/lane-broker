import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, tid } from './lane-dispatch-fixture.js';

const GUARDS = {
  'ld_claim offers a ticket that requires tools only to an agent naming all of them, and the older 4-argument claim never takes it': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1), { requires: ['redis-server', 'unzip'] });
    await enqueue(s1, tid(2));
    assert.equal((await claim(a1)).id, tid(2), 'a ticket with no requires is still claimed by the old call');
    assert.equal((await claim(a1, { has: [] })).status, 'none');
    assert.equal((await claim(a1, { has: ['redis-server'] })).status, 'none', 'one of two tools is not enough');
    assert.equal((await claim(a1)).status, 'none', 'the old call never takes a ticket that requires a tool');
    const taken = await claim(a1, { has: ['redis-server', 'unzip', 'ffmpeg'] });
    assert.equal(taken.id, tid(1));
    assert.deepEqual(taken.requires, ['redis-server', 'unzip'], 'claim_view hands the agent the tools to re-check before it starts');
  },

  'ld_enqueue_fenced stores requires, and the old call shape stores none': async (w) => {
    const s1 = await w.as(logins.s1);
    const ref = await call(s1, 'ld_queue_ref');
    const base = [{ protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x' }, [], 1, 1000, 0];
    await call(s1, 'ld_enqueue_fenced', ref, tid(3), 'repoA', 'test', 'lk3', ...base, 1, true, null, null, [], ['ffmpeg']);
    await call(s1, 'ld_enqueue_fenced', ref, tid(4), 'repoA', 'test', 'lk4', ...base);
    assert.deepEqual([(await w.ticket(tid(3))).requires, (await w.ticket(tid(4))).requires], [['ffmpeg'], []]);
  },

  'ld_needed_tools lists the distinct, sorted tools of the queued tickets this agent is eligible for': async (w) => {
    const [s1, a1, a2] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2)];
    await enqueue(s1, tid(1), { requires: ['unzip', 'redis-server'] });
    await enqueue(s1, tid(2), { requires: ['redis-server'] });
    await enqueue(s1, tid(3), { repo: 'repoB', requires: ['ffmpeg'] });
    await enqueue(s1, tid(4), { tags: ['gpu'], requires: ['nvidia-smi'] });
    await enqueue(s1, tid(5), { requires: ['jq'] });
    await (await w.su()).query("UPDATE lane_dispatch.tickets SET state = 'finished' WHERE id = $1", [tid(5)]);
    assert.deepEqual(await call(a1, 'ld_needed_tools'), ['redis-server', 'unzip'], 'a1 may run repoA only, and has no gpu tag');
    assert.deepEqual(await call(a2, 'ld_needed_tools'), ['ffmpeg', 'redis-server', 'unzip']);
  },
};

const CANARIES = [
  ['ld_claim offers a ticket that requires tools only to an agent naming all of them, and the older 4-argument claim never takes it', "     AND t.requires <@ coalesce(p_has_tools, '{}')\n", ''],
  ['ld_claim offers a ticket that requires tools only to an agent naming all of them, and the older 4-argument claim never takes it', "coalesce(p_requires, '{}'), CASE WHEN", "'{}', CASE WHEN"],
  ['ld_claim offers a ticket that requires tools only to an agent naming all of them, and the older 4-argument claim never takes it', "'requires', t.requires, ", ''],
  ['ld_enqueue_fenced stores requires, and the old call shape stores none', 'p_avoid_tags, p_requires, p_min_cpu_cores, p_dispatch_key);', 'p_avoid_tags, NULL, p_min_cpu_cores, p_dispatch_key);'],
  ['ld_needed_tools lists the distinct, sorted tools of the queued tickets this agent is eligible for',
    "WHERE t.state = 'queued' AND lane_dispatch.eligible(t.requires_tags, t.repo_key, v_me.tags, v_me.allowed_repos));", "WHERE t.state = 'queued');"],
  ['ld_needed_tools lists the distinct, sorted tools of the queued tickets this agent is eligible for', "WHERE t.state = 'queued' AND lane_dispatch.eligible(", 'WHERE lane_dispatch.eligible('],
];

describe('lane_dispatch pull dispatch honours requires (BRAIN-1135) on a disposable Postgres', { skip: pgSkipOption, timeout: 600000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) test(name, async () => scenario(await World.create(cluster)));

  for (const [name, from, to] of CANARIES) {
    test(`canary: with "${from.trim().slice(0, 50).replaceAll('\n', ' ')}" mutated, "${name}" fails`, async (t) => {
      let failure;
      await GUARDS[name](await World.create(cluster, { mutate: [from, to] })).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      assert.equal(failure.code, 'ERR_ASSERTION', `failed, but not on an assertion: ${failure.stack}`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
