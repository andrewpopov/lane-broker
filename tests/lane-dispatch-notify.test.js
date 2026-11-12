import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, start, release, tid } from './lane-dispatch-fixture.js';

const WAIT_MS = 1500;

/** a1's own LISTEN connection, opened after the scenario's setup so only the call under test can reach it. */
async function listenAsA1(w) {
  const client = await w.cluster.client(w.db, logins.a1);
  const heard = [];
  client.on('notification', (n) => heard.push(n));
  await client.query('LISTEN ld_work');
  await client.query('LISTEN ld_host_a1');
  return {
    heard,
    async expect(channel) {
      for (let waited = 0; waited < WAIT_MS && !heard.some((n) => n.channel === channel); waited += 25) await new Promise((r) => setTimeout(r, 25));
      assert.ok(heard.some((n) => n.channel === channel), `no ${channel} within ${WAIT_MS} ms; heard ${JSON.stringify(heard.map((n) => n.channel))}`);
      assert.ok(heard.filter((n) => n.channel === channel).every((n) => n.payload === ''), 'the hint carries no payload');
    },
  };
}

async function claimedByA1(w, { started = false } = {}) {
  const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
  await enqueue(s1, tid(1));
  const got = await claim(a1);
  if (started) await start(a1, got);
  return got;
}

const GUARDS = {
  'ld_enqueue sends an empty ld_work hint': async (w) => {
    const s1 = await w.as(logins.s1);
    const listener = await listenAsA1(w);
    await enqueue(s1, tid(1));
    await listener.expect('ld_work');
  },

  'ld_release to queued sends an empty ld_work hint': async (w) => {
    const a1 = await w.as(logins.a1);
    const got = await claimedByA1(w);
    const listener = await listenAsA1(w);
    assert.equal((await release(a1, got)).state, 'queued');
    await listener.expect('ld_work');
  },

  'ld_reap that requeues an expired claim sends an empty ld_work hint': async (w) => {
    const [admin, su] = [await w.as(logins.admin), await w.su()];
    await claimedByA1(w);
    await su.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '1 second'");
    const listener = await listenAsA1(w);
    assert.equal((await call(admin, 'ld_reap')).requeued, 1);
    await listener.expect('ld_work');
  },

  'ld_cancel of a claimed row sends an empty ld_host_<host> hint to its host': async (w) => {
    const s1 = await w.as(logins.s1);
    await claimedByA1(w);
    const listener = await listenAsA1(w);
    await call(s1, 'ld_cancel', tid(1));
    await listener.expect('ld_host_a1');
  },

  'ld_cancel of a running row sends an empty ld_host_<host> hint to its host': async (w) => {
    const s1 = await w.as(logins.s1);
    await claimedByA1(w, { started: true });
    const listener = await listenAsA1(w);
    await call(s1, 'ld_cancel', tid(1));
    await listener.expect('ld_host_a1');
  },

  'ld_attach_urls sends an empty ld_host_<host> hint to the claiming host': async (w) => {
    const s1 = await w.as(logins.s1);
    const got = await claimedByA1(w);
    const listener = await listenAsA1(w);
    assert.equal(await call(s1, 'ld_attach_urls', tid(1), got.epoch, { get: 'u' }), true);
    await listener.expect('ld_host_a1');
  },

  'ld_set_principal refuses a host that is not a safe channel name': async (w) => {
    const admin = await w.as(logins.admin);
    for (const host of ['A1', 'a-1', 'a1; DROP', '', 'a'.repeat(41), 'a.b']) {
      await assert.rejects(call(admin, 'ld_set_principal', logins.a1, 'agent', host, [], ['*'], 1), /not a valid channel name/, `host ${JSON.stringify(host)}`);
    }
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a'.repeat(40), [], ['*'], 1);
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a_1', [], ['*'], 1);
  },
};

const CANARIES = [
  ['ld_enqueue sends an empty ld_work hint', "IF FOUND THEN PERFORM pg_notify('ld_work', ''); END IF;\n  IF NOT EXISTS", 'IF NOT EXISTS'],
  ['ld_release to queued sends an empty ld_work hint', "IF v_state = 'queued' THEN PERFORM pg_notify('ld_work', ''); END IF;", ''],
  ['ld_reap that requeues an expired claim sends an empty ld_work hint', "IF v_requeued > 0 THEN PERFORM pg_notify('ld_work', ''); END IF;", ''],
  ['ld_cancel of a claimed row sends an empty ld_host_<host> hint to its host', "PERFORM pg_notify('ld_host_' || v_t.host, '');", ''],
  ['ld_cancel of a running row sends an empty ld_host_<host> hint to its host', "PERFORM pg_notify('ld_host_' || v_t.host, '');", ''],
  ['ld_attach_urls sends an empty ld_host_<host> hint to the claiming host', "IF FOUND THEN PERFORM pg_notify('ld_host_' || v_host, ''); END IF;", ''],
  ['ld_set_principal refuses a host that is not a safe channel name', "IF p_host IS NOT NULL AND p_host !~ '^[a-z0-9_]{1,40}$' THEN", 'IF false THEN'],
];

describe('lane_dispatch NOTIFY hints on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) {
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
