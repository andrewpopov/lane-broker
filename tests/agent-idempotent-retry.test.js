import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, enqueue, tid, holdLock, waitBlocked } from './lane-dispatch-fixture.js';
import { openAgent, closeAgents, backendPids, guardUntilKill, takeTicket } from './agent-harness.js';

/** A connection whose answer to the first `name` call is lost after the database committed it: the call's effects exist, its response never arrives. */
function loseFirstAnswerTo(name) {
  let pending = true;
  return async (config) => {
    const client = new pg.Client(config);
    client.on('error', () => {});
    await client.connect();
    const query = client.query.bind(client);
    client.query = async (sql, args) => {
      const result = await query(sql, args);
      if (pending && typeof sql === 'string' && sql.includes(`lane_dispatch.${name}(`)) {
        pending = false;
        await client.end();
        throw new Error('Connection terminated unexpectedly');
      }
      return result;
    };
    return client;
  };
}

const callsTo = (calls, name) => calls.filter(([n]) => n === name).map(([, args]) => args);

async function blockedThenGuarded(w, c, issue) {
  const [callPid] = await backendPids(w, 'SELECT lane_dispatch%');
  const hold = await holdLock(w, `SELECT 1 FROM lane_dispatch.tickets WHERE id = '${c.id}' FOR UPDATE`);
  const blocked = issue();
  await waitBlocked(w, callPid);
  await guardUntilKill(w);
  await hold.release();
  return blocked;
}

const BLOCKED_THEN_KILLED = {
  ld_start: async (w, client, calls, held) => {
    const c = await takeTicket(w, client, held, { started: false });
    assert.equal(await blockedThenGuarded(w, c, () => client.start(c.id, c.epoch, c.token)), true);
    assert.deepEqual(callsTo(calls, 'ld_start').map((a) => a.slice(0, 3)), [[c.id, c.epoch, c.token], [c.id, c.epoch, c.token]], 'the same call, twice');
    const row = await w.ticket(c.id);
    assert.deepEqual([row.state, row.started_at !== null], ['running', true]);
  },
  ld_heartbeat: async (w, client, calls, held) => {
    const c = await takeTicket(w, client, held);
    assert.equal((await blockedThenGuarded(w, c, () => client.heartbeat(c.id, c.epoch, c.token))).fenced, false);
    assert.deepEqual(callsTo(calls, 'ld_heartbeat').at(-2), callsTo(calls, 'ld_heartbeat').at(-1));
    assert.equal((await w.ticket(c.id)).state, 'running');
  },
  ld_finish: async (w, client, calls, held) => {
    const c = await takeTicket(w, client, held);
    const result = { kind: 'ok', exitCode: 0, logSha256: 'abc' };
    assert.deepEqual(await blockedThenGuarded(w, c, () => client.finish(c.id, c.epoch, c.token, result)), { fenced: false, state: 'finished' });
    assert.deepEqual(callsTo(calls, 'ld_finish').map((a) => a[3]), [result, result]);
    const row = await w.ticket(c.id);
    assert.deepEqual([row.state, row.result], ['finished', { ...result, epoch: c.epoch }]);
  },
};

const LOST_RESPONSE = {
  ld_start: async (w, client, calls) => {
    const c = await takeTicket(w, client, [], { started: false });
    assert.equal(await client.start(c.id, c.epoch, c.token), true);
    assert.equal(callsTo(calls, 'ld_start').length, 2, 'sent twice');
    assert.equal((await w.ticket(c.id)).state, 'running');
  },
  ld_log: async (w, client, calls) => {
    const c = await takeTicket(w, client, []);
    assert.equal(await client.log(c.id, c.epoch, c.token, 0, Buffer.from('hello')), 'ok');
    assert.equal(callsTo(calls, 'ld_log').length, 2, 'sent twice');
    const su = await w.su();
    assert.equal((await su.query('SELECT count(*)::int AS n FROM lane_dispatch.log_chunks WHERE ticket = $1', [c.id])).rows[0].n, 1);
    assert.equal(Number((await su.query('SELECT log_bytes FROM lane_dispatch.attempts WHERE ticket = $1', [c.id])).rows[0].log_bytes), 5, 'the bytes are counted once');
  },
  ld_finish: async (w, client, calls) => {
    const c = await takeTicket(w, client, []);
    assert.deepEqual(await client.finish(c.id, c.epoch, c.token, { kind: 'ok', exitCode: 0 }), { fenced: false, state: 'finished' });
    assert.equal(callsTo(calls, 'ld_finish').length, 2, 'sent twice');
    const row = await w.ticket(c.id);
    assert.deepEqual([row.state, row.result], ['finished', { kind: 'ok', exitCode: 0, epoch: c.epoch }]);
  },
};

const CLAIM_REPLAY = 'T12 a lost claim response is replayed by the retry: one ticket is claimed, and the retry returns that claim';

const SCENARIOS = {
  [CLAIM_REPLAY]: async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const { client, calls } = await openAgent(w, { connect: loseFirstAnswerTo('ld_claim') });
    const req = randomUUID();
    const answer = await client.claim(req, 8, 1e9);
    assert.deepEqual(callsTo(calls, 'ld_claim').map((a) => a[0]), [req, req], 'the request id is reused');
    const su = await w.su();
    const { rows } = await su.query("SELECT id, claim_token FROM lane_dispatch.tickets WHERE state = 'claimed'");
    assert.deepEqual(rows, [{ id: answer.id, claim_token: answer.token }], 'exactly one ticket is claimed, and it is the one returned');
    assert.equal((await su.query("SELECT count(*)::int AS n FROM lane_dispatch.tickets WHERE state = 'queued'")).rows[0].n, 1);
  },

  'T12 a lost release response is answered fenced by the retry, and the row is requeued once': async (w) => {
    const { client, calls } = await openAgent(w, { connect: loseFirstAnswerTo('ld_release') });
    const c = await takeTicket(w, client, [], { started: false });
    assert.deepEqual(await client.release(c.id, c.epoch, c.token, 'never-started'), { fenced: true, state: 'queued' });
    assert.equal(callsTo(calls, 'ld_release').length, 2);
    const row = await w.ticket(c.id);
    assert.deepEqual([row.state, row.epoch, row.claim_token], ['queued', c.epoch, null]);
  },
};

for (const [name, scenario] of Object.entries(BLOCKED_THEN_KILLED)) {
  SCENARIOS[`T12 ${name} that the guard kills while it waits on a row lock is retried verbatim and takes effect once`] = async (w) => {
    const held = [];
    const { client, calls } = await openAgent(w, { snapshot: () => held });
    await scenario(w, client, calls, held);
  };
}

for (const [name, scenario] of Object.entries(LOST_RESPONSE)) {
  SCENARIOS[`T12 a lost ${name} response is answered by the retry with one effect`] = async (w) => {
    const { client, calls } = await openAgent(w, { connect: loseFirstAnswerTo(name) });
    await scenario(w, client, calls);
  };
}

const CANARIES = [[CLAIM_REPLAY, 'WHERE c.login_role = session_user AND c.req = p_req;', 'WHERE c.login_role = session_user AND c.req = p_req AND false;']];

describe('agent ld client idempotent retries on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(async () => { await closeAgents(); await cluster.closeClients(); });

  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    test(name, async () => scenario(await World.create(cluster)));
  }

  for (const [name, from, to] of CANARIES) {
    test(`canary: with the replay guard removed, "${name}" fails`, async (t) => {
      let failure;
      await SCENARIOS[name](await World.create(cluster, { mutate: [from, to] })).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
