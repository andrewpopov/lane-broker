import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, tid, holdLock, waitBlocked } from './lane-dispatch-fixture.js';
import { openAgent, closeAgents, backendPids, terminateBackend, guardUntilKill, sleep, takeTicket } from './agent-harness.js';

const FIRST_CALLS = ['ld_register'];

async function untilEvent(events, reason) {
  for (let i = 0; i < 100 && !events.some((e) => e.reason === reason); i++) await sleep(50);
  assert.ok(events.some((e) => e.reason === reason), `no ${reason} event`);
}

const SCENARIOS = {
  'T19a terminating only the LISTEN backend does not re-register the call connection, and the new LISTEN connection still hears work': async (w) => {
    const { client, names, events } = await openAgent(w, {});
    const c = await takeTicket(w, client);
    const [listenPid] = await backendPids(w, 'LISTEN%');
    await terminateBackend(w, listenPid);
    await untilEvent(events, 'listen-reconnected');
    const before = names().length;
    await client.heartbeat(c.id, c.epoch, c.token);
    assert.deepEqual(names().slice(before), ['ld_heartbeat']);
    assert.ok(!(await backendPids(w, 'LISTEN%')).includes(listenPid));
    await enqueue(await w.as(logins.s1), tid(2));
    for (let i = 0; i < 100 && !events.some((e) => e.reason === 'notify' && e.channel === 'ld_work'); i++) await sleep(25);
    assert.ok(events.some((e) => e.reason === 'notify' && e.channel === 'ld_work'), 'ld_work reached the reconnected listener');
  },

  'T19a the host channel is quoted: an agent whose host contains a hyphen listens on ld_host_<host>': async (w) => {
    await call(await w.as(logins.admin), 'ld_set_principal', logins.x, 'agent', 'wintop-1', [], ['*'], 1);
    const { client, events } = await openAgent(w, { login: logins.x });
    const c = await takeTicket(w, client, { started: false });
    await call(await w.as(logins.s1), 'ld_cancel', c.id);
    for (let i = 0; i < 100 && !events.some((e) => e.reason === 'notify' && e.channel === 'ld_host_wintop-1'); i++) await sleep(25);
    assert.ok(events.some((e) => e.reason === 'notify' && e.channel === 'ld_host_wintop-1'), `heard ${JSON.stringify(events)}`);
  },

  'T19b the guard kills a call blocked on the ticket row: the retry goes through register first, with the same arguments': async (w) => {
    const { client, calls, names } = await openAgent(w, {});
    const c = await takeTicket(w, client);
    const [callPid] = await backendPids(w, 'SELECT lane_dispatch%');
    const hold = await holdLock(w, `SELECT 1 FROM lane_dispatch.tickets WHERE id = '${c.id}' FOR UPDATE`);
    const before = calls.length;
    const blocked = client.heartbeat(c.id, c.epoch, c.token);
    await waitBlocked(w, callPid);
    await guardUntilKill(w);
    await hold.release();
    assert.equal((await blocked).fenced, false);
    assert.deepEqual(names().slice(before), ['ld_heartbeat', ...FIRST_CALLS, 'ld_heartbeat']);
    const heartbeats = calls.slice(before).filter(([name]) => name === 'ld_heartbeat');
    assert.deepEqual(heartbeats[0][1], heartbeats[1][1]);
  },

  'T19c calls queued behind a dead call connection keep their order behind register': async (w) => {
    const { client, names } = await openAgent(w);
    const c = await takeTicket(w, client);
    const [callPid] = await backendPids(w, 'SELECT lane_dispatch%');
    await terminateBackend(w, callPid);
    const before = names().length;
    await Promise.all([client.heartbeat(c.id, c.epoch, c.token), client.claim(randomUUID(), 8, 1e9)]);
    const sent = names().slice(before);
    const firstRegister = sent.indexOf('ld_register');
    assert.ok(firstRegister >= 0, `no ld_register after the call backend died: ${sent}`);
    assert.ok(sent.slice(0, firstRegister).every((n) => n === 'ld_heartbeat'), 'only the heartbeat that found the dead connection precedes register');
    assert.deepEqual(sent.slice(firstRegister), [...FIRST_CALLS, 'ld_heartbeat', 'ld_claim']);
  },
};

describe('agent ld client reconnects and queue on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(async () => { await closeAgents(); await cluster.closeClients(); });

  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    test(name, async () => scenario(await World.create(cluster)));
  }
});
