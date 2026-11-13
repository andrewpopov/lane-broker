import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, enqueue, tid } from './lane-dispatch-fixture.js';
import { openAgent, closeAgents, backendPids, terminateBackend, sleep, takeTicket } from './agent-harness.js';
import { LdClient } from '../src/agent/ld-client.js';
import { importMutant } from './helpers/mutant-module.js';

const SCENARIOS = {
  'T7 a client that has never connected sends ld_register before the claim it was asked for': async (w, { LdClient }) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const { client, names } = await openAgent(w, { open: false, LdClient });
    assert.equal((await client.claim(randomUUID(), 8, 1e9)).status, 'claimed');
    assert.deepEqual(names(), ['ld_register', 'ld_claim']);
  },

  'T7 after the guard terminates the call backend the next calls are ld_register then the call itself': async (w, { LdClient }) => {
    const { client, names } = await openAgent(w, { LdClient });
    const c = await takeTicket(w, client);
    const [pid] = await backendPids(w, 'SELECT lane_dispatch%');
    const before = names().length;
    await terminateBackend(w, pid);
    await sleep(100);
    await client.heartbeat(c.id, c.epoch, c.token);
    assert.deepEqual(names().slice(before).slice(-2), ['ld_register', 'ld_heartbeat']);
  },
};

describe('agent register-first on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(async () => { await closeAgents(); await cluster.closeClients(); });

  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    test(name, async () => scenario(await World.create(cluster), { LdClient }));
  }

  for (const name of Object.keys(SCENARIOS)) {
    test(`canary: with registration bypassed, "${name}" fails on its own call-order assertion`, async (t) => {
      const bypassed = ["      this.registered = await this.#sql('ld_register', [this.version, PROTOCOL, this.schedule === null ? null : JSON.stringify(this.schedule)]);\n", "      this.registered ??= { host: 'a1' };\n"];
      const mutant = await importMutant(path.join(import.meta.dirname, '..', 'src', 'agent', 'ld-client.js'), bypassed);
      let failure;
      await SCENARIOS[name](await World.create(cluster), { LdClient: mutant.LdClient }).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with registration bypassed, so it proves nothing`);
      assert.equal(failure.code, 'ERR_ASSERTION', `it must fail on its assertion, not crash: ${failure.stack}`);
      assert.match(failure.message, /ld_register/);
      t.diagnostic(`failed as expected: ${failure.message.split('\n').join(' ').slice(0, 200)}`);
    });
  }
});
