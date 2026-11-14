import test from 'node:test';
import assert from 'node:assert/strict';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { sleep } from './helpers.js';

const fakeClient = () => {
  const client = {
    destroyed: 0,
    connection: { stream: { destroy: () => { client.destroyed += 1; } } },
    on: () => {},
    escapeIdentifier: (name) => name,
    end: () => new Promise(() => {}),
    query: async () => ({ rows: [{ v: { host: 'h' } }] }),
  };
  return client;
};

const scenario = async (load) => {
  const { LdClient } = await load('agent/ld-client.js');
  const clients = [];
  const ld = new LdClient({ pg: {}, version: 't', connect: async () => { clients.push(fakeClient()); return clients.at(-1); }, closeTimeoutMs: 100 });
  await ld.open();
  assert.equal(ld.registeredOnConnection, true);
  assert.equal(ld.listening, true);
  const started = Date.now();
  const outcome = await Promise.race([ld.close().then(() => 'closed'), sleep(3000).then(() => 'hung')]);
  assert.equal(outcome, 'closed', 'close() returns although end() never settles');
  assert.ok(Date.now() - started < 2000);
  assert.deepEqual(clients.map((c) => c.destroyed), [1, 1], 'both sockets were destroyed');
  assert.equal(ld.registeredOnConnection, false, 'a closed connection is not registered');
};

test('LdClient.close() is bounded: an end() that never settles is cut by destroying the socket', () => scenario(realSrc));
for (const mutation of [
  ['Promise.race([client.end().catch(() => {}), timedOut])', 'client.end().catch(() => {})'],
  ['client.connection?.stream?.destroy();', ''],
]) {
  test(`canary (agent/ld-client.js): with ${JSON.stringify(mutation[0])} changed, the bounded-close scenario fails on its own assertion`, async (t) => {
    const failure = await assertCanaryFails('agent/ld-client.js', mutation, scenario);
    t.diagnostic(`failed as expected: ${failure.message.split('\n')[0]}`);
  });
}
