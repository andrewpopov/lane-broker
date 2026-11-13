import assert from 'node:assert/strict';
import pg from 'pg';
import { sleep, waitFor, scenarioSuite } from './agent-run-harness.js';

const claimedWithin = (agent, ms) => waitFor(() => agent.core.table.all().length === 1, { timeoutMs: ms, intervalMs: 20 }).then(() => true, () => false);

const noListen = async (config) => {
  const client = new pg.Client(config);
  client.on('error', () => {});
  await client.connect();
  const query = client.query.bind(client);
  client.query = (sql, ...rest) => (String(sql).startsWith('LISTEN') ? Promise.resolve({ rows: [] }) : query(sql, ...rest));
  return client;
};

const scenarios = {
  'a NOTIFY wakes the claim loop: the ticket is claimed within 1 s although the poll fallback is a minute away': async (w, rw, load) => {
    const agent = await rw.agent({ load, config: { pollMs: 60_000 } });
    await sleep(300);
    await rw.submit();
    assert.equal(await claimedWithin(agent, 1000), true, 'claimed within 1 s of the NOTIFY');
  },

  'with LISTEN disabled the poll fallback claims within pollMs': async (w, rw, load) => {
    const agent = await rw.agent({ load, config: { pollMs: 400 }, coreOptions: { connect: noListen } });
    await sleep(100);
    await rw.submit();
    assert.equal(await claimedWithin(agent, 2500), true, 'claimed by the poll');
  },
};

const RUN = 'agent/run.js';
const canaries = [
  ['a NOTIFY wakes the claim loop: the ticket is claimed within 1 s although the poll fallback is a minute away', RUN, ['driver?.poke(event.channel); waker.poke();', '']],
  ['with LISTEN disabled the poll fallback claims within pollMs', RUN, ['await waker.wait(Math.max(0, config.pollMs - (now() - startedAt)));', 'await waker.wait(1e9);']],
];

scenarioSuite('agent claim wakeups on a disposable Postgres', { scenarios, canaries });
