import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';

const POLL_MS = 400;

const failure = (code, message) => Object.assign(new Error(message), { code });

/** A runner whose connection drops (57P01) and whose every login is then refused (28000), as when its role goes NOLOGIN or the node is sealed. */
async function refusedLoginAttempts(load) {
  const { startAgent } = await load('agent/run.js');
  const tmp = makeTmpDir('ping-backoff-');
  let connects = 0;
  let broken = false;
  const client = {
    on() {}, end: async () => {}, escapeIdentifier: (s) => `"${s}"`,
    async query() {
      if (broken) throw failure('57P01', 'terminating connection due to administrator command');
      return { rows: [{ v: { host: 'a1', max_claims: 1, protocol_min: 1 } }] };
    },
  };
  const connect = async () => {
    connects += 1;
    if (broken) throw failure('28000', 'pg_hba.conf rejects connection');
    return client;
  };
  class Driver { adoptAll() {} poke() {} async close() {} }
  const agent = await startAgent({
    remoteRoot: path.join(tmp, 'remote'), version: 't', s3: {}, Driver, figures: () => null, draining: () => true, log: () => {},
    config: { runnerName: 'a1', pollMs: POLL_MS, schedule: null, stopClaimingBeforeCloseMs: 0, brokerHome: path.join(tmp, 'home'), stateDir: path.join(tmp, 'state'), pg: {} },
    coreOptions: { connect, backoff: { minMs: 20, maxMs: 100 } },
  });
  try {
    broken = true;
    const before = connects;
    await new Promise((resolve) => setTimeout(resolve, 3 * POLL_MS));
    return connects - before;
  } finally {
    await agent.stop();
  }
}

const attemptsStayBounded = async (load) => {
  const attempts = await refusedLoginAttempts(load);
  assert.ok(attempts < 5, `${attempts} connect attempts in 3 poll intervals after the login was refused`);
};

test('a ping whose login is refused is not liveness: the loop waits a poll interval before the next attempt', () => attemptsStayBounded(realSrc));

test('canary: with a failed ping counted as no sign of life, a refused login spins the loop', async () => {
  await assertCanaryFails('agent/run.js', ['const lastSignOfLife = () => Math.max(lastLivenessAt, pingAt);', 'const lastSignOfLife = () => lastLivenessAt;'], attemptsStayBounded);
});
