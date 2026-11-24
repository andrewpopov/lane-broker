// BRAIN-1301 runners.last_seen is liveness: a claim or heartbeat refreshes it, not only ld_register, at most once per 30 s.
import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, enqueue, claim, heartbeat, setNow, tid } from './lane-dispatch-fixture.js';

const T0 = Date.parse('2030-01-01T00:00:00Z');
const at = (s) => new Date(T0 + s * 1000).toISOString();

async function lastSeen(w, host) {
  const { rows: [row] } = await (await w.su()).query('SELECT last_seen FROM lane_dispatch.runners WHERE host = $1', [host]);
  return row.last_seen.toISOString();
}

async function claimAt(w, s, login = logins.a1, opts = {}) {
  await setNow(w, at(s));
  return claim(await w.as(login), opts);
}

const GUARDS = {
  'a claim that finds nothing still marks the runner seen': async (w) => {
    assert.deepEqual(await claimAt(w, 0), { status: 'none' });
    assert.equal(await lastSeen(w, 'a1'), at(0));
  },

  'claims within 30 s of the last mark write nothing; the next one after that does': async (w) => {
    await claimAt(w, 0);
    await claimAt(w, 29);
    assert.equal(await lastSeen(w, 'a1'), at(0));
    await claimAt(w, 31);
    assert.equal(await lastSeen(w, 'a1'), at(31));
  },

  'a runner at max_claims, which returns none early, is still marked seen': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1), { repo: 'repoB' });
    assert.equal((await claimAt(w, 0, logins.a2)).id, tid(1));
    assert.deepEqual(await claimAt(w, 120, logins.a2), { status: 'none' });
    assert.equal(await lastSeen(w, 'a2'), at(120));
  },

  'a heartbeat marks the runner seen while it is busy and claims nothing': async (w) => {
    await setNow(w, at(0));
    await enqueue(await w.as(logins.s1), tid(1));
    const claimed = await claimAt(w, 0);
    await setNow(w, at(300));
    assert.deepEqual(await heartbeat(await w.as(logins.a1), claimed), { fenced: false, cancel: false });
    assert.equal(await lastSeen(w, 'a1'), at(300));
  },

  "a claim marks only the caller's runner": async (w) => {
    const before = await lastSeen(w, 'a2');
    await claimAt(w, 0);
    assert.equal(await lastSeen(w, 'a2'), before);
  },
};

const CANARIES = [
  ['a claim that finds nothing still marks the runner seen', '  PERFORM lane_dispatch.runner_seen(v_me.host);\n  v_out :=', '  v_out :='],
  ['a heartbeat marks the runner seen while it is busy and claims nothing', "  PERFORM lane_dispatch.runner_seen(v_me.host);\n  SELECT * INTO v_t", '  SELECT * INTO v_t'],
  ['claims within 30 s of the last mark write nothing; the next one after that does', " AND last_seen < lane_dispatch.now() - interval '30 seconds'", ''],
  ["a claim marks only the caller's runner", 'WHERE host = p_host AND last_seen', 'WHERE last_seen'],
];

describe('lane_dispatch runner liveness (BRAIN-1301) on a disposable Postgres', { skip: pgSkipOption, timeout: 600000 }, () => {
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
