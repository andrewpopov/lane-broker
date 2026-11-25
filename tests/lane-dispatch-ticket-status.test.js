import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, start, finish, release, liftBackoff, tid } from './lane-dispatch-fixture.js';

const WAIT_MS = 1500;
const CHANNEL = 'lane_ticket_events';
const TRAIN = 'lane_train_t';

/** A LISTEN session on the JSON channel, opened after setup so only the calls under test reach it. */
async function listen(w) {
  const client = await w.cluster.client(w.db, 'postgres');
  const heard = [];
  client.on('notification', (n) => { if (n.channel === CHANNEL) heard.push(JSON.parse(n.payload)); });
  await client.query(`LISTEN ${CHANNEL}`);
  return {
    heard,
    /** The next `n` events, waiting for them to arrive. */
    async events(n) {
      for (let waited = 0; waited < WAIT_MS && heard.length < n; waited += 25) await new Promise((r) => setTimeout(r, 25));
      return heard.slice(0, n);
    },
    /** Every event after a settle round-trip, to prove nothing more came. */
    async all() {
      await client.query('SELECT 1');
      await new Promise((r) => setTimeout(r, 100));
      return heard;
    },
  };
}

const states = (events) => events.map((e) => e.state);
const su = (w) => w.su();

const GUARDS = {
  'a LISTEN session sees claimed, running, finished in order with the ticket, epoch, repo and lane': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const l = await listen(w);
    const got = await claim(a1);
    await start(a1, got);
    await finish(a1, got, { kind: 'ok', exitCode: 0, logSha256: 'abc' });
    const events = await l.events(3);
    assert.deepEqual(states(events), ['claimed', 'running', 'finished']);
    for (const e of events) assert.deepEqual([e.ticket, e.epoch, e.repo, e.lane], [tid(1), 1, 'repoA', 'test']);
    assert.equal(events[2].reason, 'ok');
    assert.equal((await l.all()).length, 3, 'the enqueue sent nothing');
  },

  'a requeue sends queued with its reason, and the next claim sends claimed with epoch + 1': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const l = await listen(w);
    const first = await claim(a1);
    assert.equal((await release(a1, first)).state, 'queued');
    await liftBackoff(w);
    const second = await claim(a1);
    assert.equal(second.epoch, first.epoch + 1);
    const events = await l.events(3);
    assert.deepEqual(states(events), ['claimed', 'queued', 'claimed']);
    assert.equal(events[1].reason, 'released-never-started');
    assert.equal(events[1].epoch, first.epoch);
    assert.equal(events[2].epoch, first.epoch + 1);
  },

  'cancelling a held ticket sends cancelled only at the real transition, an unheld cancel at once': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2), { laneKey: 'other' });
    const l = await listen(w);
    const got = await claim(a1);
    assert.equal((await call(s1, 'ld_cancel', tid(1))).state, 'claimed');
    assert.deepEqual(states(await l.all()), ['claimed'], 'the cancel request is not a transition');
    assert.equal((await release(a1, got)).state, 'cancelled');
    assert.deepEqual(states(await l.all()), ['claimed', 'cancelled']);
    assert.equal(l.heard[1].epoch, got.epoch);
    assert.equal((await call(s1, 'ld_cancel', tid(2))).state, 'cancelled');
    const events = await l.events(3);
    assert.deepEqual([events[2].state, events[2].ticket, events[2].reason], ['cancelled', tid(2), 'cancelled-while-queued']);
  },

  'a withdrawn ticket sends withdrawn': async (w) => {
    const s1 = await w.as(logins.s1);
    await enqueue(s1, tid(1));
    const l = await listen(w);
    assert.equal(await call(s1, 'ld_withdraw', tid(1)), 'withdrawn');
    assert.deepEqual(states(await l.events(1)), ['withdrawn']);
  },

  'the reaper sends lost for a running row past lost_after, and log-missing once for a finished row with no log after 10 minutes': async (w) => {
    const [s1, a1, admin, db] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin), await su(w)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2), { laneKey: 'k2' });
    await enqueue(s1, tid(3), { laneKey: 'k3' });
    const [r1, f2, f3] = [await claim(a1), await claim(a1), await claim(a1)];
    await start(a1, r1);
    await finish(a1, f2, { kind: 'ok', exitCode: 0 });
    await finish(a1, f3, { kind: 'ok', exitCode: 0, logSha256: 'abc' });
    const l = await listen(w);
    await call(admin, 'ld_reap');
    assert.deepEqual(await l.all(), [], 'a fresh finish is not yet log-missing and nothing is lost');
    await db.query("UPDATE lane_dispatch.tickets SET finished_at = now() - interval '9 minutes' WHERE state = 'finished'");
    await call(admin, 'ld_reap');
    assert.deepEqual(await l.all(), [], 'nine minutes is inside the window');
    await db.query("UPDATE lane_dispatch.tickets SET finished_at = now() - interval '11 minutes' WHERE state = 'finished'");
    await db.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '3 hours' WHERE state = 'running'");
    await call(admin, 'ld_reap');
    const events = await l.all();
    assert.deepEqual(events.map((e) => [e.state, e.ticket]).sort(), [['log-missing', tid(2)], ['lost', tid(1)]]);
    assert.equal(events.find((e) => e.state === 'lost').reason, 'runner-lost');
    assert.equal((await w.ticket(tid(2))).log_state, 'missing');
    assert.equal((await w.ticket(tid(3))).log_state, null, 'a ticket with logSha256 is never marked');
    await call(admin, 'ld_reap');
    assert.equal((await l.all()).length, 2, 'log-missing is sent once');
    await enqueue(s1, tid(4), { laneKey: 'k4' });
    const f4 = await claim(a1);
    await finish(a1, f4, { kind: 'ok', exitCode: 0 });
    await db.query("UPDATE lane_dispatch.tickets SET finished_at = now() - interval '2 days' WHERE id = $1", [tid(4)]);
    const seen = (await l.all()).length;
    await call(admin, 'ld_reap');
    assert.equal((await w.ticket(tid(4))).log_state, null, 'a ticket finished two days ago is never marked');
    assert.equal((await l.all()).length, seen, 'and sends no event');
  },

  'the reaper requeues an expired claim with reason lease-expired': async (w) => {
    const [s1, a1, admin, db] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin), await su(w)];
    await enqueue(s1, tid(1));
    await claim(a1);
    await db.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '1 second'");
    const l = await listen(w);
    await call(admin, 'ld_reap');
    const [e] = await l.events(1);
    assert.deepEqual([e.state, e.reason, e.epoch], ['queued', 'lease-expired', 1]);
  },

  'a late finish after lost keeps state lost, stores late_result and answers fenced': async (w) => {
    const [s1, a1, admin, db] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin), await su(w)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await start(a1, got);
    await db.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '3 hours'");
    await call(admin, 'ld_reap');
    const before = await w.ticket(tid(1));
    assert.equal(before.state, 'lost');
    assert.equal(before.late_result, null);
    const answer = await finish(a1, got, { kind: 'ok', exitCode: 0, logSha256: 'late' });
    assert.deepEqual(answer, { fenced: true, state: 'lost' });
    const after = await w.ticket(tid(1));
    assert.equal(after.state, 'lost');
    assert.deepEqual(after.result, before.result);
    assert.equal(after.late_result.logSha256, 'late');
    assert.ok(after.late_result_at);
    await finish(a1, got, { kind: 'ok', exitCode: 1 });
    assert.equal((await w.ticket(tid(1))).late_result.exitCode, 1, 'the latest late result wins');
  },

  'a late finish from an agent that never held the attempt stores nothing': async (w) => {
    const [s1, a1, a2, admin, db] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.a2), await w.as(logins.admin), await su(w)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await db.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '3 hours', state = 'running', started_at = now()");
    await call(admin, 'ld_reap');
    assert.equal((await finish(a2, got, { kind: 'ok' })).fenced, true);
    assert.equal((await w.ticket(tid(1))).late_result, null);
  },

  'ticket_status_v1 shows the row, log_ready from logSha256, and never the claim token': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    const db = await su(w);
    const cols = (await db.query('SELECT * FROM lane.ticket_status_v1')).fields.map((f) => f.name);
    assert.deepEqual(cols, ['dispatch_key', 'ticket_id', 'repo_key', 'lane', 'state', 'claim_generation', 'host', 'lease_until', 'result', 'log_ready', 'log_sha256', 'log_state', 'late_result_at', 'updated_at']);
    let [row] = (await db.query('SELECT * FROM lane.ticket_status_v1')).rows;
    assert.deepEqual([row.dispatch_key, row.ticket_id, row.state, row.claim_generation, row.host, row.log_ready], [null, tid(1), 'claimed', 1, 'a1', false]);
    await start(a1, got);
    await finish(a1, got, { kind: 'ok', exitCode: 0, logSha256: 'abc' });
    [row] = (await db.query('SELECT * FROM lane.ticket_status_v1')).rows;
    assert.deepEqual([row.state, row.log_ready, row.log_sha256, row.result.logSha256], ['finished', true, 'abc', 'abc']);
    assert.ok(row.updated_at >= (await w.ticket(tid(1))).finished_at);
  },

  'train_reader selects the view only: no table, no ld_* function, no lane_dispatch schema': async (w) => {
    const [s1, a1, db] = [await w.as(logins.s1), await w.as(logins.a1), await su(w)];
    await enqueue(s1, tid(1));
    await claim(a1);
    const train = await w.as(TRAIN);
    assert.equal((await train.query('SELECT * FROM lane.ticket_status_v1')).rowCount, 1);
    await assert.rejects(train.query('SELECT * FROM lane_dispatch.tickets'), /permission denied/);
    await assert.rejects(train.query('SELECT * FROM lane.pull_tickets'), /permission denied/);
    await assert.rejects(train.query('SELECT claim_token FROM lane.ticket_status_v1'), /does not exist/);
    await assert.rejects(call(train, 'ld_get', tid(1)), /permission denied/);
    await assert.rejects(call(train, 'ld_reap'), /permission denied/);
    await assert.rejects(call(train, 'notify_ticket', tid(1), 'claimed', 1, null), /permission denied/);
    const { rows: [{ usage }] } = await db.query("SELECT has_schema_privilege('train_reader', 'lane_dispatch', 'USAGE') AS usage");
    assert.equal(usage, false);
  },

  'a restore sends lost with queue-restored for each settled ticket': async (w) => {
    const [s1, a1, admin] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2), { laneKey: 'k2' });
    await claim(a1);
    const l = await listen(w);
    assert.equal(await call(admin, 'ld_restore_reset'), 2);
    const events = await l.events(2);
    assert.deepEqual(events.map((e) => [e.state, e.reason]), [['lost', 'queue-restored'], ['lost', 'queue-restored']]);
  },
};

// Each canary removes one thing and the scenario that guards it must fail.
const CANARIES = [
  ['a LISTEN session sees claimed, running, finished in order with the ticket, epoch, repo and lane', "PERFORM lane_dispatch.notify_ticket(v_id, 'claimed', v_epoch, NULL);", ''],
  ['a requeue sends queued with its reason, and the next claim sends claimed with epoch + 1', "PERFORM lane_dispatch.notify_ticket(p_id, v_state, v_t.epoch, CASE WHEN v_state = 'lost' THEN 'release-cap' ELSE p_reason END);", ''],
  ['the reaper sends lost for a running row past lost_after, and log-missing once for a finished row with no log after 10 minutes', "SET log_state = 'missing'", "SET log_state = NULL"],
  ['a late finish after lost keeps state lost, stores late_result and answers fenced', "SET late_result = p_result - 'author', late_result_at = lane_dispatch.now()", "SET late_result_at = late_result_at"],
  ['train_reader selects the view only: no table, no ld_* function, no lane_dispatch schema', 'GRANT SELECT ON lane.ticket_status_v1 TO train_reader;', ''],
];

describe('lane ticket status: view, change events, late results, log-missing (PKG-437)', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => {
    cluster = await startDispatchCluster();
    const admin = await cluster.admin();
    await admin.query(`CREATE ROLE ${TRAIN} LOGIN IN ROLE train_reader`);
  });
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
