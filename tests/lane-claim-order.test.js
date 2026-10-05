import { describe, test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { claimNext } from '../src/lane-db.js';
import { PgCluster, PG_SKIP_REASON, roleNames, ago, gid, addGroup, jobsJson, ROOM } from './lane-db-harness.js';

/**
 * BRAIN-400: one test per counterexample from the Codex vets of spec rev6 section 5. Each runs against its own
 * database cloned from a seeded template on a disposable Postgres, as the real per-host agent login.
 */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const claim = (agent, room = ROOM) => claimNext(agent, { generation: 1, room, memBytes: 1e12, token: randomUUID() });
const claimedGroup = async (agent, room) => (await claim(agent, room))?.group_id ?? null;

async function waitForLockWait(admin, pid) {
  for (let i = 0; i < 200; i++) {
    const { rows } = await admin.query("SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock'", [pid]);
    if (rows.length) return;
    await sleep(25);
  }
  throw new Error(`backend ${pid} never waited on a lock`);
}

describe('lane.claim_next ordering', { skip: PG_SKIP_REASON ?? false, timeout: 120000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });

  async function open(opts) {
    const db = await cluster.freshDb(opts);
    return { db, su: await cluster.client(db), agent: await cluster.client(db, roleNames.agent1) };
  }

  test('overdue beats fresh higher-tier work', async () => {
    const { su, agent } = await open();
    // 21 minutes of aging also lifts the overdue group to band 2, so the decoy wins every later key unless overdue ranks first.
    await addGroup(su, { n: 1, tier: 2, activatedAt: ago(1), jobs: [{ est: 9000 }] });
    await addGroup(su, { n: 2, tier: 0, activatedAt: ago(21 * 60) });
    assert.equal(await claimedGroup(agent), gid(2));
  });

  test('the highest non-overdue band wins over a lower band even when the lower one has a smaller vtime', async () => {
    const { su, agent } = await open();
    await addGroup(su, { n: 1, tier: 0, jobs: [{ cls: 'test' }] });
    await addGroup(su, { n: 2, tier: 2, jobs: [{ cls: 'sim' }] });
    await su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'test', 0), ('h1', 'sim', 50)");
    assert.equal(await claimedGroup(agent), gid(2));
  });

  async function strideCounts(opts, selections = 400) {
    const { su, agent } = await open(opts);
    await su.query("UPDATE lane.host_class_policy SET weight = 3 WHERE class = 'test'");
    await addGroup(su, { n: 1, jobs: [{ cls: 'test', count: selections }] });
    await addGroup(su, { n: 2, jobs: [{ cls: 'sim', count: selections }] });
    const counts = { test: 0, sim: 0 };
    for (let i = 0; i < selections; i++) counts[(await claim(agent)).class]++;
    return counts;
  }

  test('the 3:1 stride gives 300:100 over 400 unit-grant selections', async () => {
    assert.deepEqual(await strideCounts(), { test: 300, sim: 100 });
  });

  test('the old double-weight comparator (vtime/weight) would give about 9:1, so the fixture catches it', async () => {
    const counts = await strideCounts({ mutate: ['round(vtime::numeric, 9) ASC,', 'round((vtime / weight)::numeric, 9) ASC,'] });
    assert.ok(counts.test / counts.sim >= 8 && counts.test / counts.sim <= 10, `expected ~9:1, got ${counts.test}:${counts.sim}`);
    assert.notDeepEqual(counts, { test: 300, sim: 100 });
  });

  test('the older-served group wins over a group with a longer job and a smaller gid', async () => {
    const { su, agent } = await open();
    await addGroup(su, { n: 1, lastClaimAt: ago(60), jobs: [{ est: 9000 }] });
    await addGroup(su, { n: 2, lastClaimAt: ago(300), jobs: [{ est: 10 }] });
    assert.equal(await claimedGroup(agent), gid(2));
  });

  for (const [longGid, shortGid] of [[1, 2], [2, 1]]) {
    test(`with tied timestamps the longer job wins regardless of gid (long job in gid ${longGid})`, async () => {
      const { su, agent } = await open();
      await addGroup(su, { n: longGid, jobs: [{ est: 9000 }] });
      await addGroup(su, { n: shortGid, jobs: [{ est: 100 }] });
      assert.equal(await claimedGroup(agent), gid(longGid));
    });
  }

  test('equal-age overdue groups are ordered by gid, not by job length or insert order', async () => {
    const { su, agent } = await open();
    const sameAge = ago(25 * 60);
    for (const n of [3, 1, 2]) await addGroup(su, { n, activatedAt: sameAge, jobs: [{ est: n * 1000 }] });
    const order = [await claimedGroup(agent), await claimedGroup(agent), await claimedGroup(agent)];
    assert.deepEqual(order, [gid(1), gid(2), gid(3)]);
  });

  test('K=0 is served on the first opportunity (and K=2 on the third)', async () => {
    const zero = await open();
    await addGroup(zero.su, { n: 1, activatedAt: ago(25 * 60), jobs: [{ cls: 'sim' }] });
    await addGroup(zero.su, { n: 2, tier: 2, activatedAt: ago(1), jobs: [{ cls: 'test' }] });
    await zero.su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'sim', 50)");
    assert.equal(await claimedGroup(zero.agent), gid(1));

    const two = await open();
    await addGroup(two.su, { n: 9, activatedAt: ago(21 * 60), jobs: [{ count: 3 }] });
    await addGroup(two.su, { n: 5, activatedAt: ago(30 * 60), jobs: [{ count: 3 }] });
    await addGroup(two.su, { n: 6, activatedAt: ago(29 * 60), jobs: [{ count: 3 }] });
    await addGroup(two.su, { n: 1, tier: 2, activatedAt: ago(1), jobs: [{ count: 3 }] });
    assert.deepEqual([await claimedGroup(two.agent), await claimedGroup(two.agent), await claimedGroup(two.agent)], [gid(5), gid(6), gid(9)]);
  });

  test('a group that crosses the overdue threshold while the claim waits on the lock is treated as overdue', async () => {
    const { db, su, agent } = await open();
    await addGroup(su, { n: 1, tier: 0, activatedAt: ago(19 * 60 + 58) });   // 2 s short of overdue
    await addGroup(su, { n: 2, tier: 2, activatedAt: ago(1) });
    const holder = await cluster.client(db);
    await holder.query('BEGIN');
    await holder.query("SELECT pg_advisory_xact_lock(hashtext('lane_sched'))");
    const { rows: [{ pid }] } = await agent.query('SELECT pg_backend_pid() AS pid');
    const pending = claimedGroup(agent);
    await waitForLockWait(su, pid);
    await sleep(3500);
    await holder.query('COMMIT');
    assert.equal(await pending, gid(1));
  });

  test('a late-committing earlier transaction cannot sort ahead of an already-activated overdue group', async () => {
    const { su } = await open();
    const submitJobs = JSON.stringify(jobsJson(1));
    const submit = (c, id) => c.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', 1::smallint, '{}'::jsonb, 'x', $2::jsonb) AS id", [id, submitJobs]);
    const late = await cluster.client(su.database, roleNames.submit);
    const other = await cluster.client(su.database, roleNames.submit);
    await late.query('BEGIN');
    const { rows: [{ id: lateId }] } = await submit(late, randomUUID());   // created_at = this transaction's start
    await sleep(100);
    const { rows: [{ id: onTimeId }] } = await submit(other, randomUUID());
    await other.query('SELECT lane.activate_group($1)', [onTimeId]);
    await late.query('COMMIT');
    await late.query('SELECT lane.activate_group($1)', [lateId]);
    const { rows: [g] } = await su.query(
      `SELECT (SELECT created_at FROM lane.groups WHERE id = $1) < (SELECT created_at FROM lane.groups WHERE id = $2) AS late_created_first,
              (SELECT activated_at FROM lane.groups WHERE id = $1) > (SELECT activated_at FROM lane.groups WHERE id = $2) AS late_activated_after`,
      [lateId, onTimeId]);
    assert.deepEqual(g, { late_created_first: true, late_activated_after: true });
    await su.query("UPDATE lane.groups SET activated_at = activated_at - interval '25 minutes', created_at = created_at - interval '25 minutes'");   // both overdue, order preserved
    const agent = await cluster.client(su.database, roleNames.agent1);
    assert.equal(await claimedGroup(agent), onTimeId);
  });

  test('a failed revalidation (job cancelled between select and lock) returns no claim', async () => {
    const { db, su, agent } = await open();
    await addGroup(su, { n: 1 });
    const { rows: [{ id: jobId }] } = await su.query('SELECT id FROM lane.jobs');
    const canceller = await cluster.client(db);
    await canceller.query('BEGIN');
    await canceller.query('SELECT 1 FROM lane.jobs WHERE id = $1 FOR UPDATE', [jobId]);
    const { rows: [{ pid }] } = await agent.query('SELECT pg_backend_pid() AS pid');
    const pending = claim(agent);
    await waitForLockWait(su, pid);                  // the claim has chosen its candidate and now waits on the job lock
    await canceller.query("UPDATE lane.jobs SET state = 'cancelled' WHERE id = $1", [jobId]);
    await canceller.query('COMMIT');
    assert.equal(await pending, null);
    const { rows: [after] } = await su.query(
      `SELECT j.state, j.epoch, (SELECT last_claim_at FROM lane.groups) AS last_claim_at,
              (SELECT count(*)::int FROM lane.class_vtime) AS vtimes, (SELECT count(*)::int FROM lane.transition_events) AS events FROM lane.jobs j`);
    assert.deepEqual(after, { state: 'cancelled', epoch: 0, last_claim_at: null, vtimes: 0, events: 0 });
  });
});
