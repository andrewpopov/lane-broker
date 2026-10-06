import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgCluster, PG_SKIP_REASON, roleNames, ago, gid, addGroup, setParent, jobsJson, claimAs } from './lane-db-harness.js';

/**
 * BRAIN-400: one test per counterexample from the Codex vets of spec rev6-rev11 section 5, ordering half. Each runs against its
 * own database cloned from a seeded template on a disposable Postgres, as the real per-host agent login. Priority classes are strict
 * bands, aging and fair share live on the scheduling parent (account, class), so fixtures that need two competitors use two accounts.
 * The rev11 counterexamples on size, floors, caps and deadlines are in lane-claim-rev11.test.js.
 */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const claimedGroup = async (agent, extra) => (await claimAs(agent, extra))?.group_id ?? null;

async function waitForLockWait(admin, pid) {
  for (let i = 0; i < 200; i++) {
    const { rows } = await admin.query("SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock'", [pid]);
    if (rows.length) return;
    await sleep(25);
  }
  throw new Error(`backend ${pid} never waited on a lock`);
}

describe('lane.claim_next ordering', { skip: PG_SKIP_REASON ?? false, timeout: 240000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());     // every test opens its own connections; the server allows only 60

  async function open(opts) {
    const db = await cluster.freshDb(opts);
    return { db, su: await cluster.client(db), agent: await cluster.client(db, roleNames.agent1) };
  }

  test('overdue beats fresh higher-class work', async () => {
    const { su, agent } = await open();
    // 21 minutes of aging must not lift the overdue batch parent over a gate parent by any other key; only the overdue rule does.
    await addGroup(su, { n: 1, account: 'fresh', prio: 'gate', activatedAt: ago(1), jobs: [{ est: 9000 }] });
    await addGroup(su, { n: 2, account: 'old', prio: 'batch', activatedAt: ago(21 * 60) });
    assert.equal(await claimedGroup(agent), gid(2));
  });

  test('the highest non-overdue band wins over a lower band even when the lower one has a smaller vtime', async () => {
    const { su, agent } = await open();
    await addGroup(su, { n: 1, account: 'low', prio: 'batch', jobs: [{ cls: 'test' }] });
    await addGroup(su, { n: 2, account: 'high', prio: 'gate', jobs: [{ cls: 'sim' }] });
    await su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'test', 0), ('h1', 'sim', 50)");
    assert.equal(await claimedGroup(agent), gid(2));
  });

  async function strideCounts(opts, selections = 400) {
    const { su, agent } = await open(opts);
    await su.query("UPDATE lane.host_class_policy SET weight = 3 WHERE class = 'test'");
    await addGroup(su, { n: 1, jobs: [{ cls: 'test', count: selections }] });
    await addGroup(su, { n: 2, jobs: [{ cls: 'sim', count: selections }] });
    const counts = { test: 0, sim: 0 };
    for (let i = 0; i < selections; i++) counts[(await claimAs(agent)).class]++;
    return counts;
  }

  test('the 3:1 stride gives 300:100 over 400 unit-grant selections (two work classes in one priority class)', async () => {
    assert.deepEqual(await strideCounts(), { test: 300, sim: 100 });
  });

  test('the old double-weight comparator (vtime/weight) would give about 9:1, so the fixture catches it', async () => {
    const counts = await strideCounts({ mutate: ['round(eff_vtime::numeric, 9) ASC,', 'round((eff_vtime / weight)::numeric, 9) ASC,'] });
    assert.ok(counts.test / counts.sim >= 8 && counts.test / counts.sim <= 10, `expected ~9:1, got ${counts.test}:${counts.sim}`);
    assert.notDeepEqual(counts, { test: 300, sim: 100 });
  });

  // Class `test` (weight 3) runs alone for 1000 grants, then `sim` (weight 1) gets work. Without the 5.2 catch-up sim's
  // vtime is still 0 against test's ~333, so sim would win every claim until it had banked the whole idle period.
  async function simShareAfterIdle(opts, selections = 400) {
    const { su, agent } = await open(opts);
    await su.query("UPDATE lane.host_class_policy SET weight = 3 WHERE class = 'test'");
    await addGroup(su, { n: 1, jobs: [{ cls: 'test', count: 1000 + selections }] });
    await addGroup(su, { n: 2, state: 'open', jobs: [{ cls: 'sim', count: selections }] });
    for (let i = 0; i < 1000; i++) assert.equal((await claimAs(agent)).class, 'test');
    await su.query("UPDATE lane.groups SET state = 'active', aging_anchor = now() WHERE id = $1", [gid(2)]);
    const counts = { test: 0, sim: 0 };
    for (let i = 0; i < selections; i++) counts[(await claimAs(agent)).class]++;
    return counts;
  }

  test('a class idle for 1000 grants returns at the active minimum and wins no more than its weight share', async () => {
    const counts = await simShareAfterIdle();
    assert.ok(counts.sim >= 95 && counts.sim <= 105, `expected sim ~100 of 400 (weights 3:1), got ${JSON.stringify(counts)}`);
  });

  test('without the catch-up the returning class would take every claim, so the fixture catches it', async () => {
    const counts = await simShareAfterIdle({ mutate: ['min(vtime) FILTER (WHERE was_active) OVER () AS active_floor', 'NULL::double precision AS active_floor'] });
    assert.ok(counts.sim > 300, `expected a banked burst, got ${JSON.stringify(counts)}`);
  });

  // Siblings (groups of one account and class share a parent) are ordered by group service age before est: same parent, so the
  // parent-level terms are equal by construction and only the sibling keys decide.
  test('the older-served sibling wins over a sibling with a longer job and a smaller gid', async () => {
    const { su, agent } = await open();
    await addGroup(su, { n: 1, lastClaimAt: ago(60), jobs: [{ est: 9000 }] });
    await addGroup(su, { n: 2, lastClaimAt: ago(300), jobs: [{ est: 10 }] });
    assert.equal(await claimedGroup(agent), gid(2));
  });

  for (const [longGid, shortGid] of [[1, 2], [2, 1]]) {
    test(`with tied sibling timestamps the longer job wins regardless of gid (long job in gid ${longGid})`, async () => {
      const { su, agent } = await open();
      const tied = ago(60);
      await addGroup(su, { n: longGid, activatedAt: tied, jobs: [{ est: 9000 }] });
      await addGroup(su, { n: shortGid, activatedAt: tied, jobs: [{ est: 100 }] });
      assert.equal(await claimedGroup(agent), gid(longGid));
    });
  }

  test('equal-age overdue parents are ordered by parent id, not by job length or insert order', async () => {
    const { su, agent } = await open();
    const sameAge = ago(25 * 60);
    for (const n of [3, 1, 2]) await addGroup(su, { n, account: `a${n}`, activatedAt: sameAge, jobs: [{ est: n * 1000 }] });
    const { rows } = await su.query('SELECT g.id FROM lane.groups g JOIN lane.sched_parents p ON p.id = g.parent_id ORDER BY p.id');
    const expected = rows.map((r) => r.id);
    const order = [await claimedGroup(agent), await claimedGroup(agent), await claimedGroup(agent)];
    assert.deepEqual(order, expected);
  });

  test('K+1 at parent level: K=0 is served on the first opportunity and K=2 on the third, equal-age predecessors included', async () => {
    const zero = await open();
    await addGroup(zero.su, { n: 1, account: 'old', prio: 'batch', activatedAt: ago(25 * 60), jobs: [{ cls: 'sim' }] });
    await addGroup(zero.su, { n: 2, account: 'fresh', prio: 'gate', activatedAt: ago(1), jobs: [{ cls: 'test' }] });
    await zero.su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'sim', 50)");
    assert.equal(await claimedGroup(zero.agent), gid(1));

    const two = await open();
    await addGroup(two.su, { n: 9, account: 'p9', activatedAt: ago(21 * 60), jobs: [{ count: 3 }] });
    await addGroup(two.su, { n: 5, account: 'p5', activatedAt: ago(30 * 60), jobs: [{ count: 3 }] });
    await addGroup(two.su, { n: 6, account: 'p6', activatedAt: ago(29 * 60), jobs: [{ count: 3 }] });
    await addGroup(two.su, { n: 1, account: 'fresh', prio: 'gate', activatedAt: ago(1), jobs: [{ count: 3 }] });
    assert.deepEqual([await claimedGroup(two.agent), await claimedGroup(two.agent), await claimedGroup(two.agent)], [gid(5), gid(6), gid(9)]);
  });

  async function busyParentWinner(opts) {
    const { su, agent } = await open(opts);
    // One parent: a sibling served a minute ago (so the parent was served a minute ago) and one unserved for 25 minutes.
    await addGroup(su, { n: 1, account: 'busy', prio: 'batch', lastClaimAt: ago(60), jobs: [{ count: 2 }] });
    await addGroup(su, { n: 2, account: 'busy', prio: 'batch', activatedAt: ago(25 * 60), jobs: [{ count: 2 }] });
    await addGroup(su, { n: 3, account: 'fresh', prio: 'gate', activatedAt: ago(1), jobs: [{ count: 2 }] });
    return claimedGroup(agent);
  }

  test('serving any sibling resets the parent: a group unserved 25 minutes inside a busy parent does not make the parent overdue', async () => {
    assert.equal(await busyParentWinner(), gid(3));
  });

  test('judging overdue by the oldest sibling instead of the parent would promote it, so the fixture catches it', async () => {
    const mutate = ["(v_now - coalesce(p.last_claim_at, p.aging_anchor) >= interval '20 minutes') AS p_overdue",
      "(v_now - (SELECT min(coalesce(o.last_claim_at, o.aging_anchor)) FROM lane.groups o WHERE o.parent_id = p.id AND o.state = 'active') >= interval '20 minutes') AS p_overdue"];
    assert.notEqual(await busyParentWinner({ mutate }), gid(3));
  });

  test('a parent that crosses the overdue threshold while the claim waits on the lock is treated as overdue', async () => {
    const { db, su, agent } = await open();
    await addGroup(su, { n: 1, account: 'old', prio: 'batch', activatedAt: ago(19 * 60 + 58) });   // 2 s short of overdue
    await addGroup(su, { n: 2, account: 'fresh', prio: 'gate', activatedAt: ago(1) });
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

  async function lateCommitWinner(opts) {
    const { su } = await open(opts);
    await su.query("UPDATE lane.principals SET allowed_accounts = '{acct,acct2}' WHERE kind = 'submit'");
    const submitJobs = JSON.stringify(jobsJson(1));
    const submit = (c, id, account) => c.query("SELECT lane.submit_group($1::uuid, 'single', $3, 'normal', '{}'::jsonb, 'x', $2::jsonb) AS id", [id, submitJobs, account]);
    const late = await cluster.client(su.database, roleNames.submit);
    const other = await cluster.client(su.database, roleNames.submit);
    await late.query('BEGIN');
    const { rows: [{ id: lateId }] } = await submit(late, randomUUID(), 'acct');   // created_at = this transaction's start
    await sleep(100);
    const { rows: [{ id: onTimeId }] } = await submit(other, randomUUID(), 'acct2');
    await other.query('SELECT lane.activate_group($1)', [onTimeId]);
    await late.query('COMMIT');
    await late.query('SELECT lane.activate_group($1)', [lateId]);
    const { rows: [g] } = await su.query(
      `SELECT (SELECT created_at FROM lane.groups WHERE id = $1) < (SELECT created_at FROM lane.groups WHERE id = $2) AS late_created_first,
              (SELECT aging_anchor FROM lane.groups WHERE id = $1) > (SELECT aging_anchor FROM lane.groups WHERE id = $2) AS group_activated_after,
              (SELECT p.aging_anchor FROM lane.sched_parents p JOIN lane.groups g ON g.parent_id = p.id WHERE g.id = $1)
                > (SELECT p.aging_anchor FROM lane.sched_parents p JOIN lane.groups g ON g.parent_id = p.id WHERE g.id = $2) AS parent_activated_after`,
      [lateId, onTimeId]);
    await su.query("UPDATE lane.groups SET aging_anchor = aging_anchor - interval '25 minutes', created_at = created_at - interval '25 minutes'");
    await su.query("UPDATE lane.sched_parents SET aging_anchor = aging_anchor - interval '25 minutes'");   // both overdue, order preserved
    const agent = await cluster.client(su.database, roleNames.agent1);
    return { g, winner: await claimedGroup(agent), onTimeId };
  }

  test('a late-committing earlier transaction cannot sort ahead of an already-activated overdue parent (anchors set under the lock)', async () => {
    const { g, winner, onTimeId } = await lateCommitWinner();
    assert.deepEqual(g, { late_created_first: true, group_activated_after: true, parent_activated_after: true });
    assert.equal(winner, onTimeId);
  });

  test('anchoring a parent at created_at instead of at serialised activation lets the late one sort ahead, so the fixture catches it', async () => {
    const { g, winner, onTimeId } = await lateCommitWinner({ mutate: [
      'UPDATE lane.sched_parents SET aging_anchor = p_now,', 'UPDATE lane.sched_parents SET aging_anchor = (SELECT created_at FROM lane.groups WHERE id = p_group),'] });
    assert.equal(g.parent_activated_after, false);
    assert.notEqual(winner, onTimeId);
  });

  test('a failed revalidation (job cancelled between select and lock) returns no claim and changes nothing', async () => {
    const { db, su, agent } = await open();
    await addGroup(su, { n: 1 });
    const { rows: [{ id: jobId }] } = await su.query('SELECT id FROM lane.jobs');
    const canceller = await cluster.client(db);
    await canceller.query('BEGIN');
    await canceller.query('SELECT 1 FROM lane.jobs WHERE id = $1 FOR UPDATE', [jobId]);
    const { rows: [{ pid }] } = await agent.query('SELECT pg_backend_pid() AS pid');
    const pending = claimAs(agent);
    await waitForLockWait(su, pid);                  // the claim has chosen its candidate and now waits on the job lock
    await canceller.query("UPDATE lane.jobs SET state = 'cancelled' WHERE id = $1", [jobId]);
    await canceller.query('COMMIT');
    assert.equal(await pending, null);
    const { rows: [after] } = await su.query(
      `SELECT j.state, j.epoch, (SELECT last_claim_at FROM lane.groups) AS last_claim_at, (SELECT last_claim_at FROM lane.sched_parents) AS parent_last_claim,
              (SELECT running_cpu FROM lane.sched_parents) AS running_cpu,
              (SELECT count(*)::int FROM lane.class_vtime) AS vtimes, (SELECT count(*)::int FROM lane.transition_events) AS events FROM lane.jobs j`);
    assert.deepEqual(after, { state: 'cancelled', epoch: 0, last_claim_at: null, parent_last_claim: null, running_cpu: 0, vtimes: 0, events: 0 });
  });
});
