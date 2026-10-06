import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgCluster, PG_SKIP_REASON, roleNames, ago, gid, addGroup, setParent, jobsJson, claimAs } from './lane-db-harness.js';

/**
 * BRAIN-400 round 2: the Codex re-review's N1-N4 (N5 and the 003 backfill are in lane-migrations.test.js). Each test was written first
 * and watched failing on the reviewed commit; each guard has a mutation canary.
 */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const NOW = new Date('2026-03-01T12:00:00Z');

describe('lane review round 2', { skip: PG_SKIP_REASON ?? false, timeout: 300000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  async function open(opts) {
    const db = await cluster.freshDb(opts);
    return { db, su: await cluster.client(db), agent: await cluster.client(db, roleNames.agent1), submit: await cluster.client(db, roleNames.submit) };
  }

  // N1: selection sees the usage accrued since the last settlement.
  async function unsettledDemotion(opts) {
    const { su, agent } = await open(opts);
    await su.query('UPDATE lane.config SET interactive_max_cores = 5');
    await addGroup(su, { n: 1, account: 'human', prio: 'interactive', jobs: [{ cls: 'test', count: 3 }] });
    await addGroup(su, { n: 2, account: 'robot', prio: 'gate', jobs: [{ cls: 'sim' }] });
    await su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'test', 50), ('h1', 'sim', 0)");
    await setParent(su, 'human', 'interactive', { running_cpu: 4, usage_decayed: 0, usage_at: ago(600) });    // 4 cores for 10 minutes: U is about 5.19
    return (await claimAs(agent)).group_id;
  }
  test('N1: 4 cores held for 10 minutes against a limit of 5 demote interactive to gate before the claim settles them', async () => {
    assert.equal(await unsettledDemotion(), gid(2));
  });
  test('N1 canary: reading only the settled history leaves it at rank 4', async () => {
    assert.equal(await unsettledDemotion({ mutate: ['SELECT p.running_cpu + lane.usage_now(p, p_now) / 1800', 'SELECT p.running_cpu + lane.decayed(p, p_now) / 1800'] }), gid(1));
  });
  async function readVsSettled(opts) {
    const { su } = await open(opts);
    await addGroup(su, { n: 1, account: 'acct', prio: 'normal' });
    await setParent(su, 'acct', 'normal', { running_cpu: 4, usage_decayed: 1000, usage_at: new Date(NOW.getTime() - 10 * 60000) });
    const { rows: [read] } = await su.query('SELECT lane.usage_now(p, $1) AS u FROM lane.sched_parents p WHERE account = $2 AND prio_class = $3', [NOW, 'acct', 'normal']);
    await su.query('SELECT lane.add_running(id, 0, $1) FROM lane.sched_parents WHERE account = $2 AND prio_class = $3', [NOW, 'acct', 'normal']);
    const { rows: [settled] } = await su.query("SELECT usage_decayed AS u FROM lane.sched_parents WHERE account = 'acct' AND prio_class = 'normal'");
    return { read: read.u, settled: settled.u };
  }
  test('N1: settlement stores exactly what the read path reported (the unsettled integral is not counted twice)', async () => {
    const r = await readVsSettled();
    assert.ok(Math.abs(r.read - r.settled) < 1e-6 && r.read > 1000, JSON.stringify(r));
  });
  test('N1 canary: settling from the history alone stores less than the read path reported', async () => {
    const r = await readVsSettled({ mutate: ['SET usage_decayed = lane.usage_now(p, p_now),', 'SET usage_decayed = lane.decayed(p, p_now),'] });
    assert.ok(Math.abs(r.read - r.settled) > 100, JSON.stringify(r));
  });

  // N2 / N3: urgency damping at bootstrap and under concurrent writers.
  async function groupWithDeadline(opts, minutes) {
    const o = await open(opts);
    await addGroup(o.su, { n: 1, account: 'acct', prio: 'normal', deadline: new Date(NOW.getTime() + minutes * 60000), jobs: [{}] });
    return o;
  }
  const urgencyNow = async (su) => (await su.query('SELECT lane.urgency(g, $1) AS u FROM lane.groups g', [NOW])).rows[0].u;
  async function firstForecast(opts) {
    const { su } = await groupWithDeadline(opts, 6);
    const live = await urgencyNow(su);                         // no forecast yet: the undamped ETA-0 fallback the scheduler is using
    await su.query('SELECT lane.record_eta($1, 300, false, $2)', [gid(1), NOW]);
    return { live, first: await urgencyNow(su) };
  }
  test('N2: a first forecast starts from the urgency the scheduler was using (90 at 6 minutes), not from 0: at most 25 points of change', async () => {
    const r = await firstForecast();
    assert.equal(Math.round(r.live), 90);
    assert.ok(Math.abs(r.first - r.live) <= 25.0001, JSON.stringify(r));
  });
  test('N2 canary: starting from 0 lets the first forecast drop by 65 points', async () => {
    const r = await firstForecast({ mutate: ['coalesce(e.urgency, lane.urgency(g, p_now))', 'coalesce(e.urgency, 0)'] });
    assert.ok(Math.abs(r.first - r.live) > 25, JSON.stringify(r));
  });

  async function overlappingForecasts(opts) {
    const { db, su } = await groupWithDeadline(opts, 30);
    await su.query('INSERT INTO lane.group_eta (group_id, eta_p90_s, cycle, urgency, boost_on, last_flip_cycle) VALUES ($1, 300, 5, 75, true, 1)', [gid(1)]);
    const a = await cluster.client(db);
    const b = await cluster.client(db);
    await a.query('BEGIN');
    await a.query('SELECT lane.record_eta($1, 300, false, $2)', [gid(1), NOW]);       // writer A computes from 75 and is not yet committed
    const second = b.query('SELECT lane.record_eta($1, 1800, false, $2)', [gid(1), NOW]);   // writer B must wait for A, then compute from A's result
    await sleep(300);
    await a.query('COMMIT');
    await second;
    const { rows: [r] } = await su.query('SELECT cycle, urgency FROM lane.group_eta');
    return { cycle: Number(r.cycle), urgency: r.urgency };
  }
  test('N3: two overlapping forecast writers are serialised: both cycles count and urgency moves at most 25 points from A to B', async () => {
    const r = await overlappingForecasts();
    assert.equal(r.cycle, 7);
    assert.ok(Math.abs(r.urgency - 50) <= 25.0001, JSON.stringify(r));        // A left 50
  });
  test('N3 canary: without the row lock (the placeholder insert and FOR UPDATE) B computes from stale state, loses a cycle and jumps 50 points', async () => {
    const r = await overlappingForecasts({ mutate: ["  INSERT INTO lane.group_eta (group_id, eta_p90_s) VALUES (p_group, 0) ON CONFLICT (group_id) DO NOTHING;\n  SELECT * INTO e FROM lane.group_eta WHERE group_id = p_group FOR UPDATE;", '  SELECT * INTO e FROM lane.group_eta WHERE group_id = p_group;'] });
    assert.ok(r.cycle < 7 && Math.abs(r.urgency - 50) > 25, JSON.stringify(r));
  });

  // N4: submit takes no scheduling lock, and parents are provisioned with the account, never created on the fly.
  async function claimWhileSubmitOpen(opts) {
    const { su, agent, submit } = await open(opts);
    await addGroup(su, { n: 1, account: 'robot', prio: 'normal', jobs: [{ cls: 'test' }] });
    await submit.query('BEGIN');
    await submit.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', 'gate', '{}'::jsonb, 'x', $2::jsonb)", [randomUUID(), JSON.stringify(jobsJson(1))]);
    await agent.query("SET lock_timeout = '1500ms'");
    const outcome = await claimAs(agent).then((got) => got?.group_id ?? null, (err) => err.code);
    await submit.query('ROLLBACK');
    return outcome;
  }
  test('N4: an open, uncommitted submit_group transaction does not block claim_next in another session', async () => {
    assert.equal(await claimWhileSubmitOpen(), gid(1));
  });
  test('N4 canary: a submit that takes lane_sched blocks the claim until lock_timeout', async () => {
    assert.equal(await claimWhileSubmitOpen({ mutate: ["  v_me := lane.principal_of('submit');", "  v_me := lane.principal_of('submit');\n  PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));"] }), '55P03');
  });
  test('N4: a missing parent is an error at submit, never created on the fly', async () => {
    const { su, submit } = await open();
    await su.query("DELETE FROM lane.sched_parents WHERE account = 'acct' AND prio_class = 'gate'");
    await assert.rejects(submit.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', 'gate', '{}'::jsonb, 'x', $2::jsonb)", [randomUUID(), JSON.stringify(jobsJson(1))]), /no scheduling parent/);
    assert.equal((await su.query("SELECT count(*)::int AS n FROM lane.sched_parents WHERE account = 'acct' AND prio_class = 'gate'")).rows[0].n, 0);
  });
  async function provisioned(opts) {
    const { su } = await open(opts);
    const count = async (account) => (await su.query('SELECT count(*)::int AS n FROM lane.sched_parents WHERE account = $1', [account])).rows[0].n;
    const out = { seeded: await count('acct') };
    await su.query("INSERT INTO lane.principals (login_role, kind, allowed_accounts) VALUES ('p_new', 'submit', '{x,y}')");
    out.created = [await count('x'), await count('y')];
    await su.query("UPDATE lane.principals SET allowed_accounts = '{x,y,z}' WHERE login_role = 'p_new'");
    out.added = await count('z');
    await su.query("INSERT INTO lane.priority_classes (name, rank, cod_per_min) VALUES ('urgent', 5, 20)");
    out.newClass = (await su.query("SELECT count(*)::int AS n FROM lane.sched_parents WHERE prio_class = 'urgent'")).rows[0].n;
    return out;
  }
  test('N4: creating a principal, adding an account, or adding a class provisions every (account, class) parent', async () => {
    const accounts = 4;                    // acct, x, y, z
    assert.deepEqual(await provisioned(), { seeded: 5, created: [5, 5], added: 5, newClass: accounts });
  });
  test('N4 canary: provisioning only on insert misses an account added later', async () => {
    const r = await provisioned({ mutate: ['AFTER INSERT OR UPDATE OF allowed_accounts ON lane.principals', 'AFTER INSERT ON lane.principals'] });
    assert.equal(r.added, 0);
  });

  test('N4: the F9 interleaving (reclass in one session, activate in another) no longer needs any parent lock and does not deadlock', async () => {
    const { db, submit } = await open();
    const setup = await cluster.client(db, roleNames.submit);
    const mk = (cls) => setup.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', $3, '{}'::jsonb, 'x', $2::jsonb) AS id", [randomUUID(), JSON.stringify(jobsJson(1)), cls]);
    const { rows: [{ id: existing }] } = await mk('normal');
    await setup.query('SELECT lane.activate_group($1)', [existing]);
    const { rows: [{ id: fresh }] } = await mk('gate');
    const b = await cluster.client(db, roleNames.submit);
    await b.query('BEGIN');
    await b.query("SELECT lane.reclass($1, 'gate')", [existing]);             // B holds lane_sched, uncommitted
    const outcome = (p) => p.then(() => null, (e) => e.code);
    const activate = outcome(submit.query('SELECT lane.activate_group($1)', [fresh]));    // A waits for B
    await sleep(300);
    await b.query('COMMIT');
    assert.equal(await activate, null);
  });
});
