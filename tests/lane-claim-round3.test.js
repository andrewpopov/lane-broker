import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgCluster, PG_SKIP_REASON, roleNames, addGroup, jobsJson } from './lane-db-harness.js';

/** BRAIN-400 round 3: the Codex re-review's N6 (provisioning race) and N7 (CPU inputs). Test first, mutation canary each. */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

describe('lane review round 3', { skip: PG_SKIP_REASON ?? false, timeout: 120000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  async function open(opts) {
    const db = await cluster.freshDb(opts);
    return { db, su: await cluster.client(db), submit: await cluster.client(db, roleNames.submit) };
  }

  // N6: a principal gaining an account and a class being added, overlapping, must still leave every (account, class) parent.
  async function overlappingProvisioning(opts) {
    const { db, su } = await open(opts);
    const a = await cluster.client(db);
    const b = await cluster.client(db);
    await a.query('BEGIN');
    await a.query("INSERT INTO lane.principals (login_role, kind, allowed_accounts) VALUES ('p_fresh', 'submit', '{fresh}')");   // sees committed classes only
    await b.query('BEGIN');
    const klass = b.query("INSERT INTO lane.priority_classes (name, rank, cod_per_min) VALUES ('archive', -1, 0.01)");           // must wait for A, then see its principal
    await sleep(300);
    await a.query('COMMIT');
    await klass;
    await b.query('COMMIT');
    return (await su.query("SELECT count(*)::int AS n FROM lane.sched_parents WHERE account = 'fresh' AND prio_class = 'archive'")).rows[0].n;
  }
  test('N6: a principal gaining an account and a class being added in overlapping transactions still leave the (fresh, archive) parent', async () => {
    assert.equal(await overlappingProvisioning(), 1);
  });
  test('N6 canary: without the shared lock in the class trigger the parent is permanently missed', async () => {
    const lock = "  PERFORM pg_advisory_xact_lock(hashtext('lane_provision'));\n";
    const target = `BEGIN\n  PERFORM lane.require_read_committed();\n${lock}  INSERT INTO lane.sched_parents (account, prio_class)\n    SELECT DISTINCT a, NEW.name`;
    assert.equal(await overlappingProvisioning({ mutate: [target, target.replace(lock, '')] }), 0);
  });

  // N7: CPU inputs are validated, so a grant can never fall below cpu_min.
  const submitCpu = (submit, cpu_req, cpu_min) => submit.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', 'normal', '{}'::jsonb, 'x', $2::jsonb)",
    [randomUUID(), JSON.stringify(jobsJson(1, { cpu: cpu_req, extra: { cpu_min } }))]);
  test('N7: submit refuses cpu_req=1 with cpu_min=2, and zero or negative CPU, with a clear error', async () => {
    const { submit } = await open();
    for (const [req, min] of [[1, 2], [0, 0], [-1, -1], [1, 0]]) {
      await assert.rejects(submitCpu(submit, req, min), /cpu_req and cpu_min must be finite, positive and cpu_min <= cpu_req/, `${req}/${min}`);
    }
    await submitCpu(submit, 2, 1);                       // a valid elastic job still goes in
  });
  test('N7 canary: without the submit check the database constraint still stops it, but not with the clear error', async () => {
    const { submit } = await open({ mutate: ['IF EXISTS (SELECT 1 FROM jsonb_to_recordset(p_jobs)', 'IF false AND EXISTS (SELECT 1 FROM jsonb_to_recordset(p_jobs)'] });
    await assert.rejects(submitCpu(submit, 1, 2), (err) => !/must be finite/.test(err.message));
  });
  test('N7: the jobs table itself refuses cpu_min > cpu_req and non-finite or non-positive CPU', async () => {
    const { su } = await open();
    for (const [i, [req, min]] of [[1, 2], [0, 0], ['Infinity', 1], ['NaN', 1]].entries()) {
      await assert.rejects(addGroup(su, { n: i + 1, jobs: [{ cpu: req, cpuMin: min }] }), /violates check constraint/, `${req}/${min}`);
    }
  });
  test('N7 canary: dropping cpu_min <= cpu_req from the constraint lets an inverted job in', async () => {
    const { su } = await open({ mutate: [' AND cpu_min <= cpu_req', ''] });
    await addGroup(su, { n: 1, jobs: [{ cpu: 1, cpuMin: 2 }] });
  });

  // N6 residual: the lock only helps if the scan after it gets a fresh snapshot, which only Read Committed gives.
  async function classInsertAt(opts, isolation) {
    const { db } = await open(opts);
    const c = await cluster.client(db);
    await c.query(`BEGIN ISOLATION LEVEL ${isolation}`);
    const outcome = await c.query("INSERT INTO lane.priority_classes (name, rank, cod_per_min) VALUES ('archive', -1, 0.01)").then(() => 'inserted', (e) => e.message);
    await c.query('ROLLBACK');
    return outcome;
  }
  test('N6: provisioning at REPEATABLE READ or SERIALIZABLE is refused with a clear error; READ COMMITTED is accepted', async () => {
    assert.match(await classInsertAt(undefined, 'REPEATABLE READ'), /lane provisioning must run at READ COMMITTED/);
    assert.match(await classInsertAt(undefined, 'SERIALIZABLE'), /lane provisioning must run at READ COMMITTED/);
    assert.equal(await classInsertAt(undefined, 'READ COMMITTED'), 'inserted');
  });
  test('N6 canary: without the isolation check a REPEATABLE READ insert goes through', async () => {
    const m = ["  IF current_setting('transaction_isolation') <> 'read committed' THEN", '  IF false THEN'];
    assert.equal(await classInsertAt({ mutate: m }, 'REPEATABLE READ'), 'inserted');
  });
});
