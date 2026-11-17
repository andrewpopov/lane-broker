import { describe, test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { applyMigrations, claimNext, LANE_SQL_DIR } from '../src/lane-db.js';
import { makeTmpDir } from './helpers/tmp.js';
import { PgCluster, PG_SKIP_REASON, roleNames, addGroup, gid, FREE } from './lane-db-harness.js';

const MIGRATIONS = fs.readdirSync(path.join(LANE_SQL_DIR, 'migrations')).filter((f) => f.endsWith('.sql')).length;

/**
 * BRAIN-400: on pitelite an ordinary admin role (not a superuser) owns the database and applies the migrations.
 * Roles are cluster-wide, so this file owns its own cluster: the first apply must be able to create them.
 */
describe('applying sql/lane as a non-superuser database owner', { skip: PG_SKIP_REASON ?? false, timeout: 60000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });

  async function ownedDb(owner, attrs) {
    const admin = await cluster.admin();
    await admin.query(`CREATE ROLE ${owner} LOGIN ${attrs}`);
    const name = `o_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    await admin.query(`CREATE DATABASE ${name} OWNER ${owner}`);
    return name;
  }

  const notSuper = async (db, owner) => {
    const su = await cluster.client(db);
    const { rows: [r] } = await su.query('SELECT rolsuper FROM pg_roles WHERE rolname = $1', [owner]);
    assert.equal(r.rolsuper, false);
    return su;
  };

  async function assertDefinerOwnsEverything(su) {
    const { rows } = await su.query(
      `SELECT c.relname AS name, r.rolname AS owner FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner
        WHERE c.relnamespace = 'lane'::regnamespace AND c.relkind IN ('r','v','c')
       UNION ALL SELECT p.proname, r.rolname FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner WHERE p.pronamespace = 'lane'::regnamespace
       UNION ALL SELECT 'schema', r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = 'lane'`);
    assert.ok(rows.length > 20);
    assert.deepEqual(rows.filter((r) => r.owner !== 'lane_definer'), []);
  }

  test('a CREATEROLE owner creates the roles, hands objects to lane_definer, and the agent can claim', async () => {
    const db = await ownedDb('lane_owner', 'CREATEROLE');
    const owner = await cluster.client(db, 'lane_owner');
    assert.equal((await applyMigrations(owner)).applied.length, MIGRATIONS);
    const su = await notSuper(db, 'lane_owner');
    await assertDefinerOwnsEverything(su);
    await cluster.seed(db);
    await addGroup(su, { n: 1 });
    const agent = await cluster.client(db, roleNames.agent1);
    const got = await claimNext(agent, { generation: 1, free: FREE, memBytes: 1e12, token: randomUUID() });
    assert.equal(got.group_id, gid(1));
    await assert.rejects(agent.query('SELECT 1 FROM lane.jobs'), /permission denied/);
  });

  test('an owner without CREATEROLE can apply and re-apply once the roles exist and an admin granted it lane_definer', async () => {
    const db = await ownedDb('lane_owner_plain', 'NOCREATEROLE');
    await (await cluster.admin()).query('GRANT lane_definer TO lane_owner_plain WITH SET TRUE');
    const owner = await cluster.client(db, 'lane_owner_plain');
    assert.equal((await applyMigrations(owner)).applied.length, MIGRATIONS);
    assert.deepEqual((await applyMigrations(owner)).applied, []);
    await assertDefinerOwnsEverything(await notSuper(db, 'lane_owner_plain'));
  });

  describe('BRAIN-1146: a migrator role that owns nothing, on a database a superuser owns and applied (pitelite)', () => {
    const next = String(MIGRATIONS + 1).padStart(3, '0');
    let db;
    let dir;
    before(async () => {
      const admin = await cluster.admin();
      db = `m_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
      await admin.query(`CREATE DATABASE ${db}`);
      await applyMigrations(await cluster.client(db), { databaseAcls: true });
      await admin.query('CREATE ROLE lane_migrate_t LOGIN NOSUPERUSER NOCREATEROLE');
      await admin.query('GRANT lane_definer TO lane_migrate_t WITH INHERIT TRUE, SET TRUE');
      await admin.query(`GRANT CONNECT ON DATABASE ${db} TO lane_migrate_t`);
      dir = makeTmpDir('lane-migrate-sql-');
      fs.cpSync(LANE_SQL_DIR, dir, { recursive: true });
      fs.writeFileSync(path.join(dir, 'migrations', `${next}-probe.sql`),
        'ALTER TABLE lane_dispatch.tickets ADD COLUMN probe text;\nCREATE TABLE lane.migrate_probe (id int);\n');
    });

    test('without CREATE on the database, re-applying functions.sql is refused', async () => {
      const migrator = await cluster.client(db, 'lane_migrate_t');
      await assert.rejects(applyMigrations(migrator), /permission denied for database/);
    });

    test('with CONNECT and CREATE it applies a new migration, re-applies, and lane_definer still owns everything', async () => {
      await (await cluster.admin()).query(`GRANT CREATE ON DATABASE ${db} TO lane_migrate_t`);
      const migrator = await cluster.client(db, 'lane_migrate_t');
      assert.deepEqual((await applyMigrations(migrator, { dir })).applied, [`${next}-probe.sql`]);
      assert.deepEqual((await applyMigrations(migrator, { dir })).applied, []);
      const su = await notSuper(db, 'lane_migrate_t');
      await assertDefinerOwnsEverything(su);
      const { rows } = await su.query(
        `SELECT n.nspname AS name, r.rolname AS owner FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = 'lane_dispatch'
         UNION ALL SELECT c.relname, r.rolname FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner
          WHERE c.relnamespace = 'lane_dispatch'::regnamespace AND c.relkind IN ('r','v','c')`);
      assert.ok(rows.some((r) => r.name === 'tickets'));
      assert.deepEqual(rows.filter((r) => r.owner !== 'lane_definer'), []);
    });
  });
});
