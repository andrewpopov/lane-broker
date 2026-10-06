import { describe, test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { applyMigrations, claimNext } from '../src/lane-db.js';
import { PgCluster, PG_SKIP_REASON, roleNames, addGroup, gid, FREE } from './lane-db-harness.js';

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
    assert.equal((await applyMigrations(owner)).applied.length, 2);
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
    assert.equal((await applyMigrations(owner)).applied.length, 2);
    assert.deepEqual((await applyMigrations(owner)).applied, []);
    await assertDefinerOwnsEverything(await notSuper(db, 'lane_owner_plain'));
  });
});
