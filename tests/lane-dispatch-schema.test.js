import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { LANE_SQL_DIR, applyDatabaseAcls, applyMigrations } from '../src/lane-db.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, tid } from './lane-dispatch-fixture.js';

const AGENT_FUNCTIONS = ['ld_register', 'ld_claim', 'ld_start', 'ld_heartbeat', 'ld_finish', 'ld_release', 'ld_log', 'ld_reassert'];
const SUBMIT_FUNCTIONS = ['ld_enqueue', 'ld_get', 'ld_list_mine', 'ld_withdraw', 'ld_cancel', 'ld_attach_urls', 'ld_log_read', 'ld_log_ack'];
const ADMIN_FUNCTIONS = ['ld_set_principal', 'ld_reap', 'ld_cancel_quarantined', 'ld_guard'];
const GRANTED = { agent: AGENT_FUNCTIONS, submit: SUBMIT_FUNCTIONS, admin: ADMIN_FUNCTIONS, reader: [] };
const CALLERS = { agent: logins.a1, submit: logins.s1, admin: logins.admin, reader: logins.reader, legacy: logins.legacy };

async function legacyIsRefused(w) {
  const legacy = await w.as(logins.legacy);
  const id = randomUUID();
  const calls = {
    ld_register: ['v', {}], ld_claim: [randomUUID(), 8, 1e9, []], ld_start: [id, 1, id], ld_heartbeat: [id, 1, id],
    ld_finish: [id, 1, id, { kind: 'ok' }], ld_release: [id, 1, id, 'never-started'], ld_log: [id, 1, id, 1, Buffer.from('x')], ld_reassert: ['[]'],
  };
  assert.deepEqual(Object.keys(calls), AGENT_FUNCTIONS);
  for (const [name, args] of Object.entries(calls)) {
    await assert.rejects(call(legacy, name, ...args), (err) => err.code === '42501' && /no agent principal for lane_agent_h1/.test(err.message), name);
  }
  const su = await w.su();
  assert.equal((await su.query('SELECT count(*)::int AS n FROM lane_dispatch.claim_requests')).rows[0].n, 0);
}

describe('lane_dispatch schema and ACLs', { skip: pgSkipOption, timeout: 180000 }, () => {
  let cluster;
  let world;
  before(async () => {
    cluster = await startDispatchCluster();
    world = await World.create(cluster);
  });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(async () => { await cluster.closeClients(); world.connections.clear(); });

  test('the server under test is PostgreSQL 16 or later (version printed)', async (t) => {
    const version = await cluster.serverVersion();
    t.diagnostic(`server_version ${version}`);
    console.log(`# lane-dispatch tests ran on PostgreSQL ${version}`);
    assert.ok(Number.parseInt(version, 10) >= 16);
  });

  test('migration 005 records version 5 and every lane_dispatch object is owned by lane_definer', async () => {
    const su = await world.su();
    const { rows: versions } = await su.query('SELECT version FROM lane.schema_version ORDER BY version');
    assert.equal(versions.at(-1).version, fs.readdirSync(path.join(LANE_SQL_DIR, 'migrations')).length);
    const { rows: owners } = await su.query(
      `SELECT c.relname AS name, r.rolname AS owner FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner
        WHERE c.relnamespace = 'lane_dispatch'::regnamespace AND c.relkind = 'r'
       UNION ALL SELECT p.proname, r.rolname FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner WHERE p.pronamespace = 'lane_dispatch'::regnamespace
       UNION ALL SELECT 'schema', r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = 'lane_dispatch'`);
    assert.ok(owners.length > 20);
    assert.deepEqual(owners.filter((o) => o.owner !== 'lane_definer'), []);
    const { rows: tables } = await su.query("SELECT relname FROM pg_class WHERE relnamespace = 'lane_dispatch'::regnamespace AND relkind = 'r' ORDER BY relname");
    assert.deepEqual(tables.map((r) => r.relname), ['attempts', 'claim_lock', 'claim_requests', 'cluster', 'log_chunks', 'principals', 'runners', 'tickets']);
  });

  test('every ld function is SECURITY DEFINER with the pinned search_path and is not executable by PUBLIC', async () => {
    const su = await world.su();
    const { rows } = await su.query(
      `SELECT proname, prosecdef, proconfig, has_function_privilege('public', oid, 'EXECUTE') AS public_exec
         FROM pg_proc WHERE pronamespace = 'lane_dispatch'::regnamespace`);
    assert.ok(rows.length >= 13);
    for (const r of rows) {
      const invoker = r.proname === 'ld_guard';
      assert.equal(r.prosecdef, !invoker, `${r.proname} is ${invoker ? 'the one SECURITY INVOKER function, run by lane_admin' : 'SECURITY DEFINER'}`);
      assert.deepEqual(r.proconfig, [invoker ? 'search_path=pg_catalog, pg_temp' : 'search_path=pg_catalog, lane_dispatch, pg_temp'], r.proname);
      assert.equal(r.public_exec, false, `${r.proname} not executable by PUBLIC`);
    }
  });

  test('ACL matrix: every (role, function) pair is granted or refused exactly as the group table says', async () => {
    const su = await world.su();
    const { rows: functions } = await su.query("SELECT proname, pronargs FROM pg_proc WHERE pronamespace = 'lane_dispatch'::regnamespace ORDER BY proname");
    const unclassified = functions.map((f) => f.proname).filter((n) => !n.startsWith('ld_') && !['ld_entry'].includes(n));
    assert.ok(unclassified.length > 0, 'internal helpers exist and must be refused to every caller');
    for (const [kind, login] of Object.entries(CALLERS)) {
      const c = await world.as(login);
      const group = kind === 'legacy' ? 'agent' : kind;
      for (const f of functions) {
        const granted = GRANTED[group].includes(f.proname);
        const { rows: [{ has }] } = await su.query(`SELECT has_function_privilege($1, 'lane_dispatch.${f.proname}'::regproc, 'EXECUTE') AS has`, [login]);
        assert.equal(has, granted, `${login} EXECUTE on ${f.proname}`);
        const nulls = Array(f.pronargs).fill('NULL').join(', ');
        const attempt = c.query(`SELECT lane_dispatch.${f.proname}(${nulls})`);
        if (granted) {
          await attempt.catch((err) => assert.doesNotMatch(err.message, /permission denied for function/, `${login} calling ${f.proname}`));
        } else {
          await assert.rejects(attempt, /permission denied for (function|schema)/, `${login} calling ${f.proname}`);
        }
      }
    }
  });

  test('ACL matrix: for every (caller role, table) SELECT, INSERT, UPDATE and DELETE are refused', async () => {
    const su = await world.su();
    const { rows: tables } = await su.query(
      `SELECT c.relname, (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 1) AS col
         FROM pg_class c WHERE c.relnamespace = 'lane_dispatch'::regnamespace AND c.relkind = 'r'`);
    assert.equal(tables.length, 8);
    for (const login of Object.values(CALLERS)) {
      const c = await world.as(login);
      for (const { relname: t, col } of tables) {
        for (const sql of [`SELECT 1 FROM lane_dispatch.${t} LIMIT 1`, `INSERT INTO lane_dispatch.${t} DEFAULT VALUES`,
          `UPDATE lane_dispatch.${t} SET ${col} = ${col}`, `DELETE FROM lane_dispatch.${t}`]) {
          await assert.rejects(c.query(sql), /permission denied/, `${login}: ${sql}`);
        }
      }
    }
  });

  test('lane_admin holds no table privilege and lane_reader no schema usage in lane_dispatch', async () => {
    const su = await world.su();
    const { rows } = await su.query(
      `SELECT r.rolname, c.relname, has_table_privilege(r.oid, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS any_priv
         FROM pg_roles r, pg_class c WHERE r.rolname IN ('lane_admin','lane_agent','lane_submit','lane_reader') AND c.relnamespace = 'lane_dispatch'::regnamespace AND c.relkind = 'r'`);
    assert.deepEqual(rows.filter((r) => r.any_priv), []);
    const { rows: [{ usage }] } = await su.query("SELECT has_schema_privilege('lane_reader', 'lane_dispatch', 'USAGE') AS usage");
    assert.equal(usage, false);
  });

  test('CREATE TABLE and CREATE TEMP TABLE fail for every caller', async () => {
    for (const login of Object.values(CALLERS)) {
      const c = await world.as(login);
      await assert.rejects(c.query('CREATE TABLE lane_dispatch.x (a int)'), /permission denied/, `${login} in lane_dispatch`);
      await assert.rejects(c.query('CREATE TABLE public.x (a int)'), /permission denied/, `${login} in public`);
      await assert.rejects(c.query('CREATE TEMP TABLE x (a int)'), /permission denied to create temporary tables/, `${login} temp`);
    }
  });

  test('the database admits only the lane groups: PUBLIC has no CONNECT, CREATE or TEMP, and a stranger login cannot connect', async () => {
    const su = await world.su();
    const { rows: [acl] } = await su.query(
      `SELECT has_database_privilege('public', current_database(), 'CONNECT') AS connect, has_database_privilege('public', current_database(), 'CREATE') AS creates,
              has_database_privilege('public', current_database(), 'TEMP') AS temp, has_schema_privilege('public', 'public', 'USAGE') AS public_usage`);
    assert.deepEqual(acl, { connect: false, creates: false, temp: false, public_usage: false });
    await assert.rejects(cluster.client(world.db, logins.stranger), /permission denied for database|not currently accepting connections/);
  });

  test('applyDatabaseAcls is opt-in for applyMigrations, and refuses a database that holds a schema besides lane, lane_dispatch and public, or any object in public', async () => {
    const publicConnect = (c) => c.query("SELECT has_database_privilege('public', current_database(), 'CONNECT') AS v").then((r) => r.rows[0].v);
    const shared = await cluster.client(await cluster.freshDb());
    await shared.query('CREATE SCHEMA app');
    await assert.rejects(applyDatabaseAcls(shared), /refusing to revoke database ACLs.*\bapp\b/);
    await assert.rejects(applyMigrations(shared, { databaseAcls: true }), /refusing to revoke database ACLs/);
    assert.equal(await publicConnect(shared), true, 'nothing was revoked');

    const publicApp = await cluster.client(await cluster.freshDb());
    await publicApp.query('CREATE TABLE public.orders (id int)');
    await assert.rejects(applyDatabaseAcls(publicApp), /schema public holds objects/);
    assert.equal(await publicConnect(publicApp), true, 'an app keeping its tables in public is refused too');

    const own = await cluster.client(await cluster.freshDb());
    await applyMigrations(own);
    assert.equal(await publicConnect(own), true, 'applyMigrations leaves the database ACLs alone by default');
    await applyMigrations(own, { databaseAcls: true });
    assert.equal(await publicConnect(own), false, 'and revokes them when asked');
  });

  test('legacy lane_agent_h1 has no lane_dispatch principal, so every agent function refuses it', () => legacyIsRefused(world));

  test('claim_lock cannot be locked, read or written by any caller, and a held lock makes ld_claim answer busy, never wait', async () => {
    for (const login of Object.values(CALLERS)) {
      const c = await world.as(login);
      await assert.rejects(c.query('SELECT 1 FROM lane_dispatch.claim_lock WHERE id = 1 FOR UPDATE NOWAIT'), /permission denied/, login);
      await assert.rejects(c.query('BEGIN; LOCK TABLE lane_dispatch.claim_lock IN ACCESS EXCLUSIVE MODE'), /permission denied/, login);
      await c.query('ROLLBACK');
    }
    const holder = await world.su();
    const agent = await world.as(logins.a1);
    await enqueue(await world.as(logins.s1), tid(1));
    await holder.query('BEGIN');
    await holder.query('SELECT 1 FROM lane_dispatch.claim_lock WHERE id = 1 FOR UPDATE');
    const started = Date.now();
    assert.deepEqual(await claim(agent), { status: 'busy' });
    assert.ok(Date.now() - started < 1500, 'busy is immediate');
    await holder.query('COMMIT');
    assert.equal((await claim(agent)).status, 'claimed');
  });

  test('admin-owned fields: a runner cannot set its own tags or repos, and ld_register touches only informational fields', async () => {
    const agent = await world.as(logins.a1);
    await assert.rejects(call(agent, 'ld_set_principal', logins.a1, 'agent', 'a1', ['gpu'], ['*'], 99), /permission denied for function/);
    await assert.rejects(agent.query("UPDATE lane_dispatch.principals SET tags = '{gpu}'"), /permission denied/);
    const before = (await (await world.su()).query('SELECT * FROM lane_dispatch.principals WHERE login_role = $1', [logins.a1])).rows[0];
    assert.deepEqual(await call(agent, 'ld_register', '1.2.3', { quiet: [1, 6] }), { host: 'a1', max_claims: 2 });
    const su = await world.su();
    assert.deepEqual((await su.query('SELECT * FROM lane_dispatch.principals WHERE login_role = $1', [logins.a1])).rows[0], before);
    assert.deepEqual((await su.query('SELECT host, version, schedule FROM lane_dispatch.runners')).rows, [{ host: 'a1', version: '1.2.3', schedule: { quiet: [1, 6] } }]);
  });

  test("canary: with the principal check removed from me(), the legacy-login refusal test fails", async (t) => {
    const mutated = await World.create(cluster, { mutate: ["  IF NOT FOUND THEN\n    RAISE EXCEPTION 'no % principal for %', p_kind, session_user USING ERRCODE = '42501';\n  END IF;\nEND $$;", 'END $$;'] });
    let failure;
    await legacyIsRefused(mutated).catch((err) => { failure = err; });
    assert.ok(failure, 'the legacy refusal test still passed with the guard removed, so it proves nothing');
    t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
  });

  test('cluster-level privileges stay out of the migration and functions SQL (they need a superuser; docs/lane-dispatch-db-runbook.md)', () => {
    const files = [path.join(LANE_SQL_DIR, 'functions.sql'), ...fs.readdirSync(path.join(LANE_SQL_DIR, 'migrations')).map((f) => path.join(LANE_SQL_DIR, 'migrations', f))];
    for (const file of files) {
      assert.doesNotMatch(fs.readFileSync(file, 'utf8'), /pg_signal_backend|pg_read_all_stats|CONNECTION LIMIT|ALTER ROLE\s+\S+\s+SET|ALTER ROLE ALL/i, file);
    }
  });
});
