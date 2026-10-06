import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { applyMigrations, LANE_SQL_DIR } from '../src/lane-db.js';
import { PgCluster, PG_SKIP_REASON } from './lane-db-harness.js';

/**
 * BRAIN-400 (review finding F2): the lane schema is versioned. sql/lane/migrations/NNN-*.sql are table migrations, each applied once in
 * its own transaction and recorded in lane.schema_version; sql/lane/functions.sql is CREATE OR REPLACE and re-applied after them on every
 * run. A lane schema the runner does not recognise (no version row, or a version it has no file for) is refused, never merged with
 * IF NOT EXISTS. The fixture directories are copies of the real one plus a test-only migration.
 */
const REAL_MIGRATIONS = fs.readdirSync(path.join(LANE_SQL_DIR, 'migrations')).filter((f) => /^\d+-.*\.sql$/.test(f)).sort();

describe('lane schema versioning', { skip: PG_SKIP_REASON ?? false, timeout: 120000 }, () => {
  let cluster;
  const tmp = [];
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); for (const d of tmp) fs.rmSync(d, { recursive: true, force: true }); });
  afterEach(() => cluster.closeClients());

  async function emptyDb() {
    const admin = await cluster.admin();
    const name = `m_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    await admin.query(`CREATE DATABASE ${name}`);
    return cluster.client(name);
  }

  /** A copy of sql/lane with extra migrations, e.g. { '900-sample.sql': 'ALTER TABLE ...' }. */
  function fixtureDir(extra = {}) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-mig-'));
    tmp.push(dir);
    fs.cpSync(LANE_SQL_DIR, dir, { recursive: true });
    for (const [name, sql] of Object.entries(extra)) fs.writeFileSync(path.join(dir, 'migrations', name), sql);
    return dir;
  }
  const versions = async (c) => (await c.query('SELECT version FROM lane.schema_version ORDER BY version')).rows.map((r) => r.version);

  test('a fresh database gets every migration in order, one version row each, then the functions', async () => {
    const c = await emptyDb();
    const r = await applyMigrations(c);
    assert.deepEqual(r.applied, REAL_MIGRATIONS);
    assert.equal(r.version, REAL_MIGRATIONS.length);
    assert.deepEqual(await versions(c), REAL_MIGRATIONS.map((_, i) => i + 1));
    assert.equal((await c.query("SELECT count(*)::int AS n FROM pg_proc WHERE proname = 'claim_next'")).rows[0].n, 1);
    assert.deepEqual((await c.query('SELECT wsjf_enabled, deadlines_enabled FROM lane.config')).rows, [{ wsjf_enabled: false, deadlines_enabled: false }]);   // off until the ETA gate passes
  });

  test('re-applying is a no-op for the tables: nothing re-runs, no new version row, data kept; the functions are re-applied', async () => {
    const c = await emptyDb();
    await applyMigrations(c);
    await c.query("INSERT INTO lane.hosts (host_id) VALUES ('keep')");
    await c.query("DROP FUNCTION lane.open_stage(uuid)");     // functions are re-created from the file on every run
    const r = await applyMigrations(c);
    assert.deepEqual(r.applied, []);
    assert.equal(r.version, REAL_MIGRATIONS.length);
    assert.equal((await versions(c)).length, REAL_MIGRATIONS.length);
    assert.equal((await c.query('SELECT count(*)::int AS n FROM lane.hosts')).rows[0].n, 1);
    assert.equal((await c.query("SELECT count(*)::int AS n FROM pg_proc WHERE proname = 'open_stage'")).rows[0].n, 1);
  });

  test('a v1 to v2 sample migration adds a column to a populated table, once, and the functions are re-applied after it', async () => {
    const c = await emptyDb();
    const next = REAL_MIGRATIONS.length + 1;
    await applyMigrations(c);
    await c.query("INSERT INTO lane.hosts (host_id) VALUES ('h-old')");
    const dir = fixtureDir({ [`${String(next).padStart(3, '0')}-sample.sql`]: "ALTER TABLE lane.hosts ADD COLUMN note text NOT NULL DEFAULT 'none';" });
    const r = await applyMigrations(c, { dir });
    assert.deepEqual(r.applied, [`${String(next).padStart(3, '0')}-sample.sql`]);
    assert.equal(r.version, next);
    assert.deepEqual((await c.query('SELECT host_id, note FROM lane.hosts')).rows, [{ host_id: 'h-old', note: 'none' }]);
    assert.deepEqual((await applyMigrations(c, { dir })).applied, []);
  });

  test('a migration that fails rolls back whole: no partial change, no version row, and the error propagates', async () => {
    const c = await emptyDb();
    const next = REAL_MIGRATIONS.length + 1;
    await applyMigrations(c);
    const dir = fixtureDir({ [`${String(next).padStart(3, '0')}-bad.sql`]: 'ALTER TABLE lane.hosts ADD COLUMN half_done int; SELECT 1 / 0;' });
    await assert.rejects(applyMigrations(c, { dir }), /division by zero/);
    assert.equal((await c.query("SELECT count(*)::int AS n FROM information_schema.columns WHERE table_schema = 'lane' AND table_name = 'hosts' AND column_name = 'half_done'")).rows[0].n, 0);
    assert.deepEqual(await versions(c), REAL_MIGRATIONS.map((_, i) => i + 1));
  });

  test('a pre-versioning lane schema (tables, no version row) is refused and left untouched, not merged with IF NOT EXISTS', async () => {
    const c = await emptyDb();
    await c.query('CREATE SCHEMA lane; CREATE TABLE lane.jobs (id bigint PRIMARY KEY)');
    await assert.rejects(applyMigrations(c), /unversioned/);
    assert.deepEqual((await c.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'lane'")).rows, [{ table_name: 'jobs' }]);
  });

  test('a lane schema with an empty version table is refused too', async () => {
    const c = await emptyDb();
    await applyMigrations(c);
    await c.query('DELETE FROM lane.schema_version');
    await assert.rejects(applyMigrations(c), /unversioned|no version/);
  });

  test('a lane schema at a version this code has no file for (newer or unknown) is refused', async () => {
    const c = await emptyDb();
    await applyMigrations(c);
    await c.query('INSERT INTO lane.schema_version (version) VALUES (999)');
    await assert.rejects(applyMigrations(c), /unknown|newer/);
  });
});
