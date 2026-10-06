import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

/**
 * BRAIN-400: the thin Postgres client for the unified queue. Nothing else in the broker uses it yet.
 * It connects, applies the versioned `sql/lane` migrations, and calls `lane.claim_next`.
 */
export const LANE_SQL_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'sql', 'lane');

/** Connect with a pg client config (host may be a Unix socket directory). The caller owns `end()`. */
export async function connect(config) {
  const client = new pg.Client(config);
  await client.connect();
  return client;
}

const TERMINAL_VERSION_ERROR = 'lane schema';

/** The numbered table migrations in `dir/migrations`, as [{version, file}], required to run 1..N with no gap or duplicate. */
function listMigrations(dir) {
  const files = fs.readdirSync(path.join(dir, 'migrations')).filter((f) => /^\d+-.*\.sql$/.test(f)).sort();
  const migrations = files.map((file) => ({ version: Number.parseInt(file, 10), file }));
  migrations.forEach((m, i) => {
    if (m.version !== i + 1) throw new Error(`${TERMINAL_VERSION_ERROR}: migration files must be numbered 1..N without gaps or duplicates, found ${m.file} at position ${i + 1}`);
  });
  return migrations;
}

/**
 * The version of the lane schema in the database: 0 when there is none (no schema, or an empty one). A schema this code does not
 * recognise is REFUSED, never merged: tables without a version row (a pre-versioning install), an empty version table, or a
 * version this code has no migration file for (a newer deployment, or a hand edit).
 */
async function schemaVersion(client, migrations) {
  const { rows: [s] } = await client.query(
    `SELECT to_regnamespace('lane') IS NOT NULL AS has_schema, to_regclass('lane.schema_version') IS NOT NULL AS has_version,
            (SELECT count(*)::int FROM pg_class WHERE relnamespace = to_regnamespace('lane'))
              + (SELECT count(*)::int FROM pg_proc WHERE pronamespace = to_regnamespace('lane')) AS objects`);
  if (!s.has_schema || (!s.has_version && s.objects === 0)) return 0;
  if (!s.has_version) throw new Error(`${TERMINAL_VERSION_ERROR} exists but is unversioned (no lane.schema_version): refusing to merge into it; migrate or drop it by hand`);
  const { rows } = await client.query('SELECT version FROM lane.schema_version ORDER BY version');
  if (rows.length === 0) throw new Error(`${TERMINAL_VERSION_ERROR} is unversioned (lane.schema_version has no version row): refusing to merge into it`);
  const known = new Set(migrations.map((m) => m.version));
  const unknown = rows.map((r) => r.version).filter((v) => !known.has(v));
  if (unknown.length) throw new Error(`${TERMINAL_VERSION_ERROR} is at unknown or newer version(s) ${unknown.join(', ')}; this code knows 1..${migrations.length}`);
  const latest = rows.at(-1).version;
  if (rows.length !== latest) throw new Error(`${TERMINAL_VERSION_ERROR} version rows are not contiguous from 1: ${rows.map((r) => r.version).join(', ')}`);
  return latest;
}

async function inTransaction(client, fn) {
  await client.query('BEGIN');
  try {
    await fn();
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/**
 * Bring the lane schema to the latest version: every `migrations/NNN-*.sql` above the recorded version runs in order, each in its
 * own transaction together with its lane.schema_version row, then `functions.sql` (CREATE OR REPLACE, ownership and grants) is
 * re-applied in a transaction of its own. Concurrent appliers are serialised by a session advisory lock.
 * `transform(sql, file)` rewrites a file before it runs (tests use it to break the SQL on purpose).
 * Returns the version reached and the migration files applied by THIS call.
 */
export async function applyMigrations(client, { dir = LANE_SQL_DIR, transform = (sql) => sql } = {}) {
  const migrations = listMigrations(dir);
  await client.query("SELECT pg_advisory_lock(hashtext('lane_migrate'))");
  try {
    const current = await schemaVersion(client, migrations);
    const applied = [];
    for (const m of migrations.filter((x) => x.version > current)) {
      await inTransaction(client, async () => {
        await client.query(transform(fs.readFileSync(path.join(dir, 'migrations', m.file), 'utf8'), m.file));
        await client.query('INSERT INTO lane.schema_version (version) VALUES ($1)', [m.version]);
      });
      applied.push(m.file);
    }
    await inTransaction(client, () => client.query(transform(fs.readFileSync(path.join(dir, 'functions.sql'), 'utf8'), 'functions.sql')));
    return { version: migrations.length, applied };
  } finally {
    await client.query("SELECT pg_advisory_unlock(hashtext('lane_migrate'))").catch(() => {});
  }
}

/**
 * One claim attempt. `free` is the CPU free before floors, `used` the CPU in use per work class and `closed` the work
 * classes shut by local gates (idle gate, exhausted cap); floors and caps are applied server-side. Returns the claim row, or null.
 */
export async function claimNext(client, { generation, free, used = {}, closed = [], memBytes, heldKeys = [], token }) {
  const { rows } = await client.query(
    'SELECT * FROM lane.claim_next($1::bigint, $2::real, $3::jsonb, $4::text[], $5::bigint, $6::text[], $7::uuid)',
    [generation, free, JSON.stringify(used), closed, memBytes, heldKeys, token],
  );
  return rows[0] ?? null;
}
