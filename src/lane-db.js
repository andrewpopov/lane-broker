import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

/**
 * BRAIN-400: the thin Postgres client for the unified queue. Nothing else in the broker uses it yet.
 * It connects, applies the idempotent `sql/lane` migrations in file order, and calls `lane.claim_next`.
 */
export const LANE_SQL_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'sql', 'lane');

/** Connect with a pg client config (host may be a Unix socket directory). The caller owns `end()`. */
export async function connect(config) {
  const client = new pg.Client(config);
  await client.connect();
  return client;
}

/** Apply every `NNN-*.sql` file in order. Each file is idempotent, so re-applying is always safe. */
export async function applyMigrations(client, dir = LANE_SQL_DIR) {
  const files = fs.readdirSync(dir).filter((f) => /^\d+-.*\.sql$/.test(f)).sort();
  for (const file of files) await client.query(fs.readFileSync(path.join(dir, file), 'utf8'));
  return files;
}

/** One claim attempt. Returns the claim row, or null when nothing was claimed. */
export async function claimNext(client, { generation, room, memBytes, heldKeys = [], token }) {
  const { rows } = await client.query(
    'SELECT * FROM lane.claim_next($1::bigint, $2::jsonb, $3::bigint, $4::text[], $5::uuid)',
    [generation, JSON.stringify(room), memBytes, heldKeys, token],
  );
  return rows[0] ?? null;
}
