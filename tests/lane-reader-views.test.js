import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { applyMigrations } from '../src/lane-db.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, enqueue, claim, tid } from './lane-dispatch-fixture.js';

const VIEWS = {
  pull_tickets: ['id', 'repo_key', 'lane', 'state', 'host', 'cpu_cores', 'requeues', 'result', 'created_at', 'started_at', 'finished_at', 'requires_tags', 'argv_md5'],
  pull_principals: ['host', 'tags', 'allowed_repos', 'max_claims', 'repo_mode'],
  pull_runners: ['host', 'last_seen'],
  pull_repo_audit: ['repo', 'named', 'submitter', 'submitter_version', 'state', 'host', 'covered_configured', 'covered_live', 'holder_would_deny', 'n'],
};

async function viewColumns(su, view) {
  const { rows } = await su.query(
    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'lane' AND table_name = $1 ORDER BY ordinal_position", [view]);
  return rows.map((r) => r.column_name);
}

async function readerCanReadEveryView(world) {
  const reader = await world.as(logins.reader);
  for (const view of Object.keys(VIEWS)) {
    const { rows } = await reader.query(`SELECT * FROM lane.${view}`);
    assert.ok(rows.length > 0, `lane_reader reads rows from lane.${view}`);
  }
}

describe('lane_reader pull-queue views (BRAIN-1154)', { skip: pgSkipOption, timeout: 180000 }, () => {
  let cluster;
  let world;
  before(async () => {
    cluster = await startDispatchCluster();
    world = await World.create(cluster);
    await enqueue(await world.as(logins.s1), tid(1));
    await (await world.su()).query(`UPDATE lane_dispatch.tickets SET header = header || '{"argv": ["npm", "test"]}' WHERE id = $1`, [tid(1)]);
    await claim(await world.as(logins.a1));
  });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(async () => { await cluster.closeClients(); world.connections.clear(); });

  test('each view exposes exactly the reader columns: no claim_token, login_role or raw header', async () => {
    const su = await world.su();
    for (const [view, columns] of Object.entries(VIEWS)) assert.deepEqual(await viewColumns(su, view), columns, `lane.${view}`);
    const reader = await world.as(logins.reader);
    await assert.rejects(reader.query('SELECT claim_token FROM lane.pull_tickets'), /column "claim_token" does not exist/);
    await assert.rejects(reader.query('SELECT login_role FROM lane.pull_principals'), /column "login_role" does not exist/);
  });

  test('lane_reader reads the views, sees a held ticket and its argv hash, and still has no USAGE on lane_dispatch', async () => {
    await readerCanReadEveryView(world);
    const reader = await world.as(logins.reader);
    const { rows: [ticket] } = await reader.query('SELECT state, host, argv_md5 FROM lane.pull_tickets WHERE id = $1', [tid(1)]);
    const su = await world.su();
    const { rows: [{ argv }] } = await su.query("SELECT header ->> 'argv' AS argv FROM lane_dispatch.tickets WHERE id = $1", [tid(1)]);
    assert.deepEqual(ticket, { state: 'claimed', host: 'a1', argv_md5: createHash('md5').update(argv).digest('hex') });
    assert.equal(argv, '["npm", "test"]');
    const { rows: principals } = await reader.query('SELECT host FROM lane.pull_principals ORDER BY host');
    assert.deepEqual(principals.map((p) => p.host), ['a1', 'a2', 'x'], 'only agent principals, which carry a host');
    await assert.rejects(reader.query('SELECT 1 FROM lane_dispatch.tickets'), /permission denied for schema lane_dispatch/);
    const { rows: [{ usage }] } = await su.query("SELECT has_schema_privilege('lane_reader', 'lane_dispatch', 'USAGE') AS usage");
    assert.equal(usage, false);
  });

  test('a functions.sql re-apply restores a revoked view grant, and a re-apply without the grant line leaves the reader refused', async () => {
    const su = await world.su();
    await su.query('REVOKE SELECT ON lane.pull_tickets, lane.pull_principals, lane.pull_runners, lane.pull_repo_audit FROM lane_reader');
    await applyMigrations(su);
    await readerCanReadEveryView(world);

    const dropGrant = (sql, file) => (file === 'functions.sql' ? sql.replace(/^\s*GRANT SELECT ON lane\.pull_(tickets|repo_audit).*$/gm, '') : sql);
    await su.query('REVOKE SELECT ON lane.pull_tickets, lane.pull_principals, lane.pull_runners, lane.pull_repo_audit FROM lane_reader');
    await applyMigrations(su, { transform: dropGrant });
    const reader = await world.as(logins.reader);
    for (const view of Object.keys(VIEWS)) {
      await assert.rejects(reader.query(`SELECT 1 FROM lane.${view}`), /permission denied for view/, `canary: without the grant lane.${view} is refused`);
    }
    await applyMigrations(su);
    await readerCanReadEveryView(world);
  });
});
