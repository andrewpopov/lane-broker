import { describe, test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { applyMigrations, claimNext } from '../src/lane-db.js';
import { PgCluster, PG_SKIP_REASON, roleNames, addGroup, gid, ROOM } from './lane-db-harness.js';

describe('lane schema, roles and claim bookkeeping', { skip: PG_SKIP_REASON ?? false, timeout: 60000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });

  const claim = (agent, gen = 1) => claimNext(agent, { generation: gen, room: ROOM, memBytes: 1e12, token: randomUUID() });

  test('re-applying the migrations is idempotent and keeps data', async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    await addGroup(su, { n: 1 });
    assert.deepEqual(await applyMigrations(su), ['001-schema.sql', '002-claim.sql']);
    const { rows } = await su.query('SELECT count(*)::int AS n FROM lane.jobs');
    assert.equal(rows[0].n, 1);
  });

  test('agents, submitters and readers have no raw table access; only definer functions', async () => {
    const db = await cluster.freshDb();
    for (const role of [roleNames.agent1, roleNames.submit, roleNames.reader]) {
      const c = await cluster.client(db, role);
      await assert.rejects(c.query('SELECT 1 FROM lane.jobs'), /permission denied/, `${role} read lane.jobs`);
      await assert.rejects(c.query('SELECT 1 FROM lane.principals'), /permission denied/);
      await assert.rejects(c.query("UPDATE lane.cluster SET claims_enabled = false"), /permission denied/);
    }
    const agent = await cluster.client(db, roleNames.agent1);
    await assert.rejects(agent.query("SELECT lane.activate_group('00000000-0000-0000-0000-000000000001')"), /permission denied/);
    const reader = await cluster.client(db, roleNames.reader);
    assert.deepEqual((await reader.query('SELECT * FROM lane.job_state_counts')).rows, []);
    await assert.rejects(reader.query('SELECT lane.claim_next(1, \'{}\', 0, \'{}\', gen_random_uuid())'), /permission denied/);
  });

  test('a claim stamps the job, the group, the stride and the outbox from one post-lock clock', async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    await addGroup(su, { n: 1, jobs: [{ cls: 'test' }] });
    const agent = await cluster.client(db, roleNames.agent1);
    const got = await claim(agent);
    assert.equal(got.group_id, gid(1));
    assert.equal(got.epoch, 1);
    assert.equal(got.grant_cpu, 1);
    const { rows: [r] } = await su.query(
      `SELECT j.state, j.host, j.lease_until - g.last_claim_at AS lease, v.vtime, e.kind, e.created_at = g.last_claim_at AS same_clock
         FROM lane.jobs j JOIN lane.groups g ON g.id = j.group_id, lane.class_vtime v, lane.transition_events e`);
    assert.deepEqual({ ...r, lease: r.lease.seconds }, { state: 'claimed', host: 'h1', lease: 30, vtime: 1, kind: 'claimed', same_clock: true });
  });

  test('a stale generation is refused, and a disabled cluster hands out nothing', async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    await addGroup(su, { n: 1 });
    const agent = await cluster.client(db, roleNames.agent1);
    await assert.rejects(claim(agent, 2), /STALE_GENERATION/);
    await su.query('UPDATE lane.cluster SET claims_enabled = false');
    assert.equal(await claim(agent), null);
  });

  test('destination authorisation is checked at claim time', async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    await addGroup(su, { n: 1 });
    await su.query("UPDATE lane.principals SET allowed_dest_hosts = '{h2}' WHERE kind = 'submit'");
    assert.equal(await claim(await cluster.client(db, roleNames.agent1)), null);
  });

  test('transition events are immutable', async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    await addGroup(su, { n: 1 });
    await claim(await cluster.client(db, roleNames.agent1));
    await assert.rejects(su.query("UPDATE lane.transition_events SET kind = 'lost'"), /not allowed/);
    await assert.rejects(su.query('DELETE FROM lane.transition_events'), /not allowed/);
  });
});
