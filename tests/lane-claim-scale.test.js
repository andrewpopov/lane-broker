import { describe, test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { claimNext } from '../src/lane-db.js';
import { PgCluster, PG_SKIP_REASON, seedScale } from './lane-db-harness.js';

/**
 * BRAIN-400: claim_next at the size the spec plans for (60,000 queued jobs, about 40 active groups, 3 classes, 5 hosts).
 * Seeding takes under a second and 1,000 claims about eight, so this is the normal suite, not a gated one.
 * The bound is generous on purpose: a Mac measures p99 ~15 ms and a wintop at load 20 (shared with rouge sims) 67 to 154 ms. The regression it guards
 * is lane.jobs_queued_head or jobs_unfinished_stage going missing: the head lookup or the open-stage lookup then scans the whole group and a claim takes
 * 630 ms or more, p99 over a second.
 *
 * No table here is ever analysed (the harness runs with autovacuum off and never calls ANALYZE), so these also prove the plan does not depend on
 * statistics. PostgreSQL 17 once chose a 200 ms plan on a never-analysed 400-job table when the open stage was asked per job; asking once per
 * group and keying the head index on that stage removed the choice, and with it the autovacuum/ANALYZE workaround.
 */
const CALLS = Number(process.env.LANE_SCALE_CALLS ?? 1000);
const P99_BOUND_MS = Number(process.env.LANE_SCALE_P99_MS ?? 300);
const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)];

/** BRAIN-453: host noise allowance. The p99 of a trivial query on the same connection, measured BEFORE the workload and capped, so a
 *  saturated host widens the bound by what its round trips already cost, while a later load spike can never excuse a regression. */
const NOISE_CAP_MS = 100;
async function noiseAllowanceMs(client) {
  const times = [];
  for (let i = 0; i < 100; i += 1) {
    const start = process.hrtime.bigint();
    await client.query('SELECT 1');
    times.push(Number(process.hrtime.bigint() - start) / 1e6);
  }
  times.sort((a, b) => a - b);
  return Math.min(percentile(times, 0.99), NOISE_CAP_MS);
}

async function assertNeverAnalysed(su) {
  const { rows: [r] } = await su.query("SELECT last_analyze, last_autoanalyze FROM pg_stat_user_tables WHERE relid = 'lane.jobs'::regclass");
  assert.deepEqual(r, { last_analyze: null, last_autoanalyze: null }, 'the fixture must not have statistics for lane.jobs');
}

describe('lane.claim_next at scale', { skip: PG_SKIP_REASON ?? false, timeout: 600000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });

  test(`p99 over ${CALLS} claims on 60,000 queued jobs stays under ${P99_BOUND_MS} ms`, async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    const agents = await seedScale(su, { jobs: 60000, groups: 40, hosts: 5 });
        await assertNeverAnalysed(su);
    const clients = await Promise.all(agents.map((a) => cluster.client(db, a.login)));
    const noise = await noiseAllowanceMs(clients[0]);
    const times = [];
    for (let i = 0; i < CALLS; i++) {
      const start = process.hrtime.bigint();
      const got = await claimNext(clients[i % clients.length], { generation: 1, free: 8, memBytes: 1e12, token: randomUUID() });
      times.push(Number(process.hrtime.bigint() - start) / 1e6);
      assert.ok(got, `claim ${i} found nothing although jobs are queued`);
    }
    times.sort((a, b) => a - b);
    const p50 = percentile(times, 0.5);
    const p99 = percentile(times, 0.99);
    const bound = P99_BOUND_MS + noise;
    console.log(`claim_next at 60k queued jobs: p50 ${p50.toFixed(2)} ms, p99 ${p99.toFixed(2)} ms, max ${times.at(-1).toFixed(2)} ms over ${CALLS} calls (PostgreSQL ${await cluster.serverVersion()})`);
    assert.ok(p99 < bound, `p99 ${p99.toFixed(1)} ms is not under ${bound.toFixed(1)} ms (${P99_BOUND_MS} + ${noise.toFixed(1)} ms pre-measured host noise)`);
    const { rows: [{ n }] } = await su.query("SELECT count(*)::int AS n FROM lane.jobs WHERE state = 'claimed'");
    assert.equal(n, CALLS);
  });

  test('a never-analysed 400-job table claims in under 50 ms at p99 (the PostgreSQL 17 plan flip)', async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    const agents = await seedScale(su, { jobs: 400, groups: 8, hosts: 1 });
    await assertNeverAnalysed(su);
    const agent = await cluster.client(db, agents[0].login);
    const noise = await noiseAllowanceMs(agent);
    const times = [];
    for (let i = 0; i < 300; i++) {
      const start = process.hrtime.bigint();
      assert.ok(await claimNext(agent, { generation: 1, free: 8, memBytes: 1e12, token: randomUUID() }));
      times.push(Number(process.hrtime.bigint() - start) / 1e6);
    }
    times.sort((a, b) => a - b);
    const bound = 50 + noise;
    console.log(`claim_next on a never-analysed 400-job table: p50 ${percentile(times, 0.5).toFixed(2)} ms, p99 ${percentile(times, 0.99).toFixed(2)} ms (PostgreSQL ${await cluster.serverVersion()})`);
    assert.ok(percentile(times, 0.99) < bound, `p99 ${percentile(times, 0.99).toFixed(1)} ms is not under ${bound.toFixed(1)} ms (50 + ${noise.toFixed(1)} ms pre-measured host noise)`);
  });
});
