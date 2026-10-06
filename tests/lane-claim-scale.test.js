import { describe, test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { claimNext } from '../src/lane-db.js';
import { PgCluster, PG_SKIP_REASON, seedScale, SCALE_CLASSES } from './lane-db-harness.js';

/**
 * BRAIN-400: claim_next at the size the spec plans for (60,000 queued jobs, about 40 active groups, 3 classes, 5 hosts).
 * Seeding takes under a second and 1,000 claims about eight, so this is the normal suite, not a gated one.
 * The bound is generous on purpose: a Mac measures p99 ~15 ms and a wintop at load 20 (shared with rouge sims) 67 to 154 ms. The regression it guards
 * is lane.jobs_unfinished_stage going missing: stage_open then scans the whole group and a claim takes 630 ms or more, p99 over a second.
 */
const CALLS = Number(process.env.LANE_SCALE_CALLS ?? 1000);
const P99_BOUND_MS = Number(process.env.LANE_SCALE_P99_MS ?? 300);
const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)];

describe('lane.claim_next at scale', { skip: PG_SKIP_REASON ?? false, timeout: 600000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });

  test(`p99 over ${CALLS} claims on 60,000 queued jobs stays under ${P99_BOUND_MS} ms`, async () => {
    const db = await cluster.freshDb();
    const su = await cluster.client(db);
    const agents = await seedScale(su, { jobs: 60000, groups: 40, hosts: 5 });
    const room = Object.fromEntries(SCALE_CLASSES.map((cls) => [cls, 4]));
    const clients = await Promise.all(agents.map((a) => cluster.client(db, a.login)));
    const times = [];
    for (let i = 0; i < CALLS; i++) {
      const start = process.hrtime.bigint();
      const got = await claimNext(clients[i % clients.length], { generation: 1, room, memBytes: 1e12, token: randomUUID() });
      times.push(Number(process.hrtime.bigint() - start) / 1e6);
      assert.ok(got, `claim ${i} found nothing although jobs are queued`);
    }
    times.sort((a, b) => a - b);
    const p50 = percentile(times, 0.5);
    const p99 = percentile(times, 0.99);
    console.log(`claim_next at 60k queued jobs: p50 ${p50.toFixed(2)} ms, p99 ${p99.toFixed(2)} ms, max ${times.at(-1).toFixed(2)} ms over ${CALLS} calls (PostgreSQL ${await cluster.serverVersion()})`);
    assert.ok(p99 < P99_BOUND_MS, `p99 ${p99.toFixed(1)} ms is not under ${P99_BOUND_MS} ms`);
    const { rows: [{ n }] } = await su.query("SELECT count(*)::int AS n FROM lane.jobs WHERE state = 'claimed'");
    assert.equal(n, CALLS);
  });
});
