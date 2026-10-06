import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { claimNext } from '../src/lane-db.js';
import { PgCluster, PG_SKIP_REASON, roleNames, seedScale, SCALE_CLASSES } from './lane-db-harness.js';

/**
 * BRAIN-400: lock-order soak (spec 5.5, 11.1 and the soak in section 10). Concurrent claimers run alongside completions,
 * releases, cancels, policy edits, group cleanup, submissions and a simulated fleet tier (shard -> experiment -> attempt,
 * then group, then job, never lane_sched). Every actor follows the global lock order, so the run must show
 *   - zero deadlocks (SQLSTATE 40P01),
 *   - zero double claims (a job handed out twice at one epoch, or while still held), and
 *   - zero lost jobs (every submitted job accounted for, client and database tallies equal).
 *
 *   node tests/lane-soak.js --minutes 30 [--claimers 8]
 * `mutate` ([from, to]) breaks sql/lane the way the harness does for the ordering canaries.
 * `injectBadOrder` adds one actor that locks job then group, the reverse of every other transaction. It exists to prove
 * the detector fires; a soak that cannot report a deadlock proves nothing.
 */
const TERMINAL = "('succeeded','failed','cancelled','skipped','lost')";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = (n) => Math.floor(Math.random() * n);
const ROOM = Object.fromEntries(SCALE_CLASSES.map((cls) => [cls, 4]));

export async function runSoak({ minutes, claimers = 8, injectBadOrder = false, mutate, groups = 40, jobs = 8000, queuedTarget = 6000, log = console.log }) {
  const cluster = PgCluster.start();
  const stats = {
    claims: 0, nullClaims: 0, completed: 0, released: 0, started: 0, cancelled: 0, policyEdits: 0, groupsClosed: 0, submittedGroups: 0,
    deadlocks: 0, doubleClaims: 0, otherErrors: 0, anomalies: 0, claimMs: [], samples: [],
  };
  const note = (kind, text) => { if (stats.samples.length < 20) stats.samples.push(`${kind}: ${text}`); };
  const outstanding = new Map();       // job id -> {epoch, token, group, host}: handed out and not yet completed or released
  const seenEpochs = new Set();        // "job:epoch" ever handed out
  let stopping = false;

  try {
    const db = await cluster.freshDb({ mutate });
    const admin = () => cluster.client(db);
    const seedClient = await admin();
    const agents = await seedScale(seedClient, { jobs, groups, hosts: 5 });
    await seedClient.query('CREATE TABLE public.soak_fleet (k int NOT NULL, tier int NOT NULL, PRIMARY KEY (k, tier))');
    await seedClient.query('INSERT INTO public.soak_fleet SELECT k, t FROM generate_series(0, 15) k, generate_series(1, 3) t');
    const baseJobs = (await seedClient.query('SELECT count(*)::int AS n FROM lane.jobs')).rows[0].n;
    let submittedJobs = baseJobs;
    const deadline = Date.now() + minutes * 60000;

    async function tx(client, label, fn) {
      try {
        await client.query('BEGIN');
        const result = await fn(client);
        await client.query('COMMIT');
        return result;
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        if (err.code === '40P01') { stats.deadlocks++; note('deadlock', `${label}: ${err.message}`); } else { stats.otherErrors++; note('error', `${label}: ${err.message}`); }
        return undefined;
      }
    }
    const lockSched = (c) => c.query("SELECT pg_advisory_xact_lock(hashtext('lane_sched'))");
    const lockHost = (c, host) => c.query('SELECT 1 FROM lane.hosts WHERE host_id = $1 FOR UPDATE', [host]);
    const lockGroup = (c, id) => c.query('SELECT 1 FROM lane.groups WHERE id = $1 FOR UPDATE', [id]);
    const appendEvent = (c, jobId, epoch, kind) => c.query(
      `INSERT INTO lane.transition_events (job_id, epoch, seq, kind)
       SELECT $1, $2, coalesce(max(seq), 0) + 1, $3 FROM lane.transition_events WHERE job_id = $1`, [jobId, epoch, kind]);

    // Claimers: the real claim_next, as the real per-host agent login.
    async function claimer(i) {
      const client = await cluster.client(db, agents[i % agents.length].login);
      while (!stopping) {
        const started = process.hrtime.bigint();
        let got;
        try {
          got = await claimNext(client, { generation: 1, room: ROOM, memBytes: 1e12, token: randomUUID() });
        } catch (err) {
          await client.query('ROLLBACK').catch(() => {});
          if (err.code === '40P01') { stats.deadlocks++; note('deadlock', `claim: ${err.message}`); } else { stats.otherErrors++; note('error', `claim: ${err.message}`); }
          continue;
        }
        stats.claimMs.push(Number(process.hrtime.bigint() - started) / 1e6);
        if (!got) { stats.nullClaims++; await sleep(5); continue; }
        stats.claims++;
        const key = `${got.job_id}:${got.epoch}`;
        if (seenEpochs.has(key) || outstanding.has(got.job_id)) { stats.doubleClaims++; note('double claim', key); continue; }
        seenEpochs.add(key);
        outstanding.set(got.job_id, { epoch: got.epoch, token: got.claim_token, group: got.group_id, host: agents[i % agents.length].host });
      }
    }

    // Completion or release of a handed-out job: lane_sched -> host -> group -> job, outbox row last.
    async function finish(client, jobId, outcome) {
      const held = outstanding.get(jobId);
      if (!held) return;
      outstanding.delete(jobId);          // the job stays claimed in the database until the commit, so no claimer can see it early
      const ok = await tx(client, outcome, async (c) => {
        await lockSched(c);
        await lockHost(c, held.host);
        await lockGroup(c, held.group);
        const { rows } = await c.query('SELECT state, epoch, claim_token FROM lane.jobs WHERE id = $1 FOR UPDATE', [jobId]);
        const j = rows[0];
        if (!j || !['claimed', 'running'].includes(j.state) || j.epoch !== held.epoch || j.claim_token !== held.token) {
          note('anomaly', `job ${jobId} epoch ${held.epoch} found ${JSON.stringify(j)}`);
          stats.anomalies++;
          return false;
        }
        if (outcome === 'released') {
          await c.query("UPDATE lane.jobs SET state = 'queued', host = NULL, claim_token = NULL, lease_until = NULL WHERE id = $1", [jobId]);
        } else {
          await c.query("UPDATE lane.jobs SET state = 'succeeded', finished_at = now(), exit_code = 0 WHERE id = $1", [jobId]);
        }
        await appendEvent(c, jobId, held.epoch, outcome);
        return true;
      });
      if (ok) stats[outcome === 'released' ? 'released' : 'completed']++;
    }
    async function completer() {
      const client = await admin();
      while (!stopping || outstanding.size) {
        const ids = [...outstanding.keys()];
        if (!ids.length) { await sleep(5); continue; }
        await sleep(rand(20));
        await finish(client, ids[rand(ids.length)], Math.random() < 0.15 ? 'released' : 'succeeded');
      }
    }

    // Cancel-apply: cancel a few queued jobs of one group. lane_sched -> host -> group -> jobs.
    async function canceller() {
      const client = await admin();
      while (!stopping) {
        await sleep(20 + rand(60));
        const host = agents[rand(agents.length)].host;
        const count = await tx(client, 'cancel', async (c) => {
          await lockSched(c);
          await lockHost(c, host);
          const { rows: [g] } = await c.query("SELECT id FROM lane.groups WHERE state = 'active' ORDER BY random() LIMIT 1");
          if (!g) return 0;
          await lockGroup(c, g.id);
          const { rows } = await c.query("SELECT id, epoch FROM lane.jobs WHERE group_id = $1 AND state = 'queued' ORDER BY id LIMIT 3 FOR UPDATE", [g.id]);
          for (const j of rows) {
            await c.query("UPDATE lane.jobs SET state = 'cancelled', finished_at = now() WHERE id = $1", [j.id]);
            await appendEvent(c, j.id, j.epoch, 'cancelled');
          }
          return rows.length;
        });
        stats.cancelled += count ?? 0;
      }
    }

    // Policy change: lane_sched -> host, rewrite the class weights.
    async function policyEditor() {
      const client = await admin();
      while (!stopping) {
        await sleep(50 + rand(100));
        const host = agents[rand(agents.length)].host;
        const done = await tx(client, 'policy', async (c) => {
          await lockSched(c);
          await lockHost(c, host);
          await c.query('UPDATE lane.host_class_policy SET weight = 1 + floor(random() * 4) WHERE host_id = $1 AND class = $2', [host, SCALE_CLASSES[rand(SCALE_CLASSES.length)]]);
          return true;
        });
        if (done) stats.policyEdits++;
      }
    }

    // Close finished groups: lane_sched -> group.
    async function groupCloser() {
      const client = await admin();
      while (!stopping) {
        await sleep(200);
        const closed = await tx(client, 'close groups', async (c) => {
          await lockSched(c);
          const { rows } = await c.query(
            `SELECT id FROM lane.groups g WHERE g.state = 'active'
                AND NOT EXISTS (SELECT 1 FROM lane.jobs j WHERE j.group_id = g.id AND j.state NOT IN ${TERMINAL})
              ORDER BY id LIMIT 20 FOR UPDATE`);
          for (const g of rows) await c.query("UPDATE lane.groups SET state = 'done' WHERE id = $1", [g.id]);
          return rows.length;
        });
        stats.groupsClosed += closed ?? 0;
      }
    }

    // Submissions through the real functions, as the real submit login, so activate_group's lane_sched lock is exercised.
    async function submitter() {
      const submit = await cluster.client(db, roleNames.submit);
      const meter = await admin();
      while (!stopping) {
        await sleep(100);
        const { rows: [{ n }] } = await meter.query("SELECT count(*)::int AS n FROM lane.jobs WHERE state = 'queued'");
        if (n >= queuedTarget) continue;
        const batch = Array.from({ length: 200 }, (_, i) => ({
          idem_key: `s${i}`, class: SCALE_CLASSES[i % SCALE_CLASSES.length], template_id: 't', template_version: 1,
          est_ref_ms: 100 + rand(60000), cpu_req: 1, dup_safe: true,
        }));
        try {
          const { rows: [{ id }] } = await submit.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', $2::smallint, '{}'::jsonb, 'x', $3::jsonb) AS id", [randomUUID(), rand(3), JSON.stringify(batch)]);
          await submit.query('SELECT lane.activate_group($1)', [id]);
          submittedJobs += batch.length;
          stats.submittedGroups++;
        } catch (err) {
          if (err.code === '40P01') { stats.deadlocks++; note('deadlock', `submit: ${err.message}`); } else { stats.otherErrors++; note('error', `submit: ${err.message}`); }
        }
      }
    }

    // Fleet tier (spec 5.5 R2): fleet rows shard -> experiment -> attempt, then group, then job; never lane_sched.
    // Marks a claimed job running, exactly as a rouge-initiated start would.
    async function fleet() {
      const client = await admin();
      while (!stopping) {
        await sleep(5 + rand(15));
        const ids = [...outstanding.keys()];
        if (!ids.length) continue;
        const jobId = ids[rand(ids.length)];
        const held = outstanding.get(jobId);
        if (!held) continue;
        const started = await tx(client, 'fleet', async (c) => {
          await c.query('SELECT 1 FROM public.soak_fleet WHERE k = $1 ORDER BY tier FOR UPDATE', [jobId % 16]);
          await lockGroup(c, held.group);
          const { rows } = await c.query("SELECT epoch FROM lane.jobs WHERE id = $1 AND state = 'claimed' FOR UPDATE", [jobId]);
          if (!rows.length || rows[0].epoch !== held.epoch) return false;
          await c.query("UPDATE lane.jobs SET state = 'running', started_at = now() WHERE id = $1", [jobId]);
          await appendEvent(c, jobId, held.epoch, 'started');
          return true;
        });
        if (started) stats.started++;
      }
    }

    // The reverse order (job, then group) held open long enough to meet a claimer that took the group first.
    async function badOrder() {
      const client = await admin();
      while (!stopping) {
        await sleep(2);
        await tx(client, 'bad order', async (c) => {
          const { rows } = await c.query("SELECT id, group_id FROM lane.jobs WHERE state = 'queued' ORDER BY id LIMIT 1");
          if (!rows.length) return;
          await c.query('SELECT 1 FROM lane.jobs WHERE id = $1 FOR UPDATE', [rows[0].id]);
          await sleep(15);
          await lockGroup(c, rows[0].group_id);
        });
      }
    }

    const actors = [
      ...Array.from({ length: claimers }, (_, i) => claimer(i)),
      completer(), completer(), canceller(), policyEditor(), groupCloser(), submitter(), fleet(), fleet(),
      ...(injectBadOrder ? [badOrder()] : []),
    ];
    const begun = Date.now();
    const ticker = setInterval(() => {
      const secs = (Date.now() - begun) / 1000;
      log(`soak ${secs.toFixed(0)}s claims ${stats.claims} (${(stats.claims / secs).toFixed(1)}/s) completed ${stats.completed} released ${stats.released} started ${stats.started} cancelled ${stats.cancelled} submitted groups ${stats.submittedGroups} deadlocks ${stats.deadlocks} doubles ${stats.doubleClaims} errors ${stats.otherErrors} anomalies ${stats.anomalies}`);
    }, 30000);
    while (Date.now() < deadline && !(injectBadOrder && stats.deadlocks)) await sleep(250);
    stopping = true;
    await Promise.all(actors);          // completers drain whatever is still held
    clearInterval(ticker);
    const seconds = (Date.now() - begun) / 1000;

    const audit = await seedClient.query(
      `SELECT (SELECT count(*)::int FROM lane.jobs) AS total,
              (SELECT count(*)::int FROM lane.jobs WHERE state = 'queued') AS queued,
              (SELECT count(*)::int FROM lane.jobs WHERE state = 'succeeded') AS succeeded,
              (SELECT count(*)::int FROM lane.jobs WHERE state = 'cancelled') AS cancelled,
              (SELECT count(*)::int FROM lane.jobs WHERE state NOT IN ('queued','succeeded','cancelled')) AS stranded,
              (SELECT count(*)::int FROM lane.jobs j WHERE j.epoch <> (SELECT count(*) FROM lane.transition_events e WHERE e.job_id = j.id AND e.kind = 'claimed')) AS epoch_mismatch,
              (SELECT count(*)::int FROM (SELECT 1 FROM lane.transition_events WHERE kind = 'claimed' GROUP BY job_id, epoch HAVING count(*) > 1) d) AS duplicate_claim_events`);
    const a = audit.rows[0];
    const lost = Math.abs(a.total - submittedJobs) + a.stranded + Math.abs(a.succeeded - stats.completed) + Math.abs(a.cancelled - stats.cancelled)
      + Math.abs(a.total - a.queued - a.succeeded - a.cancelled);
    const doubles = stats.doubleClaims + a.epoch_mismatch + a.duplicate_claim_events;
    stats.claimMs.sort((x, y) => x - y);
    const pct = (p) => stats.claimMs[Math.min(stats.claimMs.length - 1, Math.ceil(stats.claimMs.length * p) - 1)] ?? 0;
    const result = {
      seconds: Math.round(seconds), claimers, claims: stats.claims, claimsPerSecond: +(stats.claims / seconds).toFixed(1),
      claimMsP50: +pct(0.5).toFixed(2), claimMsP99: +pct(0.99).toFixed(2), claimMsMax: +(stats.claimMs.at(-1) ?? 0).toFixed(2),
      completed: stats.completed, released: stats.released, started: stats.started, cancelled: stats.cancelled,
      policyEdits: stats.policyEdits, groupsClosed: stats.groupsClosed, submittedGroups: stats.submittedGroups,
      deadlocks: stats.deadlocks, doubleClaims: doubles, lostJobs: lost, anomalies: stats.anomalies, otherErrors: stats.otherErrors,
      database: { ...a, submittedJobs }, samples: stats.samples, postgres: await cluster.serverVersion(),
    };
    return result;
  } finally {
    stopping = true;
    await cluster.closeClients();
    cluster.stop();
  }
}

export const soakPassed = (r) => r.deadlocks === 0 && r.doubleClaims === 0 && r.lostJobs === 0 && r.anomalies === 0 && r.otherErrors === 0 && r.claims > 0;

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const arg = (name, fallback) => { const i = process.argv.indexOf(`--${name}`); return i === -1 ? fallback : process.argv[i + 1]; };
  if (PG_SKIP_REASON) { console.error(PG_SKIP_REASON); process.exit(2); }
  const result = await runSoak({
    minutes: Number(arg('minutes', 1)), claimers: Number(arg('claimers', 8)), injectBadOrder: process.argv.includes('--inject-bad-order'),
  });
  console.log(JSON.stringify(result, null, 2));
  console.log(soakPassed(result) ? 'SOAK PASS' : 'SOAK FAIL');
  process.exit(soakPassed(result) ? 0 : 1);
}
