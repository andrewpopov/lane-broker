import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgCluster, PG_SKIP_REASON, roleNames, ago, gid, addGroup, setParent, jobsJson, claimAs } from './lane-db-harness.js';

/**
 * BRAIN-400: the Codex review of task 3 (findings F1, F3-F9; F2 is lane-migrations.test.js). Each test was written first and watched
 * failing on the reviewed commit, then the fix landed. Numbers in the names are the review's finding numbers.
 */
describe('lane review findings', { skip: PG_SKIP_REASON ?? false, timeout: 300000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  async function open(opts) {
    const db = await cluster.freshDb(opts);
    return { db, su: await cluster.client(db), agent: await cluster.client(db, roleNames.agent1), submit: await cluster.client(db, roleNames.submit) };
  }
  const jobOf = async (su, id) => (await su.query('SELECT * FROM lane.jobs WHERE id = $1', [id])).rows[0];
  const reservations = async (su) => (await su.query('SELECT state FROM lane.host_reservations')).rows.map((r) => r.state);

  // F1: A1 must never hide an overdue parent's ordinary jobs behind an exclusive one.
  async function overdueClaim(opts, siblings) {
    const { su, agent } = await open(opts);
    if (siblings) {
      await addGroup(su, { n: 1, account: 'cal', prio: 'scavenger', activatedAt: ago(25 * 60), jobs: [{ cls: 'sim', est: 600, exclusive: true }] });
      await addGroup(su, { n: 3, account: 'cal', prio: 'scavenger', activatedAt: ago(24 * 60), jobs: [{ cls: 'sim', est: 60 }] });
    } else {
      await addGroup(su, { n: 1, account: 'cal', prio: 'scavenger', activatedAt: ago(25 * 60), jobs: [{ cls: 'sim', est: 600, exclusive: true }, { cls: 'sim', est: 60 }] });
    }
    await addGroup(su, { n: 2, account: 'human', prio: 'gate', activatedAt: ago(1), jobs: [{ cls: 'test' }] });
    const got = await claimAs(agent);
    return { group: got.group_id, exclusive: (await jobOf(su, got.job_id)).exclusive, reservations: await reservations(su) };
  }
  test('F1: an overdue parent with an exclusive 600 s job and an ordinary 60 s job in ONE group claims the ordinary job', async () => {
    assert.deepEqual(await overdueClaim(undefined, false), { group: gid(1), exclusive: false, reservations: [] });
  });
  test('F1: an overdue parent whose OLDER sibling group holds only an exclusive job still claims its ordinary sibling job', async () => {
    assert.deepEqual(await overdueClaim(undefined, true), { group: gid(3), exclusive: false, reservations: [] });
  });
  test('F1 canary: choosing the longest job first hides the ordinary job behind the exclusive one in a group, and the fresh gate wins', async () => {
    const r = await overdueClaim({ mutate: ['ORDER BY (c.p_overdue AND x.exclusive), x.est_p50_s DESC', 'ORDER BY x.est_p50_s DESC'] }, false);
    assert.equal(r.group, gid(2));                       // the exclusive job is not overdue, so the fresh gate wins
  });
  test('F1 canary: ordering siblings by age alone lets an older exclusive sibling hide the ordinary one', async () => {
    const r = await overdueClaim({ mutate: ['ORDER BY pid, class, (p_overdue AND exclusive), g_overdue DESC', 'ORDER BY pid, class, g_overdue DESC'] }, true);
    assert.equal(r.group, gid(2));                       // the exclusive job is not overdue, so the fresh gate wins
  });

  // F3: the service clock moves only on a real transition.
  async function clocksAfterRetries(opts) {
    const { su, submit } = await open(opts);
    const anchor = ago(21 * 60);
    const lastClaim = ago(5 * 60);
    await addGroup(su, { n: 1, account: 'acct', prio: 'normal', activatedAt: anchor, parentAnchor: anchor, parentLastClaim: lastClaim });
    await addGroup(su, { n: 2, account: 'acct', prio: 'gate', activatedAt: anchor, parentAnchor: anchor, parentLastClaim: lastClaim });
    const parents = async () => (await su.query('SELECT prio_class, aging_anchor, last_claim_at FROM lane.sched_parents ORDER BY prio_class')).rows;
    const before = await parents();
    const retried = (await submit.query('SELECT lane.activate_group($1) AS ok', [gid(1)])).rows[0].ok;
    const afterRetry = await parents();
    await submit.query("SELECT lane.reclass($1, 'normal')", [gid(1)]);
    const afterSameClass = await parents();
    await submit.query("SELECT lane.reclass($1, 'gate')", [gid(1)]);        // the target parent already has an active group: it keeps its clock
    return { before, retried, afterRetry, afterSameClass, afterMove: await parents() };
  }
  test('F3: retrying activate_group and a same-class reclass leave the parent anchor and last_claim_at unchanged; a move into a busy parent too', async () => {
    const r = await clocksAfterRetries();
    assert.equal(r.retried, false);
    assert.deepEqual([r.afterRetry, r.afterSameClass, r.afterMove], [r.before, r.before, r.before]);
  });
  test('F3 canary: a retried activation that restarts the clock moves the anchor, so the test catches it', async () => {
    const r = await clocksAfterRetries({ mutate: ['  IF NOT FOUND THEN RETURN false; END IF;                                      -- a retry is not an activation: no clock moves', ''] });
    assert.notDeepEqual(r.afterRetry, r.before);
  });

  // F4: live CPU follows the group to its new parent.
  async function reclassIntoDemotion(opts) {
    const { su, agent, submit } = await open(opts);
    await su.query('UPDATE lane.config SET interactive_max_cores = 4');
    await su.query("UPDATE lane.principals SET max_class = 'interactive', can_interactive = true WHERE kind = 'submit'");
    await addGroup(su, { n: 1, account: 'acct', prio: 'normal', jobs: [{ cls: 'test', count: 2 }] });
    await su.query("UPDATE lane.jobs SET state = 'running', host = 'h1', grant_cpu = 8, epoch = 1 WHERE id = (SELECT min(id) FROM lane.jobs)");
    await setParent(su, 'acct', 'normal', { running_cpu: 8 });
    await addGroup(su, { n: 2, account: 'robot', prio: 'gate', jobs: [{ cls: 'sim' }] });
    await su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'test', 50), ('h1', 'sim', 0)");
    await submit.query("SELECT lane.reclass($1, 'interactive')", [gid(1)]);
    const { rows } = await su.query('SELECT prio_class, running_cpu FROM lane.sched_parents ORDER BY prio_class');
    return { winner: (await claimAs(agent)).group_id, running: Object.fromEntries(rows.map((r) => [r.prio_class, r.running_cpu])) };
  }
  test('F4: reclassing a group with 8 running cores into interactive moves the cores, so the over-limit parent is demoted to rank 3', async () => {
    const r = await reclassIntoDemotion();
    assert.equal(r.running.interactive, 8);
    assert.equal(r.running.normal, 0);
    assert.equal(r.winner, gid(2));
  });
  test('F4 canary: without crediting the new parent the group keeps rank 4 and wins', async () => {
    const r = await reclassIntoDemotion({ mutate: ['    PERFORM lane.add_running(v_pid, v_held, v_now);', ''] });
    assert.deepEqual([r.running.interactive, r.winner], [0, gid(1)]);
  });

  // F5: usage is the integral of the decayed contribution over the interval.
  async function settledUsage(steps, opts) {
    const { su } = await open(opts);
    const t0 = new Date('2026-01-01T00:00:00Z');
    await addGroup(su, { n: 1, account: 'acct', prio: 'normal', jobs: [{}] });
    await setParent(su, 'acct', 'normal', { running_cpu: 4, usage_decayed: 0, usage_at: t0 });
    for (let i = 1; i <= steps; i++) {
      await su.query("SELECT lane.add_running(id, 0, $1::timestamptz + make_interval(secs => 1800.0 * $2 / $3)) FROM lane.sched_parents",
        [t0, i, steps]);
    }
    const { rows: [r] } = await su.query("SELECT lane.decayed(p, $1::timestamptz + interval '30 minutes') AS u FROM lane.sched_parents p WHERE p.account = 'acct' AND p.prio_class = 'normal'", [t0]);
    return r.u;
  }
  test('F5: 4 cores for 30 minutes settle to about 5194 core-seconds, and 1 step agrees with 30 steps within 1%', async () => {
    const one = await settledUsage(1);
    const many = await settledUsage(30);
    assert.ok(Math.abs(one - 5194) < 52, `one step: ${one}`);
    assert.ok(Math.abs(one - many) / one < 0.01, `1 step ${one} vs 30 steps ${many}`);
  });
  test('F5 canary: integrating the rate without the decay settles to the wrong total', async () => {
    const one = await settledUsage(1, { mutate: ['(1800 / ln(2::double precision))', '1800'] });
    assert.ok(Math.abs(one - 5194) > 500, `got ${one}`);
  });
  test('F5: a claim settles the held cores through the same decay (not as 4 x 1800 fresh core-seconds)', async () => {
    const { su, agent } = await open();
    await addGroup(su, { n: 1, account: 'acct', prio: 'normal', jobs: [{}] });
    await setParent(su, 'acct', 'normal', { running_cpu: 4, usage_decayed: 0, usage_at: ago(1800) });
    await claimAs(agent);
    const { rows: [r] } = await su.query("SELECT usage_decayed FROM lane.sched_parents WHERE account = 'acct' AND prio_class = 'normal'");
    assert.ok(Math.abs(r.usage_decayed - 5194) < 60, `got ${r.usage_decayed}`);
  });

  // F6: estimates belong to the server.
  test('F6: a 0.01 s estimate hint is stored as the work-class default (900 s sim, 300 s test) with est_source default; a larger hint is kept as a hint', async () => {
    const { su, submit } = await open();
    const jobs = [...jobsJson(1, { cls: 'test', est: 0.01 }), { ...jobsJson(1, { cls: 'sim', est: 0.01 })[0], idem_key: 'sim0' },
      { ...jobsJson(1, { cls: 'test', est: 5000 })[0], idem_key: 'big' }, { ...jobsJson(1, { cls: 'sim' })[0], idem_key: 'none', est_p50_s: null }];
    await submit.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', 'normal', '{}'::jsonb, 'x', $2::jsonb)", [randomUUID(), JSON.stringify(jobs)]);
    const { rows } = await su.query('SELECT idem_key, est_p50_s, est_p90_s, est_source FROM lane.jobs ORDER BY idem_key');
    assert.deepEqual(rows.map((r) => [r.idem_key, r.est_p50_s, r.est_p90_s, r.est_source]), [
      ['big', 5000, 10000, 'hint'], ['k0', 300, 600, 'default'], ['none', 900, 1800, 'default'], ['sim0', 900, 1800, 'default']]);
  });

  test('F6 canary: trusting the hint stores 0.01 s', async () => {
    const { su, submit } = await open({ mutate: ['greatest(coalesce(r.est_p50_s, 0), d.v) AS p50', 'coalesce(r.est_p50_s, d.v) AS p50'] });
    await submit.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', 'normal', '{}'::jsonb, 'x', $2::jsonb)", [randomUUID(), JSON.stringify(jobsJson(1, { est: 0.01 }))]);
    assert.equal((await su.query('SELECT est_p50_s FROM lane.jobs')).rows[0].est_p50_s, 0.01);
  });

  // F7: remaining work counts what is left of a running job.
  async function runningResiduals(opts) {
    const { su } = await open(opts);
    const NOW = new Date('2026-03-01T12:00:00Z');
    const at = (sec) => new Date(NOW.getTime() - sec * 1000);
    await addGroup(su, { n: 1, account: 'acct', prio: 'normal', jobs: [{ cls: 'test', est: 600 }] });
    const rem = async () => {
      await su.query('SELECT lane.refresh_rem_ref($1)', [NOW]);
      return (await su.query("SELECT rem_ref FROM lane.sched_parents WHERE account = 'acct' AND prio_class = 'normal'")).rows[0].rem_ref.test;
    };
    const out = {};
    await su.query("UPDATE lane.jobs SET state = 'running', host = 'h1', started_at = $1", [at(590)]);
    out.nearlyDone = await rem();
    // factor 2, p50 20 / p90 40, 120 s elapsed: elapsed_ref 60 > p90, residual 0.5 x 60 = 30 ref-s
    await su.query("INSERT INTO lane.host_factors (host_id, class, bucket, factor) VALUES ('h1', 'test', 'short', 2)");
    await su.query('UPDATE lane.jobs SET est_p50_s = 20, est_p90_s = 40, started_at = $1, max_ms = 1000000', [at(120)]);
    out.overrun = await rem();
    await su.query('UPDATE lane.jobs SET max_ms = 130000');         // (130 - 120) / 2 = 5 ref-s left before the kill
    out.capped = await rem();
    return out;
  }
  test('F7: a running 600 s job at 590 reference seconds contributes 10; past p90 the overrun residual applies, capped by max_ms', async () => {
    assert.deepEqual(await runningResiduals(), { nearlyDone: { mid: 10 }, overrun: { short: 30 }, capped: { short: 5 } });
  });
  test('F7 canary: counting a running job in full contributes 600', async () => {
    const r = await runningResiduals({ mutate: ['ELSE greatest(0, j.est_p50_s - t.wall / t.factor) END AS rem', 'ELSE j.est_p50_s END AS rem'] });
    assert.deepEqual(r.nearlyDone, { mid: 600 });
  });

  // F8: deadline urgency is damped (5.8).
  async function alternatingForecasts(opts) {
    const { su } = await open(opts);
    const NOW = new Date('2026-03-01T12:00:00Z');
    await addGroup(su, { n: 1, account: 'acct', prio: 'normal', deadline: new Date(NOW.getTime() + 30 * 60000), jobs: [{}] });
    const series = [];
    const flips = [];
    for (let cycle = 0; cycle < 20; cycle++) {
      await su.query('SELECT lane.record_eta($1, $2, false, $3)', [gid(1), cycle % 2 === 0 ? 300 : 1800, NOW]);
      const { rows: [r] } = await su.query('SELECT lane.urgency(g, $1) AS u, e.boost_on FROM lane.groups g JOIN lane.group_eta e ON e.group_id = g.id', [NOW]);
      series.push(r.u);
      flips.push(r.boost_on);
    }
    const steps = series.slice(1).map((u, i) => Math.abs(u - series[i]));
    return { series, maxStep: Math.max(...steps), maxUrgency: Math.max(...series), flips: flips.slice(1).filter((on, i) => on !== flips[i]).length };
  }
  test('F8: alternating forecasts cannot toggle urgency between 0 and 150: at most 25 points a cycle, at most 7 flips in 20 cycles', async () => {
    const r = await alternatingForecasts();
    assert.ok(r.maxStep <= 25.0001, `slew: ${JSON.stringify(r.series)}`);
    assert.ok(r.maxUrgency > 0, 'the boost must switch on at least once');
    assert.ok(r.flips <= 7, `flips: ${r.flips}`);
  });
  test('F8 canary: without the slew limit urgency jumps by more than 25 points', async () => {
    const r = await alternatingForecasts({ mutate: ['least(25, greatest(-25, CASE WHEN v_on THEN v_raw ELSE 0 END - v_prev))', '(CASE WHEN v_on THEN v_raw ELSE 0 END - v_prev)'] });
    assert.ok(r.maxStep > 25, `got ${r.maxStep}`);
  });
  test('F8 canary: without the dwell the boost flips on every cycle', async () => {
    const r = await alternatingForecasts({ mutate: ['AND e.last_flip_cycle IS NOT NULL AND v_cycle - e.last_flip_cycle < 3', 'AND false'] });
    assert.ok(r.flips > 7, `got ${r.flips}`);
  });

  // F9 (parent creation vs the scheduling lock) was superseded by N4: parents are provisioned, see lane-claim-round2.test.js.
});
