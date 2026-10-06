import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { PgCluster, PG_SKIP_REASON, roleNames, ago, gid, addGroup, setParent, jobsJson, claimAs } from './lane-db-harness.js';

/**
 * BRAIN-400: the rev11 counterexamples (Codex vets of rev8-rev10 plus amendment A1), each as a named test, and each guard
 * with a canary: the same scenario run against sql/lane with the guard broken (the harness's `mutate` must match, so a canary
 * cannot silently no-op), asserting the scenario then yields the WRONG answer. A guard whose canary does not fail is decoration.
 */
const claimedGroup = async (agent, extra) => (await claimAs(agent, extra))?.group_id ?? null;

describe('lane.claim_next rev11 counterexamples', { skip: PG_SKIP_REASON ?? false, timeout: 300000 }, () => {
  let cluster;
  before(() => { cluster = PgCluster.start(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());     // every test opens its own connections; the server allows only 60

  async function open(opts) {
    const db = await cluster.freshDb(opts);
    return { db, su: await cluster.client(db), agent: await cluster.client(db, roleNames.agent1) };
  }

  // 5.1: classes are strict bands. Score, WSJF, age and deadline urgency order work only inside a class.
  async function gateVsBatch(opts, batchAgeS) {
    const { su, agent } = await open(opts);
    await addGroup(su, { n: 1, account: 'gate', prio: 'gate', jobs: [{ cls: 'test', count: 3 }] });
    await setParent(su, 'gate', 'gate', { rem_ref: JSON.stringify({ test: { long: 6000 } }) });          // R = 100 slot-minutes
    await addGroup(su, { n: 2, account: 'batch', prio: 'batch', activatedAt: ago(batchAgeS), deadline: new Date(Date.now() + 5 * 60000), jobs: [{ cls: 'sim', count: 3 }] });
    await su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'test', 50), ('h1', 'sim', 0)");   // batch also has the lower vtime
    const { rows: [s] } = await su.query(
      `SELECT lane.parent_score(a, 'h1', now()) AS gate, lane.parent_score(b, 'h1', now()) + lane.urgency(g, now()) AS batch
         FROM lane.sched_parents a, lane.sched_parents b, lane.groups g WHERE a.account = 'gate' AND b.account = 'batch' AND g.id = $1`, [gid(2)]);
    assert.ok(s.batch > s.gate + 50, `the batch parent must outscore the gate or the fixture proves nothing: ${JSON.stringify(s)}`);
    return claimedGroup(agent);
  }
  const SCORE_BAND = ['band DESC,                                  -- 2.', 'score DESC, band DESC,                       -- 2.'];

  test('a fresh gate (R=100) beats a 3-minute-old batch boosted by a deadline, even at a higher vtime', async () => {
    assert.equal(await gateVsBatch(undefined, 3 * 60), gid(1));
  });
  test('a fresh gate beats a 19-minute-old batch (not yet overdue) even at a higher vtime', async () => {
    assert.equal(await gateVsBatch(undefined, 19 * 60), gid(1));
  });
  test('letting score cross the band would hand either batch parent the claim, so the fixtures catch it', async () => {
    assert.equal(await gateVsBatch({ mutate: SCORE_BAND }, 3 * 60), gid(2));
    assert.equal(await gateVsBatch({ mutate: SCORE_BAND }, 19 * 60), gid(2));
  });

  // 5.3: splitting buys nothing. Parent quantities only, so 100 one-minute groups score exactly like one 100-minute group.
  const T0 = new Date();
  async function splitScores(opts, split) {
    const { su, agent } = await open(opts);
    const parentAnchor = new Date(T0.getTime() - 10 * 60000);
    const parentLastClaim = new Date(T0.getTime() - 60000);
    // Split: one child was served a minute ago (the parent's turn), the other 99 have never been served and carry their own old ages.
    if (split) {
      for (let n = 1; n <= 100; n++) {
        await addGroup(su, { n, account: 'sub', activatedAt: parentAnchor, lastClaimAt: n === 1 ? parentLastClaim : null, parentAnchor, parentLastClaim, jobs: [{ est: 60 }] });
      }
    } else {
      await addGroup(su, { n: 1, account: 'sub', activatedAt: parentAnchor, lastClaimAt: parentLastClaim, parentAnchor, parentLastClaim, jobs: [{ est: 60, count: 100 }] });
    }
    // a competitor that is older by one minute of parent age (+5 points), still far from overdue
    await addGroup(su, { n: 200, account: 'rival', activatedAt: new Date(T0.getTime() - 2 * 60000), jobs: [{ count: 3 }] });
    const score = async () => {
      await su.query('SELECT lane.refresh_rem_ref()');
      const { rows } = await su.query("SELECT account, lane.parent_score(p, 'h1', $1) AS s FROM lane.sched_parents p", [T0]);
      return Object.fromEntries(rows.map((r) => [r.account, r.s]));
    };
    const before = await score();
    await su.query("UPDATE lane.jobs SET state = 'succeeded' WHERE id = (SELECT min(j.id) FROM lane.jobs j JOIN lane.groups g ON g.id = j.group_id WHERE g.account = 'sub')");
    const after = await score();
    return { before, after, winner: await claimedGroup(agent) };      // the claim last: it stamps the parent it serves
  }

  test('the 100x1-minute split scores identically to the unsplit group, before and after a child finishes, and a better parent beats both', async () => {
    const unsplit = await splitScores(undefined, false);
    const split = await splitScores(undefined, true);
    assert.equal(split.before.sub, unsplit.before.sub);
    assert.equal(split.after.sub, unsplit.after.sub);
    assert.notEqual(unsplit.before.sub, unsplit.after.sub, 'finishing a child must move the score or this proves nothing');
    for (const r of [unsplit.before, unsplit.after, split.before, split.after]) assert.ok(r.rival > r.sub, `the rival must outscore the parent: ${JSON.stringify(r)}`);
    assert.deepEqual([unsplit.winner, split.winner], [gid(200), gid(200)]);
  });

  test('aging a parent by its oldest sibling (rev9) would score the split differently from the unsplit group, so the fixture catches it', async () => {
    const mutate = ['extract(epoch FROM p_now - coalesce(p.last_claim_at, p.aging_anchor))',
      "extract(epoch FROM p_now - (SELECT min(coalesce(o.last_claim_at, o.aging_anchor)) FROM lane.groups o WHERE o.parent_id = p.id AND o.state = 'active'))"];
    const unsplit = await splitScores({ mutate }, false);
    const split = await splitScores({ mutate }, true);
    assert.notEqual(split.before.sub, unsplit.before.sub);
  });

  // 5.3: effective rank in SQL, with hysteresis.
  async function interactiveWinners(opts) {
    const { su, agent } = await open(opts);
    await su.query('UPDATE lane.config SET interactive_max_cores = 4');
    await addGroup(su, { n: 1, account: 'human', prio: 'interactive', jobs: [{ cls: 'test', count: 3 }] });
    await addGroup(su, { n: 2, account: 'robot', prio: 'gate', jobs: [{ cls: 'sim', count: 3 }] });
    await su.query("INSERT INTO lane.class_vtime (host_id, class, vtime) VALUES ('h1', 'test', 50), ('h1', 'sim', 0)");
    const winners = [];
    for (const running of [5, 3.5, 3.0]) {            // over the limit, between 0.8x and 1x (still demoted), below 0.8x (restored)
      await setParent(su, 'human', 'interactive', { running_cpu: running });
      winners.push(await claimedGroup(agent));
    }
    return winners;
  }

  test('interactive over its cap is ranked gate (and loses to the gate at the lower vtime); it stays demoted above 0.8x and is restored below it', async () => {
    assert.deepEqual(await interactiveWinners(), [gid(2), gid(2), gid(1)]);
  });
  test('without the demotion an over-limit interactive group keeps rank 4, so the fixture catches it', async () => {
    assert.deepEqual(await interactiveWinners({ mutate: ['AND p.demoted THEN', 'AND false THEN'] }), [gid(1), gid(1), gid(1)]);
  });
  test('without the hysteresis band the group is restored the moment it dips under the limit, so the fixture catches it', async () => {
    assert.deepEqual(await interactiveWinners({ mutate: ['OR (q.demoted AND u.cores >= 0.8 * lim.v)', ''] }), [gid(2), gid(1), gid(1)]);
  });

  // 5.2: floors are priority-aware and demand-armed. Host: 8 cores, a 75% sim floor and a 15% test floor.
  async function floorWinner(opts, { testPrio, simPrio, testOverdue = false, testCpu = 3 }) {
    const { su, agent } = await open(opts);
    await su.query("UPDATE lane.host_class_policy SET floor_cpu_pct = CASE class WHEN 'sim' THEN 75 ELSE 15 END");
    await addGroup(su, { n: 1, account: 'tests', prio: testPrio, activatedAt: ago(testOverdue ? 25 * 60 : 60), jobs: [{ cls: 'test', cpu: testCpu }] });
    await addGroup(su, { n: 2, account: 'sims', prio: simPrio, jobs: [{ cls: 'sim', cpu: 1 }] });
    return claimedGroup(agent);
  }
  const RANK_BLIND = ['(c.class <> p_class AND (p_rho ->> c.class)::int >= p_rank)', '(c.class <> p_class)'];

  test('8-core WSL host, 75% sim floor: a 3-core gate test claims over a 1-core batch sim (a floor never binds a higher rank)', async () => {
    assert.equal(await floorWinner(undefined, { testPrio: 'gate', simPrio: 'batch' }), gid(1));
  });
  test('the same host with rank-blind floors would hold the gate test to the 25% remainder, so the fixture catches it', async () => {
    assert.equal(await floorWinner({ mutate: RANK_BLIND }, { testPrio: 'gate', simPrio: 'batch' }), gid(2));
  });
  test('the floor does bind an equal-rank test: a normal 3-core test yields to normal sims holding 75%', async () => {
    assert.equal(await floorWinner(undefined, { testPrio: 'normal', simPrio: 'normal' }), gid(2));
  });
  test('an overdue test is never excluded by a floor', async () => {
    assert.equal(await floorWinner(undefined, { testPrio: 'normal', simPrio: 'normal', testOverdue: true }), gid(1));
  });
  test('without the overdue exemption the floor would exclude it, so the fixture catches it', async () => {
    const mutate = ['THEN p_free ELSE p_room_after_floors END', 'THEN p_room_after_floors ELSE p_room_after_floors END'];
    assert.equal(await floorWinner({ mutate }, { testPrio: 'normal', simPrio: 'normal', testOverdue: true }), gid(2));
  });

  // Floor lemma (5.2): for r_H > r_L, every reservation applying to H applies to L, so room(H) >= room(L) before caps.
  const rng = (seed) => () => {
    seed = (seed + 0x6D2B79F5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  async function lemmaViolations(opts, trials = 300) {
    const { su } = await open(opts);
    const classes = ['a', 'b', 'c'];
    for (const c of classes) await su.query("INSERT INTO lane.host_class_policy (host_id, class, weight) VALUES ('h1', $1, 1)", [c]);
    const rand = rng(11);
    const pick = (xs) => xs[Math.floor(rand() * xs.length)];
    let violations = 0;
    let reserving = 0;
    for (let i = 0; i < trials; i++) {
      await su.query('UPDATE lane.host_class_policy p SET floor_cpu_pct = v.f FROM unnest($1::text[], $2::real[]) AS v(c, f) WHERE p.host_id = $3 AND p.class = v.c',
        [classes, classes.map(() => pick([0, 15, 25, 50, 75])), 'h1']);
      const rho = {};
      for (const c of classes) if (rand() < 0.7) rho[c] = Math.floor(rand() * 5);
      const cH = pick(classes); const cL = pick(classes);
      const rH = 1 + Math.floor(rand() * 4); const rL = Math.floor(rand() * rH);
      rho[cH] = Math.max(rho[cH] ?? -1, rH);              // a candidate's own class always has waiting work at least its rank
      rho[cL] = Math.max(rho[cL] ?? -1, rL);
      const used = Object.fromEntries(classes.map((c) => [c, pick([0, 1, 2, 4])]));
      const { rows: [r] } = await su.query(
        `SELECT lane.reserved_floor('h1', 8, $1::jsonb, $2::jsonb, $3, $4::int, '{}') AS h, lane.reserved_floor('h1', 8, $1::jsonb, $2::jsonb, $5, $6::int, '{}') AS l`,
        [JSON.stringify(used), JSON.stringify(rho), cH, rH, cL, rL]);
      if (r.h > 0) reserving++;
      if (r.h > r.l + 1e-4) violations++;
    }
    return { violations, reserving };
  }
  test('floor lemma, property test over 300 random states: a higher rank is never reserved against more than a lower one', async () => {
    const r = await lemmaViolations();
    assert.equal(r.violations, 0);
    assert.ok(r.reserving >= 100, `the generator must exercise reservations, got ${r.reserving}`);
  });
  test('a rank comparison pointing the wrong way breaks the lemma, so the property test catches it', async () => {
    const r = await lemmaViolations({ mutate: ['(c.class <> p_class AND (p_rho ->> c.class)::int >= p_rank)', '(c.class <> p_class AND (p_rho ->> c.class)::int <= p_rank)'] });
    assert.ok(r.violations > 0, `expected violations, got ${JSON.stringify(r)}`);
  });

  // 5.5: the longest FITTING job per (group, class), chosen after the CPU/memory/floor/cap filter.
  async function hiddenFitting(opts, overdue) {
    const { su, agent } = await open(opts);
    await addGroup(su, { n: 1, account: 'gate', prio: 'gate', activatedAt: ago(overdue ? 25 * 60 : 60),
      jobs: [{ cls: 'test', est: 500, cpu: 9, cpuMin: 9 }, { cls: 'test', est: 10, cpu: 1 }] });
    await addGroup(su, { n: 2, account: 'sims', prio: 'batch', jobs: [{ cls: 'sim', cpu: 1 }] });
    const got = await claimAs(agent);
    return got && { group: got.group_id, grant: got.grant_cpu };
  }
  const LIMIT_BEFORE_FIT = ['AND j.cpu_min <= least(lane.job_floor_room(c.p_overdue, j.exclusive, p_free, c.floor_room_held), c.cap_room)', ''];
  for (const overdue of [false, true]) {
    test(`a gate's fitting 1-core job is not hidden behind its 9-core head on an 8-core host (${overdue ? 'overdue parent' : 'fresh parent'})`, async () => {
      assert.deepEqual(await hiddenFitting(undefined, overdue), { group: gid(1), grant: 1 });
    });
    test(`taking the head before the fit test hides it (${overdue ? 'overdue' : 'fresh'}), so the fixture catches it`, async () => {
      assert.equal(await hiddenFitting({ mutate: LIMIT_BEFORE_FIT }, overdue), null);
    });
  }

  // 5.5: grant = least(cpu_req, floor_room, class_cap_room), and both rooms are re-checked under the locks.
  async function capGrant(opts, used) {
    const { su, agent } = await open(opts);
    await su.query("UPDATE lane.host_class_policy SET cap_cpu_pct = 50 WHERE class = 'test'");     // budget 8: cap 4 cores
    await addGroup(su, { n: 1, jobs: [{ cls: 'test', cpu: 4, cpuMin: 1 }] });
    const got = await claimAs(agent, { free: 5, used: { test: used } });                          // floor room 5
    return got && got.grant_cpu;
  }
  test('class-cap grant overrun is prevented: budget 8, cap 4, usage 3, floor room 5, cpu_min 1 / cpu_req 4 is granted 1', async () => {
    assert.equal(await capGrant(undefined, 3), 1);
  });
  test('a job whose cpu_min exceeds the cap room is not claimed at all', async () => {
    assert.equal(await capGrant(undefined, 3.5), null);
  });
  test('granting without the cap room would give 4, so the fixture catches it', async () => {
    assert.equal(await capGrant({ mutate: ['least(v_job.cpu_req, v_pick.floor_room, v_pick.cap_room)', 'least(v_job.cpu_req, v_pick.floor_room)'] }, 3), 4);
  });
  test('an unbounded cap room would claim the job at 3.5 used, so the fixture catches it', async () => {
    assert.notEqual(await capGrant({ mutate: ['pw.cap_cpu_pct / 100 * v_host.budget', 'pw.cap_cpu_pct * 1e9'] }, 3.5), null);
  });

  async function revalidateResults(opts) {
    const { su } = await open(opts);
    await addGroup(su, { n: 1, jobs: [{ cpu: 4, cpuMin: 1 }] });
    const ask = async (floor, cap) => (await su.query(
      `SELECT lane.revalidate_claim(j, g.id, h, $1::real, $2::real, 1000000000000, '{}', now()) AS ok
         FROM lane.jobs j, lane.groups g, lane.hosts h WHERE h.host_id = 'h1'`, [floor, cap])).rows[0].ok;
    return [await ask(8, 8), await ask(0.5, 8), await ask(8, 0.5)];
  }
  test('revalidation checks both the floor room and the class-cap room under the locks', async () => {
    assert.deepEqual(await revalidateResults(), [true, false, false]);
  });
  test('revalidation without the cap room check passes a cap-violating job, so the fixture catches it', async () => {
    assert.deepEqual(await revalidateResults({ mutate: ['AND j.cpu_min <= p_cap_room', ''] }), [true, false, true]);
  });

  // A1 (spec amendment): an exclusive job is never promoted by the overdue rule, so an overdue calibration cannot
  // park a draining host fence in front of interactive and gate work.
  async function calibration(opts, { interactive = false, siblingSims = false, lease = false }) {
    const { su, agent } = await open(opts);
    await su.query("INSERT INTO lane.host_class_policy (host_id, class, weight) VALUES ('h1', 'calibration', 1)");
    await addGroup(su, { n: 1, account: 'cal', prio: 'scavenger', activatedAt: ago(25 * 60), jobs: [{ cls: 'calibration', exclusive: true }] });
    if (siblingSims) await addGroup(su, { n: 3, account: 'cal', prio: 'scavenger', activatedAt: ago(25 * 60), jobs: [{ cls: 'sim' }] });
    if (interactive) await addGroup(su, { n: 2, account: 'human', prio: 'interactive', activatedAt: ago(1), jobs: [{ cls: 'test' }] });
    if (lease) {
      await addGroup(su, { n: 4, account: 'busy', prio: 'gate', state: 'held', jobs: [{ cls: 'test' }] });
      await su.query("UPDATE lane.jobs SET state = 'running', host = 'h1' WHERE group_id = $1", [gid(4)]);
    }
    const got = await claimedGroup(agent);
    const { rows } = await su.query('SELECT state, owner_job_id FROM lane.host_reservations');
    return { got, reservations: rows.map((r) => r.state) };
  }
  test('A1: an overdue exclusive calibration does not outrank a fresh interactive job and creates no host reservation', async () => {
    assert.deepEqual(await calibration(undefined, { interactive: true }), { got: gid(2), reservations: [] });
  });
  test('A1: promoting the exclusive job by the overdue rule would put it ahead of the interactive job, so the fixture catches it', async () => {
    const r = await calibration({ mutate: ['(c.p_overdue AND NOT j.exclusive) AS overdue', 'c.p_overdue AS overdue'] }, { interactive: true });
    assert.deepEqual(r, { got: gid(1), reservations: ['active'] });
  });
  test('A1: with nothing else eligible the calibration claims and takes the active host reservation', async () => {
    assert.deepEqual(await calibration(undefined, {}), { got: gid(1), reservations: ['active'] });
  });
  test('A1: with a lease running on the host the calibration only starts a draining reservation and claims nothing', async () => {
    assert.deepEqual(await calibration(undefined, { lease: true }), { got: null, reservations: ['draining'] });
  });
  test('A1: the parent\'s other, non-exclusive jobs keep overdue service', async () => {
    assert.deepEqual(await calibration(undefined, { interactive: true, siblingSims: true }), { got: gid(3), reservations: [] });
  });

  // 4.2 / 5.8: capabilities and the deadline-state precedence.
  async function submitAs(su, cls) {
    const c = await cluster.client(su.database, roleNames.submit);
    return c.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', $2, '{}'::jsonb, 'x', $3::jsonb) AS id", [randomUUID(), cls, JSON.stringify(jobsJson(1))]);
  }
  async function interactiveSubmit(opts, grants) {
    const { su } = await open(opts);
    await su.query('UPDATE lane.principals SET max_class = $1, can_interactive = $2 WHERE kind = $3', [grants.maxClass, grants.can, 'submit']);
    return submitAs(su, 'interactive').then(() => 'accepted', (e) => e.code);
  }
  test('no capability means no interactive class: it needs both can_interactive and a max_class that reaches it', async () => {
    assert.equal(await interactiveSubmit(undefined, { maxClass: 'gate', can: false }), '42501');
    assert.equal(await interactiveSubmit(undefined, { maxClass: 'gate', can: true }), '42501');
    assert.equal(await interactiveSubmit(undefined, { maxClass: 'interactive', can: false }), '42501');
    assert.equal(await interactiveSubmit(undefined, { maxClass: 'interactive', can: true }), 'accepted');
  });
  test('dropping the class bound would let a gate-capped login submit interactive, so the fixture catches it', async () => {
    assert.equal(await interactiveSubmit({ mutate: ['AND pc.rank <= cap.rank', ''] },
      { maxClass: 'gate', can: true }), 'accepted');
  });

  test('reclass needs the target class capability, re-parents the group and starts the new parent\'s clock', async () => {
    const { su } = await open();
    const { rows: [{ id }] } = await submitAs(su, 'normal');
    const c = await cluster.client(su.database, roleNames.submit);
    await c.query('SELECT lane.activate_group($1)', [id]);
    await assert.rejects(c.query("SELECT lane.reclass($1, 'interactive')", [id]), /not allowed/);
    assert.equal((await c.query("SELECT lane.reclass($1, 'gate')", [id])).rows[0].reclass, true);
    const { rows: [g] } = await su.query('SELECT g.prio_class, p.prio_class AS parent_class, p.aging_anchor IS NOT NULL AS anchored FROM lane.groups g JOIN lane.sched_parents p ON p.id = g.parent_id');
    assert.deepEqual(g, { prio_class: 'gate', parent_class: 'gate', anchored: true });
  });

  test('deadline-state precedence: invalid and unauthorised deadlines carry no urgency and no EDF; a missed one is fixed at 75 with no EDF', async () => {
    const { su } = await open();
    const when = (min) => new Date(Date.now() + min * 60000);
    await addGroup(su, { n: 1, account: 'a1', deadline: when(5), deadlineValid: false });     // invalid (e.g. infeasible at submit, or no can_deadline)
    await addGroup(su, { n: 2, account: 'a2', deadline: when(-5), deadlineValid: true });     // missed
    await addGroup(su, { n: 3, account: 'a3', deadline: when(5), deadlineValid: true });      // valid, slack 5 min against S0 15 min
    const { rows } = await su.query('SELECT g.id, lane.urgency(g, now()) AS urg, lane.deadline_eff(g, now()) IS NOT NULL AS edf FROM lane.groups g ORDER BY g.id');
    assert.deepEqual(rows.map((r) => [Number(r.urg.toFixed(1)), r.edf]), [[0, false], [75, false], [100, true]]);
    await su.query('UPDATE lane.config SET deadlines_enabled = false');
    const { rows: off } = await su.query('SELECT lane.urgency(g, now()) AS urg, lane.deadline_eff(g, now()) IS NOT NULL AS edf FROM lane.groups g');
    assert.deepEqual(off.map((r) => [r.urg, r.edf]), [[0, false], [0, false], [0, false]]);
  });
  test('a submitter without can_deadline gets deadline_valid false, one with it gets true', async () => {
    const { su } = await open();
    const c = await cluster.client(su.database, roleNames.submit);
    const deadline = new Date(Date.now() + 3600000);
    const submit = (can) => su.query('UPDATE lane.principals SET can_deadline = $1 WHERE kind = $2', [can, 'submit'])
      .then(() => c.query("SELECT lane.submit_group($1::uuid, 'single', 'acct', 'normal', '{}'::jsonb, 'x', $2::jsonb, $3) AS id", [randomUUID(), JSON.stringify(jobsJson(1)), deadline]))
      .then(({ rows: [{ id }] }) => su.query('SELECT deadline_valid FROM lane.groups WHERE id = $1', [id]))
      .then(({ rows: [r] }) => r.deadline_valid);
    assert.equal(await submit(false), false);
    assert.equal(await submit(true), true);
  });

  test('remaining work is kept per (work class, bucket) and each bucket is converted with its own host factor', async () => {
    const { su } = await open();
    await addGroup(su, { n: 1, jobs: [{ cls: 'test', est: 60 }, { cls: 'test', est: 300 }, { cls: 'sim', est: 700, count: 2 }] });
    await su.query('SELECT lane.refresh_rem_ref()');
    const { rows: [p] } = await su.query("SELECT rem_ref FROM lane.sched_parents WHERE account = 'acct' AND prio_class = 'normal'");
    assert.deepEqual(p.rem_ref, { test: { short: 60, mid: 300 }, sim: { long: 1400 } });
    await su.query("INSERT INTO lane.host_factors (host_id, class, bucket, factor) VALUES ('h1', 'sim', 'long', 2)");
    const { rows: [r] } = await su.query("SELECT lane.rem_min(p, 'h1') AS m FROM lane.sched_parents p WHERE p.account = 'acct' AND p.prio_class = 'normal'");
    assert.equal(r.m, (60 + 300 + 1400 * 2) / 60);
  });
});
