import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, release, tid, waitBlocked } from './lane-dispatch-fixture.js';

/**
 * BRAIN-1116 S1: default-deny repo authorization. A runner in enforce mode claims only the repos its allowed_repos list names ('*' names nothing),
 * a ticket carries its repo name in header.repoName (none means 'adhoc'), open and audit ignore the list. The fixture world has a1 enforcing ['repoA'],
 * a2 and x open with '*'.
 */
const ARITIES = { 4: {}, 5: { has: [] }, 7: { capacity: 8 }, 9: { capacity: 8, freeWeight: 100, weightLimit: 1 } };
const A = (w) => w.as(logins.admin);
const setX = async (w, mode, list, maxClaims = 1) => call(await A(w), 'ld_set_principal', logins.x, 'agent', 'x', [], list, maxClaims, mode);
const principal = async (w, login) => (await (await w.su()).query('SELECT repo_mode, allowed_repos FROM lane_dispatch.principals WHERE login_role = $1', [login])).rows[0];

/** Claims once as x, and puts a claimed ticket back and withdraws it, so the next probe starts from an empty queue. */
async function probe(w, id, options, ticketOptions) {
  const [s1, x] = [await w.as(logins.s1), await w.as(logins.x)];
  await enqueue(s1, id, ticketOptions);
  const got = await claim(x, options);
  if (got.status !== 'none') await release(x, got);
  await call(s1, 'ld_withdraw', id);
  return got.status === 'none' ? null : got.id;
}

const reservation = (mode) => async (w) => {
  const [s1, x, su] = [await w.as(logins.s1), await w.as(logins.x), await w.su()];
  await setX(w, mode, ['repoA'], 2);
  await enqueue(s1, tid(1), { repo: 'repoA' });
  assert.equal((await claim(x)).id, tid(1), 'x holds one claim, so its next claim is checked against the head');
  await enqueue(s1, tid(2), { repo: 'repoB', cpu: 4 });
  await su.query("UPDATE lane_dispatch.tickets SET created_at = now() - interval '1 day' WHERE id = $1", [tid(2)]);
  await enqueue(s1, tid(3), { repo: 'repoA' });
  const got = await claim(x, { cpu: 1, capacity: 8 });
  if (mode === 'enforce') assert.equal(got.id, tid(3), 'a head the runner may not claim is not reserved: the ticket it may claim goes');
  else assert.deepEqual(got, { status: 'none', reserved_for: tid(2) }, 'a head the runner may claim, but cannot fit yet, is reserved');
};

const GUARDS = {
  'open and audit ignore allowed_repos, enforce claims only the repos it names, on the 4, 5, 7 and 9-argument claims and in tool discovery': async (w) => {
    const [s1, x] = [await w.as(logins.s1), await w.as(logins.x)];
    await enqueue(s1, tid(100), { repo: 'repoB', requires: ['ffmpeg'] });
    let n = 0;
    for (const [mode, takesOthers] of [['open', true], ['audit', true], ['enforce', false]]) {
      await setX(w, mode, ['repoA']);
      assert.deepEqual(await call(x, 'ld_needed_tools'), takesOthers ? ['ffmpeg'] : [], `${mode}: tool discovery sees the tickets the claim can take`);
      for (const [arity, options] of Object.entries(ARITIES)) {
        assert.equal(await probe(w, tid(++n), options, { repo: 'repoB' }), takesOthers ? tid(n) : null, `${mode}, ${arity}-argument claim, a repo its list does not name`);
        assert.equal(await probe(w, tid(++n), options, { repo: 'repoA' }), tid(n), `${mode}, ${arity}-argument claim, a repo its list names`);
      }
    }
  },

  'a ticket with no repo name is adhoc: an enforcing runner takes it only when its list names adhoc': async (w) => {
    await setX(w, 'enforce', ['repoA']);
    assert.equal(await probe(w, tid(1), {}, { repoName: null }), null, 'no name, list without adhoc');
    assert.equal(await probe(w, tid(2), {}, { repoName: '' }), null, 'an empty name is no name');
    await setX(w, 'enforce', ['repoA', 'adhoc']);
    assert.equal(await probe(w, tid(3), {}, { repoName: null }), tid(3));
    assert.equal(await probe(w, tid(4), {}, { repoName: '' }), tid(4));
    assert.equal(await probe(w, tid(5), {}, { repo: 'repoB' }), null, 'adhoc is not a wildcard');
  },

  'eligible: * names nothing under enforce, a missing list denies, open and audit ignore the list, tags still gate every mode': async (w) => {
    const su = await w.su();
    const ok = async (tags, repo, ptags, list, mode) => (await su.query('SELECT lane_dispatch.eligible($1::text[], $2, $3::text[], $4::text[], $5) AS v', [tags, repo, ptags, list, mode])).rows[0].v;
    assert.equal(await ok([], 'repoB', [], ['*'], 'enforce'), false);
    assert.equal(await ok([], 'repoB', [], [], 'enforce'), false);
    assert.equal(await ok([], 'repoB', [], null, 'enforce'), false);
    assert.equal(await ok([], 'repoB', [], ['repoB'], 'enforce'), true);
    for (const mode of ['open', 'audit']) {
      assert.equal(await ok([], 'repoB', [], [], mode), true, mode);
      assert.equal(await ok(['gpu'], 'repoB', [], [], mode), false, `${mode} still needs the tags`);
    }
    assert.equal(await ok(['gpu'], 'repoB', [], ['repoB'], 'enforce'), false);
    assert.equal(await su.query("SELECT lane_dispatch.ticket_repo('{}') AS v").then((r) => r.rows[0].v), 'adhoc');
    assert.equal(await su.query('SELECT lane_dispatch.ticket_repo($1::jsonb) AS v', ['{"repoName": "popovlabs/lane-broker"}']).then((r) => r.rows[0].v), 'popovlabs/lane-broker');
  },

  'scarcity counts the runners eligible for the ticket by its repo name, never its lease key': async (w) => {
    const [s1, x] = [await w.as(logins.s1), await w.as(logins.x)];
    await enqueue(s1, tid(1), { repo: 'repoB', repoName: 'repoA', priority: 8 });
    await enqueue(s1, tid(2), { repo: 'repoA', repoName: 'repoB', priority: 8 });
    assert.equal((await claim(x)).id, tid(2), 'repoB is named by two runners and repoA by three, so the repoB ticket goes first');
  },

  'a head the runner may not claim is not reserved for it (enforce)': async (w) => {
    await reservation('enforce')(w);
  },

  'a head the runner may claim but cannot fit yet is reserved (open)': async (w) => {
    await reservation('open')(w);
  },

  'ld_placeable scores a ticket by the name it is sent, and the 2-argument probe as adhoc': async (w) => {
    const [s1, admin] = [await w.as(logins.s1), await A(w)];
    assert.equal(await call(s1, 'ld_placeable', 'key', [], 'repoA'), 3);
    assert.equal(await call(s1, 'ld_placeable', 'key', [], 'repoB'), 2, 'a1 enforces a list without repoB');
    assert.equal(await call(s1, 'ld_placeable', 'repoA', []), 2, 'no name is adhoc');
    assert.equal(await call(s1, 'ld_placeable', 'repoA', [], ''), 2);
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast'], ['repoA', 'adhoc'], 2);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', []), 3);
  },

  'the guard trigger refuses * and an empty list for an enforcing principal, and passes what leaves mode and list alone': async (w) => {
    const [admin, su] = [await A(w), await w.su()];
    await assert.rejects(call(admin, 'ld_set_principal', 'lane_agent_n1', 'agent', 'n1', [], ['*'], 1), /cannot hold \*/, 'a new host through the 6-argument setter defaults to enforce');
    await assert.rejects(call(admin, 'ld_set_principal', 'lane_agent_n1', 'agent', 'n1', [], ['repoA', '*'], 1, 'enforce'), /cannot hold \*/);
    await call(admin, 'ld_set_principal', 'lane_agent_n2', 'agent', 'n2', [], [], 1);
    assert.deepEqual(await principal(w, 'lane_agent_n2'), { repo_mode: 'enforce', allowed_repos: [] }, 'a new host starts default-deny with nothing listed');
    for (const mode of ['open', 'audit']) await call(admin, 'ld_set_principal', `lane_agent_${mode}`, 'agent', mode, [], ['*'], 1, mode);

    await assert.rejects(call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast'], [], 2), /empty allowed_repos/, 'a list emptied under enforce');
    await assert.rejects(su.query("UPDATE lane_dispatch.principals SET allowed_repos = '{*}' WHERE login_role = $1", [logins.a1]), /cannot hold \*/, 'not even a superuser');
    await su.query("UPDATE lane_dispatch.principals SET max_claims = 3, tags = '{linux}' WHERE login_role = $1", [logins.a1]);
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux'], ['repoA'], 3);

    await assert.rejects(call(admin, 'ld_set_repo_mode', 'x', 'enforce'), /cannot hold \*/, 'x lists * and so would claim nothing');
    await setX(w, 'open', []);
    await assert.rejects(call(admin, 'ld_set_repo_mode', 'x', 'enforce'), /empty allowed_repos/);
    await setX(w, 'open', ['repoA']);
    await call(admin, 'ld_set_repo_mode', 'x', 'enforce');
    assert.deepEqual(await principal(w, logins.x), { repo_mode: 'enforce', allowed_repos: ['repoA'] });
  },

  'a submit principal can be created before any agent exists, and it starts default-deny': async (w) => {
    await (await w.su()).query('DELETE FROM lane_dispatch.principals');
    await call(await A(w), 'ld_set_principal', 'lane_submit_first', 'submit', null, [], [], 1);
    assert.deepEqual(await principal(w, 'lane_submit_first'), { repo_mode: 'enforce', allowed_repos: [] });
  },

  'the 6-argument setter keeps the mode of a host that exists: an open host re-added with * stays open, an enforcing host re-added with its list succeeds': async (w) => {
    const admin = await A(w);
    await call(admin, 'ld_set_principal', logins.x, 'agent', 'x', ['gpu'], ['*'], 4);
    assert.deepEqual(await principal(w, logins.x), { repo_mode: 'open', allowed_repos: ['*'] });
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast'], ['repoA'], 2);
    assert.deepEqual(await principal(w, logins.a1), { repo_mode: 'enforce', allowed_repos: ['repoA'] });
    await assert.rejects(call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast'], ['*'], 2), /cannot hold \*/, 'an enforcing host cannot be handed *');
    await call(admin, 'ld_set_principal', logins.x, 'agent', 'x', [], ['*'], 1, 'audit');
    assert.equal((await principal(w, logins.x)).repo_mode, 'audit', 'the 7-argument setter sets the mode it is given');
  },

  'a concurrent first insert of a host is not flipped to enforce by a 6-argument setter that cannot see it yet': async (w) => {
    const first = await w.cluster.client(w.db, logins.admin);
    const second = await w.cluster.client(w.db, logins.admin);
    await first.query('BEGIN');
    await call(first, 'ld_set_principal', 'lane_agent_race', 'agent', 'race', [], ['*'], 1, 'open');
    const { rows: [{ pid }] } = await second.query('SELECT pg_backend_pid() AS pid');
    const pending = call(second, 'ld_set_principal', 'lane_agent_race', 'agent', 'race', [], ['repoA'], 1);
    await waitBlocked(w, pid);
    await first.query('COMMIT');
    await pending;
    assert.deepEqual(await principal(w, 'lane_agent_race'), { repo_mode: 'open', allowed_repos: ['repoA'] });
  },

  'ld_set_repo_mode changes the mode of one agent by host, refuses an unknown host and a mode that does not exist': async (w) => {
    const admin = await A(w);
    await call(admin, 'ld_set_repo_mode', 'a1', 'audit');
    assert.deepEqual(await principal(w, logins.a1), { repo_mode: 'audit', allowed_repos: ['repoA'] });
    await assert.rejects(call(admin, 'ld_set_repo_mode', 'nowhere', 'open'), (err) => err.code === 'P0002');
    await assert.rejects(call(admin, 'ld_set_repo_mode', 'a1', 'strict'), (err) => err.code === '23514');
  },

  'pull_repo_audit groups the active and recent tickets by repo, name, submitter and state, with coverage and the holder check': async (w) => {
    const [s1, s2, su, reader] = [await w.as(logins.s1), await w.as(logins.s2), await w.su(), await w.as(logins.reader)];
    await enqueue(s1, tid(1));
    await su.query(`UPDATE lane_dispatch.tickets SET header = header || '{"brokerVersion": "0.119.0"}' WHERE id = $1`, [tid(1)]);
    await enqueue(s1, tid(2), { repoName: null });
    await enqueue(s2, tid(3), { repo: 'repoB', tags: ['gpu'] });
    for (const [n, repo] of [[4, 'repoA'], [5, 'repoB'], [6, 'repoA'], [7, 'repoA'], [8, 'repoA'], [9, 'repoA'], [10, 'repoA']]) await enqueue(s1, tid(n), { repo });
    const hold = (n, state, host) => su.query('UPDATE lane_dispatch.tickets SET state = $2, host = $3, claim_token = gen_random_uuid(), lease_until = now() + interval \'1 hour\' WHERE id = $1', [tid(n), state, host]);
    await hold(4, 'claimed', 'a1');
    await hold(5, 'running', 'x');
    await hold(6, 'running', 'gone');
    await su.query("UPDATE lane_dispatch.tickets SET state = 'lost', finished_at = now() WHERE id = $1", [tid(7)]);
    await su.query("UPDATE lane_dispatch.tickets SET state = 'finished', finished_at = now(), host = 'a1' WHERE id = $1", [tid(8)]);
    await su.query("UPDATE lane_dispatch.tickets SET state = 'finished', finished_at = now() - interval '1 hour', host = 'a1' WHERE id = $1", [tid(9)]);
    await su.query("UPDATE lane_dispatch.tickets SET state = 'withdrawn', finished_at = now() WHERE id = $1", [tid(10)]);
    const rows = async () => (await reader.query('SELECT * FROM lane.pull_repo_audit ORDER BY repo, state, host NULLS FIRST, submitter')).rows.map((r) => ({ ...r, n: Number(r.n) }));
    const row = (repo, named, submitter, version, state, host, configured, live, wouldDeny) => ({
      repo, named, submitter, submitter_version: version, state, host, covered_configured: configured, covered_live: live, holder_would_deny: wouldDeny, n: 1 });
    const [S1, S2] = [logins.s1, logins.s2];
    assert.deepEqual(await rows(), [
      row('adhoc', false, S1, null, 'queued', null, false, false, false),
      row('repoA', true, S1, null, 'claimed', 'a1', true, true, false),
      row('repoA', true, S1, null, 'finished', 'a1', true, true, false),
      row('repoA', true, S1, null, 'lost', null, true, true, false),
      row('repoA', true, S1, '0.119.0', 'queued', null, true, true, false),
      row('repoA', true, S1, null, 'running', 'gone', true, true, true),
      row('repoA', true, S1, null, 'withdrawn', null, true, true, false),
      row('repoB', true, S2, null, 'queued', null, false, false, false),
      row('repoB', true, S1, null, 'running', 'x', false, false, true),
    ], 'the old finished ticket is out; a holder with no principal and a holder whose list lacks the repo would be denied; no gpu runner covers the gpu ticket');

    await su.query("UPDATE lane_dispatch.runners SET last_seen = now() - interval '1 hour' WHERE host = 'a1'");
    const stale = (await rows()).filter((r) => r.repo === 'repoA' && r.state === 'queued');
    assert.deepEqual(stale.map((r) => [r.covered_configured, r.covered_live]), [[true, false]], 'a runner not seen for an hour covers the repo on paper only');

    await call(await A(w), 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast'], [], 2, 'open');
    const emptied = (await rows()).filter((r) => r.repo === 'repoA');
    assert.ok(emptied.length > 0 && emptied.every((r) => !r.covered_configured && !r.covered_live), 'an empty list covers nothing');
    assert.deepEqual(emptied.filter((r) => r.host === 'a1').map((r) => r.holder_would_deny), [true, true]);
  },

  'pull_repo_audit is readable by lane_reader (it calls ticket_repo as the reader), and lane_admin holds SELECT on it and nothing else': async (w) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const [reader, su] = [await w.as(logins.reader), await w.su()];
    assert.equal((await reader.query('SELECT * FROM lane.pull_repo_audit')).rows.length, 1);
    assert.equal((await (await w.as(logins.admin)).query('SELECT * FROM lane.pull_repo_audit')).rows.length, 1, 'an admin login reads the view too');
    const { rows: [priv] } = await su.query(
      `SELECT has_table_privilege('lane_admin', 'lane.pull_repo_audit', 'SELECT') AS admin_select,
              has_table_privilege('lane_admin', 'lane.pull_repo_audit', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS admin_write,
              has_table_privilege('lane_agent', 'lane.pull_repo_audit', 'SELECT') AS agent_select, has_table_privilege('lane_submit', 'lane.pull_repo_audit', 'SELECT') AS submit_select`);
    assert.deepEqual(priv, { admin_select: true, admin_write: false, agent_select: false, submit_select: false });
  },

  'pull_repo_audit reads only the active and recent tickets, however many terminal ones the table keeps': async (w) => {
    const [su, reader] = [await w.as('postgres'), await w.as(logins.reader)];
    const insert = (state, count, finishedAt) => su.query(
      `INSERT INTO lane_dispatch.tickets (id, owner, repo_key, lane, lane_key, binding, header, snapshot, cpu_cores, memory_bytes, incarnation, state, host, claim_token, lease_until, finished_at)
       SELECT gen_random_uuid(), 'lane_submit_s1', 'repoA', 'test', $1::text || g, '{}', '{"repoName": "repoA"}', '{}', 1, 1000, 0, $1::text, $2::text, CASE WHEN $1::text = 'claimed' THEN gen_random_uuid() END,
              CASE WHEN $1::text = 'claimed' THEN now() + interval '1 hour' END, $3::timestamptz FROM generate_series(1, $4::int) g`,
      [state, state === 'claimed' ? 'a1' : null, finishedAt, count]);
    await insert('finished', 20000, new Date(Date.now() - 3600e3));
    await insert('queued', 30, null);
    await insert('claimed', 10, null);
    await insert('finished', 5, new Date());
    await su.query('ANALYZE lane_dispatch.tickets');
    const { rows } = await reader.query('SELECT state, sum(n)::int AS n FROM lane.pull_repo_audit GROUP BY state ORDER BY state');
    assert.deepEqual(rows, [{ state: 'claimed', n: 10 }, { state: 'finished', n: 5 }, { state: 'queued', n: 30 }]);
    const { rows: plan } = await reader.query('EXPLAIN (COSTS OFF) SELECT * FROM lane.pull_repo_audit');
    const text = plan.map((r) => r['QUERY PLAN']).join('\n');
    assert.doesNotMatch(text, /Seq Scan on tickets/, text);
    for (const index of ['tickets_queued', 'tickets_held|tickets_lease', 'tickets_terminal_recent']) assert.match(text, new RegExp(`(${index})\\b`), `${index} is used\n${text}`);
  },
};

const GUARD_MODE = 'open and audit ignore allowed_repos, enforce claims only the repos it names, on the 4, 5, 7 and 9-argument claims and in tool discovery';
const TRIGGER = 'the guard trigger refuses * and an empty list for an enforcing principal, and passes what leaves mode and list alone';
const KEEP_MODE = 'the 6-argument setter keeps the mode of a host that exists: an open host re-added with * stays open, an enforcing host re-added with its list succeeds';
const RACE = 'a concurrent first insert of a host is not flipped to enforce by a 6-argument setter that cannot see it yet';
const AUDIT = 'pull_repo_audit groups the active and recent tickets by repo, name, submitter and state, with coverage and the holder check';
const ACCESS = 'pull_repo_audit is readable by lane_reader (it calls ticket_repo as the reader), and lane_admin holds SELECT on it and nothing else';
const HEAD_AVOID = "AND (NOT t.avoid_tags && v_me.tags OR v_now - t.created_at >= v_avoid_grace)\n     AND lane_dispatch.claim_need(t.cpu_cores, t.min_cpu_cores, v_now - t.created_at, v_floor_after) <= p_capacity_cpu";
const HEAD_ELIGIBLE = `AND lane_dispatch.eligible(t.requires_tags, lane_dispatch.ticket_repo(t.header), v_me.tags, v_me.allowed_repos, v_me.repo_mode)\n     ${HEAD_AVOID}`;

const CANARIES = [
  [GUARD_MODE, "p_repo_mode <> 'enforce' OR ", ''],
  [GUARD_MODE, 'v_me.tags, v_me.allowed_repos, v_me.repo_mode)\n     AND (NOT', 'v_me.tags, v_me.allowed_repos, NULL)\n     AND (NOT'],
  ['scarcity counts the runners eligible for the ticket by its repo name, never its lease key',
    'lane_dispatch.scarcity(t.requires_tags, lane_dispatch.ticket_repo(t.header)), t.created_at, t.id LIMIT 1 FOR UPDATE SKIP LOCKED', 'lane_dispatch.scarcity(t.requires_tags, t.repo_key), t.created_at, t.id LIMIT 1 FOR UPDATE SKIP LOCKED'],
  ['a head the runner may not claim is not reserved for it (enforce)', HEAD_ELIGIBLE, HEAD_AVOID],
  ['eligible: * names nothing under enforce, a missing list denies, open and audit ignore the list, tags still gate every mode', '(p_repo_name = ANY (p_allowed_repos)) IS TRUE', "(p_repo_name = ANY (p_allowed_repos) OR '*' = ANY (p_allowed_repos))"],
  [TRIGGER, 'CREATE OR REPLACE TRIGGER principals_repo_guard BEFORE INSERT OR UPDATE ON lane_dispatch.principals\n      FOR EACH ROW EXECUTE FUNCTION lane_dispatch.principals_repo_guard();', ''],
  [KEEP_MODE, "coalesce(p_repo_mode, v_mode, 'enforce')", "coalesce(p_repo_mode, 'enforce')"],
  [RACE, 'repo_mode = CASE WHEN p_repo_mode IS NULL THEN lane_dispatch.principals.repo_mode ELSE EXCLUDED.repo_mode END', 'repo_mode = EXCLUDED.repo_mode'],
  [AUDIT, '(g.repo = ANY (h.allowed_repos)) IS NOT TRUE', 'NOT (g.repo = ANY (h.allowed_repos))'],
  [ACCESS, 'GRANT EXECUTE ON FUNCTION lane_dispatch.ticket_repo(jsonb) TO lane_reader, lane_admin;', ''],
  [ACCESS, 'TO lane_reader, lane_admin;', 'TO lane_reader;'],
  [ACCESS, "'pull_runners', 'pull_repo_audit', 'ticket_status_v1'", "'pull_runners', 'ticket_status_v1'"],
];

describe('lane_dispatch repo authorization (BRAIN-1116) on a disposable Postgres', { skip: pgSkipOption, timeout: 900000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) test(name, async () => scenario(await World.create(cluster)));

  for (const [name, from, to] of CANARIES) {
    test(`canary: with "${from.slice(0, 60).replaceAll('\n', ' ')}" mutated, "${name}" fails`, async (t) => {
      const world = await World.create(cluster, { mutate: [from, to] });
      let failure;
      await GUARDS[name](world).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
