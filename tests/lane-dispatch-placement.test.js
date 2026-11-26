import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, finish, setNow, tid, waitBlocked } from './lane-dispatch-fixture.js';

/** PKG-439: placement constraints (capability class, avoid_hosts, target_host) on the queue, enforced by ld_claim through one predicate. */

const T0 = Date.parse('2030-01-01T00:00:00Z');
const at = (ms) => new Date(T0 + ms).toISOString();
const MINUTE = 60_000;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const KEY = 'train:repoA:9:1';

/** a1 and a2 are linux and darwin runners, x has no platform recorded. */
async function placedWorld(cluster, options) {
  const w = await World.create(cluster, options);
  const admin = await w.as(logins.admin);
  await call(admin, 'ld_set_platform', 'a1', 'linux');
  await call(admin, 'ld_set_platform', 'a2', 'darwin');
  return w;
}

const none = { status: 'none' };
const submit = (w) => w.as(logins.s1);
const claimAs = async (w, login, options) => claim(await w.as(login), options);
const code = (sqlstate) => (err) => err.code === sqlstate;

const GUARDS = {
  'a ticket that avoids a host is never claimed by it and is claimed by another eligible host': async (w) => {
    await enqueue(await submit(w), tid(1), { avoidHosts: ['a1'] });
    assert.deepEqual(await claimAs(w, logins.a1), none, 'the avoided host gets nothing, though its tags and repos fit');
    assert.equal((await claimAs(w, logins.a2)).id, tid(1));
    assert.deepEqual((await w.ticket(tid(1))).avoid_hosts, ['a1']);
  },

  'avoid_hosts is hard: it holds past the soft avoid_tags grace period': async (w) => {
    await setNow(w, at(0));
    await enqueue(await submit(w), tid(1), { avoidHosts: ['a1'], avoidTags: ['linux'] });
    await setNow(w, at(3 * 60 * MINUTE));
    assert.deepEqual(await claimAs(w, logins.a1), none);
    assert.equal((await claimAs(w, logins.a2)).id, tid(1));
  },

  'a capability class of one platform is claimed only by a principal of that platform, never by another or one with none recorded': async (w) => {
    await enqueue(await submit(w), tid(1), { capabilityClass: 'darwin' });
    assert.deepEqual(await claimAs(w, logins.a1), none, 'a linux principal');
    assert.deepEqual(await claimAs(w, logins.x), none, 'a principal with no platform');
    const got = await claimAs(w, logins.a2);
    assert.equal(got.id, tid(1));
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.capability_class, row.class_platform], ['darwin', 'darwin']);
  },

  'the tags of a class are required tags too: linux+fast needs a linux principal that also has the tag, and the class is stored canonical': async (w) => {
    const admin = await w.as(logins.admin);
    await call(admin, 'ld_set_platform', 'a2', 'linux');
    await enqueue(await submit(w), tid(1), { capabilityClass: 'Linux+fast+fast' });
    const row = await w.ticket(tid(1));
    assert.equal(row.capability_class, 'linux+fast');
    assert.deepEqual(row.requires_tags, ['fast']);
    assert.deepEqual(await claimAs(w, logins.a2), none, 'linux, but without the fast tag');
    assert.equal((await claimAs(w, logins.a1)).id, tid(1));
  },

  'a target host is the only host that claims the ticket': async (w) => {
    await enqueue(await submit(w), tid(1), { targetHost: 'a2' });
    assert.deepEqual(await claimAs(w, logins.a1), none);
    assert.deepEqual(await claimAs(w, logins.x), none);
    assert.equal((await claimAs(w, logins.a2)).id, tid(1));
    assert.equal((await w.ticket(tid(1))).target_host, 'a2');
  },

  'an unconstrained ticket is claimed by every eligible host as before, and an old call shape stores no placement': async (w) => {
    await enqueue(await submit(w), tid(1));
    await enqueue(await submit(w), tid(2));
    await enqueue(await submit(w), tid(3));
    for (const login of [logins.a1, logins.a2, logins.x]) assert.equal((await claimAs(w, login)).status, 'claimed', login);
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.capability_class, row.class_platform, row.avoid_hosts, row.target_host], [null, null, [], null]);
  },

  'a host does not reserve capacity for an aged head it can never claim': async (w) => {
    await setNow(w, at(0));
    await enqueue(await submit(w), tid(9), { cpu: 2 });
    const running = await claimAs(w, logins.a1, { cpu: 12, capacity: 12 });
    await enqueue(await submit(w), tid(1), { cpu: 8, avoidHosts: ['a1'] });
    await setNow(w, at(61 * MINUTE));
    await enqueue(await submit(w), tid(2), { cpu: 4 });
    const got = await claimAs(w, logins.a1, { cpu: 6, capacity: 12 });
    assert.equal(got.id, tid(2), 'the 4-core ticket is claimed, not held back for a head this host may not run');
    await finish(await w.as(logins.a1), running);
  },

  'a host still reserves for an aged head it may claim (the control for the head-reserve guard)': async (w) => {
    await setNow(w, at(0));
    await enqueue(await submit(w), tid(9), { cpu: 2 });
    await claimAs(w, logins.a1, { cpu: 12, capacity: 12 });
    await enqueue(await submit(w), tid(1), { cpu: 8 });
    await setNow(w, at(61 * MINUTE));
    await enqueue(await submit(w), tid(2), { cpu: 4 });
    assert.deepEqual(await claimAs(w, logins.a1, { cpu: 6, capacity: 12 }), { status: 'none', reserved_for: tid(1) });
  },

  'ld_needed_tools lists only the tools of tickets this agent may claim': async (w) => {
    await enqueue(await submit(w), tid(1), { requires: ['gh'], avoidHosts: ['a2'] });
    await enqueue(await submit(w), tid(2), { requires: ['ffmpeg'], capabilityClass: 'darwin' });
    await enqueue(await submit(w), tid(3), { requires: ['jq'], targetHost: 'x' });
    assert.deepEqual(await call(await w.as(logins.a1), 'ld_needed_tools'), ['gh']);
    assert.deepEqual(await call(await w.as(logins.a2), 'ld_needed_tools'), ['ffmpeg']);
    assert.deepEqual(await call(await w.as(logins.x), 'ld_needed_tools'), ['gh', 'jq']);
  },

  'scarcity counts the principals a placement leaves, and an unconstrained ticket orders exactly as it did': async (w) => {
    const su = await w.su();
    const scarcity = async (...args) => (await su.query(`SELECT lane_dispatch.scarcity(${args.map((_, i) => `$${i + 1}`).join(', ')}) AS n`, args)).rows[0].n;
    assert.equal(await scarcity([], 'repoA'), 3);
    assert.equal(await scarcity([], 'repoA', null, [], null), 3, 'the defaults are no constraint');
    assert.equal(await scarcity(['linux'], 'repoA'), 2);
    assert.equal(await scarcity([], 'repoA', 'darwin', [], null), 1);
    assert.equal(await scarcity([], 'repoA', null, ['a1', 'a2'], null), 1);
    assert.equal(await scarcity([], 'repoA', null, [], 'x'), 1);
    assert.equal(await scarcity([], 'repoA', null, ['a2'], 'a2'), 0);
  },

  'a malformed or unknown placement is refused before anything is stored': async (w) => {
    const s1 = await submit(w);
    await assert.rejects(enqueue(s1, tid(1), { targetHost: 'nobody' }), (err) => err.code === 'LK422' && /unknown host nobody/.test(err.message));
    await assert.rejects(enqueue(s1, tid(1), { avoidHosts: ['nobody'] }), (err) => err.code === 'LK422' && /unknown host nobody/.test(err.message));
    await assert.rejects(enqueue(s1, tid(1), { avoidHosts: ['a1'], targetHost: 'a1' }), (err) => err.code === 'LK422' && /also in avoid_hosts/.test(err.message));
    await assert.rejects(enqueue(s1, tid(1), { capabilityClass: 'plan9' }), code('22023'));
    await assert.rejects(enqueue(s1, tid(1), { capabilityClass: 'linux+Bad Tag' }), code('22023'));
    await assert.rejects(enqueue(s1, tid(1), { avoidHosts: ['not a host'] }), code('22023'));
    assert.equal(await w.ticket(tid(1)), undefined);
  },

  'host ids are lower-cased and the lists stored sorted and de-duplicated': async (w) => {
    await enqueue(await submit(w), tid(1), { avoidHosts: ['X', 'a2', 'x'], targetHost: 'A1' });
    const row = await w.ticket(tid(1));
    assert.deepEqual([row.avoid_hosts, row.target_host], [['a2', 'x'], 'a1']);
  },

  'ld_placeable counts what a placement leaves, is zero when every principal is avoided, and refuses an unknown host': async (w) => {
    const s1 = await submit(w);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', []), 3, 'the two-argument call is unchanged');
    assert.equal(await call(s1, 'ld_placeable', 'repoA', [], null, [], null), 3);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', [], null, ['a1'], null), 2);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', [], 'darwin', [], null), 1);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', [], null, ['a1', 'a2', 'x'], null), 0);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', ['nothing'], 'darwin', [], null), 0);
    await assert.rejects(call(s1, 'ld_placeable', 'repoA', [], null, ['nobody'], null), code('LK422'));
    await assert.rejects(call(s1, 'ld_placeable', 'repoA', [], null, [], 'nobody'), code('LK422'));
  },

  'a ticket stays queued when its only host goes away or changes platform, and its view says why': async (w) => {
    const s1 = await submit(w);
    const su = await w.su();
    const view = async (id) => call(s1, 'ld_get', id);
    await enqueue(s1, tid(1), { targetHost: 'a2' });
    await enqueue(s1, tid(2), { capabilityClass: 'darwin' });
    await enqueue(s1, tid(4));
    assert.deepEqual([(await view(tid(1))).unplaceable, (await view(tid(2))).unplaceable, (await view(tid(4))).unplaceable], [null, null, null]);
    await su.query("DELETE FROM lane_dispatch.principals WHERE host = 'a2'");
    assert.equal((await view(tid(1))).state, 'queued');
    assert.equal((await view(tid(1))).unplaceable, 'target');
    assert.equal((await view(tid(2))).unplaceable, 'class');
    assert.equal((await view(tid(4))).unplaceable, null);
    assert.deepEqual(await claimAs(w, logins.a1, { cpu: 1 }).then((c) => c.id), tid(4), 'the placeable ticket is unaffected');
    await su.query("UPDATE lane_dispatch.principals SET max_claims = 0");
    assert.equal((await view(tid(1))).unplaceable, 'no-runner');
  },

  'the unplaceable reason is each failing test in turn: tags, class, avoid, target': async (w) => {
    const s1 = await submit(w);
    await enqueue(s1, tid(1), { tags: ['gpu'] });
    await enqueue(s1, tid(2), { capabilityClass: 'win32' });
    await enqueue(s1, tid(3), { repo: 'repoA', avoidHosts: ['a1', 'a2', 'x'] });
    await enqueue(s1, tid(4), { targetHost: 'a1', repo: 'repoZ' });
    const reasons = Object.fromEntries(await Promise.all([1, 2, 3, 4].map(async (n) => [n, (await call(s1, 'ld_get', tid(n))).unplaceable])));
    assert.deepEqual(reasons, { 1: 'tags', 2: 'class', 3: 'avoid', 4: 'target' });
  },

  'a repeat of a committed targeted enqueue is answered after the target runner is removed, keyed or not, and a new one is refused': async (w) => {
    const s1 = await submit(w);
    const su = await w.su();
    await enqueue(s1, tid(1), { targetHost: 'a2' });
    await enqueue(s1, tid(2), { targetHost: 'a2', dispatchKey: KEY });
    await su.query("DELETE FROM lane_dispatch.principals WHERE host = 'a2'");
    assert.equal((await enqueue(s1, tid(1), { targetHost: 'a2' })).id, tid(1), 'the same id replays');
    assert.equal((await enqueue(s1, tid(2), { targetHost: 'a2', dispatchKey: KEY })).id, tid(2), 'the same keyed id replays');
    assert.equal((await enqueue(s1, tid(7), { targetHost: 'a2', dispatchKey: KEY })).id, tid(2), 'another id under the key is answered with the holder');
    await assert.rejects(enqueue(s1, tid(3), { targetHost: 'a2' }), code('LK422'));
  },

  'a placed request adopts only a ticket placed as asked (LK423 on every holder path), while an unconstrained request adopts any holder': async (w) => {
    const s1 = await submit(w);
    const holder = await enqueue(s1, tid(1), { dispatchKey: KEY });
    assert.equal(holder.target_host, null);
    await assert.rejects(enqueue(s1, tid(2), { dispatchKey: KEY, targetHost: 'a2' }), (err) => err.code === 'LK423' && /different placement/.test(err.message));
    await assert.rejects(enqueue(s1, tid(1), { dispatchKey: KEY, avoidHosts: ['a1'] }), code('LK423'), 'the same id with another placement');
    await assert.rejects(enqueue(s1, tid(1), { avoidHosts: ['a1'] }), code('LK423'), 'the same id, unkeyed');
    assert.equal((await enqueue(s1, tid(3), { dispatchKey: KEY })).id, tid(1), 'the same (empty) placement adopts it');
    assert.equal(await call(s1, 'ld_ticket_by_key', KEY).then((v) => v.id), tid(1), 'the one-argument lookup is unchanged');
    await assert.rejects(call(s1, 'ld_ticket_by_key', KEY, { capability_class: 'linux', avoid_hosts: [], target_host: null }), code('LK423'));
    assert.equal((await call(s1, 'ld_ticket_by_key', KEY, { capability_class: null, avoid_hosts: [], target_host: null })).id, tid(1));
    assert.equal(await call(s1, 'ld_ticket_by_key', 'train:none', { capability_class: 'linux' }), null, 'no holder, nothing to refuse');
    const placed = await enqueue(s1, tid(5), { dispatchKey: 'train:placed', targetHost: 'a2', avoidHosts: ['x', 'a1'] });
    assert.deepEqual([placed.capability_class, placed.avoid_hosts, placed.target_host], [null, ['a1', 'x'], 'a2'], 'the view carries the placement');
    assert.equal((await call(s1, 'ld_ticket_by_key', 'train:placed', { avoid_hosts: ['A1', 'x'], target_host: 'A2' })).id, tid(5), 'compared canonical');
    assert.equal((await enqueue(s1, tid(6), { dispatchKey: 'train:placed' })).id, tid(5), 'an unconstrained request adopts the placed holder');
    assert.equal((await enqueue(s1, tid(5))).id, tid(5), 'and replays the placed id');
    assert.equal((await call(s1, 'ld_ticket_by_key', 'train:placed', { capability_class: null, avoid_hosts: [], target_host: null })).id, tid(5));
    await assert.rejects(enqueue(s1, tid(7), { dispatchKey: 'train:placed', targetHost: 'a1' }), code('LK423'));
  },

  'two concurrent submits of one key with different placement: the loser is refused LK423, the winner stands': async (w) => {
    const first = await w.cluster.client(w.db, logins.s1);
    await first.query('BEGIN');
    await enqueue(first, tid(1), { dispatchKey: KEY, targetHost: 'a2' });
    const second = await w.cluster.client(w.db, logins.s1);
    const answer = enqueue(second, tid(2), { dispatchKey: KEY, targetHost: 'a1' }).then((value) => ({ value }), (error) => ({ error }));
    await waitBlocked(w, second.processID);
    await first.query('COMMIT');
    assert.equal((await answer).error?.code, 'LK423');
    assert.equal((await w.ticket(tid(2))), undefined);
  },

  'an unconstrained submit that loses the key race to a placed holder adopts it instead of failing': async (w) => {
    const first = await w.cluster.client(w.db, logins.s1);
    await first.query('BEGIN');
    await enqueue(first, tid(1), { dispatchKey: KEY, targetHost: 'a2' });
    const second = await w.cluster.client(w.db, logins.s1);
    const answer = enqueue(second, tid(2), { dispatchKey: KEY }).then((value) => ({ value }), (error) => ({ error }));
    await waitBlocked(w, second.processID);
    await first.query('COMMIT');
    const { value, error } = await answer;
    assert.equal(error, undefined, error?.message);
    assert.equal(value.id, tid(1));
    assert.equal(await w.ticket(tid(2)), undefined);
  },

  'an avoided host may be the failed host of a runner that has since been removed, an unknown spelling may not, and a target must be a current runner': async (w) => {
    const s1 = await submit(w);
    const su = await w.su();
    await enqueue(s1, tid(1));
    await claimAs(w, logins.a2);
    await su.query("DELETE FROM lane_dispatch.principals WHERE host = 'a2'");
    assert.equal((await enqueue(s1, tid(2), { avoidHosts: ['a2'] })).id, tid(2), 'the host is on a retained ticket');
    await assert.rejects(enqueue(s1, tid(3), { avoidHosts: ['a22'] }), code('LK422'));
    await assert.rejects(enqueue(s1, tid(4), { targetHost: 'a2' }), code('LK422'));
    assert.deepEqual(await call(s1, 'ld_known_hosts'), ['a1', 'a2', 'x']);
  },

  'a host that only an attempt row names is known too': async (w) => {
    const su = await w.su();
    await enqueue(await submit(w), tid(1));
    await claimAs(w, logins.x);
    await su.query("UPDATE lane_dispatch.tickets SET host = NULL, state = 'queued', claim_token = NULL, lease_until = NULL WHERE id = $1", [tid(1)]);
    await su.query("DELETE FROM lane_dispatch.principals WHERE host = 'x'");
    assert.deepEqual(await call(await submit(w), 'ld_known_hosts'), ['a1', 'a2', 'x']);
    assert.equal((await enqueue(await submit(w), tid(2), { avoidHosts: ['x'] })).id, tid(2));
  },

  'ld_set_platform changes the platform alone, for the admin alone, and refuses an unknown host or platform': async (w) => {
    const su = await w.su();
    const admin = await w.as(logins.admin);
    const row = async () => (await su.query("SELECT tags, allowed_repos, max_claims, platform FROM lane_dispatch.principals WHERE host = 'a1'")).rows[0];
    await su.query("UPDATE lane_dispatch.principals SET max_claims = 5 WHERE host = 'a1'");
    const before = await row();
    await call(admin, 'ld_set_platform', 'a1', 'win32');
    assert.deepEqual(await row(), { ...before, platform: 'win32' });
    await call(admin, 'ld_set_platform', 'a1', null);
    assert.deepEqual(await row(), { ...before, platform: null });
    for (const login of [logins.a1, logins.s1, logins.reader]) await assert.rejects((await w.as(login)).query("SELECT lane_dispatch.ld_set_platform('a1', 'linux')"), /permission denied for (function|schema)/, login);
    await assert.rejects(call(admin, 'ld_set_platform', 'nobody', 'linux'), code('22023'));
    await assert.rejects(call(admin, 'ld_set_platform', 'a1', 'plan9'), code('22023'));
    assert.deepEqual(await row(), { ...before, platform: null }, 'refusals changed nothing');
  },

  'ld_claim reads the platform under the principal lock: a platform change committed while a claim waits is seen by that claim': async (w) => {
    await enqueue(await submit(w), tid(1), { capabilityClass: 'darwin' });
    const a1 = await w.as(logins.a1);
    const admin = await w.cluster.client(w.db, logins.admin);
    await admin.query('BEGIN');
    await call(admin, 'ld_set_platform', 'a1', 'darwin');
    const racing = claim(a1);
    assert.equal(await Promise.race([racing, sleep(400).then(() => 'waiting')]), 'waiting', 'the claim waits for the open platform change');
    await admin.query('COMMIT');
    assert.equal((await racing).id, tid(1), 'and then sees the darwin platform');
  },

  'the views carry the placement and the platform, appended to the existing columns': async (w) => {
    await enqueue(await submit(w), tid(1), { capabilityClass: 'darwin', avoidHosts: ['a1'], targetHost: 'a2' });
    const reader = await w.as(logins.reader);
    const { rows: [ticket] } = await reader.query('SELECT capability_class, avoid_hosts, target_host FROM lane.pull_tickets');
    assert.deepEqual(ticket, { capability_class: 'darwin', avoid_hosts: ['a1'], target_host: 'a2' });
    assert.deepEqual((await reader.query('SELECT host, platform FROM lane.pull_principals ORDER BY host')).rows, [{ host: 'a1', platform: 'linux' }, { host: 'a2', platform: 'darwin' }, { host: 'x', platform: null }]);
  },
};

/** The guard each one proves, then the text that disables it. A canary that does not make its guard fail proves nothing. */
const SQL_CANARIES = [
  ['an avoided host never claims: ld_claim candidate query', 'a ticket that avoids a host is never claimed by it and is claimed by another eligible host',
    'AND lane_dispatch.placeable_on(t.class_platform, t.avoid_hosts, t.target_host, v_me.host, v_me.platform)\n     AND (NOT t.avoid_tags', 'AND (NOT t.avoid_tags'],
  ['a class never claimed by another platform: placeable_on platform test', 'a capability class of one platform is claimed only by a principal of that platform, never by another or one with none recorded',
    "(p_class_platform IS NULL OR coalesce(p_class_platform = p_platform, false))", 'true'],
  ['a null platform never matches a class', 'a capability class of one platform is claimed only by a principal of that platform, never by another or one with none recorded',
    "(p_class_platform IS NULL OR coalesce(p_class_platform = p_platform, false))", '(p_class_platform IS NULL OR p_platform IS NULL OR p_class_platform = p_platform)'],
  ['the avoid test of placeable_on', 'a ticket that avoids a host is never claimed by it and is claimed by another eligible host',
    "AND NOT coalesce(p_host = ANY (coalesce(p_avoid_hosts, '{}')), false)", ''],
  ['the target test of placeable_on', 'a target host is the only host that claims the ticket',
    "AND (p_target_host IS NULL OR coalesce(p_target_host = p_host, false));", ';'],
  ['the head reserve ignores placement', 'a host does not reserve capacity for an aged head it can never claim',
    'AND lane_dispatch.placeable_on(t.class_platform, t.avoid_hosts, t.target_host, v_me.host, v_me.platform)\n     AND (NOT t.avoid_tags && v_me.tags OR v_now - t.created_at >= v_avoid_grace)\n     AND lane_dispatch.claim_need(t.cpu_cores, t.min_cpu_cores, v_now - t.created_at, v_floor_after) <= p_capacity_cpu',
    'AND (NOT t.avoid_tags && v_me.tags OR v_now - t.created_at >= v_avoid_grace)\n     AND lane_dispatch.claim_need(t.cpu_cores, t.min_cpu_cores, v_now - t.created_at, v_floor_after) <= p_capacity_cpu'],
  ['ld_needed_tools ignores placement', 'ld_needed_tools lists only the tools of tickets this agent may claim',
    'AND lane_dispatch.placeable_on(t.class_platform, t.avoid_hosts, t.target_host, v_me.host, v_platform));', ');'],
  ['scarcity ignores placement', 'scarcity counts the principals a placement leaves, and an unconstrained ticket orders exactly as it did',
    '             AND lane_dispatch.placeable_on(p_class_platform, p_avoid_hosts, p_target_host, p.host, p.platform));', ');'],
  ['a replay is validated against the current hosts', 'a repeat of a committed targeted enqueue is answered after the target runner is removed, keyed or not, and a new one is refused',
    "  -- the caller's own row under this id is the submission again, whatever has happened to the hosts since\n  IF EXISTS", "  PERFORM lane_dispatch.check_placement_hosts(v_avoid, v_target);\n  IF EXISTS"],
  ['a holder placed otherwise is adopted', 'a placed request adopts only a ticket placed as asked (LK423 on every holder path), while an unconstrained request adopts any holder',
    'IF FOUND AND NOT lane_dispatch.same_placement(v_t.capability_class, v_t.avoid_hosts, v_t.target_host, p_class, p_avoid_hosts, p_target_host) THEN\n    RAISE EXCEPTION \'dispatch key', 'IF false THEN\n    RAISE EXCEPTION \'dispatch key'],
  ['an unconstrained request is held to the holder placement', 'an unconstrained submit that loses the key race to a placed holder adopts it instead of failing',
    "(p_want_class IS NULL AND coalesce(cardinality(p_want_avoid), 0) = 0 AND p_want_target IS NULL)\n      OR ", 'false\n      OR '],
  ['the claim reads the platform without the principal lock', 'ld_claim reads the platform under the principal lock: a platform change committed while a claim waits is seen by that claim',
    "p.kind = 'agent' FOR UPDATE;", "p.kind = 'agent';"],
  ['an unknown avoided host is accepted', 'a malformed or unknown placement is refused before anything is stored',
    "IF NOT lane_dispatch.known_host(v_host) THEN RAISE EXCEPTION 'unknown host %', v_host USING ERRCODE = 'LK422'; END IF;", ''],
  ['the platform setter touches more than the platform', 'ld_set_platform changes the platform alone, for the admin alone, and refuses an unknown host or platform',
    'SET platform = p_platform WHERE host = p_host AND kind = \'agent\';', "SET platform = p_platform, max_claims = 1 WHERE host = p_host AND kind = 'agent';"],
];

describe('placement constraints on a disposable Postgres (PKG-439)', { skip: pgSkipOption, timeout: 900000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) test(name, async () => scenario(await placedWorld(cluster)));

  for (const [what, guard, from, to] of SQL_CANARIES) {
    test(`canary: with ${what} mutated, "${guard}" fails`, async (t) => {
      assert.ok(GUARDS[guard], 'the canary names a guard that exists');
      const world = await placedWorld(cluster, { mutate: [from, to] });
      let failure;
      await GUARDS[guard](world).catch((err) => { failure = err; });
      assert.ok(failure, `"${guard}" still passed with the guard removed, so it proves nothing`);
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
