import { describe, test, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, call, enqueue, claim, finish, release, liftBackoff, tid, simulateRestore } from './lane-dispatch-fixture.js';

const DAY = 86400;
const fenced = (client, ref, id) =>
  call(client, 'ld_enqueue_fenced', ref, id, 'repoA', 'test', `lk-${id}`, { protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x' }, [], 1, 1000, 0);
const rowCount = async (w, id) => (await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0].n;
const wasFinishedAgo = (w, id, state, seconds) =>
  w.su().then((su) => su.query("UPDATE lane_dispatch.tickets SET state = $2, finished_at = clock_timestamp() - ($3 || ' seconds')::interval WHERE id = $1", [id, state, seconds]));
const refOf = (token) => createHash('sha256').update(`lane-attempt-ref:${token}`).digest('hex');
const sample = (n) => Array.from({ length: n }, (_, i) => tid(i + 1));

async function pruneSetup(w) {
  const [s1, admin] = [await w.as(logins.s1), await w.as(logins.admin)];
  const ids = sample(5);
  for (const id of ids) await enqueue(s1, id);
  return { s1, admin, ids };
}

const GUARDS = {
  'ld_queue_ref is stable between calls and moves after a restore': async (w) => {
    const s1 = await w.as(logins.s1);
    const first = await call(s1, 'ld_queue_ref');
    assert.equal(await call(s1, 'ld_queue_ref'), first);
    await simulateRestore(w);
    const after = await call(s1, 'ld_queue_ref');
    assert.notEqual(after, first);
    assert.equal(await call(s1, 'ld_queue_ref'), after);
  },

  'ld_enqueue_fenced inserts nothing on a stale ref, inserts on a matching ref, replays idempotently and survives a restore': async (w) => {
    const s1 = await w.as(logins.s1);
    const ref = await call(s1, 'ld_queue_ref');
    const stale = await fenced(s1, 'stale', tid(1));
    assert.deepEqual(stale, { state: 'stale-queue-ref', queue_ref: ref });
    assert.equal(await rowCount(w, tid(1)), 0);
    const first = await fenced(s1, ref, tid(1));
    assert.equal(first.state, 'queued');
    assert.deepEqual(await fenced(s1, ref, tid(1)), first, 'a replay with the matching ref is idempotent');
    assert.equal(await rowCount(w, tid(1)), 1);
    await simulateRestore(w);
    const afterRestore = await fenced(s1, ref, tid(2));
    assert.equal(afterRestore.state, 'stale-queue-ref');
    assert.notEqual(afterRestore.queue_ref, ref);
    assert.equal(await rowCount(w, tid(2)), 0, 'an enqueue fenced on the pre-restore ref lands nowhere');
    assert.equal((await fenced(s1, afterRestore.queue_ref, tid(2))).state, 'queued');
  },

  'ticket_view carries a stable attempt_ref per claim and never the claim token': async (w) => {
    const [s1, a1, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.su()];
    await enqueue(s1, tid(1));
    assert.equal((await call(s1, 'ld_get', tid(1))).attempt_ref, null, 'queued: no claim, no ref');
    const first = await claim(a1);
    const view = await call(s1, 'ld_get', tid(1));
    assert.equal(view.attempt_ref, refOf(first.token));
    assert.equal((await call(s1, 'ld_get', tid(1))).attempt_ref, view.attempt_ref);
    assert.equal('claim_token' in view, false);
    assert.equal(JSON.stringify(view).includes(first.token), false, 'the raw token appears nowhere in the view');
    await call(a1, 'ld_release', first.id, first.epoch, first.token, 'never-started');
    await liftBackoff(w);
    const second = await claim(a1);
    assert.notEqual((await call(s1, 'ld_get', tid(1))).attempt_ref, view.attempt_ref, 'a new claim of the same ticket has a new ref');
    await finish(a1, second);
    const done = await call(s1, 'ld_get', tid(1));
    assert.equal(done.state, 'finished');
    assert.equal(done.attempt_ref, refOf((await su.query('SELECT claim_token FROM lane_dispatch.tickets')).rows[0].claim_token));
  },

  'ld_cancel stores a server-authored result and a replay never upgrades confirmation': async (w) => {
    const [s1, admin] = [await w.as(logins.s1), await w.as(logins.admin)];
    await enqueue(s1, tid(1));
    assert.deepEqual(await call(s1, 'ld_cancel', tid(1)), { state: 'cancelled', cancel_requested: true, confirmed: true });
    assert.deepEqual((await w.ticket(tid(1))).result, { kind: 'cancelled', cancelConfirmed: true, source: 'queued', epoch: 0, author: 'queue' });
    assert.equal((await call(s1, 'ld_cancel', tid(1))).confirmed, true, 'queued cancel stays confirmed on replay');

    await enqueue(s1, tid(2));
    await call(admin, 'ld_restore_reset');
    const lost = { state: 'lost', cancel_requested: false, confirmed: false };
    assert.deepEqual(await call(s1, 'ld_cancel', tid(2)), lost, 'a restored (lost) row is unchanged by a cancel and never reads as confirmed');
    assert.deepEqual((await w.ticket(tid(2))).result, { kind: 'lost', reason: 'queue-restored', epoch: 0, author: 'queue' });
    assert.deepEqual(await call(s1, 'ld_cancel', tid(2)), lost, 'and stays so on replay');

    await enqueue(s1, tid(3));
    await finish(await w.as(logins.a1), await claim(await w.as(logins.a1)), { kind: 'cancelled', cancelConfirmed: false });
    assert.equal((await call(s1, 'ld_cancel', tid(3))).confirmed, false, 'a runner-reported unconfirmed cancel never reads as confirmed on replay');
  },

  'ld_finish never stores an author key, so only the queue itself can write a queue-authored record': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const claimed = await claim(a1);
    await finish(a1, claimed, { kind: 'cancelled', cancelConfirmed: true, source: 'queued', author: 'queue' });
    const stored = (await w.ticket(tid(1))).result;
    assert.equal(Object.hasOwn(stored, 'author'), false);
    assert.deepEqual(stored, { kind: 'cancelled', cancelConfirmed: true, source: 'queued', epoch: claimed.epoch });
  },

  'ld_placeable counts the agents eligible for the repo and tags': async (w) => {
    const [s1, admin] = [await w.as(logins.s1), await w.as(logins.admin)];
    assert.equal(await call(s1, 'ld_placeable', 'repoA', []), 3, 'a1 (repoA), a2 and x (*) are eligible with no tags');
    assert.equal(await call(s1, 'ld_placeable', 'repoB', []), 2, 'a1 does not allow repoB');
    assert.equal(await call(s1, 'ld_placeable', 'repoA', ['fast']), 1, 'only a1 has the fast tag');
    assert.equal(await call(s1, 'ld_placeable', 'repoA', ['gpu']), 0);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', null), 3);
    await call(admin, 'ld_set_principal', logins.x, 'agent', 'x', [], ['*'], 0);
    assert.equal(await call(s1, 'ld_placeable', 'repoA', []), 2, 'a principal with max_claims 0 never counts');
  },

  'ld_prune_candidates pages by keyset from a NULL first page and marks row_due; a half-null cursor raises': async (w) => {
    const { admin, ids } = await pruneSetup(w);
    const [a, b, c, d] = ids;
    await wasFinishedAgo(w, a, 'finished', 10 * DAY);
    await wasFinishedAgo(w, b, 'cancelled', 9 * DAY);
    await wasFinishedAgo(w, c, 'lost', 8 * DAY);
    await wasFinishedAgo(w, d, 'withdrawn', 3 * DAY);
    await enqueue(await w.as(logins.s1), tid(6));
    await wasFinishedAgo(w, tid(6), 'finished', 60);
    const page = (after, limit = 2) => call(admin, 'ld_prune_candidates', '1 day', '7 days', after?.finished_at ?? null, after?.id ?? null, limit);
    const p1 = await page(null);
    assert.deepEqual(p1.items.map((i) => [i.id, i.row_due]), [[a, true], [b, true]]);
    const p2 = await page(p1.next);
    assert.deepEqual(p2.items.map((i) => [i.id, i.row_due]), [[c, true], [d, false]]);
    const p3 = await page(p2.next);
    assert.deepEqual(p3, { items: [], next: null }, 'the young terminal row and the queued row are not candidates; the exhausted page has no next');
    await assert.rejects(call(admin, 'ld_prune_candidates', '1 day', '7 days', null, a, 2), (err) => err.code === '22023');
    await assert.rejects(call(admin, 'ld_prune_candidates', '1 day', '7 days', p1.next.finished_at, null, 2), (err) => err.code === '22023');
  },

  'ld_prune deletes only terminal rows past the row age, with their chunks and attempts': async (w) => {
    const { admin, ids } = await pruneSetup(w);
    const [old, fresh, live, queued] = ids;
    const su = await w.su();
    await wasFinishedAgo(w, old, 'finished', 10 * DAY);
    await wasFinishedAgo(w, fresh, 'finished', 60);
    await wasFinishedAgo(w, live, 'queued', 10 * DAY);
    await su.query("INSERT INTO lane_dispatch.attempts (ticket, epoch, host, claim_token) VALUES ($1, 1, 'a1', gen_random_uuid())", [old]);
    for (const id of [old, fresh, live]) await su.query("INSERT INTO lane_dispatch.log_chunks (ticket, epoch, seq, bytes) VALUES ($1, 1, 1, 'x')", [id]);
    assert.equal(await call(admin, 'ld_prune', [old, fresh, live, queued], '7 days'), 1);
    assert.equal(await rowCount(w, old), 0);
    for (const id of [fresh, live, queued]) assert.equal(await rowCount(w, id), 1, 'fresh terminal, non-terminal and queued rows survive');
    const chunks = (await su.query('SELECT ticket FROM lane_dispatch.log_chunks ORDER BY ticket')).rows.map((r) => r.ticket);
    assert.deepEqual(chunks, [fresh, live], 'the pruned ticket chunks went with it, the survivors keep theirs');
    assert.equal((await su.query('SELECT count(*)::int AS n FROM lane_dispatch.attempts')).rows[0].n, 0, 'attempts cascade');
    await assert.rejects(call(admin, 'ld_prune', sample(501).map((_, i) => tid(i + 1)), '7 days'), (err) => err.code === '22023');
  },

  'ld_prune_chunks removes orphans and long-terminal chunks only, at most limit per call': async (w) => {
    const { admin, ids } = await pruneSetup(w);
    const [oldTerminal, freshTerminal, live] = ids;
    const su = await w.su();
    await wasFinishedAgo(w, oldTerminal, 'finished', 7200);
    await wasFinishedAgo(w, freshTerminal, 'finished', 60);
    for (const id of [oldTerminal, freshTerminal, live, tid(40), tid(41), tid(42)]) {
      await su.query("INSERT INTO lane_dispatch.log_chunks (ticket, epoch, seq, bytes) VALUES ($1, 1, 1, 'x')", [id]);
    }
    assert.equal(await call(admin, 'ld_prune_chunks', '1 hour', 2), 2, 'bounded by the limit');
    assert.equal(await call(admin, 'ld_prune_chunks', '1 hour', 10), 2, 'the other two of the four removable chunks');
    const left = (await su.query('SELECT ticket FROM lane_dispatch.log_chunks ORDER BY ticket')).rows.map((r) => r.ticket);
    assert.deepEqual(left, [freshTerminal, live].sort());
  },

  'ld_blobs_live reports only blobs referenced by a non-terminal ticket and bounds its input': async (w) => {
    const [s1, admin] = [await w.as(logins.s1), await w.as(logins.admin)];
    const snapshot = (sha) => ({ key: 'snap', sha256: 'x', blobs: [{ ns: 'n', sha256: sha }] });
    const enq = (id, sha) => call(s1, 'ld_enqueue', id, 'repoA', 'test', `lk-${id}`, { protocol: 1 }, { cmd: ['true'] }, snapshot(sha), [], 1, 1000, 0);
    await enq(tid(1), 'live-sha');
    await enq(tid(2), 'dead-sha');
    await wasFinishedAgo(w, tid(2), 'finished', 60);
    const refs = [{ ns: 'n', sha256: 'live-sha' }, { ns: 'n', sha256: 'dead-sha' }, { ns: 'other', sha256: 'live-sha' }, { ns: 'n', sha256: 'unknown' }];
    assert.deepEqual(await call(admin, 'ld_blobs_live', JSON.stringify(refs)), [refs[0]]);
    assert.deepEqual(await call(admin, 'ld_blobs_live', '[]'), []);
    await assert.rejects(call(admin, 'ld_blobs_live', JSON.stringify(Array(501).fill(refs[0]))), (err) => err.code === '22023');
    await assert.rejects(call(admin, 'ld_blobs_live', '{}'), (err) => err.code === '22023');
  },

  'ld_cancel reads a row released as cancelled after a cancel request as confirmed': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    assert.equal((await call(s1, 'ld_cancel', tid(1))).confirmed, false, 'a held row is not confirmed by the request');
    assert.deepEqual(await release(a1, got, 'cancel-confirmed'), { fenced: false, state: 'cancelled' });
    assert.equal((await w.ticket(tid(1))).result, null);
    assert.deepEqual(await call(s1, 'ld_cancel', tid(1)), { state: 'cancelled', cancel_requested: true, confirmed: true });
  },

  'ld_cancel reads a claimed row reaped as cancelled after a cancel request as confirmed': async (w) => {
    const [s1, a1, admin, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin), await w.su()];
    await enqueue(s1, tid(1));
    await claim(a1);
    await call(s1, 'ld_cancel', tid(1));
    await su.query("UPDATE lane_dispatch.tickets SET lease_until = now() - interval '10 seconds'");
    await su.query('UPDATE lane_dispatch.cluster SET last_tick = clock_timestamp()');
    assert.equal((await call(admin, 'ld_reap')).cancelled, 1);
    assert.deepEqual(await call(s1, 'ld_cancel', tid(1)), { state: 'cancelled', cancel_requested: true, confirmed: true });
  },

  'ld_cancel reads a non-boolean cancelConfirmed as false': async (w) => {
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    const got = await claim(a1);
    await finish(a1, got, { kind: 'cancelled', cancelConfirmed: 'yes' });
    assert.equal((await call(s1, 'ld_cancel', tid(1))).confirmed, false);
  },

  'ld_blobs_live survives a ticket whose snapshot blobs is not an array, and ld_enqueue_fenced refuses one': async (w) => {
    const [s1, admin] = [await w.as(logins.s1), await w.as(logins.admin)];
    const raw = (id, snapshot) => call(s1, 'ld_enqueue', id, 'repoA', 'test', `lk-${id}`, { protocol: 1 }, { cmd: ['true'] }, snapshot, [], 1, 1000, 0);
    await raw(tid(1), { key: 'snap', sha256: 'x', blobs: null });
    await raw(tid(2), { key: 'snap', sha256: 'x', blobs: [{ ns: 'n', sha256: 'live' }] });
    const refs = [{ ns: 'n', sha256: 'live' }, { ns: 'n', sha256: 'other' }];
    assert.deepEqual(await call(admin, 'ld_blobs_live', JSON.stringify(refs)), [refs[0]]);
  },

  'ld_enqueue_fenced refuses a snapshot whose blobs is not an array': async (w) => {
    const s1 = await w.as(logins.s1);
    const ref = await call(s1, 'ld_queue_ref');
    for (const [i, blobs] of [null, 'x', {}].entries()) {
      await assert.rejects(
        call(s1, 'ld_enqueue_fenced', ref, tid(i + 1), 'repoA', 'test', `lk${i}`, { protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x', blobs }, [], 1, 1000, 0),
        (err) => err.code === '22023');
      assert.equal(await rowCount(w, tid(i + 1)), 0);
    }
    const ok = await call(s1, 'ld_enqueue_fenced', ref, tid(9), 'repoA', 'test', 'lk9', { protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x', blobs: [] }, [], 1, 1000, 0);
    assert.equal(ok.state, 'queued');
  },

  'ld_prune deletes the claim_requests of the pruned tickets': async (w) => {
    const [s1, a1, admin, su] = [await w.as(logins.s1), await w.as(logins.a1), await w.as(logins.admin), await w.su()];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const got = await claim(a1);
    await finish(a1, got);
    await claim(a1);
    const remaining = (await su.query('SELECT ticket FROM lane_dispatch.claim_requests ORDER BY ticket')).rows.map((r) => r.ticket);
    assert.deepEqual(remaining, [tid(1), tid(2)]);
    await wasFinishedAgo(w, tid(1), 'finished', 10 * DAY);
    assert.equal(await call(admin, 'ld_prune', [tid(1), tid(2)], '7 days'), 1);
    assert.deepEqual((await su.query('SELECT ticket FROM lane_dispatch.claim_requests')).rows.map((r) => r.ticket), [tid(2)],
      'the pruned ticket mapping goes, the live ticket mapping stays');
  },

  'ticket_view returns only the callers own tickets from ld_get, ld_list_mine and ld_enqueue': async (w) => {
    const [s1, s2] = [await w.as(logins.s1), await w.as(logins.s2)];
    await enqueue(s1, tid(1));
    assert.equal(await call(s2, 'ld_get', tid(1)), null);
    assert.deepEqual(await call(s2, 'ld_list_mine'), []);
    assert.equal((await call(s1, 'ld_get', tid(1))).id, tid(1));
    assert.deepEqual((await call(s1, 'ld_list_mine')).map((t) => t.id), [tid(1)]);
    await assert.rejects(enqueue(s2, tid(1)), (err) => err.code === '23505');
  },
};

const CANARIES = [
  ['ld_queue_ref is stable between calls and moves after a restore',
    "RETURN (SELECT c.timeline::text || '.' || c.incarnation::text FROM lane_dispatch.cluster c WHERE c.id = 1);", "RETURN '1.0';"],
  ['ld_enqueue_fenced inserts nothing on a stale ref, inserts on a matching ref, replays idempotently and survives a restore',
    'IF p_expect_ref IS DISTINCT FROM v_ref THEN', 'IF false THEN'],
  ['ticket_view carries a stable attempt_ref per claim and never the claim token',
    "ELSE encode(sha256(convert_to('lane-attempt-ref:' || t.claim_token::text, 'UTF8')), 'hex') END)", "ELSE t.claim_token::text END, 'claim_token', t.claim_token)"],
  ['ticket_view carries a stable attempt_ref per claim and never the claim token',
    "ELSE encode(sha256(convert_to('lane-attempt-ref:' || t.claim_token::text, 'UTF8')), 'hex') END)", "ELSE 'constant' END)"],
  ['ld_cancel stores a server-authored result and a replay never upgrades confirmation',
    "coalesce(CASE WHEN jsonb_typeof(v_t.result->'cancelConfirmed') = 'boolean' THEN (v_t.result->>'cancelConfirmed')::boolean END, v_t.state = 'cancelled' AND v_t.result IS NULL)", "v_t.state = 'cancelled'"],
  ['ld_finish never stores an author key, so only the queue itself can write a queue-authored record', "(p_result - 'author') ||", 'p_result ||'],
  ['ld_cancel stores a server-authored result and a replay never upgrades confirmation', "'epoch', v_t.epoch, 'author', 'queue')", "'epoch', v_t.epoch)"],
  ['ld_placeable counts the agents eligible for the repo and tags', ' AND lane_dispatch.eligible(coalesce(p_requires_tags, \'{}\'), p_repo_key, p.tags, p.allowed_repos));', ');'],
  ['ld_placeable counts the agents eligible for the repo and tags', "p.kind = 'agent' AND p.max_claims > 0", "p.kind = 'agent'"],
  ['ld_prune_candidates pages by keyset from a NULL first page and marks row_due; a half-null cursor raises',
    'OR (p_after_finished IS NOT NULL AND p_after_id IS NOT NULL AND (t.finished_at, t.id) > (p_after_finished, p_after_id)))', 'OR true)'],
  ['ld_prune_candidates pages by keyset from a NULL first page and marks row_due; a half-null cursor raises',
    'IF (p_after_finished IS NULL) <> (p_after_id IS NULL) THEN', 'IF false THEN'],
  ['ld_prune deletes only terminal rows past the row age, with their chunks and attempts',
    "t.id = ANY (p_ids) AND t.state IN ('finished','cancelled','withdrawn','lost') AND", 't.id = ANY (p_ids) AND'],
  ['ld_prune deletes only terminal rows past the row age, with their chunks and attempts', 'AND t.finished_at < lane_dispatch.now() - p_rows_after', ''],
  ['ld_prune deletes only terminal rows past the row age, with their chunks and attempts', 'DELETE FROM lane_dispatch.log_chunks WHERE ticket = ANY (v_due);', ''],
  ['ld_prune_chunks removes orphans and long-terminal chunks only, at most limit per call', 'LIMIT least(greatest(coalesce(p_limit, 10000), 1), 100000)', ''],
  ['ld_prune_chunks removes orphans and long-terminal chunks only, at most limit per call',
    "WHERE t.id IS NULL OR (t.state IN ('finished','cancelled','withdrawn','lost') AND t.finished_at < lane_dispatch.now() - p_terminal_after)", 'WHERE true'],
  ['ld_blobs_live reports only blobs referenced by a non-terminal ticket and bounds its input',
    "WHERE t.state NOT IN ('finished','cancelled','withdrawn','lost')\n       AND b->>", 'WHERE true\n       AND b->>'],
  ['ld_cancel reads a row released as cancelled after a cancel request as confirmed', "v_t.state = 'cancelled' AND v_t.result IS NULL)", 'false)'],
  ['ld_cancel reads a claimed row reaped as cancelled after a cancel request as confirmed', "v_t.state = 'cancelled' AND v_t.result IS NULL)", 'false)'],
  ['ld_cancel reads a non-boolean cancelConfirmed as false', "CASE WHEN jsonb_typeof(v_t.result->'cancelConfirmed') = 'boolean' THEN (v_t.result->>'cancelConfirmed')::boolean END", "(v_t.result->>'cancelConfirmed')::boolean"],
  ['ld_blobs_live survives a ticket whose snapshot blobs is not an array, and ld_enqueue_fenced refuses one', "jsonb_array_elements(CASE WHEN jsonb_typeof(t.snapshot->'blobs') = 'array' THEN t.snapshot->'blobs' ELSE '[]'::jsonb END) b", "jsonb_array_elements(t.snapshot->'blobs') b"],
  ['ld_enqueue_fenced refuses a snapshot whose blobs is not an array', "IF p_snapshot ? 'blobs' AND jsonb_typeof(p_snapshot->'blobs') <> 'array' THEN", 'IF false THEN'],
  ['ld_prune deletes the claim_requests of the pruned tickets', 'DELETE FROM lane_dispatch.claim_requests WHERE ticket = ANY (v_due);', ''],
  ['ticket_view returns only the callers own tickets from ld_get, ld_list_mine and ld_enqueue', 'WHERE t.id = p_id AND t.owner = p_owner);', 'WHERE t.id = p_id);'],
];

describe('lane_dispatch submit-side surface for the pull client on a disposable Postgres', { skip: pgSkipOption, timeout: 600000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());

  for (const [name, scenario] of Object.entries(GUARDS)) {
    test(name, async () => scenario(await World.create(cluster)));
  }

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
