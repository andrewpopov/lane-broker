import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createAttempt } from '../src/attempts.js';
import { pullTicketDir } from '../src/pull-envelope.js';
import { ledgerAccept, readLedger } from '../src/pull-ledger.js';
import { createS3Client, S3Error } from '../src/s3-client.js';
import { ensureStateDirs, readJsonSafe } from '../src/state.js';
import { freshEnv } from './helpers.js';
import { ARTIFACT_PATH, artifactsStream, definePullSuite, pullFixture } from './helpers/pull-fixture.js';
import { harnessClient } from './helpers/queue-fixture.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './helpers/s3-stub.js';
import { call, enqueue, logins, tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R6: `lane pull gc` against the real SQL and the S3 stub (T18, T30, T31, T35, the re-HEAD barrier, rows_after). */

const DAY = 86_400;
const NS = 'n'.repeat(32);
const sha = (n) => String(n).repeat(64).slice(0, 64);
const OUT = 'lane-out';
const SNAP = 'lane-snap';

async function gcFixture(t, w, load, { cfg = {}, now } = {}) {
  const stub = await startS3Stub();
  t.after(() => stub.close());
  const s3 = createS3Client({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS });
  const { state } = freshEnv();
  ensureStateDirs(state);
  const admin = harnessClient(t, w, { login: logins.admin }).queue;
  const submit = await w.as(logins.s1);
  const su = await w.su();
  const { gcOnce } = await load('pull-gc.js');
  const events = [];
  const recorded = {
    s3: { ...s3, deleteObject: async (bucket, key) => { events.push(['delete', key]); return s3.deleteObject(bucket, key); } },
    queue: { ...admin, prune: async (ids, ms) => { events.push(['prune', ids]); return admin.prune(ids, ms); } },
  };
  const fx = {
    state, stub, s3, admin, events,
    run: (over = {}) => gcOnce({ root: state, queue: recorded.queue, s3: recorded.s3, cfg, now, ...over }),
    async terminal(n, ageSeconds, st = 'finished') {
      await enqueue(submit, tid(n));
      await su.query("UPDATE lane_dispatch.tickets SET state = $2, finished_at = clock_timestamp() - ($3 || ' seconds')::interval WHERE id = $1", [tid(n), st, ageSeconds]);
      return tid(n);
    },
    async queuedWithBlobs(n, blobs, ageSeconds = 0) {
      const snapshot = { key: 'snap', sha256: 'x', blobs: blobs.map((sha256) => ({ ns: NS, sha256, size: 1 })) };
      await call(submit, 'ld_enqueue', tid(n), 'repoA', 'test', `lk-${tid(n)}`, { protocol: 1 }, { cmd: ['true'] }, snapshot, [], 1, 1000, 0);
      await su.query("UPDATE lane_dispatch.tickets SET created_at = clock_timestamp() - ($2 || ' seconds')::interval WHERE id = $1", [tid(n), ageSeconds]);
    },
    put(bucket, key, ageMs = 0) {
      stub.objects.set(`${bucket}/${key}`, { body: Buffer.from('x'), lastModified: Date.now() - ageMs });
    },
    has: (bucket, key) => stub.objects.has(`${bucket}/${key}`),
    putTicketObjects(id) {
      fx.put(OUT, `${id}/1/aa/log`);
      fx.put(OUT, `${id}/1/aa/artifacts`);
      fx.put(SNAP, `streams/${id}-0-${sha(1)}.gz`);
    },
    ticketObjects: (id) => [[OUT, `${id}/1/aa/log`], [OUT, `${id}/1/aa/artifacts`], [SNAP, `streams/${id}-0-${sha(1)}.gz`]],
    rows: async () => (await su.query('SELECT id FROM lane_dispatch.tickets ORDER BY id')).rows.map((r) => r.id),
    listRequests: () => stub.requests.filter((r) => r.method === 'GET' && r.query.includes('list-type')).length,
    state_: () => readJsonSafe(path.join(state, 'pull', 'gc-state.json')),
  };
  return fx;
}

const ticking = (stepMs) => {
  let at = Date.now();
  return () => (at += stepMs);
};

const objectsOf = (fx, id) => fx.ticketObjects(id).filter(([bucket, key]) => fx.has(bucket, key));

const SCENARIOS = {
  'T18: objects are deleted before the rows, a failing delete leaves its row for the next run, and orphan chunks go last': async (load, w, t) => {
    const fx = await gcFixture(t, w, load);
    const [a, b] = [await fx.terminal(1, 10 * DAY), await fx.terminal(2, 9 * DAY)];
    for (const id of [a, b]) fx.putTicketObjects(id);
    const su = await w.su();
    await su.query("INSERT INTO lane_dispatch.log_chunks (ticket, epoch, seq, bytes) VALUES ($1, 1, 1, 'x'), ($2, 1, 1, 'x')", [a, tid(99)]);
    const failing = {
      ...fx.s3,
      deleteObject: async (bucket, key) => {
        fx.events.push(['delete', key]);
        if (key.startsWith(`${b}/`) || key.startsWith(`streams/${b}-`)) throw new S3Error('denied', { status: 403 });
        return fx.s3.deleteObject(bucket, key);
      },
    };
    const first = await fx.run({ s3: failing });
    assert.deepEqual(first.failures.map((f) => f.id), [b]);
    assert.deepEqual(await fx.rows(), [b], 'the row whose objects survived is kept; the clean one is pruned');
    assert.equal(objectsOf(fx, a).length, 0);
    assert.ok(objectsOf(fx, b).length > 0);
    const deletesOfA = fx.events.flatMap((e, i) => (e[0] === 'delete' && (e[1].startsWith(`${a}/`) || e[1].startsWith(`streams/${a}-`)) ? [i] : []));
    const pruneOfA = fx.events.findIndex((e) => e[0] === 'prune' && e[1].includes(a));
    assert.ok(deletesOfA.length === 3 && pruneOfA > Math.max(...deletesOfA), 'every object of a ticket is gone before its row is pruned');
    const chunks = (await su.query('SELECT ticket FROM lane_dispatch.log_chunks')).rows;
    assert.deepEqual(chunks, [], 'the pruned ticket chunks and the orphan chunk are gone');
    const second = await fx.run();
    assert.deepEqual([second.failures, second.rowsPruned], [[], 1]);
    assert.deepEqual(await fx.rows(), []);
    assert.equal(objectsOf(fx, b).length, 0);
  },

  'T18: a blob referenced by a non-terminal ticket survives, an unreferenced or only terminal-referenced old blob goes, and the live set is exact': async (load, w, t) => {
    const fx = await gcFixture(t, w, load);
    const old = 20 * DAY * 1000;
    const keyOf = (n) => `blobs/${NS}/${sha(n)}`;
    for (const n of [1, 2, 3, 4]) fx.put(SNAP, keyOf(n), old);
    fx.put(SNAP, keyOf(5), 2 * DAY * 1000);
    fx.put(SNAP, 'blobs/readme', old);
    await fx.queuedWithBlobs(1, [sha(1)], 30 * DAY);
    await fx.queuedWithBlobs(2, [sha(1), sha(2)], 1);
    await fx.queuedWithBlobs(3, [sha(3)]);
    await (await w.su()).query("UPDATE lane_dispatch.tickets SET state = 'finished', finished_at = clock_timestamp() WHERE id = $1", [tid(3)]);
    const summary = await fx.run();
    assert.equal(summary.blobsDeleted, 2);
    assert.deepEqual([1, 2, 3, 4, 5].map((n) => fx.has(SNAP, keyOf(n))), [true, true, false, false, true], 'the older and the newer queued ticket both keep blob 1; blob 2 is kept by the newer one only');
    assert.ok(fx.has(SNAP, 'blobs/readme'), 'a key that is not a blob is never touched');
    const off = await gcFixture(t, w, load, { cfg: { blobGc: false } });
    off.put(SNAP, keyOf(9), old);
    assert.equal((await off.run()).blobsDeleted, 0);
    assert.ok(off.has(SNAP, keyOf(9)), 'blobGc false switches the blob sweep off');
  },

  'V4-3: a blob re-PUT after gc chose it survives, because gc looks at its age again before the delete': async (load, w, t) => {
    const fx = await gcFixture(t, w, load);
    const key = `blobs/${NS}/${sha(7)}`;
    fx.put(SNAP, key, 20 * DAY * 1000);
    const tmp = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'gc-reput-')), 'blob');
    fs.writeFileSync(tmp, 'x');
    let paused = false;
    const summary = await fx.run({ hooks: { beforeBlobDelete: async () => { paused = true; await fx.s3.putObjectFromFile(SNAP, key, tmp); } } });
    assert.ok(paused, 'gc did pick the blob and stop at the hook');
    assert.ok(fx.has(SNAP, key), 'the re-PUT blob survived');
    assert.equal(summary.blobsDeleted, 0);
  },

  'T31: a ticket queued for 40 days keeps its stream through every run, a terminal ticket loses its objects after 24 hours only': async (load, w, t) => {
    const fx = await gcFixture(t, w, load);
    await fx.queuedWithBlobs(1, [], 40 * DAY);
    fx.putTicketObjects(tid(1));
    const fresh = await fx.terminal(2, 23 * 3600);
    fx.putTicketObjects(fresh);
    const done = await fx.terminal(3, 25 * 3600);
    fx.putTicketObjects(done);
    for (let run = 0; run < 3; run++) await fx.run();
    assert.equal(objectsOf(fx, tid(1)).length, 3, 'queued work has no age cutoff');
    assert.equal(objectsOf(fx, fresh).length, 3, 'terminal for less than 24 hours');
    assert.equal(objectsOf(fx, done).length, 0);
    assert.deepEqual(await fx.rows(), [tid(1), fresh, done], 'rows outlive objects until rows_after');
  },

  'T35a: with every ticket object-due and row-young, every page is row_due false: objects cleaned, no row pruned, the row cursor stays at the start': async (load, w, t) => {
    const fx = await gcFixture(t, w, load, { now: ticking(60_000), cfg: { blobGc: false } });
    const ids = [];
    for (let n = 1; n <= 5; n++) ids.push(await fx.terminal(n, (3 - n / 10) * DAY));
    ids.forEach((id) => fx.putTicketObjects(id));
    const summary = await fx.run({ pageLimit: 2 });
    assert.deepEqual([summary.pages, summary.rowsPruned, summary.budgetCut], [3, 0, false]);
    assert.ok(ids.every((id) => objectsOf(fx, id).length === 0));
    assert.equal(fx.state_().rowsFrom, null);
    assert.equal(fx.state_().objectsDone.id, ids.at(-1));
    assert.equal((await fx.rows()).length, 5);
  },

  'T35b: older row-due pages are pruned and later young pages are not, the next run resumes after the pruned ones with no store call': async (load, w, t) => {
    const fx = await gcFixture(t, w, load, { now: ticking(60_000), cfg: { blobGc: false } });
    const due = [];
    for (let n = 1; n <= 4; n++) due.push(await fx.terminal(n, (12 - n) * DAY));
    const young = [];
    for (let n = 5; n <= 7; n++) young.push(await fx.terminal(n, (3 - n / 10) * DAY));
    [...due, ...young].forEach((id) => fx.putTicketObjects(id));
    const first = await fx.run({ pageLimit: 2 });
    assert.deepEqual([first.pages, first.rowsPruned], [4, 4]);
    assert.deepEqual(await fx.rows(), young);
    assert.equal(fx.state_().rowsFrom.id, due.at(-1), 'the cursor sits at the last pruned ticket');
    const lists = fx.listRequests();
    const second = await fx.run({ pageLimit: 2 });
    assert.deepEqual([second.rowsPruned, second.pages], [0, 2]);
    assert.equal(fx.listRequests(), lists, 'the objects are already clean: the second run asks the database only');
  },

  'T35c: the first page is admitted with both cursors null, and a half-null cursor is refused by the real SQL': async (load, w, t) => {
    const fx = await gcFixture(t, w, load);
    const id = await fx.terminal(1, 3 * DAY);
    const page = await fx.admin.pruneCandidates({ objectsAfterMs: 1000, rowsAfterMs: 7 * DAY * 1000 });
    assert.deepEqual([page.items.map((i) => i.id), page.items[0].row_due, page.next], [[id], false, null]);
    await assert.rejects(fx.admin.pruneCandidates({ objectsAfterMs: 1000, rowsAfterMs: 1000, after: { finished_at: page.items[0].finished_at, id: null } }), (err) => err.code === '22023');
    await assert.rejects(fx.admin.pruneCandidates({ objectsAfterMs: 1000, rowsAfterMs: 1000, after: { finished_at: null, id } }), (err) => err.code === '22023');
  },

  'T35d: a run stops when its budget is spent after k pages, and the next run continues from the persisted cursors without re-cleaning them': async (load, w, t) => {
    const fx = await gcFixture(t, w, load, { now: ticking(120_000), cfg: { blobGc: false } });
    const ids = [];
    for (let n = 1; n <= 7; n++) ids.push(await fx.terminal(n, (3 - n / 10) * DAY));
    ids.forEach((id) => fx.putTicketObjects(id));
    const first = await fx.run({ pageLimit: 2, budgetMs: 300_000 });
    assert.deepEqual([first.pages, first.budgetCut], [3, true]);
    assert.ok(ids.slice(0, 6).every((id) => objectsOf(fx, id).length === 0));
    assert.equal(objectsOf(fx, ids[6]).length, 3, 'the cut run never reached the last ticket');
    const lists = fx.listRequests();
    const second = await fx.run({ pageLimit: 2, now: ticking(1000), budgetMs: 300_000 });
    assert.deepEqual([second.pages, second.budgetCut], [1, false], 'the database cursor of the cut run was persisted: only the last page is read');
    assert.equal(objectsOf(fx, ids[6]).length, 0);
    assert.equal(fx.listRequests() - lists, 2, 'only the last ticket costs a LIST per prefix; the first six are skipped');
  },

  'a failed delete on one page never lets a later clean page advance objectsDone past it: its objects are cleaned before its row goes': async (load, w, t) => {
    const fx = await gcFixture(t, w, load, { now: ticking(1000), cfg: { blobGc: false } });
    const ids = [];
    for (let n = 1; n <= 4; n++) ids.push(await fx.terminal(n, (12 - n) * DAY));
    ids.forEach((id) => fx.putTicketObjects(id));
    const failing = { ...fx.s3, deleteObject: async (bucket, key) => { if (key.startsWith(`${ids[0]}/`)) throw new S3Error('denied', { status: 403 }); return fx.s3.deleteObject(bucket, key); } };
    const first = await fx.run({ pageLimit: 2, s3: failing });
    assert.deepEqual(first.failures.map((f) => f.id), [ids[0]]);
    assert.equal(fx.state_().objectsDone, null, 'the second page cleaned fine but the first failed');
    assert.ok(objectsOf(fx, ids[0]).length > 0);
    await fx.run({ pageLimit: 2 });
    assert.equal(objectsOf(fx, ids[0]).length, 0, 'the retry cleaned the objects the first run could not');
    assert.deepEqual(await fx.rows(), []);
  },

  'the blob sweep stops at the run budget like the ticket sweep': async (load, w, t) => {
    const fx = await gcFixture(t, w, load, { now: ticking(1000) });
    for (const n of [1, 2, 3]) fx.put(SNAP, `blobs/${NS}/${sha(n)}`, 20 * DAY * 1000);
    const summary = await fx.run({ budgetMs: 2500 });
    assert.deepEqual([summary.blobsDeleted, summary.budgetCut], [1, true]);
  },

  'local retention: a ticket accepted longer ago than ledgerRetentionMs loses its ledger entry and pull directory, unless an attempt exists; a younger one and the floor are respected': async (load, w, t) => {
    const fx = await gcFixture(t, w, load, { cfg: { blobGc: false } });
    const day = 24 * 3_600_000;
    const entry = (n, ageDays) => ledgerAccept(fx.state, { logicalId: tid(n), decision: { kind: 'lost', exit: 75, reason: 'x' }, epoch: null, generation: 0, acceptedAt: Date.now() - ageDays * day });
    const dir = (n) => pullTicketDir(fx.state, tid(n));
    for (const [n, age] of [[1, 50], [2, 10], [3, 50]]) {
      entry(n, age);
      fs.mkdirSync(path.join(dir(n), 'stage', 'x'), { recursive: true });
    }
    await createAttempt(fx.state, tid(3));
    for (const [n, age] of [[4, 50], [5, 1]]) {
      fs.mkdirSync(dir(n), { recursive: true });
      const when = new Date(Date.now() - age * day);
      fs.utimesSync(dir(n), when, when);
    }
    const summary = await fx.run();
    assert.equal(summary.ledgersExpired, 2, 'the old ticket and the old unowned directory');
    assert.deepEqual([1, 2, 3].map((n) => readLedger(fx.state, tid(n)) !== null), [false, true, true]);
    assert.deepEqual([1, 2, 3, 4, 5].map((n) => fs.existsSync(dir(n))), [false, true, true, false, true]);
    const low = await gcFixture(t, w, load, { cfg: { ledgerRetentionMs: 1000 } });
    await assert.rejects(low.run(), /below the .*queue backup retention/);
  },

  'a published result drops its staged log and artifact copies, and a crash before the drop is finished by the next step': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    const artifacts = artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"a":1}' });
    await fx.runner.finish(await fx.runner.putOutputs({ log: 'out\n', artifacts }));
    const stage = path.join(pullTicketDir(fx.state, fx.id), 'stage');
    fx.hooks.afterLedger = () => { throw new Error('killed'); };
    await assert.rejects(fx.step(), /killed/);
    assert.ok(fs.existsSync(stage), 'staged before the publish');
    delete fx.hooks.afterLedger;
    assert.equal((await fx.step()).state, 'settled');
    assert.equal(fs.existsSync(stage), false);
    fs.mkdirSync(path.join(stage, 'again'), { recursive: true });
    assert.equal((await fx.step()).already, true);
    assert.equal(fs.existsSync(stage), false, 'an already settled ticket is cleaned too');
  },

  'rows_after derives from lanePull.rowRetentionMs and a smaller value is refused': async (load, w, t) => {
    const fx = await gcFixture(t, w, load, { cfg: { rowRetentionMs: 3 * DAY * 1000 } });
    const [old, recent] = [await fx.terminal(1, 4 * DAY), await fx.terminal(2, 2 * DAY)];
    await assert.rejects(fx.run({ rowsAfterMs: 2 * DAY * 1000 }), /below lanePull\.rowRetentionMs/);
    assert.deepEqual(await fx.rows(), [old, recent], 'the refusal happens before anything is deleted');
    const summary = await fx.run();
    assert.equal(summary.rowsPruned, 1, 'the 3 day config value, not a hard-coded 7 days, decides which row is due');
    assert.deepEqual(await fx.rows(), [recent]);
    assert.equal((await fx.run({ rowsAfterMs: 3 * DAY * 1000 })).rowsPruned, 0);
  },
};

const ORDER = 'T18: objects are deleted before the rows, a failing delete leaves its row for the next run, and orphan chunks go last';
const BLOBS = 'T18: a blob referenced by a non-terminal ticket survives, an unreferenced or only terminal-referenced old blob goes, and the live set is exact';

const CANARIES = [
  [ORDER, 'pull-gc.js', ['const due = page.items.filter((item) => item.row_due && cleaned.has(item.id));', 'const due = page.items.filter((item) => item.row_due);']],
  [ORDER, 'pull-gc.js', ['summary.chunksPruned = await queue.pruneChunks({ terminalAfterMs: CHUNKS_TERMINAL_AFTER_MS, limit: CHUNKS_PER_RUN });', 'summary.chunksPruned = 0;']],
  [BLOBS, 'pull-gc.js', ['for (const { key } of batch.filter((c) => !live.has(c.key))) {', 'for (const { key } of batch) {']],
  [BLOBS, 'pull-gc.js', ['if (settings.blobGc) await sweepBlobs(ctx, summary);', 'await sweepBlobs(ctx, summary);']],
  ['V4-3: a blob re-PUT after gc chose it survives, because gc looks at its age again before the delete', 'pull-gc.js', ['if (!head.exists || !isOldBlob(head.lastModified, ctx.now())) continue;', 'if (!head.exists) continue;']],
  ['T31: a ticket queued for 40 days keeps its stream through every run, a terminal ticket loses its objects after 24 hours only', 'pull-gc.js', ['`streams/${id}-`]]', "'streams/']]"]],
  ['T35a: with every ticket object-due and row-young, every page is row_due false: objects cleaned, no row pruned, the row cursor stays at the start', 'pull-gc.js', ['    after = page.next;\n', '    after = state.rowsFrom;\n']],
  ['T35d: a run stops when its budget is spent after k pages, and the next run continues from the persisted cursors without re-cleaning them', 'pull-gc.js', ['const cut = !finished && overBudget(ctx);', 'const cut = false;']],
  ['T35d: a run stops when its budget is spent after k pages, and the next run continues from the persisted cursors without re-cleaning them', 'pull-gc.js', ['  atomicWriteJson(gcStateFile(root), state);\n', '']],
  ['T35b: older row-due pages are pruned and later young pages are not, the next run resumes after the pruned ones with no store call', 'pull-gc.js', ['const blockedAt = pruned < due.length ? 0 : page.items.findIndex((item) => !due.includes(item));', 'const blockedAt = -1;']],
  ['a failed delete on one page never lets a later clean page advance objectsDone past it: its objects are cleaned before its row goes', 'pull-gc.js', ['  const cleaned = new Set();\n  for (const item of items) {', '  const cleaned = new Set();\n  ctx.prefixClean = true;\n  for (const item of items) {']],
  ['the blob sweep stops at the run budget like the ticket sweep', 'pull-gc.js', ['      if (overBudget(ctx)) {\n        summary.budgetCut = true;\n        return;\n      }\n      await ctx.hooks.beforeBlobDelete', '      await ctx.hooks.beforeBlobDelete']],
  ['T35d: a run stops when its budget is spent after k pages, and the next run continues from the persisted cursors without re-cleaning them', 'pull-gc.js', ['state.resume = cut ? page.next : null;', 'state.resume = null;']],
  ['local retention: a ticket accepted longer ago than ledgerRetentionMs loses its ledger entry and pull directory, unless an attempt exists; a younger one and the floor are respected', 'pull-gc.js', ['if (readAttempt(root, id) || now - (entry.acceptedAt ?? now) < retentionMs) continue;', 'if (readAttempt(root, id)) continue;']],
  ['local retention: a ticket accepted longer ago than ledgerRetentionMs loses its ledger entry and pull directory, unless an attempt exists; a younger one and the floor are respected', 'pull-gc.js', ['if (readAttempt(root, id) || now - (entry.acceptedAt ?? now) < retentionMs) continue;', 'if (now - (entry.acceptedAt ?? now) < retentionMs) continue;']],
  ['local retention: a ticket accepted longer ago than ledgerRetentionMs loses its ledger entry and pull directory, unless an attempt exists; a younger one and the floor are respected', 'pull-gc.js', [' || ageOf(path.join(pullDir, name), now) < retentionMs', '']],
  ['local retention: a ticket accepted longer ago than ledgerRetentionMs loses its ledger entry and pull directory, unless an attempt exists; a younger one and the floor are respected', 'pull-gc.js', ['if (settings.ledgerRetentionMs < LEDGER_RETENTION_FLOOR_MS) throw', 'if (false) throw']],
  ['a published result drops its staged log and artifact copies, and a crash before the drop is finished by the next step', 'pull-ticket.js', ['  dropStaged(run);\n  return settled;', '  return settled;']],
  ['a published result drops its staged log and artifact copies, and a crash before the drop is finished by the next step', 'pull-ticket.js', ['    dropStaged(run);\n    return { state: \'settled\', already: true', '    return { state: \'settled\', already: true']],
  ['rows_after derives from lanePull.rowRetentionMs and a smaller value is refused', 'pull-gc.js', ['return requestedMs ?? settings.rowRetentionMs;', 'return requestedMs ?? 7 * 24 * HOUR_MS;']],
  ['rows_after derives from lanePull.rowRetentionMs and a smaller value is refused', 'pull-gc.js', ['if (requestedMs !== undefined && requestedMs < settings.rowRetentionMs) {', 'if (false) {']],
];

definePullSuite('lane pull gc against the real retention SQL and the S3 stub', SCENARIOS, CANARIES);
