import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { readAttempt } from '../src/attempts.js';
import { readEnvelope } from '../src/pull-envelope.js';
import { readLedger } from '../src/pull-ledger.js';
import { S3Error } from '../src/s3-client.js';
import { QueueError } from '../src/queue-client.js';
import { sha256Hex } from '../src/s3-presign.js';
import { definePullSuite, pullFixture, artifactsStream, ARTIFACT_PATH, UNPROVEN_CANCEL_ROW } from './helpers/pull-fixture.js';
import { simulateRestore, tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R5: the step machine against a disposable queue, a stub runner and the S3 stub (T1, T3, T4-T6, T8, T12, T16, T23, T27-T29). */

const LOST_RESPONSE = new Error('response lost');
const HOUR = 3_600_000;
const rowCount = async (fx) => (await (await fx.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets WHERE id = $1', [fx.id])).rows[0].n;
const deleteRow = async (fx) => (await fx.su()).query('DELETE FROM lane_dispatch.tickets WHERE id = $1', [fx.id]);
const next = (t, w, load, n, first) => pullFixture(t, w, { load, id: tid(n), stub: first?.stub });
const withTimeout = (promise, ms) => Promise.race([promise, new Promise((_, reject) => setTimeout(() => reject(new Error(`still running after ${ms}ms`)), ms))]);

const BAD_BINDING = { manifestHash: 'not-the-manifest', generation: 7, protocol: 2, ticketId: tid(99) };

const SCENARIOS = {
  'T3: a lost enqueue response is replayed with the same id and leaves exactly one row': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.prepare();
    const committing = { ...fx.queue, enqueueFenced: async (args) => { await fx.queue.enqueueFenced(args); throw LOST_RESPONSE; } };
    await assert.rejects(fx.step({ queue: committing }), LOST_RESPONSE);
    assert.equal(await rowCount(fx), 1, 'the send did commit');
    assert.deepEqual(await fx.step(), { state: 'waiting', next: 0 });
    assert.equal(await rowCount(fx), 1);
    assert.equal((await fx.row()).id, fx.id);
    assert.equal((await fx.step()).state, 'queued');
  },

  'T1: a withdraw proves only a committed queued -> withdrawn; every other answer leaves the ticket stepping': async (load, w, t) => {
    const queued = await next(t, w, load, 1);
    await queued.enqueue();
    assert.deepEqual(await queued.withdraw(), { proof: true });
    assert.equal(queued.pull().withdrawProof, true);
    assert.equal((await queued.row()).state, 'withdrawn');

    const committed = await next(t, w, load, 2, queued);
    await committed.enqueue();
    const droppedAfterCommit = { ...committed.queue, withdraw: async (id) => { await committed.queue.withdraw(id); throw LOST_RESPONSE; } };
    assert.deepEqual(await committed.withdraw({ queue: droppedAfterCommit }), { proof: true }, 'a dropped response is settled by re-reading the committed row');

    const notCommitted = await next(t, w, load, 3, queued);
    await notCommitted.enqueue();
    const droppedBeforeCommit = { ...notCommitted.queue, withdraw: async () => { throw LOST_RESPONSE; } };
    assert.deepEqual(await notCommitted.withdraw({ queue: droppedBeforeCommit }), { proof: false, state: 'queued' });
    assert.equal(notCommitted.pull().withdrawProof, undefined);
    assert.deepEqual(await notCommitted.withdraw(), { proof: true });

    const held = await next(t, w, load, 4, queued);
    await held.enqueue();
    await held.runner.claim();
    assert.deepEqual(await held.withdraw(), { proof: false, state: 'claimed' });
    await held.runner.start();
    assert.deepEqual(await held.withdraw(), { proof: false, state: 'running' });
    await held.setRow("state = 'lost'");
    assert.deepEqual(await held.withdraw(), { proof: false, state: 'lost' });
    assert.equal(held.pull().withdrawProof, undefined);

    const missing = await next(t, w, load, 5, queued);
    await missing.enqueue();
    await deleteRow(missing);
    assert.deepEqual(await missing.withdraw(), { proof: false, state: 'missing' });
  },

  'T4: a result that does not bind to the ticket settles lost 75 result-unbound and installs nothing': async (load, w, t) => {
    let first;
    let n = 0;
    for (const [field, bad] of Object.entries(BAD_BINDING)) {
      const fx = await next(t, w, load, ++n, first);
      first ??= fx;
      await fx.hold();
      const artifacts = artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"ok":1}' });
      await fx.runner.finish({ ...(await fx.runner.putOutputs({ log: 'out\n', artifacts })), [field]: bad });
      const settled = await fx.step();
      assert.equal(settled.state, 'settled', field);
      assert.deepEqual([settled.decision.kind, settled.decision.reason, settled.result.exit], ['lost', 'result-unbound', 75], field);
      assert.equal(fs.existsSync(path.join(fx.worktreeRoot, ARTIFACT_PATH)), false, `${field}: nothing was installed`);
    }
  },

  'T5: a log object that does not match its hash is not used: the streamed log stands, a warning is recorded, the exit is unchanged': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.log('good\n');
    await fx.step();
    await fx.runner.put('log', Buffer.from('evil\n'));
    await fx.runner.finish({ logSha256: sha256Hex('good\n'), logBytes: 5 });
    const settled = await fx.step();
    assert.equal(settled.result.exit, 0);
    assert.match(settled.result.remoteLogWarning, /does not match/);
    assert.equal(fs.readFileSync(fx.logPath, 'utf8'), 'good\n');
    assert.ok(fx.stub.requests.some((r) => r.method === 'GET' && r.path.endsWith('/log')), 'the object really was fetched and compared');
  },

  'T6: an artifacts object that does not match its hash installs nothing and records a warning': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    const declared = artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"good":1}' });
    await fx.runner.put('artifacts', artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"evil":1}' }));
    await fx.runner.finish({ artifactsSha256: sha256Hex(declared), artifactsBytes: declared.length });
    const settled = await fx.step();
    assert.equal(settled.result.exit, 0);
    assert.match(settled.result.remoteArtifactsWarning, /does not match/);
    assert.equal(fs.existsSync(path.join(fx.worktreeRoot, ARTIFACT_PATH)), false);
  },

  'T8: a running row the reaper marks lost settles 75 runner-lost, while a queued row of the same age is untouched however long it waits': async (load, w, t) => {
    const lostFx = await pullFixture(t, w, { load });
    await lostFx.hold();
    const queuedFx = await next(t, w, load, 2, lostFx);
    await queuedFx.enqueue();
    const reap = async () => (await w.su()).query("SELECT lane_dispatch.ld_reap(interval '120 seconds', interval '2 seconds', interval '4 seconds')");
    await reap();
    await lostFx.setRow("lease_until = now() - interval '1 hour'");
    await queuedFx.setRow("created_at = now() - interval '1 hour'");
    await reap();
    assert.equal((await lostFx.row()).state, 'lost');
    assert.equal((await queuedFx.row()).state, 'queued');

    const settled = await lostFx.step();
    assert.deepEqual([settled.decision?.reason, settled.result?.exit, settled.result?.reason], ['runner-lost', 75, 'runner-lost']);
    queuedFx.clock.now += 24 * HOUR;
    assert.equal((await queuedFx.step()).state, 'queued', 'a queued ticket has no timeout of any kind');
    assert.equal((await queuedFx.row()).state, 'queued');
  },

  'T12: a row that vanished after the enqueue settles lost 75 queue-row-missing; with an acceptance the accepted result is published': async (load, w, t) => {
    const gone = await pullFixture(t, w, { load });
    await gone.enqueue();
    await deleteRow(gone);
    const settled = await gone.step();
    assert.deepEqual([settled.state, settled.decision?.reason, settled.result?.exit], ['settled', 'queue-row-missing', 75]);

    const accepted = await next(t, w, load, 2, gone);
    await accepted.hold();
    await accepted.runner.finish();
    accepted.hooks.afterLedger = () => { throw LOST_RESPONSE; };
    await assert.rejects(accepted.step(), LOST_RESPONSE);
    delete accepted.hooks.afterLedger;
    await deleteRow(accepted);
    const published = await accepted.step();
    assert.deepEqual([published.decision.kind, published.result.exit], ['finished', 0], 'the ledger, not the vanished row, decides');
  },

  'T23: an enqueue that committed, a restore, and a restart settle lost 75 enqueue-crossed-restore and insert nothing': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.prepare();
    const committing = { ...fx.queue, enqueueFenced: async (args) => { await fx.queue.enqueueFenced(args); throw LOST_RESPONSE; } };
    await assert.rejects(fx.step({ queue: committing }), LOST_RESPONSE);
    await fx.runner.claim();
    await fx.runner.start();
    await deleteRow(fx);
    await simulateRestore(w);
    const settled = await fx.step();
    assert.deepEqual([settled.state, settled.decision?.reason, settled.result?.exit], ['settled', 'enqueue-crossed-restore', 75]);
    assert.equal(await rowCount(fx), 0, 'no new runnable row for that id');
  },

  'T27: URLs are per epoch: a reclaimed ticket gets fresh ones, and the stale epoch keys stay out of the current epoch': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.claimed();
    const stale = fx.runner.urls;
    assert.equal(stale.epoch, 1);
    await fx.runner.release('never-started');
    await fx.runner.claim();
    assert.equal(fx.runner.claimed.epoch, 2);
    assert.equal((await fx.runner.heartbeat()).urls, null, 'the queue withholds the epoch-1 URLs from the epoch-2 claim');
    await fx.step();
    const fresh = (await fx.runner.heartbeat()).urls;
    assert.equal(fresh?.epoch, 2, 'the supervisor attached URLs for the new epoch');
    await fx.runner.start();
    const { nonce } = readEnvelope(fx.state, fx.id);
    await fx.runner.putOutputs({ log: 'epoch two\n' });
    assert.ok(fx.stub.objects.has(`lane-out/${fx.id}/2/${nonce}/log`));
    assert.ok(!fx.stub.objects.has(`lane-out/${fx.id}/1/${nonce}/log`));
    assert.ok(new URL(stale.log).pathname.includes(`/${fx.id}/1/`), 'a runner still holding the old URLs writes only to epoch 1 keys');
  },

  'T28: a death before the snapshot descriptor settles lost prepare-interrupted; after it the enqueue continues from the envelope, not the worktree': async (load, w, t) => {
    const crash = new Error('killed');
    const marker = await pullFixture(t, w, { load });
    marker.hooks.afterMarker = () => { throw crash; };
    await assert.rejects(marker.prepare(), crash);
    assert.deepEqual(await marker.step(), { state: 'preparing', next: 15_000 }, 'a live preparer is never declared interrupted');
    assert.equal(readLedger(marker.state, marker.id), null);
    await marker.killSupervisor();
    const first = await marker.step();
    assert.deepEqual([first.state, first.decision?.reason, first.result?.exit], ['settled', 'prepare-interrupted', 75]);
    assert.equal(await rowCount(marker), 0);

    const upload = await next(t, w, load, 2, marker);
    const failingSpool = { envelopeFields: upload.envelopeFields, worktreeRoot: upload.worktreeRoot, manifest: upload.manifest, uploadOptions: { openSpool: () => { throw crash; } } };
    await assert.rejects(upload.prepare({ prepare: failingSpool }), crash);
    assert.ok(readEnvelope(upload.state, upload.id), 'the envelope exists but holds no snapshot');
    assert.equal((await upload.step()).state, 'preparing');
    await upload.killSupervisor();
    assert.equal((await upload.step()).decision?.reason, 'prepare-interrupted');
    assert.equal(await rowCount(upload), 0);

    const described = await next(t, w, load, 3, marker);
    described.hooks.afterSnapshot = () => { throw crash; };
    await assert.rejects(described.prepare(), crash);
    delete described.hooks.afterSnapshot;
    fs.writeFileSync(path.join(described.worktreeRoot, 'a.txt'), 'changed since the manifest\n');
    await described.step();
    const envelope = readEnvelope(described.state, described.id);
    const row = await described.row();
    assert.deepEqual(row.snapshot, envelope.snapshot);
    assert.deepEqual(row.header, envelope.header);
    assert.equal(row.binding.manifestHash, described.manifest.manifestHash);
    assert.equal((await described.step()).state, 'queued');
  },

  'T29: a stalled object fetch holds only its own ticket: another ticket\'s URL attach lands first, and the stalled one settles with a warning after 5 passes': async (load, w, t) => {
    const stalled = await pullFixture(t, w, { load });
    await stalled.hold();
    const other = await next(t, w, load, 2, stalled);
    await other.enqueue();
    await other.runner.claim();
    await stalled.runner.finish(await stalled.runner.putOutputs({ log: 'out\n' }));
    stalled.stub.hangMatching = (p) => p.includes(stalled.id) && p.endsWith('/log');

    const done = {};
    const stalledStep = (over) => stalled.step({ objectDeadlineMs: 300, ...over }).then((result) => { done.stalled = Date.now(); return result; });
    const [first, attached] = await withTimeout(Promise.all([stalledStep(), other.step().then((r) => { done.other = Date.now(); return r; })]), 5000);
    assert.equal(first.state, 'fetching');
    assert.equal(attached.state, 'claimed');
    assert.ok((await other.row()).urls, 'the other ticket got its URLs');
    assert.ok(done.other < done.stalled, 'the other ticket was not delayed by the stalled fetch');

    for (let pass = 2; pass < 5; pass++) assert.equal((await stalledStep()).state, 'fetching', `pass ${pass}`);
    const settled = await stalledStep();
    assert.equal(settled.state, 'settled');
    assert.equal(settled.result.exit, 0);
    assert.match(settled.result.remoteLogWarning, /not retrieved after 5 passes/);
  },
};

SCENARIOS['a bound rejected result settles lost 75 with no second enqueue: snapshot-blob-missing for a cache reason, runner-rejected otherwise'] = async (load, w, t) => {
  const cases = [['cache-miss: 1', 'snapshot-blob-missing'], ['cache-corrupt: 2', 'snapshot-blob-missing'], ['no space left', 'runner-rejected']];
  let first;
  for (const [n, [reason, expected]] of cases.entries()) {
    const fx = await next(t, w, load, n + 1, first);
    first ??= fx;
    let sends = 0;
    const counting = { ...fx.queue, enqueueFenced: async (args) => { sends += 1; return fx.queue.enqueueFenced(args); } };
    await fx.prepare();
    await fx.step({ queue: counting });
    await fx.runner.claim();
    await fx.step({ queue: counting });
    await fx.runner.heartbeat();
    await fx.runner.start();
    await fx.runner.finish({ kind: 'rejected', reason });
    const settled = await fx.step({ queue: counting });
    assert.deepEqual([settled.decision.reason, settled.result.exit], [expected, 75], reason);
    assert.equal(sends, 1, 'no replacement row is enqueued');
    assert.equal(await rowCount(fx), 1);
  }
};

SCENARIOS['runPullTicket prepares, waits out a queue failure and returns the confirmed outcome'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  let failures = 0;
  const flaky = { ...fx.queue, get: async (id) => { if (failures++ === 0) throw new QueueError('get failed: connection terminated', { code: 'ECONNRESET', cause: new Error('reset') }); return fx.queue.get(id); } };
  const sleeps = [];
  const drive = async (ms) => {
    sleeps.push(ms);
    const row = await fx.row();
    if (row?.state === 'queued') await fx.runner.claim();
    else if (row?.state === 'claimed' && row.urls) {
      await fx.runner.heartbeat();
      await fx.runner.start();
      await fx.runner.finish();
    }
  };
  const outcome = await withTimeout(fx.run({ queue: flaky, sleep: drive, prepare: { envelopeFields: fx.envelopeFields, worktreeRoot: fx.worktreeRoot, manifest: fx.manifest } }), 20_000);
  assert.deepEqual([outcome.outcome, outcome.exitCode, outcome.already], ['confirmed', 0, false]);
  assert.ok(failures > 1, 'the failed read was retried, not surfaced');
  assert.ok(sleeps.includes(15_000), 'the retry waited a poll interval');
};

SCENARIOS['a snapshot object that vanished before the first send settles lost snapshot-missing and sends nothing'] = async (load, w, t) => {
  const beforeSend = await pullFixture(t, w, { load });
  await beforeSend.prepare();
  beforeSend.stub.objects.delete(`lane-snap/${readEnvelope(beforeSend.state, beforeSend.id).snapshot.key}`);
  const first = await beforeSend.step();
  assert.deepEqual([first.decision?.reason, first.result?.exit], ['snapshot-missing', 75]);
  assert.equal(await rowCount(beforeSend), 0, 'nothing was sent');

};

SCENARIOS['enqueueOnce sends nothing once a decision is ledgered'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.prepare();
  const { enqueueOnce: enqueueFrom } = await load('pull-enqueue.js');
  const outcome = await enqueueFrom({ root: fx.state, id: fx.id, generation: fx.generation, queue: fx.queue, ledgered: () => true });
  assert.deepEqual(outcome, { outcome: 'settled' });
  assert.equal(await rowCount(fx), 0);
};

const sqlError = (code) => new QueueError('ld_get failed', { code, cause: Object.assign(new Error('server said no'), { severity: 'ERROR', code }) });
const transportError = new QueueError('ld_get failed: connection reset', { code: 'ECONNRESET', cause: new Error('reset') });
const STOP = new Error('stopped waiting');
const failingGet = (fx, err) => ({ ...fx.queue, get: async () => { throw err; } });
const retriesThenStops = (limit = 3) => { const sleeps = []; return { sleeps, sleep: async (ms) => { sleeps.push(ms); if (sleeps.length >= limit) throw STOP; } }; };
const mustNotWait = async () => { assert.fail('the run waited on an error that will answer the same way again'); };

SCENARIOS['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green'] = async (load, w, t) => {
  let first;
  let n = 0;
  for (const code of ['23505', '42501', '22023']) {
    const fx = await next(t, w, load, ++n, first);
    first ??= fx;
    await fx.enqueue();
    const outcome = await fx.run({ queue: failingGet(fx, sqlError(code)), sleep: mustNotWait });
    assert.deepEqual([outcome.outcome, outcome.reason, outcome.code], ['unresolved', 'pull-error', code]);
    assert.match(readAttempt(fx.state, fx.id).unresolved, /^pull-error/);
    assert.equal(readLedger(fx.state, fx.id), null);
    assert.equal(fx.published(), null);
  }
  const waiting = await next(t, w, load, ++n, first);
  await waiting.enqueue();
  for (const err of [sqlError('53300'), sqlError('57P01'), sqlError('08006'), transportError]) {
    const { sleeps, sleep } = retriesThenStops();
    await assert.rejects(waiting.run({ queue: failingGet(waiting, err), sleep }), STOP, String(err.code));
    assert.deepEqual(sleeps, [15_000, 15_000, 15_000], `${err.code} was waited out`);
  }
  for (const code of ['57014', '55P03', '40P01', '40001', '57P02', '57P03']) {
    const { sleeps, sleep } = retriesThenStops();
    await assert.rejects(waiting.run({ queue: failingGet(waiting, sqlError(code)), sleep }), STOP, code);
    assert.equal(sleeps.length, 3, `${code} is a retryable server answer`);
  }
  const tlsFailures = [
    new QueueError('ld_get failed: self signed', { code: 'DEPTH_ZERO_SELF_SIGNED_CERT', cause: Object.assign(new Error('self signed'), { code: 'DEPTH_ZERO_SELF_SIGNED_CERT' }) }),
    new QueueError('ld_get failed: altname', { code: 'ERR_TLS_CERT_ALTNAME_INVALID', cause: Object.assign(new Error('altname'), { code: 'ERR_TLS_CERT_ALTNAME_INVALID' }) }),
  ];
  for (const err of tlsFailures) assert.equal((await waiting.run({ queue: failingGet(waiting, err), sleep: mustNotWait })).outcome, 'unresolved', String(err.code));
  const denied = await next(t, w, load, ++n, first);
  await denied.prepare();
  const refused = { ...denied.s3, headObject: async () => { throw new S3Error('s3 HEAD answered 403', { status: 403, code: 'AccessDenied' }); } };
  assert.equal((await denied.run({ s3: refused, sleep: mustNotWait })).outcome, 'unresolved');
  const expired = new S3Error('s3 HEAD failed: CERT_HAS_EXPIRED', { code: 'Network', cause: new TypeError('fetch failed', { cause: Object.assign(new Error('expired'), { code: 'CERT_HAS_EXPIRED' }) }) });
  assert.equal((await denied.run({ s3: { ...denied.s3, headObject: async () => { throw expired; } }, sleep: mustNotWait })).outcome, 'unresolved', 'a TLS failure wrapped in an S3Error is permanent');
  for (const err of [new S3Error('s3 HEAD answered 503', { status: 503 }), new S3Error('s3 HEAD timed out', { code: 'Timeout' })]) {
    const { sleep } = retriesThenStops(2);
    await assert.rejects(denied.run({ s3: { ...denied.s3, headObject: async () => { throw err; } }, sleep }), STOP);
  }
};

SCENARIOS['a log that never drains is bounded: the result is ledgered after 5 passes and the gap hold, with a log warning'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  await fx.runner.finish();
  let seq = 0;
  const endless = { ...fx.queue, logRead: async () => [{ seq: seq++, data: Buffer.from('x\n').toString('base64') }] };
  let settled;
  let passes = 0;
  while (settled === undefined && passes < 20) {
    passes += 1;
    fx.clock.now += 1_000;
    const out = await fx.step({ queue: endless });
    if (out.state === 'settled') settled = out;
    else assert.equal(out.state, 'draining-log');
  }
  assert.ok(settled, 'the terminal result was never ledgered');
  assert.equal(passes, 13, 'never before the gap hold has had its 10 s plus a retry interval, however many passes');
  assert.match(settled.result.remoteLogWarning, /not fully drained after 5 passes/);
  assert.equal(settled.result.exit, 0);
};

SCENARIOS['a running row never needs the stream: a vanished stream is ignored and the refreshed URLs carry none'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  const before = (await fx.row()).urls;
  assert.ok(before.stream, 'the claimed attach carried the stream');
  fx.stub.objects.delete(`lane-snap/${readEnvelope(fx.state, fx.id).snapshot.key}`);
  fx.clock.now += 20 * 60_000;
  assert.equal((await fx.step()).state, 'running');
  const after = (await fx.row()).urls;
  assert.deepEqual([after.stream, after.blobs, after.epoch], [undefined, undefined, 1]);
  assert.ok(after.expiresAt > before.expiresAt, 'the URLs were refreshed');
  assert.equal((await fx.row()).cancel_requested, false);
  assert.equal(fx.published(), null);
};

SCENARIOS['a claimed row with a vanished stream is cancelled durably and settles lost only once it leaves the held state, never while it may run'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.enqueue();
  await fx.runner.claim();
  fx.stub.objects.delete(`lane-snap/${readEnvelope(fx.state, fx.id).snapshot.key}`);
  assert.equal((await fx.step()).state, 'claimed');
  assert.equal((await fx.row()).cancel_requested, true);
  assert.ok(fx.pull().snapshotMissing, 'the finding is durable');
  assert.equal(readLedger(fx.state, fx.id), null, 'nothing is ledgered while the row may still run');
  assert.equal((await fx.runner.heartbeat()).cancel, true);
  await fx.runner.release('never-started');
  const settled = await fx.step();
  assert.deepEqual([settled.decision.reason, settled.result.exit], ['snapshot-missing', 75]);

  const raced = await next(t, w, load, 2, fx);
  await raced.enqueue();
  await raced.runner.claim();
  raced.stub.objects.delete(`lane-snap/${readEnvelope(raced.state, raced.id).snapshot.key}`);
  await raced.step();
  await raced.runner.finish();
  assert.deepEqual([(await raced.step()).decision.kind, raced.published().exit], ['finished', 0], 'a run that happened anyway is not discarded');

  const stuck = await next(t, w, load, 3, fx);
  await stuck.enqueue();
  await stuck.runner.claim();
  stuck.stub.objects.delete(`lane-snap/${readEnvelope(stuck.state, stuck.id).snapshot.key}`);
  await stuck.step();
  stuck.clock.now += 31 * 60_000;
  const unresolved = await stuck.step();
  assert.deepEqual([unresolved.state, unresolved.exitCode], ['unresolved', 1]);
  assert.equal(readLedger(stuck.state, stuck.id), null, 'a bounded wait is still not lost while it may run');
};

SCENARIOS['a stream that vanished while the agent started anyway never publishes 75: an unproven cancel stays unresolved'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.enqueue();
  await fx.runner.claim();
  fx.stub.objects.delete(`lane-snap/${readEnvelope(fx.state, fx.id).snapshot.key}`);
  let started = false;
  const racing = { ...fx.queue, get: async (id) => {
    const row = await fx.queue.get(id);
    if (!started) { started = true; await fx.runner.start(); }
    return row;
  } };
  assert.equal((await fx.step({ queue: racing })).state, 'claimed', 'the row read claimed; the agent started behind it');
  assert.equal((await fx.row()).state, 'running');
  assert.equal((await fx.row()).cancel_requested, true);
  await fx.runner.finishCancelled(false);
  const out = await fx.step();
  assert.deepEqual([out.state, out.exitCode], ['unresolved', 1]);
  assert.equal(readLedger(fx.state, fx.id), null, 'not lost while the process may be alive');
  assert.equal(fx.published(), null);
};

SCENARIOS['runPullTicket ends the run when a step is unresolved instead of looping on a renewed lease'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.enqueue();
  await fx.runner.claim();
  fx.stub.objects.delete(`lane-snap/${readEnvelope(fx.state, fx.id).snapshot.key}`);
  let sleeps = 0;
  const sleep = async () => {
    if (++sleeps > 4) assert.fail('the run kept waiting on an unresolved step');
    fx.clock.now += 31 * 60_000;
  };
  const outcome = await fx.run({ sleep });
  assert.deepEqual([outcome.outcome, outcome.reason, outcome.exitCode], ['unresolved', 'snapshot-missing', 1]);
};

const cancelFixture = async (t, w, load, n, first) => {
  const fx = await next(t, w, load, n, first);
  fx.cancelCalls = 0;
  const counting = { ...fx.queue, cancel: async (id) => { fx.cancelCalls += 1; return fx.queue.cancel(id); } };
  fx.cancelStep = () => fx.step({ queue: counting, cancelRequested: () => true });
  return fx;
};

SCENARIOS['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, an operator-cancelled row stays unproven'] = async (load, w, t) => {
  const queued = await cancelFixture(t, w, load, 1);
  await queued.enqueue();
  const queuedOut = await queued.cancelStep();
  assert.deepEqual([queuedOut.decision.kind, queuedOut.result.exit, queuedOut.result.remoteCancelConfirmed], ['cancelled', 130, true]);

  const released = await cancelFixture(t, w, load, 2, queued);
  await released.enqueue();
  await released.runner.claim();
  assert.equal((await released.cancelStep()).state, 'claimed');
  await released.runner.release('never-started');
  assert.equal((await released.row()).result, null);
  assert.deepEqual([(await released.step()).decision.kind, released.published().exit], ['cancelled', 130]);

  const confirmed = await cancelFixture(t, w, load, 3, queued);
  await confirmed.hold();
  assert.equal((await confirmed.cancelStep()).state, 'running');
  assert.equal((await confirmed.runner.heartbeat()).cancel, true);
  await confirmed.runner.finishCancelled(true);
  assert.deepEqual([(await confirmed.step()).decision.kind, confirmed.published().exit], ['cancelled', 130]);

  const raced = await cancelFixture(t, w, load, 4, queued);
  await raced.hold();
  await raced.cancelStep();
  await raced.runner.finish();
  assert.deepEqual([(await raced.step()).decision.kind, raced.published().exit], ['finished', 0], 'a run that completed is never turned into 130');

  const quarantined = await cancelFixture(t, w, load, 5, queued);
  await quarantined.hold();
  await quarantined.runner.finishCancelled(false);
  const unproven = await quarantined.cancelStep();
  assert.deepEqual([unproven.state, unproven.exitCode], ['unresolved', 1]);
  assert.ok(readAttempt(quarantined.state, quarantined.id), 'the attempt is kept');
  assert.equal(quarantined.published(), null);
  quarantined.clock.now += 31 * 60_000;
  const timedOut = await quarantined.cancelStep();
  assert.deepEqual([timedOut.decision.reason, quarantined.published().exit], ['cancel-unproven', 75]);
  assert.equal(quarantined.cancelCalls, 2, 'the unproven settle sent one more best-effort cancel');

  const lostFirst = await cancelFixture(t, w, load, 6, queued);
  await lostFirst.hold();
  await lostFirst.setRow(UNPROVEN_CANCEL_ROW);
  const droppedAfterCommit = { ...lostFirst.queue, cancel: async (id) => { lostFirst.cancelCalls += 1; await lostFirst.queue.cancel(id); throw LOST_RESPONSE; } };
  await assert.rejects(lostFirst.step({ queue: droppedAfterCommit, cancelRequested: () => true }), LOST_RESPONSE);
  assert.equal((await lostFirst.cancelStep()).state, 'unresolved', 'the lost first response never upgrades to confirmed');
  assert.equal((await lostFirst.cancelStep()).state, 'unresolved');
  assert.equal(lostFirst.cancelCalls, 2, 'the first answer is persisted, so the cancel is not sent again');
};

const CANARIES = [
  ['a stream that vanished while the agent started anyway never publishes 75: an unproven cancel stays unresolved', 'pull-ticket.js', ["if (pull?.snapshotMissing && decision.kind === 'cancelled') decision", "if (pull?.snapshotMissing && (decision.kind === 'cancelled' || decision.kind === 'unproven')) decision"]],
  ['runPullTicket ends the run when a step is unresolved instead of looping on a renewed lease', 'pull-ticket.js', ["    if (step.state === 'unresolved') return { outcome: 'unresolved', reason: step.reason, exitCode: step.exitCode };\n", '']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ["['53300', '57P01', '57P02', '57P03', '57014', '55P03', '40P01', '40001']", "['53300', '57P01']"]],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ['  if (tlsFailure(err)) return false;\n', '']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ['  return err.cause !== undefined && typeof err.cause.severity !== \'string\';', '  return true;']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ['return err.status === null || err.status >= 500 || err.status === 408 || err.status === 429;', 'return true;']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ["if (TRANSIENT_SQLSTATES.has(code) || code.startsWith('08')) return true;\n", '']],
  ['a log that never drains is bounded: the result is ledgered after 5 passes and the gap hold, with a log warning', 'pull-ticket.js', ['    if (passes < LOG_DRAIN_PASSES || run.now() - drainSince < run.cfg.logGapHoldMs + LOG_DRAIN_RETRY_MS) {', '    if (true) {']],
  ['a log that never drains is bounded: the result is ledgered after 5 passes and the gap hold, with a log warning', 'pull-ticket.js', [' || run.now() - drainSince < run.cfg.logGapHoldMs + LOG_DRAIN_RETRY_MS', '']],
  ['a running row never needs the stream: a vanished stream is ignored and the refreshed URLs carry none', 'pull-ticket.js', ['const head = running ? { exists: true } : await run.s3.headObject(', 'const head = await run.s3.headObject(']],
  ['a claimed row with a vanished stream is cancelled durably and settles lost only once it leaves the held state, never while it may run', 'pull-ticket.js', ["  if (pull?.snapshotMissing && decision.kind === 'cancelled') decision = lost('snapshot-missing');\n", '']],
  ['a claimed row with a vanished stream is cancelled durably and settles lost only once it leaves the held state, never while it may run', 'pull-ticket.js', ['  await requestCancel(run);\n  if (run.now() - since <', '  if (run.now() - since <']],
  ['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, an operator-cancelled row stays unproven', 'pull-ticket.js', ["  await run.queue.cancel(run.id).catch(() => {});\n  await hook(run, 'afterLedger');", "  await hook(run, 'afterLedger');"]],
  ['a bound rejected result settles lost 75 with no second enqueue: snapshot-blob-missing for a cache reason, runner-rejected otherwise', 'pull-ticket.js', ["  if (result.kind === 'rejected' && provesNeverStarted(result, expected)) return lost(CACHE_REJECT_RE.test(result.reason ?? '') ? 'snapshot-blob-missing' : 'runner-rejected', result.reason);\n", '']],
  ['a snapshot object that vanished before the first send settles lost snapshot-missing and sends nothing', 'pull-ticket.js', ["    if (reason) return { outcome: 'lost', reason };\n", '']],
  ['enqueueOnce sends nothing once a decision is ledgered', 'pull-enqueue.js', ["    if (ledgered()) return { outcome: 'settled' };\n", '']],
  ['T3: a lost enqueue response is replayed with the same id and leaves exactly one row', 'pull-enqueue.js', ['    id: envelope.logicalId,', '    id: globalThis.crypto.randomUUID(),']],
  ['T1: a withdraw proves only a committed queued -> withdrawn; every other answer leaves the ticket stepping', 'pull-ticket.js', ["if (answer !== 'withdrawn' && reread?.state !== 'withdrawn')", "if (answer === null && reread?.state !== 'withdrawn')"]],
  ['T4: a result that does not bind to the ticket settles lost 75 result-unbound and installs nothing', 'pull-ticket.js', ["  if (classified.outcome !== 'confirmed') return lost('result-unbound', classified.reason);\n", '']],
  ['T5: a log object that does not match its hash is not used: the streamed log stands, a warning is recorded, the exit is unchanged', 'pull-ticket.js', ['if (got.sha256 !== expected.sha256 || got.size !== expected.bytes) {', 'if (false) {']],
  ['T6: an artifacts object that does not match its hash installs nothing and records a warning', 'pull-ticket.js', ['if (got.sha256 !== expected.sha256 || got.size !== expected.bytes) {', 'if (false) {']],
  ['T8: a running row the reaper marks lost settles 75 runner-lost, while a queued row of the same age is untouched however long it waits', 'pull-ticket.js', ["      return { state: 'queued', next: run.cfg.pollMs };", "      return settle(run, entryFor(run, row, lost('runner-lost')));"]],
  ['T12: a row that vanished after the enqueue settles lost 75 queue-row-missing; with an acceptance the accepted result is published', 'pull-ticket.js', ["  if (row === null) return settle(run, entryFor(run, null, lost('queue-row-missing')));\n", ''], /reading 'state'/],
  ['T23: an enqueue that committed, a restore, and a restart settle lost 75 enqueue-crossed-restore and insert nothing', 'pull-ticket.js', ["if (sent.outcome === 'lost') return", "if (false) return"]],
  ['T27: URLs are per epoch: a reclaimed ticket gets fresh ones, and the stale epoch keys stay out of the current epoch', 'pull-ticket.js', [' || row.urls.epoch !== row.epoch', '']],
  ['T28: a death before the snapshot descriptor settles lost prepare-interrupted; after it the enqueue continues from the envelope, not the worktree', 'pull-ticket.js', ["    return settle(run, entryFor(run, null, lost('prepare-interrupted')));\n", "    return { state: 'preparing', next: 0 };\n"]],
  ['T28: a death before the snapshot descriptor settles lost prepare-interrupted; after it the enqueue continues from the envelope, not the worktree', 'pull-ticket.js', ["    if (supervisorAlive(attempt)) return { state: 'preparing', next: run.cfg.pollMs };\n", '']],
  ['T29: a stalled object fetch holds only its own ticket: another ticket\'s URL attach lands first, and the stalled one settles with a warning after 5 passes', 'pull-ticket.js', ['key, file, { deadlineMs: run.objectDeadlineMs }', 'key, file, { deadlineMs: 600_000 }'], /still running/],
  ['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, an operator-cancelled row stays unproven', 'pull-ticket.js', ["    return result.source === 'queued' ? { kind: 'cancelled', exit: 130, source: 'queued' } : { kind: 'unproven', source: result.source };", "    return { kind: 'cancelled', exit: 130, source: result.source };"]],
  ['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, an operator-cancelled row stays unproven', 'pull-ticket.js', ['  if (pull?.cancel?.answer !== undefined) return;\n', '']],
];

definePullSuite('pull ticket step machine on a disposable Postgres', SCENARIOS, CANARIES);
