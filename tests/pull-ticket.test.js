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
import { call, logins, simulateRestore, tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R5: the step machine against a disposable queue, a stub runner and the S3 stub (T1, T3, T4-T6, T8, T12, T16, T23, T27-T29). */

const LOST_RESPONSE = new Error('response lost');
const HOUR = 3_600_000;
const rowCount = async (fx) => (await (await fx.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets WHERE id = $1', [fx.id])).rows[0].n;
const deleteRow = async (fx) => (await fx.su()).query('DELETE FROM lane_dispatch.tickets WHERE id = $1', [fx.id]);
const next = (t, w, load, n, first) => pullFixture(t, w, { load, id: tid(n), stub: first?.stub });
const withTimeout = (promise, ms) => Promise.race([promise, new Promise((_, reject) => setTimeout(() => reject(new Error(`still running after ${ms}ms`)), ms))]);

const GLOBAL_CFG = { runners: [], runnerLostAfterMs: 0 };
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

  'T5: a log object that does not match its hash is not used: nothing is written, a warning is recorded, the exit is unchanged': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.put('log', Buffer.from('evil\n'));
    await fx.runner.finish({ logSha256: sha256Hex('good\n'), logBytes: 5 });
    const settled = await fx.step();
    assert.equal(settled.result.exit, 0);
    assert.match(settled.result.remoteLogWarning, /does not match/);
    assert.equal(fs.existsSync(fx.logPath) ? fs.readFileSync(fx.logPath, 'utf8') : '', '', 'the mismatched object was never written to the log');
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

  'T27: output keys are derived from the row, never sent: a reclaimed ticket reads only its current epoch and its own host, whatever else sits in the bucket': async (load, w, t) => {
    await call(await w.as(logins.admin), 'ld_set_principal', logins.a2, 'agent', 'wintop', ['linux'], ['*'], 1);
    const fx = await pullFixture(t, w, { load, runnerLogin: logins.a2 });
    await fx.enqueue();
    await fx.runner.claim();
    assert.equal(fx.runner.host, 'wintop', 'the runner\'s host is not its login\'s suffix');
    await fx.runner.put('log', Buffer.from('epoch one\n'));
    await fx.runner.release('never-started');
    await fx.runner.claim();
    assert.equal(fx.runner.claimed.epoch, 2);
    await fx.runner.start();
    const decoy = Buffer.from('another host\n');
    fx.stub.objects.set(`lane-out/a2/${fx.id}/e2/log`, { body: decoy, lastModified: Date.now() });
    const fields = await fx.runner.putOutputs({ log: 'epoch two\n' });
    assert.ok(fx.stub.objects.has(`lane-out/${fx.runner.host}/${fx.id}/e2/log`));
    await fx.runner.finish(fields);
    const settled = await fx.step();
    assert.equal(settled.state, 'settled');
    assert.equal(settled.result.remoteLogWarning, undefined, 'the object read was the one the result names');
    assert.equal(fs.readFileSync(fx.logPath, 'utf8'), 'epoch two\n');
    assert.ok(fx.stub.requests.some((r) => r.method === 'GET' && r.path === `/lane-out/${fx.runner.host}/${fx.id}/e2/log`));
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

  'T29: a stalled object fetch holds only its own ticket: another ticket\'s step lands first, and the stalled one settles with a warning after 5 passes': async (load, w, t) => {
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
    else if (row?.state === 'claimed') {
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

SCENARIOS['runPullTicket ends the run when a step is unresolved instead of looping on a renewed lease'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  await fx.runner.finishCancelled(false);
  const outcome = await fx.run({ sleep: async () => assert.fail('the run kept waiting on an unresolved step') });
  assert.deepEqual([outcome.outcome, outcome.reason, outcome.exitCode], ['unresolved', 'cancel-unconfirmed', 1]);
};


const lostOutcome = (out) => [out.decision.reason, out.result.exit, out.result.reason];

SCENARIOS['a lost row settles with the reason the queue wrote: queue-restored, release-cap, runner-lost; an unknown or missing reason reads runner-lost, and the queue\'s record is never taken for a runner\'s result'] = async (load, w, t) => {
  const restored = await pullFixture(t, w, { load });
  await restored.enqueue();
  await call(await w.as(logins.admin), 'ld_restore_reset');
  assert.deepEqual(lostOutcome(await restored.step()), ['queue-restored', 75, 'queue-restored']);
  const entry = readLedger(restored.state, restored.id);
  assert.deepEqual([entry.resultSha256, entry.attemptRef], [null, null], 'a queue-authored lost record is not a runner result');

  const capped = await next(t, w, load, 2, restored);
  await capped.enqueue();
  for (let release = 0; release < 6; release++) {
    await capped.runner.claim();
    await capped.runner.release('never-started');
  }
  assert.equal((await capped.row()).result.reason, 'release-cap');
  assert.deepEqual(lostOutcome(await capped.step()), ['release-cap', 75, 'release-cap']);

  const unknown = await next(t, w, load, 3, restored);
  await unknown.hold();
  await unknown.setRow(`state = 'lost', finished_at = now(), result = jsonb_build_object('kind', 'lost', 'reason', 'something-new', 'author', 'queue', 'epoch', epoch)`);
  assert.deepEqual(lostOutcome(await unknown.step()), ['runner-lost', 75, 'runner-lost']);

  const bare = await next(t, w, load, 4, restored);
  await bare.hold();
  await bare.setRow("state = 'lost', finished_at = now(), result = NULL");
  assert.deepEqual(lostOutcome(await bare.step()), ['runner-lost', 75, 'runner-lost']);
};

SCENARIOS['an unproven cancel stays unresolved until unprovenCancelLostAfterMs (10 min by default), then settles lost 75 cancel-unproven'] = async (load, w, t) => {
  const configured = await pullFixture(t, w, { load, cfg: { unprovenCancelLostAfterMs: 120_000 } });
  await configured.hold();
  await configured.runner.finishCancelled(false);
  assert.equal((await configured.step()).state, 'unresolved');
  configured.clock.now += 119_000;
  assert.equal((await configured.step()).state, 'unresolved', 'not before the configured deadline');
  configured.clock.now += 2_000;
  const settled = await configured.step();
  assert.equal(settled.state, 'settled', 'at the configured deadline');
  assert.deepEqual([settled.decision.reason, configured.published().exit], ['cancel-unproven', 75]);

  const byDefault = await next(t, w, load, 2, configured);
  await byDefault.hold();
  await byDefault.runner.finishCancelled(false);
  assert.equal((await byDefault.step()).state, 'unresolved');
  byDefault.clock.now += 9 * 60_000;
  assert.equal((await byDefault.step()).state, 'unresolved', 'nine minutes is inside the default');
  byDefault.clock.now += 2 * 60_000;
  const lastly = await byDefault.step();
  assert.equal(lastly.state, 'settled', 'eleven minutes is past the default');
  assert.equal(lastly.decision.reason, 'cancel-unproven');
};

const deadQueue = (fx) => ({ ...fx.queue, ...Object.fromEntries(['get', 'cancel', 'withdraw', 'queueRef', 'enqueueFenced'].map((name) => [name, async () => { throw transportError; }])) });
const advancing = (fx, limit = 20) => {
  const sleeps = [];
  return { sleeps, sleep: async (ms) => { assert.ok(sleeps.push(ms) <= limit, 'the run never gave up on the dead queue'); fx.clock.now += ms; } };
};

SCENARIOS['after queueUnreachableAfterMs of a dead queue the ticket settles lost 75 queue-unreachable at once; the cancel it owes the row waits in a marker the reconcile sweep sends once the queue answers'] = async (load, w, t) => {
  const { sweepOrphans } = await load('reconcile-sweep.js');
  const { owedCancelIds } = await load('pull-envelope.js');
  const sweep = (fx, queue) => sweepOrphans(fx.state, { globalCfg: GLOBAL_CFG, force: true, pullContext: (ticket) => fx.ctx({ generation: ticket.generation, ...(queue ? { queue } : {}) }) });
  let first;
  for (const [n, held] of [[1, false], [2, true]]) {
    const fx = await pullFixture(t, w, { load, id: tid(n), stub: first?.stub, cfg: { queueUnreachableAfterMs: 120_000 } });
    first ??= fx;
    await (held ? fx.hold() : fx.enqueue());
    const { sleeps, sleep } = advancing(fx);
    const outcome = await fx.run({ queue: deadQueue(fx), sleep });
    assert.deepEqual([outcome.outcome, outcome.exitCode, outcome.decision.reason], ['lost', 75, 'queue-unreachable']);
    assert.equal(sleeps.length, 8, 'it waited the whole 120 s, a poll at a time, before giving up');
    assert.deepEqual([fx.published().exit, fx.published().reason], [75, 'queue-unreachable'], 'the caller is answered at once');
    assert.equal(readLedger(fx.state, fx.id).decision.reason, 'queue-unreachable', 'ledgered like any decision');
    assert.ok(!readAttempt(fx.state, fx.id), 'the attempt is settled, not left unresolved');
    assert.deepEqual(owedCancelIds(fx.state), [fx.id], 'the cancel the row is owed is kept apart from the attempt');
    assert.equal((await fx.row()).state, held ? 'running' : 'queued', 'the cancel could not be sent');

    const down = await sweep(fx, deadQueue(fx));
    assert.deepEqual([down.results[0].id, down.results[0].state], [fx.id, 'unreachable'], 'a sweep that cannot reach the queue sends nothing');
    assert.deepEqual(owedCancelIds(fx.state), [fx.id], 'and keeps the marker');

    const back = await sweep(fx);
    assert.deepEqual([back.results[0].id, back.results[0].state], [fx.id, 'cancel-sent']);
    assert.deepEqual(owedCancelIds(fx.state), []);
    const row = await fx.row();
    assert.deepEqual([row.state, row.cancel_requested], [held ? 'running' : 'cancelled', true], 'cancelled while queued, told to stop while held');
    assert.equal(fx.published().exit, 75, 'the answer already given stands');
  }
};

SCENARIOS['the unreachable clock counts a dead queue only: a store outage never abandons the run, and one answered call starts the count over'] = async (load, w, t) => {
  const cfg = { queueUnreachableAfterMs: 60_000 };
  const store = await pullFixture(t, w, { load, cfg });
  await store.prepare();
  const unavailable = new S3Error('s3 HEAD answered 503', { status: 503 });
  const outage = advancing(store, 12);
  await assert.rejects(store.run({ s3: { ...store.s3, headObject: async () => { throw unavailable; } }, sleep: async (ms) => { if (outage.sleeps.length >= 12) throw STOP; await outage.sleep(ms); } }), STOP);
  assert.equal(outage.sleeps.length, 12, 'twelve polls of a store outage, three times the queue limit, and the run still waits');

  const flaky = await pullFixture(t, w, { load, id: tid(2), stub: store.stub, cfg });
  await flaky.enqueue();
  let calls = 0;
  const everyFourth = { ...flaky.queue, get: async (id) => { if (++calls % 4 !== 0) throw transportError; return flaky.queue.get(id); } };
  const answered = advancing(flaky, 16);
  await assert.rejects(flaky.run({ queue: everyFourth, sleep: async (ms) => { if (answered.sleeps.length >= 16) throw STOP; await answered.sleep(ms); } }), STOP);
  assert.equal(answered.sleeps.length, 16, 'three failures at a time never reach 60 s, however often they come back');
};
SCENARIOS['the priority a requested tier is enqueued at sits in the band the queue scores for that tier: low, medium and high score 0, 1 and 2'] = async (load, w) => {
  const { pullQueuePriority } = await load('priority.js');
  const su = await w.su();
  const scores = [];
  for (const tier of ['low', 'medium', 'high']) scores.push((await su.query('SELECT lane_dispatch.eff_score($1, false, 0) AS score', [pullQueuePriority(tier)])).rows[0].score);
  assert.deepEqual(scores, [0, 1, 2]);
};

const prepareOver = (fx, over) => fx.prepareRetrying({ s3: { ...fx.s3, putObjectFromFile: async () => { throw new S3Error('s3 PUT answered 503', { status: 503 }); } }, ...over });

SCENARIOS['a store outage during preparation ends within prepareBudgetMs (the snapshot could not be prepared), and a cancel during it ends it at once'] = async (load, w, t) => {
  const { PrepareAbandoned } = await load('pull-ticket.js');
  const outage = await pullFixture(t, w, { load, cfg: { prepareBudgetMs: 60_000 } });
  const { sleeps, sleep } = advancing(outage);
  await assert.rejects(prepareOver(outage, { sleep }), (err) => err instanceof PrepareAbandoned && err.reason === 'budget' && /503/.test(err.message));
  assert.equal(sleeps.length, 4, 'four polls of 15 s, then the budget is spent');

  const cancelled = await pullFixture(t, w, { load, id: tid(2), stub: outage.stub, cfg: { prepareBudgetMs: 60_000 } });
  let stop = false;
  const polls = advancing(cancelled);
  const failingThenCancelled = { putObjectFromFile: async () => { stop = true; throw new S3Error('s3 PUT answered 503', { status: 503 }); } };
  await assert.rejects(cancelled.prepareRetrying({ s3: { ...cancelled.s3, ...failingThenCancelled }, cancelRequested: () => stop, sleep: polls.sleep }), (err) => err instanceof PrepareAbandoned && err.reason === 'cancelled');
  assert.equal(polls.sleeps.length, 0, 'a cancel that arrived during the failing attempt is seen before any wait');
  assert.equal((await cancelled.proveNeverEnqueued()).proof, true, 'nothing was enqueued, so the supervisor may publish the cancel');

  const early = await pullFixture(t, w, { load, id: tid(3), stub: outage.stub });
  const before = early.stub.requests.filter((r) => r.method === 'PUT').length;
  await assert.rejects(early.prepareRetrying({ cancelRequested: () => true }), (err) => err instanceof PrepareAbandoned && err.reason === 'cancelled');
  assert.equal(early.stub.requests.filter((r) => r.method === 'PUT').length, before, 'a ticket cancelled before preparation uploads nothing');
};

SCENARIOS['a cancel that is in force when the enqueue is about to be recorded ends the ticket cancelled 130 with no row and no send, whatever the earlier checks saw'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.prepare();
  let sends = 0;
  const counting = { ...fx.queue, enqueueFenced: async (args) => { sends += 1; return fx.queue.enqueueFenced(args); } };
  const settled = await fx.step({ queue: counting, cancelRequested: () => true });
  assert.equal(settled.state, 'settled');
  assert.deepEqual([settled.decision.kind, settled.decision.source, settled.result.exit], ['cancelled', 'never-enqueued', 130]);
  assert.deepEqual([sends, await rowCount(fx)], [0, 0]);
  assert.equal(readLedger(fx.state, fx.id).decision.source, 'never-enqueued');
  assert.equal(fx.published().exit, 130);

  const sent = await pullFixture(t, w, { load, id: tid(2), stub: fx.stub });
  await sent.enqueue();
  const after = await sent.step({ cancelRequested: () => true });
  assert.notEqual(after.decision?.source, 'never-enqueued', 'once a row exists the cancel goes to the queue, not this branch');
  assert.equal(await rowCount(sent), 1);
};

SCENARIOS['a cancel decided while another stepper waits for the ticket lock is ledgered inside the lock that proved nothing was sent: the waiting stepper finds the decision and enqueues nothing'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.prepare();
  let sweeper;
  const cancelling = () => {
    sweeper = sweeper ?? fx.step();
    sweeper.catch(() => {});
    return true;
  };
  const cancelled = await fx.step({ cancelRequested: cancelling });
  const swept = await sweeper;
  assert.deepEqual([cancelled.state, cancelled.decision.source], ['settled', 'never-enqueued']);
  assert.equal(swept.state, 'settled', 'the sweeper found the ledgered decision, not a ticket to enqueue');
  assert.equal(await rowCount(fx), 0, 'no row exists for a ticket ledgered as never enqueued');
  assert.equal(readLedger(fx.state, fx.id).decision.source, 'never-enqueued');
};

SCENARIOS['a log that takes the whole step budget does not cost the artifacts: they are fetched on the next pass, the log is not fetched again, and no retry is counted'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  const artifacts = artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"late":1}' });
  await fx.runner.finish(await fx.runner.putOutputs({ log: 'slow log\n', artifacts }));
  let slowed = false;
  fx.stub.delayMatching = (method, pathname) => {
    if (method === 'GET' && pathname.endsWith('/log') && !slowed) {
      slowed = true;
      fx.clock.now += 30_000;
    }
    return 0;
  };
  const gets = (name) => fx.stub.requests.filter((r) => r.method === 'GET' && r.path.endsWith(`/${name}`)).length;
  const first = await fx.step({ stepBudgetMs: 10_000 });
  assert.equal(first.state, 'fetching', 'the log used the budget; the artifacts were not attempted');
  assert.equal(gets('artifacts'), 0);
  assert.equal(fx.pull()?.objectRetries, undefined, 'an object never attempted is not a failed retry');
  const second = await fx.step({ stepBudgetMs: 10_000 });
  assert.equal(second.state, 'settled');
  assert.equal(gets('log'), 1, 'the log staged by the first pass was reused');
  assert.equal(fx.published().remoteArtifactsWarning, undefined);
  assert.equal(fs.readFileSync(path.join(fs.realpathSync(fx.worktreeRoot), ARTIFACT_PATH), 'utf8'), '{"late":1}');
};

SCENARIOS['a competing stepper that enqueues in the gap after the never-enqueued proof finds the decision already ledgered: no queue row, no intent'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.prepare();
  let raced = false;
  fx.hooks.afterEnqueueStage = async () => {
    if (raced) return;
    raced = true;
    await fx.step({ cancelRequested: () => false });
  };
  const cancelled = await fx.step({ cancelRequested: () => true });
  assert.equal(raced, true, 'the competitor ran at the old gap');
  assert.equal(await rowCount(fx), 0, 'the competitor enqueued a row for a ticket that was ledgered as never enqueued');
  assert.equal(fx.pull()?.enqueue, undefined, 'and recorded an enqueue intent');
  assert.deepEqual([cancelled.state, readLedger(fx.state, fx.id).decision.source], ['settled', 'never-enqueued']);
};

SCENARIOS['a pass that starts with its step budget already spent still fetches one object, so deferred objects always make progress'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  const artifacts = artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"p":1}' });
  await fx.runner.finish(await fx.runner.putOutputs({ log: 'a log\n', artifacts }));
  const gets = (name) => fx.stub.requests.filter((r) => r.method === 'GET' && r.path.endsWith(`/${name}`)).length;
  const first = await fx.step({ stepBudgetMs: -1 });
  assert.deepEqual([first.state, gets('log'), gets('artifacts')], ['fetching', 1, 0], 'exactly one object, though the budget was spent before the pass began');
  const second = await fx.step({ stepBudgetMs: -1 });
  assert.deepEqual([second.state, gets('log'), gets('artifacts')], ['settled', 1, 1]);
  assert.equal(fx.published().remoteArtifactsWarning, undefined);
};

SCENARIOS['a log that keeps failing while the artifacts are deferred still settles after 5 passes, with a warning for each'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  const artifacts = artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"p":1}' });
  await fx.runner.finish(await fx.runner.putOutputs({ log: 'a log\n', artifacts }));
  fx.stub.failMatching = (method, pathname) => (method === 'GET' && pathname.endsWith('/log') ? 503 : 0);
  const states = [];
  for (let pass = 0; pass < 8 && states.at(-1) !== 'settled'; pass++) states.push((await fx.step({ stepBudgetMs: -1 })).state);
  assert.deepEqual(states, ['fetching', 'fetching', 'fetching', 'fetching', 'settled'], 'the 5-pass cap holds whatever was deferred');
  assert.match(fx.published().remoteLogWarning, /not retrieved after 5 passes/);
  assert.match(fx.published().remoteArtifactsWarning, /not retrieved after 5 passes/);
};

SCENARIOS['a log that fails for good (hash mismatch) while the artifacts are deferred is remembered: it is fetched once, the ticket settles in a bounded number of passes, and its warning is kept'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  const artifacts = artifactsStream(fx.id, { [ARTIFACT_PATH]: '{"ok":1}' });
  await fx.runner.put('log', Buffer.from('evil\n'));
  const fields = await fx.runner.putOutputs({ artifacts });
  await fx.runner.finish({ ...fields, logSha256: sha256Hex('good\n'), logBytes: 5 });
  const gets = (name) => fx.stub.requests.filter((r) => r.method === 'GET' && r.path.endsWith(`/${name}`)).length;
  const states = [];
  for (let pass = 0; pass < 8 && states.at(-1) !== 'settled'; pass++) states.push((await fx.step({ stepBudgetMs: -1 })).state);
  assert.equal(states.at(-1), 'settled', `the ticket never settled: the permanently bad log was fetched again every pass (${states})`);
  assert.ok(states.length <= 3, `settled after ${states.length} passes`);
  assert.equal(gets('log'), 1, 'the bad log was downloaded once');
  assert.match(fx.published().remoteLogWarning, /does not match/);
  assert.equal(fx.published().remoteArtifactsWarning, undefined);
  assert.equal(fs.readFileSync(path.join(fs.realpathSync(fx.worktreeRoot), ARTIFACT_PATH), 'utf8'), '{"ok":1}');
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

  const unprovenRun = await cancelFixture(t, w, load, 5, queued);
  await unprovenRun.hold();
  await unprovenRun.runner.finishCancelled(false);
  const unproven = await unprovenRun.cancelStep();
  assert.deepEqual([unproven.state, unproven.exitCode], ['unresolved', 1]);
  assert.ok(readAttempt(unprovenRun.state, unprovenRun.id), 'the attempt is kept');
  assert.equal(unprovenRun.published(), null);
  unprovenRun.clock.now += 11 * 60_000;
  const timedOut = await unprovenRun.cancelStep();
  assert.deepEqual([timedOut.decision.reason, unprovenRun.published().exit], ['cancel-unproven', 75]);
  assert.equal(unprovenRun.cancelCalls, 1, 'the cancel was sent once; settling an unproven cancel sends no more, the row being terminal already');

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
  ['runPullTicket ends the run when a step is unresolved instead of looping on a renewed lease', 'pull-ticket.js', ["    if (step.state === 'unresolved') return { outcome: 'unresolved', reason: step.reason, exitCode: step.exitCode };\n", '']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ["['53300', '57P01', '57P02', '57P03', '57014', '55P03', '40P01', '40001']", "['53300', '57P01']"]],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ['  if (tlsFailure(err)) return false;\n', '']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ['  return err.cause !== undefined && typeof err.cause.severity !== \'string\';', '  return true;']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ['return err.status === null || err.status >= 500 || err.status === 408 || err.status === 429;', 'return true;']],
  ['a transient failure is waited out; any other server error or a store 4xx ends the run unresolved, neither lost nor green', 'pull-ticket.js', ["if (TRANSIENT_SQLSTATES.has(code) || code.startsWith('08')) return true;\n", '']],
  ['a bound rejected result settles lost 75 with no second enqueue: snapshot-blob-missing for a cache reason, runner-rejected otherwise', 'pull-ticket.js', ["  if (result.kind === 'rejected' && provesNeverStarted(result, expected)) return lost(CACHE_REJECT_RE.test(result.reason ?? '') ? 'snapshot-blob-missing' : 'runner-rejected', result.reason);\n", '']],
  ['a snapshot object that vanished before the first send settles lost snapshot-missing and sends nothing', 'pull-ticket.js', ["    if (reason) return { outcome: 'lost', reason };\n", '']],
  ['enqueueOnce sends nothing once a decision is ledgered', 'pull-enqueue.js', ["    if (ledgered()) return { outcome: 'settled' };\n", '']],
  ['T3: a lost enqueue response is replayed with the same id and leaves exactly one row', 'pull-enqueue.js', ['    id: envelope.logicalId,', '    id: globalThis.crypto.randomUUID(),']],
  ['T1: a withdraw proves only a committed queued -> withdrawn; every other answer leaves the ticket stepping', 'pull-ticket.js', ["if (answer !== 'withdrawn' && reread?.state !== 'withdrawn')", "if (answer === null && reread?.state !== 'withdrawn')"]],
  ['T4: a result that does not bind to the ticket settles lost 75 result-unbound and installs nothing', 'pull-ticket.js', ["  if (classified.outcome !== 'confirmed') return lost('result-unbound', classified.reason);\n", '']],
  ['T5: a log object that does not match its hash is not used: nothing is written, a warning is recorded, the exit is unchanged', 'pull-ticket.js', ['if (got.tooLarge || got.sha256 !== expected.sha256 || got.size !== expected.bytes) {', 'if (false) {']],
  ['T6: an artifacts object that does not match its hash installs nothing and records a warning', 'pull-ticket.js', ['if (got.tooLarge || got.sha256 !== expected.sha256 || got.size !== expected.bytes) {', 'if (false) {']],
  ['T8: a running row the reaper marks lost settles 75 runner-lost, while a queued row of the same age is untouched however long it waits', 'pull-ticket.js', ["expireQueued(run, attempt) : { state: 'queued', next: run.cfg.pollMs };", "expireQueued(run, attempt) : settle(run, entryFor(run, row, lost('runner-lost')));"]],
  ['T12: a row that vanished after the enqueue settles lost 75 queue-row-missing; with an acceptance the accepted result is published', 'pull-ticket.js', ["  if (row === null) return settle(run, entryFor(run, null, lost('queue-row-missing')));\n", ''], /reading 'host'/],
  ['T23: an enqueue that committed, a restore, and a restart settle lost 75 enqueue-crossed-restore and insert nothing', 'pull-ticket.js', ["if (sent.outcome === 'lost') return", "if (false) return"]],
  ['T27: output keys are derived from the row, never sent: a reclaimed ticket reads only its current epoch and its own host, whatever else sits in the bucket', 'pull-ticket.js', ['key: outKey(row.host, run.id, row.epoch, name)', "key: outKey('a2', run.id, row.epoch, name)"]],
  ['T27: output keys are derived from the row, never sent: a reclaimed ticket reads only its current epoch and its own host, whatever else sits in the bucket', 'pull-ticket.js', ['`${host}/${id}/e${epoch}/${name}`', '`${id}/e${epoch}/${name}`']],
  ['T27: output keys are derived from the row, never sent: a reclaimed ticket reads only its current epoch and its own host, whatever else sits in the bucket', 'pull-ticket.js', ['`${host}/${id}/e${epoch}/${name}`', '`a2/${id}/e${epoch}/${name}`']],
  ['T27: output keys are derived from the row, never sent: a reclaimed ticket reads only its current epoch and its own host, whatever else sits in the bucket', 'pull-ticket.js', ['`${host}/${id}/e${epoch}/${name}`', '`${host}/${id}/e1/${name}`']],
  ['the priority a requested tier is enqueued at sits in the band the queue scores for that tier: low, medium and high score 0, 1 and 2', 'priority.js', ['const PULL_QUEUE_BAND_START = [0, 4, 7];', 'const PULL_QUEUE_BAND_START = [0, 3, 6];']],
  ['a store outage during preparation ends within prepareBudgetMs (the snapshot could not be prepared), and a cancel during it ends it at once', 'pull-ticket.js', ['      if (now() - startedAt >= prepareBudgetMs) throw new PrepareAbandoned(\'budget\', err);\n', '']],
  ['a store outage during preparation ends within prepareBudgetMs (the snapshot could not be prepared), and a cancel during it ends it at once', 'pull-ticket.js', ["      if (ctx.cancelRequested?.()) throw new PrepareAbandoned('cancelled', err);\n", '']],
  ['a store outage during preparation ends within prepareBudgetMs (the snapshot could not be prepared), and a cancel during it ends it at once', 'pull-ticket.js', ["    if (ctx.cancelRequested?.()) throw new PrepareAbandoned('cancelled');\n", '']],
  ['a cancel that is in force when the enqueue is about to be recorded ends the ticket cancelled 130 with no row and no send, whatever the earlier checks saw', 'pull-enqueue.js', ['    if (intent.attempts.length === 0 && cancelRequested()) return { outcome: CANCELLED_BEFORE_ENQUEUE, settled: await settleCancelled() };\n', '']],
  ['a cancel decided while another stepper waits for the ticket lock is ledgered inside the lock that proved nothing was sent: the waiting stepper finds the decision and enqueues nothing', 'pull-enqueue.js', ['settled: await settleCancelled() };', 'settled: undefined };'], /reading|undefined/],
  ['a log that takes the whole step budget does not cost the artifacts: they are fetched on the next pass, the log is not fetched again, and no retry is counted', 'pull-ticket.js', ['    deferred.push(object.name);', "    transient.push([object.name, 'step budget spent']);"]],
  ['a log that takes the whole step budget does not cost the artifacts: they are fetched on the next pass, the log is not fetched again, and no retry is counted', 'pull-ticket.js', ['    if (await stagedMatches(part)) {\n      parts[object.name] = part;\n      continue;\n    }\n', '']],
  ['a competing stepper that enqueues in the gap after the never-enqueued proof finds the decision already ledgered: no queue row, no intent', 'pull-ticket.js', [
    ["    settleCancelled: () => settle(run, entryFor(run, null, { kind: 'cancelled', exit: CANCELLED_EXIT, source: 'never-enqueued' })),\n", '    settleCancelled: async () => null,\n'],
    ['    if (sent.outcome === CANCELLED_BEFORE_ENQUEUE) return await afterStep(run, sent.settled);', "    if (sent.outcome === CANCELLED_BEFORE_ENQUEUE) return await afterStep(run, await guarded(run, () => settle(run, entryFor(run, null, { kind: 'cancelled', exit: CANCELLED_EXIT, source: 'never-enqueued' }))));"],
  ]],
  ['a pass that starts with its step budget already spent still fetches one object, so deferred objects always make progress', 'pull-ticket.js', ['    if (attempted > 0 && overBudget(run)) {', '    if (overBudget(run)) {']],
  ['a log that keeps failing while the artifacts are deferred still settles after 5 passes, with a warning for each', 'pull-ticket.js', ['    if (retries < OBJECT_RETRY_PASSES) {', '    if (retries < OBJECT_RETRY_PASSES || deferred.length > 0) {']],
  ['a log that keeps failing while the artifacts are deferred still settles after 5 passes, with a warning for each', 'pull-ticket.js', ['    for (const name of deferred) warnings[name] =', '    for (const name of []) warnings[name] =']],
  ['a log that fails for good (hash mismatch) while the artifacts are deferred is remembered: it is fetched once, the ticket settles in a bounded number of passes, and its warning is kept', 'pull-ticket.js', ['      await persistPull(run, (pull) => ({ objectWarnings: { ...pull.objectWarnings, [object.name]: got.reason } }));\n', ''], null, /never settled/],
  ['a log that fails for good (hash mismatch) while the artifacts are deferred is remembered: it is fetched once, the ticket settles in a bounded number of passes, and its warning is kept', 'pull-ticket.js', ['      warnings[object.name] = settledFailures[object.name];\n      continue;\n', '      continue;\n'], null, /does not match|undefined/],
  ['a lost row settles with the reason the queue wrote: queue-restored, release-cap, runner-lost; an unknown or missing reason reads runner-lost, and the queue\'s record is never taken for a runner\'s result', 'pull-ticket.js', ["  if (state === 'lost') return lost(lostReasonOf(row));", "  if (state === 'lost') return lost('runner-lost');"]],
  ['a lost row settles with the reason the queue wrote: queue-restored, release-cap, runner-lost; an unknown or missing reason reads runner-lost, and the queue\'s record is never taken for a runner\'s result', 'pull-ticket.js', ['(QUEUE_LOST_REASONS.has(row.result?.reason) ? row.result.reason : \'runner-lost\')', "(row.result?.reason ?? 'runner-lost')"]],
  ['a lost row settles with the reason the queue wrote: queue-restored, release-cap, runner-lost; an unknown or missing reason reads runner-lost, and the queue\'s record is never taken for a runner\'s result', 'pull-ticket.js', ["(row.state === 'lost' || (row.state === 'cancelled' && SERVER_CANCEL_SOURCES.has(row.result.source)))", "(row.state === 'cancelled' && SERVER_CANCEL_SOURCES.has(row.result.source))"]],
  ['an unproven cancel stays unresolved until unprovenCancelLostAfterMs (10 min by default), then settles lost 75 cancel-unproven', 'pull-ticket.js', ['run.cfg.unprovenCancelLostAfterMs', '600_000']],
  ['an unproven cancel stays unresolved until unprovenCancelLostAfterMs (10 min by default), then settles lost 75 cancel-unproven', 'pull-ticket.js', ['if (run.now() - since >= run.cfg.unprovenCancelLostAfterMs) return settle(', 'if (true) return settle(']],
  ['after queueUnreachableAfterMs of a dead queue the ticket settles lost 75 queue-unreachable at once; the cancel it owes the row waits in a marker the reconcile sweep sends once the queue answers', 'pull-ticket.js', ['        if (queueDownSince !== null && now() - queueDownSince >= queueUnreachableAfterMs) return abandonUnreachableQueue(ctx);\n', '']],
  ['after queueUnreachableAfterMs of a dead queue the ticket settles lost 75 queue-unreachable at once; the cancel it owes the row waits in a marker the reconcile sweep sends once the queue answers', 'pull-ticket.js', ['    writeOwedCancel(run.root, run.id, { generation: run.generation, at: run.now() });\n', '']],
  ['after queueUnreachableAfterMs of a dead queue the ticket settles lost 75 queue-unreachable at once; the cancel it owes the row waits in a marker the reconcile sweep sends once the queue answers', 'pull-recover.js', ['      await withDeadline(ctxFor({ id, generation: owed?.generation ?? 0 }).queue.cancel(id), Math.max(1, deadline - now()));\n', '']],
  ['after queueUnreachableAfterMs of a dead queue the ticket settles lost 75 queue-unreachable at once; the cancel it owes the row waits in a marker the reconcile sweep sends once the queue answers', 'pull-recover.js', ['      await withDeadline(ctxFor({ id, generation: owed?.generation ?? 0 }).queue.cancel(id), Math.max(1, deadline - now()));\n      await clearOwedCancel(root, id);', '      await clearOwedCancel(root, id);\n      await withDeadline(ctxFor({ id, generation: owed?.generation ?? 0 }).queue.cancel(id), Math.max(1, deadline - now()));']],
  ['the unreachable clock counts a dead queue only: a store outage never abandons the run, and one answered call starts the count over', 'pull-ticket.js', ["if (isNamed(err, 'QueueError')) queueDownSince ??= now();", 'queueDownSince ??= now();']],
  ['the unreachable clock counts a dead queue only: a store outage never abandons the run, and one answered call starts the count over', 'pull-ticket.js', ['      queueDownSince = null;\n', '']],
  ['T28: a death before the snapshot descriptor settles lost prepare-interrupted; after it the enqueue continues from the envelope, not the worktree', 'pull-ticket.js', ["    return settle(run, entryFor(run, null, lost('prepare-interrupted')));\n", "    return { state: 'preparing', next: 0 };\n"]],
  ['T28: a death before the snapshot descriptor settles lost prepare-interrupted; after it the enqueue continues from the envelope, not the worktree', 'pull-ticket.js', ["    if (supervisorAlive(attempt)) return { state: 'preparing', next: run.cfg.pollMs };\n", '']],
  ['T29: a stalled object fetch holds only its own ticket: another ticket\'s step lands first, and the stalled one settles with a warning after 5 passes', 'pull-ticket.js', ['{ deadlineMs: run.objectDeadlineMs, maxBytes: expected.bytes }', '{ deadlineMs: 600_000, maxBytes: expected.bytes }'], /still running/],
  ['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, an operator-cancelled row stays unproven', 'pull-ticket.js', ["    return result.source === 'queued' ? { kind: 'cancelled', exit: 130, source: 'queued' } : { kind: 'unproven', source: result.source };", "    return { kind: 'cancelled', exit: 130, source: result.source };"]],
  ['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, an operator-cancelled row stays unproven', 'pull-ticket.js', ['  if (pull?.cancel?.answer !== undefined) return;\n', '']],
];

definePullSuite('pull ticket step machine on a disposable Postgres', SCENARIOS, CANARIES);
