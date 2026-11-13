import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { readAttempt } from '../src/attempts.js';
import { readEnvelope } from '../src/pull-envelope.js';
import { QueueError } from '../src/queue-client.js';
import { sha256Hex } from '../src/s3-presign.js';
import { definePullSuite, pullFixture, artifactsStream, ARTIFACT_PATH } from './helpers/pull-fixture.js';
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
    await held.setRow("state = 'quarantined'");
    assert.deepEqual(await held.withdraw(), { proof: false, state: 'quarantined' });
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
    const reap = async () => (await w.su()).query("SELECT lane_dispatch.ld_reap(interval '2 seconds', interval '120 seconds', interval '2 seconds')");
    await reap();
    await lostFx.setRow("lease_until = now() - interval '1 hour'");
    await queuedFx.setRow("created_at = now() - interval '1 hour'");
    await reap();
    assert.equal((await lostFx.row()).state, 'lost');
    assert.equal((await queuedFx.row()).state, 'queued');

    const settled = await lostFx.step();
    assert.deepEqual([settled.decision.reason, settled.result.exit, settled.result.reason], ['runner-lost', 75, 'runner-lost']);
    queuedFx.clock.now += 24 * HOUR;
    assert.equal((await queuedFx.step()).state, 'queued', 'a queued ticket has no timeout of any kind');
    assert.equal((await queuedFx.row()).state, 'queued');
  },

  'T12: a row that vanished after the enqueue settles lost 75 queue-row-missing; with an acceptance the accepted result is published': async (load, w, t) => {
    const gone = await pullFixture(t, w, { load });
    await gone.enqueue();
    await deleteRow(gone);
    const settled = await gone.step();
    assert.deepEqual([settled.state, settled.decision.reason, settled.result.exit], ['settled', 'queue-row-missing', 75]);

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
    assert.deepEqual([settled.state, settled.decision.reason, settled.result.exit], ['settled', 'enqueue-crossed-restore', 75]);
    assert.equal(await rowCount(fx), 0, 'no new runnable row for that id');
  },

  'T27: URLs are per epoch: a reclaimed ticket gets fresh ones, and the stale epoch keys stay out of the current epoch': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    const stale = fx.runner.urls;
    assert.equal(stale.epoch, 1);
    await fx.runner.release('never-started');
    await fx.runner.claim();
    assert.equal(fx.runner.claimed.epoch, 2);
    assert.equal((await fx.runner.heartbeat()).urls, null, 'the queue withholds the epoch-1 URLs from the epoch-2 claim');
    await fx.step();
    const fresh = (await fx.runner.heartbeat()).urls;
    assert.equal(fresh.epoch, 2, 'the supervisor attached URLs for the new epoch');
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
    const first = await marker.step();
    assert.deepEqual([first.state, first.decision.reason, first.result.exit], ['settled', 'prepare-interrupted', 75]);
    assert.equal(await rowCount(marker), 0);

    const upload = await next(t, w, load, 2, marker);
    const failingSpool = { envelopeFields: upload.envelopeFields, worktreeRoot: upload.worktreeRoot, manifest: upload.manifest, uploadOptions: { openSpool: () => { throw crash; } } };
    await assert.rejects(upload.prepare({ prepare: failingSpool }), crash);
    assert.ok(readEnvelope(upload.state, upload.id), 'the envelope exists but holds no snapshot');
    assert.equal((await upload.step()).decision.reason, 'prepare-interrupted');
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

SCENARIOS['a quarantined row settles lost 75 queue-restored only after quarantineLostAfterMs, and a reassert starts the clock over'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.hold();
  await fx.setRow("state = 'quarantined'");
  assert.deepEqual(await fx.step(), { state: 'quarantined', next: 15_000 });
  fx.clock.now += 20 * 60_000;
  await fx.setRow("state = 'running'");
  assert.equal((await fx.step()).state, 'running');
  assert.equal(fx.pull().quarantinedSince, undefined, 'a reassert clears the clock');
  await fx.setRow("state = 'quarantined'");
  assert.equal((await fx.step()).state, 'quarantined', 'the 20 minutes before the reassert do not count');
  fx.clock.now += 31 * 60_000;
  const settled = await fx.step();
  assert.deepEqual([settled.decision.reason, settled.result.exit], ['queue-restored', 75]);
};

SCENARIOS['runPullTicket prepares, waits out a queue failure and returns the confirmed outcome'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  let failures = 0;
  const flaky = { ...fx.queue, get: async (id) => { if (failures++ === 0) throw new QueueError('get failed: connection terminated'); return fx.queue.get(id); } };
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

SCENARIOS['a snapshot object that vanished settles lost snapshot-missing: before the first send nothing is sent, after it the row is cancelled'] = async (load, w, t) => {
  const beforeSend = await pullFixture(t, w, { load });
  await beforeSend.prepare();
  beforeSend.stub.objects.delete(`lane-snap/${readEnvelope(beforeSend.state, beforeSend.id).snapshot.key}`);
  const first = await beforeSend.step();
  assert.deepEqual([first.decision.reason, first.result.exit], ['snapshot-missing', 75]);
  assert.equal(await rowCount(beforeSend), 0, 'nothing was sent');

  const afterSend = await next(t, w, load, 2, beforeSend);
  await afterSend.enqueue();
  await afterSend.runner.claim();
  afterSend.stub.objects.delete(`lane-snap/${readEnvelope(afterSend.state, afterSend.id).snapshot.key}`);
  const second = await afterSend.step();
  assert.deepEqual([second.decision.reason, second.result.exit], ['snapshot-missing', 75]);
  assert.equal((await afterSend.row()).cancel_requested, true, 'the held row was told to stop');
};

SCENARIOS['enqueueOnce sends nothing once a decision is ledgered'] = async (load, w, t) => {
  const fx = await pullFixture(t, w, { load });
  await fx.prepare();
  const { enqueueOnce: enqueueFrom } = await load('pull-enqueue.js');
  const outcome = await enqueueFrom({ root: fx.state, id: fx.id, generation: fx.generation, queue: fx.queue, ledgered: () => true });
  assert.deepEqual(outcome, { outcome: 'settled' });
  assert.equal(await rowCount(fx), 0);
};

const cancelFixture = async (t, w, load, n, first) => {
  const fx = await next(t, w, load, n, first);
  fx.cancelCalls = 0;
  const counting = { ...fx.queue, cancel: async (id) => { fx.cancelCalls += 1; return fx.queue.cancel(id); } };
  fx.cancelStep = () => fx.step({ queue: counting, cancelRequested: () => true });
  return fx;
};

SCENARIOS['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, a quarantined cancel stays unproven'] = async (load, w, t) => {
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
  await quarantined.setRow("state = 'quarantined'");
  const unproven = await quarantined.cancelStep();
  assert.deepEqual([unproven.state, unproven.exitCode], ['unresolved', 1]);
  assert.ok(readAttempt(quarantined.state, quarantined.id), 'the attempt is kept');
  assert.equal(quarantined.published(), null);
  quarantined.clock.now += 31 * 60_000;
  const timedOut = await quarantined.cancelStep();
  assert.deepEqual([timedOut.decision.reason, quarantined.published().exit], ['cancel-unproven', 75]);

  const lostFirst = await cancelFixture(t, w, load, 6, queued);
  await lostFirst.hold();
  await lostFirst.setRow("state = 'quarantined'");
  const droppedAfterCommit = { ...lostFirst.queue, cancel: async (id) => { lostFirst.cancelCalls += 1; await lostFirst.queue.cancel(id); throw LOST_RESPONSE; } };
  await assert.rejects(lostFirst.step({ queue: droppedAfterCommit, cancelRequested: () => true }), LOST_RESPONSE);
  assert.equal((await lostFirst.cancelStep()).state, 'unresolved', 'the lost first response never upgrades to confirmed');
  assert.equal((await lostFirst.cancelStep()).state, 'unresolved');
  assert.equal(lostFirst.cancelCalls, 2, 'the first answer is persisted, so the cancel is not sent again');
};

const CANARIES = [
  ['a bound rejected result settles lost 75 with no second enqueue: snapshot-blob-missing for a cache reason, runner-rejected otherwise', 'pull-ticket.js', ["  if (result.kind === 'rejected' && provesNeverStarted(result, expected)) return lost(CACHE_REJECT_RE.test(result.reason ?? '') ? 'snapshot-blob-missing' : 'runner-rejected', result.reason);\n", '']],
  ['a quarantined row settles lost 75 queue-restored only after quarantineLostAfterMs, and a reassert starts the clock over', 'pull-ticket.js', ["if (run.now() - since >= run.cfg.quarantineLostAfterMs) return settle(run, entryFor(run, row, lost('queue-restored')));", "return settle(run, entryFor(run, row, lost('queue-restored')));"]],
  ['a quarantined row settles lost 75 queue-restored only after quarantineLostAfterMs, and a reassert starts the clock over', 'pull-ticket.js', ["  if (row.state !== 'quarantined' && attempt.pull.quarantinedSince !== undefined) await persistPull(run, { quarantinedSince: undefined });\n", '']],
  ['runPullTicket prepares, waits out a queue failure and returns the confirmed outcome', 'pull-ticket.js', ['const RETRYABLE = [QueueError, S3Error, LockTimeoutError];', 'const RETRYABLE = [];']],
  ['a snapshot object that vanished settles lost snapshot-missing: before the first send nothing is sent, after it the row is cancelled', 'pull-ticket.js', ["    if (reason) return { outcome: 'lost', reason };\n", '']],
  ['a snapshot object that vanished settles lost snapshot-missing: before the first send nothing is sent, after it the row is cancelled', 'pull-ticket.js', ['    if (!head.exists) {', '    if (false) {']],
  ['enqueueOnce sends nothing once a decision is ledgered', 'pull-enqueue.js', ["    if (ledgered()) return { outcome: 'settled' };\n", '']],
  ['T3: a lost enqueue response is replayed with the same id and leaves exactly one row', 'pull-enqueue.js', ['    id: envelope.logicalId,', '    id: globalThis.crypto.randomUUID(),']],
  ['T1: a withdraw proves only a committed queued -> withdrawn; every other answer leaves the ticket stepping', 'pull-ticket.js', ["if (answer !== 'withdrawn' && reread?.state !== 'withdrawn')", "if (answer === null && reread?.state !== 'withdrawn')"]],
  ['T4: a result that does not bind to the ticket settles lost 75 result-unbound and installs nothing', 'pull-ticket.js', ["  if (classified.outcome !== 'confirmed') return lost('result-unbound', classified.reason);\n", '']],
  ['T5: a log object that does not match its hash is not used: the streamed log stands, a warning is recorded, the exit is unchanged', 'pull-ticket.js', ['if (got.sha256 !== expected.sha256 || got.size !== expected.bytes) {', 'if (false) {']],
  ['T6: an artifacts object that does not match its hash installs nothing and records a warning', 'pull-ticket.js', ['if (got.sha256 !== expected.sha256 || got.size !== expected.bytes) {', 'if (false) {']],
  ['T8: a running row the reaper marks lost settles 75 runner-lost, while a queued row of the same age is untouched however long it waits', 'pull-ticket.js', ["      return { state: 'queued', next: run.cfg.pollMs };", "      return settle(run, entryFor(run, row, lost('runner-lost')));"]],
  ['T12: a row that vanished after the enqueue settles lost 75 queue-row-missing; with an acceptance the accepted result is published', 'pull-ticket.js', ["  if (row === null) return settle(run, entryFor(run, null, lost('queue-row-missing')));\n", '']],
  ['T23: an enqueue that committed, a restore, and a restart settle lost 75 enqueue-crossed-restore and insert nothing', 'pull-ticket.js', ["if (sent.outcome === 'lost') return", "if (false) return"]],
  ['T27: URLs are per epoch: a reclaimed ticket gets fresh ones, and the stale epoch keys stay out of the current epoch', 'pull-ticket.js', [' || row.urls.epoch !== row.epoch', '']],
  ['T28: a death before the snapshot descriptor settles lost prepare-interrupted; after it the enqueue continues from the envelope, not the worktree', 'pull-ticket.js', ["if (run.envelope === null || run.envelope.snapshot === undefined) return settle(run, entryFor(run, null, lost('prepare-interrupted')));\n", '']],
  ['T29: a stalled object fetch holds only its own ticket: another ticket\'s URL attach lands first, and the stalled one settles with a warning after 5 passes', 'pull-ticket.js', ['key, file, { deadlineMs: run.objectDeadlineMs }', 'key, file, { deadlineMs: 600_000 }']],
  ['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, a quarantined cancel stays unproven', 'pull-ticket.js', ["    return result.source === 'queued' ? { kind: 'cancelled', exit: 130, source: 'queued' } : { kind: 'unproven', source: result.source };", "    return { kind: 'cancelled', exit: 130, source: result.source };"]],
  ['T16: cancel outcomes: queued, released, runner-confirmed and a normal finish are decided from the row, a quarantined cancel stays unproven', 'pull-ticket.js', ['  if (pull?.cancel?.answer !== undefined) return;\n', '']],
];

definePullSuite('pull ticket step machine on a disposable Postgres', SCENARIOS, CANARIES);
