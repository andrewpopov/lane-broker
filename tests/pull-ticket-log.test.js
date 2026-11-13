import assert from 'node:assert/strict';
import fs from 'node:fs';
import { GAP_MARKER } from '../src/pull-ticket.js';
import { pullTicketDir } from '../src/pull-envelope.js';
import { cursorFile, readCursor } from '../src/pull-log-cursor.js';
import { sha256Hex } from '../src/s3-presign.js';
import { definePullSuite, pullFixture } from './helpers/pull-fixture.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';
import { tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R5: the streamed log, its cursor and the append-only gap fill (T13, T13b, V4-2). */

const CRASH = new Error('killed');
const MARKER_RE = /\[lane-broker\] log truncated/g;
const next = (t, w, load, n, first, extra = {}) => pullFixture(t, w, { load, id: tid(n), stub: first?.stub, ...extra });
const chunkCount = async (fx) => (await (await fx.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.log_chunks WHERE ticket = $1', [fx.id])).rows[0].n;
const logText = (fx) => fs.readFileSync(fx.logPath, 'utf8');
const crashAt = async (fx, point) => {
  fx.hooks[point] = () => { throw CRASH; };
  await assert.rejects(fx.step(), CRASH);
  delete fx.hooks[point];
};
const finishWithLog = async (fx, full, streamedHash = full) => fx.runner.finish(await fx.runner.putOutputs({ log: full }).then((fields) => ({ ...fields, logSha256: sha256Hex(streamedHash), logBytes: Buffer.byteLength(streamedHash) })));

const SCENARIOS = {
  'T13: a log replayed after a restart has every line exactly once, whichever point the supervisor died at': async (load, w, t) => {
    let first;
    for (const [n, point] of ['afterDurable', 'afterCursor', 'afterAck'].entries()) {
      const fx = await next(t, w, load, n + 1, first);
      first ??= fx;
      await fx.hold();
      for (const line of ['l0\n', 'l1\n', 'l2\n']) await fx.runner.log(line);
      await crashAt(fx, point);
      await fx.step();
      assert.equal(logText(fx), 'l0\nl1\nl2\n', point);
      assert.equal(readCursor(fx.state, fx.id).ackedSeq, 2, point);
      assert.equal(readCursor(fx.state, fx.id).logBytes, 9, point);
      assert.equal(await chunkCount(fx), 0, `${point}: every chunk was acked`);
      await fx.runner.finish();
    }
  },

  'T13: the log is fsynced before the cursor, the cursor and its directory before the ack': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.log('l0\n');
    const logIno = fs.statSync(fx.logPath).ino;
    const recorder = recordFsyncs();
    let events;
    try {
      const acking = { ...fx.queue, logAck: async (...args) => { recorder.events.push({ op: 'ack' }); return fx.queue.logAck(...args); } };
      await fx.step({ queue: acking });
    } finally {
      recorder.stop();
      events = recorder.events;
    }
    const cursorDir = pullTicketDir(fx.state, fx.id);
    const logSync = events.findIndex((e) => e.op === 'fsync' && e.ino === logIno);
    const cursorRename = events.findIndex((e) => e.op === 'rename' && e.path === cursorFile(fx.state, fx.id));
    const cursorDirSync = events.findIndex((e, i) => i > cursorRename && e.op === 'fsync' && e.path === cursorDir);
    const ack = events.findIndex((e) => e.op === 'ack');
    assert.ok(logSync >= 0, 'the log file was never fsynced');
    assert.ok(logSync < cursorRename, 'the cursor was written before the log was durable');
    assert.ok(cursorDirSync > cursorRename && cursorDirSync < ack, 'the ack went out before the cursor was durable');
  },

  'T13b: a lost tail is appended once, even when the supervisor dies after the append and before the cursor': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.log('l0\n');
    await fx.step();
    await finishWithLog(fx, 'l0\nl1\n');
    await crashAt(fx, 'afterFill');
    assert.equal(logText(fx), 'l0\nl1\n', 'the append reached the file');
    assert.equal((await fx.step()).state, 'settled');
    assert.equal(logText(fx), 'l0\nl1\n', 'the replay did not append it a second time');
  },

  'T13b: a gap inside the streamed segment appends a marker and the whole verified log': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.log('l0\nXX\n');
    await fx.step();
    await finishWithLog(fx, 'l0\nl1\n');
    await fx.step();
    assert.equal(logText(fx), `l0\nXX\n${GAP_MARKER}l0\nl1\n`);
  },

  'T13b: a prior epoch prefix stays byte-identical and the fill touches only the current epoch': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.log('e1\n');
    await fx.step();
    await fx.runner.release('never-started');
    await fx.runner.claim();
    await fx.step();
    await fx.runner.heartbeat();
    await fx.runner.start();
    await fx.runner.log('e2\n');
    await fx.step();
    const restarted = '[lane] restarted on a1\n';
    assert.equal(logText(fx), `e1\n${restarted}e2\n`);
    assert.deepEqual([readCursor(fx.state, fx.id).epoch, readCursor(fx.state, fx.id).epochStartBytes], [2, Buffer.byteLength(`e1\n${restarted}`)]);
    await finishWithLog(fx, 'e2\ne3\n');
    await fx.step();
    assert.equal(logText(fx), `e1\n${restarted}e2\ne3\n`);
  },

  'T13b: a log shorter than the cursor is a detected loss, healed by the verified log after a marker': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.log('l0\n');
    await fx.step();
    fs.truncateSync(fx.logPath, 1);
    await finishWithLog(fx, 'l0\nl1\n');
    await fx.step();
    assert.equal(logText(fx), `l${GAP_MARKER}l0\nl1\n`);
    assert.equal(readCursor(fx.state, fx.id).logLossDetected, true);
  },

  'T13b: a log at its cap stays bounded with one truncation marker and the fill leaves it alone': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    const chunks = ['a', 'b', 'c'].map((c) => c.repeat(60));
    for (const chunk of chunks) await fx.runner.log(chunk);
    await fx.step({ logCapBytes: 100 });
    const capped = logText(fx);
    assert.equal((capped.match(MARKER_RE) ?? []).length, 1);
    assert.equal(capped.indexOf('\n[lane-broker]'), 100, 'the log holds exactly the cap before the marker');
    await finishWithLog(fx, chunks.join(''));
    await fx.step({ logCapBytes: 100 });
    assert.equal(logText(fx), capped);
    assert.equal(readCursor(fx.state, fx.id).logBytes, capped.length);
  },

  'T13c: a chunk is acked only while the seqs are contiguous, so a lower seq committing later is never lost; a gap that never fills is skipped after the hold': async (load, w, t) => {
    const late = await pullFixture(t, w, { load });
    await late.hold();
    await late.runner.log('a\n', 0);
    await late.runner.log('c\n', 2);
    await late.step();
    assert.equal(logText(late), 'a\n', 'seq 2 waits behind the gap');
    assert.equal(readCursor(late.state, late.id).ackedSeq, 0);
    assert.equal(await chunkCount(late), 1, 'seq 2 is still in the queue, not acked');
    await late.runner.log('b\n', 1);
    await late.step();
    assert.equal(logText(late), 'a\nb\nc\n');
    assert.equal(readCursor(late.state, late.id).ackedSeq, 2);
    assert.equal(await chunkCount(late), 0);
    await late.runner.finish();

    const never = await next(t, w, load, 2, late);
    await never.hold();
    await never.runner.log('a\n', 0);
    await never.runner.log('c\n', 2);
    await never.step();
    never.clock.now += 11_000;
    await never.step();
    assert.equal(logText(never), 'a\nc\n');
    assert.deepEqual([readCursor(never.state, never.id).ackedSeq, readCursor(never.state, never.id).logGapSkipped], [2, true]);
  },

  'a gap fill with no cursor writes the cursor first, so a crash after the append does not duplicate the log': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithLog(fx, 'l0\nl1\n');
    await crashAt(fx, 'afterLedger');
    fs.rmSync(cursorFile(fx.state, fx.id));
    await crashAt(fx, 'afterFill');
    assert.equal(logText(fx), 'l0\nl1\n');
    assert.equal((await fx.step()).state, 'settled');
    assert.equal(logText(fx), 'l0\nl1\n', 'the replay did not append it again');
  },

  'V4-2: the file is reconciled to the cursor before the writer exists, so a replay past a cap-hit crash has one marker and the replayed tail': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    const committed = 'c'.repeat(400);
    await fx.runner.log(committed);
    await fx.step({ logCapBytes: 2000 });
    assert.equal(readCursor(fx.state, fx.id).logBytes, 400);
    const replay = ['x', 'y', 'z'].map((c) => c.repeat(600));
    for (const chunk of replay) await fx.runner.log(chunk);
    fs.appendFileSync(fx.logPath, `${'u'.repeat(1600)}\n[lane-broker] log truncated at 50MB\n`);
    await fx.step({ logCapBytes: 2000 });
    const text = logText(fx);
    assert.equal(text.slice(0, 400), committed);
    assert.equal(text.slice(400, 2000), replay.join('').slice(0, 1600), 'the replayed tail, not the uncommitted bytes');
    assert.equal(text.match(MARKER_RE).length, 1);
    assert.equal(text.indexOf('\n[lane-broker]'), 2000);
  },
};

const CANARIES = [
  ['T13c: a chunk is acked only while the seqs are contiguous, so a lower seq committing later is never lost; a gap that never fills is skipped after the hold', 'pull-ticket.js', ['    if (chunk.seq !== firstSeq + kept.length) break;\n', '']],
  ['T13c: a chunk is acked only while the seqs are contiguous, so a lower seq committing later is never lost; a gap that never fills is skipped after the hold', 'pull-ticket.js', ['        if (run.now() - since < run.cfg.logGapHoldMs) {', '        if (true) {']],
  ['a gap fill with no cursor writes the cursor first, so a crash after the append does not duplicate the log', 'pull-ticket.js', ['    if (session.cursor === null) writeCursor(run.root, run.id, cursor);\n', '']],
  ['T13: a log replayed after a restart has every line exactly once, whichever point the supervisor died at', 'pull-log-cursor.js', ['    fs.truncateSync(logPath, cursor.logBytes);\n', '']],
  ['T13: a log replayed after a restart has every line exactly once, whichever point the supervisor died at', 'pull-log-cursor.js', ['export const INITIAL_ACKED_SEQ = -1;', 'export const INITIAL_ACKED_SEQ = 0;']],
  ['T13: a log replayed after a restart has every line exactly once, whichever point the supervisor died at', 'pull-log-cursor.js', ['logBytes: fileSize(logPath) };', 'logBytes: cursor.logBytes };']],
  ['T13: the log is fsynced before the cursor, the cursor and its directory before the ack', 'pull-ticket.js', ['const durable = await writer.durable();', 'const durable = true;']],
  ['T13b: a lost tail is appended once, even when the supervisor dies after the append and before the cursor', 'pull-log-cursor.js', ['    fs.truncateSync(logPath, cursor.logBytes);\n', '']],
  ['T13b: a log shorter than the cursor is a detected loss, healed by the verified log after a marker', 'pull-log-cursor.js', ['if (size > cursor.logBytes) {', 'if (size !== cursor.logBytes) {']],
  ['T13b: a log at its cap stays bounded with one truncation marker and the fill leaves it alone', 'supervisor.js', ['if (this.bytes + chunk.length > this.capBytes) {', 'if (this.bytes + chunk.length > this.capBytes + 1_000_000) {']],
  ['V4-2: the file is reconciled to the cursor before the writer exists, so a replay past a cap-hit crash has one marker and the replayed tail', 'pull-ticket.js', [
    '  const cursor = stored === null ? null : reconcileLogFile(run.root, run.id, run.logPath, stored);\n  return { cursor, writer: new CappedLogWriter(run.logPath, { capBytes: run.logCapBytes, deriveTruncatedFromMarker: true }) };',
    '  const writer = new CappedLogWriter(run.logPath, { capBytes: run.logCapBytes, deriveTruncatedFromMarker: true });\n  const cursor = stored === null ? null : reconcileLogFile(run.root, run.id, run.logPath, stored);\n  return { cursor, writer };',
  ]],
];

definePullSuite('pull ticket log cursor on a disposable Postgres', SCENARIOS, CANARIES);
