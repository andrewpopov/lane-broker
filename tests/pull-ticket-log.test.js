import assert from 'node:assert/strict';
import fs from 'node:fs';
import { Writable } from 'node:stream';
import path from 'node:path';
import { paths } from '../src/state.js';
import { sha256Hex } from '../src/s3-presign.js';
import { ARTIFACT_PATH, artifactsStream, definePullSuite, pullFixture } from './helpers/pull-fixture.js';
import { tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 rev 6: nothing streams; the whole log is fetched at settle from the key both sides derive, and written through CappedLogWriter. */

const CRASH = new Error('killed behind the log write');
const MARKER_RE = /\[lane-broker\] log truncated/g;
const FULL_LOG = 'line one\nline two\nline three\n';
const logText = (fx) => (fs.existsSync(fx.logPath) ? fs.readFileSync(fx.logPath, 'utf8') : null);
const finishWithLog = async (fx, log = FULL_LOG) => fx.runner.finish(await fx.runner.putOutputs({ log }));

const SCENARIOS = {
  'the full log is fetched at settle from the derived key and forwarded once; nothing is read from the queue before then': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    assert.equal((await fx.step()).state, 'running');
    assert.equal(logText(fx), null, 'a running ticket has written no log: nothing streams');
    await finishWithLog(fx);
    const forwarded = [];
    const settled = await fx.step({ forward: { write: (bytes) => forwarded.push(Buffer.from(bytes)) } });
    assert.equal(settled.result.exit, 0);
    assert.equal(logText(fx), FULL_LOG);
    assert.equal(Buffer.concat(forwarded).toString(), FULL_LOG, 'the caller sees the log as it is written');
    assert.ok(fx.stub.requests.some((r) => r.method === 'GET' && r.path === `/lane-out/${fx.runner.host}/${fx.id}/e1/log`), 'the log came from the derived key');
  },

  'the log goes through the capped writer: past the cap it is cut with one truncation marker': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithLog(fx, 'x'.repeat(40));
    await fx.step({ logCapBytes: 10 });
    const text = logText(fx);
    assert.ok(text?.startsWith('x'.repeat(10)), 'the first 10 bytes are kept');
    assert.equal(text.indexOf('x'.repeat(11)), -1, 'nothing past the cap is kept');
    assert.equal((text.match(MARKER_RE) ?? []).length, 1);
  },

  'a runner-confirmed cancel brings its log home too': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.queue.cancel(fx.id);
    await fx.runner.finish({ kind: 'cancelled', exit: undefined, cancelConfirmed: true, ...(await fx.runner.putOutputs({ log: FULL_LOG })) });
    const settled = await fx.step();
    assert.equal(settled.decision.kind, 'cancelled');
    assert.equal(logText(fx), FULL_LOG);
  },

  'a replay behind the log write rewrites from where the log began: once, after whatever was already there': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    fs.writeFileSync(fx.logPath, 'lane: queued for pull dispatch\n');
    await finishWithLog(fx);
    fx.hooks.afterFill = () => { throw CRASH; };
    await assert.rejects(fx.step(), CRASH);
    assert.equal(logText(fx), `lane: queued for pull dispatch\n${FULL_LOG}`);
    delete fx.hooks.afterFill;
    await fx.step();
    assert.equal(logText(fx), `lane: queued for pull dispatch\n${FULL_LOG}`, 'the replay did not append the log a second time');
    assert.equal(fx.published().exit, 0);
  },
  'a ledger rebuild after the attempt is gone rewrites the log from the size recorded in the entry, not from zero': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    fs.writeFileSync(fx.logPath, 'lane: queued for pull dispatch\n');
    await finishWithLog(fx);
    fx.hooks.afterFill = () => { throw CRASH; };
    await assert.rejects(fx.step(), CRASH);
    delete fx.hooks.afterFill;
    fs.rmSync(path.join(paths(fx.state).attempts, `${fx.id}.json`));
    const out = await fx.step();
    assert.deepEqual([out.state, out.reconstructed], ['settled', true]);
    assert.equal(logText(fx), `lane: queued for pull dispatch\n${FULL_LOG}`);
  },

  'a cancel whose log does not match its hash still says so in the published result': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.queue.cancel(fx.id);
    await fx.runner.put('log', Buffer.from('evil\n'));
    await fx.runner.finish({ kind: 'cancelled', exit: undefined, cancelConfirmed: true, logSha256: sha256Hex('good\n'), logBytes: 5 });
    const settled = await fx.step();
    assert.equal(settled.decision.kind, 'cancelled');
    assert.match(fx.published().remoteLogWarning, /does not match/);
  },

  'a runner\'s claim about its own object is bounded before anything is downloaded: past the limit or with no size is a warning, never a read': async (load, w, t) => {
    const refused = async (n, fields, key, pattern) => {
      const fx = await pullFixture(t, w, { load, id: tid(n) });
      await fx.hold();
      await fx.runner.put('log', Buffer.from('small\n'));
      await fx.runner.put('artifacts', artifactsStream(fx.id, { [ARTIFACT_PATH]: '{}' }));
      await fx.runner.finish(fields);
      const settled = await fx.step();
      assert.equal(settled.result.exit, 0);
      assert.match(settled.result[key], pattern);
      assert.equal(fx.stub.requests.filter((r) => r.method === 'GET' && /\/(log|artifacts)$/.test(r.path)).length, 0, 'nothing was downloaded');
    };
    const sha = sha256Hex('small\n');
    await refused(1, { logSha256: sha, logBytes: 60 * 1024 * 1024 }, 'remoteLogWarning', /past the \d+ byte limit/);
    await refused(2, { artifactsSha256: sha, artifactsBytes: 100 * 1024 * 1024 }, 'remoteArtifactsWarning', /past the \d+ byte limit/);
    await refused(3, { logSha256: sha }, 'remoteLogWarning', /names no size/);
  },

  'a disk error after backpressure ends the log write with a warning instead of holding the ticket lock forever': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithLog(fx, 'z'.repeat(3 * 1024 * 1024));
    const real = fs.createWriteStream;
    fs.createWriteStream = (target, ...rest) => (String(target) === fx.logPath
      ? new Writable({ highWaterMark: 16, write(_chunk, _encoding, done) { setImmediate(() => done(Object.assign(new Error('ENOSPC: no space left on device'), { code: 'ENOSPC' }))); } })
      : real(target, ...rest));
    try {
      const settled = await Promise.race([fx.step(), new Promise((_, reject) => setTimeout(() => reject(new Error('still running after 8000ms: the write never ended')), 8000))]);
      assert.equal(settled.state, 'settled');
      assert.match(fx.published().remoteLogWarning, /could not be written: ENOSPC/);
      assert.equal(fx.published().exit, 0, 'the run\'s own exit is unchanged by a log that could not be kept');
    } finally {
      fs.createWriteStream = real;
    }
  },

  'an fsync error when the log is made durable is a warning too, even though the write itself succeeded': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithLog(fx);
    const real = fs.fsync;
    fs.fsync = (fd, callback) => callback(Object.assign(new Error('EIO: i/o error, fsync'), { code: 'EIO' }));
    try {
      const settled = await fx.step();
      assert.equal(settled.state, 'settled');
    } finally {
      fs.fsync = real;
    }
    assert.match(fx.published().remoteLogWarning, /could not be written: EIO/);
    assert.equal(fx.published().exit, 0);
  },

  'the verified log is streamed into the writer in bounded chunks and never read whole': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    const big = 'y'.repeat(3.5 * 1024 * 1024);
    await finishWithLog(fx, big);
    const forwarded = [];
    const wholeReads = [];
    const realRead = fs.readFileSync;
    fs.readFileSync = (target, ...rest) => {
      if (String(target).includes(`${path.sep}stage${path.sep}`)) wholeReads.push(String(target));
      return realRead(target, ...rest);
    };
    try {
      await fx.step({ forward: { write: (bytes) => forwarded.push(bytes.length) } });
    } finally {
      fs.readFileSync = realRead;
    }
    assert.deepEqual(wholeReads, [], 'the staged log was never read into memory whole');
    assert.equal(forwarded.reduce((a, b) => a + b, 0), big.length);
    assert.ok(forwarded.length >= 4 && Math.max(...forwarded) <= 1024 * 1024, `chunks ${forwarded}`);
    assert.equal(fs.statSync(fx.logPath).size, big.length);
  },
};

const CANARIES = [
  ['the full log is fetched at settle from the derived key and forwarded once; nothing is read from the queue before then', 'pull-ticket.js', ["  if (entry.decision.kind === 'finished' || entry.decision.kind === 'cancelled') await writeFullLog(run, entry, warnings);\n", '']],
  ['the full log is fetched at settle from the derived key and forwarded once; nothing is read from the queue before then', 'pull-ticket.js', ['    forward?.write(chunk);\n', '']],
  ['the log goes through the capped writer: past the cap it is cut with one truncation marker', 'pull-ticket.js', ['{ capBytes: run.logCapBytes, deriveTruncatedFromMarker: true }', '{ deriveTruncatedFromMarker: true }']],
  ['a runner-confirmed cancel brings its log home too', 'pull-ticket.js', ["entry.decision.kind === 'finished' || entry.decision.kind === 'cancelled') await writeFullLog", "entry.decision.kind === 'finished') await writeFullLog"]],
  ['a runner-confirmed cancel brings its log home too', 'pull-ticket.js', ["OUTPUTS.map(([name]) => name) : ['log'];", 'OUTPUTS.map(([name]) => name) : [];']],
  ['a replay behind the log write rewrites from where the log began: once, after whatever was already there', 'pull-ticket.js', ['  truncateLogTo(run.logPath, entry.logBase ?? 0);\n', '']],
  ['a replay behind the log write rewrites from where the log began: once, after whatever was already there', 'pull-ticket.js', ['    logBase: logSizeOf(run.logPath),\n', '']],
  ['a ledger rebuild after the attempt is gone rewrites the log from the size recorded in the entry, not from zero', 'pull-ticket.js', ['truncateLogTo(run.logPath, entry.logBase ?? 0);', 'truncateLogTo(run.logPath, 0);']],
  ['a ledger rebuild after the attempt is gone rewrites the log from the size recorded in the entry, not from zero', 'pull-ticket.js', ['    logBase: logSizeOf(run.logPath),\n', '']],
  ['a cancel whose log does not match its hash still says so in the published result', 'pull-ticket.js', ["transport: 'pull', remoteCancelConfirmed: true, ...extra };", "transport: 'pull', remoteCancelConfirmed: true };"]],
  ['a runner\'s claim about its own object is bounded before anything is downloaded: past the limit or with no size is a warning, never a read', 'pull-ticket.js', ['  if (refused !== null) return { ok: false, retry: false, reason: refused };\n', '']],
  ['a runner\'s claim about its own object is bounded before anything is downloaded: past the limit or with no size is a warning, never a read', 'pull-ticket.js', ["return expected.bytes > cap ? `the result names ${expected.bytes} bytes, past the ${cap} byte limit` : null;", 'return null;']],
  ['a runner\'s claim about its own object is bounded before anything is downloaded: past the limit or with no size is a warning, never a read', 'pull-ticket.js', ["  if (!Number.isInteger(expected.bytes) || expected.bytes < 0) return 'the result names no size for the object';\n", '']],
  ['a runner\'s claim about its own object is bounded before anything is downloaded: past the limit or with no size is a warning, never a read', 'pull-ticket.js', ['.maxTotalBytes) + OUTPUT_MARGIN_BYTES;', '.maxTotalBytes * 1000) + OUTPUT_MARGIN_BYTES;']],
  ['a disk error after backpressure ends the log write with a warning instead of holding the ticket lock forever', 'pull-ticket.js', ['    if (!writer.write(chunk) && !writer.failed) await drained(writer.stream);', "    if (!writer.write(chunk) && !writer.failed) await new Promise((resolve) => writer.stream.once('drain', resolve));"], /still running/],
  ['a disk error after backpressure ends the log write with a warning instead of holding the ticket lock forever', 'pull-ticket.js', ["    await writer.durable();\n    if (writer.failed) warnings.log = `the log could not be written: ${writer.error?.message ?? 'write failed'}`;\n", '']],
  ['an fsync error when the log is made durable is a warning too, even though the write itself succeeded', 'pull-ticket.js', ["    await writer.durable();\n    if (writer.failed) warnings.log = `the log could not be written: ${writer.error?.message ?? 'write failed'}`;", "    if (writer.failed) warnings.log = `the log could not be written: ${writer.error?.message ?? 'write failed'}`;\n    await writer.durable();"]],
  ['the verified log is streamed into the writer in bounded chunks and never read whole', 'pull-ticket.js', ['for await (const chunk of fs.createReadStream(file, { highWaterMark: STREAM_CHUNK_BYTES })) {', 'for (const chunk of [fs.readFileSync(file)]) {']],
];

definePullSuite('pull ticket full log at settle on a disposable Postgres', SCENARIOS, CANARIES);
