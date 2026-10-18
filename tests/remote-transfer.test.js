import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { spawn, execFileSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { freshEnv, writeGlobalConfig, BIN, laneRun, sleep } from './helpers.js';
import { tmpDir, setup, makeFakeSshBin, makeRunner, clientEnv, makeGitWorktree, makeDispatchArgs, markerCmd, detachAndWait } from './remote-harness.js';
import { dispatchRemote } from '../src/remote-client.js';
import { buildManifest } from '../src/remote-manifest.js';
import { encodeSnapshot, makeReader, extractSnapshot } from '../src/remote-stream.js';
import { createStrictGunzip, parseTransferFlag, REMOTE_EXEC_NO_TICKET_EXIT } from '../src/remote-gzip.js';
import { CAPABILITIES } from '../src/capabilities.js';

/** BRAIN-642 slice B: the `snapshot-gzip/1` capability and the gzip transfer, client and runner side. */

function sha(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function makeSource(files) {
  const dir = tmpDir('remote-transfer-src');
  const entries = [];
  for (const [rel, content] of Object.entries(files)) {
    const buf = Buffer.isBuffer(content) ? content : Buffer.from(content, 'utf8');
    fs.writeFileSync(path.join(dir, rel), buf);
    entries.push({ path: rel, type: 'file', exec: false, size: buf.length, sha256: sha(buf) });
  }
  return { dir, entries };
}

function makeHeader(overrides = {}) {
  return { ticketId: crypto.randomUUID(), generation: 0, repoKey: 'remote-transfer-repo', lane: 'default', argv: [process.execPath, '-e', 'process.exit(0)'], relCwd: '', ...overrides };
}

async function rawStream(src, header, entries) {
  const chunks = [];
  for await (const c of encodeSnapshot(src, header, entries)) chunks.push(c);
  return Buffer.concat(chunks);
}

const spawned = [];
afterEach(() => {
  for (const child of spawned.splice(0)) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
});

function shadowEnv(extra) {
  const f = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '1000', ...extra });
  writeGlobalConfig(f.home, {});
  return f;
}

/** Spawn the real `lane remote-exec` with `args` appended, feeding it `bytes`; resolves when it exits. */
function remoteExec(bytes, { env, root, args = [] }) {
  const child = spawn(process.execPath, [BIN, 'remote-exec', ...args, '--root', root], { env });
  spawned.push(child);
  let stderr = '';
  child.stderr.on('data', (d) => { stderr += d; });
  child.stdin.on('error', () => {});
  child.stdin.end(bytes);
  return new Promise((resolve) => child.on('close', (code) => resolve({ code, stderr })));
}

async function resultOf(ticketId, root, env) {
  const { stdout } = await laneRun(['remote-result', ticketId, '--root', root], { env });
  return JSON.parse(stdout.trim());
}

const ticketDirs = (root) => (fs.existsSync(path.join(root, 'tickets')) ? fs.readdirSync(path.join(root, 'tickets')) : []);

/** Runs a gzip transfer of `gz` through the runner and returns the runner exit, plus the result record when a header got through. */
async function gzipExec(gz, header, { limit } = {}) {
  const { env } = shadowEnv(limit ? { LANE_BROKER_TEST_TRANSFER_MAX_BYTES: String(limit) } : undefined);
  const root = tmpDir('remote-transfer-root');
  const run = await remoteExec(gz, { env, root, args: ['--transfer', 'gzip'] });
  const dirs = ticketDirs(root);
  const result = dirs.length > 0 ? await resultOf(header.ticketId, root, env) : null;
  return { ...run, dirs, result };
}

// ---- the capability gate (client side, through the real CLI and a tee'd ssh) ----

test('remote-transfer: capable runner receives a gzip stream (first bytes 1f8b) smaller than half the raw tree', async () => {
  assert.ok(CAPABILITIES.includes('snapshot-gzip/1'));
  const ctx = setup({ ssh: 'tee' });
  fs.writeFileSync(path.join(ctx.repoDir, 'big.txt'), 'const answer = 42; // a compressible line\n'.repeat(20_000));
  const marker = path.join(tmpDir('marker'), 'where');
  const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], ctx.env, ctx.repoDir);
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const sent = fs.readFileSync(ctx.teePath);
  assert.deepEqual([...sent.subarray(0, 2)], [0x1f, 0x8b]);
  const raw = zlib.gunzipSync(sent);
  assert.equal(raw[0], 0x7b);
  assert.ok(raw.length > 800_000);
  assert.ok(sent.length < raw.length / 2, `sent ${sent.length} of raw ${raw.length}`);
});

test('remote-transfer: a runner without the capabilities receives a legacy stream', async () => {
  const ctx = setup({ ssh: 'strict-legacy' });
  fs.writeFileSync(path.join(ctx.repoDir, 'big.txt'), 'const answer = 42; // a compressible line\n'.repeat(20_000));
  const marker = path.join(tmpDir('marker'), 'where');
  const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], ctx.env, ctx.repoDir);
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  assert.equal(fs.readFileSync(ctx.teePath)[0], 0x7b);
  assert.equal(fs.readFileSync(ctx.violationsPath, 'utf8'), '', 'a legacy runner must see no new flag, key or compressed byte');
});

test('remote-transfer: the gzip level is a validated global config key', async () => {
  const { loadGlobalConfig, DEFAULT_GLOBAL_CONFIG } = await import('../src/config.js');
  assert.equal(DEFAULT_GLOBAL_CONFIG.remoteSnapshotGzipLevel, 3);
  for (const bad of [0, 10, 3.5, '3']) {
    const { env, home } = freshEnv();
    writeGlobalConfig(home, { remoteSnapshotGzipLevel: bad });
    const prior = { ...process.env };
    Object.assign(process.env, { LANE_BROKER_HOME: env.LANE_BROKER_HOME, LANE_BROKER_STATE: env.LANE_BROKER_STATE });
    try {
      assert.throws(() => loadGlobalConfig(), /remoteSnapshotGzipLevel/, `level ${JSON.stringify(bad)} must be refused`);
    } finally {
      process.env = prior;
    }
  }
});

test('remote-transfer: --transfer parsing accepts gzip, treats absence as legacy and rejects anything else', () => {
  assert.deepEqual(parseTransferFlag(['--root', 'x']), { ok: true, transfer: undefined });
  assert.deepEqual(parseTransferFlag(['--transfer', 'gzip', '--root', 'x']), { ok: true, transfer: 'gzip' });
  for (const args of [['--transfer', 'zstd'], ['--transfer'], ['--transfer', '']]) assert.equal(parseTransferFlag(args).ok, false);
});

test('remote-transfer: an unknown --transfer value is rejected by lane remote-exec before it reads anything', async () => {
  const { env } = shadowEnv();
  const root = tmpDir('remote-transfer-root');
  const run = await remoteExec(Buffer.alloc(0), { env, root, args: ['--transfer', 'zstd'] });
  assert.equal(run.code, 2);
  assert.match(run.stderr, /unknown --transfer value: zstd/);
  assert.deepEqual(ticketDirs(root), []);
});

// ---- the runner: old client, good gzip, and the failure matrix ----

test('remote-transfer: an old client (no --transfer flag, plain stream) still runs on the new runner', async () => {
  const { dir, entries } = makeSource({ 'a.txt': 'hello' });
  const header = makeHeader();
  const { env } = shadowEnv();
  const root = tmpDir('remote-transfer-root');
  const run = await remoteExec(await rawStream(dir, header, entries), { env, root });
  assert.equal(run.code, 0, run.stderr);
  assert.equal((await resultOf(header.ticketId, root, env)).kind, 'completed');
});

test('remote-transfer: a well-formed gzip stream with --transfer gzip runs to completion', async () => {
  const { dir, entries } = makeSource({ 'a.txt': 'hello'.repeat(10_000) });
  const header = makeHeader();
  const out = await gzipExec(zlib.gzipSync(await rawStream(dir, header, entries)), header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'completed');
});

test('remote-transfer: a gzip bomb is capped on decompressed bytes and rejected', async () => {
  const { dir, entries } = makeSource({ 'zeros.bin': Buffer.alloc(4 * 1024 * 1024) });
  const header = makeHeader();
  const gz = zlib.gzipSync(await rawStream(dir, header, entries));
  assert.ok(gz.length < 64 * 1024, 'the bomb must be small on the wire');
  const out = await gzipExec(gz, header, { limit: 1024 * 1024 });
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
  assert.match(out.result.reason, /decompressed stream exceeds 1048576 bytes/);
});

test('remote-transfer: compressed input past the compressed cap is rejected', async () => {
  const { dir, entries } = makeSource({ 'rand.bin': crypto.randomBytes(256 * 1024) });
  const header = makeHeader();
  const gz = zlib.gzipSync(await rawStream(dir, header, entries));
  // the decoded stream (~256 KiB + header) fits under 300 KiB; the compressed one (incompressible) does not fit under 100 KiB
  const { createStrictGunzip: make } = await import('../src/remote-gzip.js');
  const g = make({ maxDecodedBytes: 10 * 1024 * 1024, maxCompressedBytes: 100 * 1024 });
  const failed = new Promise((resolve) => g.output.on('error', resolve));
  g.output.resume();
  g.input.on('error', () => {});
  g.input.end(gz);
  assert.match((await failed).message, /compressed stream exceeds 102400 bytes/);
});

test('remote-transfer: truncated gzip after the header is rejected with a reason, not a crash', async () => {
  const { dir, entries } = makeSource({ 'a.txt': crypto.randomBytes(50_000) });
  const header = makeHeader();
  const gz = zlib.gzipSync(await rawStream(dir, header, entries));
  const out = await gzipExec(gz.subarray(0, Math.floor(gz.length * 0.7)), header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
  assert.ok(out.result.reason.length > 0);
});

test('remote-transfer: gzip cut inside the header creates no ticket and exits 66 (never started)', async () => {
  const { dir, entries } = makeSource({ 'a.txt': 'x' });
  const header = makeHeader();
  const gz = zlib.gzipSync(await rawStream(dir, header, entries));
  const out = await gzipExec(gz.subarray(0, 14), header);
  assert.equal(out.code, REMOTE_EXEC_NO_TICKET_EXIT);
  assert.deepEqual(out.dirs, []);
  assert.match(out.stderr, /lane remote-exec:/);
});

/** A gzip member big enough that the runner has the header (and made a ticket) long before the trailer arrives. */
async function bigMember() {
  const { dir, entries } = makeSource({ 'a.txt': crypto.randomBytes(400_000) });
  const header = makeHeader();
  return { header, gz: Buffer.from(zlib.gzipSync(await rawStream(dir, header, entries))) };
}

test('remote-transfer: a corrupt gzip body is rejected by the gzip layer, not by a file hash', async () => {
  const { header, gz } = await bigMember();
  gz[Math.floor(gz.length / 2)] ^= 0xff;
  const out = await gzipExec(gz, header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
  assert.doesNotMatch(out.result.reason, /sha|hash/i);
});

test('remote-transfer: flipping only a trailer CRC byte is rejected with a checksum mismatch', async () => {
  const { header, gz } = await bigMember();
  gz[gz.length - 8] ^= 0x01; // every decoded byte, and so every file hash, is still correct
  const out = await gzipExec(gz, header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
  assert.match(out.result.reason, /gzip trailer checksum mismatch/);
});

test('remote-transfer: a wrong trailer ISIZE is rejected with a size mismatch', async () => {
  const { header, gz } = await bigMember();
  gz.writeUInt32LE(999, gz.length - 4);
  const out = await gzipExec(gz, header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
  assert.match(out.result.reason, /gzip trailer size mismatch/);
});

test('remote-transfer: not-gzip bytes with --transfer gzip create no ticket', async () => {
  const { dir, entries } = makeSource({ 'a.txt': 'x' });
  const header = makeHeader();
  const out = await gzipExec(await rawStream(dir, header, entries), header);
  assert.equal(out.code, REMOTE_EXEC_NO_TICKET_EXIT);
  assert.deepEqual(out.dirs, []);
});

test('remote-transfer: appended empty gzip members after the first are rejected', async () => {
  // Large enough that the header is decoded and handed over long before the appended members arrive (so this is a post-header rejection).
  const { dir, entries } = makeSource({ 'a.txt': crypto.randomBytes(400_000) });
  const header = makeHeader();
  const gz = zlib.gzipSync(await rawStream(dir, header, entries));
  const out = await gzipExec(Buffer.concat([gz, zlib.gzipSync(Buffer.alloc(0)), zlib.gzipSync(Buffer.alloc(0))]), header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
  assert.match(out.result.reason, /compressed data after the end of the gzip member/);
});

test('remote-transfer: decoded trailing garbage after the terminator is rejected', async () => {
  const { dir, entries } = makeSource({ 'a.txt': 'hello' });
  const header = makeHeader();
  const raw = await rawStream(dir, header, entries);
  const out = await gzipExec(zlib.gzipSync(Buffer.concat([raw, Buffer.from('garbage\n')])), header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
  assert.match(out.result.reason, /trailing bytes after terminator/);
});

// ---- the reader and decompressor, direct ----

test('remote-transfer: the reader still splits lines and bodies containing newlines correctly across tiny chunks', async () => {
  const wire = Buffer.from('one\ntwo words\nBODY\nWITH\nNEWLINES\nlast\n');
  const tiny = Readable.from((function* () { for (let i = 0; i < wire.length; i += 3) yield wire.subarray(i, i + 3); })());
  const reader = makeReader(tiny);
  assert.equal(await reader.readLine(100), 'one');
  assert.equal(await reader.readLine(100), 'two words');
  assert.equal((await reader.readBytes(18)).toString(), 'BODY\nWITH\nNEWLINES');
  assert.equal(await reader.readLine(100), '');
  assert.equal(await reader.readLine(100), 'last');
  assert.equal(await reader.readLine(100), null);
});

test('remote-transfer: createStrictGunzip round-trips a stream written in tiny chunks', async () => {
  const data = crypto.randomBytes(300_000);
  const gz = zlib.gzipSync(data);
  const g = createStrictGunzip({ maxDecodedBytes: 10_000_000, maxCompressedBytes: 10_000_000 });
  const parts = [];
  g.output.on('data', (d) => parts.push(d));
  const done = new Promise((resolve, reject) => { g.output.on('end', resolve); g.output.on('error', reject); });
  for (let i = 0; i < gz.length; i += 777) g.input.write(gz.subarray(i, i + 777));
  g.input.end();
  await done;
  assert.ok(Buffer.concat(parts).equals(data));
});

/** Drive createStrictGunzip by hand: `settled` is 'end', or the error, once the output finished; `ended()` peeks without waiting. */
function driveGunzip() {
  const g = createStrictGunzip({ maxDecodedBytes: 10_000_000, maxCompressedBytes: 10_000_000 });
  const parts = [];
  let verdict = null;
  g.output.on('data', (d) => parts.push(d));
  g.output.on('end', () => { verdict = verdict ?? 'end'; });
  g.output.on('error', (err) => { verdict = verdict ?? err; });
  g.input.on('error', () => {});
  return { g, decoded: () => Buffer.concat(parts), verdict: () => verdict };
}

test('remote-transfer: createStrictGunzip does not finish on a full deflate payload plus one trailer byte, and rejects the EOF', async () => {
  const data = crypto.randomBytes(100_000);
  const gz = zlib.gzipSync(data);
  const d = driveGunzip();
  d.g.input.write(gz.subarray(0, gz.length - 7));
  await sleep(200);
  assert.equal(d.verdict(), null, 'must not signal end-of-input before the whole trailer');
  assert.ok(d.decoded().equals(data));
  d.g.input.end();
  await sleep(100);
  assert.match(String(d.verdict()?.message), /truncated gzip stream: missing trailer/);
});

test('remote-transfer: createStrictGunzip accepts the stream only once a delayed trailer arrives', async () => {
  const data = crypto.randomBytes(100_000);
  const gz = zlib.gzipSync(data);
  const d = driveGunzip();
  d.g.input.write(gz.subarray(0, gz.length - 8));
  await sleep(200);
  assert.equal(d.verdict(), null);
  d.g.input.write(gz.subarray(gz.length - 8));
  d.g.input.end();
  await sleep(100);
  assert.equal(d.verdict(), 'end');
  assert.ok(d.decoded().equals(data));
});

test('remote-transfer: createStrictGunzip rejects a wrong CRC, a wrong ISIZE and any byte after the trailer', async () => {
  const gz = zlib.gzipSync(crypto.randomBytes(50_000));
  const badCrc = Buffer.from(gz);
  badCrc[badCrc.length - 8] ^= 0x01;
  const badSize = Buffer.from(gz);
  badSize.writeUInt32LE(7, badSize.length - 4);
  const cases = [
    [badCrc, /gzip trailer checksum mismatch/],
    [badSize, /gzip trailer size mismatch/],
    [Buffer.concat([gz, Buffer.from([0])]), /compressed data after the end of the gzip member/],
    [gz.subarray(0, gz.length - 3), /truncated gzip stream: missing trailer/],
  ];
  for (const [bytes, reason] of cases) {
    const d = driveGunzip();
    d.g.input.end(bytes);
    await sleep(150);
    assert.match(String(d.verdict()?.message), reason);
  }
});

test('remote-transfer: the reader\'s cost per one-byte chunk does not grow with the number of chunks queued', async () => {
  // Array.prototype.shift is cheap on a short array and memmoves on a long one, so the comparison spans a 12x range of chunk counts.
  const drain = async (n) => {
    const reader = makeReader(Readable.from((function* () { for (let i = 0; i < n; i++) yield Buffer.from([65]); })()));
    const started = process.hrtime.bigint();
    assert.equal((await reader.readBytes(n)).length, n);
    assert.equal(await reader.hasMore(), false);
    return Number(process.hrtime.bigint() - started) / n;
  };
  await drain(5_000); // warm up
  const small = Math.min(await drain(16_000), await drain(16_000));
  const large = Math.min(await drain(192_000), await drain(192_000));
  assert.ok(large / small < 3, `per-chunk cost grew ${(large / small).toFixed(1)}x for 12x the chunks`);
});

// ---- the client: upstream failure, cancellation, teardown ----

function psLines(needle) {
  return execFileSync('ps', ['-axo', 'pid=,command=']).toString().split('\n').filter((l) => l.includes(needle) && !l.includes('ps -axo'));
}

test('remote-transfer: an upstream encode failure (same-size mutation after the manifest) fails the transfer and leaves no ssh child', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'aaaa'.repeat(1000), 'b.txt': 'bbbb'.repeat(1000) });
  const manifest = buildManifest(src);
  fs.writeFileSync(path.join(src, 'b.txt'), 'cccc'.repeat(1000)); // same size, different bytes
  const root = tmpDir('remote-transfer-client-root');
  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'tee', root }),
    worktreeRoot: src,
    manifest,
    sshBin,
    env,
    transfer: 'gzip',
    gzipLevel: 3,
    deadlines: { transferMs: 20_000, resultMs: 5000, resultAttempts: 1 },
  });
  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /snapshot stream error: file changed content on disk/);
  await sleep(300);
  assert.deepEqual(psLines(root), [], 'no ssh process may survive the failed transfer');
});

test('remote-transfer: cancelling mid-transfer tears down every stage and the ssh child', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'rand.bin': crypto.randomBytes(24 * 1024 * 1024) });
  const root = tmpDir('remote-transfer-client-root');
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 1500);
  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'no-read', root }),
    worktreeRoot: src,
    sshBin,
    env,
    transfer: 'gzip',
    abortSignal: controller.signal,
    deadlines: { transferMs: 60_000, resultMs: 5000, resultAttempts: 1 },
  });
  assert.equal(result.outcome, 'cancelled');
  await sleep(300);
  assert.deepEqual(psLines(root), []);
});

test('remote-transfer: the client reports raw and sent byte counts for a gzip dispatch', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'same line again\n'.repeat(50_000) });
  let stats;
  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'tee', root: tmpDir('remote-transfer-client-root') }),
    worktreeRoot: src,
    sshBin,
    env,
    transfer: 'gzip',
    onTransferStats: (s) => { stats = s; },
  });
  assert.equal(result.outcome, 'confirmed');
  assert.equal(stats.mode, 'gzip');
  assert.ok(stats.rawBytes > 700_000);
  assert.ok(stats.sentBytes > 0 && stats.sentBytes < stats.rawBytes / 4, JSON.stringify(stats));
});

test('remote-transfer: a pre-header failure on the runner is never started via its exit code alone, with no withdraw', async () => {
  // The runner refused the corrupt gzip before creating any ticket and exited 66; that exit code is the proof. canWithdraw is off, so
  // nothing else can have supplied it.
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const root = tmpDir('remote-transfer-client-root');
  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'corrupt-gzip', root }),
    worktreeRoot: src,
    sshBin,
    env,
    transfer: 'gzip',
    canWithdraw: false,
    deadlines: { transferMs: 20_000, resultMs: 5000, resultAttempts: 2 },
  });
  assert.equal(result.outcome, 'unconfirmed');
  assert.equal(result.neverStarted, true);
  assert.equal(result.retryElsewhere, true);
  assert.match(result.reason, /refused the snapshot stream before creating a ticket/);
  assert.deepEqual(ticketDirs(root), []);
});

async function dropDispatch(ssh) {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  return dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh, root: tmpDir('remote-transfer-client-root') }),
    worktreeRoot: makeGitWorktree({ 'a.txt': 'hello' }),
    sshBin,
    env,
    transfer: 'gzip',
    canWithdraw: true,
    deadlines: { transferMs: 20_000, resultMs: 5000, resultAttempts: 1 },
  });
}

test('remote-transfer: a tombstoned, absent ticket on withdraw is NOT proof it never started (a GC-removed finished ticket)', async () => {
  const result = await dropDispatch('drop-gone');
  assert.equal(result.outcome, 'unconfirmed');
  assert.notEqual(result.neverStarted, true);
  assert.notEqual(result.retryElsewhere, true);
  assert.equal(result.mayStillBeRunning, true);
});

test('remote-transfer: any other non-zero remote-exec exit stays unconfirmed', async () => {
  for (const ssh of ['drop-exit1', 'drop-stuck']) {
    const result = await dropDispatch(ssh);
    assert.equal(result.outcome, 'unconfirmed', ssh);
    assert.notEqual(result.neverStarted, true, ssh);
    assert.equal(result.mayStillBeRunning, true, ssh);
  }
});

test('remote-transfer: a throwing onTransferStats neither rejects the dispatch nor leaves the ssh child alive on abort', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const throwing = () => { throw new Error('stats sink broke'); };
  const done = await dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'tee', root: tmpDir('remote-transfer-client-root') }),
    worktreeRoot: makeGitWorktree({ 'a.txt': 'hello' }),
    sshBin,
    env,
    transfer: 'gzip',
    onTransferStats: throwing,
  });
  assert.equal(done.outcome, 'confirmed');

  const root = tmpDir('remote-transfer-client-root');
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 1500);
  const cancelled = await dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'no-read', root }),
    worktreeRoot: makeGitWorktree({ 'rand.bin': crypto.randomBytes(24 * 1024 * 1024) }),
    sshBin,
    env,
    transfer: 'gzip',
    abortSignal: controller.signal,
    onTransferStats: throwing,
    deadlines: { transferMs: 60_000, resultMs: 5000, resultAttempts: 1 },
  });
  assert.equal(cancelled.outcome, 'cancelled');
  await sleep(300);
  assert.deepEqual(psLines(root), []);
});
