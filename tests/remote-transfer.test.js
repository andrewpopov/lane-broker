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
import { createStrictGunzip, parseTransferFlag } from '../src/remote-gzip.js';
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

test('remote-transfer: gzip cut inside the header creates no ticket and exits 1 (never started)', async () => {
  const { dir, entries } = makeSource({ 'a.txt': 'x' });
  const header = makeHeader();
  const gz = zlib.gzipSync(await rawStream(dir, header, entries));
  const out = await gzipExec(gz.subarray(0, 14), header);
  assert.equal(out.code, 1);
  assert.deepEqual(out.dirs, []);
  assert.match(out.stderr, /lane remote-exec:/);
});

test('remote-transfer: a corrupt gzip body is rejected', async () => {
  const { dir, entries } = makeSource({ 'a.txt': crypto.randomBytes(50_000) });
  const header = makeHeader();
  const gz = Buffer.from(zlib.gzipSync(await rawStream(dir, header, entries)));
  gz[Math.floor(gz.length / 2)] ^= 0xff;
  const out = await gzipExec(gz, header);
  assert.equal(out.code, 0, out.stderr);
  assert.equal(out.result.kind, 'rejected');
});

test('remote-transfer: not-gzip bytes with --transfer gzip create no ticket', async () => {
  const { dir, entries } = makeSource({ 'a.txt': 'x' });
  const header = makeHeader();
  const out = await gzipExec(await rawStream(dir, header, entries), header);
  assert.equal(out.code, 1);
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

test('remote-transfer: a 64 MiB highly compressible file delivered in 1 KiB chunks is read in linear time', async () => {
  const total = 64 * 1024 * 1024;
  const piece = Buffer.alloc(1024);
  async function* chunks() {
    yield Buffer.from('header\n');
    for (let sent = 0; sent < total; sent += piece.length) yield piece;
    yield Buffer.from('tail\n');
  }
  const reader = makeReader(Readable.from(chunks()));
  const started = Date.now();
  assert.equal(await reader.readLine(1000), 'header');
  const body = await reader.readBytes(total);
  assert.equal(body.length, total);
  assert.equal(await reader.readLine(1000), 'tail');
  assert.equal(await reader.hasMore(), false);
  assert.ok(Date.now() - started < 5000, `took ${Date.now() - started} ms`);
});

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

test('remote-transfer: a pre-header failure on the runner is reported as never started and safe to retry elsewhere', async () => {
  // A runner whose exec dies before creating any ticket (here: it is handed a corrupt gzip by a transport bug) has nothing to
  // report; the client must still prove "never started" via the withdraw tombstone rather than leave the dispatch possibly running.
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
    canWithdraw: true,
    deadlines: { transferMs: 20_000, resultMs: 5000, resultAttempts: 2 },
  });
  assert.equal(result.outcome, 'unconfirmed');
  assert.equal(result.neverStarted, true);
  assert.equal(result.retryElsewhere, true);
  assert.deepEqual(ticketDirs(root), []);
});
