import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, BIN, laneRun } from './helpers.js';
import { tmpDir, setup, makeFakeSshBin, makeRunner, clientEnv, makeGitWorktree, makeDispatchArgs, markerCmd, detachAndWait, linesOf } from './remote-harness.js';
import { dispatchRemote, haveDeadlineFor } from '../src/remote-client.js';
import { buildManifest } from '../src/remote-manifest.js';
import { repoIdentity } from '../src/config.js';
import { advertisedCapabilities, CAPABILITIES } from '../src/capabilities.js';
import { blobNamespace, blobPath, hasBlob, publishBlob, evictBlobs, leasedHashes, MIN_CACHEABLE_BYTES, BLOB_CACHE_CAPABILITY } from '../src/remote-blobs.js';

/** BRAIN-642 slice C2: `lane remote-have`, the `blob-cache/1` capability, the client's have-query and the one-shot repair retry. */

const NO_LOCK = (fn) => fn();
const PLENTY = { freeBytes: () => Number.MAX_SAFE_INTEGER };
const BIG = 5 * 1024 * 1024; // over the client's 4 MiB have-query threshold
const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
const entryOf = (name, buf) => ({ path: name, type: 'file', exec: false, size: buf.length, sha256: sha(buf) });
const blobOf = (seed, n = MIN_CACHEABLE_BYTES + 64) => crypto.createHash('sha512').update(seed).digest().toString('hex').repeat(Math.ceil(n / 128)).slice(0, n);

test('haveDeadlineFor: twice the probe round trip, clamped to 3 s .. 15 s', () => {
  for (const probe of [undefined, null, {}, { roundTripMs: NaN }, { roundTripMs: Infinity }, { roundTripMs: -5 }, { roundTripMs: '2000' }]) {
    assert.equal(haveDeadlineFor(probe), 3000);
  }
  assert.equal(haveDeadlineFor({ roundTripMs: 1000 }), 3000);
  assert.equal(haveDeadlineFor({ roundTripMs: 1700 }), 3400);
  assert.equal(haveDeadlineFor({ roundTripMs: 20000 }), 15000);
});

const spawned = [];
afterEach(() => {
  for (const child of spawned.splice(0)) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
});

// ---- `lane remote-have` (runner side, real CLI) ----

function runnerEnv(cfg = {}) {
  const f = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '1000' });
  writeGlobalConfig(f.home, cfg);
  return f.env;
}

function haveExec(input, { env, root }) {
  const child = spawn(process.execPath, [BIN, 'remote-have', '--root', root], { env });
  spawned.push(child);
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (d) => { stdout += d; });
  child.stderr.on('data', (d) => { stderr += d; });
  child.stdin.on('error', () => {});
  child.stdin.end(input);
  return new Promise((resolve) => child.on('close', (code) => resolve({ code, stdout, stderr })));
}

const haveRequest = (repoIdentityValue, hashes) => JSON.stringify({ v: 1, repoIdentity: repoIdentityValue, hashes });

function seedBlob(root, identity, seed) {
  const buf = Buffer.from(blobOf(seed));
  const entry = entryOf(`${seed}.bin`, buf);
  assert.equal(publishBlob(root, blobNamespace(identity), entry, buf, PLENTY), 'published');
  return entry;
}

test('remote-have: validation refuses a malformed request, and the input is capped at 8 MB', async () => {
  const env = runnerEnv();
  const root = tmpDir('have-runner');
  const good = 'a'.repeat(64);
  const bad = [
    'not json',
    JSON.stringify({ v: 2, repoIdentity: '/r/.git', hashes: [] }),
    JSON.stringify({ v: 1, hashes: [] }),
    JSON.stringify({ v: 1, repoIdentity: '', hashes: [] }),
    JSON.stringify({ v: 1, repoIdentity: '/r/.git', hashes: 'nope' }),
    haveRequest('/r/.git', [good, '../../victim']),
    haveRequest('/r/.git', ['A'.repeat(64)]),
    haveRequest('/r/.git', ['a'.repeat(63)]),
    haveRequest('/r/.git', [5]),
    ' '.repeat(8 * 1024 * 1024 + 10),
  ];
  for (const input of bad) {
    const r = await haveExec(input, { env, root });
    assert.equal(r.code, 2, `${input.slice(0, 60)} -> ${r.stdout}${r.stderr}`);
    assert.equal(r.stdout, '');
  }
  assert.equal(fs.existsSync(path.join(root, 'blobs')), false, 'a refused request touches no store');
  const ok = await haveExec(haveRequest('/r/.git', []), { env, root });
  assert.equal(ok.code, 0);
  assert.deepEqual(JSON.parse(ok.stdout), { v: 1, have: [] });
});

test('remote-have: reports only intact blobs of the same repo, removes a corrupt one, and leases every queried hash', async () => {
  const env = runnerEnv();
  const root = tmpDir('have-runner');
  const identity = '/repos/one/.git';
  const intact = seedBlob(root, identity, 'intact');
  const corrupt = seedBlob(root, identity, 'corrupt');
  const foreign = seedBlob(root, '/repos/two/.git', 'foreign');
  const absent = 'c'.repeat(64);
  fs.chmodSync(blobPath(root, blobNamespace(identity), corrupt.sha256), 0o600);
  const f = blobPath(root, blobNamespace(identity), corrupt.sha256);
  const flipped = fs.readFileSync(f);
  flipped[3] ^= 0xff;
  fs.writeFileSync(f, flipped);

  const hashes = [intact.sha256, absent, corrupt.sha256, foreign.sha256];
  const r = await haveExec(haveRequest(identity, hashes), { env, root });
  assert.equal(r.code, 0, r.stderr);
  assert.deepEqual(JSON.parse(r.stdout), { v: 1, have: [0] });
  assert.equal(fs.existsSync(f), false, 'the corrupt blob is removed, never reported');
  assert.equal(hasBlob(root, blobNamespace('/repos/two/.git'), foreign.sha256, foreign.size), 'hit', 'the other repo keeps its own blob');

  const leasesDir = path.join(root, 'blobs', '.leases');
  const leases = fs.readdirSync(leasesDir).filter((n) => n.endsWith('.json')).map((n) => JSON.parse(fs.readFileSync(path.join(leasesDir, n), 'utf8')));
  assert.equal(leases.length, 1);
  assert.ok(leases[0].expiresAt > Date.now(), 'the lease outlives the have process on a clock, not on its pid');
  for (const h of hashes) assert.ok(leases[0].hashes.includes(h));
  assert.ok(leasedHashes(root).held.has(intact.sha256), 'a dead pid does not release it');
  assert.equal(leasedHashes(root, { now: Date.now() + 3_600_000 }).held.size, 0, 'it expires on its own');
});

test('remote-have: eviction that picked a victim before the have-query cannot remove it (lease, not the LRU touch, protects it)', async () => {
  const env = runnerEnv();
  const root = tmpDir('have-runner');
  const identity = '/repos/race/.git';
  const entry = seedBlob(root, identity, 'race');
  const file = blobPath(root, blobNamespace(identity), entry.sha256);
  const longAgo = new Date(Date.now() - 3_600_000);
  fs.utimesSync(file, longAgo, longAgo);
  const evicted = await evictBlobs(root, {
    maxAgeMs: 1000,
    lock: NO_LOCK,
    afterSelect: async (victims) => {
      assert.deepEqual(victims, [entry.sha256], 'the blob was selected as a victim');
      const r = await haveExec(haveRequest(identity, [entry.sha256]), { env, root });
      assert.deepEqual(JSON.parse(r.stdout), { v: 1, have: [0] });
      fs.utimesSync(file, longAgo, longAgo); // undo the LRU touch: only the lease may now keep it
    },
  });
  assert.deepEqual(evicted, []);
  assert.equal(fs.existsSync(file), true);
  const later = await evictBlobs(root, { maxAgeMs: 1000, lock: NO_LOCK, now: Date.now() + 3_600_000 });
  assert.deepEqual(later, [entry.sha256], 'once the lease has expired the blob is evictable again');
});

test('remote-have: a disabled cache answers an empty have and takes no lease', async () => {
  const env = runnerEnv({ remoteBlobCache: false });
  const root = tmpDir('have-runner');
  const entry = seedBlob(root, '/repos/off/.git', 'off');
  const r = await haveExec(haveRequest('/repos/off/.git', [entry.sha256]), { env, root });
  assert.deepEqual(JSON.parse(r.stdout), { v: 1, have: [] });
  assert.equal(fs.existsSync(path.join(root, 'blobs', '.leases')), false);
});

// ---- the capability ----

test('blob-cache/1 is advertised only while the runner cache is enabled (capabilities and remote-probe)', async () => {
  assert.equal(CAPABILITIES.includes(BLOB_CACHE_CAPABILITY), false, 'not in the static list');
  assert.ok(advertisedCapabilities({ remoteBlobCache: true }).includes(BLOB_CACHE_CAPABILITY));
  assert.equal(advertisedCapabilities({ remoteBlobCache: false }).includes(BLOB_CACHE_CAPABILITY), false);
  for (const [cfg, expected] of [[{}, true], [{ remoteBlobCache: false }, false]]) {
    const env = runnerEnv(cfg);
    const caps = JSON.parse((await laneRun(['capabilities', '--json'], { env })).stdout).capabilities;
    assert.equal(caps.includes(BLOB_CACHE_CAPABILITY), expected);
    const probe = JSON.parse((await laneRun(['remote-probe', '--root', tmpDir('have-runner')], { env })).stdout);
    assert.equal(probe.capabilities.includes(BLOB_CACHE_CAPABILITY), expected);
  }
});

// ---- the client, through the real CLI over the fake ssh ----

function parseStream(buf) {
  const gzip = buf[0] === 0x1f && buf[1] === 0x8b;
  const raw = gzip ? zlib.gunzipSync(buf) : buf;
  let pos = 0;
  const line = () => {
    const nl = raw.indexOf(10, pos);
    const text = raw.subarray(pos, nl).toString('utf8');
    pos = nl + 1;
    return text;
  };
  const header = JSON.parse(line());
  const frames = [];
  for (;;) {
    const frame = JSON.parse(line());
    if (frame.end) break;
    frames.push(frame);
    if (frame.type === 'file' && frame.body !== 'cached') pos += frame.size;
  }
  return { header, frames, gzip, bytes: buf.length };
}

function bigCtx(opts = {}) {
  const ctx = setup({ ssh: 'tee', ...opts });
  fs.writeFileSync(path.join(ctx.repoDir, 'big.bin'), crypto.randomBytes(BIG));
  ctx.bigSha = sha(fs.readFileSync(path.join(ctx.repoDir, 'big.bin')));
  return ctx;
}

async function runOnce(ctx, extraEnv = {}) {
  const marker = path.join(tmpDir('marker'), 'where');
  const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], { ...ctx.env, ...extraEnv }, ctx.repoDir);
  return { code: waited.code, where: fs.existsSync(marker) ? fs.readFileSync(marker, 'utf8') : null, stderr: waited.stderr };
}

const lastExecCmd = (ctx) => linesOf(ctx.execCmdsPath).at(-1);
const bigFrame = (stream) => stream.frames.find((f) => f.path === 'big.bin');

for (const [name, drop, gzip, cache] of [
  ['gzip + cache', '', true, true],
  ['gzip only', 'blob-cache/', true, false],
  ['cache only', 'snapshot-gzip/', false, true],
  ['neither', 'snapshot-gzip/,blob-cache/', false, false],
]) {
  test(`remote-have: capability combination ${name}: the stream, flags and have-query follow each capability independently`, async () => {
    const ctx = bigCtx();
    const env = { LANE_FAKE_CAPS_DROP: drop };
    const first = await runOnce(ctx, env);
    assert.equal(first.code, 0, first.stderr);
    assert.equal(first.where, 'remote');
    const second = await runOnce(ctx, env);
    assert.equal(second.code, 0, second.stderr);
    assert.equal(second.where, 'remote');

    const cmds = linesOf(ctx.execCmdsPath);
    assert.equal(cmds.length, 2, 'no repair retry on the happy path');
    const stream = parseStream(fs.readFileSync(ctx.teePath));
    assert.equal(stream.gzip, gzip);
    for (const cmd of cmds) {
      assert.equal(/--transfer\b[^-]*gzip/.test(cmd), gzip, cmd);
      assert.equal(cmd.includes('--transfer-cache'), cache, cmd);
    }
    if (cache) {
      assert.equal(bigFrame(stream).body, 'cached', 'the second dispatch sends no body for the cached file');
      assert.equal(stream.header.repoIdentity, repoIdentity(ctx.repoDir));
      assert.ok(stream.bytes < 1024 * 1024, `sent ${stream.bytes}`);
      assert.deepEqual(JSON.parse(fs.readFileSync(ctx.haveLogPath, 'utf8')), { v: 1, repoIdentity: repoIdentity(ctx.repoDir), hashes: [ctx.bigSha] });
      assert.equal(hasBlob(ctx.runnerRoot, blobNamespace(repoIdentity(ctx.repoDir)), ctx.bigSha, BIG), 'hit');
    } else {
      assert.equal('body' in bigFrame(stream), false);
      assert.equal('repoIdentity' in stream.header, false);
      assert.ok(stream.bytes > BIG, 'full bodies');
      assert.equal(fs.existsSync(ctx.haveLogPath), false, 'no have-query without the capability');
    }
  });
}

test('remote-have: an old runner (neither capability) gets today\'s exact stream, and a client with the cache off sends no flag or query', async () => {
  const legacy = bigCtx();
  legacy.env = { ...legacy.env };
  const old = setup({ ssh: 'strict-legacy' });
  fs.writeFileSync(path.join(old.repoDir, 'big.bin'), crypto.randomBytes(BIG));
  for (let i = 0; i < 2; i += 1) assert.equal((await runOnce(old)).where, 'remote');
  assert.equal(fs.readFileSync(old.violationsPath, 'utf8'), '', 'a legacy runner sees no new flag, key or compressed byte');
  assert.equal(fs.existsSync(old.haveLogPath), false);

  const off = bigCtx({ globalExtra: { remoteBlobCache: false } });
  for (let i = 0; i < 2; i += 1) assert.equal((await runOnce(off)).where, 'remote');
  assert.equal(fs.existsSync(off.haveLogPath), false, 'the client config gates the have-query');
  assert.ok(linesOf(off.execCmdsPath).every((c) => !c.includes('--transfer-cache')));
});

test('remote-have: a failed or hung have-query sends full bodies (plain without gzip, gzip with it) and the run still succeeds', async () => {
  for (const [drop, gzip] of [['', true], ['snapshot-gzip/', false]]) {
    const ctx = bigCtx();
    assert.equal((await runOnce(ctx, { LANE_FAKE_CAPS_DROP: drop })).where, 'remote'); // publishes
    const failed = await runOnce(ctx, { LANE_FAKE_CAPS_DROP: drop, LANE_FAKE_HAVE: 'fail' });
    assert.equal(failed.where, 'remote');
    assert.equal(failed.code, 0, failed.stderr);
    const stream = parseStream(fs.readFileSync(ctx.teePath));
    assert.equal(stream.gzip, gzip);
    assert.equal('body' in bigFrame(stream), false, 'full body after a failed have-query');
    assert.equal(lastExecCmd(ctx).includes('--transfer-cache'), false, '--transfer-cache only when the have-query succeeded');
    assert.equal(linesOf(ctx.execCmdsPath).length, 2);
  }
});

test('remote-have: a have-query that never answers is abandoned at the deadline and everything is sent', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir, { LANE_FAKE_HAVE: 'hang' });
  const src = makeGitWorktree({ 'big.bin': crypto.randomBytes(BIG) });
  const root = tmpDir('have-client-root');
  const started = Date.now();
  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'tee', root }),
    worktreeRoot: src,
    sshBin,
    env: { ...env, LANE_FAKE_HAVE: 'hang' },
    blobCache: true,
    deadlines: { haveMs: 300 },
  });
  assert.equal(result.outcome, 'confirmed', result.reason);
  assert.ok(Date.now() - started < 20_000);
  assert.equal(fs.existsSync(path.join(root, 'blobs')), false, 'no --transfer-cache, so nothing published');
});

// ---- freshness: a cached file is still read and verified at encode time ----

test('remote-have: a cached file mutated after the have-query fails the encode (never a stale cached frame)', async () => {
  const { binDir, sshBin, teePath } = makeFakeSshBin();
  const bytes = crypto.randomBytes(BIG);
  const src = makeGitWorktree({ 'big.bin': bytes });
  const root = tmpDir('have-client-root');
  const entry = buildManifest(src).entries.find((e) => e.path === 'big.bin');
  assert.equal(publishBlob(root, blobNamespace(repoIdentity(src)), entry, bytes, PLENTY), 'published');
  const run = (extra) => dispatchRemote({
    ...makeDispatchArgs(),
    runner: makeRunner({ ssh: 'tee', root }),
    worktreeRoot: src,
    sshBin,
    env: { ...clientEnv(binDir).env, ...extra },
    blobCache: true,
  });

  const control = await run({});
  assert.equal(control.outcome, 'confirmed', control.reason);
  assert.equal(bigFrameOf(teePath).body, 'cached', 'control: an unmutated cached file travels without a body');

  const mutated = await run({ LANE_FAKE_AFTER_HAVE: `mutate:${path.join(src, 'big.bin')}` }); // same size, different first byte
  assert.equal(mutated.outcome, 'unconfirmed');
  assert.equal(mutated.neverStarted, true);
  assert.match(mutated.reason, /snapshot stream error: file changed content on disk/);
});

function bigFrameOf(teePath) {
  return bigFrame(parseStream(fs.readFileSync(teePath)));
}

// ---- the bounded repair retry, through the supervisor ----

for (const [label, repoConfig] of [
  ['single-runner fleet', null],
  ['localRefused lane (never falls back to local)', { version: 1, lanes: { default: { weight: 1, remote: true, localRefused: true } } }],
]) {
  for (const [fault, spec, reason] of [['a blob that vanished after the have-answer', 'delete', 'cache-miss'], ['a blob corrupted after the have-answer', 'corrupt', 'cache-corrupt']]) {
    test(`remote-have: ${label}: ${fault} is repaired once on the same runner with full bodies`, async () => {
      const ctx = bigCtx();
      if (repoConfig) writeRepoConfig(ctx.repoDir, repoConfig);
      assert.equal((await runOnce(ctx)).where, 'remote');
      const ticketsAfterFirst = fs.readdirSync(path.join(ctx.runnerRoot, 'tickets')).length;
      assert.equal(ticketsAfterFirst, 1);

      const second = await runOnce(ctx, { LANE_FAKE_AFTER_HAVE: `${spec}:${path.join(ctx.runnerRoot, 'blobs')}` });
      assert.equal(second.code, 0, second.stderr);
      assert.equal(second.where, 'remote', `the repaired run ran on the runner (${reason})`);
      const cmds = linesOf(ctx.execCmdsPath);
      assert.equal(cmds.length, 3, 'first run, the cached attempt, exactly one repair');
      assert.ok(cmds[1].includes('--transfer-cache'));
      assert.equal(cmds[2].includes('--transfer-cache'), false, 'the repair sends full bodies with the cache off');
      assert.equal(fs.readdirSync(path.join(ctx.runnerRoot, 'tickets')).length, 3, 'the repair used a fresh ticket');
      const stream = parseStream(fs.readFileSync(ctx.teePath));
      assert.equal('body' in bigFrame(stream), false);
      assert.ok(stream.bytes > BIG);
    });
  }
}
