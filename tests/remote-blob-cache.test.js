import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { freshEnv, writeGlobalConfig, BIN, laneRun } from './helpers.js';
import { tmpDir } from './remote-harness.js';
import { manifestHashOf } from '../src/remote-manifest.js';
import { encodeSnapshot, serializeHeader, makeReader, extractFrames } from '../src/remote-stream.js';
import { sanitizeKey, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { CAPABILITIES } from '../src/capabilities.js';
import {
  MIN_CACHEABLE_BYTES, blobNamespace, blobPath, hasBlob, materializeBlob, publishBlob, acquireBlobLeases, evictBlobs, parseTransferCacheFlag,
} from '../src/remote-blobs.js';

/** BRAIN-642 slice C1: the runner-side blob store and the `body:'cached'` extract hook. Direct streams, no ssh. */

const BLOBS_MODULE = fileURLToPath(new URL('../src/remote-blobs.js', import.meta.url));
const NO_LOCK = (fn) => fn();
const PLENTY = { freeBytes: () => Number.MAX_SAFE_INTEGER };
const DAY = 86_400_000;

const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
const big = (seed, n = MIN_CACHEABLE_BYTES + 100) => Buffer.from(`${seed}:`.repeat(Math.ceil(n / (String(seed).length + 1))).slice(0, n));
const entryOf = (name, buf, exec = false) => ({ path: name, type: 'file', exec, size: buf.length, sha256: sha(buf) });
const manifestOf = (entries) => ({ entries, manifestHash: manifestHashOf(entries) });

function store() {
  const root = tmpDir('blob-root');
  return { root, ns: blobNamespace(`/repos/${path.basename(root)}/.git`) };
}

function seed(s, entry, buf) {
  assert.equal(publishBlob(s.root, s.ns, entry, buf, PLENTY), 'published');
}

const blobFile = (s, entry) => blobPath(s.root, s.ns, entry.sha256);

/** A frame stream: each item is `{frame, body}`; the terminator is appended. */
function streamOf(items) {
  const chunks = [];
  for (const { frame, body } of items) {
    chunks.push(Buffer.from(`${JSON.stringify(frame)}\n`));
    if (body) chunks.push(body);
  }
  chunks.push(Buffer.from('{"end":true}\n'));
  return Readable.from(chunks);
}

const cachedFrame = (entry) => ({ frame: { path: entry.path, type: 'file', exec: entry.exec, size: entry.size, body: 'cached' } });
const bodyFrame = (entry, body) => ({ frame: { path: entry.path, type: 'file', exec: entry.exec, size: entry.size }, body });

async function extract(s, items, entries, { cache = true, limits = {}, publishOptions = PLENTY } = {}) {
  const dest = path.join(tmpDir('blob-dest'), 'work');
  const stats = { hits: 0, misses: 0, corrupt: 0, published: 0 };
  const res = await extractFrames(makeReader(streamOf(items)), dest, manifestOf(entries), {
    ...limits,
    ...(cache ? { blobCache: { root: s.root, ns: s.ns, stats, publishOptions } } : {}),
  });
  return { res, dest, stats };
}

const files = (dir) => (fs.existsSync(dir) ? fs.readdirSync(dir, { recursive: true }) : []);

// ---- corruption and tampering ---------------------------------------------------------------------------------------------

test('remote-blob-cache: a same-size byte flip in a cached blob is cache-corrupt, the blob is removed and the workdir never holds the tampered bytes', async () => {
  const s = store();
  const buf = big('flip');
  const entry = entryOf('a.bin', buf);
  seed(s, entry, buf);
  const tampered = Buffer.from(buf);
  tampered[10] ^= 0xff;
  fs.chmodSync(blobFile(s, entry), 0o600);
  fs.writeFileSync(blobFile(s, entry), tampered);

  const first = await extract(s, [cachedFrame(entry)], [entry]);
  assert.equal(first.res.ok, false);
  assert.match(first.res.reason, /^cache-corrupt: 1$/);
  assert.equal(first.stats.corrupt, 1);
  assert.equal(fs.existsSync(blobFile(s, entry)), false, 'the corrupt blob is removed');
  assert.equal(fs.existsSync(path.join(first.dest, 'a.bin')), false, 'tampered bytes never reached the workdir');

  const retry = await extract(s, [bodyFrame(entry, buf)], [entry]);
  assert.equal(retry.res.ok, true, retry.res.reason);
  assert.deepEqual(fs.readFileSync(path.join(retry.dest, 'a.bin')), buf);
});

test('remote-blob-cache: a truncated cached blob is cache-corrupt and removed', async () => {
  const s = store();
  const buf = big('trunc');
  const entry = entryOf('a.bin', buf);
  seed(s, entry, buf);
  fs.chmodSync(blobFile(s, entry), 0o600);
  fs.truncateSync(blobFile(s, entry), buf.length - 5);
  const r = await extract(s, [cachedFrame(entry)], [entry]);
  assert.match(r.res.reason, /^cache-corrupt: 1$/);
  assert.equal(fs.existsSync(blobFile(s, entry)), false);
  assert.equal(fs.existsSync(path.join(r.dest, 'a.bin')), false);
});

test('remote-blob-cache: a blob replaced by a symlink to a file outside the cache is rejected', async () => {
  const s = store();
  const buf = big('link');
  const entry = entryOf('a.bin', buf);
  seed(s, entry, buf);
  const outside = path.join(tmpDir('blob-outside'), 'same-bytes');
  fs.writeFileSync(outside, buf); // identical size AND hash: only the no-follow open can refuse it
  fs.rmSync(blobFile(s, entry));
  fs.symlinkSync(outside, blobFile(s, entry));
  const r = await extract(s, [cachedFrame(entry)], [entry]);
  assert.match(r.res.reason, /^cache-corrupt: 1$/);
  assert.equal(fs.existsSync(path.join(r.dest, 'a.bin')), false);
  assert.deepEqual(fs.readFileSync(outside), buf, 'the link target is untouched');
});

test('remote-blob-cache: a missing blob is cache-miss', async () => {
  const s = store();
  const entry = entryOf('a.bin', big('miss'));
  const r = await extract(s, [cachedFrame(entry)], [entry]);
  assert.match(r.res.reason, /^cache-miss: 1$/);
  assert.equal(r.stats.misses, 1);
});

test('remote-blob-cache: a symlinked <ns>/<aa> ancestor is rejected on lookup, publish and evict', async () => {
  const s = store();
  const buf = big('anc');
  const entry = entryOf('a.bin', buf);
  const outside = tmpDir('blob-outside');
  fs.writeFileSync(path.join(outside, entry.sha256), buf); // a perfectly valid blob, behind a link
  fs.mkdirSync(path.join(s.root, 'blobs', s.ns), { recursive: true });
  const link = path.join(s.root, 'blobs', s.ns, entry.sha256.slice(0, 2));
  fs.symlinkSync(outside, link);

  assert.equal(hasBlob(s.root, s.ns, entry.sha256, entry.size), 'miss', 'lookup must not follow the link');
  const dest = path.join(tmpDir('blob-dest'), 'f');
  assert.equal(materializeBlob(s.root, s.ns, entry, dest).status, 'miss');
  assert.equal(fs.existsSync(dest), false);

  const other = big('anc2');
  const otherEntry = entryOf('b.bin', other);
  const sameShard = fs.readdirSync(outside).length;
  assert.equal(publishBlob(s.root, s.ns, entryOf('x', buf), buf, PLENTY), 'not-cached', 'publish must not write through the link');
  assert.equal(fs.readdirSync(outside).length, sameShard);
  void otherEntry;

  await evictBlobs(s.root, { maxAgeMs: -1, lock: NO_LOCK });
  assert.equal(fs.existsSync(link), false, 'evict removes the link');
  assert.deepEqual(fs.readFileSync(path.join(outside, entry.sha256)), buf, 'evict never touches the link target');
});

test('remote-blob-cache: two identities whose sanitizeKey collides get different namespaces', () => {
  const a = '/repos/a/b/.git';
  const b = '/repos/a_b/.git';
  assert.equal(sanitizeKey(a.replace(/\//g, '_')), sanitizeKey(b.replace(/\//g, '_')).replace('a_b', 'a_b'));
  assert.equal(sanitizeKey('/repos/a/b'), sanitizeKey('/repos/a_b'), 'premise: sanitizeKey is lossy');
  assert.notEqual(blobNamespace('/repos/a/b'), blobNamespace('/repos/a_b'));
  assert.match(blobNamespace(a), /^[0-9a-f]{32}$/);
});

test('remote-blob-cache: a body frame whose hash differs from the manifest is rejected and never cached', async () => {
  const s = store();
  const good = big('good');
  const entry = entryOf('a.bin', good);
  const evil = Buffer.from(good);
  evil[3] ^= 0x01; // same size, different hash
  const r = await extract(s, [bodyFrame(entry, evil)], [entry]);
  assert.equal(r.res.ok, false);
  assert.equal(r.stats.published, 0);
  assert.equal(hasBlob(s.root, s.ns, entry.sha256, entry.size), 'miss');
  assert.equal(publishBlob(s.root, s.ns, entry, evil, PLENTY), 'not-cached');
  assert.deepEqual(files(path.join(s.root, 'blobs')).filter((f) => /[0-9a-f]{64}$/.test(f)), []);
});

// ---- frame validation ----------------------------------------------------------------------------------------------------

test("remote-blob-cache: body:'cached' without --transfer-cache is rejected", async () => {
  const s = store();
  const buf = big('noflag');
  const entry = entryOf('a.bin', buf);
  seed(s, entry, buf);
  const r = await extract(s, [cachedFrame(entry)], [entry], { cache: false });
  assert.equal(r.res.ok, false);
  assert.match(r.res.reason, /cached body not enabled/);
});

test("remote-blob-cache: body:'cached' for a file under 8 KiB is rejected", async () => {
  const s = store();
  const small = Buffer.from('tiny');
  const entry = entryOf('a.txt', small);
  const r = await extract(s, [cachedFrame(entry)], [entry]);
  assert.match(r.res.reason, /below minimum size/);
});

test('remote-blob-cache: cached frames still count toward MAX_FILE_BYTES and MAX_TOTAL_BYTES', async () => {
  const s = store();
  const a = big('a', 12_000);
  const b = big('b', 12_000);
  const ea = entryOf('a.bin', a);
  const eb = entryOf('b.bin', b);
  seed(s, ea, a);
  seed(s, eb, b);
  const items = [cachedFrame(ea), cachedFrame(eb)];
  const perFile = await extract(s, items, [ea, eb], { limits: { maxFileBytes: 10_000 } });
  assert.match(perFile.res.reason, /oversize frame/);
  const total = await extract(s, items, [ea, eb], { limits: { maxTotalBytes: 20_000 } });
  assert.match(total.res.reason, /oversize total: b\.bin/);
  const fits = await extract(s, items, [ea, eb], { limits: { maxFileBytes: 12_000, maxTotalBytes: 24_000 } });
  assert.equal(fits.res.ok, true, fits.res.reason);
});

test('remote-blob-cache: a cached hit materializes the bytes with the exec bit from the manifest', async () => {
  const s = store();
  const run = big('run');
  const data = big('data');
  const er = entryOf('run.sh', run, true);
  const ed = entryOf('data.bin', data, false);
  seed(s, er, run);
  seed(s, ed, data);
  const r = await extract(s, [cachedFrame(ed), cachedFrame(er)], [er, ed]);
  assert.equal(r.res.ok, true, r.res.reason);
  assert.equal(r.stats.hits, 2);
  assert.equal(fs.statSync(path.join(r.dest, 'run.sh')).mode & 0o777, 0o755);
  assert.equal(fs.statSync(path.join(r.dest, 'data.bin')).mode & 0o777, 0o644);
});

test('remote-blob-cache: mutating a materialized file does not poison the cache (copy, not hardlink)', async () => {
  const s = store();
  const buf = big('poison');
  const entry = entryOf('a.bin', buf);
  seed(s, entry, buf);
  const r = await extract(s, [cachedFrame(entry)], [entry]);
  assert.equal(r.res.ok, true, r.res.reason);
  const out = path.join(r.dest, 'a.bin');
  assert.equal(fs.statSync(out).nlink, 1, 'not hardlinked to the blob');
  fs.appendFileSync(out, 'lane mutated this');
  assert.equal(hasBlob(s.root, s.ns, entry.sha256, entry.size), 'hit');
  assert.deepEqual(fs.readFileSync(blobFile(s, entry)), buf);
  assert.equal(fs.statSync(blobFile(s, entry)).mode & 0o777, 0o400);
});

test('remote-blob-cache: body frames are published, small ones are not, and a second extract hits', async () => {
  const s = store();
  const buf = big('pub');
  const small = Buffer.from('tiny');
  const entry = entryOf('a.bin', buf);
  const es = entryOf('s.txt', small);
  const first = await extract(s, [bodyFrame(entry, buf), bodyFrame(es, small)], [entry, es]);
  assert.equal(first.res.ok, true, first.res.reason);
  assert.equal(first.stats.published, 1);
  assert.equal(hasBlob(s.root, s.ns, es.sha256, es.size), 'miss');
  const second = await extract(s, [cachedFrame(entry), bodyFrame(es, small)], [entry, es]);
  assert.equal(second.res.ok, true, second.res.reason);
  assert.equal(second.stats.hits, 1);
});

test('remote-blob-cache: insufficient free-space headroom skips publication without failing the extract', async () => {
  const s = store();
  const buf = big('full');
  const entry = entryOf('a.bin', buf);
  const r = await extract(s, [bodyFrame(entry, buf)], [entry], { publishOptions: { headroomBytes: 1000, freeBytes: () => 1500 } });
  assert.equal(r.res.ok, true, r.res.reason);
  assert.equal(r.stats.published, 0);
  assert.equal(hasBlob(s.root, s.ns, entry.sha256, entry.size), 'miss');
  assert.equal(publishBlob(s.root, s.ns, entry, buf, { headroomBytes: 1000, freeBytes: () => 1500 }), 'skipped');
});

test('remote-blob-cache: concurrent publishers of the same blob are idempotent', async () => {
  const s = store();
  const buf = big('race', 200_000);
  const entry = entryOf('a.bin', buf);
  const script = `
    import { publishBlob } from ${JSON.stringify(BLOBS_MODULE)};
    const [root, ns, sha, size, seed] = process.argv.slice(1);
    const bytes = Buffer.from(seed.repeat(Math.ceil(Number(size) / seed.length)).slice(0, Number(size)));
    const out = publishBlob(root, ns, { type: 'file', sha256: sha, size: Number(size) }, bytes, { freeBytes: () => Number.MAX_SAFE_INTEGER });
    process.stdout.write(out);`;
  const runs = Array.from({ length: 6 }, () => new Promise((resolve) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script, s.root, s.ns, entry.sha256, String(entry.size), 'race:'], { stdio: ['ignore', 'pipe', 'inherit'] });
    let out = '';
    child.stdout.on('data', (d) => { out += d; });
    child.on('close', (code) => resolve({ code, out }));
  }));
  for (const r of await Promise.all(runs)) {
    assert.equal(r.code, 0);
    assert.match(r.out, /^(published|exists)$/);
  }
  assert.equal(hasBlob(s.root, s.ns, entry.sha256, entry.size), 'hit');
  assert.deepEqual(files(path.join(s.root, 'blobs', s.ns)).filter((f) => f.includes('.tmp-')), []);
});

// ---- eviction and leases ---------------------------------------------------------------------------------------------------

function age(s, entry, ms) {
  const t = new Date(Date.now() - ms);
  fs.utimesSync(blobFile(s, entry), t, t);
}

function seeded(s, name, n = 10_000) {
  const buf = big(name, n);
  const entry = entryOf(name, buf);
  seed(s, entry, buf);
  return entry;
}

test('remote-blob-cache: eviction removes blobs older than the max age and keeps fresh ones', async () => {
  const s = store();
  const old = seeded(s, 'old');
  const fresh = seeded(s, 'fresh');
  age(s, old, 20 * DAY);
  const gone = await evictBlobs(s.root, { maxAgeMs: 14 * DAY, lock: NO_LOCK });
  assert.deepEqual(gone, [old.sha256]);
  assert.equal(hasBlob(s.root, s.ns, old.sha256, old.size), 'miss');
  assert.equal(hasBlob(s.root, s.ns, fresh.sha256, fresh.size), 'hit');
  assert.deepEqual(fs.readdirSync(path.join(s.root, 'blobs')).filter((n) => n.startsWith('.trash-')), []);
});

test('remote-blob-cache: eviction trims to the byte budget, least recently used first', async () => {
  const s = store();
  const a = seeded(s, 'a');
  const b = seeded(s, 'b');
  const c = seeded(s, 'c');
  age(s, a, 3 * 3_600_000);
  age(s, b, 2 * 3_600_000);
  age(s, c, 1 * 3_600_000);
  const gone = await evictBlobs(s.root, { maxBytes: 15_000, lock: NO_LOCK });
  assert.deepEqual(new Set(gone), new Set([a.sha256, b.sha256]));
  assert.equal(hasBlob(s.root, s.ns, c.sha256, c.size), 'hit');
});

test('remote-blob-cache: a leased blob is not evicted, and goes once the lease is released', async () => {
  const s = store();
  const held = seeded(s, 'held');
  const loose = seeded(s, 'loose');
  age(s, held, 30 * DAY);
  age(s, loose, 30 * DAY);
  const release = await acquireBlobLeases(s.root, [held.sha256], NO_LOCK);
  const first = await evictBlobs(s.root, { maxAgeMs: DAY, lock: NO_LOCK });
  assert.deepEqual(first, [loose.sha256]);
  assert.equal(hasBlob(s.root, s.ns, held.sha256, held.size), 'hit');
  await release();
  age(s, held, 30 * DAY); // the lookup above counted as a use
  assert.deepEqual(await evictBlobs(s.root, { maxAgeMs: DAY, lock: NO_LOCK }), [held.sha256]);
});

test('remote-blob-cache: a lease held by a dead process is reaped and no longer protects its blobs', async () => {
  const s = store();
  const entry = seeded(s, 'orphan');
  age(s, entry, 30 * DAY);
  const dead = spawn(process.execPath, ['-e', '0']);
  await new Promise((resolve) => dead.on('close', resolve));
  const leases = path.join(s.root, 'blobs', '.leases');
  fs.mkdirSync(leases, { recursive: true });
  const leaseFile = path.join(leases, `${dead.pid}-x.json`);
  fs.writeFileSync(leaseFile, JSON.stringify({ pid: dead.pid, start: null, hashes: [entry.sha256] }));
  assert.deepEqual(await evictBlobs(s.root, { maxAgeMs: DAY, lock: NO_LOCK }), [entry.sha256]);
  assert.equal(fs.existsSync(leaseFile), false);
});

test('remote-blob-cache: GC selects a victim, a lease is taken, GC then renames: the leased blob survives', async () => {
  const s = store();
  const entry = seeded(s, 'race');
  age(s, entry, 30 * DAY);
  let release;
  const evicted = await evictBlobs(s.root, {
    maxAgeMs: DAY,
    lock: NO_LOCK,
    afterSelect: async (victims) => {
      assert.deepEqual(victims, [entry.sha256], 'the blob was selected before the lease existed');
      release = await acquireBlobLeases(s.root, [entry.sha256], NO_LOCK);
    },
  });
  assert.deepEqual(evicted, []);
  assert.equal(hasBlob(s.root, s.ns, entry.sha256, entry.size), 'hit');
  await release();
});

test('remote-blob-cache: a blob used between selection and rename is kept', async () => {
  const s = store();
  const entry = seeded(s, 'touched');
  age(s, entry, 30 * DAY);
  const evicted = await evictBlobs(s.root, {
    maxAgeMs: DAY,
    lock: NO_LOCK,
    afterSelect: () => { assert.equal(hasBlob(s.root, s.ns, entry.sha256, entry.size), 'hit'); },
  });
  assert.deepEqual(evicted, []);
});

// ---- the runner, through the real CLI ---------------------------------------------------------------------------------------

const spawned = [];
afterEach(() => {
  for (const child of spawned.splice(0)) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
});

function remoteExec(bytes, { env, root, args = [] }) {
  const child = spawn(process.execPath, [BIN, 'remote-exec', ...args, '--root', root], { env });
  spawned.push(child);
  let stderr = '';
  child.stderr.on('data', (d) => { stderr += d; });
  child.stdin.on('error', () => {});
  child.stdin.end(bytes);
  return new Promise((resolve) => child.on('close', (code) => resolve({ code, stderr })));
}

const headerOf = (extra = {}) => ({ ticketId: crypto.randomUUID(), generation: 0, repoKey: 'blob-repo', lane: 'default', argv: [process.execPath, '-e', 'process.exit(0)'], relCwd: '', ...extra });

async function resultOf(ticketId, root, env) {
  const { stdout } = await laneRun(['remote-result', ticketId, '--root', root], { env });
  return JSON.parse(stdout.trim());
}

function runnerEnv(cfg = {}) {
  const f = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '1000' });
  writeGlobalConfig(f.home, cfg);
  return f.env;
}

async function plainStream(src, header, entries) {
  const chunks = [];
  for await (const c of encodeSnapshot(src, header, entries)) chunks.push(c);
  return Buffer.concat(chunks);
}

function cachedStream(header, entries, cachedPaths) {
  const out = [Buffer.from(serializeHeader(header, entries))];
  for (const e of [...entries].sort((x, y) => (x.path < y.path ? -1 : 1))) {
    const cached = cachedPaths.includes(e.path);
    out.push(Buffer.from(`${JSON.stringify({ path: e.path, type: 'file', exec: !!e.exec, size: e.size, ...(cached ? { body: 'cached' } : {}) })}\n`));
  }
  out.push(Buffer.from('{"end":true}\n'));
  return Buffer.concat(out);
}

test('remote-blob-cache: a runner without --transfer-cache behaves as in 0.50.0 (no store, no stats, cached frames refused)', async () => {
  const env = runnerEnv();
  const root = tmpDir('blob-runner');
  const buf = big('plain');
  const src = tmpDir('blob-src');
  fs.writeFileSync(path.join(src, 'a.bin'), buf);
  const entry = entryOf('a.bin', buf);
  const header = headerOf({ repoIdentity: '/repos/x/.git' });
  const run = await remoteExec(await plainStream(src, header, [entry]), { env, root });
  assert.equal(run.code, 0, run.stderr);
  const result = await resultOf(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal('blobStats' in result, false);
  assert.equal(fs.existsSync(path.join(root, 'blobs')), false);

  const header2 = headerOf({ repoIdentity: '/repos/x/.git' });
  await remoteExec(cachedStream(header2, [entry], ['a.bin']), { env, root });
  const rejected = await resultOf(header2.ticketId, root, env);
  assert.equal(rejected.kind, 'rejected');
  assert.match(rejected.reason, /cached body not enabled/);
});

test('remote-blob-cache: with --transfer-cache the runner publishes, then serves a cached frame, and reports blobStats; a miss is cache-miss', async () => {
  const env = runnerEnv();
  const root = tmpDir('blob-runner');
  const buf = big('e2e');
  const src = tmpDir('blob-src');
  fs.writeFileSync(path.join(src, 'a.bin'), buf);
  const entry = entryOf('a.bin', buf);
  const identity = '/repos/e2e/.git';
  const args = ['--transfer-cache'];

  const h1 = headerOf({ repoIdentity: identity });
  assert.equal((await remoteExec(await plainStream(src, h1, [entry]), { env, root, args })).code, 0);
  const r1 = await resultOf(h1.ticketId, root, env);
  assert.equal(r1.kind, 'completed');
  assert.deepEqual(r1.blobStats, { hits: 0, misses: 0, corrupt: 0, published: 1 });
  assert.equal(hasBlob(root, blobNamespace(identity), entry.sha256, entry.size), 'hit');

  const h2 = headerOf({ repoIdentity: identity });
  await remoteExec(cachedStream(h2, [entry], ['a.bin']), { env, root, args });
  const r2 = await resultOf(h2.ticketId, root, env);
  assert.equal(r2.kind, 'completed');
  assert.deepEqual(r2.blobStats, { hits: 1, misses: 0, corrupt: 0, published: 0 });

  const h3 = headerOf({ repoIdentity: '/repos/other/.git' });
  await remoteExec(cachedStream(h3, [entry], ['a.bin']), { env, root, args });
  const r3 = await resultOf(h3.ticketId, root, env);
  assert.equal(r3.kind, 'rejected');
  assert.equal(r3.reason, 'cache-miss: 1', 'another repo namespace cannot reach this blob');
  assert.equal(r3.blobStats.misses, 1);
});

test('remote-blob-cache: --transfer-cache without a repoIdentity, or with remoteBlobCache off, uses no store', async () => {
  const buf = big('noid');
  const src = tmpDir('blob-src');
  fs.writeFileSync(path.join(src, 'a.bin'), buf);
  const entry = entryOf('a.bin', buf);
  for (const [cfg, identity] of [[{}, undefined], [{ remoteBlobCache: false }, '/repos/x/.git']]) {
    const env = runnerEnv(cfg);
    const root = tmpDir('blob-runner');
    const header = headerOf(identity ? { repoIdentity: identity } : {});
    await remoteExec(await plainStream(src, header, [entry]), { env, root, args: ['--transfer-cache'] });
    const result = await resultOf(header.ticketId, root, env);
    assert.equal(result.kind, 'completed');
    assert.equal(fs.existsSync(path.join(root, 'blobs')), false);
    const h2 = headerOf(identity ? { repoIdentity: identity } : {});
    await remoteExec(cachedStream(h2, [entry], ['a.bin']), { env, root, args: ['--transfer-cache'] });
    assert.match((await resultOf(h2.ticketId, root, env)).reason, /cached body not enabled/);
  }
});

// ---- flag, config and capability ---------------------------------------------------------------------------------------------

test('remote-blob-cache: --transfer-cache parsing is a bare flag and refuses a valued spelling', () => {
  assert.deepEqual(parseTransferCacheFlag(['--root', 'x']), { ok: true, transferCache: false });
  assert.deepEqual(parseTransferCacheFlag(['--transfer-cache', '--root', 'x']), { ok: true, transferCache: true });
  assert.equal(parseTransferCacheFlag(['--transfer-cache=yes']).ok, false);
});

test('remote-blob-cache: blob-cache/1 is not advertised until remote-have exists', () => {
  assert.equal(CAPABILITIES.includes('blob-cache/1'), false);
});

test('remote-blob-cache: the global config keys have defaults and are validated', async () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.remoteBlobCache, true);
  assert.equal(DEFAULT_GLOBAL_CONFIG.remoteBlobCacheMaxBytes, 10 * 1024 ** 3);
  assert.equal(DEFAULT_GLOBAL_CONFIG.remoteBlobCacheMaxAgeMs, 14 * DAY);
  const { loadGlobalConfig } = await import('../src/config.js');
  for (const bad of [{ remoteBlobCache: 'yes' }, { remoteBlobCacheMaxBytes: 0 }, { remoteBlobCacheMaxBytes: 1.5 }, { remoteBlobCacheMaxAgeMs: -1 }]) {
    const { env, home } = freshEnv();
    writeGlobalConfig(home, bad);
    const prior = { ...process.env };
    Object.assign(process.env, { LANE_BROKER_HOME: env.LANE_BROKER_HOME, LANE_BROKER_STATE: env.LANE_BROKER_STATE });
    try {
      assert.throws(() => loadGlobalConfig(), new RegExp(Object.keys(bad)[0]));
    } finally {
      process.env = prior;
    }
  }
});

// ---- BRAIN-642 slice C2 review fixes ---------------------------------------------------------------------------------------

test('remote-blob-cache: a non-hex hash or namespace never reaches a path (no unlink outside the store)', async () => {
  const s = store();
  const real = big('real');
  seed(s, entryOf('r.bin', real), real); // the store directories must exist for a traversal to have anywhere to start
  const victim = path.join(s.root, 'victim');
  fs.writeFileSync(victim, 'not a blob');
  const traversal = { path: 'a.bin', sha256: '../victim', size: 3, exec: false };
  assert.equal(hasBlob(s.root, s.ns, '../victim', 10), 'miss');
  assert.equal(materializeBlob(s.root, s.ns, traversal, path.join(tmpDir('blob-dest'), 'f')).status, 'miss');
  assert.equal(hasBlob(s.root, '../..', 'a'.repeat(64), 10), 'miss');
  assert.equal(fs.readFileSync(victim, 'utf8'), 'not a blob', 'the file outside the store survives');

  // a manifest entry whose hash is not hex cannot even ask for a cached body
  const entry = { path: 'a.bin', type: 'file', exec: false, size: MIN_CACHEABLE_BYTES + 1, sha256: '../victim' };
  const r = await extract(s, [cachedFrame(entry)], [entry]);
  assert.equal(r.res.ok, false);
  assert.match(r.res.reason, /cached body without a valid sha256/);
  assert.equal(fs.readFileSync(victim, 'utf8'), 'not a blob');
});

test('remote-blob-cache: a symlinked blobs/.leases is never read or emptied by eviction bookkeeping', async () => {
  const s = store();
  const outside = tmpDir('lease-outside');
  const stale = path.join(outside, 'x.json');
  fs.writeFileSync(stale, JSON.stringify({ pid: 2_000_000_000, hashes: [] })); // a dead process: a plain .leases would delete it
  fs.mkdirSync(path.join(s.root, 'blobs'), { recursive: true });
  fs.symlinkSync(outside, path.join(s.root, 'blobs', '.leases'));
  const { leasedHashes } = await import('../src/remote-blobs.js');
  const { held, blockAll } = leasedHashes(s.root);
  assert.equal(blockAll, true, 'eviction is blocked while the lease directory is a link');
  assert.equal(held.size, 0);
  assert.equal(fs.existsSync(stale), true, 'the file behind the link is untouched');
});

test('remote-blob-cache: cached frames that miss are charged to the total cap before materialization', async () => {
  const s = store();
  const entries = ['a', 'b', 'c'].map((n) => entryOf(`${n}.bin`, big(n)));
  const items = entries.map(cachedFrame);
  const total = entries.reduce((sum, e) => sum + e.size, 0);
  const r = await extract(s, items, entries, { limits: { maxTotalBytes: total - 1 } });
  assert.equal(r.res.ok, false);
  assert.match(r.res.reason, /oversize total/, 'the third declared size does not fit once the two misses are charged');
});

test('remote-blob-cache: remote-exec holds a lease on the manifest hashes while it extracts, and releases it after', async () => {
  const env = runnerEnv();
  const root = tmpDir('blob-runner');
  const buf = big('lease');
  const entry = entryOf('a.bin', buf);
  const identity = '/repos/lease/.git';
  const ns = blobNamespace(identity);
  seed({ root, ns }, entry, buf);
  const header = headerOf({ repoIdentity: identity });
  const bytes = cachedStream(header, [entry], ['a.bin']);
  const terminator = Buffer.from('{"end":true}\n');
  const child = spawn(process.execPath, [BIN, 'remote-exec', '--transfer-cache', '--root', root], { env });
  spawned.push(child);
  child.stdin.on('error', () => {});
  const exited = new Promise((resolve) => child.on('close', (code) => resolve(code)));
  child.stdin.write(bytes.subarray(0, bytes.length - terminator.length)); // everything but the terminator: extraction cannot finish
  const leasesDir = path.join(root, 'blobs', '.leases');
  const held = async () => (fs.existsSync(leasesDir) ? fs.readdirSync(leasesDir).filter((n) => n.endsWith('.json')).map((n) => fs.readFileSync(path.join(leasesDir, n), 'utf8')) : []);
  let seen = [];
  for (let i = 0; i < 100 && seen.length === 0; i += 1) {
    seen = await held();
    if (seen.length === 0) await new Promise((r) => setTimeout(r, 100));
  }
  assert.equal(seen.length, 1, 'a lease exists mid-extraction');
  assert.ok(seen[0].includes(entry.sha256));
  child.stdin.end(terminator);
  assert.equal(await exited, 0);
  assert.deepEqual(await held(), [], 'the lease is released afterwards');
});
