import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { isProcessAlive, processStartTime } from './process-liveness.js';

/**
 * BRAIN-642 slice C1: the runner-side content-addressed blob store, `<remoteRoot>/blobs/<ns>/<aa>/<sha256>`.
 *
 * What it is for: a snapshot file the runner has seen before travels as a body-less `body:'cached'` frame and is copied out of
 * this store instead of crossing the wire again. What it is NOT: a trust boundary. The ssh account is already trusted; the
 * namespace only prevents accidental cross-repo reuse. The control that matters is that the hash comes from the validated
 * MANIFEST entry and every blob is re-read and re-hashed on use, so a flipped, truncated or swapped blob can never reach a workdir.
 *
 * Every function takes the runner's `root` first. `ns` and `sha` are hex-validated and never raw client strings in a path.
 */

// BRAIN-642: advertised by `lane remote-probe` only while this runner's blob store is enabled; a client sends `--transfer-cache` only to a runner that lists it.
export const BLOB_CACHE_CAPABILITY = 'blob-cache/1';
/** A have-query lease outlives its (short-lived) process: it covers the client's 5 minute transfer deadline plus slack. */
export const HAVE_LEASE_TTL_MS = 10 * 60_000;

/** Smaller files cost more to look up than to send. */
export const MIN_CACHEABLE_BYTES = 8 * 1024;
export const DEFAULT_MAX_BYTES = 10 * 1024 * 1024 * 1024;
export const DEFAULT_MAX_AGE_MS = 14 * 24 * 3_600_000;
/** Publication is skipped (soft cap) unless this much would stay free on the volume afterwards. */
export const DEFAULT_HEADROOM_BYTES = 2 * 1024 * 1024 * 1024;
/** Entries one GC pass examines (whole `<aa>` shards, so a little over); keeps the pass bounded on a huge cache. */
export const DEFAULT_MAX_EXAMINED = 5000;
const STALE_TMP_MS = 3_600_000;
const STALE_UNREADABLE_LEASE_MS = 24 * 3_600_000;

const NS_RE = /^[0-9a-f]{32}$/;
const SHA_RE = /^[0-9a-f]{64}$/;
const AA_RE = /^[0-9a-f]{2}$/;
const O_NOFOLLOW = fs.constants.O_NOFOLLOW;

/** `--transfer-cache` takes no value; any `--transfer-cache=...` spelling is refused rather than silently ignored. */
export function parseTransferCacheFlag(args) {
  const bad = args.find((a) => a.startsWith('--transfer-cache=') );
  if (bad !== undefined) return { ok: false, reason: `unknown --transfer-cache value: ${bad.slice('--transfer-cache='.length)}` };
  return { ok: true, transferCache: args.includes('--transfer-cache') };
}

/** Namespace of one repository: a hash of the FULL git-common-dir string the client supplies (not the lossy `sanitizeKey`). */
export function blobNamespace(identity) {
  if (typeof identity !== 'string' || identity.length === 0) throw new Error('blob namespace needs a non-empty repo identity');
  return crypto.createHash('sha256').update(`lane-broker-blob-ns/1\0${identity}`).digest('hex').slice(0, 32);
}

export function isBlobHash(value) {
  return typeof value === 'string' && SHA_RE.test(value);
}

export function isCacheable(entry) {
  return entry.type === 'file' && entry.size >= MIN_CACHEABLE_BYTES && SHA_RE.test(entry.sha256 ?? '');
}

function sha256Of(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function blobsDir(root) {
  return path.join(root, 'blobs');
}

export function blobPath(root, ns, sha) {
  if (!NS_RE.test(ns)) throw new Error('invalid blob namespace');
  if (!SHA_RE.test(sha)) throw new Error('invalid blob hash');
  return path.join(blobsDir(root), ns, sha.slice(0, 2), sha);
}

/**
 * lstat-validate `blobs/`, `<ns>/` and `<aa>/`: each must be a real directory, never a symlink. Returns the `<aa>` directory, or
 * null when an ancestor is absent (and not `create`). A symlink or non-directory ancestor throws; it is never followed.
 */
function validatedShardDir(root, ns, sha, { create = false } = {}) {
  let dir = root;
  for (const part of ['blobs', ns, sha.slice(0, 2)]) {
    dir = path.join(dir, part);
    const st = fs.lstatSync(dir, { throwIfNoEntry: false });
    if (!st) {
      if (!create) return null;
      fs.mkdirSync(dir, { mode: 0o700 });
    } else if (!st.isDirectory()) {
      throw new Error(`blob store ancestor is not a plain directory: ${dir}`);
    }
  }
  return dir;
}

function touch(file) {
  const now = new Date();
  try {
    fs.utimesSync(file, now, now);
  } catch {
    // the LRU clock is best-effort
  }
}

function unlinkQuiet(file) {
  try {
    fs.unlinkSync(file);
  } catch {
    // already gone
  }
}

/** Read a blob without following a symlink; `{status:'hit', bytes}`, `'miss'` or `'corrupt'` (the bad blob is removed). */
function readBlobVerified(root, ns, entry) {
  // before ANY fs access: an unvalidated hash or namespace is a path component ('../victim') and a mismatch below unlinks it
  if (!NS_RE.test(ns) || !isBlobHash(entry.sha256)) return { status: 'miss' };
  let shard;
  try {
    shard = validatedShardDir(root, ns, entry.sha256);
  } catch {
    return { status: 'miss' };
  }
  if (shard === null) return { status: 'miss' };
  const file = path.join(shard, entry.sha256);
  if (!fs.lstatSync(file, { throwIfNoEntry: false })) return { status: 'miss' };
  let bytes;
  try {
    // O_NOFOLLOW is the symlink defence: a link planted at the blob path fails the open (ELOOP) and is removed as corrupt.
    const fd = fs.openSync(file, fs.constants.O_RDONLY | O_NOFOLLOW);
    try {
      const st = fs.fstatSync(fd);
      if (!st.isFile() || st.size !== entry.size) throw new Error('not a regular file of the expected size');
      bytes = fs.readFileSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    unlinkQuiet(file);
    return { status: 'corrupt' };
  }
  if (bytes.length !== entry.size || sha256Of(bytes) !== entry.sha256) {
    unlinkQuiet(file);
    return { status: 'corrupt' };
  }
  touch(file);
  return { status: 'hit', bytes };
}

/** Is a blob of exactly this size and hash present and intact? `'hit' | 'miss' | 'corrupt'`; a corrupt blob is removed. */
export function hasBlob(root, ns, sha, size) {
  return readBlobVerified(root, ns, { sha256: sha, size }).status;
}

/**
 * Copy the verified blob to `destPath` (O_EXCL|O_NOFOLLOW, mode per the manifest's `exec`), never hardlinking, so a lane that
 * rewrites its workdir file cannot change the cache. The bytes written are the bytes that were hashed. Returns
 * `{status:'hit'|'miss'|'corrupt'}`; a failure to write the destination throws.
 */
export function materializeBlob(root, ns, entry, destPath) {
  const read = readBlobVerified(root, ns, entry);
  if (read.status !== 'hit') return { status: read.status };
  const fd = fs.openSync(destPath, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | O_NOFOLLOW, 0o644);
  try {
    fs.writeSync(fd, read.bytes);
  } finally {
    fs.closeSync(fd);
  }
  fs.chmodSync(destPath, entry.exec ? 0o755 : 0o644);
  return { status: 'hit' };
}

/**
 * The have-query's answer: the indices of `hashes` whose blob is present and intact under `ns`. The size comes from the blob itself
 * (the query carries only hashes) and `hasBlob` then hashes the content, so a corrupt blob is removed rather than reported. Each
 * lookup goes through the same lstat-validated ancestors as every other read.
 */
export function haveBlobs(root, ns, hashes) {
  const have = [];
  hashes.forEach((sha, index) => {
    let size;
    try {
      const shard = validatedShardDir(root, ns, sha);
      const st = shard === null ? undefined : fs.lstatSync(path.join(shard, sha), { throwIfNoEntry: false });
      if (st?.isFile()) size = st.size;
    } catch {
      return;
    }
    if (size !== undefined && hasBlob(root, ns, sha, size) === 'hit') have.push(index);
  });
  return have;
}

function freeBytesOf(root) {
  const st = fs.statfsSync(root);
  return Number(st.bavail) * Number(st.bsize);
}

/**
 * Store `bytes` as the blob `entry.sha256` describes. The bytes are hashed against the manifest entry BEFORE anything is
 * written, so a mismatching body is never cached. Atomic (unique tmp name, rename) and idempotent under concurrent writers.
 * Best-effort: returns `'published' | 'exists' | 'skipped' | 'not-cached'` and never throws; `freeBytes` is a test seam.
 */
export function publishBlob(root, ns, entry, bytes, { headroomBytes = DEFAULT_HEADROOM_BYTES, freeBytes = freeBytesOf } = {}) {
  try {
    if (!isCacheable(entry) || bytes.length !== entry.size) return 'not-cached';
    if (sha256Of(bytes) !== entry.sha256) return 'not-cached';
    const shard = validatedShardDir(root, ns, entry.sha256, { create: true });
    const file = path.join(shard, entry.sha256);
    const existing = fs.lstatSync(file, { throwIfNoEntry: false });
    if (existing?.isFile() && existing.size === entry.size) return 'exists';
    if (freeBytes(root) - bytes.length < headroomBytes) return 'skipped';
    const tmp = path.join(shard, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
    const fd = fs.openSync(tmp, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | O_NOFOLLOW, 0o400);
    try {
      fs.writeSync(fd, bytes);
    } finally {
      fs.closeSync(fd);
    }
    try {
      fs.renameSync(tmp, file);
    } catch (err) {
      unlinkQuiet(tmp);
      throw err;
    }
    return 'published';
  } catch {
    return 'not-cached';
  }
}

// ---- leases ---------------------------------------------------------------------------------------------------------------

function leasesDir(root) {
  return path.join(blobsDir(root), '.leases');
}

function ensureLeasesDir(root) {
  for (const dir of [blobsDir(root), leasesDir(root)]) {
    const st = fs.lstatSync(dir, { throwIfNoEntry: false });
    if (!st) fs.mkdirSync(dir, { mode: 0o700 });
    else if (!st.isDirectory()) throw new Error(`blob store directory is not a plain directory: ${dir}`);
  }
  return leasesDir(root);
}

/**
 * Lease a set of blob hashes for the life of a run: eviction skips them. Taken and checked under the SAME `lock(fn)` (the
 * broker lock in production) that eviction renames under, so a run either holds its lease before eviction renames a victim or
 * finds the blob gone (a miss). Returns `async release()`. The lease names this process; a dead process's lease is reaped.
 */
export async function acquireBlobLeases(root, hashes, lock, { ttlMs } = {}) {
  const valid = [...new Set(hashes)].filter((h) => SHA_RE.test(h));
  const file = await lock(() => {
    const dir = ensureLeasesDir(root);
    const target = path.join(dir, `${process.pid}-${crypto.randomBytes(6).toString('hex')}.json`);
    const tmp = path.join(dir, `.tmp-${crypto.randomBytes(6).toString('hex')}`);
    const expiresAt = ttlMs === undefined ? undefined : Date.now() + ttlMs;
    fs.writeFileSync(tmp, JSON.stringify({ pid: process.pid, start: processStartTime(process.pid) ?? null, hashes: valid, ...(expiresAt === undefined ? {} : { expiresAt }) }));
    fs.renameSync(tmp, target);
    return target;
  });
  return async () => {
    await lock(() => fs.rmSync(file, { force: true }));
  };
}

/** Hashes held by a live lease. A lease with an `expiresAt` (a have-query's: its process is gone before the run starts) lives until then;
 *  otherwise a dead process's lease is removed; an unreadable one blocks eviction until a day old. */
export function leasedHashes(root, { now = Date.now() } = {}) {
  const held = new Set();
  const dir = leasesDir(root);
  // `blobs/` and `blobs/.leases` must be plain directories: through a symlink this function would read and REMOVE files elsewhere.
  // Eviction is blocked outright until an operator clears the link.
  for (const d of [blobsDir(root), dir]) {
    const st = fs.lstatSync(d, { throwIfNoEntry: false });
    if (!st) return { held, blockAll: false };
    if (!st.isDirectory()) return { held, blockAll: true };
  }
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return { held, blockAll: false };
  }
  let blockAll = false;
  for (const name of names) {
    if (name.startsWith('.tmp-')) continue;
    const file = path.join(dir, name);
    if (!fs.lstatSync(file, { throwIfNoEntry: false })?.isFile()) {
      blockAll = true;
      continue;
    }
    let lease;
    try {
      lease = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      const st = fs.statSync(file, { throwIfNoEntry: false });
      if (st && now - st.mtimeMs > STALE_UNREADABLE_LEASE_MS) fs.rmSync(file, { force: true });
      else blockAll = true;
      continue;
    }
    if (!lease || !Number.isInteger(lease.pid) || !Array.isArray(lease.hashes)) {
      blockAll = true;
      continue;
    }
    const live = Number.isFinite(lease.expiresAt) ? lease.expiresAt > now : isProcessAlive(lease.pid, lease.start ?? undefined);
    if (live) for (const h of lease.hashes) held.add(h);
    else fs.rmSync(file, { force: true });
  }
  return { held, blockAll };
}

// ---- eviction -------------------------------------------------------------------------------------------------------------

function readdirTyped(dir) {
  try {
    return fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return [];
  }
}

/** Remove a symlink (or other non-directory) found where a store directory belongs: only the link itself, never its target. */
function removeBadAncestor(dir) {
  const st = fs.lstatSync(dir, { throwIfNoEntry: false });
  if (st && !st.isDirectory()) unlinkQuiet(dir);
}

function shuffled(list) {
  const out = [...list];
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = crypto.randomInt(i + 1);
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

/** Collect candidates from random `<ns>/<aa>` shards until `maxExamined` entries were looked at. Bounded; never the whole cache. */
function scanShards(root, { maxExamined, now }) {
  const base = blobsDir(root);
  const shards = [];
  for (const nsEntry of readdirTyped(base)) {
    if (!NS_RE.test(nsEntry.name)) continue;
    const nsDir = path.join(base, nsEntry.name);
    if (!nsEntry.isDirectory()) {
      removeBadAncestor(nsDir);
      continue;
    }
    for (const aaEntry of readdirTyped(nsDir)) {
      if (!AA_RE.test(aaEntry.name)) continue;
      const aaDir = path.join(nsDir, aaEntry.name);
      if (!aaEntry.isDirectory()) removeBadAncestor(aaDir);
      else shards.push(aaDir);
    }
  }
  const candidates = [];
  let examined = 0;
  let scanned = 0;
  for (const aaDir of shuffled(shards)) {
    if (examined >= maxExamined) break;
    scanned += 1;
    for (const f of readdirTyped(aaDir)) {
      examined += 1;
      const file = path.join(aaDir, f.name);
      const st = fs.lstatSync(file, { throwIfNoEntry: false });
      if (!st) continue;
      if (f.name.startsWith('.tmp-')) {
        if (now - st.mtimeMs > STALE_TMP_MS) unlinkQuiet(file);
        continue;
      }
      if (!SHA_RE.test(f.name) || !st.isFile()) {
        unlinkQuiet(file); // junk in a store directory: a symlink is unlinked, never followed
        continue;
      }
      candidates.push({ file, aaDir, sha: f.name, size: st.size, mtimeMs: st.mtimeMs });
    }
  }
  return { candidates, scanned, totalShards: shards.length };
}

/**
 * One bounded GC pass. Victims are chosen outside the lock (age, then least-recently-used until the sampled bytes fit
 * `maxBytes` scaled by the fraction of shards examined). Under `lock` each victim is re-checked against the CURRENT leases,
 * re-validated and re-stat'ed (a blob used since selection is kept), then moved by rename to `blobs/.trash-*`; the trash is
 * purged afterwards. `afterSelect` is a test seam for the select -> lease -> rename interleaving. Returns the evicted hashes.
 */
export async function evictBlobs(root, { maxBytes = DEFAULT_MAX_BYTES, maxAgeMs = DEFAULT_MAX_AGE_MS, maxExamined = DEFAULT_MAX_EXAMINED, now = Date.now(), lock, afterSelect } = {}) {
  const base = blobsDir(root);
  const baseStat = fs.lstatSync(base, { throwIfNoEntry: false });
  if (!baseStat || !baseStat.isDirectory()) return [];
  purgeTrash(root);
  const { candidates, scanned, totalShards } = scanShards(root, { maxExamined, now });
  const victims = new Map();
  for (const c of candidates) if (now - c.mtimeMs > maxAgeMs) victims.set(c.file, c);
  const fraction = totalShards === 0 || scanned === 0 ? 1 : scanned / totalShards;
  const budget = maxBytes * fraction;
  let kept = candidates.filter((c) => !victims.has(c.file)).reduce((sum, c) => sum + c.size, 0);
  for (const c of candidates.filter((x) => !victims.has(x.file)).sort((a, b) => a.mtimeMs - b.mtimeMs)) {
    if (kept <= budget) break;
    victims.set(c.file, c);
    kept -= c.size;
  }
  if (afterSelect) await afterSelect([...victims.values()].map((v) => v.sha));

  const evicted = [];
  await lock(() => {
    const { held, blockAll } = leasedHashes(root, { now });
    if (blockAll) return;
    for (const v of victims.values()) {
      if (held.has(v.sha)) continue;
      const aaStat = fs.lstatSync(v.aaDir, { throwIfNoEntry: false });
      if (!aaStat || !aaStat.isDirectory()) {
        if (aaStat) unlinkQuiet(v.aaDir); // the link only
        continue;
      }
      const nsStat = fs.lstatSync(path.dirname(v.aaDir), { throwIfNoEntry: false });
      if (!nsStat || !nsStat.isDirectory()) {
        if (nsStat) unlinkQuiet(path.dirname(v.aaDir));
        continue;
      }
      const now2 = fs.lstatSync(v.file, { throwIfNoEntry: false });
      if (!now2 || now2.mtimeMs > v.mtimeMs) continue; // used since selection
      try {
        fs.renameSync(v.file, path.join(base, `.trash-${crypto.randomBytes(6).toString('hex')}`));
        evicted.push(v.sha);
      } catch {
        // best-effort; found again next pass
      }
    }
  });
  purgeTrash(root);
  return evicted;
}

/** Delete what earlier passes moved aside. */
export function purgeTrash(root) {
  for (const e of readdirTyped(blobsDir(root))) {
    if (e.name.startsWith('.trash-')) fs.rmSync(path.join(blobsDir(root), e.name), { recursive: true, force: true });
  }
}
