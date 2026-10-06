import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { atomicWriteFile, atomicWriteJson, fsyncDirectory } from '../state.js';
import { manifestHashOf, isCanonicalRelPath } from '../remote-manifest.js';
import { MAX_HEADER_BYTES } from '../remote-stream.js';
import { Journal } from './journal.js';
import { acquireStoreLock, acquireStoreLockAsync, LockLostError } from './lock.js';
import { RETENTION_DEFAULTS } from './retention.js';
import { isSha256, isJobId, blobRelPath, manifestRelPath, parseObjectPath } from './ids.js';

export class StoreError extends Error {
  constructor(status, message, extra) {
    super(message);
    this.name = 'StoreError';
    this.status = status;
    this.extra = extra;
  }
}

export const DEFAULT_MAX_BLOB_BYTES = 512 * 1024 * 1024;
/** The ONE manifest limit: the snapshot header line a receiver must read is the manifest plus a few bytes of framing. */
export const MAX_MANIFEST_BYTES = MAX_HEADER_BYTES - 4096;
const ADMIT_FRACTION = 0.95; // spec 8.2: refuse new data at 95% of the cap

const READ_NOFOLLOW = fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW;

/** Regular files under `dir` (never following symlinks; a symlink is simply not listed). */
function walkFiles(dir, out = []) {
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch (err) {
    if (err.code === 'ENOENT') return out;
    throw err;
  }
  for (const name of names) {
    const full = path.join(dir, name);
    const st = fs.lstatSync(full);
    if (st.isDirectory()) walkFiles(full, out);
    else if (st.isFile()) out.push({ full, size: st.size, mtimeMs: st.mtimeMs });
  }
  return out;
}

async function hashStream(stream) {
  const hash = crypto.createHash('sha256');
  let size = 0;
  for await (const chunk of stream) {
    hash.update(chunk);
    size += chunk.length;
  }
  return { sha256: hash.digest('hex'), size };
}

/** Hash a regular file opened with O_NOFOLLOW: a symlink at the leaf is refused, never followed. */
export function hashFile(file) {
  const fd = fs.openSync(file, READ_NOFOLLOW);
  return hashStream(fs.createReadStream(null, { fd }));
}

/** Every committed object under `root` as `{path, size, sha256?}`, sorted by path. `deep` re-hashes from disk. */
export async function listObjects(root, { deep = false } = {}) {
  const out = [];
  for (const sub of ['blobs', 'manifests']) {
    for (const f of walkFiles(path.join(root, sub))) {
      const rel = path.relative(root, f.full).split(path.sep).join('/');
      if (!parseObjectPath(rel)) continue;
      const entry = { path: rel, size: f.size };
      if (deep) entry.sha256 = (await hashFile(f.full)).sha256;
      out.push(entry);
    }
  }
  return out.sort((a, b) => (a.path < b.path ? -1 : 1));
}

function exactKeys(obj, allowed, required, what) {
  const keys = Object.keys(obj);
  const extra = keys.filter((k) => !allowed.includes(k));
  if (extra.length) throw new StoreError(400, `${what} has unknown field(s): ${extra.join(', ')}`);
  for (const k of required) if (!(k in obj)) throw new StoreError(400, `${what} is missing ${k}`);
}

/** Strict shape check for a registered manifest. Path/symlink safety is NOT judged here: that is the receiver's job (materialize.js). */
function parseManifestDoc(bytes) {
  let doc;
  try {
    doc = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw new StoreError(400, 'manifest is not JSON');
  }
  if (!doc || typeof doc !== 'object' || Array.isArray(doc)) throw new StoreError(400, 'manifest must be an object');
  exactKeys(doc, ['manifestHash', 'entries'], ['manifestHash', 'entries'], 'manifest');
  if (!isSha256(doc.manifestHash) || !Array.isArray(doc.entries)) throw new StoreError(400, 'manifest needs {manifestHash, entries}');
  for (const e of doc.entries) {
    if (!e || typeof e !== 'object' || !isCanonicalRelPath(e.path)) throw new StoreError(400, `manifest entry has a non-canonical path: ${e && e.path}`);
    if (e.type === 'file') {
      exactKeys(e, ['path', 'type', 'exec', 'size', 'sha256'], ['path', 'type', 'size', 'sha256'], `entry ${e.path}`);
      if (!isSha256(e.sha256) || !Number.isInteger(e.size) || e.size < 0) throw new StoreError(400, `bad file entry: ${e.path}`);
      if ('exec' in e && typeof e.exec !== 'boolean') throw new StoreError(400, `bad exec flag: ${e.path}`);
    } else if (e.type === 'symlink') {
      exactKeys(e, ['path', 'type', 'target'], ['path', 'type', 'target'], `entry ${e.path}`);
      if (typeof e.target !== 'string') throw new StoreError(400, `bad symlink entry: ${e.path}`);
    } else {
      throw new StoreError(400, `bad entry type: ${e.path}`);
    }
  }
  if (manifestHashOf(doc.entries) !== doc.manifestHash) throw new StoreError(422, 'manifestHash does not match entries');
  return doc;
}

/**
 * The on-disk store: content-addressed blobs (`blobs/ab/cd/<sha256>`), immutable job manifests
 * (`manifests/<job>.json`) and mutable per-job state (`meta/<job>.json`: pin, terminal mark). Every state change
 * (object commit, terminal mark, pin, deletion) is a journal record, so a replica can mirror retention exactly.
 * Nothing under the root may be a symlink: ancestors are lstat-checked and leaves opened with O_NOFOLLOW.
 */
export class ObjectStore {
  constructor(root, { now = Date.now, maxBlobBytes = DEFAULT_MAX_BLOB_BYTES, capBytes = Infinity, replicaMode = false, replicaGraceHours = RETENTION_DEFAULTS.manifestAfterTerminalMs / 3_600_000, lock } = {}) {
    fs.mkdirSync(root, { recursive: true });
    this.root = fs.realpathSync(root); // confine to the real directory; every later path is checked against symlinks below it
    this.lock = lock ?? acquireStoreLock(this.root); // `ObjectStore.open` passes the platform lock (kernel-held on Linux)
    try {
      this.init({ now, maxBlobBytes, capBytes, replicaMode, replicaGraceHours });
    } catch (err) {
      this.lock.release();
      throw err;
    }
  }

  /** Open a store holding the platform's lock (the kernel-held abstract socket on Linux, the file lock elsewhere). */
  static async open(root, opts = {}) {
    fs.mkdirSync(root, { recursive: true });
    const lock = await acquireStoreLockAsync(fs.realpathSync(root));
    return new ObjectStore(root, { ...opts, lock }); // the constructor releases the lock itself if initialisation fails
  }

  init({ now, maxBlobBytes, capBytes, replicaMode, replicaGraceHours }) {
    this.now = now;
    this.replicaGraceMs = replicaGraceHours * 3600 * 1000;
    this.maxBlobBytes = maxBlobBytes;
    this.capBytes = capBytes;
    this.replicaMode = replicaMode;
    for (const d of ['tmp', 'blobs', 'manifests', 'meta']) {
      const dir = path.join(this.root, d);
      try {
        if (!fs.lstatSync(dir).isDirectory()) throw new Error(`${dir} is not a real directory: symlinks under the store root are refused`);
      } catch (err) {
        if (err.code !== 'ENOENT') throw err;
        fs.mkdirSync(dir);
      }
    }
    for (const f of walkFiles(path.join(this.root, 'tmp'))) fs.unlinkSync(f.full); // leftovers of a crash mid-upload
    this.journal = new Journal(this.root, { trackPending: !replicaMode, guard: () => this.assertOwner() });
    this.bytes = [...walkFiles(path.join(this.root, 'blobs')), ...walkFiles(path.join(this.root, 'manifests'))].reduce((n, f) => n + f.size, 0);
    this.reserved = 0; // bytes admitted but not yet committed
    this.unjournaled = new Set(); // blobs on disk without a journal record (append or post-rename step failed AND the rollback failed)
    this.pendingDeletes = new Set(); // objects unlinked whose delete record is not yet journaled
    this.verifying = new Map(); // path -> number of in-flight rejournal hashes; retention defers a path until the count is 0
    this.fenced = false;
    this.onLockLost = null;
    this.intents = new Set(); // paths with a journaled delete-intent not yet completed or cancelled
    this.lost = new Set(); // journaled objects missing on disk with no delete intent: accidental loss, never tombstoned
    this.jobBlobCache = new Map();
    for (const [rel, st] of this.journal.objectState()) if (st === 'intent') this.intents.add(rel);
    this.reconcileReport = this.reconcile();
  }

  /** Fence: every journal append and replication-state write calls this first. A displaced owner never writes again. */
  assertOwner() {
    try {
      this.lock.assertHeld();
    } catch (err) {
      if (err instanceof LockLostError && !this.fenced) {
        this.fenced = true;
        this.onLockLost?.(err);
      }
      throw err;
    }
  }

  /** Returns the lock release (a promise on Linux): await it before re-opening the same root. */
  close() {
    this.journal.close();
    return this.lock.release();
  }

  /** Absolute path of a store-relative path, refusing a symlink at any existing component. */
  abs(rel) {
    const segs = rel.split('/');
    let cur = this.root;
    for (const seg of segs) {
      cur = path.join(cur, seg);
      let st;
      try {
        st = fs.lstatSync(cur);
      } catch (err) {
        if (err.code === 'ENOENT') break;
        throw err;
      }
      if (st.isSymbolicLink()) throw new StoreError(500, `store integrity: symlink at ${rel}`);
    }
    return path.join(this.root, ...segs);
  }

  /** Open a store file read-only without following a symlink; null when absent. */
  openRead(rel) {
    const file = this.abs(rel);
    let fd;
    try {
      fd = fs.openSync(file, READ_NOFOLLOW);
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      if (err.code === 'ELOOP') throw new StoreError(500, `store integrity: symlink at ${rel}`);
      throw err;
    }
    const st = fs.fstatSync(fd);
    if (!st.isFile()) {
      fs.closeSync(fd);
      throw new StoreError(500, `store integrity: not a regular file: ${rel}`);
    }
    return { fd, size: st.size };
  }

  readFileSafe(rel) {
    const f = this.openRead(rel);
    if (!f) return null;
    try {
      return fs.readFileSync(f.fd);
    } finally {
      fs.closeSync(f.fd);
    }
  }

  /**
   * Startup reconcile, both directions, before serving. (1) Every object on disk with no live put record is journaled
   * (blobs before manifests). (2) An object missing on disk whose last record is a `delete-intent` is an interrupted
   * retention: its `delete` is journaled. (3) An object missing with NO intent is a LOSS: it is never tombstoned (that
   * would delete the replica's intact copy); it is recorded in `lost`, reported on /metrics and by compare, and
   * replication of that path blocks until an operator re-uploads it or pulls it back from the replica (README).
   */
  reconcile() {
    const state = this.journal.objectState();
    const report = { journaled: [], corrupt: [], deleted: [], lost: [] };
    const orphans = [];
    const onDisk = new Set();
    for (const sub of ['blobs', 'manifests']) {
      for (const f of walkFiles(path.join(this.root, sub))) {
        const rel = path.relative(this.root, f.full).split(path.sep).join('/');
        const parsed = parseObjectPath(rel);
        if (!parsed) continue;
        onDisk.add(rel);
        const st = state.get(rel);
        if (st !== 'put' && st !== 'intent') orphans.push({ rel, parsed });
      }
    }
    orphans.sort((a, b) => (a.parsed.kind === b.parsed.kind ? 0 : a.parsed.kind === 'blob' ? -1 : 1));
    for (const o of orphans) {
      const seen = hashFileSync(this.abs(o.rel));
      if (o.parsed.kind === 'blob' && seen.sha256 !== o.parsed.sha) {
        report.corrupt.push(o.rel);
        continue;
      }
      this.journal.append({ kind: o.parsed.kind, path: o.rel, sha256: seen.sha256, size: seen.size, createdAt: this.now() });
      report.journaled.push(o.rel);
    }
    for (const [rel, st] of state) {
      if (onDisk.has(rel)) continue;
      if (st === 'intent') {
        this.journal.append({ kind: 'delete', path: rel, createdAt: this.now() });
        report.deleted.push(rel);
      } else if (st === 'put') {
        this.lost.add(rel);
        report.lost.push(rel);
      }
    }
    return report;
  }

  blobSize(sha) {
    try {
      return fs.lstatSync(this.abs(blobRelPath(sha))).size;
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  hasBlob(sha) {
    return this.blobSize(sha) !== null;
  }

  /** Reserve `n` bytes against the admission threshold (95% of the cap); throws 507 when they do not fit. */
  reserve(n) {
    if (this.bytes + this.reserved + n >= this.capBytes * ADMIT_FRACTION) throw new StoreError(507, 'store is above 95% of its cap');
    this.reserved += n;
  }

  release(n) {
    this.reserved -= n;
  }

  journalBlob(sha) {
    const rel = blobRelPath(sha);
    this.journal.append({ kind: 'blob', path: rel, sha256: sha, size: this.blobSize(sha), createdAt: this.now() });
    this.unjournaled.delete(sha);
  }

  /** True when the blob exists and its bytes ON DISK have exactly the declared size and the address's hash. */
  async intactBlob(sha, declaredSize) {
    if (this.blobSize(sha) !== declaredSize) return false;
    try {
      const seen = await hashFile(this.abs(blobRelPath(sha)));
      return seen.sha256 === sha && seen.size === declaredSize;
    } catch (err) {
      if (err.code === 'ENOENT') return false;
      throw err;
    }
  }

  /** An intact blob already exists: verify the body against the address (without storing it) and acknowledge. No reservation. */
  async acknowledgeDuplicate(sha, readable) {
    const body = await hashStream(readable);
    if (body.sha256 !== sha) throw new StoreError(422, 'sha256 of the body does not match the address');
    if (!this.hasBlob(sha)) throw new StoreError(503, 'blob was removed while it was being re-uploaded; retry');
    const t = new Date(this.now());
    fs.utimesSync(this.abs(blobRelPath(sha)), t, t);
    if (this.unjournaled.has(sha)) this.journalBlob(sha);
    this.cancelIntent(blobRelPath(sha));
    return { stored: false };
  }

  /** The object is needed again: void a pending delete-intent so startup will never complete it into a tombstone. */
  cancelIntent(rel) {
    if (!this.intents.has(rel)) return;
    this.journal.append({ kind: 'delete-cancel', path: rel, createdAt: this.now() });
    this.intents.delete(rel);
  }

  assertRealTmp() {
    const dir = path.join(this.root, 'tmp');
    if (!fs.lstatSync(dir).isDirectory()) throw new StoreError(500, 'store integrity: tmp/ is not a real directory');
    return dir;
  }

  /**
   * Whole-file immutable upload. An intact existing blob is acknowledged WITHOUT reserving anything (so a retried upload
   * of data the store already holds can never be refused for capacity). New bytes reserve their declared size; the body is
   * streamed to tmp/ (every byte accounted for through `bytesWritten`), fsynced, then the bytes ON DISK are re-hashed and
   * sized. Commit checks the admission threshold plus live reservations BEFORE the rename; any failure after the rename
   * rolls the blob back, and if that fails too the blob is counted and recorded in `unjournaled` so a retry or a manifest
   * journals it first.
   */
  async putBlob(sha, readable, declaredSize) {
    if (!isSha256(sha)) throw new StoreError(400, 'bad sha256');
    if (declaredSize > this.maxBlobBytes) throw new StoreError(413, `blob exceeds the ${this.maxBlobBytes}-byte cap`);
    const final = this.abs(blobRelPath(sha));
    if (await this.intactBlob(sha, declaredSize)) return this.acknowledgeDuplicate(sha, readable);
    this.reserve(declaredSize);
    let held = declaredSize;
    let tmp;
    try {
      tmp = path.join(this.assertRealTmp(), `${sha}.${crypto.randomBytes(6).toString('hex')}`);
      const fh = await fs.promises.open(tmp, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW, 0o640);
      let size = 0;
      try {
        for await (const chunk of readable) {
          size += chunk.length;
          if (size > this.maxBlobBytes) throw new StoreError(413, `blob exceeds the ${this.maxBlobBytes}-byte cap`);
          let off = 0;
          while (off < chunk.length) {
            const { bytesWritten } = await fh.write(chunk, off, chunk.length - off);
            if (!(bytesWritten > 0)) throw new Error('blob write made no progress');
            off += bytesWritten;
          }
        }
        await fh.sync();
      } finally {
        await fh.close();
      }
      const onDisk = await hashFile(tmp);
      if (onDisk.size !== size || onDisk.sha256 !== sha) {
        throw new StoreError(422, 'the bytes on disk do not hash to the address');
      }
      if (await this.intactBlob(sha, size)) {
        // a concurrent upload of the same blob won the race while we were streaming
        const t = new Date(this.now());
        fs.utimesSync(final, t, t);
        if (this.unjournaled.has(sha)) this.journalBlob(sha);
        return { stored: false };
      }
      const replaced = this.blobSize(sha) ?? 0; // a corrupt blob under this address is overwritten, not trusted
      if (this.bytes + size + (this.reserved - held) >= this.capBytes * ADMIT_FRACTION) throw new StoreError(507, 'store is above 95% of its cap');
      this.flushPendingDeletes();
      fs.mkdirSync(path.dirname(final), { recursive: true });
      this.abs(blobRelPath(sha)); // re-check the ancestors now that they exist
      fs.renameSync(tmp, final);
      this.bytes += size - replaced; // counted from the moment it exists, whatever happens next
      try {
        const stamped = new Date(this.now());
        fs.utimesSync(final, stamped, stamped); // retention reads mtime as "last uploaded", on the store's clock
        fsyncDirectory(path.dirname(final));
        this.journalBlob(sha);
        this.intents.delete(blobRelPath(sha));
        this.lost.delete(blobRelPath(sha));
      } catch (err) {
        try {
          fs.unlinkSync(final);
          this.bytes -= size;
        } catch {
          this.unjournaled.add(sha); // still on disk without a record: the dedupe path and manifest registration journal it
        }
        throw err;
      }
      return { stored: true };
    } finally {
      this.release(held);
      held = 0;
      if (tmp) fs.rmSync(tmp, { force: true });
    }
  }

  readManifest(job) {
    if (!isJobId(job)) return null;
    const bytes = this.readFileSafe(manifestRelPath(job));
    return bytes ? JSON.parse(bytes.toString('utf8')) : null;
  }

  manifestSize(job) {
    try {
      return fs.lstatSync(this.abs(manifestRelPath(job))).size;
    } catch (err) {
      if (err.code === 'ENOENT') return 0;
      throw err;
    }
  }

  /**
   * Register `bytes` (a strict `{manifestHash, entries}` document) as the immutable manifest of `job`. Every
   * referenced blob must be stored AND journaled (an unjournaled one is journaled here first), so no journal prefix
   * ever contains a manifest without the blobs it needs. Manifest bytes count toward the cap.
   */
  registerManifest(job, bytes) {
    if (!isJobId(job)) throw new StoreError(400, 'bad job id');
    if (bytes.length > MAX_MANIFEST_BYTES) throw new StoreError(413, 'manifest too large');
    const doc = parseManifestDoc(bytes);
    const existing = this.readManifest(job);
    if (existing) {
      if (existing.manifestHash !== doc.manifestHash) throw new StoreError(409, 'job already has a different manifest');
      return { stored: false };
    }
    const missing = [];
    for (const e of doc.entries) {
      if (e.type !== 'file') continue;
      if (this.blobSize(e.sha256) !== e.size) missing.push(e.sha256);
    }
    if (missing.length) throw new StoreError(409, 'manifest references blobs the store does not have', { missing: [...new Set(missing)] });
    if (this.bytes + this.reserved + bytes.length >= this.capBytes * ADMIT_FRACTION) throw new StoreError(507, 'store is above 95% of its cap');
    this.flushPendingDeletes();
    for (const e of doc.entries) if (e.type === 'file' && this.unjournaled.has(e.sha256)) this.journalBlob(e.sha256);
    for (const e of doc.entries) if (e.type === 'file') this.cancelIntent(blobRelPath(e.sha256));
    const rel = manifestRelPath(job);
    const metaRel = `meta/${job}.json`;
    atomicWriteFile(this.abs(rel), bytes, { fsync: true });
    this.bytes += bytes.length;
    try {
      atomicWriteJson(this.abs(metaRel), { registeredAt: this.now(), terminalAt: null, pinned: false }, { fsync: true });
      this.journal.append({ kind: 'manifest', path: rel, sha256: crypto.createHash('sha256').update(bytes).digest('hex'), size: bytes.length, createdAt: this.now() });
    } catch (err) {
      fs.rmSync(this.abs(rel), { force: true });
      fs.rmSync(this.abs(metaRel), { force: true });
      this.bytes -= bytes.length;
      throw err;
    }
    this.lost.delete(rel);
    return { stored: true };
  }

  /** Set of blob shas the manifest of `job` references (cached; manifests are immutable). null when the job has no manifest. */
  jobBlobs(job) {
    if (this.jobBlobCache.has(job)) return this.jobBlobCache.get(job);
    const doc = this.readManifest(job);
    if (!doc) return null;
    const set = new Set(doc.entries.filter((e) => e.type === 'file').map((e) => e.sha256));
    if (this.jobBlobCache.size >= 64) this.jobBlobCache.delete(this.jobBlobCache.keys().next().value);
    this.jobBlobCache.set(job, set);
    return set;
  }

  meta(job) {
    const bytes = isJobId(job) ? this.readFileSafe(`meta/${job}.json`) : null;
    return bytes ? JSON.parse(bytes.toString('utf8')) : { registeredAt: null, terminalAt: null, pinned: false };
  }

  /** Change one retention field of a job and journal it; undone if the journal append fails. Idempotent: a no-op is not journaled. */
  setJobState(job, field, value) {
    if (!this.readManifest(job)) throw new StoreError(404, 'no such job');
    this.flushPendingDeletes();
    const before = this.meta(job);
    if (before[field] === value) return false;
    const metaRel = `meta/${job}.json`;
    atomicWriteJson(this.abs(metaRel), { ...before, [field]: value }, { fsync: true });
    try {
      const kind = field === 'pinned' ? 'pin' : 'terminal';
      this.journal.append({ kind, path: manifestRelPath(job), job, [field]: value, createdAt: this.now() });
    } catch (err) {
      atomicWriteJson(this.abs(metaRel), before, { fsync: true });
      throw err;
    }
    return true;
  }

  markTerminal(job, at = this.now()) {
    return this.setJobState(job, 'terminalAt', at);
  }

  setPinned(job, pinned) {
    return this.setJobState(job, 'pinned', !!pinned);
  }

  /** True while a pinned or non-terminal manifest references the blob: nobody, whoever they are, may delete it. */
  isProtectedBlob(sha) {
    for (const name of fs.readdirSync(path.join(this.root, 'manifests'))) {
      const parsed = parseObjectPath(`manifests/${name}`);
      if (!parsed) continue;
      const m = this.meta(parsed.job);
      if (!m.pinned && m.terminalAt != null) continue;
      if (this.jobBlobs(parsed.job)?.has(sha)) return true;
    }
    return false;
  }

  journalDelete(rel) {
    this.journal.append({ kind: 'delete', path: rel, createdAt: this.now() });
    this.pendingDeletes.delete(rel);
  }

  /** Complete unlinked-but-unjournaled deletes. Called before EVERY append that could be ordered after them, and periodically. */
  flushPendingDeletes() {
    for (const rel of [...this.pendingDeletes]) this.journalDelete(rel);
  }

  /** Periodic in-process housekeeping (the server schedules it): nothing stays pending until the next restart. */
  maintain() {
    this.flushPendingDeletes();
  }

  /**
   * Retention/replication delete in three durable steps: journal `delete-intent`, unlink, journal `delete`. A failed
   * unlink leaves the object live (the intent alone changes nothing); a failed final append leaves the delete pending,
   * retried by `maintain` and before any later append, and by startup reconcile (intent present, object gone). Without an
   * intent an absent object is a loss, never a deletion.
   */
  deleteObject(rel) {
    const parsed = parseObjectPath(rel);
    if (!parsed) throw new StoreError(400, 'not an object path');
    if (parsed.kind === 'blob' && this.isProtectedBlob(parsed.sha)) throw new StoreError(409, 'blob is still referenced by a pinned or non-terminal manifest');
    if (this.verifying.get(rel) > 0) return false; // rejournal is hashing it right now: deferred to the next sweep
    this.flushPendingDeletes();
    const file = this.abs(rel);
    let st;
    try {
      st = fs.lstatSync(file);
    } catch (err) {
      if (err.code === 'ENOENT') return false;
      throw err;
    }
    this.journal.append({ kind: 'delete-intent', path: rel, createdAt: this.now() });
    this.intents.add(rel);
    fs.unlinkSync(file);
    fsyncDirectory(path.dirname(file)); // the unlink must be durable before the delete record, or a crash resurrects the file
    this.bytes -= st.size;
    this.pendingDeletes.add(rel);
    if (parsed.kind === 'manifest') {
      fs.rmSync(this.abs(`meta/${parsed.job}.json`), { force: true });
      fsyncDirectory(path.join(this.root, 'meta'));
      this.jobBlobCache.delete(parsed.job);
    }
    this.journalDelete(rel);
    this.intents.delete(rel);
    return true;
  }

  /** Re-read an object from disk and hash it: what a peer reports so the replicator can compare against the journal. */
  async verifyObject(rel) {
    if (!parseObjectPath(rel)) throw new StoreError(400, 'not an object path');
    const f = this.openRead(rel);
    if (!f) return null;
    return hashStream(fs.createReadStream(null, { fd: f.fd }));
  }

  /** Append journal records for committed objects that have none (journal loss, or a crash the startup reconcile has not seen yet). */
  async rejournal(paths) {
    this.flushPendingDeletes();
    const added = [];
    for (const rel of paths) {
      if (!parseObjectPath(rel)) throw new StoreError(400, 'not an object path');
      this.verifying.set(rel, (this.verifying.get(rel) ?? 0) + 1);
      try {
        const v = await this.verifyObject(rel);
        if (!v) continue;
        this.abs(rel); // still there and still not a symlink; no await between this check and the append
        added.push(this.journal.append({ kind: parseObjectPath(rel).kind, path: rel, sha256: v.sha256, size: v.size, createdAt: this.now() }));
      } finally {
        const left = this.verifying.get(rel) - 1;
        if (left > 0) this.verifying.set(rel, left);
        else this.verifying.delete(rel);
      }
    }
    return added;
  }
}

function hashFileSync(file) {
  const fd = fs.openSync(file, READ_NOFOLLOW);
  let buf;
  try {
    buf = fs.readFileSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  return { sha256: crypto.createHash('sha256').update(buf).digest('hex'), size: buf.length };
}
