import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { atomicWriteFile, atomicWriteJson, fsyncDirectory, readJsonSafe } from '../state.js';
import { manifestHashOf, isCanonicalRelPath } from '../remote-manifest.js';
import { Journal } from './journal.js';
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
export const MAX_MANIFEST_BYTES = 16 * 1024 * 1024; // same bound as the snapshot header (remote-stream MAX_HEADER_BYTES)

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

async function hashFile(file) {
  const hash = crypto.createHash('sha256');
  let size = 0;
  for await (const chunk of fs.createReadStream(file)) {
    hash.update(chunk);
    size += chunk.length;
  }
  return { sha256: hash.digest('hex'), size };
}

/** Every committed object under `root` as `{path, size, sha256?}`, sorted by path. `deep` re-hashes blobs from disk. */
export async function listObjects(root, { deep = false } = {}) {
  const out = [];
  for (const sub of ['blobs', 'manifests']) {
    for (const f of walkFiles(path.join(root, sub))) {
      const rel = path.relative(root, f.full).split(path.sep).join('/');
      const parsed = parseObjectPath(rel);
      if (!parsed) continue;
      const entry = { path: rel, size: f.size };
      if (deep) entry.sha256 = (await hashFile(f.full)).sha256;
      out.push(entry);
    }
  }
  return out.sort((a, b) => (a.path < b.path ? -1 : 1));
}

/** Shape check for a registered manifest. Path/symlink safety is NOT judged here: that is the receiver's job (materialize.js). */
function parseManifestDoc(bytes) {
  let doc;
  try {
    doc = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw new StoreError(400, 'manifest is not JSON');
  }
  if (!doc || !isSha256(doc.manifestHash) || !Array.isArray(doc.entries)) throw new StoreError(400, 'manifest needs {manifestHash, entries}');
  for (const e of doc.entries) {
    if (!e || !isCanonicalRelPath(e.path)) throw new StoreError(400, `manifest entry has a non-canonical path: ${e && e.path}`);
    if (e.type === 'file') {
      if (!isSha256(e.sha256) || !Number.isInteger(e.size) || e.size < 0) throw new StoreError(400, `bad file entry: ${e.path}`);
    } else if (e.type !== 'symlink' || typeof e.target !== 'string') {
      throw new StoreError(400, `bad entry type: ${e.path}`);
    }
  }
  if (manifestHashOf(doc.entries) !== doc.manifestHash) throw new StoreError(422, 'manifestHash does not match entries');
  return doc;
}

/**
 * The on-disk store: content-addressed blobs (`blobs/ab/cd/<sha256>`), immutable job manifests
 * (`manifests/<job>.json`, journaled and replicated) and mutable per-job state (`meta/<job>.json`: pin, terminal
 * mark; not journaled). Every committed blob/manifest is appended to the journal after it is durable.
 */
export class ObjectStore {
  constructor(root, { now = Date.now, maxBlobBytes = DEFAULT_MAX_BLOB_BYTES, capBytes = Infinity } = {}) {
    this.root = path.resolve(root);
    this.now = now;
    this.maxBlobBytes = maxBlobBytes;
    this.capBytes = capBytes;
    fs.mkdirSync(path.join(this.root, 'tmp'), { recursive: true });
    this.journal = new Journal(this.root);
    this.bytes = walkFiles(path.join(this.root, 'blobs')).reduce((n, f) => n + f.size, 0);
    this.jobBlobCache = new Map();
    for (const f of walkFiles(path.join(this.root, 'tmp'))) fs.unlinkSync(f.full); // leftovers of a crash mid-upload
  }

  close() {
    this.journal.close();
  }

  abs(rel) {
    return path.join(this.root, ...rel.split('/'));
  }

  hasBlob(sha) {
    return fs.existsSync(this.abs(blobRelPath(sha)));
  }

  blobSize(sha) {
    try {
      return fs.statSync(this.abs(blobRelPath(sha))).size;
    } catch {
      return null;
    }
  }

  /** True when a NEW blob of `incoming` bytes may be accepted: spec 8.2 refuses submits at 95% of the cap. */
  hasRoom(incoming) {
    return this.bytes + incoming < this.capBytes * 0.95;
  }

  /**
   * Whole-file immutable upload: stream into tmp/, hash while writing, refuse a mismatch or oversize, fsync, then
   * rename into place and journal. A blob that already exists is verified against the same bytes and deduplicated
   * (its mtime is refreshed, restarting the unreferenced-blob grace window). Returns `{stored}`.
   */
  async putBlob(sha, readable, declaredSize) {
    if (!isSha256(sha)) throw new StoreError(400, 'bad sha256');
    if (declaredSize > this.maxBlobBytes) throw new StoreError(413, `blob exceeds the ${this.maxBlobBytes}-byte cap`);
    const final = this.abs(blobRelPath(sha));
    if (!fs.existsSync(final) && !this.hasRoom(declaredSize)) throw new StoreError(507, 'store is above 95% of its cap');
    const tmp = path.join(this.root, 'tmp', `${sha}.${crypto.randomBytes(6).toString('hex')}`);
    const fh = await fs.promises.open(tmp, 'wx', 0o640);
    const hash = crypto.createHash('sha256');
    let size = 0;
    try {
      for await (const chunk of readable) {
        size += chunk.length;
        if (size > this.maxBlobBytes) throw new StoreError(413, `blob exceeds the ${this.maxBlobBytes}-byte cap`);
        hash.update(chunk);
        await fh.write(chunk);
      }
      await fh.sync();
    } catch (err) {
      await fh.close();
      fs.rmSync(tmp, { force: true });
      throw err;
    }
    await fh.close();
    if (hash.digest('hex') !== sha) {
      fs.rmSync(tmp, { force: true });
      throw new StoreError(422, 'sha256 of the body does not match the address');
    }
    if (fs.existsSync(final)) {
      fs.rmSync(tmp, { force: true });
      const t = new Date(this.now());
      fs.utimesSync(final, t, t);
      return { stored: false };
    }
    fs.mkdirSync(path.dirname(final), { recursive: true });
    fs.renameSync(tmp, final);
    const stamped = new Date(this.now());
    fs.utimesSync(final, stamped, stamped); // retention reads mtime as "last uploaded", on the store's clock
    fsyncDirectory(path.dirname(final));
    this.bytes += size;
    this.journal.append({ kind: 'blob', path: blobRelPath(sha), sha256: sha, size, createdAt: this.now() });
    return { stored: true };
  }

  readManifest(job) {
    if (!isJobId(job)) return null;
    try {
      return JSON.parse(fs.readFileSync(this.abs(manifestRelPath(job)), 'utf8'));
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  /** Register `bytes` (a `{manifestHash, entries}` document) as the immutable manifest of `job`. */
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
    const rel = manifestRelPath(job);
    atomicWriteFile(this.abs(rel), bytes, { fsync: true });
    atomicWriteJson(this.abs(`meta/${job}.json`), { registeredAt: this.now(), terminalAt: null, pinned: false }, { fsync: true });
    this.journal.append({ kind: 'manifest', path: rel, sha256: crypto.createHash('sha256').update(bytes).digest('hex'), size: bytes.length, createdAt: this.now() });
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
    return readJsonSafe(this.abs(`meta/${job}.json`)) ?? { registeredAt: null, terminalAt: null, pinned: false };
  }

  updateMeta(job, patch) {
    if (!this.readManifest(job)) throw new StoreError(404, 'no such job');
    atomicWriteJson(this.abs(`meta/${job}.json`), { ...this.meta(job), ...patch }, { fsync: true });
  }

  /** Re-read an object from disk and hash it: what a replica reports so the replicator can compare against the journal. */
  async verifyObject(rel) {
    if (!parseObjectPath(rel)) throw new StoreError(400, 'not an object path');
    try {
      return await hashFile(this.abs(rel));
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  /** Append journal records for committed objects that have none (recovery from a crash between commit and append, or journal loss). */
  async rejournal(paths) {
    const added = [];
    for (const rel of paths) {
      const v = await this.verifyObject(rel);
      if (!v) continue;
      added.push(this.journal.append({ kind: parseObjectPath(rel).kind, path: rel, sha256: v.sha256, size: v.size, createdAt: this.now() }));
    }
    return added;
  }
}
