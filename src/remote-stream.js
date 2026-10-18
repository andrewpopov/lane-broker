import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { Readable } from 'node:stream';
import { manifestHashOf, sortEntries, verifyManifestNoGit, isCanonicalRelPath } from './remote-manifest.js';
import { MIN_CACHEABLE_BYTES, isBlobHash, materializeBlob, publishBlob } from './remote-blobs.js';

const NEWLINE = 0x0a;

/**
 * Upper bound on the snapshot HEADER line (protocol + ticket fields +
 * the full sorted manifest, one JSON line -- see `serializeHeader`).
 * BRAIN-320 (follow-up): a large repo's manifest alone can exceed the old
 * 1 MB frame-header-sized cap well before hitting any real file-count
 * limit (rouge's 8,788-file manifest serializes to ~1.6 MB), so the
 * snapshot header gets its own, much larger budget. This bounds ONLY the
 * snapshot header line read by `lane remote-exec` (`remote-runner.js`) and
 * the client-side precheck in `dispatchRemote` (`remote-client.js`) -- it
 * does not change the per-FRAME header default (`extractFrames`/
 * `extractSnapshot`'s own `maxLineBytes`), which bounds one manifest
 * entry's frame line, not the whole manifest.
 */
export const MAX_HEADER_BYTES = 16 * 1024 * 1024;

/**
 * Reject a `relPath` whose ancestor directories are no longer plain
 * directories by the time we're about to open it -- lstat each ancestor
 * component under `worktreeRoot` rather than trusting the manifest's earlier
 * snapshot of the tree shape. Paired with `O_NOFOLLOW` on the leaf open
 * itself (below), this closes the TOCTOU where a manifest entry's own
 * ancestor is swapped for a symlink between `buildManifest` and encode time.
 */
function assertAncestorsUnchanged(worktreeRoot, relPath) {
  const segs = relPath.split('/');
  let acc = worktreeRoot;
  for (let i = 0; i < segs.length - 1; i += 1) {
    acc = path.join(acc, segs[i]);
    let st;
    try {
      st = fs.lstatSync(acc);
    } catch (err) {
      throw new Error(`ancestor missing since manifest was built: ${relPath}: ${err.message}`);
    }
    if (!st.isDirectory()) {
      throw new Error(`ancestor is no longer a directory since manifest was built: ${relPath}`);
    }
  }
}

/**
 * Read one manifest-listed file's bytes at encode time, re-verifying it
 * against the manifest entry it claims to be BEFORE the bytes are ever
 * yielded onto the wire (BRAIN-319 review finding #2): open with
 * `O_NOFOLLOW` (refusing a leaf swapped for a symlink), require the open fd
 * to fstat as a regular file, then check both `size` and `sha256` against
 * the entry -- a same-length content swap passed only a length check before.
 * Any mismatch throws, which the generator surfaces as a stream 'error'
 * (never a partial/wrong body reaching ssh).
 */
export function readVerifiedFile(worktreeRoot, entry) {
  assertAncestorsUnchanged(worktreeRoot, entry.path);
  const absPath = path.join(worktreeRoot, entry.path);
  let fd;
  try {
    fd = fs.openSync(absPath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  } catch (err) {
    throw new Error(`open failed for encode: ${entry.path}: ${err.message}`);
  }
  let buf;
  try {
    const st = fs.fstatSync(fd);
    if (!st.isFile()) throw new Error(`not a regular file: ${entry.path}`);
    buf = fs.readFileSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  if (buf.length !== entry.size) {
    throw new Error(`file changed size on disk since manifest was built: ${entry.path}`);
  }
  const sha256 = crypto.createHash('sha256').update(buf).digest('hex');
  if (sha256 !== entry.sha256) {
    throw new Error(`file changed content on disk since manifest was built: ${entry.path}`);
  }
  return buf;
}

/**
 * Serialize the snapshot HEADER line exactly as `encodeSnapshot` emits it as
 * its first yielded chunk -- factored out so the client-side oversized-
 * header precheck in `dispatchRemote` (remote-client.js) computes the same
 * bytes `encodeSnapshot` will actually send, rather than a second
 * hand-rolled serialization that could silently disagree with it. `entries`
 * is sorted here (matching `encodeSnapshot`'s own `sortEntries` call); pass
 * a precomputed `manifestHash` to avoid rehashing when the caller already
 * has one.
 */
export function serializeHeader(header, entries, manifestHash) {
  const ordered = sortEntries(entries);
  const hash = manifestHash ?? manifestHashOf(entries);
  return `${JSON.stringify({ protocol: 1, ...header, manifest: { entries: ordered, manifestHash: hash } })}\n`;
}

/**
 * Encode a worktree snapshot as a framed byte stream: one header line, then
 * per entry a frame-header line followed by exactly `size` raw bytes for a
 * file (no body for a symlink), then a `{end:true}` terminator line. Reads
 * file bytes off disk at encode time (re-verified against the manifest via
 * `readVerifiedFile`), in path-sorted order. `readFile(entry)` replaces that
 * disk read when the bytes come from somewhere else (lane-store's blobs).
 *
 * BRAIN-642: a path in `cached` (the runner's have-answer) travels as a body-less `body:'cached'` frame. The file is STILL read and
 * verified here (`readFile`), so one mutated since the manifest was built fails the encode exactly as an uncached one would.
 */
export function encodeSnapshot(worktreeRoot, header, entries, readFile = (entry) => readVerifiedFile(worktreeRoot, entry), { cached = new Set() } = {}) {
  const manifestHash = manifestHashOf(entries);
  const ordered = sortEntries(entries);
  const headerLine = serializeHeader(header, entries, manifestHash);

  async function* generate() {
    yield Buffer.from(headerLine, 'utf8');
    for (const entry of ordered) {
      if (entry.type === 'file') {
        const isCached = cached.has(entry.path);
        yield Buffer.from(`${JSON.stringify({ path: entry.path, type: 'file', exec: !!entry.exec, size: entry.size, ...(isCached ? { body: 'cached' } : {}) })}\n`, 'utf8');
        const bytes = await readFile(entry);
        if (!isCached) yield bytes;
      } else {
        yield Buffer.from(`${JSON.stringify({ path: entry.path, type: 'symlink', target: entry.target })}\n`, 'utf8');
      }
    }
    yield Buffer.from(`${JSON.stringify({ end: true })}\n`, 'utf8');
  }

  return Readable.from(generate());
}

/**
 * Buffered line/byte reader over any async-iterable of Buffer/string chunks. Chunks are queued, never re-concatenated per
 * arrival: a body delivered in many small chunks (a decompressor's output) costs one copy at the end, and a line costs one
 * scan of each new chunk.
 */
export function makeReader(readable) {
  const queue = [];
  let head = 0; // index of the first unconsumed chunk; shifting the array per chunk would be quadratic over many tiny chunks
  let queued = 0;
  let newline = -1; // offset of the first newline in the queue, -1 when none is known
  let done = false;
  const it = readable[Symbol.asyncIterator]();

  function rescan() {
    newline = -1;
    let pos = 0;
    for (let i = head; i < queue.length; i++) {
      const chunk = queue[i];
      const idx = chunk.indexOf(NEWLINE);
      if (idx !== -1) {
        newline = pos + idx;
        return;
      }
      pos += chunk.length;
    }
  }

  async function fill(scan) {
    if (done) return false;
    const { value, done: iterDone } = await it.next();
    if (iterDone) {
      done = true;
      return false;
    }
    const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
    if (scan && newline === -1) {
      const idx = chunk.indexOf(NEWLINE);
      if (idx !== -1) newline = queued + idx;
    }
    queue.push(chunk);
    queued += chunk.length;
    return true;
  }

  function take(n) {
    const parts = [];
    let need = n;
    while (need > 0) {
      const first = queue[head];
      if (first.length <= need) {
        parts.push(first);
        queue[head] = undefined;
        head += 1;
        need -= first.length;
      } else {
        parts.push(first.subarray(0, need));
        queue[head] = first.subarray(need);
        need = 0;
      }
    }
    if (head > 1024 && head * 2 > queue.length) {
      queue.splice(0, head);
      head = 0;
    }
    queued -= n;
    rescan();
    return parts.length === 1 ? Buffer.from(parts[0]) : Buffer.concat(parts, n);
  }

  async function readLine(maxLen) {
    for (;;) {
      if (newline !== -1) {
        if (newline > maxLen) throw new Error('line exceeds max length');
        const line = take(newline).toString('utf8');
        take(1);
        return line;
      }
      if (queued > maxLen) throw new Error('line exceeds max length');
      if (!(await fill(true))) return null;
    }
  }

  async function readBytes(n) {
    while (queued < n) {
      if (!(await fill(false))) return null;
    }
    return take(n);
  }

  async function hasMore() {
    if (queued > 0) return true;
    return fill(false);
  }

  return { readLine, readBytes, hasMore };
}

function validateFrame(frame, expectedByPath, seen, symlinkPaths, maxFileBytes, remainingBudget, cacheEnabled) {
  if (!frame || typeof frame.path !== 'string') return { ok: false, reason: 'malformed frame' };
  const p = frame.path;
  if (path.isAbsolute(p)) return { ok: false, reason: `absolute path: ${p}` };
  if (!isCanonicalRelPath(p)) return { ok: false, reason: `invalid path: ${p}` };
  const segs = p.split('/');

  const entry = expectedByPath.get(p);
  if (!entry) return { ok: false, reason: `unlisted path: ${p}` };
  if (seen.has(p)) return { ok: false, reason: `duplicate path: ${p}` };

  let prefix = '';
  for (const part of segs.slice(0, -1)) {
    prefix = prefix ? `${prefix}/${part}` : part;
    if (symlinkPaths.has(prefix)) return { ok: false, reason: `ancestor is symlink: ${p}` };
  }

  if (frame.type !== entry.type) return { ok: false, reason: `type mismatch: ${p}` };
  if (entry.type === 'file') {
    if (!!frame.exec !== !!entry.exec) return { ok: false, reason: `exec mismatch: ${p}` };
    if (typeof frame.size !== 'number' || !Number.isInteger(frame.size) || frame.size < 0) {
      return { ok: false, reason: `invalid size: ${p}` };
    }
    if (frame.size !== entry.size) return { ok: false, reason: `size mismatch: ${p}` };
    if (frame.size > maxFileBytes) return { ok: false, reason: `oversize frame: ${p}` };
    if (frame.size > remainingBudget) return { ok: false, reason: `oversize total: ${p}` };
    if (frame.body === 'cached') {
      if (!cacheEnabled) return { ok: false, reason: `cached body not enabled: ${p}` };
      if (entry.size < MIN_CACHEABLE_BYTES) return { ok: false, reason: `cached body below minimum size: ${p}` };
      if (!isBlobHash(entry.sha256)) return { ok: false, reason: `cached body without a valid sha256: ${p}` };
    } else if (cacheEnabled && frame.body !== undefined) {
      return { ok: false, reason: `unknown body encoding: ${p}` };
    }
  } else if (frame.target !== entry.target) {
    return { ok: false, reason: `target mismatch: ${p}` };
  }
  return { ok: true };
}

/**
 * Same chain-aware walk as `checkSymlinkEscape` in remote-manifest.js (see
 * its comment for why a naive `path.normalize` is unsafe): step through
 * `target` one component at a time against `relPath`'s directory, rejecting
 * both an out-of-root escape and a walk that passes through another manifest
 * symlink entry. `symlinkPaths` is the manifest's full symlink-path set.
 */
function checkExtractSymlinkTarget(relPath, target, symlinkPaths) {
  if (typeof target !== 'string' || path.isAbsolute(target)) {
    return { ok: false, reason: `symlink target absolute: ${relPath}` };
  }

  const dirname = path.posix.dirname(relPath);
  const dirSegs = dirname === '.' ? [] : dirname.split('/');

  let acc = '';
  for (const seg of dirSegs) {
    acc = acc ? `${acc}/${seg}` : seg;
    if (symlinkPaths.has(acc)) return { ok: false, reason: `symlink target escapes (ancestor is a symlink): ${relPath}` };
  }

  const stack = [...dirSegs];
  for (const part of target.split('/')) {
    if (part === '' || part === '.') continue;
    if (part === '..') {
      if (stack.length === 0) return { ok: false, reason: `symlink target escapes: ${relPath}` };
      stack.pop();
      continue;
    }
    stack.push(part);
    const cur = stack.join('/');
    if (symlinkPaths.has(cur) && cur !== relPath) {
      return { ok: false, reason: `symlink target escapes (passes through a symlink entry): ${relPath}` };
    }
  }
  return { ok: true };
}

/**
 * Read and JSON-parse the header line off an already-constructed `reader`
 * (from `makeReader`), without consuming anything past it. Returns
 * `{ok:true, header}` or `{ok:false, reason}` -- shared by `extractSnapshot`
 * (which reads the header for you) and callers such as `lane remote-exec`
 * that must validate header fields (ticketId, generation, repoKey, ...)
 * themselves before any extraction happens.
 */
export async function readHeaderLine(reader, maxLineBytes = MAX_HEADER_BYTES) {
  let headerLine;
  try {
    headerLine = await reader.readLine(maxLineBytes);
  } catch (err) {
    return { ok: false, reason: `header line: ${err.message}` };
  }
  if (headerLine === null) return { ok: false, reason: 'truncated stream: missing header' };
  let header;
  try {
    header = JSON.parse(headerLine);
  } catch {
    return { ok: false, reason: 'malformed json: header' };
  }
  return { ok: true, header };
}

/**
 * Extract the frame stream (everything after the header line) from an
 * already-constructed `reader` into a fresh `destDir` (refused if it already
 * exists), validating every frame against `expectedManifest` BEFORE writing
 * anything, then re-verifying the written tree with `verifyManifestNoGit`.
 * Returns `{ok:true}` or `{ok:false, reason}` -- never throws for a
 * malformed/adversarial stream, and never writes outside `destDir`. Split out
 * of `extractSnapshot` so a caller that must inspect the header itself first
 * (e.g. `lane remote-exec`, which needs ticketId/repoKey/argv before it can
 * decide where `destDir` even is) can read the header with `readHeaderLine`
 * and then hand the same reader here.
 *
 * BRAIN-642: `limits.blobCache = {root, ns, stats}` switches on the runner's blob store: a `body:'cached'` frame (no body bytes,
 * only for a manifest file of at least MIN_CACHEABLE_BYTES) is copied out of the store using the MANIFEST entry's hash, and a
 * body frame is published to it. `stats` ({hits, misses, corrupt, published}) is filled in for the caller; `publishOptions` is a test seam. Without it a
 * `body:'cached'` frame is refused and nothing here changes.
 */
export async function extractFrames(reader, destDir, expectedManifest, limits = {}) {
  const maxFileBytes = limits.maxFileBytes ?? Infinity;
  const maxTotalBytes = limits.maxTotalBytes ?? Infinity;
  const maxLineBytes = limits.maxLineBytes ?? 1_000_000;

  try {
    fs.mkdirSync(destDir);
  } catch (err) {
    if (err.code === 'EEXIST') return { ok: false, reason: 'destDir already exists' };
    throw err;
  }

  const expectedByPath = new Map(expectedManifest.entries.map((e) => [e.path, e]));
  const symlinkPaths = new Set(
    expectedManifest.entries.filter((e) => e.type === 'symlink').map((e) => e.path),
  );
  const seen = new Set();
  let cumulativeBytes = 0;
  const blobCache = limits.blobCache;
  const stats = blobCache?.stats;
  const missing = [];
  const corrupt = [];

  for (;;) {
    let line;
    try {
      line = await reader.readLine(maxLineBytes);
    } catch (err) {
      return { ok: false, reason: `frame header: ${err.message}` };
    }
    if (line === null) return { ok: false, reason: 'truncated stream: missing terminator' };
    let frame;
    try {
      frame = JSON.parse(line);
    } catch {
      return { ok: false, reason: 'malformed json: frame' };
    }
    if (frame && frame.end === true) break;

    const check = validateFrame(frame, expectedByPath, seen, symlinkPaths, maxFileBytes, maxTotalBytes - cumulativeBytes, !!blobCache);
    if (!check.ok) return check;
    seen.add(frame.path);

    const fullPath = path.join(destDir, frame.path);
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });

    if (frame.type === 'file' && frame.body === 'cached') {
      const entry = expectedByPath.get(frame.path);
      // charged up front: a miss or corrupt blob still counts, so no mix of them can step past the total cap
      cumulativeBytes += frame.size;
      let result;
      try {
        result = materializeBlob(blobCache.root, blobCache.ns, entry, fullPath);
      } catch (err) {
        return { ok: false, reason: `open failed: ${frame.path}: ${err.message}` };
      }
      if (result.status === 'hit') {
        stats.hits += 1;
      } else if (result.status === 'corrupt') {
        stats.corrupt += 1;
        corrupt.push(frame.path);
      } else {
        stats.misses += 1;
        missing.push(frame.path);
      }
    } else if (frame.type === 'file') {
      let bytes;
      try {
        bytes = await reader.readBytes(frame.size);
      } catch (err) {
        return { ok: false, reason: `file body: ${err.message}` };
      }
      if (bytes === null) return { ok: false, reason: `truncated stream: body of ${frame.path}` };
      let fd;
      try {
        fd = fs.openSync(
          fullPath,
          fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW,
          0o644,
        );
      } catch (err) {
        return { ok: false, reason: `open failed: ${frame.path}: ${err.message}` };
      }
      try {
        fs.writeSync(fd, bytes);
      } finally {
        fs.closeSync(fd);
      }
      fs.chmodSync(fullPath, frame.exec ? 0o755 : 0o644);
      cumulativeBytes += frame.size;
      if (blobCache && frame.size >= MIN_CACHEABLE_BYTES && publishBlob(blobCache.root, blobCache.ns, expectedByPath.get(frame.path), bytes, blobCache.publishOptions) === 'published') {
        stats.published += 1;
      }
    } else {
      const escapeCheck = checkExtractSymlinkTarget(frame.path, frame.target, symlinkPaths);
      if (!escapeCheck.ok) return escapeCheck;
      try {
        fs.symlinkSync(frame.target, fullPath);
      } catch (err) {
        return { ok: false, reason: `symlink create failed: ${frame.path}: ${err.message}` };
      }
    }
  }

  if (seen.size !== expectedManifest.entries.length) {
    return { ok: false, reason: 'missing entry: stream ended before all manifest entries arrived' };
  }

  try {
    if (await reader.hasMore()) return { ok: false, reason: 'trailing bytes after terminator' };
  } catch (err) {
    return { ok: false, reason: `after terminator: ${err.message}` };
  }

  if (corrupt.length > 0) return { ok: false, reason: `cache-corrupt: ${corrupt.length}` };
  if (missing.length > 0) return { ok: false, reason: `cache-miss: ${missing.length}` };

  return verifyManifestNoGit(destDir, expectedManifest);
}

/**
 * Extract a framed snapshot stream into a fresh `destDir` (refused if it
 * already exists), validating every frame against `expectedManifest` BEFORE
 * writing anything, then re-verifying the written tree with
 * `verifyManifestNoGit`. Returns `{ok:true}` or `{ok:false, reason}` -- never
 * throws for a malformed/adversarial stream, and never writes outside
 * `destDir`. Reads and discards the header line itself, checking only that
 * its embedded manifest hash matches `expectedManifest.manifestHash`; a
 * caller that needs the rest of the header (ticketId, argv, ...) should use
 * `readHeaderLine` + `extractFrames` directly instead.
 */
export async function extractSnapshot(readable, destDir, expectedManifest, limits = {}) {
  const maxLineBytes = limits.maxLineBytes ?? 1_000_000;
  const reader = makeReader(readable);

  const headerResult = await readHeaderLine(reader, maxLineBytes);
  if (!headerResult.ok) return headerResult;
  const { header } = headerResult;
  if (header && header.manifest && header.manifest.manifestHash !== expectedManifest.manifestHash) {
    return { ok: false, reason: 'header manifest hash mismatch' };
  }

  return extractFrames(reader, destDir, expectedManifest, limits);
}
