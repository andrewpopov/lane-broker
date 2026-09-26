import fs from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { manifestHashOf, sortEntries, verifyManifestNoGit } from './remote-manifest.js';

const NEWLINE = 0x0a;

/**
 * Encode a worktree snapshot as a framed byte stream: one header line, then
 * per entry a frame-header line followed by exactly `size` raw bytes for a
 * file (no body for a symlink), then a `{end:true}` terminator line. Reads
 * file bytes off disk at encode time, in path-sorted order.
 */
export function encodeSnapshot(worktreeRoot, header, entries) {
  const manifestHash = manifestHashOf(entries);
  const ordered = sortEntries(entries);

  async function* generate() {
    yield Buffer.from(`${JSON.stringify({ protocol: 1, ...header, manifest: { entries: ordered, manifestHash } })}\n`, 'utf8');
    for (const entry of ordered) {
      if (entry.type === 'file') {
        yield Buffer.from(`${JSON.stringify({ path: entry.path, type: 'file', exec: !!entry.exec, size: entry.size })}\n`, 'utf8');
        const buf = fs.readFileSync(path.join(worktreeRoot, entry.path));
        if (buf.length !== entry.size) {
          throw new Error(`file changed size on disk since manifest was built: ${entry.path}`);
        }
        yield buf;
      } else {
        yield Buffer.from(`${JSON.stringify({ path: entry.path, type: 'symlink', target: entry.target })}\n`, 'utf8');
      }
    }
    yield Buffer.from(`${JSON.stringify({ end: true })}\n`, 'utf8');
  }

  return Readable.from(generate());
}

/** Buffered line/byte reader over any async-iterable of Buffer/string chunks. */
function makeReader(readable) {
  let buf = Buffer.alloc(0);
  let done = false;
  const it = readable[Symbol.asyncIterator]();

  async function fill() {
    if (done) return false;
    const { value, done: iterDone } = await it.next();
    if (iterDone) {
      done = true;
      return false;
    }
    buf = Buffer.concat([buf, Buffer.isBuffer(value) ? value : Buffer.from(value)]);
    return true;
  }

  async function readLine(maxLen) {
    for (;;) {
      const idx = buf.indexOf(NEWLINE);
      if (idx !== -1) {
        if (idx > maxLen) throw new Error('line exceeds max length');
        const line = buf.subarray(0, idx).toString('utf8');
        buf = buf.subarray(idx + 1);
        return line;
      }
      if (buf.length > maxLen) throw new Error('line exceeds max length');
      if (!(await fill())) return null;
    }
  }

  async function readBytes(n) {
    while (buf.length < n) {
      if (!(await fill())) return null;
    }
    const out = Buffer.from(buf.subarray(0, n));
    buf = buf.subarray(n);
    return out;
  }

  async function hasMore() {
    if (buf.length > 0) return true;
    return fill();
  }

  return { readLine, readBytes, hasMore };
}

function validateFrame(frame, expectedByPath, seen, symlinkPaths, maxFileBytes, remainingBudget) {
  if (!frame || typeof frame.path !== 'string') return { ok: false, reason: 'malformed frame' };
  const p = frame.path;
  if (path.isAbsolute(p)) return { ok: false, reason: `absolute path: ${p}` };
  const segs = p.split('/');
  if (segs.some((s) => s === '..' || s === '')) return { ok: false, reason: `invalid path: ${p}` };

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
  } else if (frame.target !== entry.target) {
    return { ok: false, reason: `target mismatch: ${p}` };
  }
  return { ok: true };
}

function checkExtractSymlinkTarget(destDir, relPath, target) {
  if (typeof target !== 'string' || path.isAbsolute(target)) {
    return { ok: false, reason: `symlink target absolute: ${relPath}` };
  }
  const dir = path.dirname(path.join(destDir, relPath));
  const resolved = path.normalize(path.join(dir, target));
  const rootNorm = path.normalize(destDir);
  if (resolved !== rootNorm && !resolved.startsWith(rootNorm + path.sep)) {
    return { ok: false, reason: `symlink target escapes: ${relPath}` };
  }
  return { ok: true };
}

/**
 * Extract a framed snapshot stream into a fresh `destDir` (refused if it
 * already exists), validating every frame against `expectedManifest` BEFORE
 * writing anything, then re-verifying the written tree with
 * `verifyManifestNoGit`. Returns `{ok:true}` or `{ok:false, reason}` -- never
 * throws for a malformed/adversarial stream, and never writes outside
 * `destDir`.
 */
export async function extractSnapshot(readable, destDir, expectedManifest, limits = {}) {
  const maxFileBytes = limits.maxFileBytes ?? Infinity;
  const maxTotalBytes = limits.maxTotalBytes ?? Infinity;
  const maxLineBytes = limits.maxLineBytes ?? 1_000_000;

  try {
    fs.mkdirSync(destDir);
  } catch (err) {
    if (err.code === 'EEXIST') return { ok: false, reason: 'destDir already exists' };
    throw err;
  }

  const reader = makeReader(readable);
  const expectedByPath = new Map(expectedManifest.entries.map((e) => [e.path, e]));
  const symlinkPaths = new Set(
    expectedManifest.entries.filter((e) => e.type === 'symlink').map((e) => e.path),
  );
  const seen = new Set();
  let cumulativeBytes = 0;

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
  if (header && header.manifest && header.manifest.manifestHash !== expectedManifest.manifestHash) {
    return { ok: false, reason: 'header manifest hash mismatch' };
  }

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

    const check = validateFrame(frame, expectedByPath, seen, symlinkPaths, maxFileBytes, maxTotalBytes - cumulativeBytes);
    if (!check.ok) return check;
    seen.add(frame.path);

    const fullPath = path.join(destDir, frame.path);
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });

    if (frame.type === 'file') {
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
    } else {
      const escapeCheck = checkExtractSymlinkTarget(destDir, frame.path, frame.target);
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

  if (await reader.hasMore()) return { ok: false, reason: 'trailing bytes after terminator' };

  return verifyManifestNoGit(destDir, expectedManifest);
}
