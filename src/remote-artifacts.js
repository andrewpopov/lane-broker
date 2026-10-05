import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { atomicWriteJson, atomicWriteFile, readJsonSafe } from './state.js';
import { isCanonicalRelPath, scrubbedGitEnv } from './remote-manifest.js';
import { readHeaderLine, readVerifiedFile } from './remote-stream.js';

/**
 * BRAIN-398: files a remote run returns to the submitter's worktree.
 *
 * Transport: the runner copies the matched files into `<ticketDir>/artifacts/files/` next to `result.json`, with a
 * `manifest.json` of path/size/sha256, BEFORE publishing the result (which carries only a summary). The submitter then
 * runs `lane remote-artifacts <ticketId>` over ssh (the same shape as `remote-result`), reads a framed stream
 * (header line, per file a frame line plus exactly `size` bytes, a `{end:true}` line -- the snapshot stream's own
 * framing and reader), verifies every sha256 and cap, and only then writes. A best-effort `remote-artifacts-release`
 * deletes the copies afterwards; `pruneStaleArtifacts` bounds what a lost release leaves behind.
 */

export const ARTIFACTS_CAPABILITY = 'artifacts/1';

const STALE_ARTIFACTS_MS = 24 * 60 * 60_000;
const MAX_FRAME_LINE_BYTES = 1_000_000;

/** The three caps, as named in a global config (`remoteArtifactMax*`). */
export const artifactLimitsOf = (cfg) => ({
  maxFileBytes: cfg.remoteArtifactMaxFileBytes,
  maxTotalBytes: cfg.remoteArtifactMaxTotalBytes,
  maxCount: cfg.remoteArtifactMaxCount,
});

export const isGlobPattern = (pattern) => pattern.includes('*');

function segmentRegex(seg) {
  return new RegExp(`^${seg.split('*').map((part) => part.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('[^/]*')}$`);
}

function matchSegments(patSegs, i, segs, j) {
  if (i === patSegs.length) return j === segs.length;
  if (patSegs[i] === '**') {
    for (let k = j; k <= segs.length; k += 1) if (matchSegments(patSegs, i + 1, segs, k)) return true;
    return false;
  }
  return j < segs.length && segmentRegex(patSegs[i]).test(segs[j]) && matchSegments(patSegs, i + 1, segs, j + 1);
}

/** `*` stays within one segment; `**` as a whole segment spans any number (including none). */
export function artifactMatches(pattern, relPath) {
  return matchSegments(pattern.split('/'), 0, relPath.split('/'), 0);
}

function isInside(root, candidate) {
  return candidate === root || candidate.startsWith(root + path.sep);
}

function sha256Of(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

/** Every file path a glob could name: walk from its literal directory prefix, never descending into a symlink. */
function walkGlob(workDir, pattern, visit) {
  const segs = pattern.split('/');
  const literal = [];
  while (literal.length < segs.length - 1 && !isGlobPattern(segs[literal.length])) literal.push(segs[literal.length]);
  const walk = (rel) => {
    let entries;
    try {
      entries = fs.readdirSync(path.join(workDir, rel), { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const child = rel ? `${rel}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        if (child !== '.git') walk(child);
      } else if (artifactMatches(pattern, child)) {
        visit(child);
      }
    }
  };
  walk(literal.join('/'));
}

/**
 * Runner side. Copy the regular files `patterns` name under `workDir` into `destDir` (`files/<rel>` plus
 * `manifest.json`), refusing the WHOLE set on any violation: a symlink, a non-regular file, a path that resolves
 * outside `workDir`, or a cap (`maxFileBytes`, `maxTotalBytes`, `maxCount`). Never throws. Returns
 * `{ok:true, count, bytes, missing}` or `{ok:false, reason}` (a refusal leaves no `destDir`).
 */
export function collectArtifacts(workDir, patterns, destDir, { maxFileBytes, maxTotalBytes, maxCount }) {
  fs.rmSync(destDir, { recursive: true, force: true });
  const refuse = (reason) => {
    fs.rmSync(destDir, { recursive: true, force: true });
    return { ok: false, reason };
  };
  try {
    const realWork = fs.realpathSync(workDir);
    const candidates = new Set();
    for (const pattern of patterns) {
      if (!isGlobPattern(pattern)) {
        candidates.add(pattern);
        continue;
      }
      walkGlob(workDir, pattern, (rel) => candidates.add(rel));
      if (candidates.size > maxCount) return refuse(`more than ${maxCount} files match (count cap)`);
    }
    if (candidates.size > maxCount) return refuse(`more than ${maxCount} files match (count cap)`);

    const files = [];
    const missing = [];
    let total = 0;
    for (const rel of [...candidates].sort()) {
      const abs = path.join(workDir, rel);
      let st;
      try {
        st = fs.lstatSync(abs);
      } catch (err) {
        if (err.code === 'ENOENT' || err.code === 'ENOTDIR') {
          missing.push(rel);
          continue;
        }
        throw err;
      }
      if (st.isSymbolicLink()) return refuse(`refusing a symlink: ${rel}`);
      if (!st.isFile()) return refuse(`not a regular file: ${rel}`);
      if (!isInside(realWork, fs.realpathSync(abs))) return refuse(`resolves outside the work dir: ${rel}`);
      if (st.size > maxFileBytes) return refuse(`${rel} is ${st.size} bytes, over the per-file cap of ${maxFileBytes}`);
      total += st.size;
      if (total > maxTotalBytes) return refuse(`artifacts exceed the total cap of ${maxTotalBytes} bytes`);
      const fd = fs.openSync(abs, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      let buf;
      try {
        if (!fs.fstatSync(fd).isFile()) return refuse(`not a regular file: ${rel}`);
        buf = fs.readFileSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      atomicWriteFile(path.join(destDir, 'files', rel), buf);
      files.push({ path: rel, size: buf.length, sha256: sha256Of(buf) });
    }
    atomicWriteJson(path.join(destDir, 'manifest.json'), { files });
    return { ok: true, count: files.length, bytes: total, missing };
  } catch (err) {
    return refuse(`could not collect artifacts: ${err.message}`);
  }
}

/** Runner side: the framed stream `lane remote-artifacts` prints, read back from what `collectArtifacts` stored. */
export function encodeArtifacts(artifactsDir, ticketId) {
  const manifest = readJsonSafe(path.join(artifactsDir, 'manifest.json'));
  if (!manifest || !Array.isArray(manifest.files)) return null;
  const filesDir = path.join(artifactsDir, 'files');
  async function* generate() {
    yield Buffer.from(`${JSON.stringify({ protocol: 1, ticketId, files: manifest.files })}\n`, 'utf8');
    for (const entry of manifest.files) {
      yield Buffer.from(`${JSON.stringify(entry)}\n`, 'utf8');
      yield readVerifiedFile(filesDir, entry);
    }
    yield Buffer.from(`${JSON.stringify({ end: true })}\n`, 'utf8');
  }
  return Readable.from(generate());
}

/** Runner side: drop stored artifacts older than a day (a submitter that never released them). Never throws. */
export function pruneStaleArtifacts(ticketsDir, now = Date.now()) {
  let names;
  try {
    names = fs.readdirSync(ticketsDir);
  } catch {
    return;
  }
  for (const name of names) {
    const dir = path.join(ticketsDir, name, 'artifacts');
    try {
      if (now - fs.statSync(dir).mtimeMs > STALE_ARTIFACTS_MS) fs.rmSync(dir, { recursive: true, force: true });
    } catch {
      // no artifacts for this ticket, or already gone
    }
  }
}

/**
 * Submitter side: read and verify the stream off `reader` (from `makeReader`) against the lane's declared `patterns`
 * and `limits`, entirely in memory -- nothing is written until everything checked out. Any bad path, undeclared path,
 * duplicate, cap breach, frame/header disagreement or sha256 mismatch refuses the whole stream.
 * Returns `{ok:true, files:[{path,size,sha256,bytes}]}` or `{ok:false, reason}`; never throws.
 */
export async function receiveArtifacts(reader, { ticketId, patterns, limits }) {
  try {
    const headerResult = await readHeaderLine(reader, MAX_FRAME_LINE_BYTES * 16);
    if (!headerResult.ok) return headerResult;
    const { header } = headerResult;
    if (!header || header.ticketId !== ticketId || !Array.isArray(header.files)) return { ok: false, reason: 'artifact stream does not bind to this ticket' };
    if (header.files.length > limits.maxCount) return { ok: false, reason: `${header.files.length} artifacts, over the count cap of ${limits.maxCount}` };
    const seen = new Set();
    let total = 0;
    for (const entry of header.files) {
      const p = entry?.path;
      if (typeof p !== 'string' || !isCanonicalRelPath(p)) return { ok: false, reason: `invalid artifact path: ${JSON.stringify(p)}` };
      if (!patterns.some((pattern) => artifactMatches(pattern, p))) return { ok: false, reason: `artifact was not declared: ${p}` };
      if (seen.has(p)) return { ok: false, reason: `duplicate artifact: ${p}` };
      seen.add(p);
      if (!Number.isInteger(entry.size) || entry.size < 0) return { ok: false, reason: `invalid artifact size: ${p}` };
      if (entry.size > limits.maxFileBytes) return { ok: false, reason: `${p} is ${entry.size} bytes, over the per-file cap of ${limits.maxFileBytes}` };
      total += entry.size;
      if (total > limits.maxTotalBytes) return { ok: false, reason: `artifacts exceed the total cap of ${limits.maxTotalBytes} bytes` };
      if (typeof entry.sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(entry.sha256)) return { ok: false, reason: `invalid artifact sha256: ${p}` };
    }

    const files = [];
    for (const entry of header.files) {
      const line = await reader.readLine(MAX_FRAME_LINE_BYTES);
      let frame;
      try {
        frame = JSON.parse(line);
      } catch {
        return { ok: false, reason: 'malformed artifact frame' };
      }
      if (frame?.path !== entry.path || frame.size !== entry.size || frame.sha256 !== entry.sha256) return { ok: false, reason: `artifact frame does not match its header entry: ${entry.path}` };
      const bytes = await reader.readBytes(entry.size);
      if (bytes === null) return { ok: false, reason: `truncated artifact stream: ${entry.path}` };
      if (sha256Of(bytes) !== entry.sha256) return { ok: false, reason: `sha256 mismatch: ${entry.path}` };
      files.push({ path: entry.path, size: entry.size, sha256: entry.sha256, bytes });
    }
    const end = await reader.readLine(MAX_FRAME_LINE_BYTES);
    if (end === null || JSON.parse(end)?.end !== true) return { ok: false, reason: 'artifact stream has no terminator' };
    if (await reader.hasMore()) return { ok: false, reason: 'trailing bytes after the artifact terminator' };
    return { ok: true, files };
  } catch (err) {
    return { ok: false, reason: `artifact stream: ${err.message}` };
  }
}

/** Paths git tracks in `worktreeRoot` among `relPaths` (literal pathspecs, so a `*` in a name is just a name). */
function trackedAmong(worktreeRoot, relPaths) {
  const out = execFileSync('git', ['--literal-pathspecs', 'ls-files', '-z', '--', ...relPaths], {
    cwd: worktreeRoot,
    env: scrubbedGitEnv(),
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
  });
  return new Set(out.split('\0').filter(Boolean));
}

/**
 * Submitter side: write verified `files` into `worktreeRoot` at the same relative paths, atomically, one file at a
 * time. Per file it refuses: a non-canonical path, a destination that resolves outside the worktree (realpath of its
 * deepest existing ancestor), a destination that is a symlink or not a regular file, and a git-tracked destination
 * unless the path is listed in `patterns` EXACTLY (a glob never overwrites a tracked file). Never throws.
 * Returns `{written:[path], refused:[{path, reason}]}`.
 */
export function installArtifacts(worktreeRoot, files, patterns) {
  const written = [];
  const refused = [];
  let realRoot;
  try {
    realRoot = fs.realpathSync(worktreeRoot);
  } catch (err) {
    return { written, refused: files.map((f) => ({ path: f.path, reason: `worktree unavailable: ${err.message}` })) };
  }
  const exact = new Set(patterns.filter((p) => !isGlobPattern(p)));
  const needsTrackedCheck = files.filter((f) => !exact.has(f.path)).map((f) => f.path);
  let tracked = new Set();
  let trackedError = null;
  if (needsTrackedCheck.length > 0) {
    try {
      tracked = trackedAmong(realRoot, needsTrackedCheck);
    } catch (err) {
      trackedError = err.message;
    }
  }

  for (const file of files) {
    const refuse = (reason) => refused.push({ path: file.path, reason });
    if (!isCanonicalRelPath(file.path)) {
      refuse('invalid path');
      continue;
    }
    if (!exact.has(file.path)) {
      if (trackedError) {
        refuse(`could not check whether git tracks it: ${trackedError}`);
        continue;
      }
      if (tracked.has(file.path)) {
        refuse('git tracks this file; only a path listed exactly in remoteArtifacts may overwrite it');
        continue;
      }
    }
    const dest = path.join(realRoot, file.path);
    let ancestor = path.dirname(dest);
    while (!fs.existsSync(ancestor) && ancestor !== realRoot) ancestor = path.dirname(ancestor);
    let outside = false;
    try {
      outside = !isInside(realRoot, fs.realpathSync(ancestor));
    } catch {
      outside = true;
    }
    if (outside) {
      refuse('destination resolves outside the worktree');
      continue;
    }
    try {
      const st = fs.lstatSync(dest, { throwIfNoEntry: false });
      if (st && !st.isFile()) {
        refuse(st.isSymbolicLink() ? 'destination is a symlink' : 'destination is not a regular file');
        continue;
      }
      atomicWriteFile(dest, file.bytes);
      written.push(file.path);
    } catch (err) {
      refuse(`write failed: ${err.message}`);
    }
  }
  return { written, refused };
}
