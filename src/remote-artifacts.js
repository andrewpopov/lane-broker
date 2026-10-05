import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { atomicWriteJson, atomicWriteFile, readJsonSafe } from './state.js';
import { isCanonicalRelPath, scrubbedGitEnv } from './remote-manifest.js';
import { literalDirPrefix } from './config.js';
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
// The stream header lists at most `maxCount` (default 200) short entries.
const MAX_STREAM_HEADER_BYTES = 256 * 1024;
export const ARTIFACT_STREAM_PROTOCOL = 1;

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

/** A reason a whole artifact set is refused; anything else thrown is an unexpected error. */
class ArtifactRefusal extends Error {}

const refusal = (reason) => new ArtifactRefusal(reason);

/**
 * Paths an artifact may never be written to or collected from, whatever the lane declares: any segment that is, or
 * starts with, `.git` (a linked worktree's `.git` FILE, `.github`, `.githooks`, `.gitmodules`...), `.env*`, `.yarnrc*`,
 * `.npmrc`, `.lane-broker.json`, `node_modules`, `package.json`, `package-lock.json`, `npm-shrinkwrap.json` and
 * `*.lock`. Compared case-insensitively, since the submitter's filesystem usually is.
 */
export function isDeniedArtifactPath(relPath) {
  return relPath.split('/').some((segment) => {
    const seg = segment.toLowerCase();
    return (
      seg.startsWith('.git') ||
      seg.startsWith('.env') ||
      seg.startsWith('.yarnrc') ||
      seg.endsWith('.lock') ||
      ['.npmrc', '.lane-broker.json', 'node_modules', 'package.json', 'package-lock.json', 'npm-shrinkwrap.json'].includes(seg)
    );
  });
}

/** True iff `relPath` sits under the glob-free directory of a declared pattern it matches. */
function isUnderDeclaredDir(relPath, patterns) {
  return patterns.some((pattern) => {
    const dir = literalDirPrefix(pattern);
    return dir !== '' && relPath.startsWith(`${dir}/`) && artifactMatches(pattern, relPath);
  });
}

// A glob over a huge tree with few matches still costs a walk; bound the walk itself, not just the match count.
const MAX_SCANNED_ENTRIES = 100_000;

/** Visit every file path a glob could name, from its literal directory prefix, never descending into a symlink. */
function walkGlob(workDir, pattern, visit) {
  let scanned = 0;
  const stack = [literalDirPrefix(pattern)];
  while (stack.length > 0) {
    const rel = stack.pop();
    // opendirSync iterates one entry at a time, so a pathologically wide directory hits the scan cap
    // instead of being listed into memory in full first (readdirSync would allocate the whole listing).
    let dir;
    try {
      dir = fs.opendirSync(path.join(workDir, rel));
    } catch {
      continue;
    }
    try {
      for (let entry = dir.readSync(); entry !== null; entry = dir.readSync()) {
        scanned += 1;
        if (scanned > MAX_SCANNED_ENTRIES) throw refusal(`more than ${MAX_SCANNED_ENTRIES} entries scanned for ${pattern}`);
        const child = rel ? `${rel}/${entry.name}` : entry.name;
        if (entry.isDirectory()) {
          if (child !== '.git') stack.push(child);
        } else if (artifactMatches(pattern, child)) {
          visit(child);
        }
      }
    } finally {
      dir.closeSync();
    }
  }
}

/** Read at most `limit` bytes from `fd`; null if the file has more. Never trusts a size reported earlier. */
function readCapped(fd, limit) {
  const chunk = Buffer.allocUnsafe(64 * 1024);
  const parts = [];
  let total = 0;
  for (;;) {
    const n = fs.readSync(fd, chunk, 0, chunk.length, null);
    if (n === 0) return Buffer.concat(parts);
    total += n;
    if (total > limit) return null;
    parts.push(Buffer.from(chunk.subarray(0, n)));
  }
}

function quietRm(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    // best-effort
  }
}

/**
 * Runner side. Copy the regular files `patterns` name under `workDir` into `destDir` (`files/<rel>` plus
 * `manifest.json`), refusing the WHOLE set on any violation: a denied path, a symlink, a non-regular file, a path that
 * resolves outside `workDir`, or a cap (`maxFileBytes`, `maxTotalBytes`, `maxCount`). The walk stops at the count cap;
 * bytes are read through an `O_NOFOLLOW` fd and limited to the cap, never trusting an earlier `lstat` size. Zero files
 * creates nothing. Never throws. Returns `{ok:true, count, bytes, missing}` or `{ok:false, reason}` (a refusal leaves
 * no `destDir`).
 */
export function collectArtifacts(workDir, patterns, destDir, { maxFileBytes, maxTotalBytes, maxCount }) {
  try {
    fs.rmSync(destDir, { recursive: true, force: true });
    const realWork = fs.realpathSync(workDir);
    const candidates = new Set();
    const add = (rel) => {
      candidates.add(rel);
      if (candidates.size > maxCount) throw refusal(`more than ${maxCount} files match (count cap)`);
    };
    for (const pattern of patterns) {
      if (isGlobPattern(pattern)) walkGlob(workDir, pattern, add);
      else add(pattern);
    }

    const files = [];
    const missing = [];
    let total = 0;
    for (const rel of [...candidates].sort()) {
      if (isDeniedArtifactPath(rel)) throw refusal(`refusing a protected path: ${rel}`);
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
      if (st.isSymbolicLink()) throw refusal(`refusing a symlink: ${rel}`);
      if (!st.isFile()) throw refusal(`not a regular file: ${rel}`);
      if (!isInside(realWork, fs.realpathSync(abs))) throw refusal(`resolves outside the work dir: ${rel}`);
      if (!isCanonicalRelPath(rel)) throw refusal(`invalid path: ${rel}`);
      const fd = fs.openSync(abs, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      let buf;
      const room = Math.min(maxFileBytes, maxTotalBytes - total);
      try {
        if (!fs.fstatSync(fd).isFile()) throw refusal(`not a regular file: ${rel}`);
        buf = readCapped(fd, room);
      } finally {
        fs.closeSync(fd);
      }
      if (buf === null) {
        throw refusal(maxFileBytes <= maxTotalBytes - total ? `${rel} is over the per-file cap of ${maxFileBytes} bytes` : `artifacts exceed the total cap of ${maxTotalBytes} bytes`);
      }
      total += buf.length;
      atomicWriteFile(path.join(destDir, 'files', rel), buf);
      files.push({ path: rel, size: buf.length, sha256: sha256Of(buf) });
    }
    if (files.length > 0) atomicWriteJson(path.join(destDir, 'manifest.json'), { files });
    return { ok: true, count: files.length, bytes: total, missing };
  } catch (err) {
    quietRm(destDir);
    return { ok: false, reason: err instanceof ArtifactRefusal ? err.message : `could not collect artifacts: ${err.message}` };
  }
}

/** Runner side: the framed stream `lane remote-artifacts` prints, read back from what `collectArtifacts` stored. */
export function encodeArtifacts(artifactsDir, ticketId) {
  const manifest = readJsonSafe(path.join(artifactsDir, 'manifest.json'));
  if (!manifest || !Array.isArray(manifest.files)) return null;
  const filesDir = path.join(artifactsDir, 'files');
  async function* generate() {
    yield Buffer.from(`${JSON.stringify({ protocol: ARTIFACT_STREAM_PROTOCOL, ticketId, files: manifest.files })}\n`, 'utf8');
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
    const headerResult = await readHeaderLine(reader, MAX_STREAM_HEADER_BYTES);
    if (!headerResult.ok) return headerResult;
    const { header } = headerResult;
    if (header?.protocol !== ARTIFACT_STREAM_PROTOCOL) return { ok: false, reason: `unsupported artifact stream protocol: ${JSON.stringify(header?.protocol)}` };
    if (!header || header.ticketId !== ticketId || !Array.isArray(header.files)) return { ok: false, reason: 'artifact stream does not bind to this ticket' };
    if (header.files.length > limits.maxCount) return { ok: false, reason: `${header.files.length} artifacts, over the count cap of ${limits.maxCount}` };
    const seen = new Set();
    let total = 0;
    for (const entry of header.files) {
      const p = entry?.path;
      if (typeof p !== 'string' || !isCanonicalRelPath(p)) return { ok: false, reason: `invalid artifact path: ${JSON.stringify(p)}` };
      if (isDeniedArtifactPath(p)) return { ok: false, reason: `protected artifact path: ${p}` };
      if (!isUnderDeclaredDir(p, patterns)) return { ok: false, reason: `artifact was not declared: ${p}` };
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

/** Why `relPath` cannot be written under `realRoot` (an ancestor that is a symlink or not a directory), or null. */
function unsafeAncestor(realRoot, relPath) {
  let acc = realRoot;
  for (const seg of relPath.split('/').slice(0, -1)) {
    acc = path.join(acc, seg);
    const st = fs.lstatSync(acc, { throwIfNoEntry: false });
    if (!st) return null; // nothing deeper exists yet either
    if (st.isSymbolicLink()) return `an ancestor directory is a symlink: ${path.relative(realRoot, acc)}`;
    if (!st.isDirectory()) return `an ancestor is not a directory: ${path.relative(realRoot, acc)}`;
  }
  return null;
}

/** `relPath` spelled as the filesystem spells it (a case-insensitive volume would otherwise hide a tracked file). */
function canonicalRel(realRoot, relPath) {
  const segs = relPath.split('/');
  let existing = path.join(realRoot, ...segs);
  const rest = [];
  while (!fs.existsSync(existing) && existing !== realRoot) {
    rest.unshift(path.basename(existing));
    existing = path.dirname(existing);
  }
  return path.relative(realRoot, path.join(fs.realpathSync.native(existing), ...rest)).split(path.sep).join('/');
}

/**
 * Write `bytes` at `relPath` under `realRoot`: a temp file in the destination directory, then a re-check that no
 * ancestor is a symlink and the directory still resolves inside the root, then a rename. Residual race, accepted: a
 * process on THIS machine that swaps a directory for a symlink between that re-check and the rename. The runner is
 * the adversary this guards against, and it cannot touch this filesystem; a local concurrent mutator can already write
 * the worktree directly.
 */
function writeContained(realRoot, relPath, bytes) {
  const dest = path.join(realRoot, relPath);
  const dir = path.dirname(dest);
  const check = () => {
    const reason = unsafeAncestor(realRoot, relPath);
    if (reason) throw new Error(reason);
    if (!isInside(realRoot, fs.realpathSync(dir))) throw new Error('destination resolves outside the worktree');
    const st = fs.lstatSync(dest, { throwIfNoEntry: false });
    if (st && !st.isFile()) throw new Error(st.isSymbolicLink() ? 'destination is a symlink' : 'destination is not a regular file');
  };
  const reason = unsafeAncestor(realRoot, relPath);
  if (reason) throw new Error(reason);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, `.${path.basename(dest)}.${process.pid}-${crypto.randomBytes(6).toString('hex')}.tmp`);
  const fd = fs.openSync(tmp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o644);
  try {
    try {
      fs.writeFileSync(fd, bytes);
    } finally {
      fs.closeSync(fd);
    }
    check();
    fs.renameSync(tmp, dest);
  } catch (err) {
    fs.rmSync(tmp, { force: true });
    throw err;
  }
}

/**
 * Submitter side: write verified `files` into `worktreeRoot` at the same relative paths, atomically, one file at a
 * time. The runner is less trusted than this machine, so per file it refuses: a non-canonical path; a protected path
 * (`isDeniedArtifactPath`); a path outside the glob-free directory of a declared pattern it matches; any ancestor
 * directory that is a symlink; a destination that is not a regular file; and ANY file git tracks (checked on the
 * canonical spelling too) -- an artifact never overwrites tracked content, however it is declared. If git cannot be
 * asked, everything is refused. Never throws. Returns `{written:[path], refused:[{path, reason}]}`.
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

  const accepted = [];
  for (const file of files) {
    const refuse = (reason) => refused.push({ path: file.path, reason });
    try {
      if (!isCanonicalRelPath(file.path)) refuse('invalid path');
      else if (isDeniedArtifactPath(file.path)) refuse('protected path');
      else if (!isUnderDeclaredDir(file.path, patterns)) refuse('not under the literal directory of a declared pattern');
      else {
        const ancestor = unsafeAncestor(realRoot, file.path);
        const st = fs.lstatSync(path.join(realRoot, file.path), { throwIfNoEntry: false });
        if (ancestor) refuse(ancestor);
        else if (st && !st.isFile()) refuse(st.isSymbolicLink() ? 'destination is a symlink' : 'destination is not a regular file');
        else {
          const canonical = canonicalRel(realRoot, file.path);
          if (isDeniedArtifactPath(canonical)) refuse('protected path');
          else accepted.push({ file, canonical });
        }
      }
    } catch (err) {
      refuse(`could not check destination: ${err.message}`);
    }
  }

  let tracked = new Set();
  if (accepted.length > 0) {
    try {
      tracked = trackedAmong(realRoot, [...new Set(accepted.flatMap(({ file, canonical }) => [file.path, canonical]))]);
    } catch (err) {
      for (const { file } of accepted) refused.push({ path: file.path, reason: `could not check whether git tracks it: ${err.message}` });
      return { written, refused };
    }
  }
  for (const { file, canonical } of accepted) {
    if (tracked.has(file.path) || tracked.has(canonical)) {
      refused.push({ path: file.path, reason: 'git tracks this file; artifacts never overwrite tracked content' });
      continue;
    }
    try {
      writeContained(realRoot, file.path, file.bytes);
      written.push(file.path);
    } catch (err) {
      refused.push({ path: file.path, reason: err.message });
    }
  }
  return { written, refused };
}
