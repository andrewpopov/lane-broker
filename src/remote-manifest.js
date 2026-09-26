import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

/**
 * Thrown by `buildManifest` when a worktree cannot be safely snapshotted for
 * a remote runner. Callers treat this as "fall back to local" (BRAIN-319 I1),
 * never as a hard failure of the local run itself.
 */
export class RemoteIneligibleError extends Error {
  constructor(reason, entryPath, options) {
    super(entryPath ? `${reason}: ${entryPath}` : reason, options);
    this.name = 'RemoteIneligibleError';
    this.reason = reason;
    this.path = entryPath;
  }
}

/**
 * Git exports its repository-local env vars (GIT_DIR, GIT_INDEX_FILE,
 * GIT_WORK_TREE, ...) to every child process a hook spawns; leaking one of
 * them into `git ls-files` here would read the wrong repo. Same scrub the
 * committed `.githooks/pre-push` and `tests/helpers.js`'s `gitFixture` apply
 * before shelling out to git -- there is no src-side helper for this yet, so
 * this is the first one and later remote-* modules should reuse it.
 */
function scrubbedGitEnv() {
  const env = { ...process.env };
  let names = [];
  try {
    names = execFileSync('git', ['rev-parse', '--local-env-vars'], { encoding: 'utf8' })
      .split('\n')
      .map((s) => s.trim())
      .filter(Boolean);
  } catch {
    // best-effort: if git itself is unusable, gitLsFiles below will fail loudly next.
  }
  for (const name of [...names, 'GIT_QUARANTINE_PATH']) delete env[name];
  return env;
}

function comparePaths(a, b) {
  return Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));
}

function sortEntries(entries) {
  return [...entries].sort((a, b) => comparePaths(a.path, b.path));
}

function canonicalizeEntry(e) {
  return e.type === 'file'
    ? { path: e.path, type: 'file', exec: !!e.exec, size: e.size, sha256: e.sha256 }
    : { path: e.path, type: 'symlink', target: e.target };
}

/**
 * Canonical manifest hash: sha256 of `JSON.stringify` over the path-sorted,
 * field-order-canonicalized entries array. Documented here because both the
 * local manifest builder and the wire format (remote-stream.js) must derive
 * the exact same hash from the same entries.
 */
export function manifestHashOf(entries) {
  const canonical = sortEntries(entries).map(canonicalizeEntry);
  return crypto.createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
}

function isDenylistedPath(relPath) {
  const base = path.posix.basename(relPath);
  if (base === '.env.example') return false;
  if (base === '.env' || base.startsWith('.env.')) return true;
  if (base.endsWith('.pem')) return true;
  if (base.startsWith('id_')) return true;
  return false;
}

function validatePathShape(relPath) {
  if (path.posix.isAbsolute(relPath) || relPath.startsWith('/')) {
    throw new RemoteIneligibleError('absolute path', relPath);
  }
  for (const seg of relPath.split('/')) {
    if (seg === '..' || seg === '') throw new RemoteIneligibleError('invalid path segment', relPath);
  }
}

/** Lexical (no realpath) escape check: does `target`, resolved relative to the
 *  directory containing `relPath` under `root`, stay inside `root`? */
function checkSymlinkEscape(root, relPath, target) {
  if (path.isAbsolute(target)) throw new RemoteIneligibleError('symlink target is absolute', relPath);
  const dir = path.dirname(path.join(root, relPath));
  const resolved = path.normalize(path.join(dir, target));
  const rootNorm = path.normalize(root);
  if (resolved !== rootNorm && !resolved.startsWith(rootNorm + path.sep)) {
    throw new RemoteIneligibleError('symlink target escapes root', relPath);
  }
}

/** Build one canonical entry from a file/symlink already lstat'd. Shared by
 *  the git-driven builder and the git-less filesystem walk used to verify. */
function computeEntry(absPath, relPath, st) {
  if (st.isSymbolicLink()) {
    return { path: relPath, type: 'symlink', target: fs.readlinkSync(absPath) };
  }
  if (!st.isFile()) {
    throw new RemoteIneligibleError('special file', relPath);
  }
  const exec = (st.mode & 0o111) !== 0;
  const buf = fs.readFileSync(absPath);
  const sha256 = crypto.createHash('sha256').update(buf).digest('hex');
  return { path: relPath, type: 'file', exec, size: buf.length, sha256 };
}

/** Parse `git ls-files -z -s` NUL-terminated output into path -> mode string. */
function parseModeMap(buf) {
  const map = new Map();
  for (const chunk of buf.toString('utf8').split('\0')) {
    if (!chunk) continue;
    const tab = chunk.indexOf('\t');
    if (tab === -1) continue;
    const meta = chunk.slice(0, tab).trim().split(/\s+/);
    const relPath = chunk.slice(tab + 1);
    map.set(relPath, meta[0]);
  }
  return map;
}

function parsePathList(buf) {
  return buf
    .toString('utf8')
    .split('\0')
    .filter((s) => s.length > 0);
}

/**
 * Build a verified manifest of a worktree snapshot: the file set is
 * `git ls-files -z -c -o --exclude-standard`, filtered to paths that exist on
 * disk, each hashed/typed. Throws `RemoteIneligibleError` for anything a
 * caller must treat as "fall back to local" (gitlinks, escaping symlinks,
 * special files, denylisted secret paths, malformed paths).
 */
export function buildManifest(worktreeRoot) {
  const root = path.resolve(worktreeRoot);
  const env = scrubbedGitEnv();
  const common = { cwd: root, env, encoding: 'buffer', maxBuffer: 1024 * 1024 * 256 };

  let pathsOut;
  let modesOut;
  try {
    pathsOut = execFileSync('git', ['ls-files', '-z', '-c', '-o', '--exclude-standard'], common);
  } catch (err) {
    throw new RemoteIneligibleError('git ls-files failed', undefined, { cause: err });
  }
  try {
    modesOut = execFileSync('git', ['ls-files', '-z', '-s', '-c', '--exclude-standard'], common);
  } catch (err) {
    throw new RemoteIneligibleError('git ls-files -s failed', undefined, { cause: err });
  }

  const modeMap = parseModeMap(modesOut);
  const candidates = parsePathList(pathsOut);

  const seen = new Set();
  const rawEntries = [];
  for (const relPath of candidates) {
    if (seen.has(relPath)) continue;
    seen.add(relPath);

    validatePathShape(relPath);
    if (isDenylistedPath(relPath)) throw new RemoteIneligibleError('denylisted secret path', relPath);
    if (modeMap.get(relPath) === '160000') throw new RemoteIneligibleError('gitlink/submodule', relPath);

    const absPath = path.join(root, relPath);
    let st;
    try {
      st = fs.lstatSync(absPath);
    } catch {
      continue; // staged-delete / unstaged-delete: not on disk -> not in the snapshot.
    }
    if (st.isDirectory()) throw new RemoteIneligibleError('directory entry', relPath);
    if (st.isSymbolicLink()) {
      const target = fs.readlinkSync(absPath);
      checkSymlinkEscape(root, relPath, target);
    }
    rawEntries.push(computeEntry(absPath, relPath, st));
  }

  const entries = sortEntries(rawEntries).map(canonicalizeEntry);
  return { entries, manifestHash: manifestHashOf(entries) };
}

function walkDir(root) {
  const results = [];
  function recurse(absDir, relDir) {
    const names = fs.readdirSync(absDir).sort();
    for (const name of names) {
      const abs = path.join(absDir, name);
      const rel = relDir ? `${relDir}/${name}` : name;
      const st = fs.lstatSync(abs);
      if (st.isDirectory()) {
        recurse(abs, rel);
      } else {
        results.push(computeEntry(abs, rel, st));
      }
    }
  }
  recurse(root, '');
  return results;
}

/**
 * Recompute a manifest for `dir` by walking the filesystem directly (no
 * git -- used on the runner side, where the extracted snapshot has no .git),
 * and compare it against `expectedManifest`. Returns `{ok:true}` or
 * `{ok:false, reason}` naming the first mismatch found while walking both
 * entry lists in sorted-path order together.
 */
export function verifyManifestNoGit(dir, expectedManifest) {
  const expected = sortEntries(expectedManifest.entries).map(canonicalizeEntry);
  const actual = sortEntries(walkDir(path.resolve(dir))).map(canonicalizeEntry);

  let i = 0;
  let j = 0;
  while (i < expected.length || j < actual.length) {
    if (i >= expected.length) return { ok: false, reason: `extra: ${actual[j].path}` };
    if (j >= actual.length) return { ok: false, reason: `missing: ${expected[i].path}` };
    const e = expected[i];
    const a = actual[j];
    const cmp = comparePaths(e.path, a.path);
    if (cmp < 0) return { ok: false, reason: `missing: ${e.path}` };
    if (cmp > 0) return { ok: false, reason: `extra: ${a.path}` };
    if (e.type !== a.type) return { ok: false, reason: `type mismatch: ${e.path}` };
    if (e.type === 'file') {
      if (e.exec !== a.exec) return { ok: false, reason: `exec mismatch: ${e.path}` };
      if (e.size !== a.size) return { ok: false, reason: `size mismatch: ${e.path}` };
      if (e.sha256 !== a.sha256) return { ok: false, reason: `hash mismatch: ${e.path}` };
    } else if (e.target !== a.target) {
      return { ok: false, reason: `target mismatch: ${e.path}` };
    }
    i += 1;
    j += 1;
  }
  return { ok: true };
}

export { sortEntries, canonicalizeEntry };
