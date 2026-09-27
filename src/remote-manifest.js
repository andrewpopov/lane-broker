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
export function scrubbedGitEnv() {
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

/** Basenames whose trailing suffix marks a secret-shaped path as a template, not a real secret. */
const ENV_TEMPLATE_SUFFIXES = ['.example', '.sample', '.template', '.dist'];

function isDenylistedPath(relPath) {
  const base = path.posix.basename(relPath);
  if (base === '.env' || base.startsWith('.env.')) {
    return !ENV_TEMPLATE_SUFFIXES.some((suffix) => base.endsWith(suffix));
  }
  if (base.endsWith('.pem')) return true;
  if (base.startsWith('id_')) return true;
  return false;
}

/**
 * True iff `p` is a canonical relative path: a plain string, POSIX-separated,
 * no absolute/leading/trailing slash, no backslash or NUL byte, and no ''/'.'/'..'
 * segment. This is the ONE predicate every manifest entry path and every
 * extracted frame path must satisfy -- both the local builder (`buildManifest`,
 * `verifyManifestNoGit`) and the wire-side extractor (`remote-stream.js`'s
 * `validateFrame`) call it before anything is created on disk. A non-canonical
 * path (e.g. `a/./b`) that still passes a bare `'..'`-only check can make a
 * symlink-ancestor lookup miss its own entry: the ancestor check compares a
 * frame's literal path segments against the manifest's literal symlink paths,
 * so a symlink registered as `a/././link` never matches the ancestor prefix
 * `a/link` computed for a sibling frame `a/link/escaped` (BRAIN-319 review).
 */
export function isCanonicalRelPath(p) {
  if (typeof p !== 'string' || p.length === 0) return false;
  if (p.includes('\\') || p.includes('\0')) return false;
  if (p.startsWith('/') || p.endsWith('/')) return false;
  if (path.posix.isAbsolute(p)) return false;
  for (const seg of p.split('/')) {
    if (seg === '' || seg === '.' || seg === '..') return false;
  }
  return true;
}

function validatePathShape(relPath) {
  if (path.posix.isAbsolute(relPath) || relPath.startsWith('/')) {
    throw new RemoteIneligibleError('absolute path', relPath);
  }
  if (!isCanonicalRelPath(relPath)) {
    throw new RemoteIneligibleError('invalid path segment', relPath);
  }
}

/**
 * Lexical (no realpath) escape check: walk `target`, resolved relative to the
 * directory containing `relPath`, one component at a time (rather than a
 * single `path.normalize`), so a `..` cannot silently cancel out a component
 * that passed through another manifest symlink entry on the way. A naive
 * `path.normalize(dir + '/' + target)` hides exactly that case: given
 * `a/b/x -> ../..` and `a/b/s -> x/../y`, normalizing `a/b/x/../y` collapses
 * straight to `a/y` -- inside the root -- even though actually FOLLOWING the
 * path means stepping through the symlink `a/b/x` first, whose own target
 * escapes upward. `symlinkPaths` is the full set of symlink entry paths in
 * the manifest (both `relPath`'s own ancestor chain and every step of the
 * walked target are checked against it). Throws `RemoteIneligibleError` for
 * an out-of-root escape OR a walk that passes through another symlink entry.
 */
function checkSymlinkEscape(relPath, target, symlinkPaths) {
  if (path.isAbsolute(target)) throw new RemoteIneligibleError('symlink target is absolute', relPath);

  const dirSegs = path.posix.dirname(relPath) === '.' ? [] : path.posix.dirname(relPath).split('/');

  // "the path walked to reach it": relPath's own ancestor directories.
  let acc = '';
  for (const seg of dirSegs) {
    acc = acc ? `${acc}/${seg}` : seg;
    if (symlinkPaths.has(acc)) {
      throw new RemoteIneligibleError('symlink target escapes root (ancestor is a symlink)', relPath);
    }
  }

  const stack = [...dirSegs];
  for (const part of target.split('/')) {
    if (part === '' || part === '.') continue;
    if (part === '..') {
      if (stack.length === 0) throw new RemoteIneligibleError('symlink target escapes root', relPath);
      stack.pop();
      continue;
    }
    stack.push(part);
    const cur = stack.join('/');
    if (symlinkPaths.has(cur) && cur !== relPath) {
      throw new RemoteIneligibleError('symlink target escapes root (passes through a symlink entry)', relPath);
    }
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
    rawEntries.push(computeEntry(absPath, relPath, st));
  }

  // Escape checking runs as a second pass, once every symlink entry's path is
  // known: a chained escape (see checkSymlinkEscape) can only be detected
  // against the FULL set of symlink paths, not path-by-path as each is
  // discovered in `git ls-files` order.
  const symlinkPaths = new Set(rawEntries.filter((e) => e.type === 'symlink').map((e) => e.path));
  for (const e of rawEntries) {
    if (e.type === 'symlink') checkSymlinkEscape(e.path, e.target, symlinkPaths);
  }

  const entries = sortEntries(rawEntries).map(canonicalizeEntry);
  return { entries, manifestHash: manifestHashOf(entries) };
}

function walkDir(root, ignoreRootGit) {
  const results = [];
  function recurse(absDir, relDir) {
    const names = fs.readdirSync(absDir).sort();
    for (const name of names) {
      if (ignoreRootGit && relDir === '' && name === '.git') continue;
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
 * entry lists in sorted-path order together. `ignoreRootGit` (BRAIN-319 T3):
 * skip exactly a `.git` directory sitting at `dir`'s own root -- nothing
 * else -- so re-verifying after `remote-exec` has git-init'd the snapshot
 * (to give real consumers a working git repo) doesn't see its own `.git` as
 * an unexpected extra entry, while still catching any actual tree mutation.
 */
export function verifyManifestNoGit(dir, expectedManifest, { ignoreRootGit = false } = {}) {
  const expected = sortEntries(expectedManifest.entries).map(canonicalizeEntry);
  const actual = sortEntries(walkDir(path.resolve(dir), ignoreRootGit)).map(canonicalizeEntry);

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

/** True iff `dir` (a canonical `remoteDeps` entry, or `.`) is `path`'s ancestor, or equal to it. */
function dirContains(dir, p) {
  if (dir === '.') return true;
  return p === dir || p.startsWith(`${dir}/`);
}

/**
 * BRAIN-320 S1a: pure client-side eligibility check for a `remoteDeps` lane,
 * run against the manifest `buildManifest` already produces -- before ever
 * probing a runner (see supervisor.js's eligibility hook, which calls this
 * right alongside `buildManifest`). `dirs` is `resolveTicketConfig`'s
 * `remoteDeps` array. Returns `{ok:true}` or `{ok:false, reason}`; a caller
 * treats `!ok` as "fall back to local" (same shape as `RemoteIneligibleError`,
 * but this never throws -- it is meant to be checked, not caught). Exported
 * (not folded into `buildManifest` itself) so the runner can re-run the exact
 * same rules against the extracted tree (1c) in the next slice.
 */
export function validateRemoteDeps(manifest, dirs) {
  const paths = manifest.entries.map((e) => e.path);
  const pathSet = new Set(paths);

  for (let i = 0; i < dirs.length; i += 1) {
    for (let j = i + 1; j < dirs.length; j += 1) {
      if (dirContains(dirs[i], dirs[j]) || dirContains(dirs[j], dirs[i])) {
        return { ok: false, reason: `remoteDeps dirs overlap: "${dirs[i]}" and "${dirs[j]}"` };
      }
    }
  }

  for (const dir of dirs) {
    const lockfile = dir === '.' ? 'package-lock.json' : `${dir}/package-lock.json`;
    if (!pathSet.has(lockfile)) {
      return { ok: false, reason: `remoteDeps dir "${dir}" has no ${lockfile} in the manifest` };
    }
    const shrinkwrap = dir === '.' ? 'npm-shrinkwrap.json' : `${dir}/npm-shrinkwrap.json`;
    if (pathSet.has(shrinkwrap)) {
      return { ok: false, reason: `remoteDeps dir "${dir}" has a ${shrinkwrap} (would override the lockfile)` };
    }
    const nodeModulesPrefix = dir === '.' ? 'node_modules/' : `${dir}/node_modules/`;
    const nodeModulesExact = dir === '.' ? 'node_modules' : `${dir}/node_modules`;
    for (const p of paths) {
      if (p === nodeModulesExact || p.startsWith(nodeModulesPrefix)) {
        return { ok: false, reason: `remoteDeps dir "${dir}" has a manifest entry under node_modules: "${p}"` };
      }
    }
  }

  return { ok: true };
}

export { sortEntries, canonicalizeEntry };
