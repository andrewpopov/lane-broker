import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { isProcessAlive, processStartTime } from './process-liveness.js';

/**
 * BRAIN-389: a content-addressed store of installed `node_modules` trees on a
 * remote runner, so a `remoteDeps` dir whose lockfile/environment has been
 * seen before is materialized instead of running `npm ci` again.
 *
 *   <cacheRoot>/<key>/node_modules   the tree, every file and dir read-only
 *   <cacheRoot>/<key>/meta.json      { bytes, createdAt }
 *   <cacheRoot>/<key>/.last-used     mtime = last use, the LRU clock
 *   <cacheRoot>/<key>/leases/<id>    one file per live run holding the key
 *   <cacheRoot>/.tmp-*, .trash-*     unpublished / evicted trees awaiting removal
 *
 * A hit is a hardlink farm (real, writable directories; files are links to the
 * read-only store files), so an in-place write to a materialized file fails
 * with EACCES instead of corrupting the store, while creating and renaming
 * over files still works.
 */

export const DEFAULT_DEPS_CACHE_MAX_BYTES = 10 * 1024 ** 3;

/** Bumping this invalidates every existing entry. */
const KEY_VERSION = 'v1';

/** npm reads npm-shrinkwrap.json in preference to package-lock.json. */
const LOCKFILES = ['npm-shrinkwrap.json', 'package-lock.json'];

const NPM_CONFIG_ENV_RE = /^npm_config_/i;

/** npm_config_* values that name a file whose BYTES also shape an install. */
const NPM_CONFIG_FILE_ENVS = ['npm_config_userconfig', 'npm_config_globalconfig'];

const KEY_RE = /^[0-9a-f]{64}$/;

/** Files up to this size are searched for the install path; a larger one is a binary blob, not a shim or config. */
const MAX_SCANNED_FILE_BYTES = 2 * 1024 * 1024;

/** A hit checks this many files from the front of the store plus this many chosen at random. */
const READ_ONLY_SAMPLE = 200;

/** Unpublished temp dirs older than this belong to a crashed publisher. */
const STALE_TMP_MS = 60 * 60 * 1000;

function readIfExists(file) {
  try {
    return fs.readFileSync(file);
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
}

/** A lockfile that links local packages (workspaces, `file:` deps) installs symlinks into files the key does not cover. */
function linksLocalPackages(lockfileBytes) {
  const lock = JSON.parse(lockfileBytes.toString('utf8'));
  const packages = lock && typeof lock === 'object' ? lock.packages : undefined;
  if (!packages || typeof packages !== 'object') return false;
  return Object.keys(packages).some((k) => k !== '' && !k.startsWith('node_modules/'));
}

/**
 * The cache key for one `remoteDeps` dir: a sha256 over everything that can change what `npm ci`
 * produces there. Returns `{ key }` or `{ key: null, reason }` when the dir cannot be cached at all.
 *
 *  - the lockfile (name and bytes), `package.json`, and the `.npmrc` of the dir and of the repo root;
 *  - node version/platform/arch (`runtime`, injectable for tests) and the npm version;
 *  - the exact install argv;
 *  - every `npm_config_*` value in `env` (the deps env the pipeline controls), plus the bytes of the
 *    user/global npmrc files those values point at.
 */
export function computeDepsKey({
  dir,
  rootDir,
  installArgv,
  env,
  npmVersion,
  runtime = { version: process.version, platform: process.platform, arch: process.arch },
}) {
  let lockfileName = null;
  let lockfile = null;
  for (const name of LOCKFILES) {
    lockfile = readIfExists(path.join(dir, name));
    if (lockfile) {
      lockfileName = name;
      break;
    }
  }
  if (!lockfile) return { key: null, reason: 'no lockfile' };
  const packageJson = readIfExists(path.join(dir, 'package.json'));
  if (!packageJson) return { key: null, reason: 'no package.json' };
  try {
    if (linksLocalPackages(lockfile)) return { key: null, reason: 'lockfile links local packages (workspaces or file: deps)' };
  } catch {
    return { key: null, reason: 'lockfile is not valid JSON' };
  }

  const parts = [
    ['key-version', KEY_VERSION],
    ['lockfile-name', lockfileName],
    ['lockfile', lockfile],
    ['package.json', packageJson],
    ['npmrc:dir', readIfExists(path.join(dir, '.npmrc')) ?? ''],
    ['npmrc:root', readIfExists(path.join(rootDir, '.npmrc')) ?? ''],
    ['node-version', runtime.version],
    ['node-platform', runtime.platform],
    ['node-arch', runtime.arch],
    ['npm-version', npmVersion],
    ['install-argv', JSON.stringify(installArgv)],
  ];
  const npmConfigNames = Object.keys(env).filter((k) => NPM_CONFIG_ENV_RE.test(k)).sort();
  for (const name of npmConfigNames) parts.push([`env:${name}`, String(env[name])]);
  for (const name of NPM_CONFIG_FILE_ENVS) {
    if (env[name]) parts.push([`file:${name}`, readIfExists(env[name]) ?? '']);
  }

  const hash = crypto.createHash('sha256');
  for (const [label, value] of parts) {
    const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8');
    hash.update(`${label}:${bytes.length}:`);
    hash.update(bytes);
  }
  return { key: hash.digest('hex') };
}

export function entryDir(cacheRoot, key) {
  return path.join(cacheRoot, key);
}

export function entryTree(cacheRoot, key) {
  return path.join(entryDir(cacheRoot, key), 'node_modules');
}

/**
 * Recreate `src` as `dst`, directory by directory: real directories, symlinks re-created with their
 * own target text, regular files handed to `onFile(srcFile, dstFile)`. Each directory is created
 * owner-writable so its entries can be added, then given `dirMode(srcMode)` once it is complete.
 */
function cloneTree(src, dst, { onFile, dirMode }) {
  const srcMode = fs.statSync(src).mode & 0o7777;
  fs.mkdirSync(dst, { mode: 0o700 });
  for (const ent of fs.readdirSync(src, { withFileTypes: true })) {
    const s = path.join(src, ent.name);
    const d = path.join(dst, ent.name);
    if (ent.isDirectory()) cloneTree(s, d, { onFile, dirMode });
    else if (ent.isSymbolicLink()) fs.symlinkSync(fs.readlinkSync(s), d);
    else if (ent.isFile()) onFile(s, d);
    else throw new Error(`unsupported entry type at ${s}`);
  }
  fs.chmodSync(dst, dirMode(srcMode));
}

/**
 * Remove a store tree. Its directories are read-only, which `rm` cannot descend, so make just the
 * directories writable first. Never the files: a running lane's farm shares their inodes.
 */
export function removeTree(target) {
  try {
    execFileSync('find', [target, '-type', 'd', '-exec', 'chmod', 'u+w', '{}', '+'], { stdio: 'ignore' });
  } catch {
    // already gone, or nothing to fix; rmSync below reports a real problem
  }
  fs.rmSync(target, { recursive: true, force: true });
}

/**
 * Materialize a stored tree at `destTree` as a hardlink farm. Throws on any failure (including a
 * `destTree` that already exists); the caller removes the partial tree and installs instead.
 */
export function materializeFromStore(cacheRoot, key, destTree) {
  cloneTree(entryTree(cacheRoot, key), destTree, {
    onFile: (s, d) => fs.linkSync(s, d),
    dirMode: (mode) => mode | 0o200,
  });
}

/**
 * Copy an installed tree into the store and publish it atomically (build under `.tmp-*`, then rename
 * onto `<key>`). It is a COPY, never a link: the lane about to run in `srcTree` may write to it.
 * Returns `{ published: true, bytes }`, or `{ published: false }` when another run published the same
 * key first (that run's tree is equally valid; ours is discarded).
 */
export function publishToStore(cacheRoot, key, srcTree) {
  fs.mkdirSync(cacheRoot, { recursive: true });
  const tmp = path.join(cacheRoot, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  fs.mkdirSync(tmp);
  try {
    let bytes = 0;
    cloneTree(srcTree, path.join(tmp, 'node_modules'), {
      onFile: (s, d) => {
        fs.copyFileSync(s, d, fs.constants.COPYFILE_FICLONE);
        const st = fs.statSync(d);
        bytes += st.size;
        fs.chmodSync(d, st.mode & 0o7555);
      },
      dirMode: (mode) => mode & 0o7555,
    });
    fs.writeFileSync(path.join(tmp, 'meta.json'), JSON.stringify({ bytes, createdAt: Date.now() }));
    fs.writeFileSync(path.join(tmp, '.last-used'), '');
    fs.mkdirSync(path.join(tmp, 'leases'));
    try {
      fs.renameSync(tmp, entryDir(cacheRoot, key));
    } catch (err) {
      if (err.code === 'ENOTEMPTY' || err.code === 'EEXIST') {
        removeTree(tmp);
        return { published: false };
      }
      throw err;
    }
    return { published: true, bytes };
  } catch (err) {
    removeTree(tmp);
    throw err;
  }
}

/** Move an entry aside atomically for `purgeTrash`; false if it was already gone. */
export function quarantineEntry(cacheRoot, key) {
  try {
    fs.renameSync(entryDir(cacheRoot, key), path.join(cacheRoot, `.trash-${key}-${crypto.randomBytes(4).toString('hex')}`));
    return true;
  } catch {
    return false;
  }
}

/**
 * Sample a stored tree's files (the first READ_ONLY_SAMPLE plus READ_ONLY_SAMPLE random others) and
 * return the first one that is writable, as a path relative to the tree, or null if all are read-only.
 * A writable store file means something chmod'ed a shared inode, so the entry can no longer be trusted.
 */
export function findWritableStoreFile(cacheRoot, key) {
  const root = entryTree(cacheRoot, key);
  const files = [];
  const walk = (abs) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      const p = path.join(abs, ent.name);
      if (ent.isDirectory()) walk(p);
      else if (ent.isFile()) files.push(p);
    }
  };
  walk(root);
  const rest = files.slice(READ_ONLY_SAMPLE);
  const sample = files.slice(0, READ_ONLY_SAMPLE);
  for (let i = 0; i < READ_ONLY_SAMPLE && rest.length > 0; i += 1) sample.push(rest[crypto.randomInt(rest.length)]);
  const writable = sample.find((f) => (fs.statSync(f).mode & 0o222) !== 0);
  return writable ? path.relative(root, writable) : null;
}

/**
 * The first file under `tree` (relative path) that holds one of `needles` (absolute paths) as bytes: a
 * symlink target, or a regular file up to MAX_SCANNED_FILE_BYTES. Such a tree only works where it was
 * installed, so it must not be cached. Null when clean.
 */
export function findInstallPathReference(tree, needles) {
  const bufs = needles.map((n) => Buffer.from(n, 'utf8'));
  const walk = (abs, rel) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      const p = path.join(abs, ent.name);
      const r = rel ? `${rel}/${ent.name}` : ent.name;
      let hit = false;
      if (ent.isDirectory()) {
        const found = walk(p, r);
        if (found) return found;
      } else if (ent.isSymbolicLink()) {
        const target = Buffer.from(fs.readlinkSync(p), 'utf8');
        hit = bufs.some((b) => target.includes(b));
      } else if (ent.isFile() && fs.statSync(p).size <= MAX_SCANNED_FILE_BYTES) {
        const content = fs.readFileSync(p);
        hit = bufs.some((b) => content.includes(b));
      }
      if (hit) return r;
    }
    return null;
  };
  return walk(tree, '');
}

/** Mark `key` as just used (the LRU clock). Best-effort: a failed touch must never fail a lane. */
export function touchLastUsed(cacheRoot, key) {
  const file = path.join(entryDir(cacheRoot, key), '.last-used');
  const now = new Date();
  try {
    fs.utimesSync(file, now, now);
  } catch {
    // the entry was evicted under us, or the volume is read-only
  }
}

/**
 * Record that this process is using `key`, so eviction skips it. Throws if the entry is gone
 * (evicted before we could lease it): the caller treats that as a miss. Returns a release function.
 */
export function acquireLease(cacheRoot, key) {
  const file = path.join(entryDir(cacheRoot, key), 'leases', `${process.pid}-${crypto.randomBytes(4).toString('hex')}`);
  fs.writeFileSync(file, JSON.stringify({ pid: process.pid, start: processStartTime(process.pid) ?? null }));
  return () => {
    try {
      fs.unlinkSync(file);
    } catch {
      // the key was evicted anyway
    }
  };
}

/** Live leases on an entry; a lease whose process is gone is removed as stale. */
function liveLeases(keyDir) {
  const dir = path.join(keyDir, 'leases');
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return 0;
  }
  let live = 0;
  for (const name of names) {
    const file = path.join(dir, name);
    let lease = null;
    try {
      lease = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      // unreadable: treated as stale below
    }
    if (lease && Number.isInteger(lease.pid) && isProcessAlive(lease.pid, lease.start)) live += 1;
    else fs.rmSync(file, { force: true });
  }
  return live;
}

function readMetaBytes(keyDir) {
  try {
    const meta = JSON.parse(fs.readFileSync(path.join(keyDir, 'meta.json'), 'utf8'));
    return Number.isFinite(meta.bytes) && meta.bytes >= 0 ? meta.bytes : null;
  } catch {
    return null;
  }
}

/**
 * Pick the least-recently-used entries until the store fits `maxBytes`, skipping every key a live run
 * holds a lease on, and move each (atomically, by rename) to `.trash-*`. Cheap and meant to run under
 * the broker lock; the caller removes the trash afterwards with `purgeTrash`, outside the lock.
 * An entry with no readable `meta.json` is damaged and goes first. Returns the evicted keys.
 */
export function evictLeastRecentlyUsed(cacheRoot, maxBytes, { now = Date.now() } = {}) {
  let names;
  try {
    names = fs.readdirSync(cacheRoot);
  } catch {
    return [];
  }
  const entries = [];
  for (const name of names) {
    const full = path.join(cacheRoot, name);
    if (name.startsWith('.tmp-')) {
      // a crashed publisher's leftovers; a live publisher's tmp is recent
      try {
        if (now - fs.statSync(full).mtimeMs > STALE_TMP_MS) fs.renameSync(full, path.join(cacheRoot, `.trash-${name.slice(1)}`));
      } catch {
        // raced with the publisher's own rename or cleanup
      }
      continue;
    }
    if (!KEY_RE.test(name)) continue;
    let lastUsed = 0;
    try {
      lastUsed = fs.statSync(path.join(full, '.last-used')).mtimeMs;
    } catch {
      // damaged entry: oldest
    }
    const bytes = readMetaBytes(full);
    entries.push({ key: name, full, bytes: bytes ?? 0, damaged: bytes === null, lastUsed });
  }

  let total = entries.reduce((sum, e) => sum + e.bytes, 0);
  const evicted = [];
  const victims = [...entries].sort((a, b) => Number(b.damaged) - Number(a.damaged) || a.lastUsed - b.lastUsed);
  for (const entry of victims) {
    if (total <= maxBytes && !entry.damaged) break;
    if (liveLeases(entry.full) > 0) continue;
    if (!quarantineEntry(cacheRoot, entry.key)) continue;
    total -= entry.bytes;
    evicted.push(entry.key);
  }
  return evicted;
}

/** Remove everything `evictLeastRecentlyUsed` moved aside (and any earlier run's leftovers). */
export function purgeTrash(cacheRoot) {
  let names;
  try {
    names = fs.readdirSync(cacheRoot);
  } catch {
    return;
  }
  for (const name of names) {
    if (name.startsWith('.trash-')) removeTree(path.join(cacheRoot, name));
  }
}

/**
 * Everything under `dir` except `node_modules` trees, as path -> "type:size:mtimeMs". Taken before and
 * after an install: any difference means a lifecycle script wrote outside `node_modules`, which a
 * cached `node_modules` could never reproduce.
 */
export function snapshotOutsideNodeModules(dir) {
  const out = new Map();
  const walk = (abs, rel) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      if (ent.name === 'node_modules') continue;
      const childRel = rel ? `${rel}/${ent.name}` : ent.name;
      const st = fs.lstatSync(path.join(abs, ent.name));
      out.set(childRel, `${ent.isDirectory() ? 'd' : ent.isSymbolicLink() ? 'l' : 'f'}:${st.size}:${st.mtimeMs}`);
      if (ent.isDirectory()) walk(path.join(abs, ent.name), childRel);
    }
  };
  walk(dir, '');
  return out;
}

/** Paths added, removed or changed between two `snapshotOutsideNodeModules` results. */
export function snapshotChanges(before, after) {
  const changed = [];
  for (const [p, sig] of after) if (before.get(p) !== sig) changed.push(p);
  for (const p of before.keys()) if (!after.has(p)) changed.push(p);
  return changed.sort();
}
