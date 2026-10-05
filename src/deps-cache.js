import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { isProcessAlive, processStartTime } from './process-liveness.js';

/**
 * BRAIN-389: a content-addressed store of installed `node_modules` trees on a
 * remote runner, so a `remoteDeps` dir whose lockfile/environment has been
 * seen before gets a copy of an installed tree instead of running `npm ci`.
 *
 *   <cacheRoot>/<key>/node_modules   the tree, every file and dir read-only
 *   <cacheRoot>/<key>/meta.json      { bytes, createdAt }
 *   <cacheRoot>/<key>/.last-used     mtime = last use, the LRU clock
 *   <cacheRoot>/<key>/leases/<id>    one file per live run holding the key
 *   <cacheRoot>/.tmp-*, .trash-*     unpublished / evicted trees awaiting removal
 *
 * A hit COPIES the stored tree into the work dir as private, writable files.
 * Nothing a lane does to its own tree can reach the store: a hardlink would
 * share the inode, and a lane that chmods a linked file, edits it and restores
 * the mode would poison every later hit.
 *
 * The cache must never hand a run a different tree than a fresh `npm ci`
 * would, so everything here fails closed: a dir is cached only when its key
 * covers every input, its sources are pinned and its install is reproducible.
 */

export const DEFAULT_DEPS_CACHE_MAX_BYTES = 10 * 1024 ** 3;

/** Bumping this invalidates every existing entry. */
const KEY_VERSION = 'v2';

/** npm reads npm-shrinkwrap.json in preference to package-lock.json. */
const LOCKFILES = ['npm-shrinkwrap.json', 'package-lock.json'];

/** npm_config_* values that name a file whose BYTES also shape an install. */
const NPM_CONFIG_FILE_ENVS = ['npm_config_userconfig', 'npm_config_globalconfig'];

/**
 * The deps environment is hashed whole (an install can read any variable: PATH, CC, HOME, proxy
 * settings...), so the variables that differ on every run without meaning anything to an install are
 * REMOVED from it before npm or any script sees them (`scrubDepsEnv`), not merely left out of the key:
 * a variable that reaches the install but not the key would let two different installs share an entry.
 * Removed: shell bookkeeping, the ssh session, git's ceiling (the work dir has its own `.git`), and lane's
 * own ticket/id variables. A few tools do need a temp dir, so TMPDIR/TMP/TEMP stay in the environment, are
 * not hashed, and their (per-run) paths join the relocatability scan instead.
 */
export const SCRUBBED_ENV_NAMES = new Set(['PWD', 'OLDPWD', 'SHLVL', '_', 'GIT_CEILING_DIRECTORIES']);
export const SCRUBBED_ENV_PREFIXES = ['SSH_', 'LANE_'];
export const PER_RUN_PATH_ENV_NAMES = ['TMPDIR', 'TMP', 'TEMP'];

const isScrubbed = (name) => SCRUBBED_ENV_NAMES.has(name) || SCRUBBED_ENV_PREFIXES.some((p) => name.startsWith(p));

export function scrubDepsEnv(env) {
  return Object.fromEntries(Object.entries(env).filter(([name]) => !isScrubbed(name)));
}

/**
 * Every root lifecycle script `npm ci` can run, in the order npm runs them. Derived from npm 11.9.0's source:
 *  - `predependencies`, `dependencies`, `postdependencies`: `@npmcli/arborist/lib/arborist/reify.js`
 *    (run after the tree is saved, whenever reification changed any dependency, which `npm ci` always does);
 *  - `preinstall`, `install`, `postinstall`, `prepublish`, `preprepare`, `prepare`, `postprepare`:
 *    `lib/commands/ci.js` (its `scripts` list, run after reify).
 * A root script outside this list is never run by `npm ci`, so it cannot matter here.
 */
export const ROOT_SCRIPT_NAMES = [
  'predependencies',
  'dependencies',
  'postdependencies',
  'preinstall',
  'install',
  'postinstall',
  'prepublish',
  'preprepare',
  'prepare',
  'postprepare',
];

/**
 * Exact root-script commands known not to touch `node_modules`, each with the one `.git/config` line it
 * is expected to write in the work dir's synthetic repo (or null for none). Any other command needs the
 * lane's explicit say-so. These are REPLAYED on every hit, since their side effect lives outside the cache.
 */
export const ALLOWED_ROOT_SCRIPTS = new Map([['git config core.hooksPath .githooks || true', /^hookspath = \.githooks$/i]]);

/** A lock `resolved` pinned to content: an npmjs.org tarball with integrity, a git commit, or a local tarball with integrity. */
const REGISTRY_RE = /^https:\/\/registry\.npmjs\.org\//;
const GIT_PINNED_RE = /^git\+.+#[0-9a-f]{40}$/;
const FILE_TARBALL_RE = /^file:.+\.tgz$/;

/** Files are searched for the install path in chunks of this size, so a large file never needs to fit in memory. */
export const SCAN_CHUNK_BYTES = 1024 * 1024;

const KEY_RE = /^[0-9a-f]{64}$/;

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

function firstLineOfCommand(cmd, env) {
  try {
    const out = execFileSync(cmd, ['--version'], { env, encoding: 'utf8', timeout: 5000, stdio: ['ignore', 'pipe', 'ignore'] });
    return out.split('\n')[0].trim();
  } catch {
    return 'missing';
  }
}

let systemMemo;
/** What the installed native code was built against: glibc (null on musl/macOS), the libc family, and the distro. */
export function currentSystem() {
  if (systemMemo) return systemMemo;
  const glibc = process.report?.getReport?.().header?.glibcVersionRuntime ?? '';
  let osId = '';
  let osVersionId = '';
  try {
    const text = fs.readFileSync('/etc/os-release', 'utf8');
    const field = (name) => text.match(new RegExp(`^${name}=(.*)$`, 'm'))?.[1].replace(/^"|"$/g, '') ?? '';
    osId = field('ID');
    osVersionId = field('VERSION_ID');
  } catch {
    // not a Linux with /etc/os-release
  }
  systemMemo = { glibc, libc: process.platform === 'linux' ? (glibc ? 'glibc' : 'musl') : undefined, osId, osVersionId };
  return systemMemo;
}

/**
 * Whether every lock entry is pinned to content, so `npm ci` from this lock is reproducible. Fails closed:
 * lock v2/v3 with a `packages` map only, and every non-root entry must be an npmjs.org tarball with
 * integrity, a git commit pin, or a `file:*.tgz` with integrity (returned in `tarballs`, which the key
 * hashes). `file:` dirs, links, workspaces and unresolved entries are refused.
 *
 * An entry with no `resolved` of its own is accepted only when its bytes ride inside an enclosing package's
 * tarball: it is `inBundle`, nested under a package that bundles it (the parent's `bundleDependencies` names
 * it, or the entry is `inDepBundle`), and that parent itself passed these checks. A top-level entry has no
 * enclosing package (root-bundled deps are fetched like any other), so it gets the normal checks.
 */
export function lockEligibility(lock) {
  if (!lock || ![2, 3].includes(lock.lockfileVersion) || !lock.packages || typeof lock.packages !== 'object') {
    return { ok: false, reason: 'lockfile is not v2 or v3 with a packages map' };
  }
  const packages = lock.packages;
  const tarballs = [];
  const verdicts = new Map();

  const check = (name) => {
    if (verdicts.has(name)) return verdicts.get(name);
    verdicts.set(name, 'in progress'); // a cycle is not a pass
    const verdict = checkEntry(name);
    verdicts.set(name, verdict);
    return verdict;
  };
  const checkEntry = (name) => {
    const entry = packages[name];
    if (!entry || typeof entry !== 'object') return `lock entry ${name} is malformed`;
    if (entry.link === true) return `lock entry ${name} is a link`;
    if (!name.startsWith('node_modules/')) return `lock entry ${name} is not under node_modules (workspace or local package)`;
    const { resolved, integrity } = entry;
    if (typeof resolved === 'string') {
      if (REGISTRY_RE.test(resolved) && typeof integrity === 'string') return null;
      if (GIT_PINNED_RE.test(resolved)) return null;
      if (FILE_TARBALL_RE.test(resolved) && typeof integrity === 'string') {
        tarballs.push(resolved.slice('file:'.length));
        return null;
      }
      return `lock entry ${name} resolves to an unpinned source: ${resolved.slice(0, 80)}`;
    }
    const nested = name.lastIndexOf('/node_modules/');
    if (entry.inBundle !== true || nested === -1) return `lock entry ${name} has no resolved`;
    const parentName = name.slice(0, nested);
    const parent = packages[parentName];
    const childName = name.slice(nested + '/node_modules/'.length);
    const bundles = Array.isArray(parent?.bundleDependencies) && parent.bundleDependencies.includes(childName);
    if (!parent || (!bundles && entry.inDepBundle !== true)) return `lock entry ${name} has no resolved and no enclosing package bundles it`;
    const parentVerdict = check(parentName);
    return parentVerdict === null ? null : `lock entry ${name} is bundled by ${parentName}, which fails: ${parentVerdict}`;
  };

  let hasInstallScript = false;
  for (const [name, entry] of Object.entries(packages)) {
    if (entry?.hasInstallScript) hasInstallScript = true;
    if (name === '') continue;
    const verdict = check(name);
    if (verdict !== null) return { ok: false, reason: verdict };
  }
  return { ok: true, tarballs: [...new Set(tarballs)].sort(), hasInstallScript };
}

/** The first root lifecycle script that is neither allowlisted nor declared safe, as "<name>: <command>"; else null. */
function unsafeRootScript(packageJson, rootScriptsSafe) {
  if (rootScriptsSafe) return null;
  const scripts = packageJson.scripts && typeof packageJson.scripts === 'object' ? packageJson.scripts : {};
  for (const name of ROOT_SCRIPT_NAMES) {
    if (name in scripts && !ALLOWED_ROOT_SCRIPTS.has(scripts[name])) return `${name}: ${String(scripts[name]).slice(0, 80)}`;
  }
  return null;
}

/** The root scripts npm would run for this package that are on the allowlist, in npm's order (what a hit must replay). */
export function allowlistedRootEvents(scripts) {
  return ROOT_SCRIPT_NAMES.filter((name) => name in scripts && ALLOWED_ROOT_SCRIPTS.has(scripts[name]));
}

const configLines = (text) => (text ?? '').split('\n').map((l) => l.trim()).filter((l) => l !== '');

/**
 * Of the outside-`node_modules` `changes` an install made, those the allowlisted root scripts do NOT explain.
 * The scripts' only expected effect is rewriting the work dir's `.git/config` (and so `.git`'s own entry);
 * that is explained only if the config differs by exactly the lines those scripts write. A new hook file, or
 * any other `.git` write, stays unexplained.
 */
export function unexplainedChanges(changes, { scripts, configBefore, configAfter }) {
  const expected = allowlistedRootEvents(scripts).map((e) => ALLOWED_ROOT_SCRIPTS.get(scripts[e])).filter(Boolean);
  const before = configLines(configBefore);
  const after = configLines(configAfter);
  const added = after.filter((l) => !before.includes(l));
  const removed = before.filter((l) => !after.includes(l));
  const configExplained = expected.length > 0 && added.every((l) => expected.some((re) => re.test(l))) && removed.every((l) => expected.some((re) => re.test(l)));
  return changes.filter((c) => !(configExplained && (c === '.git' || c === '.git/config')));
}

/**
 * The cache key for one `remoteDeps` dir: a sha256 over everything that can change what `npm ci` produces
 * there. Returns `{ key, lock, scripts }`, or `{ key: null, reason }` when the dir must not be cached at all
 * (lockfile not pinned, or a root lifecycle script that is not known to leave `node_modules` alone).
 *
 *  - the lockfile (name and bytes), `package.json`, the `.npmrc` of the dir and of the repo root, and the
 *    bytes of every `file:` tarball the lockfile names;
 *  - node version/platform/arch (`runtime`), the glibc runtime and distro (`system`), the npm version;
 *  - when any lock entry has an install script, the first line of `cc --version` and `python3 --version`;
 *  - the exact install argv;
 *  - the whole `env` (already scrubbed by `scrubDepsEnv`; TMPDIR/TMP/TEMP are also left out), plus the
 *    bytes of the user/global npmrc files it points at.
 */
export function computeDepsKey({
  dir,
  rootDir,
  installArgv,
  env,
  npmVersion,
  rootScriptsSafe = false,
  runtime = { version: process.version, platform: process.platform, arch: process.arch },
  system = currentSystem(),
  toolVersion = (cmd) => firstLineOfCommand(cmd, env),
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

  let lock;
  let pkg;
  try {
    lock = JSON.parse(lockfile.toString('utf8'));
    pkg = JSON.parse(packageJson.toString('utf8'));
  } catch {
    return { key: null, reason: 'lockfile or package.json is not valid JSON' };
  }
  const eligibility = lockEligibility(lock);
  if (!eligibility.ok) return { key: null, reason: eligibility.reason };
  const script = unsafeRootScript(pkg, rootScriptsSafe);
  if (script) return { key: null, reason: `root lifecycle script not known to leave node_modules alone (${script})` };

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
    ['glibc', system.glibc],
    ['os-id', system.osId],
    ['os-version-id', system.osVersionId],
    ['npm-version', npmVersion],
    ['install-argv', JSON.stringify(installArgv)],
  ];
  if (eligibility.hasInstallScript) parts.push(['cc-version', toolVersion('cc')], ['python3-version', toolVersion('python3')]);
  for (const rel of eligibility.tarballs) {
    const bytes = readIfExists(path.resolve(dir, rel));
    if (!bytes) return { key: null, reason: `file: tarball ${rel} is not in the snapshot` };
    parts.push([`tarball:${rel}`, bytes]);
  }
  const unhashed = (name) => isScrubbed(name) || PER_RUN_PATH_ENV_NAMES.includes(name);
  for (const name of Object.keys(env).filter((k) => !unhashed(k)).sort()) parts.push([`env:${name}`, String(env[name])]);
  for (const name of NPM_CONFIG_FILE_ENVS) {
    if (env[name]) parts.push([`file:${name}`, readIfExists(env[name]) ?? '']);
  }

  const hash = crypto.createHash('sha256');
  for (const [label, value] of parts) {
    const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8');
    hash.update(`${label}:${bytes.length}:`);
    hash.update(bytes);
  }
  return { key: hash.digest('hex'), lock, scripts: pkg.scripts && typeof pkg.scripts === 'object' ? pkg.scripts : {} };
}

function listAllows(list, value) {
  if (!Array.isArray(list) || list.length === 0) return true;
  if (list.includes(`!${value}`)) return false;
  const positive = list.filter((x) => !x.startsWith('!'));
  return positive.length === 0 || positive.includes(value);
}

/**
 * Lock entries that apply to this platform (honouring `os`, `cpu` and `libc`) but are absent from
 * `installed`, npm's own record (`node_modules/.package-lock.json`'s `packages`) of what it put on disk.
 * npm skips an optional dependency it cannot install without failing; a tree missing one must not be cached.
 */
export function findMissingInstalled(lock, installed, { platform, arch, libc }) {
  const missing = [];
  for (const [name, entry] of Object.entries(lock.packages)) {
    if (name === '' || !entry) continue;
    if (!listAllows(entry.os, platform) || !listAllows(entry.cpu, arch)) continue;
    if (libc !== undefined && !listAllows(entry.libc, libc)) continue;
    if (!(name in installed)) missing.push(name.slice('node_modules/'.length));
  }
  return missing.sort();
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

/** Copy one file (a reflink where the filesystem has them) and set its mode; returns its size. */
function copyFileAs(src, dst, transformMode) {
  fs.copyFileSync(src, dst, fs.constants.COPYFILE_FICLONE);
  const st = fs.statSync(src);
  fs.chmodSync(dst, transformMode(st.mode & 0o7777));
  return st.size;
}

/**
 * Remove a store tree. Its directories are read-only, which `rm` cannot descend, so make just the
 * directories writable first.
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
 * Copy a stored tree to `destTree` as private files and directories the lane may write freely. Throws on
 * any failure (including a `destTree` that already exists); the caller removes the partial tree and installs.
 */
export function materializeFromStore(cacheRoot, key, destTree) {
  cloneTree(entryTree(cacheRoot, key), destTree, {
    onFile: (s, d) => copyFileAs(s, d, (mode) => mode | 0o200),
    dirMode: (mode) => mode | 0o200,
  });
}

/**
 * Copy an installed tree into the store, read-only, and publish it atomically (build under `.tmp-*`,
 * then rename onto `<key>`). Returns `{ published: true, bytes }`, or `{ published: false }` when another
 * run published the same key first (that run's tree is equally valid; ours is discarded).
 */
export function publishToStore(cacheRoot, key, srcTree) {
  fs.mkdirSync(cacheRoot, { recursive: true });
  const tmp = path.join(cacheRoot, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  fs.mkdirSync(tmp);
  try {
    let bytes = 0;
    cloneTree(srcTree, path.join(tmp, 'node_modules'), {
      onFile: (s, d) => {
        bytes += copyFileAs(s, d, (mode) => mode & 0o7555);
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
 * Leases and eviction share ONE lock (`lock(fn)`, the broker lock in production). A lease is taken, and
 * eviction chooses and quarantines its victims, each as a single critical section, so a run either holds
 * its lease before eviction looks (the key is skipped) or finds the entry gone when it tries (a miss).
 * Throws if the entry is gone; the caller treats that as a miss.
 *
 * The lease file is written under a temp name and renamed in, so a reader never sees a half-written one.
 * Returns `async release()`, which drops the lease and trims the store to `maxBytes` (an entry that
 * was over the bound only because a lease protected it can go now).
 */
export async function acquireLease(cacheRoot, key, maxBytes, lock) {
  const leasePath = await lock(() => {
    const keyDir = entryDir(cacheRoot, key);
    const tmp = path.join(keyDir, `.lease-tmp-${crypto.randomBytes(4).toString('hex')}`);
    const file = path.join(keyDir, 'leases', `${process.pid}-${crypto.randomBytes(4).toString('hex')}`);
    fs.writeFileSync(tmp, JSON.stringify({ pid: process.pid, start: processStartTime(process.pid) ?? null }));
    fs.renameSync(tmp, file);
    return file;
  });
  return async () => {
    await lock(() => {
      fs.rmSync(leasePath, { force: true });
      evictLeastRecentlyUsed(cacheRoot, maxBytes);
    });
    purgeTrash(cacheRoot);
  };
}

/** Trim the store to `maxBytes` under the lock, then delete what was evicted outside it. */
export async function trimStore(cacheRoot, maxBytes, lock) {
  await lock(() => evictLeastRecentlyUsed(cacheRoot, maxBytes));
  purgeTrash(cacheRoot);
}

/** Live leases on an entry. A dead process's lease is removed as stale; one that cannot be read counts as live. */
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
    let lease;
    try {
      lease = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      live += 1;
      continue;
    }
    if (lease && Number.isInteger(lease.pid) && isProcessAlive(lease.pid, lease.start)) live += 1;
    else if (lease && Number.isInteger(lease.pid)) fs.rmSync(file, { force: true });
    else live += 1;
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
 * the lock; the caller removes the trash afterwards with `purgeTrash`, outside it. An entry with no
 * readable `meta.json` is damaged and goes first. Returns the evicted keys.
 * `beforeQuarantine(key)` is a test seam, called between the lease check and the move.
 */
export function evictLeastRecentlyUsed(cacheRoot, maxBytes, { now = Date.now(), beforeQuarantine } = {}) {
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
    if (beforeQuarantine) beforeQuarantine(entry.key);
    try {
      fs.renameSync(entry.full, path.join(cacheRoot, `.trash-${entry.key}-${crypto.randomBytes(4).toString('hex')}`));
    } catch {
      continue;
    }
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

/** Does the file hold any of `needles`? Files over SCAN_CHUNK_BYTES are read in chunks overlapping by the longest needle. */
function fileContains(file, needles) {
  const size = fs.statSync(file).size;
  if (size <= SCAN_CHUNK_BYTES) {
    const content = fs.readFileSync(file);
    return needles.some((n) => content.includes(n));
  }
  const overlap = Math.max(...needles.map((n) => n.length)) - 1;
  const buf = Buffer.alloc(SCAN_CHUNK_BYTES + overlap);
  const fd = fs.openSync(file, 'r');
  try {
    let carry = 0;
    for (let pos = 0; pos < size; ) {
      const n = fs.readSync(fd, buf, carry, SCAN_CHUNK_BYTES, pos);
      if (n === 0) break;
      const window = buf.subarray(0, carry + n);
      if (needles.some((needle) => window.includes(needle))) return true;
      carry = Math.min(overlap, window.length);
      window.copy(buf, 0, window.length - carry);
      pos += n;
    }
  } finally {
    fs.closeSync(fd);
  }
  return false;
}

/**
 * The first file under `tree` (relative path) that holds one of `needles` (absolute paths) as bytes: a
 * symlink target, or ANY regular file at all (no size cap). Such a tree only works where it was
 * installed, so it must not be cached. Null when clean.
 */
export function findInstallPathReference(tree, needles) {
  const bufs = needles.map((n) => Buffer.from(n, 'utf8'));
  const walk = (abs, rel) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      const p = path.join(abs, ent.name);
      const r = rel ? `${rel}/${ent.name}` : ent.name;
      if (ent.isDirectory()) {
        const found = walk(p, r);
        if (found) return found;
      } else if (ent.isSymbolicLink()) {
        const target = Buffer.from(fs.readlinkSync(p), 'utf8');
        if (bufs.some((b) => target.includes(b))) return r;
      } else if (ent.isFile() && fileContains(p, bufs)) {
        return r;
      }
    }
    return null;
  };
  return walk(tree, '');
}

/**
 * Everything under `dir` except `node_modules` trees, `.git` included, as path -> "type:size:mtimeMs".
 * Taken before and after an install: any difference means a lifecycle script wrote outside `node_modules`,
 * which a cached `node_modules` could never reproduce.
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
