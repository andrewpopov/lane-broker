import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { isProcessAlive, processStartTime } from './process-liveness.js';

/**
 * BRAIN-389: a content-addressed store of installed `node_modules` trees on a
 * remote runner, so a `remoteDeps` dir whose lockfile/environment has been
 * seen before gets a copy of an installed tree instead of running `npm ci`.
 *
 *   <cacheRoot>/<key>/node_modules   the tree, every file and dir read-only
 *   <cacheRoot>/<key>/meta.json      { bytes, createdAt, relocation? } (relocation: see `collectInstallPathReferences`)
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
const KEY_VERSION = 'v4';

/** npm reads npm-shrinkwrap.json in preference to package-lock.json. */
const LOCKFILES = ['npm-shrinkwrap.json', 'package-lock.json'];

/** npm_config_* values that name a file whose BYTES also shape an install. */
const NPM_CONFIG_FILE_ENVS = ['npm_config_userconfig', 'npm_config_globalconfig'];

/**
 * The deps environment is hashed whole (an install can read any variable: PATH, CC, HOME, proxy
 * settings...), so the variables that differ on every run without meaning anything to an install are
 * REMOVED from it before npm or any script sees them (`scrubDepsEnv`), not merely left out of the key:
 * a variable that reaches the install but not the key would let two different installs share an entry.
 * Removed: shell bookkeeping, git's ceiling (the work dir has its own `.git`), and lane's own ticket/id
 * variables. A few tools do need a temp dir, so TMPDIR/TMP/TEMP stay in the environment, are
 * not hashed, and their (per-run) paths join the relocatability scan instead.
 */
export const SCRUBBED_ENV_NAMES = new Set(['PWD', 'OLDPWD', 'SHLVL', '_', 'GIT_CEILING_DIRECTORIES']);
export const SCRUBBED_ENV_PREFIXES = ['LANE_'];

/**
 * Authentication inputs reach the install (a git+ssh dependency needs SSH_AUTH_SOCK, GIT_SSH or GIT_SSH_COMMAND)
 * but are not hashed. That is safe because a git dependency is cached only when pinned to a 40-hex commit
 * (`lockEligibility`): credentials decide whether the install SUCCEEDS, never what it installs.
 */
export const AUTH_ENV_NAMES = new Set(['SSH_AUTH_SOCK', 'SSH_AGENT_PID', 'GIT_SSH', 'GIT_SSH_COMMAND']);

/** Per-connection ssh session info, different on every run and read by no install. Any OTHER `SSH_*` variable is hashed. */
export const SESSION_ENV_NAMES = new Set(['SSH_CONNECTION', 'SSH_CLIENT', 'SSH_TTY']);
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

/** Mount flags that change what a process can do in a directory; the rest (atime policy, ...) is irrelevant. */
const BEHAVIOUR_FLAGS = ['ro', 'noexec', 'nosuid', 'nodev'];

const unescapeMountPath = (p) => p.replace(/\\([0-7]{3})/g, (_, o) => String.fromCharCode(parseInt(o, 8)));
const coversPath = (mountPoint, target) => mountPoint === '/' || target === mountPoint || target.startsWith(`${mountPoint}/`);

function describeMount(fstype, options) {
  const flags = options.map((o) => (o === 'read-only' ? 'ro' : o)).filter((o) => BEHAVIOUR_FLAGS.includes(o)).sort();
  return `${fstype}|${flags.join(',')}`;
}

/** The fs type and behaviour flags of the Linux mount covering `target`, from `/proc/self/mountinfo` text. */
export function parseMountInfo(text, target) {
  let best = null;
  for (const line of text.split('\n')) {
    const [pre, post] = line.split(' - ');
    if (!post) continue;
    const fields = pre.split(' ');
    const mountPoint = unescapeMountPath(fields[4] ?? '');
    if (!coversPath(mountPoint, target) || (best && mountPoint.length < best.mountPoint.length)) continue;
    const superOptions = (post.split(' ')[2] ?? '').split(',');
    best = { mountPoint, desc: describeMount(post.split(' ')[0], [...(fields[5] ?? '').split(','), ...superOptions]) };
  }
  return best?.desc ?? null;
}

/** Same for macOS `mount` output lines: `/dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled)`. */
export function parseMountOutput(text, target) {
  let best = null;
  for (const line of text.split('\n')) {
    const m = line.match(/^.+? on (.+?) \((.*)\)$/);
    if (!m || !coversPath(m[1], target) || (best && m[1].length < best.mountPoint.length)) continue;
    const [fstype, ...options] = m[2].split(',').map((x) => x.trim());
    best = { mountPoint: m[1], desc: describeMount(fstype, options) };
  }
  return best?.desc ?? null;
}

/**
 * The filesystem properties of each temp dir the install sees (TMPDIR, TMP, TEMP): fs type and
 * noexec/nosuid/nodev/ro. The paths themselves differ per run and stay out of the key, but whether a
 * build can execute from its temp dir does not. Unreadable is a value ('unknown'), never a crash.
 */
export function tempDirFsProperties(env, { readMounts = defaultReadMounts, platform = process.platform } = {}) {
  const describe = (dir) => {
    try {
      const real = fs.realpathSync(dir);
      const mounts = readMounts();
      return (platform === 'darwin' ? parseMountOutput(mounts, real) : parseMountInfo(mounts, real)) ?? 'unknown';
    } catch {
      return 'unknown';
    }
  };
  // By variable NAME, so swapping which variable points at which filesystem changes the fingerprint.
  const parts = PER_RUN_PATH_ENV_NAMES.map((name) => `${name}=${env[name] ? describe(env[name]) : 'unset'}`);
  // What a tool that ignores the variables falls back to (node's os.tmpdir() order, evaluated on the install's env).
  if (!env.TMPDIR) parts.push(`os.tmpdir=${describe(env.TMP || env.TEMP || '/tmp')}`);
  return parts.join(';');
}

function defaultReadMounts() {
  return process.platform === 'darwin' ? execFileSync('mount', { encoding: 'utf8' }) : fs.readFileSync('/proc/self/mountinfo', 'utf8');
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
 *  - the effective npm `ignore-scripts` and `script-shell` (`npmConfig`, resolved by npm itself in the
 *    install's env and cwd), and the filesystem properties of the temp dirs (`tempFsProps`);
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
  npmConfig = { ignoreScripts: 'false', scriptShell: 'null' },
  tempFsProps = () => tempDirFsProperties(env),
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
    ['npm-ignore-scripts', npmConfig.ignoreScripts],
    ['npm-script-shell', npmConfig.scriptShell],
    ['temp-fs', tempFsProps()],
    ['install-argv', JSON.stringify(installArgv)],
  ];
  if (eligibility.hasInstallScript) parts.push(['cc-version', toolVersion('cc')], ['python3-version', toolVersion('python3')]);
  for (const rel of eligibility.tarballs) {
    const bytes = readIfExists(path.resolve(dir, rel));
    if (!bytes) return { key: null, reason: `file: tarball ${rel} is not in the snapshot` };
    parts.push([`tarball:${rel}`, bytes]);
  }
  const unhashed = (name) =>
    isScrubbed(name) || PER_RUN_PATH_ENV_NAMES.includes(name) || AUTH_ENV_NAMES.has(name) || SESSION_ENV_NAMES.has(name);
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

function platformSkips(entry, { platform, arch, libc }) {
  if (!listAllows(entry.os, platform) || !listAllows(entry.cpu, arch)) return true;
  return libc !== undefined && !listAllows(entry.libc, libc);
}

/** Names a lock entry asks npm to resolve; the root also lists its devDependencies, other entries' are never installed. */
function requestedNames(path, entry) {
  const peers = Object.keys(entry.peerDependencies ?? {}).filter((n) => !entry.peerDependenciesMeta?.[n]?.optional);
  const dev = path === '' ? Object.keys(entry.devDependencies ?? {}) : [];
  return [...Object.keys(entry.dependencies ?? {}), ...Object.keys(entry.optionalDependencies ?? {}), ...peers, ...dev];
}

/** The lock path node's resolution (own `node_modules`, then each parent's, up to the root) gives `name` from `fromPath`. */
function resolveLockPath(packages, fromPath, name) {
  let base = fromPath;
  for (;;) {
    const candidate = `${base ? `${base}/` : ''}node_modules/${name}`;
    if (candidate in packages) return candidate;
    if (base === '') return null;
    const cut = base.lastIndexOf('/node_modules/');
    base = cut < 0 ? '' : base.slice(0, cut);
  }
}

/** lock path -> the lock paths whose dependency lists resolve to it. */
function dependentsByPath(packages) {
  const dependents = new Map();
  for (const [from, entry] of Object.entries(packages)) {
    if (!entry) continue;
    for (const name of requestedNames(from, entry)) {
      const target = resolveLockPath(packages, from, name);
      if (target === null) continue;
      if (!dependents.has(target)) dependents.set(target, new Set());
      dependents.get(target).add(from);
    }
  }
  return dependents;
}

/**
 * Lock entries that apply to this platform (honouring `os`, `cpu` and `libc`) but are absent from
 * `installed`, npm's own record (`node_modules/.package-lock.json`'s `packages`) of what it put on disk.
 * npm skips an optional dependency it cannot install without failing; a tree missing one must not be cached.
 * An absent optional entry is still expected when everything that depends on it is itself skipped for this
 * platform or expected-absent (the transitive dependencies of a platform-skipped optional package, which
 * npm never fetches), so those are found as a fixpoint grown from the platform-skipped set.
 */
export function findMissingInstalled(lock, installed, system) {
  const { packages } = lock;
  const gone = new Set(); // platform-skipped, then expected-absent
  const absent = [];
  for (const [name, entry] of Object.entries(packages)) {
    if (name === '' || !entry) continue;
    if (platformSkips(entry, system)) gone.add(name);
    else if (!(name in installed)) absent.push(name);
  }
  const dependents = dependentsByPath(packages);
  for (let grew = true; grew; ) {
    grew = false;
    for (const name of absent) {
      if (gone.has(name) || !packages[name].optional) continue;
      const parents = [...(dependents.get(name) ?? [])];
      if (parents.length > 0 && parents.every((d) => gone.has(d))) {
        gone.add(name);
        grew = true;
      }
    }
  }
  return absent.filter((name) => !gone.has(name)).map((name) => name.slice('node_modules/'.length)).sort();
}

export function entryDir(cacheRoot, key) {
  return path.join(cacheRoot, key);
}

export function entryTree(cacheRoot, key) {
  return path.join(entryDir(cacheRoot, key), 'node_modules');
}

/**
 * Recreate `src` as `dst`, directory by directory: real directories, symlinks re-created with their
 * target text (through `linkTarget(target, rel)`, identity by default), regular files handed to
 * `onFile(srcFile, dstFile, rel)`. Each directory is created owner-writable so its entries can be added,
 * then given `dirMode(srcMode)` once it is complete.
 */
function cloneTree(src, dst, hooks, rel = '') {
  const { onFile, dirMode, linkTarget = (target) => target } = hooks;
  const srcMode = fs.statSync(src).mode & 0o7777;
  fs.mkdirSync(dst, { mode: 0o700 });
  for (const ent of fs.readdirSync(src, { withFileTypes: true })) {
    const s = path.join(src, ent.name);
    const d = path.join(dst, ent.name);
    const r = rel ? `${rel}/${ent.name}` : ent.name;
    if (ent.isDirectory()) cloneTree(s, d, hooks, r);
    else if (ent.isSymbolicLink()) fs.symlinkSync(linkTarget(fs.readlinkSync(s), r), d);
    else if (ent.isFile()) onFile(s, d, r);
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
 * run published the same key first (that run's tree is equally valid; ours is discarded). `relocation` is what
 * `collectInstallPathReferences` found: the stored COPY of each recorded file or symlink has its install paths
 * replaced by placeholder tokens (the source tree is untouched), and the record goes into `meta.json`.
 */
export function publishToStore(cacheRoot, key, srcTree, relocation = null) {
  fs.mkdirSync(cacheRoot, { recursive: true });
  const tmp = path.join(cacheRoot, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  fs.mkdirSync(tmp);
  try {
    let bytes = 0;
    const pairs = relocation ? placeholderPairs(relocation) : [];
    const recorded = new Map((relocation?.entries ?? []).map((e) => [e.relPath, e.kind]));
    const anywhere = () => true;
    cloneTree(srcTree, path.join(tmp, 'node_modules'), {
      onFile: (s, d, rel) => {
        if (recorded.get(rel) !== 'text') {
          bytes += copyFileAs(s, d, (mode) => mode & 0o7555);
          return;
        }
        const templated = rewritePrefixes(fs.readFileSync(s), pairs, anywhere);
        fs.writeFileSync(d, templated);
        fs.chmodSync(d, fs.statSync(s).mode & 0o7555);
        bytes += templated.length;
      },
      linkTarget: (target, rel) => (recorded.get(rel) === 'symlink' ? rewritePrefixes(Buffer.from(target, 'utf8'), pairs, anywhere).toString('utf8') : target),
      dirMode: (mode) => mode & 0o7555,
    });
    fs.writeFileSync(path.join(tmp, 'meta.json'), JSON.stringify({ bytes, createdAt: Date.now(), ...(relocation ? { relocation } : {}) }));
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

/**
 * Call `visit(window)` over `file` in windows of at most SCAN_CHUNK_BYTES plus `overlap` carried-over bytes, so
 * a sequence of up to `overlap + 1` bytes straddling a chunk boundary is seen whole. `visit` returning true stops.
 */
function forEachWindow(file, overlap, visit) {
  const size = fs.statSync(file).size;
  if (size <= SCAN_CHUNK_BYTES) return visit(fs.readFileSync(file));
  const buf = Buffer.alloc(SCAN_CHUNK_BYTES + overlap);
  const fd = fs.openSync(file, 'r');
  try {
    let carry = 0;
    for (let pos = 0; pos < size; ) {
      const n = fs.readSync(fd, buf, carry, SCAN_CHUNK_BYTES, pos);
      if (n === 0) break;
      const window = buf.subarray(0, carry + n);
      if (visit(window)) return true;
      carry = Math.min(overlap, window.length);
      window.copy(buf, 0, window.length - carry);
      pos += n;
    }
  } finally {
    fs.closeSync(fd);
  }
  return false;
}

const longest = (needles) => Math.max(...needles.map((n) => n.length)) - 1;

/** Does the file hold any of `needles`? */
function fileContains(file, needles) {
  return forEachWindow(file, longest(needles), (window) => needles.some((n) => window.includes(n)));
}

/** Which of `needles` (by index) the file holds, and whether it has a NUL byte anywhere (so is not text). The whole file is read. */
function fileNeedleHits(file, needles) {
  const hits = new Set();
  let binary = false;
  forEachWindow(file, longest(needles), (window) => {
    needles.forEach((n, i) => {
      if (window.includes(n)) hits.add(i);
    });
    if (window.includes(0)) binary = true;
    return false;
  });
  return { hits, binary };
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
 * The places an install path can be named in a run: the work dir, its real path and each per-run temp dir, as
 * `{ role, path }`. A restore maps each recorded role onto the same role of the NEW run.
 */
export function installPathRoles(workDir, env) {
  return [
    { role: 'workDir', path: workDir },
    { role: 'realWorkDir', path: fs.realpathSync(workDir) },
    ...PER_RUN_PATH_ENV_NAMES.filter((name) => env[name]).map((name) => ({ role: name, path: env[name] })),
  ];
}

/*
 * Relocation, as conda relocates an install prefix, decided ONCE at store time against the original tree (where
 * the true install path is known): every occurrence of an install path in a recorded text file or symlink target
 * becomes a placeholder token `@@LANE_PREFIX_<roles>_<nonce>@@` in the stored copy, and a restore only substitutes
 * tokens. Anything that cannot be decided with certainty makes the tree non-relocatable instead.
 */
const TOKEN_MARKER = '@@LANE_PREFIX_';

/** Extensions of files that are rewritable text (and `.bin` scripts below); any other file naming a path is `binary`. */
const REWRITABLE_EXTENSIONS = new Set(['.js', '.cjs', '.mjs', '.ts', '.json', '.map', '.prisma', '.txt', '.md', '.sh', '.yml', '.yaml']);

const UTF8 = new TextDecoder('utf-8', { fatal: true });

/** Bytes that may follow an install path: `/ " ' \` ) ] } , ; : \ =`, whitespace, or the end. Anything else might be a longer name. */
const PATH_TERMINATORS = new Set(Buffer.from('/"\'`)]},;:\\= \t\n\r\v\f', 'latin1'));

/** Bytes that make a preceding path glued to a longer name: ASCII name characters and anything non-ASCII. */
const isNameByte = (b) => (b >= 0x30 && b <= 0x39) || (b >= 0x41 && b <= 0x5a) || (b >= 0x61 && b <= 0x7a) || '_-.+~@%'.includes(String.fromCharCode(b)) || b >= 0x80;

const strictOccurrence = (buf, start, end) => (start === 0 || !isNameByte(buf[start - 1])) && (end === buf.length || PATH_TERMINATORS.has(buf[end]));

const tokenFor = (roles, nonce) => Buffer.from(`${TOKEN_MARKER}${roles.join('+')}_${nonce}@@`);
const tokenTail = (nonce) => Buffer.from(`_${nonce}@@`);
const placeholderPairs = ({ nonce, prefixes }) => prefixes.map(({ roles, path: p }) => ({ from: Buffer.from(p, 'utf8'), to: tokenFor(roles, nonce) }));

/**
 * Replace each occurrence of a `from` in `buf` by its `to`, as bytes, in one left-to-right pass (replaced text is
 * never rescanned), longest `from` first where two start at the same byte. Each occurrence must satisfy
 * `accept(buf, start, end)`; the first that does not makes the result null.
 */
function rewritePrefixes(buf, pairs, accept) {
  const ordered = [...pairs].sort((a, b) => b.from.length - a.from.length);
  const parts = [];
  let last = 0;
  for (;;) {
    let best = null;
    let bestIdx = -1;
    for (const pair of ordered) {
      const idx = buf.indexOf(pair.from, last);
      if (idx >= 0 && (bestIdx < 0 || idx < bestIdx)) {
        best = pair;
        bestIdx = idx;
      }
    }
    if (!best) break;
    const end = bestIdx + best.from.length;
    if (!accept(buf, bestIdx, end)) return null;
    parts.push(buf.subarray(last, bestIdx), best.to);
    last = end;
  }
  parts.push(buf.subarray(last));
  return Buffer.concat(parts);
}

/** Is this file, which names an install path, safe to rewrite as text? `.bin` shims are extensionless scripts. */
function isRewritableText(relPath, content) {
  const ext = path.extname(relPath).toLowerCase();
  const isBinShim = ext === '' && relPath.startsWith('.bin/') && content.subarray(0, 2).toString('latin1') === '#!';
  if (!REWRITABLE_EXTENSIONS.has(ext) && !isBinShim) return false;
  if (content.includes(0)) return false;
  try {
    UTF8.decode(content);
    return true;
  } catch {
    return false;
  }
}

/**
 * Every file or symlink under `tree` that names an install path in `roles` (see `installPathRoles`). Returns
 *   { binary: <rel> }      a file naming a path that is not allowlisted, valid UTF-8 text: refuse the tree;
 *   { ambiguous: <rel> }   an occurrence not followed by a path terminator (or glued to a longer name before it): refuse;
 *   { relocation }         else: `{ nonce, prefixes: [{ roles, path }], entries: [{ relPath, kind: 'text' | 'symlink' }] }`,
 *                          roles sharing one path being ONE group; `publishToStore` templates the entries.
 * The nonce (a fresh one if the tree already contains the token text) names this entry's placeholder tokens.
 */
export function collectInstallPathReferences(tree, roles, { newNonce = () => crypto.randomBytes(16).toString('hex') } = {}) {
  const groups = [];
  for (const { role, path: p } of roles) {
    const group = groups.find((g) => g.path === p);
    if (group) group.roles.push(role);
    else groups.push({ roles: [role], path: p });
  }
  const pairs = groups.map((g) => ({ from: Buffer.from(g.path, 'utf8'), to: Buffer.alloc(0) }));
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const nonce = newNonce();
    const marker = Buffer.from(`${TOKEN_MARKER}${nonce}`);
    const outcome = scanTree(tree, groups, pairs, marker);
    if (outcome.collision) continue;
    if (outcome.binary) return { binary: outcome.binary };
    if (outcome.ambiguous) return { ambiguous: outcome.ambiguous };
    const found = groups.filter((_, i) => outcome.found.has(i));
    return { relocation: { nonce, prefixes: found.map(({ roles: r, path: p }) => ({ roles: r, path: p })), entries: outcome.entries } };
  }
  return { ambiguous: TOKEN_MARKER };
}

function scanTree(tree, groups, pairs, marker) {
  const bufs = [...pairs.map((p) => p.from), marker];
  const markerIdx = bufs.length - 1;
  const found = new Set();
  const entries = [];
  const walk = (abs, rel) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      const p = path.join(abs, ent.name);
      const r = rel ? `${rel}/${ent.name}` : ent.name;
      let outcome = null;
      if (ent.isDirectory()) outcome = walk(p, r);
      else if (ent.isSymbolicLink()) {
        const target = Buffer.from(fs.readlinkSync(p), 'utf8');
        if (target.includes(marker)) return { collision: true };
        const hit = pairs.some((pr) => target.includes(pr.from));
        if (hit && rewritePrefixes(target, pairs, strictOccurrence) === null) return { ambiguous: r };
        if (hit) {
          pairs.forEach((pr, i) => target.includes(pr.from) && found.add(i));
          entries.push({ relPath: r, kind: 'symlink' });
        }
      } else if (ent.isFile()) {
        const { hits } = fileNeedleHits(p, bufs);
        if (hits.has(markerIdx)) return { collision: true };
        if (hits.size === 0) continue;
        const content = fs.readFileSync(p);
        if (!isRewritableText(r, content)) return { binary: r };
        if (rewritePrefixes(content, pairs, strictOccurrence) === null) return { ambiguous: r };
        hits.forEach((i) => found.add(i));
        entries.push({ relPath: r, kind: 'text' });
      }
      if (outcome) return outcome;
    }
    return null;
  };
  return walk(tree, '') ?? { found, entries };
}

/** The relocation record of a stored entry: empty when the tree names no install path. Throws on a malformed record. */
export function readRelocation(cacheRoot, key) {
  const meta = JSON.parse(fs.readFileSync(path.join(entryDir(cacheRoot, key), 'meta.json'), 'utf8'));
  const rec = meta.relocation ?? { nonce: '0'.repeat(32), prefixes: [], entries: [] };
  const okPrefix = (p) => p && Array.isArray(p.roles) && p.roles.length > 0 && p.roles.every((r) => typeof r === 'string') && typeof p.path === 'string' && p.path.length > 0;
  const okEntry = (e) => e && typeof e.relPath === 'string' && (e.kind === 'text' || e.kind === 'symlink');
  if (!/^[0-9a-f]{32}$/.test(rec.nonce) || !Array.isArray(rec.prefixes) || !Array.isArray(rec.entries) || !rec.prefixes.every(okPrefix) || !rec.entries.every(okEntry)) {
    throw new Error('relocation record is malformed');
  }
  return rec;
}

/**
 * Make a restored tree name the NEW run's install paths: substitute each recorded entry's placeholder tokens with
 * the new path of the token's role (exact bytes, via a temp file renamed over the original with its mode kept; a
 * symlink is re-created with the substituted target), then verify that no token of this entry's nonce is left in
 * any recorded entry. Throws on any failure (roles that now differ, a missing role, an entry with no token, a
 * leftover token); the caller discards the tree.
 */
export function relocateRestoredTree(tree, { nonce, prefixes, entries }, newPaths) {
  const pairs = prefixes.map(({ roles }) => {
    const targets = roles.map((role) => {
      if (!newPaths[role]) throw new Error(`relocation: no new path for ${role}`);
      return newPaths[role];
    });
    if (new Set(targets).size > 1) throw new Error(`relocation: ambiguous roles ${roles.join('+')}`);
    return { from: tokenFor(roles, nonce), to: Buffer.from(targets[0], 'utf8') };
  });
  const anywhere = () => true;
  const tail = tokenTail(nonce);

  for (const { relPath, kind } of entries) {
    const file = path.join(tree, relPath);
    if (kind === 'symlink') {
      const target = Buffer.from(fs.readlinkSync(file), 'utf8');
      if (!target.includes(tail)) throw new Error(`relocation: ${relPath} holds no placeholder`);
      fs.rmSync(file);
      fs.symlinkSync(rewritePrefixes(target, pairs, anywhere).toString('utf8'), file);
    } else {
      const content = fs.readFileSync(file);
      if (!content.includes(tail)) throw new Error(`relocation: ${relPath} holds no placeholder`);
      const mode = fs.statSync(file).mode & 0o7777;
      const tmp = `${file}.relocate-${crypto.randomBytes(4).toString('hex')}`;
      fs.writeFileSync(tmp, rewritePrefixes(content, pairs, anywhere));
      fs.chmodSync(tmp, mode);
      fs.renameSync(tmp, file);
    }
  }

  for (const { relPath, kind } of entries) {
    const file = path.join(tree, relPath);
    const content = kind === 'symlink' ? Buffer.from(fs.readlinkSync(file), 'utf8') : fs.readFileSync(file);
    if (content.includes(tail)) throw new Error(`relocation: ${relPath} still holds a placeholder`);
  }
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
