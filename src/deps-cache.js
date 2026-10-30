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
const KEY_VERSION = 'v6';

/** npm reads npm-shrinkwrap.json in preference to package-lock.json. */
const LOCKFILES = ['npm-shrinkwrap.json', 'package-lock.json'];

/** npm_config_* values that name a file whose BYTES also shape an install. */
const NPM_CONFIG_FILE_ENVS = ['npm_config_userconfig', 'npm_config_globalconfig'];

/**
 * The deps environment is hashed whole (an install can read any variable: PATH, CC, HOME, proxy
 * settings...), so the variables that differ on every run without meaning anything to an install are
 * REMOVED from it before npm or any script sees them (`scrubDepsEnv`), not merely left out of the key:
 * a variable that reaches the install but not the key would let two different installs share an entry.
 * Removed: shell bookkeeping, git's ceiling (the work dir has its own `.git`), lane's own ticket/id
 * variables, and what systemd/logind stamp on every invocation or login session (`INVOCATION_ID`,
 * `XDG_SESSION_ID`, `MEMORY_PRESSURE_*`...), which made every run's key different (BRAIN-423). A few tools do need a temp dir, so TMPDIR/TMP/TEMP stay in the environment, are
 * not hashed, and their (per-run) paths join the relocatability scan instead.
 */
export const SCRUBBED_ENV_NAMES = new Set(['PWD', 'OLDPWD', 'SHLVL', '_', 'GIT_CEILING_DIRECTORIES', 'INVOCATION_ID', 'JOURNAL_STREAM', 'SYSTEMD_EXEC_PID', 'NOTIFY_SOCKET']);
export const SCRUBBED_ENV_PREFIXES = ['LANE_', 'XDG_SESSION_', 'MEMORY_PRESSURE_', 'LISTEN_'];

/**
 * Authentication inputs reach the install (a git+ssh dependency needs SSH_AUTH_SOCK, GIT_SSH or GIT_SSH_COMMAND)
 * but are not hashed. That is safe because a git dependency is cached only when pinned to a 40-hex commit
 * (`lockEligibility`): credentials decide whether the install SUCCEEDS, never what it installs.
 */
export const AUTH_ENV_NAMES = new Set(['SSH_AUTH_SOCK', 'SSH_AGENT_PID', 'GIT_SSH', 'GIT_SSH_COMMAND']);

/** Per-connection ssh session info, different on every run and read by no install. Any OTHER `SSH_*` variable is hashed. */
export const SESSION_ENV_NAMES = new Set(['SSH_CONNECTION', 'SSH_CLIENT', 'SSH_TTY']);
export const PER_RUN_PATH_ENV_NAMES = ['TMPDIR', 'TMP', 'TEMP'];

/** BRAIN-597: the documented executor contract (README "Environment a lane command sees"): reaches the install, and is hashed with the rest of the env. */
const EXECUTOR_CONTRACT_ENV_NAMES = new Set(['LANE_BROKER_EXECUTOR', 'LANE_BROKER_RUNNER']);

const isScrubbed = (name) => !EXECUTOR_CONTRACT_ENV_NAMES.has(name) && (SCRUBBED_ENV_NAMES.has(name) || SCRUBBED_ENV_PREFIXES.some((p) => name.startsWith(p)));

const isLocaleName = (name) => name === 'LANG' || name.startsWith('LC_');

let installLocaleMemo = null;
/** `C.UTF-8` where the host has it (glibc spells it `C.utf8` in `locale -a`), else plain `C`; decided once per process. */
export function resolveInstallLocale() {
  if (installLocaleMemo) return installLocaleMemo;
  let available = '';
  try {
    available = execFileSync('locale', ['-a'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
  } catch {
    // no `locale` binary: fall through to C
  }
  installLocaleMemo = available.split('\n').some((l) => /^C\.utf-?8$/i.test(l.trim())) ? 'C.UTF-8' : 'C';
  return installLocaleMemo;
}

/**
 * The install runs under ONE locale whatever the submitter's shell had (BRAIN-429): every `LC_*` and `LANG` is
 * removed and `LC_ALL` is set, so two submitters with different locales share a key. `computeDepsKey` hashes the
 * pinned value as `install-locale` instead of hashing the locale variables. The user's command keeps its own env.
 */
export function pinInstallLocale(env, locale = resolveInstallLocale()) {
  const out = Object.fromEntries(Object.entries(env).filter(([name]) => !isLocaleName(name)));
  out.LC_ALL = locale;
  return out;
}

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

/**
 * BRAIN-466: npm workspaces. A lock `link: true` entry is a workspace (or in-repo `file:` dir) symlink that `npm ci`
 * creates in `node_modules`. The store never holds those symlinks or the workspace sources: it holds only installed
 * packages, with each workspace's own nested `node_modules` stashed under this reserved name (a dot-dir, so never
 * a package) inside the stored tree. A restore moves each back next to its workspace and recreates the links.
 */
const WORKSPACE_STASH = '.lane-workspaces';

/** The stash directories above a workspace's stored `node_modules`, deepest first, that exist (so they can be made read-only like the rest). */
function stashParents(storedTree, resolved) {
  const dirs = [];
  for (let rel = path.posix.join(WORKSPACE_STASH, resolved); rel !== '.'; rel = path.posix.dirname(rel)) {
    if (fs.existsSync(path.join(storedTree, rel))) dirs.push(path.join(storedTree, rel));
  }
  return dirs;
}

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

/** A workspace path npm writes and this cache can trust: relative, canonical, POSIX, no empty/`.`/`..`/`node_modules` segment. */
function isPlainWorkspacePath(p) {
  if (p === '' || p.includes('\\') || p.includes('\0') || p.startsWith('/') || p.endsWith('/')) return false;
  return p.split('/').every((seg) => seg !== '' && seg !== '.' && seg !== '..' && seg !== 'node_modules');
}

/**
 * Throws unless `<baseDir>/<rel>` lies inside `baseDir` in the real filesystem: no component between them may be a
 * symlink, and what exists must resolve under `realpath(baseDir)`. Every read, copy, move or delete of a workspace
 * path goes through this first. A component that does not exist yet ends the walk (nothing there to follow).
 */
export function assertContained(baseDir, rel) {
  if (!isPlainWorkspacePath(rel)) throw new Error(`workspace path ${JSON.stringify(rel)} is not a plain relative path`);
  let current = baseDir;
  for (const seg of rel.split('/')) {
    current = path.join(current, seg);
    let st;
    try {
      st = fs.lstatSync(current);
    } catch (err) {
      if (err.code === 'ENOENT') return;
      throw err;
    }
    if (st.isSymbolicLink()) throw new Error(`workspace path ${rel} passes through a symlink at ${seg}`);
  }
  const inside = path.relative(fs.realpathSync(baseDir), fs.realpathSync(current));
  if (inside.startsWith('..') || path.isAbsolute(inside)) throw new Error(`workspace path ${rel} resolves outside the work dir`);
}

/**
 * Whether every lock entry is pinned to content, so `npm ci` from this lock is reproducible. Fails closed:
 * lock v2/v3 with a `packages` map only, and every non-root entry must be an npmjs.org tarball with
 * integrity, a git commit pin, or a `file:*.tgz` with integrity (returned in `tarballs`, which the key
 * hashes). `file:` dirs and unresolved entries are refused. A `link: true` entry is accepted only when it is a
 * plain relative path inside the repo whose target has a lock entry of its own (an npm workspace); those come
 * back in `workspaces` as `{ resolved, links }`. A link that escapes the repo, or a workspace entry no link
 * names, is refused.
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
  const linkTargets = Object.values(packages).filter((e) => e?.link === true && typeof e.resolved === 'string').map((e) => e.resolved);
  const workspaceLinks = new Map(); // workspace dir -> names (under node_modules) of the links to it

  /** Why a link entry is refused; null when it is a plain relative link to an in-repo dir that has its own lock entry. */
  const linkProblem = (name, entry) => {
    const { resolved } = entry;
    if (!/^node_modules\/(@[^/]+\/)?[^/]+$/.test(name)) return `lock entry ${name} is a link that is not a top-level package`;
    if (typeof resolved !== 'string' || !isPlainWorkspacePath(resolved)) {
      return `lock entry ${name} is a link to ${String(resolved).slice(0, 80)}, which is not a plain relative path inside the repo`;
    }
    if (linkTargets.some((other) => other !== resolved && other.toLowerCase() === resolved.toLowerCase())) return `lock entry ${name} is a link to ${resolved}, which differs from another workspace path only by case`;
    const target = packages[resolved];
    if (!target || typeof target !== 'object' || target.link === true || typeof target.resolved === 'string') return `lock entry ${name} is a link to ${resolved}, which has no workspace entry in the lock`;
    return null;
  };
  for (const [name, entry] of Object.entries(packages)) {
    if (entry?.link === true && linkProblem(name, entry) === null) {
      workspaceLinks.set(entry.resolved, [...(workspaceLinks.get(entry.resolved) ?? []), name.slice('node_modules/'.length)]);
    }
  }
  const owningWorkspace = (name) => [...workspaceLinks.keys()].find((dir) => name.startsWith(`${dir}/node_modules/`));

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
    if (entry.link === true) return linkProblem(name, entry);
    if (workspaceLinks.has(name)) return null; // the workspace package itself: its dependency fields only, no source in the lock
    const owner = owningWorkspace(name);
    if (!(owner ? name.slice(owner.length + 1) : name).startsWith('node_modules/')) {
      return `lock entry ${name} is not under node_modules (workspace or local package)`;
    }
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
  const workspaces = [...workspaceLinks].map(([resolved, links]) => ({ resolved, links: links.sort() })).sort((a, b) => a.resolved.localeCompare(b.resolved));
  return { ok: true, tarballs: [...new Set(tarballs)].sort(), hasInstallScript, workspaces };
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

/** JSON with every object's keys sorted, so equal manifests hash equal whatever their key order. */
const canonicalJson = (value) =>
  JSON.stringify(value, (_, v) => (v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, v[k]])) : v));

/** Glob segment -> regex, for the only wildcard supported: `*` within a segment. */
const segmentPattern = (seg) => new RegExp(`^${seg.split('*').map((x) => x.replace(/[.]/g, '\\.')).join('[^/]*')}$`);

/**
 * The directories (relative, sorted, each with a package.json) the root `workspaces` globs select in `dir`, as npm's
 * workspace discovery would. Only plain segments, `*` and `**` are understood; anything else (negation, braces, `?`, classes,
 * the object form, a symlinked entry on the way) throws, and the caller treats the dir as ineligible.
 */
export function discoverWorkspaceDirs(dir, rootPackage) {
  const patterns = rootPackage.workspaces;
  if (!Array.isArray(patterns)) {
    if (patterns === undefined) return [];
    throw new Error('workspaces is not a plain array of patterns');
  }
  const skip = new Set(['node_modules', '.git']);
  const children = (rel) =>
    fs.readdirSync(path.join(dir, rel), { withFileTypes: true }).filter((e) => !skip.has(e.name)).map((e) => {
      if (e.isSymbolicLink()) throw new Error(`workspace discovery crosses a symlink at ${rel ? `${rel}/` : ''}${e.name}`);
      return e;
    }).filter((e) => e.isDirectory()).map((e) => e.name);
  const expand = (segs, rel, out) => {
    if (segs.length === 0) return out.add(rel);
    const [first, ...rest] = segs;
    const join = (name) => (rel ? `${rel}/${name}` : name);
    if (first === '**') {
      expand(rest, rel, out);
      for (const name of children(rel)) expand(segs, join(name), out);
    } else if (!first.includes('*')) {
      if (fs.existsSync(path.join(dir, join(first)))) {
        if (fs.lstatSync(path.join(dir, join(first))).isSymbolicLink()) throw new Error(`workspace discovery crosses a symlink at ${join(first)}`);
        expand(rest, join(first), out);
      }
    } else {
      const re = segmentPattern(first);
      for (const name of children(rel).filter((n) => re.test(n))) expand(rest, join(name), out);
    }
    return out;
  };
  const selected = new Set();
  for (const raw of patterns) {
    if (typeof raw !== 'string') throw new Error('workspaces entry is not a string');
    const pattern = raw.replace(/^\.\//, '').replace(/\/+$/, '');
    if (!/^[A-Za-z0-9_.@\-/*]+$/.test(pattern) || pattern.split('/').some((seg) => seg === '' || seg === '.' || seg === '..' || (seg.includes('**') && seg !== '**'))) {
      throw new Error(`workspaces pattern ${JSON.stringify(raw)} is not one this cache understands`);
    }
    expand(pattern.split('/'), '', selected);
  }
  return [...selected].filter((rel) => rel !== '' && fs.existsSync(path.join(dir, rel, 'package.json'))).sort();
}

/**
 * The key parts for an npm-workspace dir (BRAIN-466), or `{ reason }` when it must not be cached. Anything not fully
 * modelled is refused:
 *  - run whenever the root `package.json` has a `workspaces` field or the lock has links: the workspaces its globs select NOW
 *    must be exactly the lock's links (empty included), with unique names, each link named after its package;
 *  - every workspace path is checked for real containment before anything is read;
 *  - a workspace with a lifecycle script (any npm runs), a `binding.gyp` or a lock `hasInstallScript` is never cached;
 *  - the key covers the discovered set and, per workspace, its path, links, lock entry and its whole package.json (canonical JSON).
 */
function workspaceKeyParts({ dir, lock, rootPackage, workspaces }) {
  const parts = [];
  let discovered;
  try {
    discovered = discoverWorkspaceDirs(dir, rootPackage);
    for (const { resolved } of workspaces) assertContained(dir, resolved);
  } catch (err) {
    return { reason: `workspaces not determined: ${err.message}` };
  }
  const linked = workspaces.map((w) => w.resolved);
  if (JSON.stringify(discovered) !== JSON.stringify(linked)) {
    return { reason: `workspaces in package.json (${discovered.join(', ') || 'none'}) differ from the lock's links (${linked.join(', ') || 'none'})` };
  }
  const names = new Set();
  const identities = [];
  for (const { resolved, links } of workspaces) {
    let manifest;
    try {
      manifest = JSON.parse(fs.readFileSync(path.join(dir, resolved, 'package.json'), 'utf8'));
    } catch {
      return { reason: `workspace ${resolved} has no readable package.json in the snapshot` };
    }
    if (typeof manifest.name !== 'string' || names.has(manifest.name)) return { reason: `workspace ${resolved} has a missing or duplicate package name` };
    names.add(manifest.name);
    if (links.length !== 1 || links[0] !== manifest.name) return { reason: `workspace ${resolved} is linked as ${links.join(', ')}, not as its package name ${manifest.name}` };
    identities.push([resolved, manifest.name]);
    const scripts = manifest.scripts && typeof manifest.scripts === 'object' ? manifest.scripts : {};
    const lifecycle = ROOT_SCRIPT_NAMES.filter((n) => n in scripts);
    if (fs.existsSync(path.join(dir, resolved, 'binding.gyp'))) lifecycle.push('binding.gyp');
    if (lock.packages[resolved]?.hasInstallScript) lifecycle.push('lock hasInstallScript');
    if (lifecycle.length > 0) return { reason: `workspace ${resolved} has lifecycle inputs (${lifecycle.join(', ')})` };
    if ('bin' in manifest) return { reason: `workspace ${resolved} declares bin, whose .bin links are not restored` };
    parts.push([`workspace:${resolved}`, canonicalJson({ links, lock: lock.packages[resolved], manifest })]);
  }
  parts.push(['workspace-set', JSON.stringify(identities)]);
  return { parts };
}

/**
 * Install scripts may read repository sources, so workspace installs with scripts key their bytes too. `roots` are
 * repo-relative paths (files or directories) walked in sorted order; the default `['']` is the whole tree.
 */
function installSourceDigest(rootDir, roots = ['']) {
  const hash = crypto.createHash('sha256');
  const root = fs.realpathSync(rootDir);
  const record = (name, value) => { hash.update(`${name}:${value.length}:`); hash.update(value); };
  const walkChildren = (relative) => {
    for (const name of fs.readdirSync(path.join(root, relative)).sort()) {
      if (name !== 'node_modules' && name !== '.git') visit(relative ? `${relative}/${name}` : name);
    }
  };
  const visit = (rel) => {
    const file = path.join(root, rel);
    const stat = fs.lstatSync(file);
    record(rel, Buffer.from(`${stat.mode & 0o7777}:`));
    if (stat.isSymbolicLink()) {
      const target = fs.realpathSync(file);
      const destination = path.relative(root, target);
      if (destination.startsWith('..') || path.isAbsolute(destination) || destination.split(path.sep).some((part) => part === 'node_modules' || part === '.git')) {
        throw new Error(`install source link ${rel} escapes the keyed source tree`);
      }
      record(`link:${rel}`, Buffer.from(fs.readlinkSync(file)));
    } else if (stat.isDirectory()) {
      walkChildren(rel);
    } else if (stat.isFile()) {
      const contents = crypto.createHash('sha256');
      const fd = fs.openSync(file, 'r');
      try {
        const chunk = Buffer.alloc(SCAN_CHUNK_BYTES);
        let count;
        while ((count = fs.readSync(fd, chunk, 0, chunk.length, null)) > 0) contents.update(chunk.subarray(0, count));
      } finally { fs.closeSync(fd); }
      record(`file:${rel}`, contents.digest());
    } else {
      throw new Error(`unsupported install source ${rel}`);
    }
  };
  for (const start of roots) {
    if (start === '') walkChildren('');
    else visit(start);
  }
  return hash.digest('hex');
}

/** PKG-277: pushes the `key-files` part (the lane's `remoteDepsCacheKeyFiles`, sorted, path names included), or returns the refusal. */
function keyFilesPart({ rootDir, keyFiles, parts }) {
  try {
    for (const rel of keyFiles) {
      assertContained(rootDir, rel);
      if (!fs.existsSync(path.join(rootDir, rel))) return { key: null, reason: `remoteDepsCacheKeyFiles path ${rel} is not in the snapshot` };
    }
    parts.push(['key-files', installSourceDigest(rootDir, [...keyFiles].sort())]);
    return null;
  } catch (error) {
    return { key: null, reason: error.message };
  }
}

/**
 * The cache key for one `remoteDeps` dir: a sha256 over everything that can change what `npm ci` produces
 * there. Returns `{ key, keyParts, lock, scripts }` (`keyParts`: a keyed digest per labelled input, for diagnosis, only when `keyPartsSecret` is given), or `{ key: null, reason }` when the dir must not be cached at all
 * (lockfile not pinned, or a root lifecycle script that is not known to leave `node_modules` alone).
 *
 *  - for npm workspaces (BRAIN-466, `workspaceKeyParts`): the discovered set, and each one's path, links, lock entry and
 *    whole package.json (never its source; a workspace with lifecycle inputs is refused);
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
  keyFiles = [],
  runtime = { version: process.version, platform: process.platform, arch: process.arch },
  system = currentSystem(),
  toolVersion = (cmd) => firstLineOfCommand(cmd, env),
  npmConfig = { ignoreScripts: 'false', scriptShell: 'null' },
  tempFsProps = () => tempDirFsProperties(env),
  keyPartsSecret = null,
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
    ['install-locale', env.LC_ALL ?? ''],
  ];
  if (eligibility.hasInstallScript) parts.push(['cc-version', toolVersion('cc')], ['python3-version', toolVersion('python3')]);
  const isWorkspaceRepo = eligibility.workspaces.length > 0 || 'workspaces' in pkg;
  if (isWorkspaceRepo) {
    const ws = workspaceKeyParts({ dir, lock, rootPackage: pkg, workspaces: eligibility.workspaces });
    if (ws.reason) return { key: null, reason: ws.reason };
    parts.push(...ws.parts);
  }
  // PKG-277: which repo sources a declared-safe root script may have read. Precedence: a workspace repo with install scripts
  // keys the whole tree (over keyFiles); else the lane's keyFiles; else a workspace repo keys the whole tree; else refuse.
  const wholeTree = () => {
    try { parts.push(['install-sources', installSourceDigest(rootDir)]); return null; }
    catch (error) { return { key: null, reason: error.message }; }
  };
  let refused = null;
  if (isWorkspaceRepo && eligibility.hasInstallScript) refused = wholeTree();
  else if (rootScriptsSafe && keyFiles.length > 0) refused = keyFilesPart({ rootDir, keyFiles, parts });
  else if (rootScriptsSafe && isWorkspaceRepo) refused = wholeTree();
  else if (rootScriptsSafe) refused = { key: null, reason: 'remoteDepsCacheRootScriptsSafe on a repo without workspaces needs remoteDepsCacheKeyFiles naming the files its root scripts read' };
  if (refused) return refused;
  for (const rel of eligibility.tarballs) {
    const bytes = readIfExists(path.resolve(dir, rel));
    if (!bytes) return { key: null, reason: `file: tarball ${rel} is not in the snapshot` };
    parts.push([`tarball:${rel}`, bytes]);
  }
  const unhashed = (name) =>
    isScrubbed(name) || isLocaleName(name) || PER_RUN_PATH_ENV_NAMES.includes(name) || AUTH_ENV_NAMES.has(name) || SESSION_ENV_NAMES.has(name);
  for (const name of Object.keys(env).filter((k) => !unhashed(k)).sort()) parts.push([`env:${name}`, String(env[name])]);
  for (const name of NPM_CONFIG_FILE_ENVS) {
    if (env[name]) parts.push([`file:${name}`, readIfExists(env[name]) ?? '']);
  }

  const hash = crypto.createHash('sha256');
  const keyParts = {};
  for (const [label, value] of parts) {
    const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8');
    hash.update(`${label}:${bytes.length}:`);
    hash.update(bytes);
    if (keyPartsSecret) keyParts[label] = crypto.createHmac('sha256', keyPartsSecret).update(bytes).digest('hex').slice(0, 12);
  }
  return { key: hash.digest('hex'), keyParts, lock, workspaces: eligibility.workspaces, scripts: pkg.scripts && typeof pkg.scripts === 'object' ? pkg.scripts : {} };
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

/**
 * The labelled key inputs that differ between `keyParts` (a lookup that missed) and the newest published entry
 * that recorded its own, as a sorted label list; null when there is no such entry. Two runs of one repo that
 * should share a key but do not name the leaking input here.
 */
/**
 * The host-local secret that keys the diagnostic digests in `meta.json`: 32 random bytes in a 0600 file in the
 * store root, created atomically (temp file, fsync, rename; a concurrent creator that wins is read instead). A plain
 * hash of an env value would let anyone who can read `meta.json` test guesses for a low-entropy secret; the cache key
 * itself is never derived from this. Null when the file is not exactly what we wrote (a symlink, someone else's,
 * another mode, another length): diagnostics are optional, so nothing is ever HMAC'd under a doubtful key.
 */
const KEY_PARTS_SECRET_BYTES = 32;

export function readKeyPartsSecret(cacheRoot) {
  const file = path.join(cacheRoot, '.keyparts.key');
  try {
    fs.mkdirSync(cacheRoot, { recursive: true });
    if (!fs.existsSync(file) && !pathIsLink(file)) {
      const tmp = path.join(cacheRoot, `.tmp-keyparts-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
      const fd = fs.openSync(tmp, 'wx', 0o600);
      try {
        fs.writeSync(fd, crypto.randomBytes(KEY_PARTS_SECRET_BYTES));
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      try {
        fs.linkSync(tmp, file); // fails if a concurrent creator already won, so theirs is never replaced
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
      } finally {
        fs.rmSync(tmp, { force: true });
      }
    }
    const st = fs.lstatSync(file);
    const ours = typeof process.getuid !== 'function' || st.uid === process.getuid();
    if (!st.isFile() || !ours || (st.mode & 0o777) !== 0o600 || st.size !== KEY_PARTS_SECRET_BYTES) return null;
    const secret = fs.readFileSync(file);
    return secret.length === KEY_PARTS_SECRET_BYTES ? secret : null;
  } catch {
    return null;
  }
}

function pathIsLink(file) {
  try {
    return fs.lstatSync(file).isSymbolicLink();
  } catch {
    return false;
  }
}

export function diffAgainstNewestEntry(cacheRoot, keyParts) {
  let newest = null;
  let names = [];
  try {
    names = fs.readdirSync(cacheRoot).filter((n) => KEY_RE.test(n));
  } catch {
    return null;
  }
  for (const name of names) {
    try {
      const meta = JSON.parse(fs.readFileSync(path.join(cacheRoot, name, 'meta.json'), 'utf8'));
      if (meta.keyParts && (!newest || meta.createdAt > newest.createdAt)) newest = meta;
    } catch {
      // unreadable entry: not a comparison base
    }
  }
  if (!newest) return null;
  const labels = new Set([...Object.keys(newest.keyParts), ...Object.keys(keyParts)]);
  return [...labels].filter((l) => newest.keyParts[l] !== keyParts[l]).sort();
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
 * then given `dirMode(srcMode)` once it is complete (after `finish(dst, rel)` has added anything else to it).
 * An entry for which `skip(rel)` is true is left out.
 */
function cloneTree(src, dst, hooks, rel = '') {
  const { onFile, dirMode, linkTarget = (target) => target, skip = () => false, finish = () => {} } = hooks;
  const srcMode = fs.statSync(src).mode & 0o7777;
  fs.mkdirSync(dst, { mode: 0o700 });
  for (const ent of fs.readdirSync(src, { withFileTypes: true })) {
    const s = path.join(src, ent.name);
    const d = path.join(dst, ent.name);
    const r = rel ? `${rel}/${ent.name}` : ent.name;
    if (skip(r)) continue;
    if (ent.isDirectory()) cloneTree(s, d, hooks, r);
    else if (ent.isSymbolicLink()) fs.symlinkSync(linkTarget(fs.readlinkSync(s), r), d);
    else if (ent.isFile()) onFile(s, d, r);
    else throw new Error(`unsupported entry type at ${s}`);
  }
  finish(dst, rel);
  fs.chmodSync(dst, dirMode(srcMode));
}

/** Copy one file and set its mode; returns its size. Node's COPYFILE_FICLONE makes a real copy on macOS, so no reflink is promised. */
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

const CLONEFILE_PY = 'import ctypes,sys\nl=ctypes.CDLL(None,use_errno=True)\nif l.clonefile(sys.argv[1].encode(),sys.argv[2].encode(),0)!=0:\n    e=ctypes.get_errno();sys.stderr.write("clonefile errno %d\\n"%e);sys.exit(e or 1)\n';
const CLONE_TIMEOUT_MS = 120_000;

/** One clonefile(2) of the whole tree (APFS copy-on-write); Node has no binding, so python3's ctypes makes the call. Throws on any failure. */
function cloneWholeTreeViaPython(src, dst) {
  execFileSync('/usr/bin/python3', ['-I', '-c', CLONEFILE_PY, src, dst], { stdio: 'ignore', timeout: CLONE_TIMEOUT_MS });
}

/** Make every directory and regular file under `dir` owner-writable, never following symlinks; throws on an unsupported entry type. */
function makeTreeWritable(dir) {
  fs.chmodSync(dir, (fs.lstatSync(dir).mode & 0o7777) | 0o200);
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) makeTreeWritable(p);
    else if (ent.isFile()) fs.chmodSync(p, (fs.lstatSync(p).mode & 0o7777) | 0o200);
    else if (!ent.isSymbolicLink()) throw new Error(`unsupported entry type at ${p}`);
  }
}

/**
 * Restore a stored tree to `destTree` as private files and directories the lane may write freely. On darwin the
 * whole tree is cloned in one call; any clone failure removes the partial tree and falls back to the file-by-file
 * copy. Throws on a failure of that copy (including a `destTree` that already exists); the caller removes the
 * partial tree and installs. Returns `{ cloned }`: a cloned file reads cold, so the caller scans the store instead.
 */
export function materializeFromStore(cacheRoot, key, destTree, { platform = process.platform, cloneWholeTree = cloneWholeTreeViaPython } = {}) {
  const src = entryTree(cacheRoot, key);
  if (platform === 'darwin' && !fs.existsSync(destTree)) {
    try {
      cloneWholeTree(src, destTree);
      makeTreeWritable(destTree);
      return { cloned: true };
    } catch (err) {
      process.stderr.write(`deps-cache clone failed, copying instead: ${err.message}\n`);
      removeTree(destTree);
    }
  }
  cloneTree(src, destTree, {
    onFile: (s, d) => copyFileAs(s, d, (mode) => mode | 0o200),
    dirMode: (mode) => mode | 0o200,
  });
  return { cloned: false };
}

/** Where the symlink for workspace `resolved` at `<tree>/<name>` points: relative, as `npm ci` writes it. */
function workspaceLinkTarget(tree, name, resolved) {
  return path.relative(path.dirname(path.join(tree, name)), path.join(path.dirname(tree), resolved));
}

/** The workspaces whose nested `node_modules` a stored entry holds (its inventory); throws unless the entry records one. */
export function readWorkspaceTrees(cacheRoot, key) {
  const meta = JSON.parse(fs.readFileSync(path.join(entryDir(cacheRoot, key), 'meta.json'), 'utf8'));
  if (!Array.isArray(meta.workspaceTrees) || !meta.workspaceTrees.every((t) => typeof t === 'string')) throw new Error('stored entry records no workspace inventory');
  return meta.workspaceTrees;
}

/**
 * After a restore: give each workspace back its nested `node_modules` (stashed in the tree under WORKSPACE_STASH)
 * and recreate the workspace symlinks in `tree`, relative to the work dir exactly as `npm ci` makes them. The stash
 * is removed. `storedTrees` is the entry's inventory of workspaces that had a nested tree: each must be in the stash,
 * nothing else may be, and NO workspace may already have a `node_modules` (a leftover would survive where `npm ci`
 * cleans it). Every path is containment-checked first. Throws (after undoing what it moved) on any mismatch.
 */
export function placeWorkspaces(tree, workspaces, storedTrees) {
  if (workspaces.length === 0) return;
  const workDir = path.dirname(tree);
  const stash = path.join(tree, WORKSPACE_STASH);
  const moved = [];
  try {
    const expected = new Set(storedTrees);
    for (const t of expected) if (!workspaces.some((w) => w.resolved === t)) throw new Error(`stored inventory names ${t}, which is not a workspace`);
    for (const { resolved } of workspaces) {
      assertContained(workDir, resolved);
      if (!fs.statSync(path.join(workDir, resolved)).isDirectory()) throw new Error(`workspace ${resolved} is not a directory`);
      const dest = path.join(workDir, resolved, 'node_modules');
      if (pathExists(dest)) throw new Error(`${dest} already exists`);
      if (pathExists(path.join(stash, resolved, 'node_modules')) !== expected.has(resolved)) {
        throw new Error(`stored nested node_modules for ${resolved} does not match the entry's inventory`);
      }
    }
    for (const { resolved, links } of workspaces) {
      if (expected.has(resolved)) {
        const dest = path.join(workDir, resolved, 'node_modules');
        fs.renameSync(path.join(stash, resolved, 'node_modules'), dest);
        moved.push(dest);
      }
      for (const name of links) {
        fs.mkdirSync(path.dirname(path.join(tree, name)), { recursive: true });
        fs.symlinkSync(workspaceLinkTarget(tree, name, resolved), path.join(tree, name));
      }
    }
    fs.rmSync(stash, { recursive: true, force: true });
  } catch (err) {
    for (const dest of moved) fs.rmSync(dest, { recursive: true, force: true });
    throw err;
  }
}

const pathExists = (p) => {
  try {
    fs.lstatSync(p);
    return true;
  } catch (err) {
    if (err.code === 'ENOENT') return false;
    throw err;
  }
};

/** Remove every workspace's nested `node_modules` under `workDir` (what `placeWorkspaces` put there); a path that fails containment is left alone. */
export function removeWorkspaceTrees(workDir, workspaces) {
  for (const { resolved } of workspaces) {
    try {
      assertContained(workDir, resolved);
    } catch {
      continue;
    }
    fs.rmSync(path.join(workDir, resolved, 'node_modules'), { recursive: true, force: true });
  }
}

/**
 * Copy an installed tree into the store, read-only, and publish it atomically (build under `.tmp-*`,
 * then rename onto `<key>`). Returns `{ published: true, bytes }`, or `{ published: false }` when another
 * run published the same key first (that run's tree is equally valid; ours is discarded). `relocation` is what
 * `collectInstallPathReferences` found: the stored COPY of each recorded file or symlink has its install paths
 * replaced by placeholder tokens (the source tree is untouched), and the record goes into `meta.json`.
 */
export function publishToStore(cacheRoot, key, srcTree, relocation = null, keyParts = null, workspaces = []) {
  fs.mkdirSync(cacheRoot, { recursive: true });
  const tmp = path.join(cacheRoot, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  fs.mkdirSync(tmp);
  try {
    let bytes = 0;
    const pairs = relocation ? placeholderPairs(relocation) : [];
    const recorded = new Map((relocation?.entries ?? []).map((e) => [e.relPath, e.kind]));
    const template = (buf, rel, accept = strictOccurrence) => {
      const out = rewritePrefixes(buf, pairs, accept);
      if (out === null) throw new Error(`relocation: ${rel} changed since collection and now has an ambiguous install path`);
      return out;
    };
    const workDir = path.dirname(srcTree);
    for (const { resolved } of workspaces) assertContained(workDir, resolved);
    const storedTrees = [];
    const linkNames = new Set(workspaces.flatMap((w) => w.links));
    for (const { resolved, links } of workspaces) {
      for (const name of links) {
        let actual = null;
        try {
          actual = fs.readlinkSync(path.join(srcTree, name));
        } catch {
          // not a symlink: refused just below
        }
        if (actual !== workspaceLinkTarget(srcTree, name, resolved)) throw new Error(`workspace link ${name} is not the symlink npm ci makes to ${resolved}`);
      }
    }
    if (workspaces.length > 0 && fs.existsSync(path.join(srcTree, WORKSPACE_STASH))) throw new Error(`${WORKSPACE_STASH} already exists in the installed tree`);
    // `base` is the tree `localRel` is relative to; `prefix` is where that tree sits inside the stored one ('' for the root).
    const hooksFor = (base, prefix) => ({
      onFile: (s, d, rel) => {
        if (recorded.get(rel) !== 'text') {
          bytes += copyFileAs(s, d, (mode) => mode & 0o7555);
          return;
        }
        const templated = template(fs.readFileSync(s), rel);
        fs.writeFileSync(d, templated);
        fs.chmodSync(d, fs.statSync(s).mode & 0o7555);
        bytes += templated.length;
      },
      linkTarget: (target, rel) => (recorded.get(rel) === 'symlink' ? template(Buffer.from(target, 'utf8'), rel, anchoredOccurrence).toString('utf8') : target),
      dirMode: (mode) => mode & 0o7555,
      skip: (rel) => (prefix ? isRegenerableBuildArtifact(base, rel.slice(prefix.length + 1)) : linkNames.has(rel) || isRegenerableBuildArtifact(base, rel)),
    });
    const storedTree = path.join(tmp, 'node_modules');
    cloneTree(srcTree, storedTree, {
      ...hooksFor(srcTree, ''),
      finish: (dst, rel) => {
        if (rel !== '') return;
        for (const { resolved } of workspaces) {
          const nested = path.join(workDir, resolved, 'node_modules');
          if (!pathExists(nested)) continue;
          storedTrees.push(resolved);
          const prefix = `${WORKSPACE_STASH}/${resolved}/node_modules`;
          fs.mkdirSync(path.dirname(path.join(dst, prefix)), { recursive: true });
          cloneTree(nested, path.join(dst, prefix), hooksFor(nested, prefix), prefix);
        }
        for (const dir of workspaces.flatMap(({ resolved }) => stashParents(dst, resolved))) fs.chmodSync(dir, 0o555);
      },
    });
    if (relocation) verifyTemplatedCopy(path.join(tmp, 'node_modules'), relocation);
    fs.writeFileSync(path.join(tmp, 'meta.json'), JSON.stringify({ bytes, createdAt: Date.now(), ...(workspaces.length > 0 ? { workspaceTrees: storedTrees.sort() } : {}), ...(relocation ? { relocation } : {}), ...(keyParts ? { keyParts } : {}) }));
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

/**
 * Bytes that may follow an install path: `/ " ' \` ) ] }`, whitespace, or the end. Every other byte (`: ; = , \` included)
 * is legal in a file name, so it might continue a longer, unrelated path: such a tree is ambiguous, never guessed.
 */
const PATH_FOLLOWERS = new Set(Buffer.from('/"\'`)]} \t\n\r\v\f', 'latin1'));

/** Bytes that may precede an install path: start of file, `" ' \` ( [ {`, or whitespace. Anything else (`/`, `:`, `=`, a name character) may make it the tail of a longer path. */
const PATH_LEADERS = new Set(Buffer.from('"\'`([{ \t\n\r\v\f', 'latin1'));

/**
 * Build leftovers that name the install path but are never loaded at run time, and that the tool which made them
 * regenerates. Exactly these are neither scanned nor stored, so a cached tree lacks them:
 *   - under `<pkg>/build/` when `<pkg>/binding.gyp` exists: `Makefile`, `binding.Makefile`, `*.target.mk`,
 *     `config.gypi`, `gyp-mac-tool`, and `Release|Debug/.deps`, `Release|Debug/obj.target`. Nothing else under
 *     `build/` (`build/deps/`, `build/Release/*.node`, anything a package reads at run time) is touched;
 *   - `__pycache__` under `node-gyp/.../pylib/` where every `.pyc` has its `.py` source beside the cache.
 * `npm rebuild` still works: it runs `node-gyp rebuild`, which configures from `binding.gyp` again. A symlink that
 * points into or through a pruned path, or is itself pruned, makes the tree uncacheable (`refusesLink`).
 */
const NODE_GYP_INTERMEDIATE_RE = /^(.*\/)?build\/(?:Makefile|binding\.Makefile|[^/]+\.target\.mk|config\.gypi|gyp-mac-tool|(?:Release|Debug)\/(?:\.deps|obj\.target))$/;
const GYP_PYCACHE_RE = /(?:^|\/)node-gyp\/(?:.*\/)?pylib\/(?:.*\/)?__pycache__$/;

function isGypPycache(tree, rel) {
  if (!GYP_PYCACHE_RE.test(rel)) return false;
  try {
    const sources = path.join(tree, rel, '..');
    const caches = fs.readdirSync(path.join(tree, rel));
    return caches.every((f) => f.endsWith('.pyc') && fs.existsSync(path.join(sources, `${f.split('.')[0]}.py`)));
  } catch {
    return false;
  }
}

function isRegenerableBuildArtifact(tree, rel) {
  if (isGypPycache(tree, rel)) return true;
  const match = NODE_GYP_INTERMEDIATE_RE.exec(rel);
  return match !== null && fs.existsSync(path.join(tree, match[1] ?? '', 'binding.gyp'));
}

/**
 * Every path a symlink passes through on its way to its target: each component of each hop, following intermediate
 * symlinks (bounded). A pruned entry anywhere on that chain is as bad as one at the end.
 */
function symlinkChain(linkAbs) {
  const visited = [];
  let current = path.dirname(linkAbs);
  const pending = [fs.readlinkSync(linkAbs)];
  for (let hops = 0; pending.length > 0; hops += 1) {
    if (hops > 64) throw new Error('too many symlink hops');
    const segments = pending.shift().split('/');
    if (segments[0] === '') current = path.parse(linkAbs).root;
    for (let i = 0; i < segments.length; i += 1) {
      const seg = segments[i];
      if (seg === '' || seg === '.') continue;
      const next = seg === '..' ? path.dirname(current) : path.join(current, seg);
      visited.push(next);
      if (seg !== '..' && fs.lstatSync(next).isSymbolicLink()) {
        pending.unshift([fs.readlinkSync(next), ...segments.slice(i + 1)].join('/'));
        break;
      }
      current = next;
    }
  }
  return visited;
}

/**
 * Does the symlink at `linkAbs` lead to, or through, a pruned path of `tree`, or somewhere we cannot vouch for? Its
 * whole chain (`symlinkChain`) and its final `realpath` are tested against the pruned set. A link that escapes the
 * tree or cannot be resolved answers true (the tree is refused, not guessed at), except a link `relocatable` says
 * relocation will point back into the work dir's own `node_modules`: that may not resolve here, so it is tested
 * lexically only.
 */
function refusesLink(tree, linkAbs, relocatable) {
  const root = fs.realpathSync(tree);
  const prunedUnder = (abs) => {
    for (const base of new Set([tree, root])) {
      const rel = path.relative(base, abs);
      if (rel === '' || rel.startsWith('..') || path.isAbsolute(rel)) continue;
      const parts = rel.split(path.sep);
      if (parts.some((_, i) => isRegenerableBuildArtifact(tree, parts.slice(0, i + 1).join('/')))) return true;
    }
    return false;
  };
  let chain;
  try {
    chain = symlinkChain(linkAbs);
  } catch {
    return !relocatable;
  }
  if (chain.some(prunedUnder)) return true;
  let real;
  try {
    real = fs.realpathSync(linkAbs);
  } catch {
    return !relocatable;
  }
  const rel = path.relative(root, real);
  if (rel.startsWith('..') || path.isAbsolute(rel)) return !relocatable;
  return prunedUnder(real);
}

const strictOccurrence = (buf, start, end) => (start === 0 || PATH_LEADERS.has(buf[start - 1])) && (end === buf.length || PATH_FOLLOWERS.has(buf[end]));

/** A symlink target is a path, not text: an install path counts only as the whole target or its leading directories. */
const anchoredOccurrence = (buf, start, end) => start === 0 && (end === buf.length || buf[end] === 0x2f);

const tokenFor = (roles, nonce) => Buffer.from(`${TOKEN_MARKER}${roles.join('+')}_${nonce}@@`);
/** The end of every token of this nonce: what a collision check and a leftover check search for. */
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
 *   { prunedLink: <rel> }  a symlink pointing into a path left out of the cached tree (`isRegenerableBuildArtifact`): refuse;
 *   { ambiguous: <rel> }   an occurrence not followed by a path terminator (or glued to a longer name before it): refuse;
 *   { relocation }         else: `{ nonce, prefixes: [{ roles, path }], entries: [{ relPath, kind: 'text' | 'symlink' }] }`,
 *                          roles sharing one path being ONE group; `publishToStore` templates the entries.
 * The nonce (a fresh one if the tree already contains the token text) names this entry's placeholder tokens.
 * With `workspaces` (BRAIN-466) the scan skips their symlinks and also covers each workspace's own `node_modules`,
 * whose entries are named by their path inside the stored tree (under the stash).
 */
export function collectInstallPathReferences(tree, roles, { newNonce = () => crypto.randomBytes(16).toString('hex'), workspaces = [] } = {}) {
  const groups = [];
  for (const { role, path: p } of roles) {
    const group = groups.find((g) => g.path === p);
    if (group) group.roles.push(role);
    else groups.push({ roles: [role], path: p });
  }
  const pairs = groups.map((g) => ({ from: Buffer.from(g.path, 'utf8'), to: Buffer.alloc(0) }));
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const nonce = newNonce();
    const marker = tokenTail(nonce);
    const outcome = scanTree(tree, groups, pairs, marker, workspaces);
    if (outcome.collision) continue;
    if (outcome.prunedLink) return { prunedLink: outcome.prunedLink };
    if (outcome.binary) return { binary: outcome.binary };
    if (outcome.ambiguous) return { ambiguous: outcome.ambiguous };
    const found = groups.filter((_, i) => outcome.found.has(i));
    return { relocation: { nonce, candidates: groups.map((g) => g.path), prefixes: found.map(({ roles: r, path: p }) => ({ roles: r, path: p })), entries: outcome.entries } };
  }
  return { ambiguous: TOKEN_MARKER };
}

/**
 * A symlink whose text starts with an install path: may relocation keep it? Only when that path is the work dir
 * (`workDir`/`realWorkDir`) and the target lies inside a `node_modules` under it; a link into a temp dir, another
 * role, or elsewhere in the work dir is not ours to vouch for (false). Null when the text names no install path as
 * a leading directory (the ambiguity checks decide that).
 */
function relocatableInstallLink(groups, text) {
  const group = groups.find((g) => text === g.path || text.startsWith(`${g.path}/`));
  if (!group) return null;
  return group.roles.some((role) => role === 'workDir' || role === 'realWorkDir') && /^\/(?:[^/]+\/)*node_modules(?:\/|$)/.test(text.slice(group.path.length));
}

function scanTree(tree, groups, pairs, marker, workspaces) {
  const bufs = [...pairs.map((p) => p.from), marker];
  const markerIdx = bufs.length - 1;
  const found = new Set();
  const entries = [];
  const linkNames = new Set(workspaces.flatMap((w) => w.links));
  // `base` is the tree `rel` is relative to; `prefix` is where it sits in the stored tree, so entries and outcomes use `prefix + rel`.
  const walk = (base, abs, rel, prefix) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      const p = path.join(abs, ent.name);
      const r = rel ? `${rel}/${ent.name}` : ent.name;
      const stored = prefix + r;
      if (!prefix && ent.isSymbolicLink() && linkNames.has(r)) continue;
      if (isRegenerableBuildArtifact(base, r)) {
        if (ent.isSymbolicLink()) return { prunedLink: stored };
        continue;
      }
      let outcome = null;
      if (ent.isDirectory()) outcome = walk(base, p, r, prefix);
      else if (ent.isSymbolicLink()) {
        const target = Buffer.from(fs.readlinkSync(p), 'utf8');
        const text = target.toString('utf8');
        const keeps = relocatableInstallLink(groups, text);
        if (keeps === false) return { prunedLink: stored };
        if (keeps !== null || !pairs.some((pr) => target.includes(pr.from))) {
          if (refusesLink(base, p, keeps === true)) return { prunedLink: stored };
        }
        if (target.includes(marker)) return { collision: true };
        const hit = pairs.some((pr) => target.includes(pr.from));
        if (hit && rewritePrefixes(target, pairs, anchoredOccurrence) === null) return { ambiguous: stored };
        if (hit) {
          pairs.forEach((pr, i) => target.includes(pr.from) && found.add(i));
          entries.push({ relPath: stored, kind: 'symlink' });
        }
      } else if (ent.isFile()) {
        const { hits } = fileNeedleHits(p, bufs);
        if (hits.has(markerIdx)) return { collision: true };
        if (hits.size === 0) continue;
        const content = fs.readFileSync(p);
        if (!isRewritableText(r, content)) return { binary: stored };
        if (rewritePrefixes(content, pairs, strictOccurrence) === null) return { ambiguous: stored };
        hits.forEach((i) => found.add(i));
        entries.push({ relPath: stored, kind: 'text' });
      }
      if (outcome) return outcome;
    }
    return null;
  };
  if (workspaces.length > 0 && fs.existsSync(path.join(tree, WORKSPACE_STASH))) return { ambiguous: WORKSPACE_STASH };
  const rootOutcome = walk(tree, tree, '', '');
  if (rootOutcome) return rootOutcome;
  for (const { resolved } of workspaces) {
    const nested = path.join(path.dirname(tree), resolved, 'node_modules');
    const prefix = `${WORKSPACE_STASH}/${resolved}/node_modules/`;
    try {
      assertContained(path.dirname(tree), resolved);
    } catch {
      return { prunedLink: prefix.slice(0, -1) };
    }
    let st = null;
    try {
      st = fs.lstatSync(nested);
    } catch {
      continue; // this workspace installed nothing of its own
    }
    if (!st.isDirectory()) return { prunedLink: prefix.slice(0, -1) };
    const outcome = walk(nested, nested, '', prefix);
    if (outcome) return outcome;
  }
  return { found, entries };
}

/** The first file or symlink under `tree` (rel path, none in `skip`) holding any of `needles`, scanning files in chunks. Null when clean. */
function findNeedleInTree(tree, needles, skip = new Set()) {
  const walk = (abs, rel) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      const p = path.join(abs, ent.name);
      const r = rel ? `${rel}/${ent.name}` : ent.name;
      if (ent.isDirectory()) {
        const found = walk(p, r);
        if (found) return found;
      } else if (skip.has(r)) {
        continue;
      } else if (ent.isSymbolicLink()) {
        const target = Buffer.from(fs.readlinkSync(p), 'utf8');
        if (needles.some((n) => target.includes(n))) return r;
      } else if (ent.isFile() && fileContains(p, needles)) {
        return r;
      }
    }
    return null;
  };
  return walk(tree, '');
}

/** The first recorded entry of `tree` (a file's content or a symlink's target) holding any of `needles`; null when clean. */
function findNeedleInEntries(tree, entries, needles) {
  for (const { relPath, kind } of entries) {
    const file = path.join(tree, relPath);
    const content = kind === 'symlink' ? Buffer.from(fs.readlinkSync(file), 'utf8') : fs.readFileSync(file);
    if (needles.some((n) => content.includes(n))) return relPath;
  }
  return null;
}

/**
 * Check the cached copy of a tree before it is published: each recorded entry holds a token and no raw install
 * path, and nothing else holds a raw install path or a token (a file created or changed after collection).
 */
function verifyTemplatedCopy(copy, { nonce, candidates, entries }) {
  const raw = candidates.map((p) => Buffer.from(p, 'utf8'));
  const tail = tokenTail(nonce);
  for (const { relPath, kind } of entries) {
    const file = path.join(copy, relPath);
    const content = kind === 'symlink' ? Buffer.from(fs.readlinkSync(file), 'utf8') : fs.readFileSync(file);
    if (!content.includes(tail)) throw new Error(`relocation: cached ${relPath} holds no placeholder`);
    if (raw.some((r) => content.includes(r))) throw new Error(`relocation: cached ${relPath} still names an install path`);
  }
  const stray = findNeedleInTree(copy, [...raw, tail], new Set(entries.map((e) => e.relPath)));
  if (stray) throw new Error(`relocation: unrecorded ${stray} names an install path or holds a placeholder`);
}

/** The relocation record of a stored entry: empty when the tree names no install path. Throws on a malformed record. */
export function readRelocation(cacheRoot, key) {
  const meta = JSON.parse(fs.readFileSync(path.join(entryDir(cacheRoot, key), 'meta.json'), 'utf8'));
  const rec = meta.relocation ?? { nonce: '0'.repeat(32), candidates: [], prefixes: [], entries: [] };
  const okPrefix = (p) => p && Array.isArray(p.roles) && p.roles.length > 0 && p.roles.every((r) => typeof r === 'string') && typeof p.path === 'string' && p.path.length > 0;
  const okEntry = (e) => e && typeof e.relPath === 'string' && (e.kind === 'text' || e.kind === 'symlink');
  if (!/^[0-9a-f]{32}$/.test(rec.nonce) || !Array.isArray(rec.candidates) || !rec.candidates.every((c) => typeof c === 'string' && c.length > 0) || !Array.isArray(rec.prefixes) || !Array.isArray(rec.entries) || !rec.prefixes.every(okPrefix) || !rec.entries.every(okEntry)) {
    throw new Error('relocation record is malformed');
  }
  return rec;
}

/**
 * Make a restored tree name the NEW run's install paths: substitute each recorded entry's placeholder tokens with
 * the new path of the token's role (exact bytes, via a temp file renamed over the original with its mode kept; a
 * symlink is re-created with the substituted target), then verify that no token of this entry's nonce is left anywhere in
 * the tree (a file the record omits included). Throws on any failure (roles that now differ, a missing role, an entry with no token, a
 * leftover token); the caller discards the tree. With `scanRoot` (the warm store entry a clone came from), the
 * leftover scan reads that instead of `tree` (skipping the recorded entries) and checks the recorded entries of `tree` itself.
 */
export function relocateRestoredTree(tree, { nonce, candidates, prefixes, entries }, newPaths, { scanRoot } = {}) {
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

  const destinations = Object.values(newPaths);
  const stale = candidates.filter((c) => !destinations.some((d) => d.includes(c))).map((c) => Buffer.from(c, 'utf8'));
  const needles = [tail, ...stale];
  const leftover = scanRoot ? findNeedleInTree(scanRoot, needles, new Set(entries.map((e) => e.relPath))) ?? findNeedleInEntries(tree, entries, needles) : findNeedleInTree(tree, needles);
  if (leftover) throw new Error(`relocation: ${leftover} still holds a placeholder or an old install path`);
}

/**
 * Everything under `dir` except `node_modules` trees, `.git` included, as path -> "type:size:mtimeMs" (files and symlinks add dev:ino, so an atomic replacement that keeps size and mtime still shows) (an exact workspace root: its child names).
 * Taken before and after an install: any difference means a lifecycle script wrote outside `node_modules`,
 * which a cached `node_modules` could never reproduce.
 */
export function snapshotOutsideNodeModules(dir, workspaceRoots = []) {
  const out = new Map();
  const roots = new Set(workspaceRoots);
  const walk = (abs, rel) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true })) {
      if (ent.name === 'node_modules') continue;
      const childRel = rel ? `${rel}/${ent.name}` : ent.name;
      const st = fs.lstatSync(path.join(abs, ent.name));
      // An exact workspace root gains a `node_modules` child during a real install, which moves its own size and mtime:
      // it is compared by its child names without `node_modules` instead (BRAIN-466). Every other directory keeps size and mtime.
      const signature = roots.has(childRel)
        ? `d:names:${fs.readdirSync(path.join(abs, ent.name)).filter((n) => n !== 'node_modules').sort().join('/')}`
        : ent.isDirectory()
          ? `d:${st.size}:${st.mtimeMs}`
          : `${ent.isSymbolicLink() ? 'l' : 'f'}:${st.size}:${st.mtimeMs}:${st.dev}:${st.ino}`;
      out.set(childRel, signature);
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
