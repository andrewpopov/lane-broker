import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync, spawn } from 'node:child_process';
import {
  computeDepsKey,
  entryDir,
  entryTree,
  materializeFromStore,
  publishToStore,
  touchLastUsed,
  acquireLease,
  trimStore,
  evictLeastRecentlyUsed,
  purgeTrash,
  removeTree,
  findInstallPathReference,
  findMissingInstalled,
  lockEligibility,
  SCAN_CHUNK_BYTES,
  ALLOWED_ROOT_SCRIPTS,
  ROOT_SCRIPT_NAMES,
  scrubDepsEnv,
  pinInstallLocale,
  parseMountInfo,
  parseMountOutput,
  tempDirFsProperties,
  unexplainedChanges,
  allowlistedRootEvents,
  snapshotOutsideNodeModules,
  snapshotChanges,
  diffAgainstNewestEntry,
  readKeyPartsSecret,
} from '../src/deps-cache.js';
import { resolveTicketConfig, loadGlobalConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { freshEnv, writeRepoConfig, writeGlobalConfig } from './helpers.js';
import { withLock } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';

/**
 * BRAIN-389: the installed-deps store. Everything here runs on synthetic trees (no npm, no network);
 * tests/remote-deps-cache.test.js drives the same code through a real `remote-exec` + real `npm ci`.
 */

const DEPS_CACHE_URL = new URL('../src/deps-cache.js', import.meta.url).href;
const IS_ROOT = typeof process.getuid === 'function' && process.getuid() === 0;

function tmpDir(prefix) {
  return makeTmpDir(`${prefix}-`);
}

/** A stand-in node_modules: nested packages, a `.bin` symlink to an executable, a file big enough to count. */
function makeTree(dir, { bytes = 1000, packages = 20 } = {}) {
  fs.mkdirSync(path.join(dir, '.bin'), { recursive: true });
  for (let i = 0; i < packages; i += 1) {
    const pkg = path.join(dir, `pkg${i}`, 'lib', 'deep');
    fs.mkdirSync(pkg, { recursive: true });
    fs.writeFileSync(path.join(dir, `pkg${i}`, 'package.json'), JSON.stringify({ name: `pkg${i}` }));
    fs.writeFileSync(path.join(pkg, 'index.js'), `module.exports = ${i};\n`);
  }
  fs.writeFileSync(path.join(dir, 'pkg0', 'payload.bin'), Buffer.alloc(bytes, 7));
  const cli = path.join(dir, 'pkg0', 'cli.js');
  fs.writeFileSync(cli, '#!/usr/bin/env node\nconsole.log("tool ran");\n', { mode: 0o755 });
  fs.symlinkSync('../pkg0/cli.js', path.join(dir, '.bin', 'tool'));
  return dir;
}

/** Content hash of a whole tree: path, type, symlink target, and file bytes. */
function treeDigest(root) {
  const h = crypto.createHash('sha256');
  const walk = (abs, rel) => {
    for (const ent of fs.readdirSync(abs, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const p = path.join(abs, ent.name);
      const r = `${rel}/${ent.name}`;
      if (ent.isSymbolicLink()) h.update(`L${r}->${fs.readlinkSync(p)}\n`);
      else if (ent.isDirectory()) {
        h.update(`D${r}\n`);
        walk(p, r);
      } else h.update(`F${r}:${fs.readFileSync(p).toString('hex')}\n`);
    }
  };
  walk(root, '');
  return h.digest('hex');
}

function newKey() {
  return crypto.randomBytes(32).toString('hex');
}

function publish(cacheRoot, opts) {
  const src = makeTree(tmpDir('deps-cache-src'), opts);
  const key = newKey();
  const result = publishToStore(cacheRoot, key, src);
  assert.equal(result.published, true);
  return { key, src, bytes: result.bytes };
}

// ---- key ----

const REGISTRY_ENTRY = (version) => ({ version, resolved: `https://registry.npmjs.org/x/-/x-${version}.tgz`, integrity: 'sha512-abc' });

function makeKeyFixture({ lockPackages, scripts, tarball } = {}) {
  const root = tmpDir('deps-cache-key');
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({ name: 'a', ...(scripts ? { scripts } : {}) }));
  fs.writeFileSync(
    path.join(root, 'package-lock.json'),
    JSON.stringify({ lockfileVersion: 3, packages: lockPackages ?? { '': { name: 'a' }, 'node_modules/x': REGISTRY_ENTRY('1.0.0') } }),
  );
  if (tarball) fs.writeFileSync(path.join(root, 'x-1.0.0.tgz'), tarball);
  const userconfig = path.join(root, 'userrc');
  const globalconfig = path.join(root, 'globalrc');
  fs.writeFileSync(userconfig, '');
  fs.writeFileSync(globalconfig, '');
  const inputs = () => ({
    dir: root,
    rootDir: root,
    installArgv: ['npm', 'ci', '--no-audit', '--no-fund'],
    env: { npm_config_cache: '/r/npm-cache', npm_config_userconfig: userconfig, npm_config_globalconfig: globalconfig, PATH: '/bin', HOME: '/home/runner' },
    npmVersion: '11.9.0',
    system: { glibc: '2.39', libc: 'glibc', osId: 'ubuntu', osVersionId: '24.04' },
    toolVersion: (cmd) => `${cmd} 13.2.0`,
    npmConfig: { ignoreScripts: 'false', scriptShell: 'null' },
    tempFsProps: () => 'ext4|',
  });
  return { root, userconfig, globalconfig, inputs };
}

test('key: identical inputs give the same key; volatile variables do not matter, any other does', () => {
  const f = makeKeyFixture();
  const a = computeDepsKey(f.inputs());
  assert.match(a.key, /^[0-9a-f]{64}$/);
  assert.equal(computeDepsKey(f.inputs()).key, a.key);

  const volatile = f.inputs();
  Object.assign(volatile.env, {
    TMPDIR: '/var/tmp/lb-other', TMP: '/x', TEMP: '/x', PWD: '/elsewhere', OLDPWD: '/o', SHLVL: '3', _: '/usr/bin/other',
    GIT_CEILING_DIRECTORIES: '/tickets/other', SSH_CONNECTION: '1.2.3.4 5 6.7.8.9 22', SSH_AUTH_SOCK: '/tmp/agent',
    LANE_BROKER_LEASE: 'other-ticket', LANE_BROKER_CPU_CORES: '7', LANE_FAKE_RUNNER: '1',
    GIT_SSH: '/usr/bin/ssh-other', GIT_SSH_COMMAND: 'ssh -i /other/key',
  });
  assert.equal(computeDepsKey(volatile).key, a.key, 'per-run and authentication variables are not part of the key (a git dep is pinned to a sha, so credentials decide success, not content)');

  const sshBuild = (value) => {
    const i = f.inputs();
    i.env.SSH_BUILD_MODE = value;
    return computeDepsKey(i).key;
  };
  assert.notEqual(sshBuild('native'), sshBuild('portable'), 'an SSH_* variable that is not auth or session info is hashed');
  const withAgent = f.inputs();
  Object.assign(withAgent.env, { SSH_AUTH_SOCK: '/other.sock', SSH_AGENT_PID: '4242', SSH_CLIENT: '1.2.3.4 5 22', SSH_TTY: '/dev/pts/9' });
  assert.equal(computeDepsKey(withAgent).key, a.key, 'named auth and session variables are not');

  for (const change of [{ PATH: '/other/bin' }, { HOME: '/home/other' }, { CC: 'clang' }, { HTTPS_PROXY: 'http://p' }, { npm_config_registry: 'http://r.invalid' }]) {
    const i = f.inputs();
    Object.assign(i.env, change);
    assert.notEqual(computeDepsKey(i).key, a.key, `${Object.keys(change)[0]} is part of the key`);
  }
});

test('key: per-run variables never change the key; any other value, path-bearing or not, does (BRAIN-423)', () => {
  const fx = makeKeyFixture();
  const keyFor = (extra) => computeDepsKey({ ...fx.inputs(), env: { ...fx.inputs().env, ...extra } }).key;
  const base = keyFor({});
  const perRun = [
    { INVOCATION_ID: 'a'.repeat(32) }, { JOURNAL_STREAM: '8:123' }, { SYSTEMD_EXEC_PID: '99' },
    { MEMORY_PRESSURE_WATCH: '/sys/fs/cgroup/x/memory.pressure' }, { XDG_SESSION_ID: '42' }, { LISTEN_PID: '7' },
    { LANE_BROKER_CPU_CORES: '3' }, { TMPDIR: '/var/tmp/lb-xyz' },
  ];
  for (const extra of perRun) assert.equal(keyFor(extra), base, `${Object.keys(extra)[0]} is not an input`);
  assert.notEqual(keyFor({ CFLAGS: '-O0' }), base, 'a real install input still is');

  assert.notEqual(keyFor({ CFLAGS: '-I/r/a/work-extra' }), keyFor({ CFLAGS: '-I/r/b/work-extra' }), 'values stay verbatim: different directories are different inputs');
});

test('key parts: digests are keyed with a host-local 0600 secret, never a plain sha256 of the value', () => {
  const root = tmpDir('deps-cache-parts');
  const secret = readKeyPartsSecret(root);
  assert.equal(secret.length, 32);
  assert.ok(readKeyPartsSecret(root).equals(secret), 'created once, then reused');
  assert.equal(fs.statSync(path.join(root, '.keyparts.key')).mode & 0o777, 0o600);
  assert.deepEqual(fs.readdirSync(root), ['.keyparts.key'], 'no temp file left behind');
  const fx = makeKeyFixture();
  const env = { ...fx.inputs().env, API_TOKEN: 'hunter2' };
  const withKey = computeDepsKey({ ...fx.inputs(), env, keyPartsSecret: secret });
  const plain = crypto.createHash('sha256').update('hunter2').digest('hex');
  assert.ok(withKey.keyParts['env:API_TOKEN'], 'a keyed digest is recorded');
  assert.ok(!plain.startsWith(withKey.keyParts['env:API_TOKEN']), 'and it is not the plain sha256');
  const key2 = computeDepsKey({ ...fx.inputs(), env, keyPartsSecret: crypto.randomBytes(32) });
  assert.equal(key2.key, withKey.key, 'the cache key does not depend on the secret');
  assert.notEqual(key2.keyParts['env:API_TOKEN'], withKey.keyParts['env:API_TOKEN']);
  assert.deepEqual(computeDepsKey({ ...fx.inputs(), env }).keyParts, {}, 'no secret, no digests');
});

test('key parts: a key file that is empty, short, a symlink or the wrong mode yields no secret, so no digests', () => {
  const plant = (make) => {
    const root = tmpDir('deps-cache-badkey');
    make(path.join(root, '.keyparts.key'), root);
    return readKeyPartsSecret(root);
  };
  assert.equal(plant((f) => fs.writeFileSync(f, '', { mode: 0o600 })), null, 'empty');
  assert.equal(plant((f) => fs.writeFileSync(f, Buffer.alloc(16, 1), { mode: 0o600 })), null, 'short');
  assert.equal(plant((f) => fs.writeFileSync(f, Buffer.alloc(33, 1), { mode: 0o600 })), null, 'long');
  assert.equal(plant((f) => fs.writeFileSync(f, Buffer.alloc(32, 1), { mode: 0o644 })), null, 'wrong mode');
  assert.equal(plant((f, root) => {
    fs.writeFileSync(path.join(root, 'real'), Buffer.alloc(32, 1), { mode: 0o600 });
    fs.symlinkSync('real', f);
  }), null, 'symlink');
  assert.equal(plant((f) => fs.symlinkSync('/nonexistent/key', f)), null, 'dangling symlink');
  assert.ok(plant((f) => fs.writeFileSync(f, Buffer.alloc(32, 1), { mode: 0o600 })), 'a valid one is accepted');
});

test('key parts: a lookup names the labels that differ from the newest published entry', () => {
  const root = tmpDir('deps-cache-diff');
  assert.equal(diffAgainstNewestEntry(root, { a: '1' }), null, 'no entry');
  const src = makeTree(tmpDir('deps-cache-src'));
  publishToStore(root, newKey(), src, null, { 'env:A': 'aaa', 'env:B': 'bbb', same: 's' });
  assert.deepEqual(diffAgainstNewestEntry(root, { 'env:A': 'aaa', 'env:B': 'xxx', 'env:C': 'ccc', same: 's' }), ['env:B', 'env:C']);
  assert.deepEqual(diffAgainstNewestEntry(root, { 'env:A': 'aaa', 'env:B': 'bbb', same: 's' }), []);
});

test('key: every key input changes the key', () => {
  const f = makeKeyFixture({
    lockPackages: { '': { name: 'a' }, 'node_modules/x': { ...REGISTRY_ENTRY('1.0.0'), hasInstallScript: true }, 'node_modules/t': { version: '1.0.0', resolved: 'file:x-1.0.0.tgz', integrity: 'sha512-t' } },
    tarball: 'tarball bytes one',
  });
  const base = computeDepsKey(f.inputs()).key;
  assert.match(base, /^[0-9a-f]{64}$/);
  const changed = {};
  const keyWith = (name, mutate) => {
    const inputs = f.inputs();
    const restore = mutate(inputs) ?? (() => {});
    const result = computeDepsKey(inputs);
    assert.ok(result.key, `${name}: ${result.reason}`);
    changed[name] = result.key;
    restore();
  };
  const rewrite = (file, content) => {
    const had = fs.existsSync(file) ? fs.readFileSync(file) : null;
    fs.writeFileSync(file, content);
    return () => (had === null ? fs.rmSync(file) : fs.writeFileSync(file, had));
  };
  const lockFile = path.join(f.root, 'package-lock.json');
  const lockWith = (fn) => {
    const lock = JSON.parse(fs.readFileSync(lockFile, 'utf8'));
    fn(lock);
    return rewrite(lockFile, JSON.stringify(lock));
  };

  keyWith('lockfile bytes', () => lockWith((lock) => (lock.packages['node_modules/x'].version = '1.0.1')));
  keyWith('lockfile name (shrinkwrap)', () => rewrite(path.join(f.root, 'npm-shrinkwrap.json'), fs.readFileSync(lockFile)));
  keyWith('package.json', () => rewrite(path.join(f.root, 'package.json'), '{"name":"a","x":1}'));
  keyWith('dir .npmrc', () => rewrite(path.join(f.root, '.npmrc'), 'foo=bar\n'));
  keyWith('file: tarball bytes', () => rewrite(path.join(f.root, 'x-1.0.0.tgz'), 'tarball bytes two'));
  keyWith('node version', (i) => {
    i.runtime = { version: 'v0.0.0', platform: process.platform, arch: process.arch };
  });
  keyWith('node platform', (i) => {
    i.runtime = { version: process.version, platform: 'plan9', arch: process.arch };
  });
  keyWith('node arch', (i) => {
    i.runtime = { version: process.version, platform: process.platform, arch: 'mips' };
  });
  keyWith('glibc', (i) => {
    i.system = { ...i.system, glibc: '2.31' };
  });
  keyWith('os id', (i) => {
    i.system = { ...i.system, osId: 'debian' };
  });
  keyWith('os version id', (i) => {
    i.system = { ...i.system, osVersionId: '22.04' };
  });
  keyWith('cc version', (i) => {
    i.toolVersion = (cmd) => (cmd === 'cc' ? 'cc 14.0.0' : 'python3 13.2.0');
  });
  keyWith('python3 version (missing is a value)', (i) => {
    i.toolVersion = (cmd) => (cmd === 'python3' ? 'missing' : 'cc 13.2.0');
  });
  keyWith('npm version', (i) => {
    i.npmVersion = '11.9.1';
  });
  keyWith('install argv', (i) => {
    i.installArgv = ['npm', 'ci', '--no-audit'];
  });
  keyWith('npm_config value', (i) => {
    i.env.npm_config_cache = '/elsewhere';
  });
  keyWith('effective npm ignore-scripts', (i) => {
    i.npmConfig = { ignoreScripts: 'true', scriptShell: 'null' };
  });
  keyWith('effective npm script-shell', (i) => {
    i.npmConfig = { ignoreScripts: 'false', scriptShell: '/bin/dash' };
  });
  keyWith('temp dir filesystem type', (i) => {
    i.tempFsProps = () => 'tmpfs|';
  });
  keyWith('temp dir mounted noexec', (i) => {
    i.tempFsProps = () => 'ext4|noexec';
  });
  keyWith('arbitrary env var', (i) => {
    i.env.CXX = 'clang++';
  });
  keyWith('userconfig bytes', () => rewrite(f.userconfig, 'registry=http://other.invalid\n'));
  keyWith('globalconfig bytes', () => rewrite(f.globalconfig, 'registry=http://other.invalid\n'));

  for (const [name, key] of Object.entries(changed)) assert.notEqual(key, base, `changing "${name}" must change the key`);
  assert.equal(new Set(Object.values(changed)).size, Object.keys(changed).length, 'each input changes the key to its own value');
  assert.equal(computeDepsKey(f.inputs()).key, base, 'restoring every input restores the key');
});

test('key: the root .npmrc is an input', () => {
  const f = makeKeyFixture();
  const sub = path.join(f.root, 'sub');
  fs.mkdirSync(sub);
  for (const n of ['package.json', 'package-lock.json']) fs.copyFileSync(path.join(f.root, n), path.join(sub, n));
  const before = computeDepsKey({ ...f.inputs(), dir: sub }).key;
  fs.writeFileSync(path.join(f.root, '.npmrc'), 'root=1\n');
  assert.notEqual(computeDepsKey({ ...f.inputs(), dir: sub }).key, before);
});

test('key: the compiler versions only matter when some lock entry has an install script', () => {
  const plain = makeKeyFixture();
  const i = plain.inputs();
  const a = computeDepsKey(i).key;
  i.toolVersion = () => 'something else entirely';
  assert.equal(computeDepsKey(i).key, a);
});

test('eligibility: only a pinned v2/v3 lock is cached; every other source is refused with its reason', () => {
  const ok = {
    lockfileVersion: 3,
    packages: {
      '': { name: 'a' },
      'node_modules/reg': REGISTRY_ENTRY('1.0.0'),
      'node_modules/git': { version: '1.0.0', resolved: 'git+ssh://git@github.com/o/r.git#0123456789abcdef0123456789abcdef01234567' },
      'node_modules/tgz': { version: '1.0.0', resolved: 'file:vendor/jun-client/jun-1.0.0.tgz', integrity: 'sha512-t' },
    },
  };
  const verdict = lockEligibility(ok);
  assert.equal(verdict.ok, true);
  assert.deepEqual(verdict.tarballs, ['vendor/jun-client/jun-1.0.0.tgz']);

  const withEntry = (name, entry) => ({ lockfileVersion: 3, packages: { '': {}, [name]: entry } });
  const refused = {
    'lockfile v1': { lockfileVersion: 1, dependencies: {} },
    'no packages map': { lockfileVersion: 3 },
    'file: dir': withEntry('node_modules/d', { version: '1.0.0', resolved: 'file:../d' }),
    'link': withEntry('node_modules/l', { resolved: 'packages/l', link: true }),
    'workspace member': withEntry('packages/w', { version: '1.0.0' }),
    'missing resolved': withEntry('node_modules/m', { version: '1.0.0', integrity: 'sha512-abc' }),
    'registry without integrity': withEntry('node_modules/r', { version: '1.0.0', resolved: 'https://registry.npmjs.org/r/-/r-1.0.0.tgz' }),
    'other registry': withEntry('node_modules/o', { version: '1.0.0', resolved: 'https://npm.example.com/o.tgz', integrity: 'sha512-abc' }),
    'http registry': withEntry('node_modules/h', { version: '1.0.0', resolved: 'http://registry.npmjs.org/h.tgz', integrity: 'sha512-abc' }),
    'git branch, not a sha': withEntry('node_modules/g', { version: '1.0.0', resolved: 'git+ssh://git@github.com/o/r.git#main' }),
    'file tarball without integrity': withEntry('node_modules/f', { version: '1.0.0', resolved: 'file:f.tgz' }),
  };
  for (const [name, lock] of Object.entries(refused)) {
    const result = lockEligibility(lock);
    assert.equal(result.ok, false, name);
    assert.ok(result.reason.length > 0, name);
  }
});

test('eligibility: a bundled entry with no resolved needs an enclosing package that bundles it and itself passes', () => {
  const lockOf = (packages) => ({ lockfileVersion: 3, packages: { '': {}, ...packages } });
  const parent = { version: '1.0.0', resolved: 'https://registry.npmjs.org/p/-/p-1.0.0.tgz', integrity: 'sha512-p', bundleDependencies: ['c', '@s/d'] };

  assert.equal(lockEligibility(lockOf({ 'node_modules/p': parent, 'node_modules/p/node_modules/c': { version: '1.0.0', inBundle: true } })).ok, true, 'named in the parent\'s bundleDependencies');
  assert.equal(lockEligibility(lockOf({ 'node_modules/p': parent, 'node_modules/p/node_modules/@s/d': { version: '1.0.0', inBundle: true } })).ok, true, 'scoped child');
  assert.equal(lockEligibility(lockOf({ 'node_modules/p': { ...parent, bundleDependencies: undefined }, 'node_modules/p/node_modules/c': { version: '1', inBundle: true, inDepBundle: true } })).ok, true, 'inDepBundle');

  const refused = {
    'top-level inBundle with no resolved (a root-bundled dep gets the normal checks)': { 'node_modules/r': { version: '1.0.0', inBundle: true } },
    'root-bundled local tarball without integrity': { 'node_modules/r': { version: '1.0.0', inBundle: true, resolved: 'file:r.tgz' } },
    'child of a parent that does not bundle it': { 'node_modules/p': { ...parent, bundleDependencies: ['other'] }, 'node_modules/p/node_modules/c': { version: '1', inBundle: true } },
    'child of a parent that fails the source checks': {
      'node_modules/p': { ...parent, resolved: 'file:../p' },
      'node_modules/p/node_modules/c': { version: '1', inBundle: true },
    },
    'child with no parent entry at all': { 'node_modules/p/node_modules/c': { version: '1', inBundle: true } },
    'child that is not marked inBundle': { 'node_modules/p': parent, 'node_modules/p/node_modules/c': { version: '1' } },
  };
  for (const [name, packages] of Object.entries(refused)) assert.equal(lockEligibility(lockOf(packages)).ok, false, name);

  const withResolved = lockEligibility(lockOf({ 'node_modules/r': { version: '1', inBundle: true, resolved: 'file:r.tgz', integrity: 'sha512-r' } }));
  assert.deepEqual(withResolved.tarballs, ['r.tgz'], 'a root-bundled local tarball is checked like any other, so its bytes are hashed');
});

test('key: the bytes of a root-bundled local tarball are part of the key', () => {
  const f = makeKeyFixture({
    lockPackages: { '': {}, 'node_modules/r': { version: '1.0.0', inBundle: true, resolved: 'file:x-1.0.0.tgz', integrity: 'sha512-r' } },
    tarball: 'one',
  });
  const a = computeDepsKey(f.inputs()).key;
  assert.ok(a);
  fs.writeFileSync(path.join(f.root, 'x-1.0.0.tgz'), 'two');
  assert.notEqual(computeDepsKey(f.inputs()).key, a);
});

test('key: an ineligible lock or a missing tarball is not cacheable and says why', () => {
  const f = makeKeyFixture({ lockPackages: { '': {}, 'node_modules/d': { version: '1.0.0', resolved: 'file:../d' } } });
  const refused = computeDepsKey(f.inputs());
  assert.equal(refused.key, null);
  assert.match(refused.reason, /node_modules\/d resolves to an unpinned source/);

  const noTarball = makeKeyFixture({ lockPackages: { '': {}, 'node_modules/t': { version: '1.0.0', resolved: 'file:x-1.0.0.tgz', integrity: 'sha512-t' } } });
  assert.match(computeDepsKey(noTarball.inputs()).reason, /tarball x-1\.0\.0\.tgz is not in the snapshot/);

  const none = makeKeyFixture();
  fs.rmSync(path.join(none.root, 'package-lock.json'));
  assert.deepEqual(computeDepsKey(none.inputs()), { key: null, reason: 'no lockfile' });
  fs.writeFileSync(path.join(none.root, 'package-lock.json'), '{not json');
  assert.match(computeDepsKey(none.inputs()).reason, /not valid JSON/);
});

test('key: a root lifecycle script outside the allowlist is not cacheable unless the lane declares it safe', () => {
  assert.ok(ALLOWED_ROOT_SCRIPTS.has('git config core.hooksPath .githooks || true'));
  const allowed = makeKeyFixture({ scripts: { prepare: 'git config core.hooksPath .githooks || true', test: 'node --test' } });
  assert.ok(computeDepsKey(allowed.inputs()).key, 'the known prepare script and non-install scripts are fine');

  // every root script `npm ci` can run (npm 11.9.0: arborist reify.js, then lib/commands/ci.js), spelled out here on purpose
  const EVERY_NPM_CI_ROOT_SCRIPT = ['predependencies', 'dependencies', 'postdependencies', 'preinstall', 'install', 'postinstall', 'prepublish', 'preprepare', 'prepare', 'postprepare'];
  assert.deepEqual(ROOT_SCRIPT_NAMES, EVERY_NPM_CI_ROOT_SCRIPT, 'the gate covers the complete set, in npm\'s order');
  for (const name of EVERY_NPM_CI_ROOT_SCRIPT) {
    const f = makeKeyFixture({ scripts: { [name]: 'husky install' } });
    const result = computeDepsKey(f.inputs());
    assert.equal(result.key, null, name);
    assert.match(result.reason, new RegExp(`${name}: husky install`));
    assert.ok(computeDepsKey({ ...f.inputs(), rootScriptsSafe: true }).key, `${name} is cacheable once the lane declares root scripts safe`);
  }
  const near = makeKeyFixture({ scripts: { prepare: 'git config core.hooksPath .githooks' } });
  assert.equal(computeDepsKey(near.inputs()).key, null, 'the allowlist is exact strings, not a prefix');
});

test('optional completeness: lock entries for this platform that npm did not install are found', () => {
  const lock = {
    packages: {
      '': {},
      'node_modules/a': { version: '1' },
      'node_modules/@s/b': { version: '1' },
      'node_modules/mac-only': { version: '1', os: ['darwin'], optional: true },
      'node_modules/not-linux': { version: '1', os: ['!linux'], optional: true },
      'node_modules/arm-only': { version: '1', cpu: ['arm64'], optional: true },
      'node_modules/musl-only': { version: '1', os: ['linux'], libc: ['musl'], optional: true },
      'node_modules/glibc-only': { version: '1', os: ['linux'], libc: ['glibc'], optional: true },
    },
  };
  const here = { platform: 'linux', arch: 'x64', libc: 'glibc' };
  assert.deepEqual(findMissingInstalled(lock, { 'node_modules/a': {}, 'node_modules/@s/b': {}, 'node_modules/glibc-only': {} }, here), []);
  assert.deepEqual(findMissingInstalled(lock, { 'node_modules/a': {} }, here), ['@s/b', 'glibc-only']);
  assert.deepEqual(findMissingInstalled(lock, { 'node_modules/a': {}, 'node_modules/@s/b': {} }, { platform: 'darwin', arch: 'arm64', libc: undefined }), ['arm-only', 'mac-only', 'not-linux'], 'on macOS: mac-only and arm-only apply, the linux ones do not');
});

// Shape of rouge's lockfile: dmg-license is darwin-only and optional, its own dependencies are optional with no `os`.
const dmgLicenseLock = () => ({
  packages: {
    '': { dependencies: { app: '1' } },
    'node_modules/app': { version: '1', dependencies: { xmlbuilder: '*' } },
    'node_modules/xmlbuilder': { version: '1' },
    'node_modules/dmg-license': {
      version: '1',
      dev: true,
      optional: true,
      os: ['darwin'],
      dependencies: { '@types/plist': '^3', 'assert-plus': '^1', 'cli-truncate': '^2' },
    },
    'node_modules/@types/plist': { version: '3', dev: true, optional: true, dependencies: { xmlbuilder: '>=11' } },
    'node_modules/assert-plus': { version: '1', dev: true, optional: true },
    'node_modules/cli-truncate': { version: '2', dev: true, optional: true, dependencies: { 'slice-ansi': '^3' } },
    'node_modules/slice-ansi': { version: '3', dev: true, optional: true },
  },
});
const linuxHost = { platform: 'linux', arch: 'x64', libc: 'glibc' };

test('optional completeness: the dependencies of a platform-skipped optional package are not missing (BRAIN-402)', () => {
  const installed = { 'node_modules/app': {}, 'node_modules/xmlbuilder': {} };
  assert.deepEqual(findMissingInstalled(dmgLicenseLock(), installed, linuxHost), []);
  assert.deepEqual(
    findMissingInstalled(dmgLicenseLock(), installed, { platform: 'darwin', arch: 'arm64', libc: undefined }),
    ['@types/plist', 'assert-plus', 'cli-truncate', 'dmg-license', 'slice-ansi'],
    'on macOS the chain applies, so its absence is real',
  );
});

test('optional completeness: an absent optional package with an installed dependent is still missing', () => {
  const lock = dmgLicenseLock();
  lock.packages['node_modules/shared-opt'] = { version: '1', optional: true };
  lock.packages['node_modules/dmg-license'].dependencies['shared-opt'] = '*';
  lock.packages['node_modules/app'].dependencies['shared-opt'] = '*';
  const installed = { 'node_modules/app': {}, 'node_modules/xmlbuilder': {} };
  assert.deepEqual(findMissingInstalled(lock, installed, linuxHost), ['shared-opt']);
  lock.packages['node_modules/shared-opt'].optional = false;
  delete lock.packages['node_modules/app'].dependencies['shared-opt'];
  assert.deepEqual(findMissingInstalled(lock, installed, linuxHost), ['shared-opt'], 'a required package is missing even when only skipped packages depend on it');
});

test('optional completeness: only dependents that resolve to the exact lock path count', () => {
  const lock = {
    packages: {
      '': { dependencies: { app: '1', 'mac-tool': '1' } },
      'node_modules/app': { version: '1', dependencies: { dep: '2' } },
      'node_modules/app/node_modules/dep': { version: '2', optional: true },
      'node_modules/mac-tool': { version: '1', optional: true, os: ['darwin'], dependencies: { dep: '1' } },
      'node_modules/dep': { version: '1', optional: true },
    },
  };
  const installed = { 'node_modules/app': {} };
  assert.deepEqual(findMissingInstalled(lock, installed, linuxHost), ['app/node_modules/dep'], 'app resolves its nested dep; the hoisted copy only serves the skipped mac-tool');
  assert.deepEqual(findMissingInstalled(lock, { ...installed, 'node_modules/app/node_modules/dep': {} }, linuxHost), []);
});

test('optional completeness: a multi-level chain under a platform-skipped package (rouge: dmg-license > verror > assert-plus) is expected, and a required one is not', () => {
  const lock = dmgLicenseLock();
  Object.assign(lock.packages['node_modules/dmg-license'].dependencies, { verror: '^1' });
  Object.assign(lock.packages, {
    'node_modules/verror': { version: '1', dev: true, optional: true, dependencies: { 'assert-plus': '^1', 'core-util-is': '1' } },
    'node_modules/core-util-is': { version: '1', dev: true, optional: true },
    'node_modules/@img/sharp-wasm32': { version: '1', optional: true, cpu: ['wasm32'], dependencies: { '@emnapi/runtime': '^1' } },
    'node_modules/@emnapi/runtime': { version: '1', optional: true },
  });
  lock.packages['node_modules/app'].optionalDependencies = { '@img/sharp-wasm32': '1' };
  const installed = { 'node_modules/app': {}, 'node_modules/xmlbuilder': {} };
  assert.deepEqual(findMissingInstalled(lock, installed, linuxHost), []);
  lock.packages['node_modules/core-util-is'].optional = false;
  assert.deepEqual(findMissingInstalled(lock, installed, linuxHost), ['core-util-is'], 'a required package absent is still missing');
});

// ---- publish / materialize ----

test('materialize reproduces the stored tree, including the .bin symlink, and the linked tool runs', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key, src } = publish(cacheRoot);
  const work = tmpDir('deps-cache-work');
  materializeFromStore(cacheRoot, key, path.join(work, 'node_modules'));
  assert.equal(treeDigest(path.join(work, 'node_modules')), treeDigest(src));
  assert.equal(fs.readlinkSync(path.join(work, 'node_modules', '.bin', 'tool')), '../pkg0/cli.js');
  const out = execFileSync(path.join(work, 'node_modules', '.bin', 'tool'), { encoding: 'utf8' });
  assert.equal(out.trim(), 'tool ran');
});

test('materialize makes private writable copies: no shared inodes, read-only store, writable farm', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  const work = tmpDir('deps-cache-work');
  const copy = path.join(work, 'node_modules');
  materializeFromStore(cacheRoot, key, copy);
  const stored = path.join(entryTree(cacheRoot, key), 'pkg1', 'package.json');
  const mine = path.join(copy, 'pkg1', 'package.json');
  assert.notEqual(fs.statSync(mine).ino, fs.statSync(stored).ino, 'a copy, never a hardlink');
  assert.equal(fs.statSync(mine).nlink, 1);
  assert.equal(fs.statSync(stored).nlink, 1);
  assert.ok((fs.statSync(mine).mode & 0o200) !== 0, 'copied files are owner-writable');
  assert.ok((fs.statSync(path.join(copy, 'pkg1')).mode & 0o200) !== 0, 'copied directories are owner-writable');
  assert.equal(fs.statSync(path.join(copy, 'pkg0', 'cli.js')).mode & 0o111, 0o111, 'the exec bit survives');
  assert.ok((fs.statSync(path.join(entryTree(cacheRoot, key), 'pkg1')).mode & 0o222) === 0, 'store directories are read-only');
  assert.ok((fs.statSync(stored).mode & 0o222) === 0, 'store files are read-only');
});

test('an in-place edit of a materialized file changes only that run\'s copy', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key, src } = publish(cacheRoot);
  const before = treeDigest(src);
  const work = tmpDir('deps-cache-work');
  const copy = path.join(work, 'node_modules');
  materializeFromStore(cacheRoot, key, copy);
  fs.appendFileSync(path.join(copy, 'pkg3', 'package.json'), ' edited');
  assert.notEqual(treeDigest(copy), before);
  assert.equal(treeDigest(entryTree(cacheRoot, key)), before, 'the store is byte-identical');
});

test('a run that chmods a file, edits it and restores the mode cannot affect another run\'s hit', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key, src } = publish(cacheRoot);
  const before = treeDigest(src);
  const [workA, workB] = [tmpDir('deps-cache-work-a'), tmpDir('deps-cache-work-b')];
  const copyA = path.join(workA, 'node_modules');
  const copyB = path.join(workB, 'node_modules');
  materializeFromStore(cacheRoot, key, copyA);

  // the attack on a hardlink farm: make the shared inode writable, edit it, put the mode back
  const target = path.join(copyA, 'pkg5', 'lib', 'deep', 'index.js');
  const mode = fs.statSync(target).mode & 0o7777;
  fs.chmodSync(target, 0o666);
  fs.writeFileSync(target, 'module.exports = "poisoned";\n');
  fs.chmodSync(target, mode & ~0o222);

  materializeFromStore(cacheRoot, key, copyB);
  assert.equal(treeDigest(copyB), before, 'the second run gets the original bytes');
  assert.equal(treeDigest(entryTree(cacheRoot, key)), before, 'and the store was never touched');
  assert.equal(fs.readFileSync(path.join(copyB, 'pkg5', 'lib', 'deep', 'index.js'), 'utf8'), 'module.exports = 5;\n');
});

test('rename-over, new files and new directories work in a materialized tree and never reach the store', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key, src } = publish(cacheRoot);
  const before = treeDigest(src);
  const work = tmpDir('deps-cache-work');
  const farm = path.join(work, 'node_modules');
  materializeFromStore(cacheRoot, key, farm);

  const replacement = path.join(work, 'replacement.json');
  fs.writeFileSync(replacement, '{"name":"replaced"}');
  fs.renameSync(replacement, path.join(farm, 'pkg2', 'package.json'));
  fs.mkdirSync(path.join(farm, '.cache', 'babel'), { recursive: true });
  fs.writeFileSync(path.join(farm, '.cache', 'babel', 'out.json'), '{}');
  fs.writeFileSync(path.join(farm, 'pkg2', 'new-file.js'), 'x');
  fs.rmSync(path.join(farm, 'pkg4'), { recursive: true });

  assert.equal(fs.readFileSync(path.join(farm, 'pkg2', 'package.json'), 'utf8'), '{"name":"replaced"}');
  assert.equal(treeDigest(entryTree(cacheRoot, key)), before, 'the store never saw any of it');
});

test('materializing onto an existing node_modules fails instead of merging into it', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  const work = tmpDir('deps-cache-work');
  fs.mkdirSync(path.join(work, 'node_modules'));
  assert.throws(() => materializeFromStore(cacheRoot, key, path.join(work, 'node_modules')), { code: 'EEXIST' });
});

test('publish stores a read-only copy that does not share inodes with the installed tree', { skip: IS_ROOT && 'root ignores file modes' }, () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key, src } = publish(cacheRoot);
  const installedFile = path.join(src, 'pkg0', 'package.json');
  const storedFile = path.join(entryTree(cacheRoot, key), 'pkg0', 'package.json');
  assert.notEqual(fs.statSync(installedFile).ino, fs.statSync(storedFile).ino);
  fs.writeFileSync(installedFile, 'the lane keeps writing to its own tree');
  assert.notEqual(fs.readFileSync(storedFile, 'utf8'), 'the lane keeps writing to its own tree');
  assert.throws(() => fs.writeFileSync(storedFile, 'x'), { code: 'EACCES' });
  assert.equal(fs.statSync(path.join(entryTree(cacheRoot, key), 'pkg0', 'cli.js')).mode & 0o111, 0o111, 'the exec bit survives');
});

test('publishing a key that already exists loses the race cleanly and leaves no temp dir', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const first = publish(cacheRoot);
  const second = publishToStore(cacheRoot, first.key, makeTree(tmpDir('deps-cache-src'), { packages: 3 }));
  assert.equal(second.published, false);
  assert.equal(treeDigest(entryTree(cacheRoot, first.key)), treeDigest(first.src), 'the first publisher\'s tree is untouched');
  assert.deepEqual(fs.readdirSync(cacheRoot).filter((n) => n.startsWith('.tmp-')), []);
});

test('two real processes publishing the same key at once: one wins, the store is intact, nothing is left behind', async () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const key = newKey();
  const srcs = [makeTree(tmpDir('deps-cache-src-a'), { packages: 150 }), makeTree(tmpDir('deps-cache-src-b'), { packages: 150 })];
  srcs.forEach((s, i) => fs.writeFileSync(path.join(s, 'marker'), `publisher ${i}`));
  const startAt = Date.now() + 600;
  const script = `
    import { publishToStore } from ${JSON.stringify(DEPS_CACHE_URL)};
    const [cacheRoot, key, src, startAt] = process.argv.slice(1);
    while (Date.now() < Number(startAt)) { /* spin to a common start line */ }
    console.log(JSON.stringify(publishToStore(cacheRoot, key, src)));
  `;
  const runOne = (src) =>
    new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ['--input-type=module', '-e', script, cacheRoot, key, src, String(startAt)]);
      let out = '';
      let err = '';
      child.stdout.on('data', (d) => (out += d));
      child.stderr.on('data', (d) => (err += d));
      child.on('close', (code) => (code === 0 ? resolve(JSON.parse(out)) : reject(new Error(`publisher exited ${code}: ${err}`))));
    });
  const results = await Promise.all(srcs.map(runOne));

  assert.equal(results.filter((r) => r.published).length, 1, `exactly one publisher wins: ${JSON.stringify(results)}`);
  assert.deepEqual(fs.readdirSync(cacheRoot).sort(), [key], 'no .tmp-* left by the loser');
  const winner = [srcs[0], srcs[1]].map(treeDigest).includes(treeDigest(entryTree(cacheRoot, key)));
  assert.ok(winner, 'the store holds exactly one publisher\'s complete tree, not a mix');
  assert.ok(fs.existsSync(path.join(entryDir(cacheRoot, key), 'meta.json')));
});

// ---- eviction ----

function ageEntry(cacheRoot, key, ageMs) {
  const when = new Date(Date.now() - ageMs);
  fs.utimesSync(path.join(entryDir(cacheRoot, key), '.last-used'), when, when);
}

function liveKeys(cacheRoot) {
  return fs.readdirSync(cacheRoot).filter((n) => /^[0-9a-f]{64}$/.test(n));
}

test('eviction trims least-recently-used entries until the store fits its bound', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const entries = [1, 2, 3, 4].map(() => publish(cacheRoot, { bytes: 10_000 }));
  entries.forEach((e, i) => ageEntry(cacheRoot, e.key, (4 - i) * 60_000)); // entries[0] is the oldest
  const perEntry = entries[0].bytes;
  const bound = perEntry * 2 + 1;

  const evicted = evictLeastRecentlyUsed(cacheRoot, bound);
  purgeTrash(cacheRoot);

  assert.deepEqual(evicted, [entries[0].key, entries[1].key], 'the two oldest go, oldest first');
  assert.deepEqual(liveKeys(cacheRoot).sort(), [entries[2].key, entries[3].key].sort());
  assert.equal(fs.readdirSync(cacheRoot).filter((n) => n.startsWith('.trash-')).length, 0, 'trash is purged even though its directories are read-only');
  const remaining = liveKeys(cacheRoot).reduce((sum, k) => sum + JSON.parse(fs.readFileSync(path.join(entryDir(cacheRoot, k), 'meta.json'), 'utf8')).bytes, 0);
  assert.ok(remaining <= bound, `${remaining} bytes remain within the bound of ${bound}`);
});

test('eviction leaves a store that is already within its bound alone', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const entries = [1, 2].map(() => publish(cacheRoot));
  assert.deepEqual(evictLeastRecentlyUsed(cacheRoot, entries[0].bytes * 10), []);
  assert.equal(liveKeys(cacheRoot).length, 2);
});

test('using an entry (touchLastUsed) makes it the newest, so the next-oldest is evicted instead', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const [a, b, c] = [1, 2, 3].map(() => publish(cacheRoot));
  ageEntry(cacheRoot, a.key, 3 * 60_000);
  ageEntry(cacheRoot, b.key, 2 * 60_000);
  ageEntry(cacheRoot, c.key, 60_000);
  touchLastUsed(cacheRoot, a.key);
  assert.deepEqual(evictLeastRecentlyUsed(cacheRoot, a.bytes * 2 + 1), [b.key]);
});

function makeLock() {
  const lockRoot = tmpDir('deps-cache-lock');
  return (fn) => withLock(lockRoot, fn);
}

test('eviction never takes a key a live run holds a lease on, even when it is the oldest and the store is over bound', async () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const lock = makeLock();
  const [oldest, middle, newest] = [1, 2, 3].map(() => publish(cacheRoot));
  ageEntry(cacheRoot, oldest.key, 3 * 60_000);
  ageEntry(cacheRoot, middle.key, 2 * 60_000);
  ageEntry(cacheRoot, newest.key, 60_000);
  const release = await acquireLease(cacheRoot, oldest.key, 1, lock);

  const evicted = evictLeastRecentlyUsed(cacheRoot, 1);
  assert.ok(!evicted.includes(oldest.key), 'the leased key survives');
  assert.deepEqual(evicted, [middle.key, newest.key], 'eviction moves on to the next-oldest');
  assert.deepEqual(liveKeys(cacheRoot), [oldest.key]);

  await release();
  assert.deepEqual(liveKeys(cacheRoot), [], 'releasing the lease re-runs the trim: the key that was over bound only because it was leased goes now');
  assert.deepEqual(fs.readdirSync(cacheRoot).filter((n) => n.startsWith('.trash-')), [], 'and the trash is purged');
});

test('a lease file is written whole (no temp file left behind) and names a live process', async () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  const release = await acquireLease(cacheRoot, key, 1 << 30, makeLock());
  const leases = fs.readdirSync(path.join(entryDir(cacheRoot, key), 'leases'));
  assert.equal(leases.length, 1);
  assert.equal(JSON.parse(fs.readFileSync(path.join(entryDir(cacheRoot, key), 'leases', leases[0]), 'utf8')).pid, process.pid);
  assert.deepEqual(fs.readdirSync(entryDir(cacheRoot, key)).filter((n) => n.startsWith('.lease-tmp-')), []);
  await release();
});

test('a lease that cannot be read counts as live and protects its key', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  fs.writeFileSync(path.join(entryDir(cacheRoot, key), 'leases', 'torn'), '{"pid": 12');
  assert.deepEqual(evictLeastRecentlyUsed(cacheRoot, 0), []);
  fs.writeFileSync(path.join(entryDir(cacheRoot, key), 'leases', 'torn'), '{"no":"pid"}');
  assert.deepEqual(evictLeastRecentlyUsed(cacheRoot, 0), []);
  assert.deepEqual(liveKeys(cacheRoot), [key]);
});

test('lease versus eviction: eviction is mid-quarantine when a run tries to lease; the lease waits, then finds the entry gone (a miss), never a lease inside the trash', async () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const lock = makeLock();
  const { key } = publish(cacheRoot);
  const order = [];
  let leasing;
  await lock(() => {
    evictLeastRecentlyUsed(cacheRoot, 0, {
      beforeQuarantine: () => {
        // the run arrives exactly between eviction's lease check and its move
        leasing = acquireLease(cacheRoot, key, 0, lock).then(
          () => order.push('lease-acquired'),
          (err) => order.push(`lease-failed:${err.code}`),
        );
        order.push('quarantine');
      },
    });
    order.push('eviction-done');
  });
  await leasing;
  assert.deepEqual(order, ['quarantine', 'eviction-done', 'lease-failed:ENOENT'], 'the lease could not slip in before the move');
  const trashed = fs.readdirSync(cacheRoot).filter((n) => n.startsWith('.trash-'));
  for (const t of trashed) assert.deepEqual(fs.readdirSync(path.join(cacheRoot, t, 'leases')), [], 'no lease ended up inside the quarantined entry');
});

test('lease versus eviction, the other order: a lease taken first protects the key from the whole eviction', async () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const lock = makeLock();
  const { key } = publish(cacheRoot);
  const release = await acquireLease(cacheRoot, key, 0, lock);
  await trimStore(cacheRoot, 0, lock);
  assert.deepEqual(liveKeys(cacheRoot), [key]);
  await release();
  assert.deepEqual(liveKeys(cacheRoot), []);
});

test('a lease left by a dead process does not protect its key', async () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  const dead = spawn(process.execPath, ['-e', '']);
  await new Promise((resolve) => dead.on('close', resolve));
  fs.writeFileSync(path.join(entryDir(cacheRoot, key), 'leases', `${dead.pid}-stale`), JSON.stringify({ pid: dead.pid, start: null }));
  assert.deepEqual(evictLeastRecentlyUsed(cacheRoot, 0), [key]);
});

test('eviction clears a crashed publisher\'s stale .tmp dir but not a fresh one', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const stale = path.join(cacheRoot, '.tmp-1-stale');
  const fresh = path.join(cacheRoot, '.tmp-2-fresh');
  for (const d of [stale, fresh]) fs.mkdirSync(path.join(d, 'node_modules'), { recursive: true });
  const old = new Date(Date.now() - 3 * 60 * 60 * 1000);
  fs.utimesSync(stale, old, old);
  evictLeastRecentlyUsed(cacheRoot, 0);
  purgeTrash(cacheRoot);
  assert.equal(fs.existsSync(stale), false);
  assert.equal(fs.existsSync(fresh), true);
});

test('removeTree removes a read-only store entry', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  removeTree(entryDir(cacheRoot, key));
  assert.equal(fs.existsSync(entryDir(cacheRoot, key)), false);
});

// ---- postinstall safety ----

test('the outside-node_modules snapshot sees added, changed and removed files and ignores node_modules and sees .git', () => {
  const dir = tmpDir('deps-cache-outside');
  fs.writeFileSync(path.join(dir, 'package.json'), '{}');
  fs.mkdirSync(path.join(dir, 'src'));
  fs.writeFileSync(path.join(dir, 'src', 'a.js'), 'a');
  const before = snapshotOutsideNodeModules(dir);

  fs.mkdirSync(path.join(dir, 'node_modules', 'x'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'node_modules', 'x', 'index.js'), 'ignored');

  assert.deepEqual(snapshotChanges(before, snapshotOutsideNodeModules(dir)), []);

  fs.writeFileSync(path.join(dir, 'generated.txt'), 'new');
  fs.writeFileSync(path.join(dir, 'src', 'a.js'), 'changed size');
  fs.rmSync(path.join(dir, 'package.json'));
  assert.deepEqual(snapshotChanges(before, snapshotOutsideNodeModules(dir)), ['generated.txt', 'package.json', 'src/a.js']);
});

test('the allowlisted git-config script explains exactly its own .git/config line, and nothing else under .git', () => {
  const scripts = { prepare: 'git config core.hooksPath .githooks || true' };
  const base = '[core]\n\trepositoryformatversion = 0\n\tbare = false\n';
  const withHooks = `${base}\thooksPath = .githooks\n`;
  const changes = ['.git', '.git/config'];
  assert.deepEqual(unexplainedChanges(changes, { scripts, configBefore: base, configAfter: withHooks }), []);
  assert.deepEqual(unexplainedChanges(changes, { scripts, configBefore: withHooks, configAfter: `${base}\thooksPath = .githooks\n` }), [], 'rewriting the same value');

  assert.deepEqual(unexplainedChanges([...changes, '.git/hooks', '.git/hooks/pre-commit'], { scripts, configBefore: base, configAfter: withHooks }), ['.git/hooks', '.git/hooks/pre-commit'], 'a new hook file is not explained');
  assert.deepEqual(unexplainedChanges(changes, { scripts, configBefore: base, configAfter: `${withHooks}\tsshCommand = evil\n` }), changes, 'another config line is not explained');
  assert.deepEqual(unexplainedChanges(changes, { scripts, configBefore: base, configAfter: `${base}\thooksPath = /tmp/evil\n` }), changes, 'a different hooksPath is not explained');
  assert.deepEqual(unexplainedChanges(changes, { scripts: {}, configBefore: base, configAfter: withHooks }), changes, 'without the allowlisted script nothing under .git is explained');
  assert.deepEqual(unexplainedChanges(['generated.txt', '.git/config'], { scripts, configBefore: base, configAfter: withHooks }), ['generated.txt']);
  assert.deepEqual(allowlistedRootEvents({ prepare: scripts.prepare, postinstall: 'husky install', prepublish: scripts.prepare }), ['prepublish', 'prepare']);
});

test('scrubDepsEnv removes shell and lane variables, and keeps the ssh/auth and temp-dir ones the install needs', () => {
  const scrubbed = scrubDepsEnv({ PATH: '/bin', HOME: '/h', TMPDIR: '/var/tmp/lb-1', TMP: '/t', TEMP: '/t', PWD: '/p', OLDPWD: '/o', SHLVL: '2', _: '/x', GIT_CEILING_DIRECTORIES: '/c', SSH_CONNECTION: 'x', SSH_AUTH_SOCK: '/agent', GIT_SSH_COMMAND: 'ssh -i k', LANE_BROKER_LEASE: 'z', LANE_OMIT: 'dev', npm_config_cache: '/c' });
  assert.deepEqual(Object.keys(scrubbed).sort(), ['GIT_SSH_COMMAND', 'HOME', 'PATH', 'SSH_AUTH_SOCK', 'SSH_CONNECTION', 'TEMP', 'TMP', 'TMPDIR', 'npm_config_cache']);
});

test('pinInstallLocale leaves only the pinned LC_ALL; the submitter env it was derived from is untouched (BRAIN-429)', () => {
  const submitter = { PATH: '/bin', LC_ALL: 'en_US.UTF-8', LC_CTYPE: 'C', LC_TIME: 'fr_FR', LANG: 'de_DE', LANGUAGE: 'de' };
  const install = pinInstallLocale(submitter, 'C.UTF-8');
  assert.deepEqual(install, { PATH: '/bin', LANGUAGE: 'de', LC_ALL: 'C.UTF-8' });
  assert.equal(submitter.LC_CTYPE, 'C', 'the command env keeps the submitter locale');
});

const MOUNTINFO = [
  '22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro',
  '30 22 0:25 / /tmp rw,nosuid,nodev shared:2 - tmpfs tmpfs rw,size=1000k',
  '31 22 0:26 / /var/tmp rw,noexec,nosuid,relatime shared:3 - ext4 /dev/sdb1 rw',
  '32 31 0:27 / /var/tmp/my\\040space ro,relatime shared:4 - xfs /dev/sdc1 rw',
].join('\n');

test('temp dir filesystem properties come from the longest covering mount: fs type and noexec/nosuid/nodev/ro', () => {
  assert.equal(parseMountInfo(MOUNTINFO, '/home/runner'), 'ext4|');
  assert.equal(parseMountInfo(MOUNTINFO, '/tmp/lb-1'), 'tmpfs|nodev,nosuid');
  assert.equal(parseMountInfo(MOUNTINFO, '/var/tmp/lb-1'), 'ext4|noexec,nosuid');
  assert.equal(parseMountInfo(MOUNTINFO, '/var/tmp/my space/x'), 'xfs|ro', 'escaped mount points are decoded');
  assert.equal(parseMountInfo(MOUNTINFO, '/var/tmpfoo'), 'ext4|', '/var/tmp does not cover /var/tmpfoo');

  const mac = ['/dev/disk3s1 on / (apfs, sealed, local, read-only, journaled)', '/dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled, nobrowse)', 'tmpfs on /private/tmp/x (tmpfs, local, noexec, nosuid)'].join('\n');
  assert.equal(parseMountOutput(mac, '/System/Volumes/Data/tmp'), 'apfs|');
  assert.equal(parseMountOutput(mac, '/usr/lib'), 'apfs|ro');
  assert.equal(parseMountOutput(mac, '/private/tmp/x/lb'), 'tmpfs|noexec,nosuid');
});

test('temp dir fingerprint is by variable name: swapping which variable is on the noexec mount changes it', () => {
  const exec = tmpDir('deps-cache-exec');
  const noexec = tmpDir('deps-cache-noexec');
  const real = (d) => fs.realpathSync(d);
  const table = [`1 0 0:1 / ${real(exec)} rw - ext4 /dev/a rw`, `2 0 0:2 / ${real(noexec)} rw,noexec - ext4 /dev/b rw`].join('\n');
  const nameDirs = (env) => tempDirFsProperties(env, { readMounts: () => table, platform: 'linux' });
  const a = nameDirs({ TMPDIR: exec, TMP: noexec });
  const b = nameDirs({ TMPDIR: noexec, TMP: exec });
  assert.notEqual(a, b, 'the same two dirs under swapped names are different environments');
  assert.match(a, /^TMPDIR=.*;TMP=.*;TEMP=unset$/);
  assert.notEqual(nameDirs({ TMPDIR: exec }), nameDirs({ TMPDIR: exec, TEMP: exec }), 'unset is a value');
  assert.match(nameDirs({ TMP: noexec }), /os\.tmpdir=/, 'the fallback is recorded when TMPDIR is unset');
  assert.doesNotMatch(nameDirs({ TMPDIR: exec }), /os\.tmpdir/);

  const f = makeKeyFixture();
  const keyFor = (env) => {
    const i = f.inputs();
    i.tempFsProps = () => nameDirs(env);
    return computeDepsKey(i).key;
  };
  assert.notEqual(keyFor({ TMPDIR: exec, TMP: noexec }), keyFor({ TMPDIR: noexec, TMP: exec }), 'and so are their keys');
});

test('tempDirFsProperties reads each temp dir the install sees, ignores their per-run paths, and never throws', () => {
  const dirA = tmpDir('deps-cache-tmpa');
  const dirB = tmpDir('deps-cache-tmpb');
  const mounts = () => MOUNTINFO;
  const a = tempDirFsProperties({ TMPDIR: dirA }, { readMounts: mounts, platform: 'linux' });
  const b = tempDirFsProperties({ TMPDIR: dirB }, { readMounts: mounts, platform: 'linux' });
  assert.equal(a, b, 'two different per-run paths on the same filesystem give the same properties');
  assert.match(a, new RegExp(`^TMPDIR=${parseMountInfo(MOUNTINFO, fs.realpathSync(dirA)).replace('|', '\\|')};`), 'looked up in the table by the dir\'s real path');
  assert.match(tempDirFsProperties({ TMPDIR: '/does/not/exist' }, { readMounts: mounts, platform: 'linux' }), /^TMPDIR=unknown;/);
  assert.match(tempDirFsProperties({ TMPDIR: dirA }, { readMounts: () => { throw new Error('no /proc'); }, platform: 'linux' }), /^TMPDIR=unknown;/);
});

// ---- absolute install path ----

test('install-path scan finds the path in a text file, a symlink target and a shim', () => {
  const tree = tmpDir('deps-cache-scan');
  const work = '/var/tmp/lb-x/tickets/abc/work';
  assert.equal(findInstallPathReference(makeTree(tree), [work]), null, 'a clean tree has none');

  const cases = {
    text: ['pkg1/config.json', () => fs.writeFileSync(path.join(tree, 'pkg1', 'config.json'), `{"root":"${work}/node_modules/pkg1"}`)],
    symlink: ['link-out', () => fs.symlinkSync(`${work}/node_modules/pkg2`, path.join(tree, 'link-out'))],
    shim: ['.bin/shim', () => fs.writeFileSync(path.join(tree, '.bin', 'shim'), `#!/bin/sh\nexec node "${work}/node_modules/pkg0/cli.js" "$@"\n`, { mode: 0o755 })],
  };
  for (const [name, [rel, plant]] of Object.entries(cases)) {
    plant();
    assert.equal(findInstallPathReference(tree, [work]), rel, `${name} is found`);
    fs.rmSync(path.join(tree, rel));
  }
});

test('install-path scan has no size cap: a large file is streamed, including a path straddling a chunk boundary', () => {
  const tree = tmpDir('deps-cache-scan');
  const work = '/var/tmp/lb-x/tickets/abc/work';
  const big = path.join(tree, 'native.node');
  const size = 3 * SCAN_CHUNK_BYTES + 1000;
  const plant = (offset) => {
    const buf = Buffer.alloc(size, 0x41);
    Buffer.from(work).copy(buf, offset);
    fs.writeFileSync(big, buf);
  };
  assert.ok(size > 2 * 1024 * 1024, 'bigger than any size cap the earlier design had');
  fs.writeFileSync(big, Buffer.alloc(size, 0x41));
  assert.equal(findInstallPathReference(tree, [work]), null, 'a large clean file is clean');
  for (const offset of [0, 7, SCAN_CHUNK_BYTES - 5, SCAN_CHUNK_BYTES - work.length, SCAN_CHUNK_BYTES, 2 * SCAN_CHUNK_BYTES - 1, size - work.length]) {
    plant(offset);
    assert.equal(findInstallPathReference(tree, [work]), 'native.node', `found at offset ${offset}`);
  }
});

// ---- config ----

test('config: remoteDepsCache defaults on with a 10 GiB bound, and both are validated', () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.remoteDepsCache, true);
  assert.equal(DEFAULT_GLOBAL_CONFIG.remoteDepsCacheMaxBytes, 10 * 1024 ** 3);
  const prevHome = process.env.LANE_BROKER_HOME;
  try {
    const valid = freshEnv();
    writeGlobalConfig(valid.home, { remoteDepsCache: false, remoteDepsCacheMaxBytes: 5_000_000 });
    process.env.LANE_BROKER_HOME = valid.home;
    const cfg = loadGlobalConfig();
    assert.equal(cfg.remoteDepsCache, false);
    assert.equal(cfg.remoteDepsCacheMaxBytes, 5_000_000);

    for (const bad of [{ remoteDepsCache: 'yes' }, { remoteDepsCacheMaxBytes: 0 }, { remoteDepsCacheMaxBytes: -1 }, { remoteDepsCacheMaxBytes: 1.5 }, { remoteDepsCacheMaxBytes: '10' }]) {
      const e = freshEnv();
      writeGlobalConfig(e.home, bad);
      process.env.LANE_BROKER_HOME = e.home;
      assert.throws(() => loadGlobalConfig(), ConfigError, JSON.stringify(bad));
    }
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});

test('config: a lane may opt out with remoteDepsCache false; absent means on; a non-boolean is refused', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo-deps-cache');
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: {
      default: { weight: 2, remote: true, remoteDeps: ['.'] },
      nocache: { weight: 2, remote: true, remoteDeps: ['.'], remoteDepsCache: false },
    },
    undeclaredLanes: { as: 'nocache' },
  });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'default' }).remoteDepsCache, true);
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'nocache' }).remoteDepsCache, false);
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'x', lane: 'adhoc' }).remoteDepsCache, false, 'an ad-hoc lane inherits its template\'s opt-out');

  const bad = path.join(base, 'repo-deps-cache-bad');
  writeRepoConfig(bad, { version: 1, lanes: { default: { weight: 2, remoteDepsCache: 'no' } } });
  assert.throws(() => resolveTicketConfig({ cwd: bad, repo: 'x', lane: 'default' }), ConfigError);
  const badSafe = path.join(base, 'repo-deps-cache-bad-safe');
  writeRepoConfig(badSafe, { version: 1, lanes: { default: { weight: 2, remoteDepsCacheRootScriptsSafe: 1 } } });
  assert.throws(() => resolveTicketConfig({ cwd: badSafe, repo: 'x', lane: 'default' }), ConfigError);

  const safe = path.join(base, 'repo-deps-cache-safe');
  writeRepoConfig(safe, { version: 1, lanes: { default: { weight: 2 }, s: { weight: 2, remoteDepsCacheRootScriptsSafe: true } }, undeclaredLanes: { as: 's' } });
  assert.equal(resolveTicketConfig({ cwd: safe, repo: 'x', lane: 'default' }).remoteDepsCacheRootScriptsSafe, false, 'defaults to false');
  assert.equal(resolveTicketConfig({ cwd: safe, repo: 'x', lane: 's' }).remoteDepsCacheRootScriptsSafe, true);
  assert.equal(resolveTicketConfig({ cwd: safe, repo: 'x', lane: 'adhoc' }).remoteDepsCacheRootScriptsSafe, true, 'an ad-hoc lane inherits it');
});
