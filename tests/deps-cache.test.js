import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
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
  evictLeastRecentlyUsed,
  purgeTrash,
  removeTree,
  findInstallPathReference,
  findWritableStoreFile,
  quarantineEntry,
  snapshotOutsideNodeModules,
  snapshotChanges,
} from '../src/deps-cache.js';
import { resolveTicketConfig, loadGlobalConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { freshEnv, writeRepoConfig, writeGlobalConfig } from './helpers.js';

/**
 * BRAIN-389: the installed-deps store. Everything here runs on synthetic trees (no npm, no network);
 * tests/remote-deps-cache.test.js drives the same code through a real `remote-exec` + real `npm ci`.
 */

const DEPS_CACHE_URL = new URL('../src/deps-cache.js', import.meta.url).href;
const IS_ROOT = typeof process.getuid === 'function' && process.getuid() === 0;

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
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

/** Test-only: remove a materialized farm (writable dirs, linked files) without touching store inodes. */
function removeFarm(dir) {
  fs.rmSync(dir, { recursive: true, force: true });
}

// ---- key ----

function makeKeyFixture() {
  const root = tmpDir('deps-cache-key');
  fs.writeFileSync(path.join(root, 'package.json'), '{"name":"a"}');
  fs.writeFileSync(path.join(root, 'package-lock.json'), JSON.stringify({ lockfileVersion: 3, packages: { '': { name: 'a' }, 'node_modules/x': { version: '1.0.0' } } }));
  const userconfig = path.join(root, 'userrc');
  const globalconfig = path.join(root, 'globalrc');
  fs.writeFileSync(userconfig, '');
  fs.writeFileSync(globalconfig, '');
  const inputs = () => ({
    dir: root,
    rootDir: root,
    installArgv: ['npm', 'ci', '--no-audit', '--no-fund'],
    env: { npm_config_cache: '/r/npm-cache', npm_config_userconfig: userconfig, npm_config_globalconfig: globalconfig, PATH: '/bin' },
    npmVersion: '11.9.0',
  });
  return { root, userconfig, globalconfig, inputs };
}

test('key: identical inputs give the same key, and a non-npm env var does not matter', () => {
  const f = makeKeyFixture();
  const a = computeDepsKey(f.inputs());
  const withOtherEnv = f.inputs();
  withOtherEnv.env.PATH = '/somewhere/else';
  assert.match(a.key, /^[0-9a-f]{64}$/);
  assert.equal(computeDepsKey(f.inputs()).key, a.key);
  assert.equal(computeDepsKey(withOtherEnv).key, a.key);
});

test('key: every key input changes the key', () => {
  const f = makeKeyFixture();
  const base = computeDepsKey(f.inputs()).key;
  const changed = {};
  const keyWith = (name, mutate) => {
    const inputs = f.inputs();
    const restore = mutate(inputs) ?? (() => {});
    changed[name] = computeDepsKey(inputs).key;
    restore();
  };
  const rewrite = (file, content) => {
    const had = fs.existsSync(file) ? fs.readFileSync(file) : null;
    fs.writeFileSync(file, content);
    return () => (had === null ? fs.rmSync(file) : fs.writeFileSync(file, had));
  };

  keyWith('lockfile bytes', () => rewrite(path.join(f.root, 'package-lock.json'), JSON.stringify({ lockfileVersion: 3, packages: { '': { name: 'a' }, 'node_modules/x': { version: '1.0.1' } } })));
  keyWith('lockfile name (shrinkwrap)', () => rewrite(path.join(f.root, 'npm-shrinkwrap.json'), fs.readFileSync(path.join(f.root, 'package-lock.json'))));
  keyWith('package.json', () => rewrite(path.join(f.root, 'package.json'), '{"name":"a","x":1}'));
  keyWith('dir .npmrc', () => rewrite(path.join(f.root, '.npmrc'), 'foo=bar\n'));
  const subdir = path.join(f.root, 'sub');
  fs.mkdirSync(subdir);
  fs.copyFileSync(path.join(f.root, 'package.json'), path.join(subdir, 'package.json'));
  fs.copyFileSync(path.join(f.root, 'package-lock.json'), path.join(subdir, 'package-lock.json'));
  const subBase = computeDepsKey({ ...f.inputs(), dir: subdir }).key;
  fs.writeFileSync(path.join(f.root, '.npmrc'), 'root=1\n');
  changed['root .npmrc'] = computeDepsKey({ ...f.inputs(), dir: subdir }).key;
  fs.rmSync(path.join(f.root, '.npmrc'));
  assert.notEqual(changed['root .npmrc'], subBase);
  keyWith('node version', (i) => {
    i.runtime = { version: 'v0.0.0', platform: process.platform, arch: process.arch };
  });
  keyWith('node platform', (i) => {
    i.runtime = { version: process.version, platform: 'plan9', arch: process.arch };
  });
  keyWith('node arch', (i) => {
    i.runtime = { version: process.version, platform: process.platform, arch: 'mips' };
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
  keyWith('extra npm_config var', (i) => {
    i.env.npm_config_registry = 'http://registry.invalid';
  });
  keyWith('userconfig bytes', () => rewrite(f.userconfig, 'registry=http://other.invalid\n'));
  keyWith('globalconfig bytes', () => rewrite(f.globalconfig, 'registry=http://other.invalid\n'));

  for (const [name, key] of Object.entries(changed)) {
    assert.match(key, /^[0-9a-f]{64}$/, name);
    assert.notEqual(key, base, `changing "${name}" must change the key`);
  }
  assert.equal(new Set(Object.values(changed)).size, Object.keys(changed).length, 'each input changes the key to its own value');
  assert.equal(computeDepsKey(f.inputs()).key, base, 'restoring every input restores the key');
});

test('key: no lockfile, or a lockfile that links local packages, is not cacheable', () => {
  const f = makeKeyFixture();
  const lock = path.join(f.root, 'package-lock.json');
  const original = fs.readFileSync(lock);
  fs.rmSync(lock);
  assert.deepEqual(computeDepsKey(f.inputs()), { key: null, reason: 'no lockfile' });
  fs.writeFileSync(lock, JSON.stringify({ packages: { '': {}, 'packages/a': {}, 'node_modules/a': { resolved: 'packages/a', link: true } } }));
  assert.match(computeDepsKey(f.inputs()).reason, /links local packages/);
  fs.writeFileSync(lock, '{not json');
  assert.match(computeDepsKey(f.inputs()).reason, /not valid JSON/);
  fs.writeFileSync(lock, original);
  assert.ok(computeDepsKey(f.inputs()).key);
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

test('materialize makes hardlinks (same inode as the store) inside real, writable directories', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  const work = tmpDir('deps-cache-work');
  const farm = path.join(work, 'node_modules');
  materializeFromStore(cacheRoot, key, farm);
  const stored = path.join(entryTree(cacheRoot, key), 'pkg1', 'package.json');
  assert.equal(fs.statSync(path.join(farm, 'pkg1', 'package.json')).ino, fs.statSync(stored).ino);
  assert.ok((fs.statSync(path.join(farm, 'pkg1')).mode & 0o200) !== 0, 'farm directories are owner-writable');
  assert.ok((fs.statSync(path.join(entryTree(cacheRoot, key), 'pkg1')).mode & 0o222) === 0, 'store directories are read-only');
  assert.ok((fs.statSync(stored).mode & 0o222) === 0, 'store files are read-only');
});

test('an in-place write to a materialized file fails with EACCES and the store stays intact', { skip: IS_ROOT && 'root ignores file modes' }, () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key, src } = publish(cacheRoot);
  const before = treeDigest(src);
  const work = tmpDir('deps-cache-work');
  const farm = path.join(work, 'node_modules');
  materializeFromStore(cacheRoot, key, farm);

  const target = path.join(farm, 'pkg3', 'package.json');
  assert.throws(() => fs.writeFileSync(target, 'corrupted'), { code: 'EACCES' });
  assert.throws(() => fs.appendFileSync(target, 'corrupted'), { code: 'EACCES' });
  assert.throws(() => fs.truncateSync(target, 0), { code: 'EACCES' });

  assert.equal(treeDigest(entryTree(cacheRoot, key)), before, 'the store is byte-identical');
  assert.equal(treeDigest(farm), before, 'and so is the farm that failed to write');
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

test('eviction never takes a key a live run holds a lease on, even when it is the oldest and the store is over bound', () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const [oldest, middle, newest] = [1, 2, 3].map(() => publish(cacheRoot));
  ageEntry(cacheRoot, oldest.key, 3 * 60_000);
  ageEntry(cacheRoot, middle.key, 2 * 60_000);
  ageEntry(cacheRoot, newest.key, 60_000);
  const release = acquireLease(cacheRoot, oldest.key);

  const evicted = evictLeastRecentlyUsed(cacheRoot, oldest.bytes + 1);
  assert.ok(!evicted.includes(oldest.key), 'the leased key survives');
  assert.deepEqual(evicted, [middle.key, newest.key], 'eviction moves on to the next-oldest, as far as the bound requires');
  assert.deepEqual(liveKeys(cacheRoot), [oldest.key]);

  release();
  assert.deepEqual(evictLeastRecentlyUsed(cacheRoot, 0), [oldest.key], 'once released it is evictable again');
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

test('removing an evicted store entry does not make a running lane\'s linked files writable', { skip: IS_ROOT && 'root ignores file modes' }, () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot);
  const work = tmpDir('deps-cache-work');
  const farm = path.join(work, 'node_modules');
  materializeFromStore(cacheRoot, key, farm);
  removeTree(entryDir(cacheRoot, key));
  assert.throws(() => fs.writeFileSync(path.join(farm, 'pkg1', 'package.json'), 'x'), { code: 'EACCES' });
  removeFarm(work);
});

// ---- postinstall safety ----

test('the outside-node_modules snapshot sees added, changed and removed files and ignores node_modules', () => {
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

// ---- absolute install path ----

test('install-path scan finds the path in a text file, a symlink target and a shim, and skips big files', () => {
  const tree = tmpDir('deps-cache-scan');
  const work = '/var/tmp/lb-x/tickets/abc/work';
  assert.equal(findInstallPathReference(makeTree(tree), [work]), null, 'a clean tree has none');

  const cases = {
    text: () => fs.writeFileSync(path.join(tree, 'pkg1', 'config.json'), `{"root":"${work}/node_modules/pkg1"}`),
    symlink: () => fs.symlinkSync(`${work}/node_modules/pkg2`, path.join(tree, 'link-out')),
    shim: () => fs.writeFileSync(path.join(tree, '.bin', 'shim'), `#!/bin/sh\nexec node "${work}/node_modules/pkg0/cli.js" "$@"\n`, { mode: 0o755 }),
  };
  for (const [name, plant] of Object.entries(cases)) {
    plant();
    assert.ok(findInstallPathReference(tree, [work]), `${name} is found`);
    fs.rmSync(path.join(tree, { text: 'pkg1/config.json', symlink: 'link-out', shim: '.bin/shim' }[name]));
  }
  fs.writeFileSync(path.join(tree, 'big.bin'), Buffer.concat([Buffer.from(work), Buffer.alloc(3 * 1024 * 1024)]));
  assert.equal(findInstallPathReference(tree, [work]), null, 'a file over the size cap is not searched');
});

// ---- read-only guard ----

test('the read-only sample flags a writable store file, in the first files and among the rest', { skip: IS_ROOT && 'root ignores file modes' }, () => {
  const cacheRoot = tmpDir('deps-cache-store');
  const { key } = publish(cacheRoot, { packages: 220 }); // 220 * 2 + 2 files: more than the 200-file head
  assert.equal(findWritableStoreFile(cacheRoot, key), null);

  const files = [];
  const walk = (d) => fs.readdirSync(d, { withFileTypes: true }).forEach((e) => (e.isDirectory() ? walk(path.join(d, e.name)) : e.isFile() && files.push(path.join(d, e.name))));
  walk(entryTree(cacheRoot, key));
  fs.chmodSync(files[0], 0o644);
  assert.ok(findWritableStoreFile(cacheRoot, key), 'a writable file at the head is found');
  fs.chmodSync(files[0], 0o444);
  for (const f of files.slice(200)) fs.chmodSync(f, 0o644);
  assert.ok(findWritableStoreFile(cacheRoot, key), 'writable files past the head are found by the random sample');
  assert.equal(quarantineEntry(cacheRoot, key), true);
  purgeTrash(cacheRoot);
  assert.deepEqual(fs.readdirSync(cacheRoot), []);
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
});
