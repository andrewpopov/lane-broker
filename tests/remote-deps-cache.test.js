import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { laneRun, writeGlobalConfig } from './helpers.js';
import { setup, tmpDir, makeGitWorktree } from './remote-harness.js';
import { paths } from '../src/state.js';

/**
 * BRAIN-389, end to end: a real `lane run` -> fake-ssh transport -> `remote-exec` -> `remote-pipeline` with
 * REAL npm. The fixture installs a tarball dependency that ships a `.bin` command, so `npm ci` needs no
 * network and a hit has a real `.bin` symlink to run. A shim in front of `npm` counts its invocations, which is
 * how "a hit runs no npm ci" is observed.
 */

const IS_ROOT = typeof process.getuid === 'function' && process.getuid() === 0;
const REAL_NPM = execFileSync('which', ['npm'], { encoding: 'utf8' }).trim();
const NPMRC = 'registry=http://127.0.0.1:9\nfetch-retries=0\nfetch-timeout=2000\n';

/** `{ 'package.json', 'package-lock.json', tarball }` for an app depending on a local tarball with a bin, built with real npm, offline. */
const fixtureBase = (() => {
  let built;
  return () => {
    if (built) return built;
    const dir = tmpDir('deps-cache-fixture');
    const npmEnv = { ...process.env, HOME: dir, npm_config_cache: path.join(dir, 'cache'), npm_config_userconfig: path.join(dir, 'userrc') };
    const tool = path.join(dir, 'tool');
    const app = path.join(dir, 'app');
    fs.mkdirSync(path.join(tool, 'bin'), { recursive: true });
    fs.mkdirSync(app);
    fs.writeFileSync(path.join(tool, 'package.json'), JSON.stringify({ name: 'hello-tool', version: '1.0.0', bin: { 'hello-tool': 'bin/hello.js' } }));
    fs.writeFileSync(path.join(tool, 'bin', 'hello.js'), '#!/usr/bin/env node\nconsole.log("hello from the bin");\n', { mode: 0o755 });
    execFileSync(REAL_NPM, ['pack', '--silent', '--pack-destination', app], { cwd: tool, env: npmEnv });
    fs.writeFileSync(path.join(app, 'package.json'), JSON.stringify({ name: 'app', version: '1.0.0', private: true, dependencies: { 'hello-tool': 'file:./hello-tool-1.0.0.tgz' } }));
    execFileSync(REAL_NPM, ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: app, env: npmEnv });
    built = {
      packageJson: fs.readFileSync(path.join(app, 'package.json'), 'utf8'),
      lock: fs.readFileSync(path.join(app, 'package-lock.json'), 'utf8'),
      tarball: fs.readFileSync(path.join(app, 'hello-tool-1.0.0.tgz')),
    };
    return built;
  };
})();

/** The files of a repo whose lane installs the fixture. `lockEdit`/`postinstall` make variants. */
function repoFiles({ lockEdit, postinstall = null, laneExtra = {} } = {}) {
  const base = fixtureBase();
  const pkg = JSON.parse(base.packageJson);
  const lock = JSON.parse(base.lock);
  if (postinstall) {
    const target = postinstall === 'embed' ? 'node_modules/hello-tool/where.txt' : 'generated-by-postinstall.txt';
    pkg.scripts = { postinstall: `node -e "require('fs').writeFileSync('${target}', process.cwd())"` };
    lock.packages[''].hasInstallScript = true;
  }
  if (lockEdit) lockEdit(lock);
  return {
    'package.json': JSON.stringify(pkg),
    'package-lock.json': JSON.stringify(lock),
    'hello-tool-1.0.0.tgz': base.tarball,
    '.npmrc': NPMRC,
    '.lane-broker.json': JSON.stringify({ version: 1, lanes: { default: { weight: 1, remote: true, remoteDeps: ['.'], ...laneExtra } } }),
  };
}

/** A `setup()` harness whose PATH has an `npm` shim that logs each invocation's argv before running real npm. */
function setupWithNpmSpy(opts) {
  const s = setup(opts);
  const spyDir = tmpDir('deps-cache-npm-spy');
  const log = path.join(spyDir, 'npm-calls.log');
  const shim = path.join(spyDir, 'npm');
  fs.writeFileSync(shim, `#!/bin/sh\necho "$@" >> ${JSON.stringify(log)}\nexec ${JSON.stringify(REAL_NPM)} "$@"\n`, { mode: 0o755 });
  s.env = { ...s.env, PATH: `${spyDir}${path.delimiter}${s.env.PATH}` };
  s.npmCiCalls = () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter((l) => l.startsWith('ci ')).length : 0);
  s.storeDir = path.join(s.runnerRoot, 'deps-cache');
  s.storedKeys = () => (fs.existsSync(s.storeDir) ? fs.readdirSync(s.storeDir).filter((n) => /^[0-9a-f]{64}$/.test(n)) : []);
  return s;
}

/**
 * The command run on the runner after deps: it must find a working `.bin` command, and it records what an
 * in-place write to an installed file does (written to `marker`, an absolute path outside the work dir).
 */
function probeCommand(marker) {
  const script = `
    const fs = require('fs');
    const { execFileSync } = require('child_process');
    const out = execFileSync('node_modules/.bin/hello-tool', { encoding: 'utf8' }).trim();
    let inPlace = 'wrote';
    try { fs.appendFileSync('node_modules/hello-tool/package.json', ' '); } catch (e) { inPlace = e.code; }
    fs.writeFileSync('node_modules/hello-tool/new-file.txt', 'creating a file works');
    fs.writeFileSync(${JSON.stringify(marker)}, JSON.stringify({ out, inPlace }));
  `;
  return [process.execPath, '-e', script];
}

const lastRow = (state) => fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l)).at(-1);

async function runLane(s, repoDir, { repo = 'r', marker } = {}) {
  const markerPath = marker ?? path.join(tmpDir('deps-cache-marker'), 'probe.json');
  const r = await laneRun(['run', '--repo', repo, '--lane', 'default', '--', ...probeCommand(markerPath)], { env: s.env, cwd: repoDir });
  const probe = fs.existsSync(markerPath) ? JSON.parse(fs.readFileSync(markerPath, 'utf8')) : null;
  return { ...r, probe, row: r.code === 0 || fs.existsSync(paths(s.state).history) ? lastRow(s.state) : null };
}

test('a miss installs and publishes; the next run is a hit that runs no npm ci, is faster, and has a working .bin', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles());

  const first = await runLane(s, repoDir);
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'miss');
  assert.match(first.stderr, /deps-cache miss key=[0-9a-f]{12} dir=\. ms=\d+ published=yes/);
  assert.equal(first.probe.out, 'hello from the bin');
  assert.equal(s.npmCiCalls(), 1);
  assert.equal(s.storedKeys().length, 1);

  const second = await runLane(s, repoDir);
  assert.equal(second.code, 0, second.stderr);
  assert.equal(second.row.depsCache, 'hit');
  assert.match(second.stderr, /deps-cache hit key=[0-9a-f]{12} dir=\. ms=\d+/);
  assert.equal(s.npmCiCalls(), 1, 'a hit runs no npm ci');
  assert.equal(second.probe.out, 'hello from the bin', 'the .bin symlink works in the materialized tree');
  assert.ok(second.row.depsMs < first.row.depsMs, `hit ${second.row.depsMs}ms should beat miss ${first.row.depsMs}ms`);
  assert.equal(s.storedKeys().length, 1, 'same key, no second entry');
  if (!IS_ROOT) assert.equal(second.probe.inPlace, 'EACCES', 'an in-place write to an installed file fails loudly');
});

test('the store survives a lane that tried to write in place: the next hit still installs the original bytes', { skip: IS_ROOT && 'root ignores file modes' }, async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles());
  await runLane(s, repoDir); // miss: publishes
  const hit = await runLane(s, repoDir); // hit: its command attempted an in-place append
  assert.equal(hit.probe.inPlace, 'EACCES');
  const key = s.storedKeys()[0];
  const stored = JSON.parse(fs.readFileSync(path.join(s.storeDir, key, 'node_modules', 'hello-tool', 'package.json'), 'utf8'));
  assert.equal(stored.name, 'hello-tool');
  assert.equal(fs.existsSync(path.join(s.storeDir, key, 'node_modules', 'hello-tool', 'new-file.txt')), false, 'files a lane created never reach the store');
  assert.equal(fs.existsSync(path.join(s.storeDir, key, 'node_modules', '.bin', 'hello-tool')), true);
});

test('a lockfile change is a miss and installs again', async () => {
  const s = setupWithNpmSpy();
  await runLane(s, makeGitWorktree(repoFiles()));
  assert.equal(s.npmCiCalls(), 1);

  const changed = await runLane(s, makeGitWorktree(repoFiles({ lockEdit: (lock) => (lock.packages[''].version = '1.0.1') })));
  assert.equal(changed.code, 0, changed.stderr);
  assert.equal(changed.row.depsCache, 'miss');
  assert.equal(s.npmCiCalls(), 2);
  assert.equal(s.storedKeys().length, 2);
});

test('a lane with remoteDepsCache false opts out: it installs every time, publishes nothing, and does not read the store', async () => {
  const s = setupWithNpmSpy();
  await runLane(s, makeGitWorktree(repoFiles())); // populate the store with the very same key
  assert.equal(s.storedKeys().length, 1);
  assert.equal(s.npmCiCalls(), 1);

  const optedOut = await runLane(s, makeGitWorktree(repoFiles({ laneExtra: { remoteDepsCache: false } })));
  assert.equal(optedOut.code, 0, optedOut.stderr);
  assert.equal(optedOut.row.depsCache, 'skip');
  assert.match(optedOut.stderr, /deps-cache skip dir=\. ms=\d+ reason=disabled/);
  assert.equal(s.npmCiCalls(), 2, 'it ran npm ci even though the store held this exact key');
  assert.equal(optedOut.probe.out, 'hello from the bin');
});

test('the runner-wide remoteDepsCache false switch disables the cache for every lane', async () => {
  const s = setupWithNpmSpy();
  writeGlobalConfig(s.runnerHome, { sampleMs: 50, capacity: 4, remoteDepsCache: false });
  const repoDir = makeGitWorktree(repoFiles());
  const a = await runLane(s, repoDir);
  const b = await runLane(s, repoDir);
  assert.equal(a.row.depsCache, 'skip');
  assert.equal(b.row.depsCache, 'skip');
  assert.equal(s.npmCiCalls(), 2);
  assert.equal(fs.existsSync(s.storeDir), false, 'nothing was ever published');
});

test('an install that writes outside node_modules is not cached, says why, and still runs on its own tree', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ postinstall: 'outside' }));

  const first = await runLane(s, repoDir);
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'miss');
  assert.match(first.stderr, /deps-cache miss key=[0-9a-f]{12} dir=\. ms=\d+ published=no reason="install changed files outside node_modules: generated-by-postinstall\.txt"/);
  assert.equal(first.probe.out, 'hello from the bin', 'the tree is usable for this run');
  assert.equal(s.storedKeys().length, 0);

  const second = await runLane(s, repoDir);
  assert.equal(second.row.depsCache, 'miss', 'never a hit: the postinstall must run again');
  assert.equal(s.npmCiCalls(), 2);
});

test('with a store bound smaller than two trees, publishing a second key evicts the first (and never the one just published)', async () => {
  const s = setupWithNpmSpy();
  writeGlobalConfig(s.runnerHome, { sampleMs: 50, capacity: 4, remoteDepsCacheMaxBytes: 1 });
  const a = await runLane(s, makeGitWorktree(repoFiles()));
  assert.equal(a.row.depsCache, 'miss');
  const keysAfterA = s.storedKeys();
  assert.equal(keysAfterA.length, 1, 'the entry the run itself holds survives even though it exceeds the bound');

  const b = await runLane(s, makeGitWorktree(repoFiles({ lockEdit: (lock) => (lock.packages[''].version = '2.0.0') })));
  assert.equal(b.row.depsCache, 'miss');
  const keysAfterB = s.storedKeys();
  assert.equal(keysAfterB.length, 1);
  assert.notEqual(keysAfterB[0], keysAfterA[0], 'the older key was evicted, the newer one kept');
  assert.deepEqual(fs.readdirSync(s.storeDir).filter((n) => n.startsWith('.trash-') || n.startsWith('.tmp-')), []);
});

test('two real runs missing on the same key at once both succeed and leave one intact entry that the next run hits', async () => {
  const s = setupWithNpmSpy();
  const files = repoFiles();
  const [a, b] = await Promise.all([
    runLane(s, makeGitWorktree(files), { repo: 'r1' }),
    runLane(s, makeGitWorktree(files), { repo: 'r2' }),
  ]);
  assert.equal(a.code, 0, a.stderr);
  assert.equal(b.code, 0, b.stderr);
  assert.equal(a.probe.out, 'hello from the bin');
  assert.equal(b.probe.out, 'hello from the bin');
  assert.equal(s.storedKeys().length, 1);
  assert.deepEqual(fs.readdirSync(s.storeDir).filter((n) => n.startsWith('.tmp-')), []);

  const calls = s.npmCiCalls();
  const next = await runLane(s, makeGitWorktree(files), { repo: 'r3' });
  assert.equal(next.row.depsCache, 'hit');
  assert.equal(s.npmCiCalls(), calls, 'the entry the racing runs left behind is complete');
  assert.equal(next.probe.out, 'hello from the bin');
});

test('an install that embeds its own absolute path under node_modules is not cached, and the run still succeeds', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ postinstall: 'embed' }));

  const first = await runLane(s, repoDir);
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'skip');
  assert.match(first.stderr, /deps-cache skip key=[0-9a-f]{12} dir=\. ms=\d+ reason=absolute-install-path file=hello-tool\/where\.txt/);
  assert.equal(first.probe.out, 'hello from the bin');
  assert.equal(s.storedKeys().length, 0);

  const second = await runLane(s, repoDir);
  assert.equal(second.row.depsCache, 'skip');
  assert.equal(s.npmCiCalls(), 2, 'never a hit: the baked-in path would be wrong in the next work dir');
});

test('a hit whose stored files were made writable is treated as corrupt: evicted, reinstalled, republished read-only', { skip: IS_ROOT && 'root ignores file modes' }, async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles());
  await runLane(s, repoDir);
  const [key] = s.storedKeys();
  const victim = path.join(s.storeDir, key, 'node_modules', 'hello-tool', 'package.json');
  fs.chmodSync(victim, 0o644); // what a tool that chmods a hardlinked file does to the shared inode
  assert.equal(s.npmCiCalls(), 1);

  const again = await runLane(s, repoDir);
  assert.equal(again.code, 0, again.stderr);
  assert.match(again.stderr, /deps-cache corrupt key=[0-9a-f]{12} writable store file hello-tool\/package\.json/);
  assert.equal(again.row.depsCache, 'miss');
  assert.equal(s.npmCiCalls(), 2, 'it fell back to npm ci');
  assert.deepEqual(s.storedKeys(), [key], 'the same key was republished');
  assert.equal(fs.statSync(victim).mode & 0o222, 0, 'the fresh entry is read-only again');
  assert.deepEqual(fs.readdirSync(s.storeDir).filter((n) => n.startsWith('.trash-')), []);

  const third = await runLane(s, repoDir);
  assert.equal(third.row.depsCache, 'hit');
});
