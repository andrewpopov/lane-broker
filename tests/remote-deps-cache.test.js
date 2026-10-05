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

const POSTINSTALLS = {
  outside: "require('fs').writeFileSync('generated-by-postinstall.txt', process.cwd())",
  embed: "require('fs').writeFileSync('node_modules/hello-tool/where.txt', process.cwd())",
  'embed-big': "require('fs').writeFileSync('node_modules/hello-tool/native.bin', Buffer.concat([Buffer.alloc(3 * 1024 * 1024, 65), Buffer.from(process.cwd())]))",
  'drop-installed-record': "const f = 'node_modules/.package-lock.json'; const l = JSON.parse(require('fs').readFileSync(f, 'utf8')); delete l.packages['node_modules/hello-tool']; require('fs').writeFileSync(f, JSON.stringify(l))",
  harmless: "process.exit(0)",
};

/**
 * The files of a repo whose lane installs the fixture. `lockEdit` makes a variant lock; `postinstall` names a
 * root script from POSTINSTALLS; `prepare` sets a root prepare script. A lane declaring a postinstall also
 * declares root scripts safe, unless `rootScriptsSafe` says otherwise.
 */
function repoFiles({ lockEdit, postinstall = null, prepare = null, rootScriptsSafe = postinstall !== null, tarball, laneExtra = {} } = {}) {
  const base = fixtureBase();
  const pkg = JSON.parse(base.packageJson);
  const lock = JSON.parse(base.lock);
  if (postinstall) {
    pkg.scripts = { ...pkg.scripts, postinstall: `node -e "${POSTINSTALLS[postinstall].replace(/"/g, '\\"')}"` };
    lock.packages[''].hasInstallScript = true;
  }
  if (prepare) pkg.scripts = { ...pkg.scripts, prepare };
  if (lockEdit) lockEdit(lock);
  return {
    'package.json': JSON.stringify(pkg),
    'package-lock.json': JSON.stringify(lock),
    'hello-tool-1.0.0.tgz': tarball ?? base.tarball,
    '.npmrc': NPMRC,
    '.lane-broker.json': JSON.stringify({
      version: 1,
      lanes: { default: { weight: 1, remote: true, remoteDeps: ['.'], ...(rootScriptsSafe ? { remoteDepsCacheRootScriptsSafe: true } : {}), ...laneExtra } },
    }),
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

async function runLane(s, repoDir, { repo = 'r', marker, env = s.env } = {}) {
  const markerPath = marker ?? path.join(tmpDir('deps-cache-marker'), 'probe.json');
  const r = await laneRun(['run', '--repo', repo, '--lane', 'default', '--', ...probeCommand(markerPath)], { env, cwd: repoDir });
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
  assert.equal(second.probe.inPlace, 'wrote', 'the hit is a private copy: an in-place write to an installed file just works');
});

test('what a lane does to its own tree never reaches the store: the next hit gets the original bytes', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles());
  await runLane(s, repoDir); // miss: publishes
  const hit = await runLane(s, repoDir); // hit: its command appended to an installed file and added a new one
  assert.equal(hit.probe.inPlace, 'wrote');
  const key = s.storedKeys()[0];
  const storedTree = path.join(s.storeDir, key, 'node_modules');
  assert.equal(JSON.parse(fs.readFileSync(path.join(storedTree, 'hello-tool', 'package.json'), 'utf8')).name, 'hello-tool');
  assert.ok(!fs.readFileSync(path.join(storedTree, 'hello-tool', 'package.json'), 'utf8').endsWith(' '), 'the in-place append stayed in the lane\'s copy');
  assert.equal(fs.existsSync(path.join(storedTree, 'hello-tool', 'new-file.txt')), false, 'files a lane created never reach the store');
  assert.equal(fs.existsSync(path.join(storedTree, '.bin', 'hello-tool')), true);
  if (!IS_ROOT) assert.equal(fs.statSync(path.join(storedTree, 'hello-tool', 'package.json')).mode & 0o222, 0, 'the store stays read-only');
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

test('a store bound that holds one tree: publishing a second key evicts the first, and releasing the lease trims what the lease had protected', async () => {
  const s = setupWithNpmSpy();
  const a = await runLane(s, makeGitWorktree(repoFiles()));
  assert.equal(a.row.depsCache, 'miss');
  const [keyA] = s.storedKeys();
  const bytes = JSON.parse(fs.readFileSync(path.join(s.storeDir, keyA, 'meta.json'), 'utf8')).bytes;

  writeGlobalConfig(s.runnerHome, { sampleMs: 50, capacity: 4, remoteDepsCacheMaxBytes: bytes + 1 });
  const b = await runLane(s, makeGitWorktree(repoFiles({ lockEdit: (lock) => (lock.packages[''].version = '2.0.0') })));
  assert.equal(b.row.depsCache, 'miss');
  const keysAfterB = s.storedKeys();
  assert.equal(keysAfterB.length, 1);
  assert.notEqual(keysAfterB[0], keyA, 'the older key was evicted, the newer one kept');
  assert.deepEqual(fs.readdirSync(s.storeDir).filter((n) => n.startsWith('.trash-') || n.startsWith('.tmp-')), []);

  writeGlobalConfig(s.runnerHome, { sampleMs: 50, capacity: 4, remoteDepsCacheMaxBytes: 1 });
  const c = await runLane(s, makeGitWorktree(repoFiles({ lockEdit: (lock) => (lock.packages[''].version = '3.0.0') })));
  assert.equal(c.row.depsCache, 'miss');
  assert.equal(s.storedKeys().length, 0, 'over the bound only while this run held its lease; trimmed when it released it');
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

test('a large file (over any size cap) that embeds the install path is not cached', async () => {
  const s = setupWithNpmSpy();
  const first = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'embed-big' })));
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'skip');
  assert.match(first.stderr, /reason=absolute-install-path file=hello-tool\/native\.bin/);
  assert.equal(s.storedKeys().length, 0);
});

test('a changed environment variable is a miss', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles());
  await runLane(s, repoDir);
  assert.equal((await runLane(s, repoDir)).row.depsCache, 'hit');
  const changed = await runLane(s, repoDir, { env: { ...s.env, CXX: 'a-different-compiler' } });
  assert.equal(changed.code, 0, changed.stderr);
  assert.equal(changed.row.depsCache, 'miss');
  assert.equal(s.storedKeys().length, 2);
  const volatile = await runLane(s, repoDir, { env: { ...s.env, SSH_CONNECTION: '1.2.3.4 1 5.6.7.8 22', LANE_FAKE_OTHER: 'x' } });
  assert.equal(volatile.row.depsCache, 'hit', 'ssh and lane variables are excluded from the key');
});

test('a changed file: tarball is a miss and installs again', async () => {
  const s = setupWithNpmSpy();
  await runLane(s, makeGitWorktree(repoFiles()));
  const changed = await runLane(s, makeGitWorktree(repoFiles({ tarball: Buffer.concat([fixtureBase().tarball, Buffer.from('changed')]) })));
  assert.equal(changed.row.depsCache, 'miss');
  assert.equal(s.npmCiCalls(), 2, 'it did not reuse the cached tree for the changed tarball');
  assert.equal(s.storedKeys().length, 2);
});

test('a lock that is not pinned to content is skipped with its reason, and the run still installs', async () => {
  const s = setupWithNpmSpy();
  const noIntegrity = (lock) => delete lock.packages['node_modules/hello-tool'].integrity;
  const run = await runLane(s, makeGitWorktree(repoFiles({ lockEdit: noIntegrity })));
  assert.equal(run.code, 0, run.stderr);
  assert.equal(run.row.depsCache, 'skip');
  assert.match(run.stderr, /deps-cache skip dir=\. ms=\d+ reason="lock entry node_modules\/hello-tool resolves to an unpinned source/);
  assert.equal(s.storedKeys().length, 0);
});

test('a root lifecycle script outside the allowlist skips the cache unless the lane declares root scripts safe', async () => {
  const s = setupWithNpmSpy();
  const skipped = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'harmless', rootScriptsSafe: false })));
  assert.equal(skipped.code, 0, skipped.stderr);
  assert.equal(skipped.row.depsCache, 'skip');
  assert.match(skipped.stderr, /reason="root lifecycle script not known to leave node_modules alone \(postinstall: node -e/);
  assert.equal(s.storedKeys().length, 0);

  const declared = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'harmless', rootScriptsSafe: true })));
  assert.equal(declared.row.depsCache, 'miss');
  assert.equal(s.storedKeys().length, 1);
});

test('the allowlisted prepare script (git config core.hooksPath) does not stop a tree being cached', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ prepare: 'git config core.hooksPath .githooks || true' }));
  const first = await runLane(s, repoDir);
  assert.equal(first.code, 0, first.stderr);
  assert.match(first.stderr, /deps-cache miss key=[0-9a-f]{12} dir=\. ms=\d+ published=yes/);
  assert.equal((await runLane(s, repoDir)).row.depsCache, 'hit');
});

test('an install npm did not fully record (a simulated missing optional dependency) is not published', async () => {
  const s = setupWithNpmSpy();
  const run = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'drop-installed-record' })));
  assert.equal(run.code, 0, run.stderr);
  assert.equal(run.row.depsCache, 'skip');
  assert.match(run.stderr, /deps-cache skip key=[0-9a-f]{12} dir=\. ms=\d+ reason=incomplete-optional missing=hello-tool/);
  assert.equal(s.storedKeys().length, 0);
});
