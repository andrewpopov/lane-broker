import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { laneRun, writeGlobalConfig } from './helpers.js';
import { setup, tmpDir, makeGitWorktree } from './remote-harness.js';
import { paths } from '../src/state.js';
import { IS_ROOT, REAL_NPM, NPMRC, repoFiles, setupWithNpmSpy, lastRow, runLane } from './remote-deps-cache-fixture.js';

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

// ---- BRAIN-466: npm workspaces ----

/**
 * A real npm-workspace repo, built offline: the root depends on hello-tool 1.0.0 and the `@x/api` workspace on
 * hello-tool 2.0.0 (both local tarballs), so npm installs the second under `packages/api/node_modules`.
 * `@x/web` depends on `@x/api`, which npm satisfies with a workspace link.
 */
const workspaceFixture = (() => {
  let built;
  return () => {
    if (built) return built;
    const dir = tmpDir('deps-cache-workspace-fixture');
    const npmEnv = { ...process.env, HOME: dir, npm_config_cache: path.join(dir, 'cache'), npm_config_userconfig: path.join(dir, 'userrc') };
    const app = path.join(dir, 'app');
    const tarballs = {};
    for (const version of ['1.0.0', '2.0.0']) {
      const tool = path.join(dir, `tool-${version}`);
      fs.mkdirSync(tool, { recursive: true });
      fs.writeFileSync(path.join(tool, 'package.json'), JSON.stringify({ name: 'hello-tool', version }));
      fs.writeFileSync(path.join(tool, 'index.js'), `module.exports = ${JSON.stringify(version)};\n`);
      fs.mkdirSync(app, { recursive: true });
      execFileSync(REAL_NPM, ['pack', '--silent', '--pack-destination', app], { cwd: tool, env: npmEnv });
      tarballs[`hello-tool-${version}.tgz`] = fs.readFileSync(path.join(app, `hello-tool-${version}.tgz`));
    }
    const write = (rel, content) => {
      fs.mkdirSync(path.dirname(path.join(app, rel)), { recursive: true });
      fs.writeFileSync(path.join(app, rel), content);
    };
    write('package.json', JSON.stringify({ name: 'ws-root', version: '1.0.0', private: true, workspaces: ['packages/*'], dependencies: { 'hello-tool': 'file:./hello-tool-1.0.0.tgz' } }));
    write('packages/api/package.json', JSON.stringify({ name: '@x/api', version: '1.0.0', main: 'index.js', dependencies: { 'hello-tool': 'file:../../hello-tool-2.0.0.tgz' } }));
    write('packages/api/index.js', 'module.exports = { source: "workspace source", tool: require("hello-tool") };\n');
    write('packages/web/package.json', JSON.stringify({ name: '@x/web', version: '1.0.0', dependencies: { '@x/api': '*' } }));
    execFileSync(REAL_NPM, ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: app, env: npmEnv });
    built = {
      tarballs,
      files: {
        'package.json': fs.readFileSync(path.join(app, 'package.json'), 'utf8'),
        'package-lock.json': fs.readFileSync(path.join(app, 'package-lock.json'), 'utf8'),
        'packages/api/package.json': fs.readFileSync(path.join(app, 'packages/api/package.json'), 'utf8'),
        'packages/api/index.js': fs.readFileSync(path.join(app, 'packages/api/index.js'), 'utf8'),
        'packages/web/package.json': fs.readFileSync(path.join(app, 'packages/web/package.json'), 'utf8'),
      },
    };
    return built;
  };
})();

function workspaceRepoFiles() {
  const base = workspaceFixture();
  return {
    ...base.files,
    ...Object.fromEntries(Object.entries(base.tarballs).map(([name, bytes]) => [name, bytes])),
    '.npmrc': NPMRC,
    '.lane-broker.json': JSON.stringify({ version: 1, lanes: { default: { weight: 1, remote: true, remoteDeps: ['.'] } } }),
  };
}

/** Run in the work dir: what `@x/api` resolves to, whether its link is a relative symlink, and which hello-tool each side sees. */
function workspaceProbe(marker) {
  const script = `
    const fs = require('fs');
    const path = require('path');
    const api = require('@x/api');
    const web = require.resolve('@x/api', { paths: [path.join(process.cwd(), 'packages/web')] });
    fs.writeFileSync(${JSON.stringify(marker)}, JSON.stringify({
      source: api.source,
      apiTool: api.tool,
      rootTool: require('hello-tool'),
      linkTarget: fs.readlinkSync('node_modules/@x/api'),
      isLink: fs.lstatSync('node_modules/@x/api').isSymbolicLink(),
      resolvedReal: fs.realpathSync(require.resolve('@x/api')),
      webResolvedReal: fs.realpathSync(web),
      cwd: fs.realpathSync(process.cwd()),
      nestedIsDir: fs.lstatSync('packages/api/node_modules/hello-tool').isDirectory(),
      stashLeft: fs.existsSync('node_modules/.lane-workspaces'),
    }));
  `;
  return ['node', '-e', script];
}

test('workspace root generators restore keyed output and regenerate after source changes', async () => {
  const s = setupWithNpmSpy();
  const files = workspaceRepoFiles();
  const manifest = JSON.parse(files['package.json']);
  manifest.scripts = { postinstall: 'node generate.cjs' };
  files['package.json'] = JSON.stringify(manifest);
  const config = JSON.parse(files['.lane-broker.json']);
  config.lanes.default.remoteDepsCacheRootScriptsSafe = true;
  files['.lane-broker.json'] = JSON.stringify(config);
  files['generate.cjs'] = `const fs = require('fs');
    fs.writeFileSync('node_modules/generated-value.json', JSON.stringify({
      value: fs.readFileSync('packages/api/schema.txt', 'utf8')
    }));`;
  const run = async (value) => {
    const repoDir = makeGitWorktree({ ...files, 'packages/api/schema.txt': value });
    const marker = path.join(tmpDir('deps-cache-generator-marker'), 'probe.json');
    const probe = `require('fs').copyFileSync('node_modules/generated-value.json', ${JSON.stringify(marker)})`;
    const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'node', '-e', probe], { env: s.env, cwd: repoDir });
    assert.equal(result.code, 0, result.stderr);
    assert.equal(JSON.parse(fs.readFileSync(marker, 'utf8')).value, value);
    return { ...result, row: lastRow(s.state) };
  };
  const first = await run('initial schema');
  assert.equal(first.row.depsCache, 'miss');
  assert.match(first.stderr, /published=yes/);
  const second = await run('initial schema');
  assert.equal(second.row.depsCache, 'hit');
  assert.equal(s.npmCiCalls(), 1, 'generated output survives a hit in another work directory');
  const changed = await run('changed schema');
  assert.equal(changed.row.depsCache, 'miss');
  assert.equal(s.npmCiCalls(), 2, 'new generator inputs require a new install');
});

test('an npm-workspace repo is cached: the second run is a hit that recreates the workspace links and the nested workspace node_modules (BRAIN-466)', async (t) => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(workspaceRepoFiles());
  const run = async () => {
    const marker = path.join(tmpDir('deps-cache-ws-marker'), 'probe.json');
    const r = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...workspaceProbe(marker)], { env: s.env, cwd: repoDir });
    return { ...r, probe: fs.existsSync(marker) ? JSON.parse(fs.readFileSync(marker, 'utf8')) : null, row: lastRow(s.state) };
  };

  const first = await run();
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'miss');
  assert.match(first.stderr, /published=yes/);
  assert.equal(s.npmCiCalls(), 1);

  const second = await run();
  assert.equal(second.code, 0, second.stderr);
  assert.equal(second.row.depsCache, 'hit');
  assert.equal(s.npmCiCalls(), 1, 'a hit runs no npm ci');
  t.diagnostic(`workspace dependency setup: miss ${first.row.depsMs}ms, hit ${second.row.depsMs}ms; same runner and fixture`);
  assert.ok(second.row.depsMs < first.row.depsMs, 'restoring a workspace cache entry beats reinstalling it');
  for (const { probe } of [first, second]) {
    assert.equal(probe.source, 'workspace source');
    assert.equal(probe.isLink, true);
    assert.equal(probe.linkTarget, '../../packages/api', 'a relative link, as npm ci makes it');
    assert.equal(probe.resolvedReal, path.join(probe.cwd, 'packages/api/index.js'), 'the link leads to the workspace source in THIS run\'s work dir');
    assert.equal(probe.webResolvedReal, probe.resolvedReal);
    assert.equal(probe.rootTool, '1.0.0');
    assert.equal(probe.apiTool, '2.0.0', 'the workspace\'s own conflicting dependency is restored under packages/api/node_modules');
    assert.equal(probe.nestedIsDir, true);
    assert.equal(probe.stashLeft, false);
  }
  assert.equal(second.probe.cwd === first.probe.cwd, false, 'the hit ran in a different work dir');

  const stored = path.join(s.storeDir, s.storedKeys()[0], 'node_modules');
  assert.equal(fs.existsSync(path.join(stored, '@x', 'api')), false, 'the store holds no workspace symlink');
  assert.equal(fs.existsSync(path.join(stored, '.lane-workspaces', 'packages', 'api', 'node_modules', 'hello-tool', 'package.json')), true);
});
