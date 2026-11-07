import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeGitWorktree } from './remote-harness.js';
import { repoFiles, setupWithNpmSpy, runLane } from './remote-deps-cache-fixture.js';

test('an install that embeds its own absolute path in a text file is cached, and the next run (another work dir) gets it rewritten', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ postinstall: 'embed' }));

  const first = await runLane(s, repoDir);
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'miss');
  assert.match(first.stderr, /deps-cache miss key=[0-9a-f]{12} dir=\. ms=\d+ published=yes/);
  assert.equal(s.storedKeys().length, 1);
  const recorded = JSON.parse(fs.readFileSync(path.join(s.storeDir, s.storedKeys()[0], 'meta.json'), 'utf8')).relocation;
  assert.deepEqual(recorded.entries, [{ relPath: 'hello-tool/where.txt', kind: 'text' }]);

  const second = await runLane(s, repoDir);
  assert.equal(second.row.depsCache, 'hit');
  assert.equal(s.npmCiCalls(), 1, 'a hit runs no npm ci');
  assert.equal(second.probe.out, 'hello from the bin');
});

test('a node-gyp build tree (Makefile and config.gypi naming the work dir) is cached without its intermediates, and the next run hits (BRAIN-423)', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ postinstall: 'node-gyp' }));

  const first = await runLane(s, repoDir);
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'miss');
  assert.match(first.stderr, /deps-cache miss key=[0-9a-f]{12} dir=\. ms=\d+ published=yes/);
  const stored = path.join(s.storeDir, s.storedKeys()[0], 'node_modules', 'hello-tool');
  assert.deepEqual(fs.readdirSync(path.join(stored, 'build')), ['Release']);
  assert.ok(fs.existsSync(path.join(stored, 'build', 'Release', 'hello.node')));

  const second = await runLane(s, repoDir);
  assert.equal(second.row.depsCache, 'hit');
  assert.equal(s.npmCiCalls(), 1, 'a hit runs no npm ci');
  assert.equal(second.probe.out, 'hello from the bin');
});

test('a snapshot with more than ~6,700 loose objects (the trigger of git gc --auto) still publishes: the snapshot commit starts no gc (BRAIN-423)', async () => {
  const s = setupWithNpmSpy();
  const files = repoFiles();
  for (let i = 0; i < 7200; i += 1) files[`many/f${i}.txt`] = `file ${i}\n`;
  const repoDir = makeGitWorktree(files);

  const first = await runLane(s, repoDir);
  assert.equal(first.code, 0, first.stderr);
  assert.match(first.stderr, /deps-cache miss key=[0-9a-f]{12} dir=\. ms=\d+ published=yes/);
  assert.equal(s.storedKeys().length, 1);
  const second = await runLane(s, repoDir);
  assert.equal(second.row.depsCache, 'hit');
});

test('an install script that creates a git tag (a ref change) is not published (BRAIN-423)', async () => {
  const s = setupWithNpmSpy();
  const run = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'git-tag' })));
  assert.equal(run.code, 0, run.stderr);
  assert.match(run.stderr, /published=no reason="install changed files outside node_modules: .*\.git/);
  assert.equal(s.storedKeys().length, 0);
});

test('a hit whose relocation cannot be verified is discarded and installed normally, with the reason logged', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ postinstall: 'embed' }));
  await runLane(s, repoDir);
  const meta = path.join(s.storeDir, s.storedKeys()[0], 'meta.json');
  const record = JSON.parse(fs.readFileSync(meta, 'utf8'));
  record.relocation.entries.push({ relPath: 'hello-tool/package.json', kind: 'text' }); // names no install path
  fs.chmodSync(meta, 0o644);
  fs.writeFileSync(meta, JSON.stringify(record));

  const second = await runLane(s, repoDir);
  assert.equal(second.code, 0, second.stderr);
  assert.match(second.stderr, /deps-cache materialize failed, installing instead: relocation: hello-tool\/package\.json holds no placeholder/);
  assert.equal(s.npmCiCalls(), 2, 'it installed');
  assert.equal(second.probe.out, 'hello from the bin');
});

test('a binary file that embeds the install path keeps the tree out of the cache', async () => {
  const s = setupWithNpmSpy();
  const first = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'embed-big' })));
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.row.depsCache, 'skip');
  assert.match(first.stderr, /reason=absolute-install-path-binary file=hello-tool\/native\.bin/);
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

test('every hit replays the allowlisted root script: core.hooksPath is set in the work dir whether the tree was installed or copied', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ prepare: 'git config core.hooksPath .githooks || true' }));
  const miss = await runLane(s, repoDir);
  assert.equal(miss.row.depsCache, 'miss');
  assert.equal(miss.probe.hooksPath, '.githooks', 'npm ci ran the script');

  const hit = await runLane(s, repoDir);
  assert.equal(hit.row.depsCache, 'hit');
  assert.equal(s.npmCiCalls(), 1);
  assert.equal(hit.probe.hooksPath, '.githooks', 'the hit replayed it, since npm ci did not run');
});

test('a root script that writes anything else under .git (a hook file) is not published', async () => {
  const s = setupWithNpmSpy();
  const run = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'hook-file' })));
  assert.equal(run.code, 0, run.stderr);
  assert.equal(run.row.depsCache, 'miss');
  assert.match(run.stderr, /published=no reason="install changed files outside node_modules: .*\.git\/hooks/);
  assert.equal(s.storedKeys().length, 0);
});

test('a prepublish root script (which npm ci runs) is not cached unless the lane declares it safe', async () => {
  const s = setupWithNpmSpy();
  const run = await runLane(s, makeGitWorktree(repoFiles({ scripts: { prepublish: 'node -e 0' } })));
  assert.equal(run.code, 0, run.stderr);
  assert.equal(run.row.depsCache, 'skip');
  assert.match(run.stderr, /reason="root lifecycle script not known to leave node_modules alone \(prepublish: node -e 0\)"/);
  assert.equal(s.storedKeys().length, 0);
});
