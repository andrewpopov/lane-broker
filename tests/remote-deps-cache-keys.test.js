import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeGitWorktree } from './remote-harness.js';
import { fixtureBase, repoFiles, setupWithNpmSpy, runLane } from './remote-deps-cache-fixture.js';

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

test('PKG-277: a non-workspace repo with safe root scripts keys its remoteDepsCacheKeyFiles: edit one and the cache misses, edit anything else and it hits', async () => {
  const s = setupWithNpmSpy();
  const files = repoFiles({ postinstall: 'harmless' });
  const run = async (edit = {}) => runLane(s, makeGitWorktree({ ...files, 'unrelated.txt': 'one', ...edit }));
  const cold = await run();
  assert.equal(cold.row.depsCache, 'miss');
  assert.match(cold.stderr, /published=yes/);
  assert.equal((await run()).row.depsCache, 'hit');
  assert.equal((await run({ 'unrelated.txt': 'two' })).row.depsCache, 'hit', 'an unrelated file is not in the key');
  const regenerated = await run({ 'gen-input.txt': 'generator input two' });
  assert.equal(regenerated.row.depsCache, 'miss', 'a changed key file must miss');
  assert.equal(s.npmCiCalls(), 2);
  assert.equal(s.storedKeys().length, 2);
});

test('a variable an .npmrc reads from a scrubbed name (LANE_OMIT) never reaches npm, so two values can not produce a false hit', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ dev: true, npmrcExtra: 'omit=${LANE_OMIT}\n' }));
  // if LANE_OMIT=dev reached npm it would leave the (dev) tool out of the tree and the command would fail
  const omitDev = await runLane(s, repoDir, { env: { ...s.env, LANE_OMIT: 'dev' } });
  assert.equal(omitDev.code, 0, omitDev.stderr);
  assert.equal(omitDev.probe.out, 'hello from the bin', 'the tree is the one a run without LANE_OMIT gets');
  const other = await runLane(s, repoDir, { env: { ...s.env, LANE_OMIT: 'optional' } });
  assert.equal(other.code, 0, other.stderr);
  assert.equal(other.probe.out, 'hello from the bin');
  assert.equal(other.row.depsCache, 'hit', 'same effective environment, same key');
  assert.equal(s.storedKeys().length, 1);
});

test('the per-run TMPDIR path is recorded for relocation too', async () => {
  const s = setupWithNpmSpy();
  const run = await runLane(s, makeGitWorktree(repoFiles({ postinstall: 'embed-tmpdir' })));
  assert.equal(run.code, 0, run.stderr);
  assert.equal(run.row.depsCache, 'miss');
  const recorded = JSON.parse(fs.readFileSync(path.join(s.storeDir, s.storedKeys()[0], 'meta.json'), 'utf8')).relocation;
  assert.deepEqual(recorded.entries, [{ relPath: 'hello-tool/tmp.txt', kind: 'text' }]);
  assert.deepEqual(recorded.prefixes.map((p) => p.roles), [['TMPDIR', 'TMP', 'TEMP']]);
});

test('runs submitted with different LC_ALL / LC_CTYPE / LANG share one key: the second is a hit (BRAIN-429)', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles());
  const first = await runLane(s, repoDir, { env: { ...s.env, LC_ALL: 'en_US.UTF-8', LANG: 'en_US.UTF-8' } });
  assert.equal(first.row.depsCache, 'miss');
  const second = await runLane(s, repoDir, { env: { ...s.env, LC_ALL: undefined, LC_CTYPE: 'C', LANG: 'fr_FR.UTF-8' } });
  assert.equal(second.code, 0, second.stderr);
  assert.match(second.stderr, /deps-cache hit key=/);
  assert.equal(s.storedKeys().length, 1);
});

test('SSH_AUTH_SOCK reaches the install (a git+ssh dependency needs it) but is not part of the key', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles());
  const first = await runLane(s, repoDir, { env: { ...s.env, SSH_AUTH_SOCK: '/run/agent-one.sock' } });
  assert.equal(first.row.depsCache, 'miss');
  assert.match(s.npmEnvLog(), /ssh_auth_sock=\/run\/agent-one\.sock/, 'npm ci was started with the agent socket');

  const second = await runLane(s, repoDir, { env: { ...s.env, SSH_AUTH_SOCK: '/run/agent-two.sock', GIT_SSH_COMMAND: 'ssh -i /other' } });
  assert.equal(second.row.depsCache, 'hit', 'a different agent socket is the same key');
});

test('with ignore-scripts in effect (from .npmrc) a hit does not replay the root script a miss did not run', async () => {
  const s = setupWithNpmSpy();
  const repoDir = makeGitWorktree(repoFiles({ prepare: 'git config core.hooksPath .githooks || true', npmrcExtra: 'ignore-scripts=true\n' }));
  const miss = await runLane(s, repoDir);
  assert.equal(miss.row.depsCache, 'miss');
  assert.equal(miss.probe.hooksPath, null, 'npm ci ran no scripts');
  const hit = await runLane(s, repoDir);
  assert.equal(hit.row.depsCache, 'hit');
  assert.equal(hit.probe.hooksPath, null, 'and neither did the hit');
});
