import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { gitFixture } from './helpers.js';
import {
  buildManifest,
  verifyManifestNoGit,
  RemoteIneligibleError,
  manifestHashOf,
  isCanonicalRelPath,
  validateRemoteDeps,
} from '../src/remote-manifest.js';

function tmpRepo() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-manifest-test-'));
  gitFixture(['init', '-q'], dir);
  return dir;
}

function entryFor(manifest, relPath) {
  return manifest.entries.find((e) => e.path === relPath);
}

test('tracked + untracked-not-ignored files are both included', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, 'tracked.txt'), 'hello');
  gitFixture(['add', 'tracked.txt'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  fs.writeFileSync(path.join(dir, 'untracked.txt'), 'world');

  const manifest = buildManifest(dir);
  assert.ok(entryFor(manifest, 'tracked.txt'));
  assert.ok(entryFor(manifest, 'untracked.txt'));
  assert.equal(entryFor(manifest, 'tracked.txt').type, 'file');
  assert.equal(entryFor(manifest, 'tracked.txt').sha256.length, 64);
});

test('ignored .env is excluded entirely', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, '.gitignore'), '.env\n');
  gitFixture(['add', '.gitignore'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  fs.writeFileSync(path.join(dir, '.env'), 'SECRET=1');

  const manifest = buildManifest(dir);
  assert.equal(entryFor(manifest, '.env'), undefined);
});

test('tracked .env is ineligible even though git tracks it', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, '.env'), 'SECRET=1');
  gitFixture(['add', '-f', '.env'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);

  assert.throws(() => buildManifest(dir), RemoteIneligibleError);
});

test('.env.example is allowed', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, '.env.example'), 'SECRET=changeme');
  gitFixture(['add', '.env.example'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);

  const manifest = buildManifest(dir);
  assert.ok(entryFor(manifest, '.env.example'));
});

// ---- BRAIN-319 review finding #4: template-suffixed .env variants ----

for (const name of ['.env.worker.example', '.env.sample', '.env.dist']) {
  test(`${name} (a template, not a secret) is allowed`, () => {
    const dir = tmpRepo();
    fs.writeFileSync(path.join(dir, name), 'SECRET=changeme');
    gitFixture(['add', '-f', name], dir);
    gitFixture(['commit', '-q', '-m', 'init'], dir);

    const manifest = buildManifest(dir);
    assert.ok(entryFor(manifest, name), `${name} should be included`);
  });
}

for (const name of ['.env', '.env.local', '.env.production']) {
  test(`${name} stays denylisted`, () => {
    const dir = tmpRepo();
    fs.writeFileSync(path.join(dir, name), 'SECRET=1');
    gitFixture(['add', '-f', name], dir);
    gitFixture(['commit', '-q', '-m', 'init'], dir);

    assert.throws(() => buildManifest(dir), RemoteIneligibleError, name);
  });
}

test('a *.pem file and an id_rsa-shaped file are ineligible', () => {
  for (const name of ['server.pem', 'id_rsa']) {
    const dir = tmpRepo();
    fs.writeFileSync(path.join(dir, name), 'x');
    gitFixture(['add', '-f', name], dir);
    gitFixture(['commit', '-q', '-m', 'init'], dir);
    assert.throws(() => buildManifest(dir), RemoteIneligibleError, name);
  }
});

test('staged add, rename, delete and an unstaged delete are all reflected via the filesystem', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, 'a.txt'), 'a');
  fs.writeFileSync(path.join(dir, 'b.txt'), 'b');
  fs.writeFileSync(path.join(dir, 'c.txt'), 'c');
  gitFixture(['add', '.'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);

  // staged add
  fs.writeFileSync(path.join(dir, 'd.txt'), 'd');
  gitFixture(['add', 'd.txt'], dir);
  // staged rename (a.txt -> a2.txt)
  fs.renameSync(path.join(dir, 'a.txt'), path.join(dir, 'a2.txt'));
  gitFixture(['add', 'a.txt', 'a2.txt'], dir);
  // staged delete
  fs.rmSync(path.join(dir, 'b.txt'));
  gitFixture(['add', 'b.txt'], dir);
  // unstaged delete
  fs.rmSync(path.join(dir, 'c.txt'));

  const manifest = buildManifest(dir);
  assert.ok(entryFor(manifest, 'd.txt'), 'staged add present');
  assert.ok(entryFor(manifest, 'a2.txt'), 'staged rename target present');
  assert.equal(entryFor(manifest, 'a.txt'), undefined, 'staged rename source absent');
  assert.equal(entryFor(manifest, 'b.txt'), undefined, 'staged delete absent');
  assert.equal(entryFor(manifest, 'c.txt'), undefined, 'unstaged delete absent');
});

test('exec bit is recorded', () => {
  const dir = tmpRepo();
  const file = path.join(dir, 'run.sh');
  fs.writeFileSync(file, '#!/bin/sh\necho hi\n');
  fs.chmodSync(file, 0o755);
  gitFixture(['add', 'run.sh'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);

  const manifest = buildManifest(dir);
  assert.equal(entryFor(manifest, 'run.sh').exec, true);
});

test('a symlink resolving inside the worktree is included', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, 'target.txt'), 'content');
  fs.symlinkSync('target.txt', path.join(dir, 'link.txt'));
  gitFixture(['add', 'target.txt', 'link.txt'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);

  const manifest = buildManifest(dir);
  const link = entryFor(manifest, 'link.txt');
  assert.equal(link.type, 'symlink');
  assert.equal(link.target, 'target.txt');
});

test('a symlink escaping the worktree is ineligible', () => {
  const dir = tmpRepo();
  fs.symlinkSync('../../../../etc/passwd', path.join(dir, 'escape.txt'));
  gitFixture(['add', 'escape.txt'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);

  assert.throws(() => buildManifest(dir), RemoteIneligibleError);
});

test('a symlink chain that escapes only when actually followed is ineligible', () => {
  // a/b/x -> ../..   (a real symlink entry, itself lexically fine: a/b/../.. == root)
  // a/b/s -> x/../y  (lexically normalizes to a/y -- inside root -- but FOLLOWING it
  //                   means stepping through a/b/x first, whose own target escapes)
  const dir = tmpRepo();
  fs.mkdirSync(path.join(dir, 'a', 'b'), { recursive: true });
  fs.symlinkSync('../..', path.join(dir, 'a', 'b', 'x'));
  fs.symlinkSync('x/../y', path.join(dir, 'a', 'b', 's'));
  gitFixture(['add', '-A'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);

  assert.throws(() => buildManifest(dir), RemoteIneligibleError);
});

test('a gitlink/submodule entry is ineligible', () => {
  const dir = tmpRepo();
  const subRepo = tmpRepo();
  fs.writeFileSync(path.join(subRepo, 'x.txt'), 'x');
  gitFixture(['add', 'x.txt'], subRepo);
  gitFixture(['commit', '-q', '-m', 'sub'], subRepo);
  const subSha = gitFixture(['rev-parse', 'HEAD'], subRepo).trim();

  // Simulate a gitlink entry without depending on network submodule fetch:
  // stage a 160000 tree entry pointing at the sub-repo's commit directly.
  gitFixture(['update-index', '--add', '--cacheinfo', `160000,${subSha},subrepo`], dir);
  gitFixture(['commit', '-q', '-m', 'add gitlink'], dir);
  fs.mkdirSync(path.join(dir, 'subrepo'));
  fs.writeFileSync(path.join(dir, 'subrepo', 'x.txt'), 'x');

  assert.throws(() => buildManifest(dir), RemoteIneligibleError);
});

// ---- BRAIN-319 review finding #1: canonical path predicate ----

test('isCanonicalRelPath accepts plain relative paths and rejects non-canonical components', () => {
  assert.equal(isCanonicalRelPath('a/b'), true);
  assert.equal(isCanonicalRelPath('a'), true);
  assert.equal(isCanonicalRelPath('a/./b'), false);
  assert.equal(isCanonicalRelPath('./a'), false);
  assert.equal(isCanonicalRelPath('a/'), false);
  assert.equal(isCanonicalRelPath('a\\b'), false);
  assert.equal(isCanonicalRelPath('a/../b'), false);
  assert.equal(isCanonicalRelPath('/a'), false);
  assert.equal(isCanonicalRelPath(''), false);
  assert.equal(isCanonicalRelPath('a//b'), false);
});

test('manifestHashOf is stable regardless of input entry order', () => {
  const a = [
    { path: 'b.txt', type: 'file', exec: false, size: 1, sha256: 'aa' },
    { path: 'a.txt', type: 'file', exec: false, size: 1, sha256: 'bb' },
  ];
  const b = [a[1], a[0]];
  assert.equal(manifestHashOf(a), manifestHashOf(b));
});

test('verifyManifestNoGit passes on a directory matching the manifest exactly', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, 'a.txt'), 'hello');
  gitFixture(['add', 'a.txt'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  const manifest = buildManifest(dir);

  const plainDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-manifest-verify-'));
  fs.writeFileSync(path.join(plainDir, 'a.txt'), 'hello');

  const result = verifyManifestNoGit(plainDir, manifest);
  assert.deepEqual(result, { ok: true });
});

test('verifyManifestNoGit reports a missing entry', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, 'a.txt'), 'hello');
  gitFixture(['add', 'a.txt'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  const manifest = buildManifest(dir);

  const plainDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-manifest-verify-'));

  const result = verifyManifestNoGit(plainDir, manifest);
  assert.equal(result.ok, false);
  assert.match(result.reason, /^missing: a\.txt$/);
});

test('verifyManifestNoGit reports an extra file', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, 'a.txt'), 'hello');
  gitFixture(['add', 'a.txt'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  const manifest = buildManifest(dir);

  const plainDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-manifest-verify-'));
  fs.writeFileSync(path.join(plainDir, 'a.txt'), 'hello');
  fs.writeFileSync(path.join(plainDir, 'extra.txt'), 'oops');

  const result = verifyManifestNoGit(plainDir, manifest);
  assert.equal(result.ok, false);
  assert.match(result.reason, /^extra: extra\.txt$/);
});

test('verifyManifestNoGit reports a hash mismatch', () => {
  const dir = tmpRepo();
  fs.writeFileSync(path.join(dir, 'a.txt'), 'hello');
  gitFixture(['add', 'a.txt'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  const manifest = buildManifest(dir);

  const plainDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-manifest-verify-'));
  fs.writeFileSync(path.join(plainDir, 'a.txt'), 'HELLO'); // same length as 'hello', different bytes

  const result = verifyManifestNoGit(plainDir, manifest);
  assert.equal(result.ok, false);
  assert.match(result.reason, /^hash mismatch: a\.txt$/);
});

// ---- BRAIN-320 S1a/1c: validateRemoteDeps ----

function manifestOf(paths) {
  return { entries: paths.map((p) => ({ path: p, type: 'file' })) };
}

test('validateRemoteDeps: missing lockfile makes the dir ineligible', () => {
  const manifest = manifestOf(['web/package.json', 'web/src/index.js']);
  const result = validateRemoteDeps(manifest, ['web']);
  assert.equal(result.ok, false);
  assert.match(result.reason, /package-lock\.json/);
});

test('validateRemoteDeps: a shrinkwrap alongside the lockfile makes the dir ineligible', () => {
  const manifest = manifestOf(['web/package-lock.json', 'web/npm-shrinkwrap.json']);
  const result = validateRemoteDeps(manifest, ['web']);
  assert.equal(result.ok, false);
  assert.match(result.reason, /npm-shrinkwrap\.json/);
});

test('validateRemoteDeps: a manifest entry under node_modules makes the dir ineligible', () => {
  const manifest = manifestOf(['web/package-lock.json', 'web/node_modules/x/index.js']);
  const result = validateRemoteDeps(manifest, ['web']);
  assert.equal(result.ok, false);
  assert.match(result.reason, /node_modules/);
});

test('validateRemoteDeps: an entry literally named <dir>/node_modules is also caught', () => {
  const manifest = manifestOf(['web/package-lock.json', 'web/node_modules']);
  const result = validateRemoteDeps(manifest, ['web']);
  assert.equal(result.ok, false);
  assert.match(result.reason, /node_modules/);
});

test('validateRemoteDeps: two overlapping dirs (one an ancestor of the other) are rejected', () => {
  const manifest = manifestOf(['package-lock.json', 'web/package-lock.json']);
  const result = validateRemoteDeps(manifest, ['.', 'web']);
  assert.equal(result.ok, false);
  assert.match(result.reason, /overlap/);
});

test('validateRemoteDeps: two identical dirs overlap', () => {
  const manifest = manifestOf(['web/package-lock.json']);
  const result = validateRemoteDeps(manifest, ['web', 'web']);
  assert.equal(result.ok, false);
  assert.match(result.reason, /overlap/);
});

test('validateRemoteDeps: "." overlaps every other dir', () => {
  const manifest = manifestOf(['package-lock.json', 'server/package-lock.json']);
  const result = validateRemoteDeps(manifest, ['.', 'server']);
  assert.equal(result.ok, false);
  assert.match(result.reason, /overlap/);
});

test('validateRemoteDeps: a valid single "." dir passes', () => {
  const manifest = manifestOf(['package-lock.json', 'src/index.js']);
  const result = validateRemoteDeps(manifest, ['.']);
  assert.deepEqual(result, { ok: true });
});

test('validateRemoteDeps: valid non-overlapping sibling dirs pass', () => {
  const manifest = manifestOf(['api/package-lock.json', 'web/package-lock.json']);
  const result = validateRemoteDeps(manifest, ['api', 'web']);
  assert.deepEqual(result, { ok: true });
});
