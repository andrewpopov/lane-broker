import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { repoIdentity } from '../src/config.js';

let counter = 0;

function freshDir() {
  counter += 1;
  return fs.mkdtempSync(path.join(os.tmpdir(), `repo-identity-nosub-${counter}-`));
}

function initRepo(dir) {
  fs.mkdirSync(dir, { recursive: true });
  execFileSync('git', ['init', '-q'], { cwd: dir });
  execFileSync('git', ['config', 'user.email', 'test@example.com'], { cwd: dir });
  execFileSync('git', ['config', 'user.name', 'Test'], { cwd: dir });
}

/** Prepend a directory containing a `git` shim to PATH; the shim marks that it ran and fails. */
function installGitTripwire() {
  const shimDir = freshDir();
  const markerFile = path.join(shimDir, 'ran-marker');
  const shimPath = path.join(shimDir, 'git');
  fs.writeFileSync(
    shimPath,
    `#!/bin/sh\ntouch "${markerFile}"\nexit 1\n`,
    { mode: 0o755 },
  );
  return {
    markerFile,
    ran: () => fs.existsSync(markerFile),
    pathWithShim: `${shimDir}${path.delimiter}${process.env.PATH}`,
  };
}

test('a plain repo resolves to the realpath of its .git dir', () => {
  const dir = freshDir();
  initRepo(dir);

  const id = repoIdentity(dir);

  assert.equal(id, fs.realpathSync(path.join(dir, '.git')));
});

test('a linked worktree resolves to the same identity as its primary checkout', () => {
  const mainDir = freshDir();
  initRepo(mainDir);
  fs.writeFileSync(path.join(mainDir, 'README.md'), 'x');
  execFileSync('git', ['add', '.'], { cwd: mainDir });
  execFileSync('git', ['commit', '-q', '-m', 'init'], { cwd: mainDir });

  const wtDir = path.join(freshDir(), 'wt');
  execFileSync('git', ['worktree', 'add', wtDir, '-b', 'b'], { cwd: mainDir });

  const mainId = repoIdentity(mainDir);
  const wtId = repoIdentity(wtDir);

  assert.ok(mainId, 'main checkout must resolve to an identity');
  assert.equal(wtId, mainId, 'a worktree and its primary checkout must share one lane key');
});

test('a directory outside any repo does not resolve to this repo (lane-broker itself)', () => {
  const dir = freshDir();

  const id = repoIdentity(dir);
  const laneBrokerRepoRoot = path.resolve(new URL('..', import.meta.url).pathname);
  let laneBrokerId = null;
  try {
    laneBrokerId = fs.realpathSync(path.join(laneBrokerRepoRoot, '.git'));
  } catch {
    laneBrokerId = null;
  }

  assert.notEqual(id, laneBrokerId);
});

test('no subprocess is spawned on the happy path', () => {
  const dir = freshDir();
  initRepo(dir);
  const tripwire = installGitTripwire();
  const originalPath = process.env.PATH;

  let id;
  try {
    process.env.PATH = tripwire.pathWithShim;
    id = repoIdentity(dir);
  } finally {
    process.env.PATH = originalPath;
  }

  assert.equal(tripwire.ran(), false, 'git must not be invoked when the filesystem walk can resolve identity');
  assert.equal(id, fs.realpathSync(path.join(dir, '.git')));
});

test('repoIdentity memoizes per resolved cwd', () => {
  const dir = freshDir();
  initRepo(dir);
  const tripwire = installGitTripwire();
  const originalPath = process.env.PATH;

  let first;
  let second;
  try {
    process.env.PATH = tripwire.pathWithShim;
    first = repoIdentity(dir);
    second = repoIdentity(dir);
  } finally {
    process.env.PATH = originalPath;
  }

  assert.equal(first, second);
  assert.equal(tripwire.ran(), false);
  assert.equal(first, fs.realpathSync(path.join(dir, '.git')));
});
