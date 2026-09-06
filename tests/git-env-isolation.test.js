import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { gitFixture } from './helpers.js';

/**
 * Proves the incident this suite hit for real cannot recur: git exports its
 * repository-local env vars (GIT_DIR, GIT_INDEX_FILE, GIT_WORK_TREE, ...) to
 * hooks, and those leak into every child process. A pre-push run leaked them
 * into `npm test`, and this suite's own git fixtures then operated on the
 * live lane-broker repo instead of their temp dirs. gitFixture() must scrub
 * that inherited environment before shelling out to git.
 */
test('a leaked GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE does not redirect a git fixture', () => {
  const victimDir = fs.mkdtempSync(path.join(os.tmpdir(), 'git-env-isolation-victim-'));
  gitFixture(['init', '-q'], victimDir);
  fs.writeFileSync(path.join(victimDir, 'README.md'), 'victim');
  gitFixture(['add', '.'], victimDir);
  gitFixture(['commit', '-q', '-m', 'victim init'], victimDir);

  const victimHeadBefore = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: victimDir, encoding: 'utf8' }).trim();
  const victimBareBefore = execFileSync('git', ['config', '--get', 'core.bare'], { cwd: victimDir, encoding: 'utf8' }).trim();
  const victimWorktreesBefore = execFileSync('git', ['worktree', 'list'], { cwd: victimDir, encoding: 'utf8' });

  const fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), 'git-env-isolation-fixture-'));
  const originalEnv = {
    GIT_DIR: process.env.GIT_DIR,
    GIT_WORK_TREE: process.env.GIT_WORK_TREE,
    GIT_INDEX_FILE: process.env.GIT_INDEX_FILE,
  };
  try {
    // Simulate a hook's leaked environment pointing at the victim repo.
    process.env.GIT_DIR = path.join(victimDir, '.git');
    process.env.GIT_WORK_TREE = victimDir;
    process.env.GIT_INDEX_FILE = path.join(victimDir, '.git', 'index');

    gitFixture(['init', '-q'], fixtureDir);
    fs.writeFileSync(path.join(fixtureDir, 'fixture.txt'), 'fixture');
    gitFixture(['add', '.'], fixtureDir);
    gitFixture(['commit', '-q', '-m', 'fixture commit'], fixtureDir);
  } finally {
    for (const [name, value] of Object.entries(originalEnv)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }

  const victimHeadAfter = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: victimDir, encoding: 'utf8' }).trim();
  const victimBareAfter = execFileSync('git', ['config', '--get', 'core.bare'], { cwd: victimDir, encoding: 'utf8' }).trim();
  const victimWorktreesAfter = execFileSync('git', ['worktree', 'list'], { cwd: victimDir, encoding: 'utf8' });

  assert.equal(victimHeadAfter, victimHeadBefore, 'the victim repo HEAD must be unchanged by a fixture run elsewhere');
  assert.equal(victimBareAfter, victimBareBefore, 'core.bare on the victim repo must be unchanged');
  assert.equal(victimWorktreesAfter, victimWorktreesBefore, 'no stray worktree may be registered against the victim repo');

  const fixtureHead = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: fixtureDir, encoding: 'utf8' }).trim();
  assert.notEqual(fixtureHead, victimHeadBefore, 'the fixture commit must land in the fixture repo, not the victim');
});
