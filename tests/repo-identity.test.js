import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { resolveTicketConfig } from '../src/config.js';
import { freshEnv } from './helpers.js';

test('--repo does not change the key for a real git repo: with and without --repo resolve identically', async () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'real-repo');
  fs.mkdirSync(repoDir, { recursive: true });
  execFileSync('git', ['init', '-q'], { cwd: repoDir });
  execFileSync('git', ['config', 'user.email', 'test@example.com'], { cwd: repoDir });
  execFileSync('git', ['config', 'user.name', 'Test'], { cwd: repoDir });
  fs.writeFileSync(path.join(repoDir, 'README.md'), 'x');
  execFileSync('git', ['add', '.'], { cwd: repoDir });
  execFileSync('git', ['commit', '-q', '-m', 'init'], { cwd: repoDir });

  const withRepo = resolveTicketConfig({ cwd: repoDir, repo: 'some-custom-name', lane: 'default' });
  const withoutRepo = resolveTicketConfig({ cwd: repoDir, repo: undefined, lane: 'default' });

  assert.equal(withRepo.key, withoutRepo.key, 'the same git repo must resolve to the same key regardless of --repo');
});
