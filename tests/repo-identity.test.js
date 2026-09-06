import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { resolveTicketConfig } from '../src/config.js';
import { freshEnv, gitFixture } from './helpers.js';

test('--repo does not change the key for a real git repo: with and without --repo resolve identically', async () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'real-repo');
  fs.mkdirSync(repoDir, { recursive: true });
  gitFixture(['init', '-q'], repoDir);
  fs.writeFileSync(path.join(repoDir, 'README.md'), 'x');
  gitFixture(['add', '.'], repoDir);
  gitFixture(['commit', '-q', '-m', 'init'], repoDir);

  const withRepo = resolveTicketConfig({ cwd: repoDir, repo: 'some-custom-name', lane: 'default' });
  const withoutRepo = resolveTicketConfig({ cwd: repoDir, repo: undefined, lane: 'default' });

  assert.equal(withRepo.key, withoutRepo.key, 'the same git repo must resolve to the same key regardless of --repo');
});
