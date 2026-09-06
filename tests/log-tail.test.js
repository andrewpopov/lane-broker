import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';

test('a child writing 200k lines has every line present in the log -- no tail lost on exit', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const logPath = path.join(base, 'chatty.log');
  const script = 'for (let i = 0; i < 200000; i++) console.log("line " + i);';

  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', 'node', '-e', script], {
    env,
    cwd: repoDir,
  });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);

  const lines = fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean);
  assert.equal(lines.length, 200000, `expected all 200000 lines, got ${lines.length}`);
  assert.equal(lines[lines.length - 1], 'line 199999', 'the last line must not be truncated');
});
