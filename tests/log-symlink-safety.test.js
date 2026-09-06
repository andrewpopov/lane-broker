import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';

test('a custom --log path that is a symlink is refused, not followed, and the lane still completes', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const realTarget = path.join(base, 'real-target.log');
  fs.writeFileSync(realTarget, '');
  const logSymlink = path.join(base, 'log-symlink.log');
  fs.symlinkSync(realTarget, logSymlink);

  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--log', logSymlink, '--', 'echo', 'hi'], {
    env,
    cwd: repoDir,
  });

  // The lane itself must still complete successfully -- a broken log stream
  // must never crash the supervisor or fail the lane it is running.
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  // The symlink target must be untouched: opening through a symlink is refused.
  assert.equal(fs.readFileSync(realTarget, 'utf8'), '', 'the symlink target must never receive the log output');
});
