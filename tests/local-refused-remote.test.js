import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig } from './helpers.js';
import { setup as remoteSetup, tmpDir, markerCmd, detachAndWait, resultOf } from './remote-harness.js';

/**
 * BRAIN-320: a `localRefused` lane (rouge's `sim`) that is ALSO
 * remote-eligible must try a remote runner FIRST -- only a fallback to
 * LOCAL execution is refused. Builds on the fake-ssh harness
 * (tests/remote-harness.js) already shared by tests/remote-dispatch.test.js.
 */

/** remoteSetup() writes a `default` lane with `remote: true` but no
 *  `localRefused` -- overwrite the repo config with both flags set. */
function setupLocalRefusedRemote(opts) {
  const ctx = remoteSetup(opts);
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, localRefused: true } } });
  return ctx;
}

test('localRefused + remote + a usable runner: runs REMOTELY, exit from the command', async () => {
  const { env, state, repoDir } = setupLocalRefusedRemote({ ssh: 'normal' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
});

test('localRefused + remote + runner down: exit 69 with the fleet hint, never ran locally', async () => {
  const { env, state, repoDir } = setupLocalRefusedRemote({ ssh: 'down' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 69, `stderr: ${waited.stderr}`);
  const result = resultOf(state, id);
  assert.match(result.error, /fleet/i);
  assert.equal(fs.existsSync(marker), false, 'the command must never have run locally');
});

test('localRefused + remote + runner down + --allow-local-sim: falls back and runs locally', async () => {
  const { env, state, repoDir } = setupLocalRefusedRemote({ ssh: 'down' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--allow-local-sim', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'local');
});

test('localRefused WITHOUT remote: refused immediately with exit 69, exactly as today', async () => {
  const { env, repoDir } = remoteSetup({ ssh: 'normal' });
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, localRefused: true } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 69);
  assert.match(result.stderr, /fleet/i);
});

test('localRefused + remote:true, but forced local via --local: refused immediately with exit 69', async () => {
  const { env, repoDir } = setupLocalRefusedRemote({ ssh: 'normal' });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 69);
  assert.match(result.stderr, /fleet/i);
});
