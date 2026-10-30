import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig } from './helpers.js';
import { paths } from '../src/state.js';
import { setup, tmpDir } from './remote-harness.js';

// BRAIN-721
const queued = (state) => (fs.existsSync(paths(state).queue) ? fs.readdirSync(paths(state).queue) : []);
const history = (state) => (fs.existsSync(paths(state).history) ? fs.readFileSync(paths(state).history, 'utf8') : '');

test('refuses a remote-eligible command naming a host-only path before admission', async () => {
  const { env, state, repoDir } = setup({ ssh: 'normal' });
  const hostFile = path.join(tmpDir('hostonly'), 'x.sh');
  fs.writeFileSync(hostFile, 'true');
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', hostFile], { env, cwd: repoDir });
  assert.equal(res.code, 64, res.stderr);
  assert.ok(res.stderr.includes(`names ${hostFile}, which exists only on this machine`), res.stderr);
  assert.deepEqual(queued(state), []);
  assert.equal(history(state), '');
});

test('the same command on a remote:false lane, or with --local --local-reason, is not refused by the host-path guard', async () => {
  const { env, repoDir } = setup({ ssh: 'normal' });
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true }, localonly: { weight: 1, remote: false } } });
  const hostFile = path.join(tmpDir('hostonly'), 'x.sh');
  fs.writeFileSync(hostFile, 'exit 0');
  const onLocalLane = await laneRun(['run', '--repo', 'r', '--lane', 'localonly', '--', 'sh', hostFile], { env, cwd: repoDir });
  assert.equal(onLocalLane.code, 0, onLocalLane.stderr);
  const forced = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'needs ssh', '--', 'sh', hostFile], { env, cwd: repoDir });
  assert.equal(forced.code, 0, forced.stderr);
  assert.doesNotMatch(onLocalLane.stderr + forced.stderr, /exists only on this machine/);
});
