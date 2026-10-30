import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { laneRun, writeRepoConfig } from './helpers.js';
import { paths } from '../src/state.js';
import { setup } from './remote-harness.js';
import { acceptedLocalReason, localReasonRefusal } from '../src/local-reason.js';

// BRAIN-724
const queued = (state) => (fs.existsSync(paths(state).queue) ? fs.readdirSync(paths(state).queue) : []);
const history = (state) => (fs.existsSync(paths(state).history) ? fs.readFileSync(paths(state).history, 'utf8') : '');

const REFUSED = [
  "worktree on this Mac's Lexar volume",
  'uses the dist built in this worktree',
  'runs against the dist built in this worktree',
  'remote lanes lack built @jun/client',
  'worktree-local-dist',
  'focused tests in worktree',
  'screenshots must be written to this Mac for the phone review',
  'review screenshots must land on this machine',
  'remote runner has no Chromium',
  'harness output lands in the worktree',
];
const ACCEPTED = [
  'needs ssh to pitelite',
  'calls gh pr view',
  'uses lane status',
  'a browser that must render on this Mac',
  'reads a gitignored input',
  'reads a path outside the worktree',
  'remote snapshots have no git history',
  'needs the local docker daemon (BRAIN-655)',
  "xcodebuild needs this Mac's Xcode",
  'Andrew approved local golden-clear recording: wintop runs replay specs far below real time',
];

for (const reason of REFUSED) test(`refuses reason: ${reason}`, () => assert.equal(acceptedLocalReason(reason), false));
for (const reason of ACCEPTED) test(`accepts reason: ${reason}`, () => assert.equal(acceptedLocalReason(reason), true));

test('the refusal quotes the reason and points at remoteSetup and remoteArtifacts', () => {
  const text = localReasonRefusal('default', 'worktree-local-dist').join('\n');
  assert.match(text, /--local-reason "worktree-local-dist"/);
  assert.match(text, /remoteSetup/);
  assert.match(text, /remoteArtifacts/);
  assert.match(text, /Accepted reasons name one of: ssh or scp/);
});

for (const reason of ["worktree on this Mac's Lexar volume", 'uses the dist built in this worktree']) {
  test(`run refuses --local-reason "${reason}" before admission`, async () => {
    const { env, state, repoDir } = setup({ ssh: 'normal' });
    const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', reason, '--', 'true'], { env, cwd: repoDir });
    assert.equal(res.code, 64, res.stderr);
    assert.ok(res.stderr.includes('remoteArtifacts'), res.stderr);
    assert.deepEqual(queued(state), []);
    assert.equal(history(state), '');
  });
}

test('run refuses a bad LANE_BROKER_LOCAL_REASON from the environment', async () => {
  const { env, state, repoDir } = setup({ ssh: 'normal' });
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], {
    env: { ...env, LANE_BROKER_LOCAL: '1', LANE_BROKER_LOCAL_REASON: 'uses the dist built in this worktree' },
    cwd: repoDir,
  });
  assert.equal(res.code, 64, res.stderr);
  assert.deepEqual(queued(state), []);
  assert.equal(history(state), '');
});

test('run accepts a genuine reason, and a bad one on a remote:false lane is not checked', async () => {
  const { env, state, repoDir } = setup({ ssh: 'normal' });
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true }, localonly: { weight: 1, remote: false } } });
  const genuine = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'needs ssh to pitelite', '--', 'true'], { env, cwd: repoDir });
  assert.equal(genuine.code, 0, genuine.stderr);
  assert.match(history(state), /needs ssh to pitelite/);
  const onLocalLane = await laneRun(['run', '--repo', 'r', '--lane', 'localonly', '--local', '--local-reason', 'worktree-local-dist', '--', 'true'], { env, cwd: repoDir });
  assert.equal(onLocalLane.code, 0, onLocalLane.stderr);
});
