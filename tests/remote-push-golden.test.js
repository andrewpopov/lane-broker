import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { dispatchRemote } from '../src/remote-client.js';
import { tmpDir, makeFakeSshBin, makeRunner, clientEnv, makeGitWorktree, makeDispatchArgs } from './remote-harness.js';

const PUSH_STDIN_SHA256 = 'a27aa27af4411ebc552a1c85861d8d44f83788b36fd6dfdbb8cbc7b6db9534da';

/** Every optional header field set, so the golden covers the whole of `buildExecHeader`, not just the always-present ones. */
const FULL_HEADER_ARGS = {
  ticketId: '00000000-0000-4000-8000-000000000001',
  weight: 2,
  cpuCores: 3,
  memoryBytes: 1024,
  relCwd: 'pkg',
  repoKey: 'golden-repo',
  lane: 'golden-lane',
  remoteDeps: [{ name: 'deps', cwd: '.', argv: ['npm', 'ci'] }],
  remoteSetup: [{ name: 'setup', argv: ['true'] }],
  remoteDepsCache: false,
  remoteDepsCacheRootScriptsSafe: true,
  remoteDepsCacheKeyFiles: ['package-lock.json'],
  remoteDepsCacheKeyFilesCoverInstallScripts: true,
  remoteArtifacts: ['reports/**'],
  remoteArtifactsOn: 'always',
  minCpuCores: 1,
  maxExecutionCpuCores: 4,
  queueTimeoutMs: 5000,
  noProgressTimeoutMs: 6000,
  silenceTimeoutMs: 7000,
  laneClass: 'sim',
  classEnforcement: 'required',
  priorityRequested: 'high',
  priorityAccruedMs: 8000,
  group: 'job-1',
};

test('push golden: the bytes `remote-exec` receives on stdin (header line and snapshot stream) are unchanged by the extraction', async () => {
  const { binDir, sshBin, teePath } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const worktreeRoot = makeGitWorktree({ 'a.txt': 'alpha\n', 'pkg/b.txt': 'bravo\n' });
  await dispatchRemote({
    ...makeDispatchArgs(FULL_HEADER_ARGS),
    runner: { ...makeRunner({ ssh: 'tee', root: tmpDir('golden-root') }), speedFactor: 1.5 },
    worktreeRoot,
    sshBin,
    env,
    transfer: undefined,
  });
  const bytes = fs.readFileSync(teePath);
  const headerLine = bytes.subarray(0, bytes.indexOf(10)).toString('utf8');
  assert.equal(
    JSON.stringify(Object.keys(JSON.parse(headerLine))),
    JSON.stringify(['protocol', 'ticketId', 'generation', 'repoKey', 'lane', 'weight', 'cpuCores', 'memoryBytes', 'argv', 'relCwd', 'runnerName', 'runnerSpeedFactor', 'remoteDeps', 'remoteSetup', 'remoteDepsCache', 'remoteDepsCacheRootScriptsSafe', 'remoteDepsCacheKeyFiles', 'remoteDepsCacheKeyFilesCoverInstallScripts', 'remoteArtifacts', 'remoteArtifactsOn', 'minCpuCores', 'maxExecutionCpuCores', 'queueTimeoutMs', 'noProgressTimeoutMs', 'silenceTimeoutMs', 'laneClass', 'classEnforcement', 'priorityRequested', 'priorityAccruedMs', 'group', 'manifest']),
  );
  assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'), PUSH_STDIN_SHA256);
});
