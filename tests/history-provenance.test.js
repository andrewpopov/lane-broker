import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { writeGlobalConfig, writeRepoConfig, gitFixture } from './helpers.js';
import { setup as remoteSetup, tmpDir, markerCmd, detachAndWait } from './remote-harness.js';
import { paths } from '../src/state.js';

/**
 * BRAIN-347: every history row says which executor ran it, and a local run of
 * a remote-capable lane says why it was local.
 */

function historyOf(state, id) {
  const rows = fs
    .readFileSync(paths(state).history, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l));
  const row = rows.find((r) => r.id === id);
  assert.ok(row, `no history row for ${id}`);
  return row;
}

async function runAndRead(ctx, extraArgs = [], env = ctx.env, stdoutText = null) {
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', ...extraArgs, '--', ...markerCmd(marker, 0, stdoutText)],
    env,
    ctx.repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  return { row: historyOf(ctx.state, id), marker };
}

test('remote run: executor remote with the runner, no localReason', async () => {
  const ctx = remoteSetup({ ssh: 'normal' });
  const { row, marker } = await runAndRead(ctx);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  assert.equal(row.executor, 'remote');
  assert.equal(row.runner, 'skybox');
  assert.equal(row.localReason, undefined);
});

test('--local on a remote-capable lane: localReason forced-flag', async () => {
  const ctx = remoteSetup({ ssh: 'normal' });
  const { row } = await runAndRead(ctx, ['--local']);
  assert.equal(row.executor, 'local');
  assert.equal(row.localReason, 'forced-flag');
});

test('LANE_BROKER_LOCAL=1 on a remote-capable lane: localReason forced-env', async () => {
  const ctx = remoteSetup({ ssh: 'normal' });
  const { row } = await runAndRead(ctx, [], { ...ctx.env, LANE_BROKER_LOCAL: '1' });
  assert.equal(row.executor, 'local');
  assert.equal(row.localReason, 'forced-env');
});

test('remote-capable lane with no runners configured: localReason not-eligible', async () => {
  const ctx = remoteSetup({ ssh: 'normal' });
  writeGlobalConfig(ctx.home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const { row } = await runAndRead(ctx);
  assert.equal(row.executor, 'local');
  assert.equal(row.localReason, 'not-eligible');
});

test('runner unreachable: local fallback carries fallback:<the supervisor reason>', async () => {
  const ctx = remoteSetup({ ssh: 'down' });
  const { row, marker } = await runAndRead(ctx);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  assert.equal(row.executor, 'local');
  assert.match(row.localReason, /^fallback:skybox: /);
});

test('a lane that never opted into remote carries no localReason', async () => {
  const ctx = remoteSetup({ ssh: 'normal' });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const { row } = await runAndRead(ctx);
  assert.equal(row.executor, 'local');
  assert.equal(row.localReason, undefined);
});

test('command is recorded truncated; headTree is the HEAD tree sha, omitted when HEAD has none', async () => {
  const ctx = remoteSetup({ ssh: 'normal' });
  const first = await runAndRead(ctx, ['--local'], ctx.env, 'y'.repeat(400));
  assert.equal(first.row.headTree, undefined, 'a repo with no commits has no HEAD tree');
  assert.equal(first.row.command.length, 300, 'command must be truncated to 300 chars');

  fs.writeFileSync(path.join(ctx.repoDir, 'f.txt'), 'x');
  gitFixture(['add', '-A'], ctx.repoDir);
  gitFixture(['commit', '-q', '-m', 'c'], ctx.repoDir);
  const tree = gitFixture(['rev-parse', 'HEAD^{tree}'], ctx.repoDir).trim();
  const second = await runAndRead(ctx, ['--local']);
  assert.equal(second.row.headTree, tree);
});
