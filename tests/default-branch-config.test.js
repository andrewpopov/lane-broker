import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, gitFixture } from './helpers.js';
import { paths } from '../src/state.js';
import { readRows } from '../src/suggest.js';
import { repoConfigSource, CONFIG_FROM_ENV } from '../src/config.js';

// BRAIN-713: `lane run` reads .lane-broker.json from the repo's default branch, so a merged lane change reaches every existing worktree.

const SHOW_GRANT = ['sh', '-c', 'echo "grant=$LANE_BROKER_CPU_CORES"'];
const withCpu = (cpuCores) => ({ version: 1, lanes: { l: { weight: 1, cpuCores } } });
const grantOf = (r) => Number(/grant=([\d.]+)/.exec(r.stdout)?.[1]);
const USING_LINE = /lane run: using \.lane-broker\.json from /g;

/** A repo on `main` whose committed config is `committed`; no git when `git` is false. */
function repoFixture({ committed, git = true }) {
  const { base, home, env, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 16, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, committed);
  if (git) {
    gitFixture(['init', '-q'], repoDir);
    gitFixture(['add', '-A'], repoDir);
    gitFixture(['commit', '-q', '-m', 'x'], repoDir);
  }
  return { env, repoDir, base, state };
}
const commitConfig = (dir, cfg) => {
  writeRepoConfig(dir, cfg);
  gitFixture(['add', '-A'], dir);
  gitFixture(['commit', '-q', '-m', 'config'], dir);
};
const run = (f, cwd, extraEnv = {}) =>
  laneRun(['run', '--repo', 'r', '--lane', 'l', '--', ...SHOW_GRANT], { env: { ...f.env, ...extraEnv }, cwd });
const lastRow = (f) => readRows(paths(f.state).history).at(-1);

test('an old worktree is granted the value main has now, and says so once on stderr', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const old = path.join(f.base, 'old');
  gitFixture(['worktree', 'add', '-q', '-b', 'old', old], f.repoDir);
  commitConfig(f.repoDir, withCpu(3));
  const r = await run(f, old);
  assert.equal(r.code, 0, r.stderr);
  assert.equal(grantOf(r), 3);
  assert.equal(r.stderr.match(USING_LINE)?.length, 1, r.stderr);
  assert.match(r.stderr, /from main \([0-9a-f]{7}\), not this worktree's copy; set LANE_BROKER_CONFIG_FROM=worktree/);
});

test('origin/HEAD wins over a stale local main', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const old = path.join(f.base, 'old');
  gitFixture(['worktree', 'add', '-q', '-b', 'old', old], f.repoDir);
  gitFixture(['checkout', '-q', '-b', 'next'], f.repoDir);
  commitConfig(f.repoDir, withCpu(3));
  const next = gitFixture(['rev-parse', 'HEAD'], f.repoDir).trim();
  gitFixture(['checkout', '-q', 'main'], f.repoDir);
  gitFixture(['update-ref', 'refs/remotes/origin/main', next], f.repoDir);
  gitFixture(['symbolic-ref', 'refs/remotes/origin/HEAD', 'refs/remotes/origin/main'], f.repoDir);
  const r = await run(f, old);
  assert.equal(grantOf(r), 3, r.stderr);
  assert.match(r.stderr, /from origin\/HEAD \(/);
});

test('a branch that raises a lane has no effect until LANE_BROKER_CONFIG_FROM=worktree, which is recorded on the history row', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const feature = path.join(f.base, 'feature');
  gitFixture(['worktree', 'add', '-q', '-b', 'feature', feature], f.repoDir);
  commitConfig(feature, withCpu(6));
  const ignored = await run(f, feature);
  assert.equal(grantOf(ignored), 2, ignored.stderr);
  // no LANE_BROKER_NO_CACHE: the override is another config, so the default-branch pass above must not be reused
  const override = await run(f, feature, { [CONFIG_FROM_ENV]: 'worktree' });
  assert.equal(grantOf(override), 6, override.stderr);
  assert.doesNotMatch(override.stderr, USING_LINE);
  assert.equal(lastRow(f).configFrom, 'worktree-override');
  assert.equal(lastRow(f).configBlob, undefined);
});

test('the default-branch path records configFrom and the blob sha on the history row', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const r = await run(f, f.repoDir);
  assert.equal(r.code, 0, r.stderr);
  const blob = gitFixture(['rev-parse', 'main:.lane-broker.json'], f.repoDir).trim();
  assert.equal(lastRow(f).configFrom, 'default-branch');
  assert.equal(lastRow(f).configBlob, blob);
});

test('no git repo: the worktree file is used, with no stderr line and no configFrom', async () => {
  const f = repoFixture({ committed: withCpu(3), git: false });
  const r = await run(f, f.repoDir);
  assert.equal(r.code, 0, r.stderr);
  assert.doesNotMatch(r.stderr, USING_LINE);
  assert.equal(lastRow(f).configFrom, undefined);
});

test('a default branch with no .lane-broker.json leaves the worktree file in force', async () => {
  const f = repoFixture({ committed: withCpu(3), git: false });
  gitFixture(['init', '-q'], f.repoDir);
  gitFixture(['commit', '-q', '--allow-empty', '-m', 'x'], f.repoDir);
  const r = await run(f, f.repoDir);
  assert.equal(r.code, 0, r.stderr);
  assert.doesNotMatch(r.stderr, USING_LINE);
  assert.equal(lastRow(f).configFrom, undefined);
});

test('a worktree identical to main prints no stderr line', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const r = await run(f, f.repoDir);
  assert.doesNotMatch(r.stderr, USING_LINE);
});

test('an invalid LANE_BROKER_CONFIG_FROM exits 64 naming the variable', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const r = await run(f, f.repoDir, { [CONFIG_FROM_ENV]: 'bogus' });
  assert.equal(r.code, 64);
  assert.match(r.stderr, /LANE_BROKER_CONFIG_FROM="bogus"/);
});

test('repoConfigSource with a configRoot reads the snapshot even inside a git repo whose main differs', () => {
  const f = repoFixture({ committed: withCpu(2) });
  writeRepoConfig(f.repoDir, withCpu(9));
  const source = repoConfigSource(f.repoDir, f.repoDir, {});
  assert.equal(source.from, 'snapshot');
  assert.equal(JSON.parse(source.text).lanes.l.cpuCores, 9);
});

test('a merged config change is never answered from a pass cached under the old one, and a cached row records its config', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const old = path.join(f.base, 'old');
  gitFixture(['worktree', 'add', '-q', '-b', 'old', old], f.repoDir);
  assert.equal(grantOf(await run(f, old)), 2);
  const cached = await run(f, old);
  assert.match(cached.stderr, /cached pass from /);
  const blobBefore = gitFixture(['rev-parse', 'main:.lane-broker.json'], f.repoDir).trim();
  assert.equal(lastRow(f).configFrom, 'default-branch');
  assert.equal(lastRow(f).configBlob, blobBefore);
  commitConfig(f.repoDir, withCpu(3));
  const after = await run(f, old);
  assert.doesNotMatch(after.stderr, /cached pass from /);
  assert.equal(grantOf(after), 3, after.stderr);
});

test('an unreadable worktree copy does not block the default branch config', async () => {
  const f = repoFixture({ committed: withCpu(2) });
  const old = path.join(f.base, 'old');
  gitFixture(['worktree', 'add', '-q', '-b', 'old', old], f.repoDir);
  fs.rmSync(path.join(old, '.lane-broker.json'));
  fs.mkdirSync(path.join(old, '.lane-broker.json'));
  const r = await run(f, old);
  assert.equal(r.code, 0, r.stderr);
  assert.equal(grantOf(r), 2);
  assert.equal(r.stderr.match(USING_LINE)?.length, 1, r.stderr);
});
