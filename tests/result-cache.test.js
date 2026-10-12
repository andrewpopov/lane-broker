import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, gitFixture, waitFor } from './helpers.js';
import { findCachedPass, MAX_UNTRACKED_FILES, computeContentKey } from '../src/result-cache.js';

/**
 * BRAIN-549. Each test runs a command whose only side effect is appending a line to a counter file OUTSIDE the worktree
 * (a counter inside it would change the content key), so "ran" is observable as a counter line, independent of the
 * cache's own output.
 */
const GLOBAL = { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 };

function setup({ lanes = { default: { weight: 1 } }, global = {} } = {}) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { ...GLOBAL, ...global });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes });
  fs.writeFileSync(path.join(repoDir, 'tracked.txt'), 'one\n');
  gitFixture(['init', '-q'], repoDir);
  gitFixture(['add', '-A'], repoDir);
  gitFixture(['commit', '-q', '-m', 'init'], repoDir);
  const counter = path.join(base, 'counter');
  const history = path.join(state, 'history.jsonl');
  const rows = () => (fs.existsSync(history) ? fs.readFileSync(history, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)) : []);
  const runs = () => (fs.existsSync(counter) ? fs.readFileSync(counter, 'utf8').split('\n').filter(Boolean).length : 0);
  const lane = (extra = [], { script = 'echo x >> "$0"', laneName = 'default', envExtra = {} } = {}) =>
    laneRun(['run', '--repo', 'r', '--lane', laneName, ...extra, '--', 'sh', '-c', script, counter], { env: { ...env, ...envExtra }, cwd: repoDir });
  // the supervisor writes the result before the history row, so a back-to-back run must wait for the row to exist
  const settled = (n) => waitFor(() => rows().filter((r) => !r.cachedFrom).length >= n);
  return { base, repoDir, rows, runs, lane, settled, history };
}

test('the same command on a clean identical tree after a pass is answered from cache, with no child run', async () => {
  const t = setup();
  const first = await t.lane();
  assert.equal(first.code, 0, first.stderr);
  assert.equal(t.runs(), 1);
  await t.settled(1);
  const second = await t.lane();
  assert.equal(second.code, 0, second.stderr);
  assert.equal(t.runs(), 1, 'the child must not have run again');
  const original = t.rows()[0];
  assert.match(second.stderr, new RegExp(`lane: cached pass from ${original.id} at \\d{4}-\\d\\d-\\d\\dT[\\d:.]+Z \\(pass --no-cache to re-run\\)`));
  const cachedRow = t.rows()[1];
  assert.equal(cachedRow.cachedFrom, original.id);
  assert.equal(cachedRow.runMs, 0);
  assert.equal(cachedRow.exit, 0);
  assert.ok(original.contentKey, 'a real run stores its key');
  assert.equal(cachedRow.contentKey, undefined, 'a cached row never renews the pass it points to');
});

test('an uncommitted edit changes the key: miss', async () => {
  const t = setup();
  await t.lane();
  await t.settled(1);
  fs.writeFileSync(path.join(t.repoDir, 'tracked.txt'), 'two\n');
  const second = await t.lane();
  assert.equal(second.code, 0, second.stderr);
  assert.doesNotMatch(second.stderr, /cached pass/);
  assert.equal(t.runs(), 2);
});

test('a staged edit and a mode change are misses too, and restoring the content is a hit again', async () => {
  const t = setup();
  await t.lane();
  await t.settled(1);
  fs.writeFileSync(path.join(t.repoDir, 'tracked.txt'), 'two\n');
  gitFixture(['add', 'tracked.txt'], t.repoDir);
  await t.lane();
  assert.equal(t.runs(), 2, 'staged edit');
  await t.settled(2);
  gitFixture(['reset', '-q', '--hard'], t.repoDir);
  const back = await t.lane();
  assert.match(back.stderr, /cached pass/, 'identical content to the first run is a hit');
  assert.equal(t.runs(), 2);
  fs.chmodSync(path.join(t.repoDir, 'tracked.txt'), 0o755);
  await t.lane();
  assert.equal(t.runs(), 3, 'exec-bit change');
});

test('a new untracked file changes the key: miss; an edit to it misses again', async () => {
  const t = setup();
  await t.lane();
  await t.settled(1);
  fs.writeFileSync(path.join(t.repoDir, 'new.txt'), 'a\n');
  const second = await t.lane();
  assert.doesNotMatch(second.stderr, /cached pass/);
  assert.equal(t.runs(), 2);
  await t.settled(2);
  fs.writeFileSync(path.join(t.repoDir, 'new.txt'), 'b\n');
  await t.lane();
  assert.equal(t.runs(), 3, 'same path, new content');
});

test('an ignored file is not part of the key', async () => {
  const t = setup();
  fs.writeFileSync(path.join(t.repoDir, '.gitignore'), 'ignored.txt\n');
  gitFixture(['add', '-A'], t.repoDir);
  gitFixture(['commit', '-q', '-m', 'ignore'], t.repoDir);
  await t.lane();
  await t.settled(1);
  fs.writeFileSync(path.join(t.repoDir, 'ignored.txt'), 'noise\n');
  const second = await t.lane();
  assert.match(second.stderr, /cached pass/);
  assert.equal(t.runs(), 1);
});

test('a prior FAILED run is never served: miss', async () => {
  const t = setup();
  const fail = 'echo x >> "$0"; exit 3';
  assert.equal((await t.lane([], { script: fail })).code, 3);
  await t.settled(1);
  const second = await t.lane([], { script: fail });
  assert.equal(second.code, 3);
  assert.doesNotMatch(second.stderr, /cached pass/);
  assert.equal(t.runs(), 2);
});

test('a different command on the same tree is a miss', async () => {
  const t = setup();
  await t.lane();
  await t.settled(1);
  await t.lane([], { script: 'echo y >> "$0"' });
  assert.equal(t.runs(), 2);
});

test('--no-cache runs, and so does LANE_BROKER_NO_CACHE=1', async () => {
  const t = setup();
  await t.lane();
  await t.settled(1);
  const flagged = await t.lane(['--no-cache']);
  assert.equal(flagged.code, 0, flagged.stderr);
  assert.doesNotMatch(flagged.stderr, /cached pass/);
  assert.equal(t.runs(), 2);
  await t.settled(2);
  const viaEnv = await t.lane([], { envExtra: { LANE_BROKER_NO_CACHE: '1' } });
  assert.doesNotMatch(viaEnv.stderr, /cached pass/);
  assert.equal(t.runs(), 3);
  await t.settled(3);
  const plain = await t.lane();
  assert.match(plain.stderr, /cached pass/, 'opting out once does not poison the cache');
  assert.equal(t.runs(), 3);
});

test('a lane with cache: false always runs', async () => {
  const t = setup({ lanes: { default: { weight: 1 }, fresh: { weight: 1, cache: false } } });
  await t.lane([], { laneName: 'fresh' });
  await t.settled(1);
  const second = await t.lane([], { laneName: 'fresh' });
  assert.doesNotMatch(second.stderr, /cached pass/);
  assert.equal(t.runs(), 2);
  assert.equal(t.rows()[0].contentKey, undefined, 'an opted-out lane records no key');
});

test('an exclusive lane never caches', async () => {
  const t = setup({ lanes: { default: { weight: 1 }, solo: { weight: 1, exclusive: true } } });
  await t.lane([], { laneName: 'solo' });
  await t.settled(1);
  const second = await t.lane([], { laneName: 'solo' });
  assert.doesNotMatch(second.stderr, /cached pass/);
  assert.equal(t.runs(), 2);
});

test('a pass older than resultCacheTtlSec is not served; resultCacheTtlSec 0 turns the cache off', async () => {
  const t = setup();
  await t.lane();
  await t.settled(1);
  // age the only real row past the 24 h default TTL
  const aged = t.rows().map((r) => ({ ...r, endedAt: r.endedAt - 25 * 3600 * 1000 }));
  fs.writeFileSync(t.history, `${aged.map((r) => JSON.stringify(r)).join('\n')}\n`);
  const second = await t.lane();
  assert.doesNotMatch(second.stderr, /cached pass/);
  assert.equal(t.runs(), 2);

  const off = setup({ global: { resultCacheTtlSec: 0 } });
  await off.lane();
  await new Promise((r) => setTimeout(r, 500));
  await off.lane();
  assert.equal(off.runs(), 2);
  assert.equal(off.rows()[0].contentKey, undefined);
});

test('a run that changes the worktree while it runs is not cached', async () => {
  const t = setup();
  const mutating = 'echo x >> "$0"; echo more >> tracked.txt';
  await t.lane([], { script: mutating });
  await t.settled(1);
  assert.equal(t.rows()[0].contentKey, undefined, 'the pass saw other content than the key describes');
  await t.lane([], { script: mutating });
  assert.equal(t.runs(), 2);
});

test('findCachedPass refuses signalled, cancelled, errored and expired rows and picks the newest clean pass', () => {
  const now = 1_000_000;
  const ok = (over) => ({ contentKey: 'k', exit: 0, startedAt: now - 500, endedAt: now - 100, id: 'a', ...over });
  const pick = (rows) => findCachedPass(rows, { contentKey: 'k', now, ttlMs: 1000 })?.id ?? null;
  assert.equal(pick([ok({ id: 'a' }), ok({ id: 'b', endedAt: now - 50 })]), 'b');
  assert.equal(pick([ok({ signal: 'SIGTERM' })]), null);
  assert.equal(pick([ok({ cancelled: true })]), null);
  assert.equal(pick([ok({ error: 'x' })]), null);
  assert.equal(pick([ok({ exit: 1 })]), null);
  assert.equal(pick([ok({ cachedFrom: 'z' })]), null);
  assert.equal(pick([ok({ endedAt: now - 2000 })]), null);
  assert.equal(pick([ok({ endedAt: now + 10 })]), null, 'a row from the future is not trusted');
  assert.equal(pick([ok({ contentKey: 'other' })]), null);
});

test('computeContentKey gives no key (never caches) past the untracked-file bound, with a submodule file, or outside git', () => {
  const t = setup();
  const args = { cwd: t.repoDir, key: 'r:default', cmdFingerprint: 'fp' };
  const key = computeContentKey(args);
  assert.match(key, /^[0-9a-f]{64}$/);
  assert.equal(computeContentKey(args), key, 'deterministic');
  assert.notEqual(computeContentKey({ ...args, key: 'r:other' }), key, 'the lane is part of the key');
  assert.notEqual(computeContentKey({ ...args, cmdFingerprint: 'fp2' }), key, 'the command is part of the key');
  assert.equal(computeContentKey({ ...args, cmdFingerprint: undefined }), null);
  assert.equal(computeContentKey({ ...args, cwd: t.base }), null, 'outside git');

  fs.writeFileSync(path.join(t.repoDir, '.gitmodules'), '');
  assert.equal(computeContentKey(args), null, 'submodules are not hashed');
  fs.rmSync(path.join(t.repoDir, '.gitmodules'));
  assert.equal(computeContentKey(args), key);

  gitFixture(['update-index', '--assume-unchanged', 'tracked.txt'], t.repoDir);
  assert.equal(computeContentKey(args), null, 'git diff cannot see an assume-unchanged edit');
  gitFixture(['update-index', '--no-assume-unchanged', 'tracked.txt'], t.repoDir);

  const many = path.join(t.repoDir, 'many');
  fs.mkdirSync(many);
  for (let i = 0; i <= MAX_UNTRACKED_FILES; i += 1) fs.writeFileSync(path.join(many, `f${i}`), '');
  assert.equal(computeContentKey(args), null, 'too many untracked files');
});
