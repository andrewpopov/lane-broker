import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, gitFixture, waitFor } from './helpers.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig } from '../src/config.js';
import { decideReuse, isQualifyingRed, MAX_UNTRACKED_FILES, computeContentKey } from '../src/result-cache.js';

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
  return { base, repoDir, rows, runs, lane, settled, history, state };
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

test('a prior FAILED run is never served: miss (the refusal is off here; see the BRAIN-700 tests)', async () => {
  const t = setup({ global: { knownRedTtlSec: 0 } });
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

  const off = setup({ global: { resultCacheTtlSec: 0, knownRedTtlSec: 0 } });
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

test('decideReuse serves only the newest clean pass: signalled, cancelled, errored, expired, future and foreign rows never serve', () => {
  const now = 1_000_000;
  const ok = (over) => ({ contentKey: 'k', exit: 0, startedAt: now - 500, endedAt: now - 100, id: 'a', ...over });
  const pick = (rows) => {
    const d = decideReuse(rows, { contentKey: 'k', now, passTtlMs: 1000, redTtlMs: 1000 });
    return d.action === 'serve' ? d.row.id : null;
  };
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

test('BRAIN-558: a cached answer writes a non-empty --log naming the source run', async () => {
  const t = setup();
  const first = await t.lane();
  assert.equal(first.code, 0, first.stderr);
  await t.settled(1);
  const logFile = path.join(t.base, 'out', 'gate.log');
  const second = await t.lane(['--log', logFile]);
  assert.equal(second.code, 0, second.stderr);
  assert.equal(t.runs(), 1, 'the child must not have run again');
  const original = t.rows()[0];
  const written = fs.readFileSync(logFile, 'utf8');
  assert.ok(written.length > 0, 'the log must not be empty');
  assert.match(written, new RegExp(`lane: cached pass from ${original.id} at `));
});

// ---------------------------------------------------------------- BRAIN-700: known-red refusal

const NOW = 10_000_000;
const row = (over) => ({ id: 'r', contentKey: 'k', exit: 1, signal: null, startedAt: NOW - 500, endedAt: NOW - 100, executor: 'local', contentKeyVerified: true, ...over });
const decide = (rows, over = {}) => decideReuse(rows, { contentKey: 'k', now: NOW, passTtlMs: 1000, redTtlMs: 1000, ...over });
const actionOf = (rows, over) => decide(rows, over).action;

test('decideReuse resolver table: the newest real outcome decides', () => {
  const pass = (over) => row({ id: 'p', exit: 0, ...over });
  const red = (over) => row({ id: 'f', exit: 1, ...over });
  assert.equal(actionOf([pass({ endedAt: NOW - 300 }), red({ endedAt: NOW - 100 })]), 'refuse', 'pass then red refuses (the BRAIN-549 bug served the older pass)');
  assert.equal(actionOf([red({ endedAt: NOW - 300 }), pass({ endedAt: NOW - 100 })]), 'serve', 'red then pass serves');
  assert.equal(actionOf([pass({ endedAt: NOW - 400 }), red({ endedAt: NOW - 300 }), red({ id: 'i', exit: 75, endedAt: NOW - 100 })]), 'run', 'a newer infra row means run');
  assert.equal(actionOf([pass({ endedAt: NOW - 400 }), red({ endedAt: NOW - 300 }), red({ id: 'i', signal: 'SIGKILL', exit: 1, endedAt: NOW - 100 })]), 'run', 'a newer signalled row means run');
  assert.equal(decide([pass({ endedAt: NOW - 100 }), red({ endedAt: NOW - 100 })]).row.id, 'f', 'a tie goes to the later row in the file');
  assert.equal(decide([red({ endedAt: NOW - 100 }), pass({ endedAt: NOW - 100 })]).row.id, 'p');
  assert.equal(actionOf([red({ endedAt: NOW + 5 })]), 'run', 'a future row is ignored');
  assert.equal(actionOf([pass({ endedAt: NOW - 300 }), red({ endedAt: NOW + 5 })]), 'serve', 'a future red does not shadow an older pass');
  assert.equal(actionOf([red({ cachedFrom: 'z' })]), 'run');
  assert.equal(actionOf([red({ refusedFrom: 'z', exit: 79 })]), 'run');
  assert.equal(actionOf([pass({ endedAt: NOW - 300 }), red({ refusedFrom: 'z', endedAt: NOW - 100 })]), 'serve', 'a refusal row is not an outcome');
  assert.equal(actionOf([red({ startedAt: undefined })]), 'run');
  assert.equal(actionOf([red({ endedAt: NOW - 2000 })]), 'run', 'expired');
  assert.equal(actionOf([pass({ endedAt: NOW - 2000 })]), 'run', 'expired');
  const { contentKeyVerified, ...legacy } = red();
  assert.equal(contentKeyVerified, true);
  assert.equal(actionOf([legacy]), 'run', 'a legacy red without contentKeyVerified never refuses');
  assert.equal(actionOf([pass({ contentKeyVerified: undefined })]), 'serve', 'a legacy pass still serves');
  assert.equal(actionOf([red()], { redTtlMs: 0 }), 'run');
  assert.equal(actionOf([pass()], { passTtlMs: 0 }), 'run');
  assert.equal(actionOf([red()], { contentKey: 'other' }), 'run');
});

test('isQualifyingRed: only the command\'s own non-zero verdict qualifies', () => {
  assert.equal(isQualifyingRed(row()), true);
  assert.equal(isQualifyingRed(row({ exit: 2 })), true);
  for (const exit of [0, 124, 130, 69, 75, 76, 64, 79, 128, 137, 255, null, 'x']) assert.equal(isQualifyingRed(row({ exit })), false, `exit ${exit}`);
  assert.equal(isQualifyingRed(row({ signal: 'SIGTERM' })), false);
  assert.equal(isQualifyingRed(row({ cancelled: true })), false);
  assert.equal(isQualifyingRed(row({ error: 'boom' })), false);
  assert.equal(isQualifyingRed(row({ reason: 'no-progress' })), false);
  assert.equal(isQualifyingRed(row({ contentKeyVerified: undefined })), false);
  const remote = { executor: 'remote', remoteKind: 'completed', remotePhase: 'command' };
  assert.equal(isQualifyingRed(row(remote)), true);
  assert.equal(isQualifyingRed(row({ ...remote, remotePhase: 'setup' })), false, 'a setup-phase red is not a test failure');
  assert.equal(isQualifyingRed(row({ ...remote, remotePhase: 'deps' })), false);
  assert.equal(isQualifyingRed(row({ ...remote, remotePhase: null })), false);
  assert.equal(isQualifyingRed(row({ ...remote, remoteKind: 'refused' })), false);
  assert.equal(isQualifyingRed(row({ ...remote, remoteKind: undefined })), false);
  assert.equal(isQualifyingRed(row({ ...remote, remoteSignal: 'SIGKILL' })), false, 'a signalled remote child (reported as exit 1) is not a test failure');
});

const RED = 'echo x >> "$0"; test -e "$0.ok"';
const stateListing = (t) => ['leases', 'queue', 'queue-v2', 'results', 'waits'].map((d) => path.join(t.state, d)).map((d) => (fs.existsSync(d) ? fs.readdirSync(d).sort().join(',') : '-')).join('|');

test('a failed run on identical content is refused with exit 79, empty stdout, no ticket, lease or queue entry; --detach too', async () => {
  const t = setup();
  const first = await t.lane([], { script: RED });
  assert.equal(first.code, 1, first.stderr);
  await t.settled(1);
  const original = t.rows()[0];
  assert.equal(original.contentKeyVerified, true);
  assert.ok(original.logPath, 'the log path is recorded');
  const before = stateListing(t);
  for (const extra of [[], ['--detach'], ['--no-cache']]) {
    const again = await t.lane(extra, { script: RED });
    assert.equal(again.code, 79, again.stderr);
    assert.equal(again.stdout, '', `no stdout (${extra})`);
    assert.match(again.stderr, new RegExp(`lane: identical content failed in ${original.id} at \\d{4}-\\d\\d-\\d\\dT[\\d:.]+Z \\(exit 1, log ${original.logPath.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\); fix the cause, or pass --retry "<hypothesis>" to re-run`));
  }
  assert.equal(stateListing(t), before, 'a refusal creates no ticket, queue entry, lease or result');
  assert.equal(t.runs(), 1);
  const refusals = t.rows().filter((r) => r.refusedFrom);
  assert.equal(refusals.length, 3);
  assert.equal(refusals[0].exit, 79);
  assert.equal(refusals[0].refusedFrom, original.id);
  assert.equal(refusals[0].runMs, 0);
  assert.equal(refusals[0].contentKey, undefined, 'a refusal row never renews or shadows anything');
});

test('--retry runs, records retryReason (trimmed, capped); env LANE_BROKER_RETRY_REASON works and the flag wins; a blank flag is usage exit 2', async () => {
  const t = setup();
  await t.lane([], { script: RED });
  await t.settled(1);
  const retry = await t.lane(['--retry', '  deps reinstalled  '], { script: RED });
  assert.equal(retry.code, 1, retry.stderr);
  assert.equal(t.runs(), 2);
  await t.settled(2);
  assert.equal(t.rows()[1].retryReason, 'deps reinstalled');
  const viaEnv = await t.lane([], { script: RED, envExtra: { LANE_BROKER_RETRY_REASON: 'x'.repeat(300) } });
  assert.equal(viaEnv.code, 1, viaEnv.stderr);
  await t.settled(3);
  assert.equal(t.rows()[2].retryReason.length, 200);
  const flagWins = await t.lane(['--retry', 'flag reason'], { script: RED, envExtra: { LANE_BROKER_RETRY_REASON: 'env reason' } });
  assert.equal(flagWins.code, 1, flagWins.stderr);
  await t.settled(4);
  assert.equal(t.rows()[3].retryReason, 'flag reason');
  const blank = await t.lane(['--retry', '   '], { script: RED });
  assert.equal(blank.code, 2, blank.stderr);
  assert.equal(t.runs(), 4);
  const blankEnv = await t.lane([], { script: RED, envExtra: { LANE_BROKER_RETRY_REASON: '  ' } });
  assert.equal(blankEnv.code, 79, 'a blank env value is absent, not a retry');
});

test('--retry also bypasses serving a pass; --no-reuse bypasses both serving and refusal and records no retryReason', async () => {
  const pass = setup();
  await pass.lane();
  await pass.settled(1);
  assert.equal((await pass.lane(['--retry', 'again'])).code, 0);
  assert.equal(pass.runs(), 2, 'retry runs even over a pass');
  await pass.settled(2);
  assert.equal((await pass.lane(['--no-reuse'])).code, 0);
  assert.equal(pass.runs(), 3);
  await pass.settled(3);
  assert.equal(pass.rows()[2].retryReason, undefined);

  const red = setup();
  await red.lane([], { script: RED });
  await red.settled(1);
  const again = await red.lane(['--no-reuse'], { script: RED });
  assert.equal(again.code, 1, again.stderr);
  assert.equal(red.runs(), 2);
  await red.settled(2);
  assert.equal(red.rows()[1].retryReason, undefined);
  assert.equal(red.rows()[1].refusedFrom, undefined);
});

test('after a retry fixes the failure, the newer pass is served again', async () => {
  const t = setup();
  await t.lane([], { script: RED });
  await t.settled(1);
  fs.writeFileSync(`${path.join(t.base, 'counter')}.ok`, '');
  assert.equal((await t.lane(['--retry', 'flag file set'], { script: RED })).code, 0);
  await t.settled(2);
  const third = await t.lane([], { script: RED });
  assert.equal(third.code, 0, third.stderr);
  assert.match(third.stderr, /cached pass from/);
  assert.equal(t.runs(), 2);
});

test('--no-cache and LANE_BROKER_NO_CACHE=1 do not bypass the refusal', async () => {
  const t = setup();
  await t.lane([], { script: RED });
  await t.settled(1);
  assert.equal((await t.lane(['--no-cache'], { script: RED })).code, 79);
  assert.equal((await t.lane([], { script: RED, envExtra: { LANE_BROKER_NO_CACHE: '1' } })).code, 79);
  assert.equal(t.runs(), 1);
});

test('a one-byte edit or a different command runs instead of being refused', async () => {
  const t = setup();
  await t.lane([], { script: RED });
  await t.settled(1);
  fs.writeFileSync(path.join(t.repoDir, 'tracked.txt'), 'onf\n');
  assert.equal((await t.lane([], { script: RED })).code, 1);
  assert.equal(t.runs(), 2);
  const other = await t.lane([], { script: `${RED}; true && false` });
  assert.equal(other.code, 1);
  assert.equal(t.runs(), 3, 'a different command is a different key');
});

test('knownRedTtlSec and resultCacheTtlSec apply separately; both 0 records no key', async () => {
  const redOnly = setup({ global: { resultCacheTtlSec: 0, knownRedTtlSec: 3600 } });
  await redOnly.lane();
  await redOnly.settled(1);
  assert.ok(redOnly.rows()[0].contentKey, 'a key is recorded while either TTL is on');
  assert.equal((await redOnly.lane()).code, 0, 'a pass is not served with resultCacheTtlSec 0');
  assert.equal(redOnly.runs(), 2);
  const redOnlyFail = setup({ global: { resultCacheTtlSec: 0, knownRedTtlSec: 3600 } });
  await redOnlyFail.lane([], { script: RED });
  await redOnlyFail.settled(1);
  assert.equal((await redOnlyFail.lane([], { script: RED })).code, 79);

  const passOnly = setup({ global: { knownRedTtlSec: 0 } });
  await passOnly.lane([], { script: RED });
  await passOnly.settled(1);
  assert.equal((await passOnly.lane([], { script: RED })).code, 1, 'no refusal with knownRedTtlSec 0');
  assert.equal(passOnly.runs(), 2);
  const passServed = setup({ global: { knownRedTtlSec: 0 } });
  await passServed.lane();
  await passServed.settled(1);
  assert.match((await passServed.lane()).stderr, /cached pass/);

  const off = setup({ global: { resultCacheTtlSec: 0, knownRedTtlSec: 0 } });
  await off.lane([], { script: RED });
  await off.settled(1);
  assert.equal(off.rows()[0].contentKey, undefined);
  assert.equal((await off.lane([], { script: RED })).code, 1);
});

test('a failing run whose tree is edited while it runs gets no key, so it is never refused', async () => {
  const t = setup();
  const mutating = 'echo x >> "$0"; echo more >> tracked.txt; exit 1';
  await t.lane([], { script: mutating });
  await t.settled(1);
  assert.equal(t.rows()[0].contentKeyVerified, undefined);
  assert.equal(t.rows()[0].contentKey, undefined, 'the failure saw other content than the key describes');
  assert.equal((await t.lane([], { script: mutating })).code, 1);
  assert.equal(t.runs(), 2);
});

test('a refusal is not an outcome: a later identical run still refuses against the original failure', async () => {
  const t = setup();
  await t.lane([], { script: RED });
  await t.settled(1);
  const original = t.rows()[0];
  await t.lane([], { script: RED });
  const second = await t.lane([], { script: RED });
  assert.match(second.stderr, new RegExp(`failed in ${original.id} `));
});

test('knownRedTtlSec defaults to 6 h and must be a non-negative integer', () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.knownRedTtlSec, 21_600);
  const { home } = freshEnv();
  const prevHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    writeGlobalConfig(home, { ...GLOBAL, knownRedTtlSec: 0 });
    assert.equal(loadGlobalConfig().knownRedTtlSec, 0);
    for (const bad of [-1, 1.5, '60']) {
      writeGlobalConfig(home, { ...GLOBAL, knownRedTtlSec: bad });
      assert.throws(() => loadGlobalConfig(), /"knownRedTtlSec" must be a non-negative integer/);
    }
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
  }
});
