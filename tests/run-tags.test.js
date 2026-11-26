import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { runTagFromFlag, noOnlineRunnerMessage, checkRunnable, RUNNER_ONLINE_WITHIN_S } from '../src/run-tags.js';
import { effectiveRequiresTags, hostTag, isValidRunTag } from '../src/runner-readiness.js';
import { computeContentKey } from '../src/result-cache.js';
import { loadGlobalConfig, ConfigError } from '../src/config.js';
import { freshEnv, gitFixture, laneRun, writeGlobalConfig, writeRepoConfig } from './helpers.js';

/** PKG-468: `lane run --tag` / `--runner`, the named no-runner refusal, and the per-run tags in a run's identity. */

const PULL_CFG = {
  lanePull: { enabled: true, passwordFile: '/p', caFile: '/c', rustfs: { endpoint: 'e', credentialsEnvFile: '/e' } },
  laneQueueUrl: 'postgres://u@h/db',
  runners: [{ name: 'pullbox', ssh: 'x', transport: 'pull' }],
};
const servicesAnswering = (answer, { count } = {}) => () => ({
  available: true,
  queue: {
    placeableDetail: async () => (answer instanceof Error ? Promise.reject(answer) : answer),
    placeable: async () => (count instanceof Error ? Promise.reject(count) : count),
  },
});
const checkWith = (answer, tags = [hostTag('evo-x2')], cfg = PULL_CFG, opts) => checkRunnable({ cfg, root: '/r', repoKey: 'rouge', tags, servicesFor: servicesAnswering(answer, opts) });

test('--runner is shorthand for --tag host:<name>; --tag takes plain and host: tags and refuses a malformed one', () => {
  assert.equal(runTagFromFlag('--runner', 'evo-x2'), 'host:evo-x2');
  assert.equal(runTagFromFlag('--tag', 'host:evo-x2'), runTagFromFlag('--runner', 'evo-x2'));
  assert.equal(runTagFromFlag('--tag', 'browser'), 'browser');
  for (const bad of ['', undefined, 'a b', 'host:', 'a/b', '--lane']) assert.throws(() => runTagFromFlag('--tag', bad), /--tag needs a tag name/);
  for (const bad of ['', undefined, 'Evo X2', 'host:evo']) assert.throws(() => runTagFromFlag('--runner', bad), /--runner needs a runner name/);
  assert.equal(isValidRunTag('host:evo-x2'), true);
});

test('per-run tags add to the lane tags, deduplicated and sorted', () => {
  assert.deepEqual(effectiveRequiresTags(['browser'], ['host:evo-x2', 'browser']), ['browser', 'host:evo-x2']);
  assert.deepEqual(effectiveRequiresTags(undefined, []), []);
});

test('no online runner has the tags: no runner at all, or every candidate offline, is a named refusal; one online runner is none', () => {
  const tags = ['host:evo-x2'];
  assert.match(noOnlineRunnerMessage({ eligible: 0, runners: [] }, tags), /^waiting: no runner has \[host:evo-x2\]/);
  assert.equal(noOnlineRunnerMessage({ eligible: 1, runners: [{ host: 'evo-x2', seenAgoS: RUNNER_ONLINE_WITHIN_S }] }, tags), null);
  assert.match(noOnlineRunnerMessage({ eligible: 1, runners: [{ host: 'evo-x2', seenAgoS: 10800 }] }, tags), /^waiting: no online runner has \[host:evo-x2\]; offline: evo-x2 \(last seen 3h ago\)$/);
  assert.match(noOnlineRunnerMessage({ eligible: 1, runners: [{ host: 'evo-x2', seenAgoS: RUNNER_ONLINE_WITHIN_S + 1 }] }, tags), /no online runner/);
  assert.match(noOnlineRunnerMessage({ eligible: 1, runners: [{ host: 'evo-x2', seenAgoS: null }] }, tags), /never registered/);
});

test('checkRunnable refuses from the queue answer, and never blocks a run when the queue cannot be asked', async () => {
  const offline = { eligible: 1, runners: [{ host: 'evo-x2', seenAgoS: 9999 }] };
  const refused = await checkWith(offline);
  assert.equal(refused.ok, false);
  assert.match(refused.message, /no online runner has \[host:evo-x2\]/);
  assert.deepEqual(await checkWith({ eligible: 1, runners: [{ host: 'evo-x2', seenAgoS: 3 }] }), { ok: true });
  const unmigrated = new Error('function ld_placeable_detail does not exist');
  assert.deepEqual(await checkWith(unmigrated, undefined, undefined, { count: 1 }), { ok: true }, 'a queue without the function but with a claimer');
  assert.match((await checkWith(unmigrated, undefined, undefined, { count: 0 })).message, /^waiting: no runner has \[host:evo-x2\]/, 'a queue without the function and no claimer');
  assert.deepEqual(await checkWith(unmigrated, undefined, undefined, { count: new Error('down') }), { ok: true }, 'an unreachable queue');
  assert.deepEqual(await checkWith(offline, [hostTag('x')], { ...PULL_CFG, lanePull: { enabled: false } }), { ok: true }, 'pull not configured');
});

test('the same content aimed at another machine is not the same run: required tags are part of the content key, none leaves it unchanged', () => {
  const { base } = freshEnv();
  const repo = path.join(base, 'repo');
  fs.mkdirSync(repo);
  fs.writeFileSync(path.join(repo, 'a.txt'), 'one\n');
  gitFixture(['init', '-q'], repo);
  gitFixture(['add', '-A'], repo);
  gitFixture(['commit', '-q', '-m', 'init'], repo);
  const args = { cwd: repo, key: 'r:default', cmdFingerprint: 'fp' };
  const plain = computeContentKey(args);
  assert.equal(computeContentKey({ ...args, requiredTags: [] }), plain, 'no per-run tags: the key an older broker computed');
  const evo = computeContentKey({ ...args, requiredTags: ['host:evo-x2'] });
  const mac = computeContentKey({ ...args, requiredTags: ['host:mac'] });
  assert.equal(new Set([plain, evo, mac]).size, 3);
  assert.equal(computeContentKey({ ...args, requiredTags: ['host:evo-x2'] }), evo, 'deterministic');
});

test('runnerTags may not claim the reserved host: prefix', () => {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, runnerTags: ['host:evo-x2'] });
  assert.throws(() => loadGlobalConfig({ home }), (err) => err instanceof ConfigError && /runnerTags/.test(err.message));
});

test('lane run --runner on a lane that runs here is refused (exit 64), and a bad --runner is a usage error', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  gitFixture(['init', '-q'], repoDir);
  gitFixture(['add', '-A'], repoDir);
  gitFixture(['commit', '-q', '-m', 'init'], repoDir);
  const pinned = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--runner', 'evo-x2', '--', 'true'], { env, cwd: repoDir });
  assert.equal(pinned.code, 64, pinned.stderr);
  assert.match(pinned.stderr, /pins a run to a runner, but this run is not remote-eligible/);
  const bad = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--runner', 'Evo', '--', 'true'], { env, cwd: repoDir });
  assert.equal(bad.code, 2, bad.stderr);
  assert.match(bad.stderr, /--runner needs a runner name/);
});
