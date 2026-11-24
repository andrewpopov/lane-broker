import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { setup, tmpDir, detachAndWait } from './remote-harness.js';
import { reapSpawned, setupRunner, makeHeader, execRemote } from './helpers/remote-exec-fixture.js';

// PKG-403: the grant reaches the command on every transport, and this repo's test runner sizes its file concurrency from it.

afterEach(reapSpawned);

const MiB = 1024 * 1024;
const REPO = fileURLToPath(new URL('..', import.meta.url));
const TEST_RUN = path.join(REPO, 'scripts', 'test-run.sh');
const GRANT_PROBE = (file) => `require('fs').writeFileSync(${JSON.stringify(file)}, JSON.stringify([process.env.LANE_BROKER_CPU_CORES, process.env.LANE_BROKER_MEMORY_BYTES, process.env.LANE_FAKE_RUNNER === '1']))`;
const readGrant = (file) => JSON.parse(fs.readFileSync(file, 'utf8'));

test('local: the command sees the CPU grant and the lane memory claim', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, memoryBytes: 64 * MiB } } });
  const seen = path.join(tmpDir('grant-local'), 'grant.json');
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--cpu', '2', '--', 'node', '-e', GRANT_PROBE(seen)], { env, cwd: repoDir });
  assert.equal(res.code, 0, res.stderr);
  assert.deepEqual(readGrant(seen), ['2', String(64 * MiB), false]);
});

test('push: the command on the runner sees the grant and the memory claim', { timeout: 180_000 }, async () => {
  const { env, repoDir } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 1, memoryBytes: 64 * MiB, remote: true, remotePolicy: 'remote-first' } } });
  const seen = path.join(tmpDir('grant-push'), 'grant.json');
  const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'node', '-e', GRANT_PROBE(seen)], env, repoDir);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(readGrant(seen), ['1', String(64 * MiB), true]);
});

test('pull: remote-exec runs the command at the core count the queue claimed, with the memory claim', async () => {
  const ctx = setupRunner();
  const seen = path.join(tmpDir('grant-pull'), 'grant.json');
  const h = makeHeader(GRANT_PROBE(seen), { cpuCores: 3, minCpuCores: 1, memoryBytes: 64 * MiB });
  const closed = await execRemote(ctx, h, { extraArgs: ['--epoch', '1', '--claim-cpu-cores', '2'] }).closed;
  assert.equal(closed.code, 0, closed.stderr);
  assert.deepEqual(readGrant(seen).slice(0, 2), ['2', String(64 * MiB)]);
});

/** `node --test` invocations `script` makes for one test file under `env`, with every other `node` call passed through. */
function testRunInvocations(script, env) {
  const bin = tmpDir('fake-node-bin');
  const record = path.join(bin, 'calls');
  fs.writeFileSync(path.join(bin, 'node'), `#!/bin/sh\nif [ "$1" = "--test" ]; then printf '%s\\n' "$*" >> "${record}"; exit 0; fi\nexec "${process.execPath}" "$@"\n`, { mode: 0o755 });
  const res = spawnSync('sh', [script, 'tests/capabilities.test.js'], { cwd: REPO, encoding: 'utf8', env: { PATH: `${bin}${path.delimiter}${process.env.PATH}`, ...env } });
  assert.equal(res.status, 0, res.stderr);
  return fs.readFileSync(record, 'utf8').trim().split('\n');
}

const concurrencyOf = (script, env) => testRunInvocations(script, env).map((call) => /--test-concurrency=(\d+)/.exec(call)?.[1] ?? null);

const concurrencyScenario = (script) => {
  assert.deepEqual(concurrencyOf(script, { LANE_BROKER_CPU_CORES: '1' }), ['1'], 'a 1-core claim runs one file at a time');
  assert.deepEqual(concurrencyOf(script, { LANE_BROKER_CPU_CORES: '3' }), ['3']);
  assert.deepEqual(concurrencyOf(script, { LANE_BROKER_CPU_CORES: '2.5' }), ['2'], 'a fractional grant rounds down');
  assert.deepEqual(concurrencyOf(script, { LANE_BROKER_CPU_CORES: '16' }), ['4'], 'at most 4');
  assert.deepEqual(concurrencyOf(script, {}), ['2'], 'outside a lane: 2, as before');
  assert.deepEqual(concurrencyOf(script, { LANE_BROKER_CPU_CORES: '1', LANE_BROKER_TEST_CONCURRENCY: '3' }), ['3'], 'the explicit override wins');
};

test('scripts/test-run.sh (what test-shard.sh and the train gate run) passes --test-concurrency from the grant', () => concurrencyScenario(TEST_RUN));

test('canary: with test-run.sh ignoring the grant, the concurrency scenario fails by name', () => {
  const source = fs.readFileSync(TEST_RUN, 'utf8');
  const guard = '"${LANE_BROKER_CPU_CORES:-}"';
  assert.equal(source.split(guard).length, 2, 'the mutation target occurs once');
  const mutant = path.join(tmpDir('test-run-mutant'), 'test-run.sh');
  fs.writeFileSync(mutant, source.replace(guard, '""'));
  assert.throws(() => concurrencyScenario(mutant), (err) => err.code === 'ERR_ASSERTION' && /a 1-core claim runs one file at a time/.test(err.message));
});
