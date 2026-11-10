import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { sleep, writeRepoConfig, writeGlobalConfig } from './helpers.js';
import {
  setup, tmpDir, markerCmd, detachAndWait, resultOf,
  makeFakeSshBin, makeRunner, clientEnv, makeGitWorktree, makeDispatchArgs, readCapturedHeader,
} from './remote-harness.js';
import { dispatchRemote } from '../src/remote-client.js';
import { remoteSelectOptions } from '../src/supervisor.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { DEPS_KEY_FILES_CAPABILITY, DEPS_KEY_FILES_COVER_CAPABILITY } from '../src/capabilities.js';

/**
 * BRAIN-926: `remoteDepsCacheKeyFilesCoverInstallScripts` -- the lane's assertion that its key files are every repo input the root
 * AND dependency install scripts read. The key itself: tests/deps-cache.test.js's `key-files-cover` tests; here, the wiring.
 */

const SETUP = [[process.execPath, '-e', 'process.exit(0)']];
const KEYED = { remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles: ['gen-input.txt'] };

async function teeDispatch(extra) {
  const fake = makeFakeSshBin();
  writeGlobalConfig(fake.runnerHome, { sampleMs: 50, capacity: 4, cpuLimit: 'off' });
  const { env } = clientEnv(fake.binDir);
  const result = await dispatchRemote({
    ...makeDispatchArgs({ remoteSetup: SETUP }),
    runner: makeRunner({ ssh: 'tee', name: 'skybox', root: tmpDir('cover-runner-root') }),
    worktreeRoot: makeGitWorktree({ 'gen-input.txt': 'one' }),
    sshBin: fake.sshBin,
    env,
    sleep: (ms) => sleep(Math.min(ms, 50)),
    deadlines: { resultMs: 5000, resultAttempts: 3 },
    ...extra,
  });
  return { result, header: readCapturedHeader(fake.teePath) };
}

test('BRAIN-926: the header carries the covered-key assertion only with the safe flag, key files and a supporting runner', async () => {
  const covered = await teeDispatch({ ...KEYED, remoteDepsCacheKeyFilesCoverInstallScripts: true });
  assert.equal(covered.result.outcome, 'confirmed', JSON.stringify(covered.result));
  assert.equal(covered.header.protocol, 2);
  assert.deepEqual(covered.header.remoteDepsCacheKeyFiles, ['gen-input.txt']);
  assert.equal(covered.header.remoteDepsCacheKeyFilesCoverInstallScripts, true);

  const silent = {
    'a runner without deps-key-files/2': KEYED,
    'the flag off': { ...KEYED, remoteDepsCacheKeyFilesCoverInstallScripts: false },
    'no key files': { remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFilesCoverInstallScripts: true },
    'no safe flag': { remoteDepsCacheKeyFiles: ['gen-input.txt'], remoteDepsCacheKeyFilesCoverInstallScripts: true },
  };
  for (const [name, extra] of Object.entries(silent)) {
    const { header } = await teeDispatch(extra);
    assert.equal('remoteDepsCacheKeyFilesCoverInstallScripts' in header, false, name);
  }
});

test('BRAIN-926: the supervisor gates the assertion on the chosen runner probe, end to end through `lane run`', async () => {
  for (const unsupported of [false, true]) {
    const s = setup({ ssh: 'tee' });
    writeRepoConfig(s.repoDir, { version: 1, lanes: { default: {
      weight: 1, remote: true, remotePolicy: 'remote-first', remoteSetup: SETUP,
      ...KEYED, remoteDepsCacheKeyFilesCoverInstallScripts: true,
    } } });
    const marker = path.join(tmpDir('marker'), 'where');
    const env = unsupported ? { ...s.env, LANE_FAKE_CAPS_DROP: DEPS_KEY_FILES_COVER_CAPABILITY } : s.env;
    const { id, waited } = await detachAndWait(
      ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
      env, s.repoDir,
    );
    assert.equal(waited.code, 0, waited.stderr);
    assert.equal(resultOf(s.state, id).executor, 'remote');
    assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
    const header = readCapturedHeader(s.teePath);
    assert.deepEqual(header.remoteDepsCacheKeyFiles, ['gen-input.txt'], 'key files ride either way');
    assert.equal(header.remoteDepsCacheKeyFilesCoverInstallScripts, unsupported ? undefined : true);
    assert.equal('remoteDepsCacheKeyFilesCoverInstallScripts' in header, !unsupported);
  }
});

test('BRAIN-926: the covered flag adds no placement requirement beyond deps-key-files/1', () => {
  const enriched = (remote) => ({ weight: 1, resources: { cpuCores: 1, memoryBytes: 1073741824 }, remote });
  const covered = remoteSelectOptions(enriched({ remoteDeps: ['.'], ...KEYED, remoteDepsCacheKeyFilesCoverInstallScripts: true }), DEFAULT_GLOBAL_CONFIG);
  assert.deepEqual(covered.requireCapabilities, [DEPS_KEY_FILES_CAPABILITY]);
  assert.equal(covered.requireCapabilities.includes(DEPS_KEY_FILES_COVER_CAPABILITY), false);
  const plain = remoteSelectOptions(enriched({ remoteDeps: ['.'] }), DEFAULT_GLOBAL_CONFIG);
  assert.deepEqual(plain.requireCapabilities, []);
});
