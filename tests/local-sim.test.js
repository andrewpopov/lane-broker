import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';

function setup() {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 }, sim: { weight: 2, localRefused: true } } });
  return { repoDir, env };
}

test('a local sim lane is refused by default with exit 69', async () => {
  const { repoDir, env } = setup();
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'sim', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 69);
  assert.match(result.stderr, /fleet/i);
});

test('without ROUGE_FLEET_SUBMIT_DSN set, the refusal names the env var to set rather than an unexpanded template', async () => {
  const { repoDir, env } = setup();
  const { ROUGE_FLEET_SUBMIT_DSN, ...envWithoutDsn } = env;
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'sim', '--', 'true'], { env: envWithoutDsn, cwd: repoDir });
  assert.equal(result.code, 69);
  assert.doesNotMatch(result.stderr, /\$\{ROUGE_FLEET_SUBMIT_DSN\}/, 'must never print the raw unexpanded template');
  assert.match(result.stderr, /set ROUGE_FLEET_SUBMIT_DSN/);
});

test('with ROUGE_FLEET_SUBMIT_DSN set, the refusal names the variable but never prints its value (BRAIN-382)', async () => {
  const { repoDir, env } = setup();
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'sim', '--', 'true'], {
    env: { ...env, ROUGE_FLEET_SUBMIT_DSN: 'postgres://fleet_submit:planted-secret-7f3a@db.example.invalid:5432/rouge_sim' },
    cwd: repoDir,
  });
  assert.equal(result.code, 69);
  assert.match(result.stderr, /ROUGE_FLEET_SUBMIT_DSN/);
  assert.doesNotMatch(result.stderr, /planted-secret-7f3a/, 'the DSN password must never reach stderr');
  assert.doesNotMatch(result.stderr, /db\.example\.invalid/, 'no part of the DSN value is printed');
});

test('--allow-local-sim overrides the refusal and runs the command', async () => {
  const { repoDir, env } = setup();
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'sim', '--allow-local-sim', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 0);
});
