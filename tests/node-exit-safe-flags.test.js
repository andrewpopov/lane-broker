import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, waitFor } from './helpers.js';
import { readLease } from '../src/lease.js';
import { exitSafeFlagsFor, NODE_EXIT_SAFE_FLAGS } from '../src/node-exit-safe.js';

// BRAIN-577: Node 24 can hang in process.exit() under concurrent Maglev/Sparkplug (nodejs/node#64274).

test('the supervisor is spawned with --no-maglev and --no-concurrent-sparkplug', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  // Neutralise any operator override in the ambient environment so the default path is what runs.
  delete env.NODE_OPTIONS;

  const detached = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'sleep', '30'], { env, cwd: repoDir });
  assert.equal(detached.code, 0, detached.stderr);
  const id = detached.stderr.match(/detached (\S+?);/)?.[1];
  assert.ok(id, `no id in: ${detached.stderr}`);
  const lease = await waitFor(() => {
    const l = readLease(state, id);
    return l && l.childPgid ? l : null;
  });
  try {
    const command = execFileSync('ps', ['-o', 'command=', '-p', String(lease.supervisorPid)], { encoding: 'utf8' });
    for (const flag of NODE_EXIT_SAFE_FLAGS) assert.ok(command.includes(flag), `supervisor command lacks ${flag}: ${command}`);
    assert.deepEqual(NODE_EXIT_SAFE_FLAGS, ['--no-maglev', '--no-concurrent-sparkplug']);
  } finally {
    await laneRun(['cancel', id], { env, cwd: repoDir });
    await waitFor(() => (fs.existsSync(lease.resultPath) ? true : null), { timeoutMs: 15000 });
  }
});

test('an operator choice in execArgv or NODE_OPTIONS is left alone', () => {
  assert.deepEqual(exitSafeFlagsFor([], ''), ['--no-maglev', '--no-concurrent-sparkplug']);
  assert.deepEqual(exitSafeFlagsFor(['--maglev'], ''), ['--no-concurrent-sparkplug']);
  assert.deepEqual(exitSafeFlagsFor([], '--max-old-space-size=64 --no-maglev'), ['--no-concurrent-sparkplug']);
  assert.deepEqual(exitSafeFlagsFor(['--concurrent-sparkplug'], '--maglev'), []);
});

test('a spawned child gets both flags even when this process was itself started with them (execArgv is not inherited)', () => {
  const root = path.resolve(import.meta.dirname, '..');
  const out = execFileSync(process.execPath, ['--no-maglev', '--input-type=module', '-e', "import { childExitSafeFlags } from './src/node-exit-safe.js'; console.log(JSON.stringify(childExitSafeFlags()));"], { cwd: root, env: { ...process.env, NODE_OPTIONS: '' }, encoding: 'utf8' });
  assert.deepEqual(JSON.parse(out), NODE_EXIT_SAFE_FLAGS);
});
