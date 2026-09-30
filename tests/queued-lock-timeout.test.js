import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, waitFor } from './helpers.js';
import { paths, withLock } from '../src/state.js';

// BRAIN-345: a global-lock acquire timeout inside a queued supervisor's poll
// loop is contention. It used to escape main() and kill the supervisor with no
// result, losing the ticket.
test('queued ticket survives a global-lock acquire timeout while polling and still runs', async () => {
  const { base, home, state, env } = freshEnv({ LANE_BROKER_TEST_LOCK_TIMEOUT_MS: '150' });
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 50 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const STARTUP_TIMEOUT_MS = 60_000;
  const listing = (dir) => {
    try {
      return fs.readdirSync(dir).filter((n) => n.endsWith('.json'));
    } catch {
      return [];
    }
  };

  const blocker = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '4'], { env, cwd: repoDir });
  let queued;
  try {
    await waitFor(() => listing(paths(state).leases).length > 0, { timeoutMs: STARTUP_TIMEOUT_MS });
    queued = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
    const queuedRun = new Promise((resolve) => {
      let stderr = '';
      queued.stderr.on('data', (d) => {
        stderr += d;
      });
      queued.on('close', (code) => resolve({ code, stderr }));
    });
    await waitFor(() => listing(paths(state).queue).length > 0, { timeoutMs: STARTUP_TIMEOUT_MS });

    // Hold the global lock well past the injected 150ms deadline, so the queued
    // supervisor's tryStart times out several times.
    const logPath = paths(state).admissionLog;
    await withLock(state, async () => {
      await waitFor(() => fs.existsSync(logPath) && fs.readFileSync(logPath, 'utf8').includes('lock-timeout'), {
        timeoutMs: 10_000,
      });
    }, { timeoutMs: 10_000 });

    const { code, stderr } = await queuedRun;
    assert.doesNotMatch(stderr, /supervisor exited unexpectedly/);
    assert.equal(code, 0, `queued ticket should run to completion, stderr: ${stderr}`);
  } finally {
    for (const c of [blocker, queued]) if (c && c.exitCode === null) c.kill('SIGKILL');
  }
});
