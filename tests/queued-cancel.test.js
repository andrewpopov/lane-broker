import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor, sleep } from './helpers.js';
import { paths } from '../src/state.js';
import { isPidAlive } from '../src/lease.js';

test('cancelling a queued ticket lets its supervisor exit instead of polling forever', async () => {
  const { base, home, state, env } = freshEnv();
  const globalConfig = { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 };
  writeGlobalConfig(home, globalConfig);
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  // Occupy the only capacity slot so the second ticket is stuck queued.
  // This wait measures host startup cost (spawn + broker claiming the lease
  // under contention), not broker behaviour under test, so it gets a
  // generous, load-scaled budget rather than a fixed one calibrated for an
  // idle box.
  const STARTUP_TIMEOUT_MS = Math.max(60_000, 30 * globalConfig.sampleMs);
  const blocker = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '5'], { env, cwd: repoDir });
  await waitFor(
    () => {
      try {
        return fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json')).length > 0;
      } catch {
        return false;
      }
    },
    { timeoutMs: STARTUP_TIMEOUT_MS },
  );

  const queued = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'true'], { env, cwd: repoDir });
  const queuedId = await new Promise((resolve) => {
    let out = '';
    queued.stdout.on('data', (d) => {
      out += d;
    });
    queued.on('exit', () => resolve(out.trim()));
  });

  const queueFileExists = () => {
    try {
      return fs.readdirSync(paths(state).queue).some((n) => n.includes(queuedId));
    } catch {
      return false;
    }
  };
  // Same startup-dominated wait: enqueuing the second ticket depends on
  // host load, not on the cancel-exit logic under test.
  await waitFor(() => queueFileExists(), { timeoutMs: STARTUP_TIMEOUT_MS });

  // Find the queued ticket's own supervisor pid so we can confirm it exits.
  const queueDir = paths(state).queue;
  const queueFileName = fs.readdirSync(queueDir).find((n) => n.includes(queuedId));
  const ticket = JSON.parse(fs.readFileSync(path.join(queueDir, queueFileName), 'utf8'));
  const supervisorPid = ticket.supervisorPid;
  assert.ok(isPidAlive(supervisorPid), 'the queued ticket supervisor should be alive before cancel');

  const cancelResult = await laneRun(['cancel', queuedId], { env, cwd: repoDir });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);
  assert.equal(queueFileExists(), false, 'the queued ticket must actually be removed');

  // The supervisor must notice its ticket is gone and exit -- not poll
  // forever. This IS the behaviour under test, so its budget stays tight
  // relative to the supervisor's own poll cadence (sampleMs) rather than
  // generous like the startup waits above: a supervisor that really polls
  // forever must still fail this assertion by name.
  await waitFor(() => !isPidAlive(supervisorPid), { timeoutMs: 20 * globalConfig.sampleMs });

  await new Promise((resolve) => {
    if (blocker.exitCode !== null || blocker.signalCode !== null) return resolve();
    blocker.on('exit', resolve);
  });
});
