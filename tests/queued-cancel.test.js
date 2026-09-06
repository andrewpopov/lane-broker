import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor, sleep } from './helpers.js';
import { paths } from '../src/state.js';
import { isPidAlive } from '../src/lease.js';

test('cancelling a queued ticket lets its supervisor exit instead of polling forever', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  // Occupy the only capacity slot so the second ticket is stuck queued.
  const blocker = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '5'], { env, cwd: repoDir });
  await waitFor(
    () => {
      try {
        return fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json')).length > 0;
      } catch {
        return false;
      }
    },
    { timeoutMs: 15000 },
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
  await waitFor(() => queueFileExists(), { timeoutMs: 15000 });

  // Find the queued ticket's own supervisor pid so we can confirm it exits.
  const queueDir = paths(state).queue;
  const queueFileName = fs.readdirSync(queueDir).find((n) => n.includes(queuedId));
  const ticket = JSON.parse(fs.readFileSync(path.join(queueDir, queueFileName), 'utf8'));
  const supervisorPid = ticket.supervisorPid;
  assert.ok(isPidAlive(supervisorPid), 'the queued ticket supervisor should be alive before cancel');

  const cancelResult = await laneRun(['cancel', queuedId], { env, cwd: repoDir });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);
  assert.equal(queueFileExists(), false, 'the queued ticket must actually be removed');

  // The supervisor must notice its ticket is gone and exit -- not poll forever.
  await waitFor(() => !isPidAlive(supervisorPid), { timeoutMs: 15000 });

  await new Promise((resolve) => {
    if (blocker.exitCode !== null || blocker.signalCode !== null) return resolve();
    blocker.on('exit', resolve);
  });
});
