import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor } from './helpers.js';
import { paths } from '../src/state.js';
import { readLease, isGroupAlive } from '../src/lease.js';

test('lane cancel on a lease whose supervisor cannot act (SIGSTOPped) exits non-zero and reports the lease still held', async (t) => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  // --timeout so the outer `lane run` CLI process self-terminates quickly
  // instead of polling forever once we SIGSTOP its supervisor -- keeps this
  // test (and the process tree it leaves behind) from dragging out the suite.
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--timeout', '2s', '--', 'sleep', '30'], { env, cwd: repoDir });

  const leaseId = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    return names[0] ? names[0].replace(/\.json$/, '') : null;
  });
  const lease = await waitFor(() => {
    const l = readLease(state, leaseId);
    return l && l.childPgid ? l : null;
  });

  process.kill(lease.supervisorPid, 'SIGSTOP');
  t.after(async () => {
    try {
      process.kill(lease.supervisorPid, 'SIGCONT');
    } catch {
      // already gone
    }
    try {
      process.kill(-lease.childPgid, 'SIGKILL');
    } catch {
      // already gone
    }
    if (child.exitCode === null && child.signalCode === null) {
      await new Promise((resolve) => child.on('exit', resolve));
    }
  });

  const result = await laneRun(['cancel', leaseId], { env, cwd: repoDir });

  assert.notEqual(result.code, 0, `lane cancel must not report success while the supervisor never reacted; stderr: ${result.stderr}`);
  assert.ok(readLease(state, leaseId), 'the lease must still be reported held, not silently dropped');
  assert.ok(isGroupAlive(lease.childPgid), 'the child group must still be running -- cancel did not lie about killing it');
});
