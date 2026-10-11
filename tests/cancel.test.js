import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor } from './helpers.js';
import { paths, atomicWriteJson, appendHistory } from '../src/state.js';
import { readLease, isGroupAlive } from '../src/lease.js';
import { createAttempt, readAttempt, publishTerminal } from '../src/attempts.js';
import { cancelAttempt } from '../src/cancel.js';

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
    // The supervisor (the test's own descendant) wakes, notices its child died and writes its
    // result into the state dir; let it finish before the dir is removed.
    await waitFor(() => {
      try {
        process.kill(lease.supervisorPid, 0);
        return false;
      } catch {
        return true;
      }
    });
  });

  const result = await laneRun(['cancel', leaseId], { env, cwd: repoDir });

  assert.notEqual(result.code, 0, `lane cancel must not report success while the supervisor never reacted; stderr: ${result.stderr}`);
  assert.ok(readLease(state, leaseId), 'the lease must still be reported held, not silently dropped');
  assert.ok(isGroupAlive(lease.childPgid), 'the child group must still be running -- cancel did not lie about killing it');
});

test('BRAIN-515: lane cancel racing an already-published attempt exits 0 and leaves exactly one result and one history row', async () => {
  const { state, home } = freshEnv();
  const previous = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    const id = 'raced1';
    await createAttempt(state, id, {});
    // cancel read this snapshot (dead supervisor), then the original publisher settled the attempt before cancel got to publish.
    const snapshot = { ...readAttempt(state, id), supervisor: { pid: 2 ** 22 + 7, startTime: 1 } };
    const resultPath = path.join(paths(state).results, `${id}.json`);
    const published = await publishTerminal(state, id, 0, () => {
      atomicWriteJson(resultPath, { id, exit: 0, cancelled: false });
      appendHistory(state, { id, exit: 0, endedAt: Date.now() });
    });
    assert.equal(published.ok, true);

    const outcome = await cancelAttempt(state, id, snapshot);
    assert.equal(outcome.exitCode, 0);
    assert.deepEqual(JSON.parse(fs.readFileSync(resultPath, 'utf8')), { id, exit: 0, cancelled: false }, 'the real result must not be overwritten');
    const rows = fs.readFileSync(paths(state).history, 'utf8').trim().split('\n');
    assert.equal(rows.length, 1, 'exactly one history row');
  } finally {
    if (previous.home === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = previous.home;
    if (previous.state === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = previous.state;
  }
});
