import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, waitFor } from './helpers.js';
import { paths } from '../src/state.js';
import { readLease } from '../src/lease.js';
import { applyHeartbeatObservation } from '../src/supervisor.js';

// BRAIN-207: observed per-lane CPU is telemetry only -- wired into the
// supervisor heartbeat via applyHeartbeatObservation, unit-tested here in
// isolation (no real process group needed for the null-reading branch),
// plus one real end-to-end run proving a live lease actually gets a numeric
// observation.

test('applyHeartbeatObservation stamps observedCpuCores + observedAt on a finite reading', () => {
  const lease = { id: 'x', heartbeatAt: 1, weight: 1 };
  const updated = applyHeartbeatObservation(lease, 0.42, 5000);
  assert.equal(updated.heartbeatAt, 5000, 'the heartbeat itself always advances');
  assert.equal(updated.observedCpuCores, 0.42);
  assert.equal(updated.observedAt, 5000);
});

test('applyHeartbeatObservation leaves a prior observation untouched on a null reading', () => {
  const lease = { id: 'x', heartbeatAt: 1, weight: 1, observedCpuCores: 0.9, observedAt: 100 };
  const updated = applyHeartbeatObservation(lease, null, 5000);
  assert.equal(updated.heartbeatAt, 5000, 'the heartbeat itself still advances');
  assert.equal(updated.observedCpuCores, 0.9, 'a failed probe must not clobber a good prior observation');
  assert.equal(updated.observedAt, 100);
});

test('applyHeartbeatObservation leaves observedCpuCores unset (not zero) when there was never a prior observation and the reading is null', () => {
  const lease = { id: 'x', heartbeatAt: 1, weight: 1 };
  const updated = applyHeartbeatObservation(lease, null, 5000);
  assert.equal('observedCpuCores' in updated, false);
});

test('a real running lane\'s lease gets a numeric observedCpuCores from a live heartbeat', async () => {
  const { base, home, state, env } = freshEnv();
  // sampleMs also drives the heartbeat interval in supervisor.js's main().
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 150 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  // Busy-loop so there is real, non-zero CPU for `ps` to observe within a
  // couple of heartbeat intervals, rather than racing a near-idle `sleep`.
  const cmd = ['node', '-e', 'const end = Date.now() + 3000; while (Date.now() < end) { Math.sqrt(Math.random()); }'];
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...cmd], { env, cwd: repoDir });

  const leaseId = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    return names[0] ? names[0].replace(/\.json$/, '') : null;
  });

  const lease = await waitFor(
    () => {
      const l = readLease(state, leaseId);
      return l && Number.isFinite(l.observedCpuCores) ? l : null;
    },
    { timeoutMs: 5000 },
  );
  assert.ok(Number.isFinite(lease.observedCpuCores), 'a live busy child must eventually get a numeric observedCpuCores');
  assert.ok(Number.isFinite(lease.observedAt), 'observedAt must be stamped alongside it');

  await new Promise((resolve) => child.on('exit', resolve));
});
