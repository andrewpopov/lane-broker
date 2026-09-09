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
  //
  // The child runs until the TEST releases it, not for a fixed span. A fixed
  // 12s busy loop raced the two waitFor budgets below (15s + 15s = up to 30s
  // of observation against a child that lived 12s): on a loaded host the
  // three node startups ate the child's lifetime, it exited, its lease went
  // with it, and the second waitFor then polled for observedCpuCores on a
  // lease that no longer existed -- reported as "never observed" when the
  // truth was "no longer running". That is the failure this test kept
  // producing (BRAIN-208), and no amount of extra timeout fixes it, because
  // a longer wait also means more time for the child to finish first.
  // The safety cap is a backstop against a stranded child only; it is well
  // clear of the observation budget so it can never be the thing that ends
  // the loop in a passing run.
  const releaseFile = path.join(base, 'release-child');
  const cmd = [
    'node',
    '-e',
    `const rel = ${JSON.stringify(releaseFile)};` +
      'const cap = Date.now() + 120000;' +
      'const fs = require("fs");' +
      'while (Date.now() < cap && !fs.existsSync(rel)) { Math.sqrt(Math.random()); }',
  ];
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...cmd], { env, cwd: repoDir });

  const leaseId = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    return names[0] ? names[0].replace(/\.json$/, '') : null;
  }, { timeoutMs: 15000 }); // same reason as below: three node startups on a loaded host

  const lease = await waitFor(
    () => {
      const l = readLease(state, leaseId);
      return l && Number.isFinite(l.observedCpuCores) ? l : null;
    },
    // Generous on purpose: under a loaded host three node startups (CLI,
    // supervisor, child) can take seconds, and the busy loop above must
    // still be alive when the first observation lands — a child that
    // finished first takes its lease with it and this reads as "never
    // observed", which is a timing artifact, not the bug this test guards.
    { timeoutMs: 15000 },
  );
  assert.ok(Number.isFinite(lease.observedCpuCores), 'a live busy child must eventually get a numeric observedCpuCores');
  assert.ok(Number.isFinite(lease.observedAt), 'observedAt must be stamped alongside it');

  // Done observing — release the child, then make sure it is gone.
  fs.writeFileSync(releaseFile, '');
  try { process.kill(-lease.childPgid, 'SIGKILL'); } catch { /* already gone */ }
  await new Promise((resolve) => child.on('exit', resolve));
});
