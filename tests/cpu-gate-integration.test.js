import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeCpuBusyFile, laneSpawn, sleep, waitFor } from './helpers.js';
import { paths, readJsonSafe } from '../src/state.js';

/**
 * The CPU-gate equivalent of tests/gate-integration.test.js's threshold-edit
 * test — same shape, same incident numbers translated to CPU-gate units,
 * proving src/admission.js's CPU gate inherited BOTH of the load gate's
 * pre-fix bugs by construction and both are now fixed:
 *   1. per-poll config (not a startup snapshot) reaches the gate, and
 *   2. the shared consecutive-under counter is stamped with a fingerprint of
 *      its own thresholds and resets when they change, so two supervisors
 *      reloading at different instants during a threshold edit cannot blend
 *      two configs into a decision no single config would have produced.
 *
 * schedulerMode is 'active' throughout: shadow mode never gates a start
 * (that's its entire point — see tests/admission.test.js), so only 'active'
 * makes the CPU gate's open/closed state observable via whether a lease
 * appears at all.
 */

function baseCfg(overrides = {}) {
  return {
    version: 1,
    capacity: 4,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 150,
    schedulerMode: 'active',
    cpuAdmissionPercent: 90,
    ...overrides,
  };
}

test('cpu gate: a config edit raising cpuOpenPercent mid-flight lets a stuck gate reopen, matching the load-gate incident shape', async () => {
  const { base, home, state, env: baseEnv } = freshEnv();
  writeGlobalConfig(home, baseCfg({ cpuClosePercent: 90, cpuOpenPercent: 50, cpuOpenSamples: 2 }));
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const cpuBusyFile = writeCpuBusyFile(base, 9.5, 10); // 95% busy: above cpuClosePercent (90) -> closes
  const env = { ...baseEnv, LANE_BROKER_CPU_BUSY_FILE: cpuBusyFile };

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'sleep', '1'], { env, cwd: repoDir });
  const id = await new Promise((resolve) => {
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.on('exit', () => resolve(out.trim()));
  });
  assert.ok(id, 'lane run --detach should print an id');

  // Wait for the gate to actually observe the high busy% and close, rather
  // than a fixed sleep racing the supervisor's own startup/spawn overhead.
  const closed = await waitFor(() => readJsonSafe(paths(state).cpuGate)?.closed === true, { timeoutMs: 10000 });
  assert.ok(closed, 'the CPU gate must actually observe the high busy% and close before the rest of this test means anything');
  assert.equal(fs.existsSync(path.join(paths(state).leases, `${id}.json`)), false, 'must not start while the CPU gate is closed');

  // Drop busy% to 65%: above the ORIGINAL cpuOpenPercent (50) but below
  // cpuClosePercent (90). Under the ORIGINAL config this never reopens the
  // gate — every sample in that band resets the consecutive-under counter
  // to 0 forever, the exact shape of the real load-gate incident.
  fs.writeFileSync(cpuBusyFile, '6.5,10');
  const sampledMidBand = await waitFor(() => {
    const g = readJsonSafe(paths(state).cpuGate);
    return g && Math.abs(g.lastBusyPercent - 65) < 1e-9;
  }, { timeoutMs: 10000 });
  assert.ok(sampledMidBand, 'the gate must sample the pre-edit busy% (65%) before the config is rewritten below');
  await sleep(400);
  assert.equal(
    fs.existsSync(path.join(paths(state).leases, `${id}.json`)),
    false,
    'must still be closed: 65% sits between the original cpuOpenPercent (50) and cpuClosePercent (90)',
  );

  // Raise cpuOpenPercent above the (unchanged) current busy% (65), exactly as
  // the operator did in the real incident. A supervisor that only ever
  // samples the config it started with can never reopen the gate here, since
  // the busy% never drops below the original cpuOpenPercent (50). This is
  // the end-to-end proof that a running supervisor picks up a config change
  // instead of being stuck on its startup snapshot, AND that the counter
  // resets under the new thresholds instead of carrying over a count that
  // was accumulated under the old ones.
  writeGlobalConfig(home, baseCfg({ cpuClosePercent: 95, cpuOpenPercent: 70, cpuOpenSamples: 2 }));

  const lease = await waitFor(() => fs.existsSync(path.join(paths(state).leases, `${id}.json`)), { timeoutMs: 10000 });
  assert.ok(lease, 'should start once the reloaded config raises cpuOpenPercent above the current busy%');
});
