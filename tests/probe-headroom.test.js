import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { probeHeadroom, remoteProbeCommand } from '../src/remote-runner.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { paths, atomicWriteJson } from '../src/state.js';
import { externalBusyCores } from '../src/external-cpu.js';

// BRAIN-506: the probe's free CPU must agree with this runner's own admission measure.
const cfg = { memoryReserveBytes: 0, preemptibleNiceMin: 10, preemptibleShare: 0.8 };
const resources = { cpuBudgetCores: 14.4, reservedCpuCores: 8, availableMemoryBytes: 8e9 };

test('a runner loaded with preemptible processes reports positive headroom', () => {
  // loadavg 20.5 would read as zero headroom; 11 of 14 busy cores are niced sims, so only 20% of those count
  const external = externalBusyCores({ hostBusyCores: 14, preemptibleBusyCores: 11 }, [], cfg); // 14 - 0.8 * 11 = 5.2
  const room = probeHeadroom(resources, cfg, true, 20.5, external);
  assert.ok(room.cpuCores > 0);
  assert.ok(Math.abs(room.cpuCores - (14.4 - 8 - 5.2)) < 1e-9);
});

test('a runner loaded with non-preemptible processes still reports zero headroom', () => {
  const external = externalBusyCores({ hostBusyCores: 20, preemptibleBusyCores: 0 }, [], cfg);
  assert.equal(probeHeadroom(resources, cfg, true, 20.5, external).cpuCores, 0);
});

test('with no CPU sample the probe falls back to loadavg', () => {
  assert.equal(probeHeadroom(resources, cfg, true, 20.5, null).cpuCores, 0);
  assert.ok(Math.abs(probeHeadroom(resources, cfg, true, 3, null).cpuCores - 6.4) < 1e-9);
  assert.ok(Math.abs(probeHeadroom(resources, cfg, true, 3).cpuCores - 6.4) < 1e-9);
});

test('remote-probe on an idle runner with a stale sample measures a window and reports headroom', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, schedulerMode: 'active', cpuReserveCores: 1, cpuAdmissionPercent: 100, preemptibleNiceMin: 1, preemptibleShare: 0.8 });
  const old = Date.now() - 600_000;
  atomicWriteJson(paths(state).cpuSample, { at: old, cpus: [], lastValid: { hostBusyCores: 99, cores: 10, at: old } });
  const sims = (cores, nice) => ({ rows: [{ pid: 1, ppid: 0, pgid: 1, nice: 0, token: 't1', deltaSec: 0 }, { pid: 4242, ppid: 1, pgid: 4242, nice, token: 't4242', deltaSec: cores }], windowMs: 1000 });
  const probe = async (window) => {
    const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE, write: process.stdout.write };
    process.env.LANE_BROKER_HOME = home;
    process.env.LANE_BROKER_STATE = state;
    let out = '';
    process.stdout.write = (chunk) => { out += chunk; return true; };
    try {
      await remoteProbeCommand({ root: path.join(state, 'remote'), scan: async () => window });
    } finally {
      process.stdout.write = prev.write;
      for (const [k, v] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
    return JSON.parse(out);
  };
  const niced = await probe(sims(6, 15)); // 6 niced cores count as 1.2
  const hot = await probe(sims(6, 0)); // 6 nice-0 cores count in full
  const budget = niced.capacity.cpuCores;
  assert.ok(Math.abs(niced.headroom.cpuCores - (budget - 1.2)) < 1e-9, `niced headroom ${niced.headroom.cpuCores} of budget ${budget}`);
  assert.ok(Math.abs(hot.headroom.cpuCores - Math.max(0, budget - 6)) < 1e-9);
});
