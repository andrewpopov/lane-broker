import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { readCpuLoad, sampleDevice } from '../src/device-gates.js';
import { claimOwner, ensureAgentDirs, writeStatus } from '../src/agent/store.js';

// PKG-440: `lane status` must never run the 250 ms CPU window; the load gate shows the running agent's own recorded reading.
async function statusWith({ recorded }) {
  const { home, state, base } = freshEnv();
  const remoteRoot = path.join(base, 'remote');
  const saved = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeGlobalConfig(home, { version: 1, capacity: 4, deviceGates: { load: { maxLoadPerCore: 2, minBusyFraction: 0.5 } } });
    ensureAgentDirs(remoteRoot);
    if (recorded) {
      await claimOwner(remoteRoot);
      writeStatus(remoteRoot, { deviceGates: { load: { closed: false, consecutiveOk: 0, reading: recorded } } });
    }
    const seen = [];
    const sampleGates = (gates, opts) => { seen.push(opts.cpuLoad); return sampleDevice(gates, { ...opts, platform: 'none' }); };
    const status = await collectStatus({ remoteRoot, sampleDeviceGates: true, sampleGates });
    return { status, seen };
  } finally {
    for (const [key, value] of [['LANE_BROKER_HOME', saved.home], ['LANE_BROKER_STATE', saved.state]]) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
  }
}

test('status samples the load gate without the CPU window, from the recorded agent reading', async () => {
  const { status, seen } = await statusWith({ recorded: { load5PerCore: 1.25, busyFraction: 0.4 } });
  assert.equal(seen.length, 1);
  assert.notEqual(seen[0], readCpuLoad, 'canary: status must not hand sampleDevice the real CPU window');
  const started = Date.now();
  assert.deepEqual(await seen[0](), { load5PerCore: 1.25, busyFraction: 0.4 });
  assert.ok(Date.now() - started < 100);
  assert.deepEqual(status.deviceGates.load.reading, { load5PerCore: 1.25, busyFraction: 0.4 });
  assert.match(renderStatusText(status), /load5\/core 1\.25/);
});

test('status hides the load gate when no current agent record exists', async () => {
  const { status, seen } = await statusWith({ recorded: null });
  assert.equal(await seen[0](), null);
  assert.deepEqual(status.deviceGates.load.reading, { load5PerCore: null, busyFraction: null });
  assert.doesNotMatch(renderStatusText(status), /load5\/core/);
});
