import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeCpuBusyFile, laneSpawn, laneRun, sleep, waitFor } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { paths, readJsonSafe } from '../src/state.js';

// PKG-404: memory pressure (paging), not only free memory, stops pull claims and extra local starts.

const MiB = 1024 * 1024;
const GiB = 1024 * MiB;
const MAC_RAM = 48 * GiB;
const INCIDENT = { swapUsedBytes: 22016 * MiB, swapTotalBytes: 23552 * MiB, compressorBytes: 20757 * MiB, availableBytes: 16 * GiB, totalMemoryBytes: MAC_RAM };
const CALM_MAC = { swapUsedBytes: 5881 * MiB, swapTotalBytes: 7168 * MiB, compressorBytes: 8290 * MiB, availableBytes: 16 * GiB, totalMemoryBytes: MAC_RAM };
const PSI = (some) => `some avg10=${some} avg60=1.00 avg300=0.50 total=12345\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n`;
const MEMINFO = (swapUsedKb) => `MemTotal:       16000000 kB\nMemFree:         1000000 kB\nSwapTotal:       8000000 kB\nSwapFree:        ${8000000 - swapUsedKb} kB\n`;
const sysctl = (level) => () => `${level}\n`;
const noHook = {};
const happens = (what, check, timeoutMs = 10_000) => waitFor(check, { timeoutMs }).then(() => true, () => false).then((ok) => assert.ok(ok, what));

test('readMemoryPressure on macOS: the kernel level and swap plus compressor as a share of RAM, from the 2026-11-23 figures', async () => {
  const { readMemoryPressure } = await realSrc('memory-pressure.js');
  const incident = readMemoryPressure({ platform: 'darwin', env: noHook, exec: sysctl(1), sampleMemory: () => INCIDENT });
  assert.equal(incident.vmPressureLevel, 1);
  assert.equal(Math.round(incident.offloadPercent), 87);
  assert.equal(incident.psiSomeAvg10, null);
  assert.equal(Math.round(readMemoryPressure({ platform: 'darwin', env: noHook, exec: sysctl(1), sampleMemory: () => CALM_MAC }).offloadPercent), 29);
  const broken = readMemoryPressure({ platform: 'darwin', env: noHook, exec: () => { throw new Error('no sysctl'); }, sampleMemory: () => null });
  assert.deepEqual(broken, { vmPressureLevel: null, offloadPercent: null, psiSomeAvg10: null }, 'a failed probe is unknown, never an error');
});

test('readMemoryPressure on Linux: PSI some avg10, and swap use only where PSI is missing', async () => {
  const { readMemoryPressure } = await realSrc('memory-pressure.js');
  const files = (psi) => (f) => {
    if (f === '/proc/pressure/memory' && psi !== null) return psi;
    if (f === '/proc/meminfo') return MEMINFO(4000000);
    throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
  };
  assert.deepEqual(readMemoryPressure({ platform: 'linux', env: noHook, readFile: files(PSI(12.5)) }), { vmPressureLevel: null, offloadPercent: null, psiSomeAvg10: 12.5 });
  assert.deepEqual(readMemoryPressure({ platform: 'linux', env: noHook, readFile: files(null) }), { vmPressureLevel: null, offloadPercent: 25, psiSomeAvg10: null });
  assert.deepEqual(readMemoryPressure({ platform: 'win32', env: noHook }), { vmPressureLevel: null, offloadPercent: null, psiSomeAvg10: null });
  assert.deepEqual(readMemoryPressure({ env: { LANE_BROKER_TEST_MEMORY_PRESSURE: '2,,7' } }), { vmPressureLevel: 2, offloadPercent: null, psiSomeAvg10: 7 });
});

const gateScenario = async (load) => {
  const { gateConfigOf, updateDeviceGate } = await load('device-gates.js');
  const cfg = gateConfigOf('memory', undefined);
  const verdict = (reading) => updateDeviceGate(undefined, 'memory', cfg, reading).closed;
  assert.equal(verdict({ vmPressureLevel: 1, offloadPercent: 87 }), true, 'the 2026-11-23 thrash closes the gate');
  assert.equal(verdict({ vmPressureLevel: 1, offloadPercent: 29 }), false, 'the same Mac at rest stays open');
  assert.equal(verdict({ vmPressureLevel: 2, offloadPercent: 10 }), true, 'macOS warn closes it');
  assert.equal(verdict({ psiSomeAvg10: 12, offloadPercent: 0 }), true, 'Linux PSI over 10 closes it');
  assert.equal(verdict({ psiSomeAvg10: 3, offloadPercent: 95 }), false, 'swap use is only a fallback: PSI says the host is not stalling');
  assert.equal(verdict({ offloadPercent: 70 }), true, 'without PSI, swap use decides');
  assert.equal(verdict({}), false, 'unknown never closes');
};

test('the memory gate is on by default and closes on the incident shape, on macOS warn, on Linux PSI, and on swap only without PSI', async () => gateScenario(realSrc));

test('canary: with the memory violation removed, the gate scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ["      return paging ? 'memory-pressure' : null;", '      return null;'], gateScenario);
});

const defaultOnScenario = async (load) => {
  const { gateConfigOf, updateDeviceGates, deviceGatesProblem } = await load('device-gates.js');
  const defaults = { maxVmPressureLevel: 1, maxOffloadPercent: 60, maxPsiSomeAvg10: 10 };
  assert.deepEqual(gateConfigOf('memory', undefined), defaults, 'no deviceGates at all');
  assert.deepEqual(gateConfigOf('memory', { thermal: { maxPressureLevel: 1 } }), defaults, 'an overlay that sets only thermal keeps it');
  assert.deepEqual(gateConfigOf('memory', { memory: { maxOffloadPercent: 75 } }), { ...defaults, maxOffloadPercent: 75 });
  assert.equal(gateConfigOf('memory', { memory: false }), null, '"memory": false turns it off');
  assert.equal(gateConfigOf('thermal', undefined), null, 'the other gates stay opt-in');
  assert.deepEqual(Object.keys(updateDeviceGates(undefined, null, {})), ['memory']);
  assert.deepEqual(Object.keys(updateDeviceGates(undefined, { memory: false }, {})), []);
  for (const good of [{ memory: false }, { memory: {} }, { memory: { maxVmPressureLevel: 4, maxOffloadPercent: 150, maxPsiSomeAvg10: 0, openSamples: 1 } }]) assert.equal(deviceGatesProblem(good), null, JSON.stringify(good));
  for (const [bad, pattern] of [[{ memory: { maxVmPressureLevel: 0 } }, /maxVmPressureLevel" must be an integer from 1 to 4/], [{ memory: { maxPsiSomeAvg10: 101 } }, /maxPsiSomeAvg10/], [{ memory: { swap: 1 } }, /"deviceGates.memory" has unknown key "swap"/], [{ thermal: false }, /"deviceGates.thermal" must be an object/]]) {
    assert.match(deviceGatesProblem(bad) ?? 'accepted', pattern, JSON.stringify(bad));
  }
};

test('memory is default-on and survives an overlay that sets other gates; false turns it off; its thresholds validate by name', async () => defaultOnScenario(realSrc));

test('canary: with the memory defaults ignored, the default-on scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ['  if (own === false || (own === undefined && !defaults)) return null;', '  if (own === false || own === undefined) return null;'], defaultOnScenario);
});

test('hysteresis: one paging sample closes the gate; it reopens only after openSamples clean ones; an unknown sample holds it', async () => {
  const { gateConfigOf, updateDeviceGate } = await realSrc('device-gates.js');
  const cfg = gateConfigOf('memory', undefined);
  let state = updateDeviceGate(undefined, 'memory', cfg, { vmPressureLevel: 2 });
  for (const [reading, closed, ok] of [[{ vmPressureLevel: 1 }, true, 1], [{}, true, 1], [{ vmPressureLevel: 1 }, true, 2], [{ vmPressureLevel: 4 }, true, 0], [{ vmPressureLevel: 1 }, true, 1], [{ vmPressureLevel: 1 }, true, 2], [{ vmPressureLevel: 1 }, false, 0]]) {
    state = updateDeviceGate(state, 'memory', cfg, reading);
    assert.deepEqual([state.closed, state.consecutiveOk], [closed, ok], JSON.stringify(reading));
  }
});

test('local admission samples the gate at most once a second into one shared state file', async () => {
  const { sampleMemoryGate, readMemoryGate } = await realSrc('device-gates.js');
  const root = makeTmpDir('memory-gate-root-');
  let reads = 0;
  const read = (reading) => () => { reads += 1; return reading; };
  assert.equal(sampleMemoryGate(root, undefined, { read: read({ vmPressureLevel: 2 }), now: 1000 }).closed, true);
  assert.equal(sampleMemoryGate(root, undefined, { read: read({ vmPressureLevel: 1 }), now: 1500 }).consecutiveOk, 0, 'reused inside the second');
  assert.equal(reads, 1);
  assert.equal(sampleMemoryGate(root, undefined, { read: read({ vmPressureLevel: 1 }), now: 2100 }).consecutiveOk, 1);
  assert.deepEqual([readMemoryGate(root).closed, readMemoryGate(root).openSamples], [true, 3]);
  assert.equal(sampleMemoryGate(root, { memory: false }, { read: read({ vmPressureLevel: 4 }), now: 9000 }).closed, false, 'off means never closed');
});

test('lane status renders a closed local memory gate and the live memory reading', async () => {
  const { renderMemoryGateLine } = await realSrc('status.js');
  const { liveDeviceGates, renderDeviceGatesLine } = await realSrc('device-gates.js');
  assert.equal(renderMemoryGateLine({ closed: true, consecutiveOk: 1, openSamples: 3 }), 'memory gate: CLOSED (memory-pressure) — no new local lane starts beside a running one; reopens after 3 clean samples (1 so far)');
  const live = liveDeviceGates(undefined, { memory: { vmPressureLevel: 1, offloadPercent: 87.4, psiSomeAvg10: null } });
  assert.equal(renderDeviceGatesLine(live), 'device gates: memory CLOSED (vm pressure 1/≤1, swap+compressor 87% of RAM/≤60%)');
  assert.equal(renderDeviceGatesLine(liveDeviceGates(undefined, {})), 'device gates: memory ok (readings unknown)');
});

const agentScenario = async (load) => {
  const { startAgent } = await load('agent/run.js');
  const tmp = makeTmpDir('memory-agent-');
  const remoteRoot = path.join(tmp, 'remote');
  const textfileDir = makeTmpDir('memory-agent-prom-');
  const client = { on() {}, end: async () => {}, escapeIdentifier: (s) => `"${s}"`, query: async () => ({ rows: [{ v: { host: 'a1', max_claims: 1, protocol_min: 1 } }] }) };
  class Driver { adoptAll() {} poke() {} watchScope() {} scopeBlock() { return null; } async close() {} }
  const agent = await startAgent({
    remoteRoot, version: 't', s3: {}, Driver, figures: () => null, draining: () => false, log: () => {}, textfileDir,
    deviceSampler: async () => ({ thermal: {}, power: {}, memory: { vmPressureLevel: 1, offloadPercent: 87, psiSomeAvg10: null } }),
    config: { runnerName: 'a1', pollMs: 200, schedule: null, stopClaimingBeforeCloseMs: 0, brokerHome: path.join(tmp, 'home'), stateDir: path.join(tmp, 'state'), pg: {} },
    coreOptions: { connect: async () => client },
  });
  try {
    const { readAgentStatus } = await load('agent/store.js');
    await happens('with no deviceGates configured the agent still pauses claims', () => readAgentStatus(remoteRoot).claims === 'paused:memory-pressure', 5000);
    const prom = () => (fs.existsSync(path.join(textfileDir, 'lane-agent.prom')) ? fs.readFileSync(path.join(textfileDir, 'lane-agent.prom'), 'utf8') : '');
    await happens('the prom file says the memory gate is closed', () => prom().includes('lane_agent_device_gate_closed{gate="memory"} 1'), 5000);
    assert.match(prom(), /^lane_agent_memory_pressure\{signal="vm_pressure_level"\} 1$/m);
    assert.match(prom(), /^lane_agent_memory_pressure\{signal="offload_percent"\} 87$/m);
  } finally {
    await agent.stop();
  }
};

test('the pull agent with no deviceGates config pauses claims as paused:memory-pressure and exports the gate and its readings', async () => agentScenario(realSrc));

test('canary: with the agent skipping device sampling when deviceGates is unset, the agent scenario fails by name', async () => {
  await assertCanaryFails('agent/run.js', ['    reloadDeviceGatesConfig();\n    let sample', '    reloadDeviceGatesConfig();\n    if (!deviceGatesConfig) {\n      deviceGates = {};\n      return;\n    }\n    let sample'], agentScenario);
});

function activeHost() {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 150, schedulerMode: 'active', cpuAdmissionPercent: 90 });
  const repoDir = path.join(base, 'repo');
  const lane = { weight: 1, cpuCores: 1, memoryBytes: 64 * MiB };
  writeRepoConfig(repoDir, { version: 1, lanes: { holder: lane, second: lane } });
  return { state, repoDir, env: { ...env, LANE_BROKER_CPU_BUSY_FILE: writeCpuBusyFile(base, 0, 8) } };
}

const detached = async (lane, args, env, cwd, bin) => (await laneRun(['run', '--repo', 'r', '--lane', lane, '--detach', ...args], { env, cwd, bin })).stdout.trim();
const leased = (state, id) => fs.existsSync(path.join(paths(state).leases, `${id}.json`));

const localScenario = async (load) => {
  const { state, repoDir, env } = activeHost();
  const release = path.join(makeTmpDir('memory-release-'), 'go');
  const holder = await detached('holder', ['--', 'node', '-e', `const fs=require('fs');const t=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)}))clearInterval(t)},50)`], env, repoDir, load.laneBin);
  try {
    await happens('the first lane starts on a calm host', () => leased(state, holder), 30_000);
    await sleep(1200);
    const paging = { ...env, LANE_BROKER_TEST_MEMORY_PRESSURE: '2,,' };
    const second = await detached('second', ['--', 'node', '-e', '0'], paging, repoDir, load.laneBin);
    await happens('the second lane sampled the paging host', () => readJsonSafe(paths(state).memoryGate)?.closed === true, 30_000);
    await sleep(1000);
    assert.equal(leased(state, second), false, 'no second lane starts beside a running one while the host pages');
    assert.equal(readJsonSafe(path.join(paths(state).waits, `${second}.json`))?.reason, 'memory-pressure');
    fs.writeFileSync(release, '');
    const waited = await laneRun(['wait', second, '--timeout', '60s'], { env, bin: load.laneBin });
    assert.equal(waited.code, 0, `once the broker is idle the paging host still runs one lane: ${waited.stderr}`);
  } finally {
    fs.writeFileSync(release, '');
  }
};

test('local admission: a paging host runs one lane at a time, not two and not none', { timeout: 120_000 }, async () => localScenario(realSrc));

test('canary: with the scheduler ignoring the memory gate, the local scenario fails by name', { timeout: 120_000 }, async () => {
  await assertCanaryFails('scheduler.js', ["    if (cfg.schedulerMode === 'active' && memoryGate.closed && !brokerIdle && !exclusiveHolder) {", '    if (false) {'], localScenario);
});
