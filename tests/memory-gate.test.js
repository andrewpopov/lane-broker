import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, writeCpuBusyFile, laneRun, sleep, waitFor } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { atomicWriteJson, paths, readJsonSafe } from '../src/state.js';

// PKG-404: current paging (kernel level, PSI, swap traffic), never how much sits in swap, stops pull claims and extra local starts.

const MiB = 1024 * 1024;
const PSI = (some) => `some avg10=${some} avg60=1.00 avg300=0.50 total=12345\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n`;
const VMSTAT_MAC = 'Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free:                               10.\nSwapins:                                    14706337.\nSwapouts:                                   31464770.\n';
const PROC_VMSTAT = 'nr_free_pages 100\npswpin 1000\npswpout 3000\n';
const macExec = (level) => (cmd) => (cmd === 'sysctl' ? `${level}\n` : VMSTAT_MAC);
const noHook = {};
const happens = (what, check, timeoutMs = 10_000) => waitFor(check, { timeoutMs }).then(() => true, () => false).then((ok) => assert.ok(ok, what));
// a host that swapped 22 GiB long ago and has since stopped: a huge counter that no longer moves
const RETAINED = 22 * 1024 * MiB;

test('readMemoryPressure on macOS: the kernel level and the since-boot swap traffic counter from vm_stat', async () => {
  const { readMemoryPressure } = await realSrc('memory-pressure.js');
  assert.deepEqual(readMemoryPressure({ platform: 'darwin', env: noHook, exec: macExec(1), now: 5 }), { vmPressureLevel: 1, psiSomeAvg10: null, swapBytes: 16384 * (14706337 + 31464770), at: 5 });
  const broken = readMemoryPressure({ platform: 'darwin', env: noHook, exec: () => { throw new Error('no sysctl'); }, now: 5 });
  assert.deepEqual(broken, { vmPressureLevel: null, psiSomeAvg10: null, swapBytes: null, at: 5 }, 'a failed probe is unknown, never an error');
});

const linuxPressure = async (load) => {
  const { readMemoryPressure } = await load('memory-pressure.js');
  const getconf = (bytes) => (cmd, args) => {
    assert.deepEqual([cmd, args], ['getconf', ['PAGESIZE']]);
    if (bytes === null) throw new Error('getconf: not found');
    return `${bytes}\n`;
  };
  const files = (psi) => (f) => {
    if (f === '/proc/pressure/memory' && psi !== null) return psi;
    if (f === '/proc/vmstat') return PROC_VMSTAT;
    throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
  };
  assert.deepEqual(readMemoryPressure({ platform: 'linux', env: noHook, readFile: files(PSI(12.5)), now: 5 }), { vmPressureLevel: null, psiSomeAvg10: 12.5, swapBytes: null, at: 5 });
  assert.equal(readMemoryPressure({ platform: 'linux', env: noHook, exec: getconf(65536), readFile: files(null), now: 5 }).swapBytes, 65536 * 4000, 'a 64 KiB kernel counts 64 KiB pages');
  assert.equal(readMemoryPressure({ platform: 'linux', env: noHook, exec: getconf(4096), readFile: files(null), now: 5 }).swapBytes, 4096 * 4000);
  assert.equal(readMemoryPressure({ platform: 'linux', env: noHook, exec: getconf(null), readFile: files(null), now: 5 }).swapBytes, null, 'an unknown page size is no counter, never a guess');
  assert.deepEqual(readMemoryPressure({ platform: 'win32', env: noHook, now: 5 }), { vmPressureLevel: null, psiSomeAvg10: null, swapBytes: null, at: 5 });
  assert.deepEqual(readMemoryPressure({ env: { LANE_BROKER_TEST_MEMORY_PRESSURE: '2,7,' }, now: 5 }), { vmPressureLevel: 2, psiSomeAvg10: 7, swapBytes: null, at: 5 });
};

test('readMemoryPressure on Linux: PSI some avg10, and the pswpin/pswpout counter at the host page size only where PSI is missing', async () => linuxPressure(realSrc));

test('canary: with Linux pages assumed to be 4 KiB, the 64 KiB kernel undercounts and the scenario fails by name', async () => {
  await assertCanaryFails('memory-pressure.js', ["parseProcVmstatSwapBytes(readFile('/proc/vmstat'), linuxPageBytes(exec))", "parseProcVmstatSwapBytes(readFile('/proc/vmstat'), 4096)"], linuxPressure);
});

/** The gate after `readings` in order, each `at` seconds apart from the first. */
const gateAfter = (update, cfg, readings) => readings.reduce((state, reading) => update(state, 'memory', cfg, reading), undefined);

const gateScenario = async (load) => {
  const { gateConfigOf, updateDeviceGate } = await load('device-gates.js');
  const cfg = gateConfigOf('memory', undefined);
  const closed = (...readings) => gateAfter(updateDeviceGate, cfg, readings).closed;
  assert.equal(closed({ vmPressureLevel: 2 }), true, 'macOS warn closes it');
  assert.equal(closed({ vmPressureLevel: 1 }), false, 'macOS normal leaves it open');
  assert.equal(closed({ psiSomeAvg10: 12 }), true, 'Linux PSI over 10 closes it');
  assert.equal(closed({ psiSomeAvg10: 3 }), false);
  assert.equal(closed({ swapBytes: 0, at: 0 }, { swapBytes: 20 * 15 * MiB, at: 15_000 }), true, '20 MiB/s of swap traffic over a 15 s poll closes it');
  assert.equal(closed({ swapBytes: 0, at: 0 }, { swapBytes: 5 * 15 * MiB, at: 15_000 }), false, '5 MiB/s does not');
  assert.equal(closed({ vmPressureLevel: 1, swapBytes: RETAINED, at: 0 }, { vmPressureLevel: 1, swapBytes: RETAINED, at: 15_000 }), false, 'retained swap that no longer moves is not pressure');
  assert.equal(closed({ swapBytes: RETAINED, at: 0 }), false, 'one sample has no rate, and an unknown never closes');
  assert.equal(closed({ swapBytes: RETAINED, at: 0 }, { swapBytes: 0, at: 15_000 }), false, 'a counter that went backwards (a reboot) is unknown');
};

test('the gate closes on the kernel level, PSI or a swap traffic rate, and never on swap a host merely retains', async () => gateScenario(realSrc));

const rateScenario = async (load) => {
  const { gateConfigOf, updateDeviceGate } = await load('device-gates.js');
  const cfg = gateConfigOf('memory', undefined);
  const after = (...readings) => gateAfter(updateDeviceGate, cfg, readings);
  const rate = (...readings) => after(...readings).reading.swapMiBPerSec;
  assert.equal(rate({ swapBytes: null, at: 0 }, { swapBytes: RETAINED, at: 15_000 }), null, 'a recovered counter after a failed read is no rate, not 22 GiB in 15 s');
  assert.equal(rate({ swapBytes: RETAINED, at: 0 }, { swapBytes: null, at: 15_000 }), null);
  assert.equal(rate({ swapBytes: RETAINED, at: null }, { swapBytes: RETAINED, at: 15_000 }), null, 'a missing time is no rate either');
  assert.equal(rate({ psiSomeAvg10: 1, swapBytes: null, at: 0 }, { psiSomeAvg10: null, swapBytes: RETAINED, at: 15_000 }), null, 'switching from PSI to the fallback starts a baseline');
  assert.equal(rate({ swapBytes: 0, at: 0 }, { swapBytes: 5 * MiB, at: 5000 }), 1);
  assert.equal(rate({ swapBytes: 0, at: 0 }, { swapBytes: 100 * MiB, at: 3_600_000 }), null, 'an hour-old baseline would average a burst away: no rate');
  assert.equal(rate({ swapBytes: 0, at: 0 }, { swapBytes: 100 * MiB, at: 3_600_000 }, { swapBytes: 200 * MiB, at: 3_605_000 }), 20, 'the stale sample re-baselines, and the next one decides');
  let unknown;
  for (let n = 0; n < 4; n += 1) unknown = updateDeviceGate(unknown ?? updateDeviceGate(undefined, 'memory', cfg, { psiSomeAvg10: 30 }), 'memory', cfg, { swapBytes: null, at: 15_000 * n });
  assert.deepEqual([unknown.closed, unknown.sensor], [false, 'unavailable'], 'null counters are unknown, so the sensor-unavailable countdown runs');
};

test('a swap rate needs two finite counters and times no more than a minute apart; otherwise the sample is unknown', async () => rateScenario(realSrc));

test('canary: with null counters coerced to 0, a recovered counter fabricates a rate and the scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ['  if (!allFinite(prev?.swapBytes, prev?.at, reading.swapBytes, reading.at)) return null;', '  if (prev === undefined) return null;'], rateScenario);
});

test('canary: without the one-minute window, an hour-old baseline yields a rate and the scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ['ms > 0 && ms <= MAX_RATE_WINDOW_MS && bytes >= 0', 'ms > 0 && bytes >= 0'], rateScenario);
});

test('canary: with the memory violation removed, the gate scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ["      return paging ? 'memory-pressure' : null;", '      return null;'], gateScenario);
});

test('canary: with occupancy (the swap counter itself) read as the signal, retained swap closes the gate and the scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ['    derive: (prev, reading) => ({ ...reading, swapMiBPerSec: swapRate(prev, reading) }),', '    derive: (prev, reading) => ({ ...reading, swapMiBPerSec: Number.isFinite(reading.swapBytes) ? reading.swapBytes / 1048576 : null }),'], gateScenario);
});

const defaultOnScenario = async (load) => {
  const { gateConfigOf, updateDeviceGates, deviceGatesProblem } = await load('device-gates.js');
  const defaults = { maxVmPressureLevel: 1, maxPsiSomeAvg10: 10, maxSwapMiBPerSec: 10 };
  assert.deepEqual(gateConfigOf('memory', undefined), defaults, 'no deviceGates at all');
  assert.deepEqual(gateConfigOf('memory', { thermal: { maxPressureLevel: 1 } }), defaults, 'an overlay that sets only thermal keeps it');
  assert.deepEqual(gateConfigOf('memory', { memory: { maxSwapMiBPerSec: 40 } }), { ...defaults, maxSwapMiBPerSec: 40 });
  assert.equal(gateConfigOf('memory', { memory: false }), null, '"memory": false turns it off');
  assert.equal(gateConfigOf('thermal', undefined), null, 'the other gates stay opt-in');
  assert.deepEqual(Object.keys(updateDeviceGates(undefined, null, {})), ['memory', 'load']);
  assert.deepEqual(Object.keys(updateDeviceGates(undefined, { memory: false, load: false }, {})), []);
  for (const good of [{ memory: false }, { memory: {} }, { memory: { maxVmPressureLevel: 4, maxSwapMiBPerSec: 150, maxPsiSomeAvg10: 0, openSamples: 1 } }]) assert.equal(deviceGatesProblem(good), null, JSON.stringify(good));
  for (const [bad, pattern] of [[{ memory: { maxVmPressureLevel: 0 } }, /maxVmPressureLevel" must be an integer from 1 to 4/], [{ memory: { maxPsiSomeAvg10: 101 } }, /maxPsiSomeAvg10/], [{ memory: { maxOffloadPercent: 60 } }, /"deviceGates.memory" has unknown key "maxOffloadPercent"/], [{ thermal: false }, /"deviceGates.thermal" must be an object/]]) {
    assert.match(deviceGatesProblem(bad) ?? 'accepted', pattern, JSON.stringify(bad));
  }
};

test('memory is default-on and survives an overlay that sets other gates; false turns it off; its thresholds validate by name', async () => defaultOnScenario(realSrc));

test('canary: with the memory defaults ignored, the default-on scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ['  if (own === false || (own === undefined && !defaults)) return null;', '  if (own === false || own === undefined) return null;'], defaultOnScenario);
});

test('hysteresis: one paging sample closes the gate; it reopens only after openSamples clean ones', async () => {
  const { gateConfigOf, updateDeviceGate } = await realSrc('device-gates.js');
  const cfg = gateConfigOf('memory', undefined);
  let state = updateDeviceGate(undefined, 'memory', cfg, { vmPressureLevel: 2 });
  for (const [reading, closed, ok] of [[{ vmPressureLevel: 1 }, true, 1], [{}, true, 1], [{ vmPressureLevel: 1 }, true, 2], [{ vmPressureLevel: 4 }, true, 0], [{ vmPressureLevel: 1 }, true, 1], [{ vmPressureLevel: 1 }, true, 2], [{ vmPressureLevel: 1 }, false, 0]]) {
    state = updateDeviceGate(state, 'memory', cfg, reading);
    assert.deepEqual([state.closed, state.consecutiveOk], [closed, ok], JSON.stringify(reading));
  }
});

const unknownScenario = async (load) => {
  const { gateConfigOf, updateDeviceGate, updateDeviceGates } = await load('device-gates.js');
  const cfg = gateConfigOf('memory', undefined);
  let state = updateDeviceGate(undefined, 'memory', cfg, { vmPressureLevel: 4 });
  assert.equal(state.sensor, 'ok');
  for (const n of [1, 2]) {
    state = updateDeviceGate(state, 'memory', cfg, {});
    assert.deepEqual([state.closed, state.sensor, state.consecutiveUnknown], [true, 'unavailable', n], `unknown sample ${n} holds it`);
  }
  state = updateDeviceGate(state, 'memory', cfg, {});
  assert.deepEqual([state.closed, state.sensor, state.reason], [false, 'unavailable', null], 'the third unknown sample reopens it: unknown never holds claims closed');
  assert.equal(updateDeviceGate(state, 'memory', cfg, { vmPressureLevel: 1 }).sensor, 'ok');
  const thermal = updateDeviceGates(updateDeviceGates(undefined, { thermal: { maxPressureLevel: 1 }, memory: false }, { thermal: { pressureLevel: 3 } }), { thermal: { maxPressureLevel: 1 }, memory: false }, { thermal: {} });
  assert.equal(thermal.thermal.closed, true, 'the bound is the memory gate\'s; thermal keeps holding (BRAIN-1088)');
};

test('a closed memory gate whose sensor reads nothing reopens after 3 unknown samples and reports sensor-unavailable', async () => unknownScenario(realSrc));

test('canary: without the unknown-sample bound, the closed gate latches and the scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ['  if (state.closed && state.consecutiveUnknown >= limit) Object.assign(state, { closed: false, consecutiveOk: 0, reason: null });', ''], unknownScenario);
});

test('local admission reuses a reading for 5 s and derives the swap rate from the state file', async () => {
  const { sampleMemoryGate } = await realSrc('device-gates.js');
  const root = makeTmpDir('memory-gate-root-');
  let reads = 0;
  const read = (reading) => () => { reads += 1; return reading; };
  assert.equal(sampleMemoryGate(root, undefined, { read: read({ swapBytes: 0, at: 0 }), now: 0 }).closed, false);
  assert.equal(sampleMemoryGate(root, undefined, { read: read({ swapBytes: 999 * MiB, at: 4000 }), now: 4000 }).closed, false, 'reused inside 5 s');
  assert.equal(reads, 1);
  const paging = sampleMemoryGate(root, undefined, { read: read({ swapBytes: 200 * MiB, at: 10_000 }), now: 10_000 });
  assert.deepEqual([paging.closed, paging.reading.swapMiBPerSec, paging.openSamples], [true, 20, 3]);
});

const disabledScenario = async (load) => {
  const { sampleMemoryGate, readMemoryGate } = await load('device-gates.js');
  const root = makeTmpDir('memory-gate-off-');
  atomicWriteJson(paths(root).memoryGate, { closed: true, consecutiveOk: 0, reason: 'memory-pressure', sampledAt: Date.now() });
  assert.equal(readMemoryGate(root, { memory: false }).closed, false, 'a disabled gate is never closed, whatever the file says');
  assert.equal(readMemoryGate(root, undefined).closed, true, 'the file still holds while the gate is on');
  assert.equal(sampleMemoryGate(root, { memory: false }).closed, false);
  assert.equal(fs.existsSync(paths(root).memoryGate), false, 'disabling it clears the persisted state');
};

test('"memory": false is never closed: reads ignore the persisted state and sampling clears it', async () => disabledScenario(realSrc));

test('canary: with reads ignoring the config, a disabled gate stays closed and the scenario fails by name', async () => {
  await assertCanaryFails('device-gates.js', ["export const readMemoryGate = (root, deviceGates) => (gateConfigOf('memory', deviceGates) === null ? OPEN_MEMORY_GATE : readJsonSafe(paths(root).memoryGate) ?? OPEN_MEMORY_GATE);", 'export const readMemoryGate = (root) => readJsonSafe(paths(root).memoryGate) ?? OPEN_MEMORY_GATE;'], disabledScenario);
});

test('lane status renders a closed local memory gate and the live memory reading, with a rate against the stored one', async () => {
  const { renderMemoryGateLine } = await realSrc('status.js');
  const { liveDeviceGates, renderDeviceGatesLine } = await realSrc('device-gates.js');
  assert.equal(renderMemoryGateLine({ closed: true, consecutiveOk: 1, openSamples: 3 }), 'memory gate: CLOSED (memory-pressure) — no new local lane starts beside a running one; reopens after 3 clean samples (1 so far)');
  const live = liveDeviceGates(undefined, { memory: { vmPressureLevel: 1, swapBytes: 300 * MiB, at: 10_000 } }, { memory: { swapBytes: 0, at: 0 } });
  assert.equal(renderDeviceGatesLine(live), 'device gates: memory CLOSED (vm pressure 1/≤1, swap 30.0 MiB/s/≤10)');
  assert.equal(renderDeviceGatesLine(liveDeviceGates(undefined, {})), 'device gates: memory ok (readings unknown)');
});

const agentScenario = async (load) => {
  const { startAgent } = await load('agent/run.js');
  const tmp = makeTmpDir('memory-agent-');
  const remoteRoot = path.join(tmp, 'remote');
  const textfileDir = makeTmpDir('memory-agent-prom-');
  const client = { on() {}, end: async () => {}, escapeIdentifier: (s) => `"${s}"`, query: async () => ({ rows: [{ v: { host: 'a1', max_claims: 1, protocol_min: 1 } }] }) };
  class Driver { adoptAll() {} poke() {} watchScope() {} scopeBlock() { return null; } async close() {} }
  const logs = [];
  let sensorDead = false;
  let swapBytes = 0;
  const deviceSampler = async () => {
    if (sensorDead) throw new Error('sysctl gone');
    swapBytes += 1024 * MiB;
    return { thermal: {}, power: {}, memory: { vmPressureLevel: 2, psiSomeAvg10: null, swapBytes, at: Date.now() } };
  };
  const agent = await startAgent({
    remoteRoot, version: 't', s3: {}, Driver, figures: () => null, draining: () => false, log: (line) => logs.push(line), textfileDir, deviceSampler,
    config: { runnerName: 'a1', pollMs: 200, schedule: null, stopClaimingBeforeCloseMs: 0, brokerHome: path.join(tmp, 'home'), stateDir: path.join(tmp, 'state'), pg: {} },
    coreOptions: { connect: async () => client },
  });
  try {
    const { readAgentStatus } = await load('agent/store.js');
    await happens('with no deviceGates configured the agent still pauses claims', () => readAgentStatus(remoteRoot).claims === 'paused:memory-pressure', 5000);
    const prom = () => (fs.existsSync(path.join(textfileDir, 'lane-agent.prom')) ? fs.readFileSync(path.join(textfileDir, 'lane-agent.prom'), 'utf8') : '');
    await happens('the prom file says the memory gate is closed', () => prom().includes('lane_agent_device_gate_closed{gate="memory"} 1'), 5000);
    assert.match(prom(), /^lane_agent_memory_pressure\{signal="vm_pressure_level"\} 2$/m);
    await happens('the prom file carries the swap rate once two samples exist', () => /^lane_agent_memory_pressure\{signal="swap_mib_per_second"\} \d/m.test(prom()), 5000);
    sensorDead = true;
    await happens('a dead sensor reopens the gate after 3 unknown samples and claims resume', () => readAgentStatus(remoteRoot).claims === 'running', 5000);
    await happens('the prom file says sensor-unavailable', () => prom().includes('lane_agent_device_gate_sensor_unavailable{gate="memory"} 1'), 5000);
    assert.equal(readAgentStatus(remoteRoot).deviceGates.memory.sensor, 'unavailable');
    assert.equal(logs.filter((line) => /memory sensor-unavailable/.test(line)).length, 1, 'logged once on the transition');
  } finally {
    await agent.stop();
  }
};

test('the pull agent with no deviceGates config pauses claims on paging, exports the readings, and a dead sensor reopens it as sensor-unavailable', async () => agentScenario(realSrc));

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
    await sleep(5500);
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
