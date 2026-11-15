import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { ConfigError, loadGlobalConfig } from '../src/config.js';
import { claimBlock } from '../src/agent/claims.js';
import { renderAgentMetrics } from '../src/agent/metrics.js';
import { renderStatusText } from '../src/status.js';
import {
  closedGateReason, deviceGatesProblem, liveDeviceGates, parseCpuSpeedLimit, parsePmsetBatt, parsePressureLevel, renderDeviceGatesLine,
  sampleDevice, updateDeviceGate, updateDeviceGates,
} from '../src/device-gates.js';

const THERMAL_INTEL = `Note: No thermal warning level has been recorded
Note: No performance warning level has been recorded
2026-11-14 14:33:43 -0800 CPU Power notify
\tCPU_Scheduler_Limit \t= 100
\tCPU_Available_CPUs \t= 12
\tCPU_Speed_Limit \t= 100
`;
const THERMAL_ARM = `Note: No thermal warning level has been recorded
Note: No performance warning level has been recorded
Note: No CPU power status has been recorded
`;
const BATT_AC_NOT_CHARGING = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=8257635)\t79%; AC attached; not charging present: true\n";
const BATT_AC_CHARGED = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=23658595)\t100%; charged; 0:00 remaining present: true\n";
const BATT_AC_DEAD = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=6226019)\t1%; AC attached; not charging\n\tDetailed failures: present: true\n";
const BATT_ON_BATTERY = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=8257635)\t42%; discharging; 3:10 remaining present: true\n";
const BATT_DESKTOP = "Now drawing from 'AC Power'\n";

const writeSupply = (root, name, files) => {
  const dir = path.join(root, 'class', 'power_supply', name);
  fs.mkdirSync(dir, { recursive: true });
  for (const [file, value] of Object.entries(files)) fs.writeFileSync(path.join(dir, file), `${value}\n`);
};
const writeZone = (root, name, milli) => {
  const dir = path.join(root, 'class', 'thermal', name);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'temp'), `${milli}\n`);
};

test('parsePressureLevel reads notifyutil output and answers null for anything else', () => {
  assert.equal(parsePressureLevel('com.apple.system.thermalpressurelevel 1\n'), 1);
  assert.equal(parsePressureLevel('com.apple.system.thermalpressurelevel 0\n'), 0);
  assert.equal(parsePressureLevel(''), null);
  assert.equal(parsePressureLevel('notifyutil: no such key'), null);
});

test('parseCpuSpeedLimit reads the Intel pmset therm fixture and is unknown on Apple Silicon', () => {
  assert.equal(parseCpuSpeedLimit(THERMAL_INTEL), 100);
  assert.equal(parseCpuSpeedLimit(THERMAL_INTEL.replace('CPU_Speed_Limit \t= 100', 'CPU_Speed_Limit \t= 62')), 62);
  assert.equal(parseCpuSpeedLimit(THERMAL_ARM), null);
});

test('parsePmsetBatt reads each pmset batt fixture, the battery-power variant and a desktop with no battery', () => {
  assert.deepEqual(parsePmsetBatt(BATT_AC_NOT_CHARGING), { onAc: true, batteryPercent: 79 });
  assert.deepEqual(parsePmsetBatt(BATT_AC_CHARGED), { onAc: true, batteryPercent: 100 });
  assert.deepEqual(parsePmsetBatt(BATT_AC_DEAD), { onAc: true, batteryPercent: 1 });
  assert.deepEqual(parsePmsetBatt(BATT_ON_BATTERY), { onAc: false, batteryPercent: 42 });
  assert.deepEqual(parsePmsetBatt(BATT_DESKTOP), { onAc: true, batteryPercent: null });
  assert.deepEqual(parsePmsetBatt('garbage'), { onAc: null, batteryPercent: null });
});

const ALL = { thermal: { maxPressureLevel: 1, minCpuSpeedLimit: 100, maxTempC: 90 }, power: { requireAc: true, minBatteryPercent: 20 } };

const fakeExec = (answers) => {
  const calls = [];
  const exec = async (file, args) => {
    calls.push([file, ...args].join(' '));
    const answer = answers[[file, ...args].join(' ')];
    if (answer === undefined) throw new Error(`no such command: ${file}`);
    return answer;
  };
  return Object.assign(exec, { calls });
};
const DARWIN = {
  '/usr/bin/notifyutil -g com.apple.system.thermalpressurelevel': 'com.apple.system.thermalpressurelevel 1\n',
  '/usr/bin/pmset -g therm': THERMAL_INTEL,
  '/usr/bin/pmset -g batt': BATT_AC_NOT_CHARGING,
};

test('sampleDevice on darwin runs the macOS commands with no shell and answers every reading it can', async () => {
  const exec = fakeExec(DARWIN);
  const sample = await sampleDevice(ALL, { platform: 'darwin', exec });
  assert.deepEqual(sample, { thermal: { pressureLevel: 1, cpuSpeedLimit: 100, maxTempC: null }, power: { onAc: true, batteryPercent: 79 } });
  assert.deepEqual(exec.calls.sort(), Object.keys(DARWIN).sort());
});

test('sampleDevice skips readers no configured threshold uses', async () => {
  const exec = fakeExec(DARWIN);
  const sample = await sampleDevice({ thermal: { maxPressureLevel: 2 } }, { platform: 'darwin', exec });
  assert.deepEqual(exec.calls, ['/usr/bin/notifyutil -g com.apple.system.thermalpressurelevel']);
  assert.deepEqual(sample, { thermal: { pressureLevel: 1, cpuSpeedLimit: null, maxTempC: null }, power: { onAc: null, batteryPercent: null } });
});

test('a missing command, an Apple Silicon therm output or a throwing reader reads as unknown, never as an error', async () => {
  const arm = await sampleDevice(ALL, { platform: 'darwin', exec: fakeExec({ ...DARWIN, '/usr/bin/pmset -g therm': THERMAL_ARM }) });
  assert.equal(arm.thermal.cpuSpeedLimit, null);
  const missing = await sampleDevice(ALL, { platform: 'darwin', exec: fakeExec({}) });
  assert.deepEqual(missing, { thermal: { pressureLevel: null, cpuSpeedLimit: null, maxTempC: null }, power: { onAc: null, batteryPercent: null } });
});

test('sampleDevice reads macOS commands only on darwin and sysfs only on linux', async () => {
  const exec = fakeExec(DARWIN);
  const linux = await sampleDevice(ALL, { platform: 'linux', exec, sysRoot: makeTmpDir('sysfs-') });
  const win = await sampleDevice(ALL, { platform: 'win32', exec });
  assert.deepEqual(exec.calls, [], 'no macOS command off darwin');
  for (const sample of [linux, win]) assert.deepEqual(sample.thermal, { pressureLevel: null, cpuSpeedLimit: null, maxTempC: null });
  const sysRoot = makeTmpDir('sysfs-');
  writeZone(sysRoot, 'thermal_zone0', 50000);
  const darwin = await sampleDevice(ALL, { platform: 'darwin', exec: fakeExec({}), sysRoot });
  assert.equal(darwin.thermal.maxTempC, null, 'sysfs is not read on darwin');
});

test('sampleDevice reads a WSL-like sysfs: Mains online, the lowest battery, the hottest thermal zone', async () => {
  const sysRoot = makeTmpDir('sysfs-');
  writeSupply(sysRoot, 'AC1', { type: 'Mains', online: 1 });
  writeSupply(sysRoot, 'BAT1', { type: 'Battery', capacity: 97, status: 'Charging' });
  writeSupply(sysRoot, 'BAT0', { type: 'Battery', capacity: 64, status: 'Discharging' });
  writeZone(sysRoot, 'thermal_zone0', 48000);
  writeZone(sysRoot, 'thermal_zone1', 71500);
  const sample = await sampleDevice(ALL, { platform: 'linux', sysRoot });
  assert.deepEqual(sample, { thermal: { pressureLevel: null, cpuSpeedLimit: null, maxTempC: 71.5 }, power: { onAc: true, batteryPercent: 64 } });
  writeSupply(sysRoot, 'AC1', { type: 'Mains', online: 0 });
  assert.equal((await sampleDevice(ALL, { platform: 'linux', sysRoot })).power.onAc, false);
});

test('sysfs: a battery with no capacity file and a Device-scope battery do not read as 0% or 5%, so a minBatteryPercent gate on battery stays open', async () => {
  const sysRoot = makeTmpDir('sysfs-');
  writeSupply(sysRoot, 'AC1', { type: 'Mains', online: 0 });
  writeSupply(sysRoot, 'BAT0', { type: 'Battery', status: 'Unknown' });
  writeSupply(sysRoot, 'hid-mouse', { type: 'Battery', scope: 'Device', capacity: 5 });
  const sample = await sampleDevice({ power: { minBatteryPercent: 20 } }, { platform: 'linux', sysRoot });
  assert.deepEqual(sample.power, { onAc: false, batteryPercent: null });
  assert.equal(updateDeviceGate(undefined, 'power', { minBatteryPercent: 20 }, sample.power).closed, false);
  writeSupply(sysRoot, 'BAT1', { type: 'Battery', capacity: 61 });
  assert.equal((await sampleDevice({ power: { minBatteryPercent: 20 } }, { platform: 'linux', sysRoot })).power.batteryPercent, 61);
});

test('sysfs: Mains is on AC if any entry is online, off only if every entry reads 0, and unknown when an entry is unreadable', async () => {
  const sysRoot = makeTmpDir('sysfs-');
  const onAc = async () => (await sampleDevice({ power: { requireAc: true } }, { platform: 'linux', sysRoot })).power.onAc;
  writeSupply(sysRoot, 'AC1', { type: 'Mains' });
  assert.equal(await onAc(), null, 'a Mains entry with no online file is unknown');
  writeSupply(sysRoot, 'AC2', { type: 'Mains', online: 0 });
  assert.equal(await onAc(), null, 'one unreadable entry among offline ones is still unknown');
  writeSupply(sysRoot, 'AC1', { type: 'Mains', online: 1 });
  assert.equal(await onAc(), true);
  writeSupply(sysRoot, 'AC1', { type: 'Mains', online: 0 });
  assert.equal(await onAc(), false);
});

test('sysfs: an unreadable thermal zone is skipped, not read as 0 degrees', async () => {
  const sysRoot = makeTmpDir('sysfs-');
  fs.mkdirSync(path.join(sysRoot, 'class', 'thermal', 'thermal_zone0'), { recursive: true });
  writeZone(sysRoot, 'thermal_zone1', 55000);
  assert.equal((await sampleDevice({ thermal: { maxTempC: 90 } }, { platform: 'linux', sysRoot })).thermal.maxTempC, 55);
  const only = makeTmpDir('sysfs-');
  fs.mkdirSync(path.join(only, 'class', 'thermal', 'thermal_zone0'), { recursive: true });
  assert.equal((await sampleDevice({ thermal: { maxTempC: 90 } }, { platform: 'linux', sysRoot: only })).thermal.maxTempC, null);
});

test('sampleDevice on a Linux box with no power or thermal entries reads everything as unknown', async () => {
  const sysRoot = makeTmpDir('sysfs-');
  fs.mkdirSync(path.join(sysRoot, 'class', 'power_supply'), { recursive: true });
  const sample = await sampleDevice(ALL, { platform: 'linux', sysRoot });
  assert.deepEqual(sample, { thermal: { pressureLevel: null, cpuSpeedLimit: null, maxTempC: null }, power: { onAc: null, batteryPercent: null } });
});

const THERMAL_GATE = { maxPressureLevel: 1, openSamples: 3 };
const hot = { pressureLevel: 2 };
const cool = { pressureLevel: 0 };
const run = (cfg, name, readings, prev) => readings.reduce((state, reading) => updateDeviceGate(state, name, cfg, reading), prev);

test('a thermal gate closes on the first violating sample and reopens only after openSamples clean ones', () => {
  let state = run(THERMAL_GATE, 'thermal', [cool], undefined);
  assert.equal(state.closed, false);
  state = run(THERMAL_GATE, 'thermal', [hot], state);
  assert.deepEqual([state.closed, state.reason, state.consecutiveOk], [true, 'thermal', 0]);
  state = run(THERMAL_GATE, 'thermal', [cool, cool], state);
  assert.deepEqual([state.closed, state.consecutiveOk], [true, 2]);
  state = run(THERMAL_GATE, 'thermal', [cool], state);
  assert.deepEqual([state.closed, state.consecutiveOk, state.reason], [false, 0, null]);
});

test('a violating sample while counting down restarts the countdown', () => {
  let state = run(THERMAL_GATE, 'thermal', [hot, cool, cool, hot, cool, cool], undefined);
  assert.deepEqual([state.closed, state.consecutiveOk], [true, 2]);
  state = run(THERMAL_GATE, 'thermal', [cool], state);
  assert.equal(state.closed, false);
});

test('a threshold change restarts the countdown and leaves closed alone', () => {
  let state = run(THERMAL_GATE, 'thermal', [hot, cool, cool], undefined);
  assert.equal(state.consecutiveOk, 2);
  const edited = { maxPressureLevel: 1, openSamples: 4 };
  state = run(edited, 'thermal', [cool], state);
  assert.deepEqual([state.closed, state.consecutiveOk], [true, 1], 'two clean samples under the old thresholds do not count');
  state = run(edited, 'thermal', [cool, cool, cool], state);
  assert.equal(state.closed, false);
  const reopenedThenEdited = run({ maxPressureLevel: 0, openSamples: 4 }, 'thermal', [cool], state);
  assert.equal(reopenedThenEdited.closed, false, 'an edit does not close an open gate');
});

test('thermal closes on each configured reading and only on a configured one', () => {
  const cfg = { maxPressureLevel: 1, minCpuSpeedLimit: 100, maxTempC: 90 };
  const closes = (reading) => updateDeviceGate(undefined, 'thermal', cfg, { pressureLevel: 0, cpuSpeedLimit: 100, maxTempC: 60, ...reading }).closed;
  assert.equal(closes({}), false);
  assert.equal(closes({ pressureLevel: 2 }), true);
  assert.equal(closes({ cpuSpeedLimit: 99 }), true);
  assert.equal(closes({ maxTempC: 91 }), true);
  assert.equal(updateDeviceGate(undefined, 'thermal', { maxTempC: 90 }, { pressureLevel: 4, cpuSpeedLimit: 1, maxTempC: 60 }).closed, false, 'an unconfigured reading never closes');
});

test('an unknown reading never closes a gate, and a closed gate holds on one: no reopening, no countdown reset', () => {
  const unknown = { pressureLevel: null, cpuSpeedLimit: null, maxTempC: null };
  assert.equal(updateDeviceGate(undefined, 'thermal', { maxPressureLevel: 0, minCpuSpeedLimit: 100, maxTempC: 1 }, unknown).closed, false);
  assert.equal(updateDeviceGate(undefined, 'power', { requireAc: true, minBatteryPercent: 50 }, { onAc: null, batteryPercent: null }).closed, false);
  assert.equal(updateDeviceGate(undefined, 'power', { minBatteryPercent: 50 }, { onAc: false, batteryPercent: null }).closed, false);
  const cfg = { maxPressureLevel: 1, openSamples: 2 };
  let state = run(cfg, 'thermal', [hot, cool, unknown, unknown, unknown], undefined);
  assert.deepEqual([state.closed, state.consecutiveOk], [true, 1], 'unknown samples neither reopen nor reset the count');
  state = run(cfg, 'thermal', [cool], state);
  assert.equal(state.closed, false);
  const power = run({ requireAc: true }, 'power', [{ onAc: false }, { onAc: null }, { onAc: null }], undefined);
  assert.equal(power.closed, true, 'a power gate holds while onAc is unknown');
  assert.equal(run({ minBatteryPercent: 20 }, 'power', [{ onAc: false, batteryPercent: 5 }, { onAc: null, batteryPercent: 90 }], undefined).closed, true);
});

test('power: requireAc closes on battery; minBatteryPercent closes only on battery and is ignored on AC', () => {
  const power = (cfg, reading, prev) => updateDeviceGate(prev, 'power', cfg, reading);
  assert.deepEqual([power({ requireAc: true }, { onAc: false, batteryPercent: 90 }).closed, power({ requireAc: true }, { onAc: false, batteryPercent: 90 }).reason], [true, 'on-battery']);
  assert.equal(power({ requireAc: true }, { onAc: true, batteryPercent: 90 }).closed, false);
  const low = power({ minBatteryPercent: 20 }, { onAc: false, batteryPercent: 15 });
  assert.deepEqual([low.closed, low.reason], [true, 'battery-low']);
  assert.equal(power({ minBatteryPercent: 20 }, { onAc: false, batteryPercent: 20 }).closed, false);
  assert.equal(power({ requireAc: true, minBatteryPercent: 20 }, { onAc: true, batteryPercent: 1 }).closed, false, 'a dead battery on AC stays open');
  assert.equal(power({ minBatteryPercent: 20 }, { onAc: true, batteryPercent: 1 }).closed, false);
  assert.equal(power({ requireAc: true, minBatteryPercent: 20 }, { onAc: false, batteryPercent: 15 }).reason, 'on-battery');
});

test('power reopens after one clean sample by default and thermal after three', () => {
  let state = updateDeviceGate(undefined, 'power', { requireAc: true }, { onAc: false });
  state = updateDeviceGate(state, 'power', { requireAc: true }, { onAc: true });
  assert.equal(state.closed, false);
  state = run({ maxPressureLevel: 1 }, 'thermal', [hot, cool, cool], undefined);
  assert.equal(state.closed, true);
  assert.equal(run({ maxPressureLevel: 1 }, 'thermal', [cool], state).closed, false);
});

test('updateDeviceGates keeps one state per configured gate and closedGateReason names the closed one', () => {
  const cfg = { thermal: { maxPressureLevel: 1 }, power: { requireAc: true } };
  const cleanSample = { thermal: cool, power: { onAc: true } };
  let states = updateDeviceGates(undefined, cfg, cleanSample);
  assert.deepEqual(Object.keys(states), ['thermal', 'power']);
  assert.equal(closedGateReason(states), null);
  states = updateDeviceGates(states, cfg, { thermal: cool, power: { onAc: false } });
  assert.equal(closedGateReason(states), 'on-battery');
  states = updateDeviceGates(states, { thermal: cfg.thermal }, cleanSample);
  assert.deepEqual(Object.keys(states), ['thermal']);
  assert.equal(closedGateReason(undefined), null);
});

test('claimBlock returns thermal, on-battery or battery-low once a gate is closed, after draining and the schedule', () => {
  const base = { schedule: null, stopClaimingBeforeCloseMs: 0, draining: () => false };
  const closedBy = (name, gateCfg, reading) => ({ [name]: updateDeviceGate(undefined, name, gateCfg, reading) });
  assert.equal(claimBlock({ ...base, deviceGates: closedBy('thermal', { maxPressureLevel: 1 }, hot) }), 'thermal');
  assert.equal(claimBlock({ ...base, deviceGates: closedBy('power', { requireAc: true }, { onAc: false }) }), 'on-battery');
  assert.equal(claimBlock({ ...base, deviceGates: closedBy('power', { minBatteryPercent: 20 }, { onAc: false, batteryPercent: 5 }) }), 'battery-low');
  assert.equal(claimBlock({ ...base, deviceGates: closedBy('thermal', { maxPressureLevel: 1 }, cool) }), null);
  assert.equal(claimBlock(base), null);
  assert.equal(claimBlock({ ...base, draining: () => true, deviceGates: closedBy('thermal', { maxPressureLevel: 1 }, hot) }), 'draining');
});

test('a good deviceGates config is accepted, with every key and gate optional', () => {
  for (const good of [{}, { thermal: { maxPressureLevel: 0 } }, { power: { requireAc: true } }, { thermal: { maxTempC: 150, openSamples: 1 } }, { power: { minBatteryPercent: 0 } }, ALL]) {
    assert.equal(deviceGatesProblem(good), null, JSON.stringify(good));
  }
});

test('deviceGates rejects each bad type and range, a gate with no threshold, and unknown keys, naming the key', () => {
  const bad = [
    [[], /"deviceGates" must be an object/],
    [{ fan: {} }, /"deviceGates" has unknown key "fan"/],
    [{ thermal: 'hot' }, /"deviceGates.thermal" must be an object/],
    [{ thermal: { maxPressureLevel: 1, colour: 1 } }, /"deviceGates.thermal" has unknown key "colour"/],
    [{ power: { requireAc: true, maxTempC: 3 } }, /"deviceGates.power" has unknown key "maxTempC"/],
    [{ thermal: { maxPressureLevel: 5 } }, /"deviceGates.thermal.maxPressureLevel" must be an integer from 0 to 4/],
    [{ thermal: { maxPressureLevel: 1.5 } }, /maxPressureLevel/],
    [{ thermal: { minCpuSpeedLimit: 0 } }, /"deviceGates.thermal.minCpuSpeedLimit" must be a number from 1 to 100/],
    [{ thermal: { minCpuSpeedLimit: 101 } }, /minCpuSpeedLimit/],
    [{ thermal: { maxTempC: 151 } }, /"deviceGates.thermal.maxTempC" must be a number from 1 to 150/],
    [{ thermal: { maxTempC: '90' } }, /maxTempC/],
    [{ power: { requireAc: 'yes' } }, /"deviceGates.power.requireAc" must be true or false/],
    [{ power: { minBatteryPercent: 101 } }, /"deviceGates.power.minBatteryPercent" must be a number from 0 to 100/],
    [{ power: { minBatteryPercent: -1 } }, /minBatteryPercent/],
    [{ thermal: { maxTempC: 90, openSamples: 0 } }, /"deviceGates.thermal.openSamples" must be a positive integer/],
    [{ power: { requireAc: true, openSamples: 1.5 } }, /openSamples/],
    [{ thermal: { maxPressureLevel: 1, constructor: 123 } }, /"deviceGates.thermal" has unknown key "constructor"/],
    [{ constructor: {} }, /"deviceGates" has unknown key "constructor"/],
    [{ thermal: { maxPressureLevel: 1, toString: 1 } }, /unknown key "toString"/],
    [{ thermal: {} }, /"deviceGates.thermal" needs at least one of/],
    [{ power: { openSamples: 2 } }, /"deviceGates.power" needs at least one of/],
  ];
  for (const [config, pattern] of bad) assert.match(deviceGatesProblem(config) ?? 'accepted', pattern, JSON.stringify(config));
});

test('loadGlobalConfig carries a valid deviceGates block through and refuses a bad one with the file in the message', () => {
  const { home, state } = freshEnv();
  const saved = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeGlobalConfig(home, {});
    assert.equal(loadGlobalConfig({ home }).deviceGates, undefined, 'absent means off');
    writeGlobalConfig(home, { deviceGates: ALL });
    assert.deepEqual(loadGlobalConfig({ home }).deviceGates, ALL);
    writeGlobalConfig(home, { deviceGates: { thermal: { maxPressureLevel: 9 } } });
    assert.throws(() => loadGlobalConfig({ home }), (err) => err instanceof ConfigError && err.message.includes('config.json') && /maxPressureLevel/.test(err.message));
  } finally {
    for (const [key, name] of [['home', 'LANE_BROKER_HOME'], ['state', 'LANE_BROKER_STATE']]) {
      if (saved[key] === undefined) delete process.env[name];
      else process.env[name] = saved[key];
    }
  }
});

const metricLines = (deviceGates) => renderAgentMetrics({ now: 0, records: [], claimed: 0, claimAttempts: {}, listening: true, registered: true, scopeAvailable: true, version: 't', deviceGates }).split('\n');

test('the agent metrics carry lane_agent_device_gate_closed for configured gates only', () => {
  assert.equal(metricLines(undefined).some((line) => line.includes('device_gate')), false);
  const states = updateDeviceGates(undefined, { thermal: { maxPressureLevel: 1 }, power: { requireAc: true } }, { thermal: hot, power: { onAc: true } });
  const lines = metricLines(states);
  assert.ok(lines.includes('lane_agent_device_gate_closed{gate="thermal"} 1'));
  assert.ok(lines.includes('lane_agent_device_gate_closed{gate="power"} 0'));
  const thermalOnly = metricLines(updateDeviceGates(undefined, { thermal: { maxPressureLevel: 1 } }, { thermal: cool }));
  assert.equal(thermalOnly.some((line) => line.includes('gate="power"')), false);
});

test('lane status prints one device gates line with each reading against its thresholds, and none when unconfigured', () => {
  const cfg = { thermal: { maxPressureLevel: 1, minCpuSpeedLimit: 100 }, power: { requireAc: true, minBatteryPercent: 20 } };
  const live = liveDeviceGates(cfg, { thermal: { pressureLevel: 0, cpuSpeedLimit: 100, maxTempC: null }, power: { onAc: true, batteryPercent: 79 } });
  assert.equal(renderDeviceGatesLine(live), 'device gates: thermal ok (pressure 0/≤1, speed 100/≥100) · power ok (AC, battery 79%)');
  const closed = liveDeviceGates(cfg, { thermal: { pressureLevel: 2, cpuSpeedLimit: null }, power: { onAc: false, batteryPercent: null } });
  assert.equal(renderDeviceGatesLine(closed), 'device gates: thermal CLOSED (pressure 2/≤1, speed unknown/≥100) · power CLOSED (on battery, battery unknown)');
  const status = { capacity: 4, used: 0, paused: null, configWarning: null, headBlock: null, loadGate: { closed: false, lastLoad: 1, sampleAgeMs: 5, consecutiveUnder: 0, loadClose: 15, loadOpen: 11, loadOpenSamples: 3 }, running: [], queued: [] };
  const lines = renderStatusText({ ...status, deviceGates: live }).split('\n').filter((line) => line.startsWith('device gates:'));
  assert.equal(lines.length, 1);
  assert.equal(renderStatusText(status).includes('device gates'), false);
});
