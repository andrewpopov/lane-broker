import fs from 'node:fs';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { fingerprintOf } from './state.js';

export const DEVICE_COMMAND_TIMEOUT_MS = 2000;

const numberIn = (min, max, { integer = false } = {}) => ({
  expects: `${integer ? 'an integer' : 'a number'} from ${min} to ${max}`,
  valid: (v) => typeof v === 'number' && Number.isFinite(v) && v >= min && v <= max && (!integer || Number.isInteger(v)),
});
const flag = { expects: 'true or false', valid: (v) => typeof v === 'boolean' };
const known = (v) => v !== null && v !== undefined;

// One table drives validation, hysteresis and sampling; `violation` answers the block reason a reading earns, or null (unknown never violates).
const GATES = {
  thermal: {
    defaultOpenSamples: 3,
    thresholds: { maxPressureLevel: numberIn(0, 4, { integer: true }), minCpuSpeedLimit: numberIn(1, 100), maxTempC: numberIn(1, 150) },
    violation: (cfg, { pressureLevel, cpuSpeedLimit, maxTempC }) => {
      const hot = known(pressureLevel) && pressureLevel > cfg.maxPressureLevel
        || known(cpuSpeedLimit) && cpuSpeedLimit < cfg.minCpuSpeedLimit
        || known(maxTempC) && maxTempC > cfg.maxTempC;
      return hot ? 'thermal' : null;
    },
  },
  power: {
    defaultOpenSamples: 1,
    thresholds: { requireAc: flag, minBatteryPercent: numberIn(0, 100) },
    violation: (cfg, { onAc, batteryPercent }) => {
      if (onAc !== false) return null;
      if (cfg.requireAc) return 'on-battery';
      return known(batteryPercent) && batteryPercent < cfg.minBatteryPercent ? 'battery-low' : null;
    },
  },
};

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const openSamplesOf = (name, gateCfg) => gateCfg.openSamples ?? GATES[name].defaultOpenSamples;

export function deviceGatesProblem(deviceGates) {
  if (!isObject(deviceGates)) return '"deviceGates" must be an object';
  for (const [name, gateCfg] of Object.entries(deviceGates)) {
    const gate = GATES[name];
    if (!gate) return `"deviceGates" has unknown key "${name}"`;
    const where = `"deviceGates.${name}"`;
    if (!isObject(gateCfg)) return `${where} must be an object`;
    for (const key of Object.keys(gateCfg)) if (key !== 'openSamples' && !gate.thresholds[key]) return `${where} has unknown key "${key}"`;
    for (const [key, { expects, valid }] of Object.entries(gate.thresholds)) if (gateCfg[key] !== undefined && !valid(gateCfg[key])) return `"deviceGates.${name}.${key}" must be ${expects}`;
    if (gateCfg.openSamples !== undefined && !(Number.isInteger(gateCfg.openSamples) && gateCfg.openSamples > 0)) return `"deviceGates.${name}.openSamples" must be a positive integer`;
    if (!Object.keys(gate.thresholds).some((key) => gateCfg[key] !== undefined)) return `${where} needs at least one of ${Object.keys(gate.thresholds).join(', ')}`;
  }
  return null;
}

const configuredGates = (deviceGates) => Object.keys(GATES).filter((name) => deviceGates?.[name] !== undefined);

export const gateViolation = (name, gateCfg, reading) => GATES[name].violation(gateCfg, reading);

// Shaped like `updateGateState` in load.js. An unknown reading counts as a sample without a violation; a threshold edit restarts the
// countdown but leaves `closed` alone, since it is no evidence the device cooled.
export function updateDeviceGate(prev, name, gateCfg, reading) {
  const openSamples = openSamplesOf(name, gateCfg);
  const fingerprint = fingerprintOf(...Object.keys(GATES[name].thresholds).map((key) => gateCfg[key] ?? null), openSamples);
  const state = prev ? { ...prev } : { closed: false, consecutiveOk: 0, reason: null };
  if (state.fingerprint !== fingerprint) {
    state.consecutiveOk = 0;
    state.fingerprint = fingerprint;
  }
  const reason = gateViolation(name, gateCfg, reading);
  if (reason) {
    state.closed = true;
    state.consecutiveOk = 0;
    state.reason = reason;
  } else if (state.closed) {
    state.consecutiveOk += 1;
    if (state.consecutiveOk >= openSamples) {
      state.closed = false;
      state.consecutiveOk = 0;
      state.reason = null;
    }
  }
  state.reading = reading;
  return state;
}

export function updateDeviceGates(prev, deviceGates, sample) {
  return Object.fromEntries(configuredGates(deviceGates).map((name) => [name, updateDeviceGate(prev?.[name], name, deviceGates[name], sample[name])]));
}

export const closedGateReason = (states) => Object.values(states ?? {}).find((state) => state.closed)?.reason ?? null;

export const parsePressureLevel = (text) => {
  const level = /thermalpressurelevel\s+(\d+)/.exec(text)?.[1];
  return level === undefined ? null : Number(level);
};

export const parseCpuSpeedLimit = (text) => {
  const limit = /CPU_Speed_Limit\s*=\s*(\d+)/.exec(text)?.[1];
  return limit === undefined ? null : Number(limit);
};

export function parsePmsetBatt(text) {
  const source = /Now drawing from '([^']+)'/.exec(text)?.[1];
  const percent = /InternalBattery[^\n]*?(\d+)%/.exec(text)?.[1];
  return { onAc: source === undefined ? null : source === 'AC Power', batteryPercent: percent === undefined ? null : Number(percent) };
}

const readText = (file) => {
  try {
    return fs.readFileSync(file, 'utf8').trim();
  } catch {
    return null;
  }
};

export function readSysfsPower(sysRoot) {
  const dir = path.join(sysRoot, 'class', 'power_supply');
  const supplies = fs.readdirSync(dir).map((name) => ({ type: readText(path.join(dir, name, 'type')), online: readText(path.join(dir, name, 'online')), capacity: Number(readText(path.join(dir, name, 'capacity'))) }));
  const mains = supplies.filter((s) => s.type === 'Mains');
  const capacities = supplies.filter((s) => s.type === 'Battery' && Number.isFinite(s.capacity)).map((s) => s.capacity);
  return { onAc: mains.length ? mains.some((s) => s.online === '1') : null, batteryPercent: capacities.length ? Math.min(...capacities) : null };
}

export function readSysfsMaxTempC(sysRoot) {
  const dir = path.join(sysRoot, 'class', 'thermal');
  const temps = fs.readdirSync(dir).filter((name) => name.startsWith('thermal_zone')).map((name) => Number(readText(path.join(dir, name, 'temp')))).filter((t) => Number.isFinite(t));
  return temps.length ? Math.max(...temps) / 1000 : null;
}

export const execCommand = (file, args) => new Promise((resolve, reject) => {
  execFile(file, args, { timeout: DEVICE_COMMAND_TIMEOUT_MS, encoding: 'utf8' }, (err, stdout) => (err ? reject(err) : resolve(stdout)));
});

const SENSORS = {
  darwin: {
    pressureLevel: async ({ exec }) => parsePressureLevel(await exec('notifyutil', ['-g', 'com.apple.system.thermalpressurelevel'])),
    cpuSpeedLimit: async ({ exec }) => parseCpuSpeedLimit(await exec('pmset', ['-g', 'therm'])),
    power: async ({ exec }) => parsePmsetBatt(await exec('pmset', ['-g', 'batt'])),
  },
  linux: {
    maxTempC: async ({ sysRoot }) => readSysfsMaxTempC(sysRoot),
    power: async ({ sysRoot }) => readSysfsPower(sysRoot),
  },
};

const read = async (sensor, io) => {
  try {
    return (await sensor?.(io)) ?? null;
  } catch {
    return null;
  }
};

// A sensor this platform lacks, no threshold needs, or that fails reads null (unknown). Never throws.
export async function sampleDevice(deviceGates, { platform = process.platform, exec = execCommand, sysRoot = '/sys' } = {}) {
  const io = { exec, sysRoot };
  const sensors = SENSORS[platform] ?? {};
  const uses = (name, ...keys) => keys.some((key) => deviceGates?.[name]?.[key] !== undefined);
  const [pressureLevel, cpuSpeedLimit, maxTempC, power] = await Promise.all([
    uses('thermal', 'maxPressureLevel') ? read(sensors.pressureLevel, io) : null,
    uses('thermal', 'minCpuSpeedLimit') ? read(sensors.cpuSpeedLimit, io) : null,
    uses('thermal', 'maxTempC') ? read(sensors.maxTempC, io) : null,
    uses('power', 'requireAc', 'minBatteryPercent') ? read(sensors.power, io) : null,
  ]);
  return { thermal: { pressureLevel, cpuSpeedLimit, maxTempC }, power: { onAc: power?.onAc ?? null, batteryPercent: power?.batteryPercent ?? null } };
}

export function liveDeviceGates(deviceGates, sample) {
  return Object.fromEntries(configuredGates(deviceGates).map((name) => [name, { reason: gateViolation(name, deviceGates[name], sample[name]), reading: sample[name], thresholds: deviceGates[name] }]));
}

const show = (value, format) => (known(value) ? format(value) : 'unknown');

const GATE_PARTS = {
  thermal: ({ pressureLevel, cpuSpeedLimit, maxTempC }, t) => [
    t.maxPressureLevel !== undefined && `pressure ${show(pressureLevel, String)}/≤${t.maxPressureLevel}`,
    t.minCpuSpeedLimit !== undefined && `speed ${show(cpuSpeedLimit, String)}/≥${t.minCpuSpeedLimit}`,
    t.maxTempC !== undefined && `temp ${show(maxTempC, (v) => `${v}C`)}/≤${t.maxTempC}C`,
  ],
  power: ({ onAc, batteryPercent }, t) => [
    show(onAc, (v) => (v ? 'AC' : 'on battery')),
    t.minBatteryPercent !== undefined && `battery ${show(batteryPercent, (v) => `${v}%`)}`,
  ],
};

export function renderDeviceGatesLine(live) {
  const gates = Object.entries(live).map(([name, { reason, reading, thresholds }]) => `${name} ${reason ? 'CLOSED' : 'ok'} (${GATE_PARTS[name](reading, thresholds).filter(Boolean).join(', ')})`);
  return `device gates: ${gates.join(' · ')}`;
}
