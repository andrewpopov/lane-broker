import fs from 'node:fs';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { atomicWriteJson, fingerprintOf, paths, readJsonSafe } from './state.js';
import { readMemoryPressure } from './memory-pressure.js';

const DEVICE_COMMAND_TIMEOUT_MS = 2000;

const numberIn = (min, max, { integer = false } = {}) => ({
  expects: `${integer ? 'an integer' : 'a number'} from ${min} to ${max}`,
  valid: (v) => typeof v === 'number' && Number.isFinite(v) && v >= min && v <= max && (!integer || Number.isInteger(v)),
});
const flag = { expects: 'true or false', valid: (v) => typeof v === 'boolean' };
const known = (v) => v !== null && v !== undefined;

// One table drives validation, hysteresis and sampling; `violation` answers the block reason a reading earns, or null (unknown never violates).
// `informative` says whether a sample carried anything to judge by: a closed gate holds, neither reopening nor resetting, on one that did not.
const GATES = {
  thermal: {
    defaultOpenSamples: 3,
    thresholds: { maxPressureLevel: numberIn(0, 4, { integer: true }), minCpuSpeedLimit: numberIn(1, 100), maxTempC: numberIn(1, 150) },
    informative: (cfg, { pressureLevel, cpuSpeedLimit, maxTempC }) => (cfg.maxPressureLevel !== undefined && known(pressureLevel)) || (cfg.minCpuSpeedLimit !== undefined && known(cpuSpeedLimit)) || (cfg.maxTempC !== undefined && known(maxTempC)),
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
    informative: (_cfg, { onAc }) => known(onAc),
    violation: (cfg, { onAc, batteryPercent }) => {
      if (onAc !== false) return null;
      if (cfg.requireAc) return 'on-battery';
      return known(batteryPercent) && batteryPercent < cfg.minBatteryPercent ? 'battery-low' : null;
    },
  },
  memory: {
    defaultOpenSamples: 3,
    // PKG-404: on by default. 60% sits between a busy but healthy 48 GB Mac (29-45% of RAM swapped or compressed) and the 2026-11-23 thrash (87%).
    defaults: { maxVmPressureLevel: 1, maxOffloadPercent: 60, maxPsiSomeAvg10: 10 },
    thresholds: { maxVmPressureLevel: numberIn(1, 4, { integer: true }), maxOffloadPercent: numberIn(1, 1000), maxPsiSomeAvg10: numberIn(0, 100) },
    informative: (_cfg, { vmPressureLevel, offloadPercent, psiSomeAvg10 }) => known(vmPressureLevel) || known(offloadPercent) || known(psiSomeAvg10),
    violation: (cfg, { vmPressureLevel, offloadPercent, psiSomeAvg10 }) => {
      const paging = known(vmPressureLevel) && vmPressureLevel > cfg.maxVmPressureLevel
        || (known(psiSomeAvg10) ? psiSomeAvg10 > cfg.maxPsiSomeAvg10 : known(offloadPercent) && offloadPercent > cfg.maxOffloadPercent);
      return paging ? 'memory-pressure' : null;
    },
  },
};

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const openSamplesOf = (name, gateCfg) => gateCfg.openSamples ?? GATES[name].defaultOpenSamples;

export function gateConfigOf(name, deviceGates) {
  const own = deviceGates?.[name];
  const { defaults } = GATES[name];
  if (own === false || (own === undefined && !defaults)) return null;
  return { ...defaults, ...own };
}

export function deviceGatesProblem(deviceGates) {
  if (!isObject(deviceGates)) return '"deviceGates" must be an object';
  for (const [name, gateCfg] of Object.entries(deviceGates)) {
    const gate = Object.hasOwn(GATES, name) ? GATES[name] : null;
    if (!gate) return `"deviceGates" has unknown key "${name}"`;
    const where = `"deviceGates.${name}"`;
    if (gateCfg === false && gate.defaults) continue;
    if (!isObject(gateCfg)) return `${where} must be an object`;
    for (const key of Object.keys(gateCfg)) if (key !== 'openSamples' && !Object.hasOwn(gate.thresholds, key)) return `${where} has unknown key "${key}"`;
    for (const [key, { expects, valid }] of Object.entries(gate.thresholds)) if (gateCfg[key] !== undefined && !valid(gateCfg[key])) return `"deviceGates.${name}.${key}" must be ${expects}`;
    if (gateCfg.openSamples !== undefined && !(Number.isInteger(gateCfg.openSamples) && gateCfg.openSamples > 0)) return `"deviceGates.${name}.openSamples" must be a positive integer`;
    if (!gate.defaults && !Object.keys(gate.thresholds).some((key) => gateCfg[key] !== undefined)) return `${where} needs at least one of ${Object.keys(gate.thresholds).join(', ')}`;
  }
  return null;
}

const configuredGates = (deviceGates) => Object.keys(GATES).filter((name) => gateConfigOf(name, deviceGates) !== null);

const gateViolation = (name, gateCfg, reading = {}) => GATES[name].violation(gateCfg, reading);

// Shaped like `updateGateState` in load.js. An uninformative sample holds the state; a threshold edit restarts the countdown but
// leaves `closed` alone, since it is no evidence the device cooled.
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
  } else if (state.closed && GATES[name].informative(gateCfg, reading)) {
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
  return Object.fromEntries(configuredGates(deviceGates).map((name) => [name, updateDeviceGate(prev?.[name], name, gateConfigOf(name, deviceGates), sample[name] ?? {})]));
}

const MEMORY_SAMPLE_REUSE_MS = 1000;

// PKG-404: one state file per host for local admission; a reading under a second old is reused, so a busy queue samples once a second.
export function sampleMemoryGate(root, deviceGates, { read = readMemoryPressure, now = Date.now() } = {}) {
  const gateCfg = gateConfigOf('memory', deviceGates);
  if (gateCfg === null) return { closed: false, reason: null };
  const prev = readJsonSafe(paths(root).memoryGate);
  if (prev && now - prev.sampledAt >= 0 && now - prev.sampledAt < MEMORY_SAMPLE_REUSE_MS) return prev;
  const next = { ...updateDeviceGate(prev, 'memory', gateCfg, read()), openSamples: openSamplesOf('memory', gateCfg), sampledAt: now };
  atomicWriteJson(paths(root).memoryGate, next);
  return next;
}

export const readMemoryGate = (root) => readJsonSafe(paths(root).memoryGate) ?? { closed: false, consecutiveOk: 0, reason: null };

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

const readNumber = (file) => {
  const text = readText(file);
  const value = text === null || text === '' ? null : Number(text);
  return Number.isFinite(value) ? value : null;
};

function readSysfsPower(sysRoot) {
  const dir = path.join(sysRoot, 'class', 'power_supply');
  const supplies = fs.readdirSync(dir)
    .map((name) => ({ type: readText(path.join(dir, name, 'type')), scope: readText(path.join(dir, name, 'scope')), online: readText(path.join(dir, name, 'online')), capacity: readNumber(path.join(dir, name, 'capacity')) }))
    .filter((s) => s.scope !== 'Device');
  const mains = supplies.filter((s) => s.type === 'Mains');
  const capacities = supplies.filter((s) => s.type === 'Battery' && s.capacity !== null).map((s) => s.capacity);
  const onAc = mains.some((s) => s.online === '1') ? true : mains.length > 0 && mains.every((s) => s.online === '0') ? false : null;
  return { onAc, batteryPercent: capacities.length ? Math.min(...capacities) : null };
}

function readSysfsMaxTempC(sysRoot) {
  const dir = path.join(sysRoot, 'class', 'thermal');
  const temps = fs.readdirSync(dir).filter((name) => name.startsWith('thermal_zone')).map((name) => readNumber(path.join(dir, name, 'temp'))).filter((t) => t !== null);
  return temps.length ? Math.max(...temps) / 1000 : null;
}

const execCommand = (file, args) => new Promise((resolve, reject) => {
  execFile(file, args, { timeout: DEVICE_COMMAND_TIMEOUT_MS, encoding: 'utf8' }, (err, stdout) => (err ? reject(err) : resolve(stdout)));
});

const SENSORS = {
  darwin: {
    pressureLevel: async ({ exec }) => parsePressureLevel(await exec('/usr/bin/notifyutil', ['-g', 'com.apple.system.thermalpressurelevel'])),
    cpuSpeedLimit: async ({ exec }) => parseCpuSpeedLimit(await exec('/usr/bin/pmset', ['-g', 'therm'])),
    power: async ({ exec }) => parsePmsetBatt(await exec('/usr/bin/pmset', ['-g', 'batt'])),
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
export async function sampleDevice(deviceGates, { platform = process.platform, exec = execCommand, sysRoot = '/sys', memoryPressure = () => readMemoryPressure({ platform }) } = {}) {
  const io = { exec, sysRoot };
  const sensors = SENSORS[platform] ?? {};
  const uses = (name, ...keys) => keys.some((key) => gateConfigOf(name, deviceGates)?.[key] !== undefined);
  const [pressureLevel, cpuSpeedLimit, maxTempC, power, memory] = await Promise.all([
    uses('thermal', 'maxPressureLevel') ? read(sensors.pressureLevel, io) : null,
    uses('thermal', 'minCpuSpeedLimit') ? read(sensors.cpuSpeedLimit, io) : null,
    uses('thermal', 'maxTempC') ? read(sensors.maxTempC, io) : null,
    uses('power', 'requireAc', 'minBatteryPercent') ? read(sensors.power, io) : null,
    gateConfigOf('memory', deviceGates) ? read(memoryPressure, io) : null,
  ]);
  return {
    thermal: { pressureLevel, cpuSpeedLimit, maxTempC },
    power: { onAc: power?.onAc ?? null, batteryPercent: power?.batteryPercent ?? null },
    memory: { vmPressureLevel: memory?.vmPressureLevel ?? null, offloadPercent: memory?.offloadPercent ?? null, psiSomeAvg10: memory?.psiSomeAvg10 ?? null },
  };
}

export function liveDeviceGates(deviceGates, sample) {
  return Object.fromEntries(configuredGates(deviceGates).map((name) => [name, { reason: gateViolation(name, gateConfigOf(name, deviceGates), sample[name]), reading: sample[name] ?? {}, thresholds: gateConfigOf(name, deviceGates) }]));
}

const show = (value, format) => (known(value) ? format(value) : 'unknown');

const GATE_PARTS = {
  thermal: ({ pressureLevel, cpuSpeedLimit, maxTempC }, t) => [
    t.maxPressureLevel !== undefined && `pressure ${show(pressureLevel, String)}/≤${t.maxPressureLevel}`,
    t.minCpuSpeedLimit !== undefined && `speed ${show(cpuSpeedLimit, String)}/≥${t.minCpuSpeedLimit}`,
    t.maxTempC !== undefined && `temp ${show(maxTempC, (v) => `${v}C`)}/≤${t.maxTempC}C`,
  ],
  memory: ({ vmPressureLevel, offloadPercent, psiSomeAvg10 }, t) => {
    const parts = [
      known(vmPressureLevel) && `vm pressure ${vmPressureLevel}/≤${t.maxVmPressureLevel}`,
      known(psiSomeAvg10) && `psi some avg10 ${psiSomeAvg10}/≤${t.maxPsiSomeAvg10}`,
      known(offloadPercent) && `swap+compressor ${Math.round(offloadPercent)}% of RAM/≤${t.maxOffloadPercent}%`,
    ];
    return parts.some(Boolean) ? parts : ['readings unknown'];
  },
  power: ({ onAc, batteryPercent }, t) => [
    show(onAc, (v) => (v ? 'AC' : 'on battery')),
    t.minBatteryPercent !== undefined && `battery ${show(batteryPercent, (v) => `${v}%`)}`,
  ],
};

export function renderDeviceGatesLine(live) {
  const gates = Object.entries(live).map(([name, { reason, reading, thresholds }]) => `${name} ${reason ? 'CLOSED' : 'ok'} (${GATE_PARTS[name](reading, thresholds).filter(Boolean).join(', ')})`);
  return `device gates: ${gates.join(' · ')}`;
}
