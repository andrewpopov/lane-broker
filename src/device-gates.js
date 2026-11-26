import fs from 'node:fs';
import os from 'node:os';
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
    unknownOpenSamples: 3,
    // PKG-404: workbook-pro's week to 2026-11-23: p95 4.85 MiB/s, over 10 MiB/s 2.5% of it; the thrash that day averaged 14.5 (README).
    defaults: { maxVmPressureLevel: 1, maxPsiSomeAvg10: 10, maxSwapMiBPerSec: 10 },
    thresholds: { maxVmPressureLevel: numberIn(1, 4, { integer: true }), maxPsiSomeAvg10: numberIn(0, 100), maxSwapMiBPerSec: numberIn(0.1, 10_000) },
    derive: (prev, reading) => ({ ...reading, swapMiBPerSec: swapRate(prev, reading) }),
    informative: (_cfg, { vmPressureLevel, psiSomeAvg10, swapMiBPerSec }) => known(vmPressureLevel) || known(psiSomeAvg10) || known(swapMiBPerSec),
    violation: (cfg, { vmPressureLevel, psiSomeAvg10, swapMiBPerSec }) => {
      const paging = known(vmPressureLevel) && vmPressureLevel > cfg.maxVmPressureLevel
        || known(psiSomeAvg10) && psiSomeAvg10 > cfg.maxPsiSomeAvg10
        || known(swapMiBPerSec) && swapMiBPerSec > cfg.maxSwapMiBPerSec;
      return paging ? 'memory-pressure' : null;
    },
  },
  // PKG-440: load average alone misleads (zirkbot: load 16 at 56% idle), so the gate closes only when the 5-minute load per core AND
  // the host's busy fraction are both high. Evidence: zirkbot load 29.6 on 12 cores; workbook-pro load 23 on 12 cores, a 2 h suite against a 36-minute median.
  load: {
    defaultOpenSamples: 3,
    unknownOpenSamples: 3,
    idleExempt: true,
    approachingFraction: 0.8,
    defaults: { maxLoadPerCore: 1.2, minBusyFraction: 0.85 },
    thresholds: { maxLoadPerCore: numberIn(0.1, 100), minBusyFraction: numberIn(0.01, 1) },
    informative: (_cfg, { load5PerCore, busyFraction }) => known(load5PerCore) && known(busyFraction),
    violation: (cfg, { load5PerCore, busyFraction }) => (known(load5PerCore) && known(busyFraction) && load5PerCore > cfg.maxLoadPerCore && busyFraction >= cfg.minBusyFraction ? 'load' : null),
  },
};

const MAX_RATE_WINDOW_MS = 60_000;
const allFinite = (...values) => values.every((v) => typeof v === 'number' && Number.isFinite(v));

// PKG-404: no rate unless both samples carry a counter and a time; a baseline older than a minute averages a burst into the gap.
function swapRate(prev, reading) {
  if (!allFinite(prev?.swapBytes, prev?.at, reading.swapBytes, reading.at)) return null;
  const ms = reading.at - prev.at;
  const bytes = reading.swapBytes - prev.swapBytes;
  return ms > 0 && ms <= MAX_RATE_WINDOW_MS && bytes >= 0 ? bytes / (ms / 1000) / (1024 * 1024) : null;
}

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
  reading = GATES[name].derive?.(prev?.reading, reading) ?? reading;
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
  if (GATES[name].unknownOpenSamples) boundUnknown(state, GATES[name].unknownOpenSamples, GATES[name].informative(gateCfg, reading));
  state.reading = reading;
  return state;
}

// PKG-404: a gate that only reads unknown cannot hold claims closed for good; it reopens and says its sensor is unavailable.
function boundUnknown(state, limit, informative) {
  state.consecutiveUnknown = informative ? 0 : (state.consecutiveUnknown ?? 0) + 1;
  state.sensor = informative ? 'ok' : 'unavailable';
  if (state.closed && state.consecutiveUnknown >= limit) Object.assign(state, { closed: false, consecutiveOk: 0, reason: null });
}

export function updateDeviceGates(prev, deviceGates, sample) {
  return Object.fromEntries(configuredGates(deviceGates).map((name) => [name, updateDeviceGate(prev?.[name], name, gateConfigOf(name, deviceGates), sample[name] ?? {})]));
}

const MEMORY_SAMPLE_REUSE_MS = 5000;
const OPEN_MEMORY_GATE = Object.freeze({ closed: false, consecutiveOk: 0, reason: null });

// PKG-404: one state file per host for local admission. A reading under 5 s old is reused, which also keeps the rate window that long.
export function sampleMemoryGate(root, deviceGates, { read = () => readMemoryPressure(), now = Date.now() } = {}) {
  const gateCfg = gateConfigOf('memory', deviceGates);
  if (gateCfg === null) {
    fs.rmSync(paths(root).memoryGate, { force: true });
    return OPEN_MEMORY_GATE;
  }
  const prev = readJsonSafe(paths(root).memoryGate);
  if (prev && now - prev.sampledAt >= 0 && now - prev.sampledAt < MEMORY_SAMPLE_REUSE_MS) return prev;
  const next = { ...updateDeviceGate(prev, 'memory', gateCfg, read()), openSamples: openSamplesOf('memory', gateCfg), sampledAt: now };
  atomicWriteJson(paths(root).memoryGate, next);
  return next;
}

export const readMemoryGate = (root, deviceGates) => (gateConfigOf('memory', deviceGates) === null ? OPEN_MEMORY_GATE : readJsonSafe(paths(root).memoryGate) ?? OPEN_MEMORY_GATE);

// PKG-440: with `idle` (the agent runs nothing) a gate marked `idleExempt` does not block: a runner always keeps one lane.
export const closedGateReason = (states, { idle = false } = {}) => Object.entries(states ?? {}).find(([name, state]) => state.closed && !(idle && GATES[name]?.idleExempt))?.[1].reason ?? null;

// PKG-440: the load reading is within `approachingFraction` of its limit, so a claim round takes one ticket: loadavg5 lags and cannot see this tick's own claims.
export function loadApproaching(states, deviceGates) {
  const gateCfg = gateConfigOf('load', deviceGates);
  const load5PerCore = states?.load?.reading?.load5PerCore;
  return gateCfg !== null && known(load5PerCore) && load5PerCore > gateCfg.maxLoadPerCore * GATES.load.approachingFraction;
}

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

// PKG-440: a whole-host CPU snapshot `{ busy, total }` in ticks, uncapped by any cgroup. On Linux the aggregate `cpu` line of /proc/stat:
// libuv's os.cpus() drops iowait, which would read an I/O-bound window as busy, so iowait and steal count as not-busy but stay in the total.
// Elsewhere os.cpus() (user + nice + sys + irq busy, plus idle).
export function parseProcStatCpu(text) {
  const fields = /^cpu\s+(.+)$/m.exec(text ?? '')?.[1].trim().split(/\s+/).map(Number);
  if (!fields || fields.length < 4 || !fields.every(Number.isFinite)) return null;
  const [user, nice, system, idle, iowait = 0, irq = 0, softirq = 0, steal = 0] = fields; // guest time is already inside user and nice
  return { busy: user + nice + system + irq + softirq, total: user + nice + system + idle + iowait + irq + softirq + steal };
}

export function readCpuSnapshot({ platform = process.platform, readStat = () => fs.readFileSync('/proc/stat', 'utf8'), cpus = os.cpus } = {}) {
  if (platform === 'linux') {
    try {
      const snapshot = parseProcStatCpu(readStat());
      if (snapshot) return snapshot;
    } catch {
      // fall through to os.cpus()
    }
  }
  const sums = cpus().reduce((sum, { times: t }) => ({ busy: sum.busy + t.user + t.nice + t.sys + t.irq, total: sum.total + t.user + t.nice + t.sys + t.irq + t.idle }), { busy: 0, total: 0 });
  return Number.isFinite(sums.total) && sums.total > 0 ? sums : null;
}

const CPU_WINDOW_MS = 250;

// PKG-440: 5-minute loadavg per logical core, and the host busy fraction over two whole-host snapshots about 250 ms apart (busy delta / total delta
// across every logical CPU). It does not use measureHostCpuReadOnly: that caps busy cores at cgroup capacity and runs a synchronous vm_stat.
export async function readCpuLoad({ loadavg = os.loadavg, cores = () => os.cpus().length, platform = process.platform, readSnapshot = () => readCpuSnapshot({ platform }), gapMs = CPU_WINDOW_MS, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)) } = {}) {
  const logical = cores();
  const first = readSnapshot();
  await sleep(gapMs);
  const second = readSnapshot();
  const total = second && first ? second.total - first.total : NaN;
  const busy = second && first ? second.busy - first.busy : NaN;
  return {
    load5PerCore: logical > 0 ? loadavg()[1] / logical : null,
    busyFraction: Number.isFinite(total) && Number.isFinite(busy) && total > 0 && busy >= 0 ? Math.min(1, busy / total) : null,
  };
}

const read = async (sensor, io) => {
  try {
    return (await sensor?.(io)) ?? null;
  } catch {
    return null;
  }
};

// A sensor this platform lacks, no threshold needs, or that fails reads null (unknown). Never throws.
export async function sampleDevice(deviceGates, { platform = process.platform, exec = execCommand, sysRoot = '/sys', memoryPressure = () => readMemoryPressure({ platform }), cpuLoad = readCpuLoad } = {}) {
  const io = { exec, sysRoot, platform };
  const sensors = SENSORS[platform] ?? {};
  const uses = (name, ...keys) => keys.some((key) => gateConfigOf(name, deviceGates)?.[key] !== undefined);
  const [pressureLevel, cpuSpeedLimit, maxTempC, power, memory, load] = await Promise.all([
    uses('thermal', 'maxPressureLevel') ? read(sensors.pressureLevel, io) : null,
    uses('thermal', 'minCpuSpeedLimit') ? read(sensors.cpuSpeedLimit, io) : null,
    uses('thermal', 'maxTempC') ? read(sensors.maxTempC, io) : null,
    uses('power', 'requireAc', 'minBatteryPercent') ? read(sensors.power, io) : null,
    gateConfigOf('memory', deviceGates) ? read(memoryPressure, io) : null,
    gateConfigOf('load', deviceGates) ? read(cpuLoad, io) : null,
  ]);
  return {
    thermal: { pressureLevel, cpuSpeedLimit, maxTempC },
    power: { onAc: power?.onAc ?? null, batteryPercent: power?.batteryPercent ?? null },
    memory: { vmPressureLevel: memory?.vmPressureLevel ?? null, psiSomeAvg10: memory?.psiSomeAvg10 ?? null, swapBytes: memory?.swapBytes ?? null, at: memory?.at ?? null },
    load: { load5PerCore: load?.load5PerCore ?? null, busyFraction: load?.busyFraction ?? null },
  };
}

// `recorded` holds a running agent's own state ({closed}) per gate: it is what the agent enforces, so a gate it still holds closed shows CLOSED
// whatever one fresh reading says (hysteresis); without it the gate is judged by the fresh reading alone.
export function liveDeviceGates(deviceGates, sample, previous = {}, recorded = {}) {
  return Object.fromEntries(configuredGates(deviceGates).map((name) => {
    const reading = GATES[name].derive?.(previous[name], sample[name] ?? {}) ?? sample[name] ?? {};
    const fresh = gateViolation(name, gateConfigOf(name, deviceGates), reading);
    const held = typeof recorded[name]?.closed === 'boolean' ? recorded[name].closed : null;
    return [name, { reason: held === null ? fresh : held ? fresh ?? name : null, reading, thresholds: gateConfigOf(name, deviceGates) }];
  }));
}

const show = (value, format) => (known(value) ? format(value) : 'unknown');

const GATE_PARTS = {
  thermal: ({ pressureLevel, cpuSpeedLimit, maxTempC }, t) => [
    t.maxPressureLevel !== undefined && `pressure ${show(pressureLevel, String)}/≤${t.maxPressureLevel}`,
    t.minCpuSpeedLimit !== undefined && `speed ${show(cpuSpeedLimit, String)}/≥${t.minCpuSpeedLimit}`,
    t.maxTempC !== undefined && `temp ${show(maxTempC, (v) => `${v}C`)}/≤${t.maxTempC}C`,
  ],
  memory: ({ vmPressureLevel, psiSomeAvg10, swapMiBPerSec }, t) => {
    const parts = [
      known(vmPressureLevel) && `vm pressure ${vmPressureLevel}/≤${t.maxVmPressureLevel}`,
      known(psiSomeAvg10) && `psi some avg10 ${psiSomeAvg10}/≤${t.maxPsiSomeAvg10}`,
      known(swapMiBPerSec) && `swap ${swapMiBPerSec.toFixed(1)} MiB/s/≤${t.maxSwapMiBPerSec}`,
    ];
    return parts.some(Boolean) ? parts : ['readings unknown'];
  },
  load: ({ load5PerCore, busyFraction }, t) => [
    `load5/core ${show(load5PerCore, (v) => v.toFixed(2))}/≤${t.maxLoadPerCore}`,
    `busy ${show(busyFraction, (v) => `${Math.round(v * 100)}%`)}/≥${Math.round(t.minBusyFraction * 100)}%`,
  ],
  power: ({ onAc, batteryPercent }, t) => [
    show(onAc, (v) => (v ? 'AC' : 'on battery')),
    t.minBatteryPercent !== undefined && `battery ${show(batteryPercent, (v) => `${v}%`)}`,
  ],
};

// PKG-440: the default-on load gate is left out while it is open with no reading (a host the agent has not sampled), so the line names only gates with something to say.
const silent = (name, { reason, reading }) => name === 'load' && !reason && !known(reading?.load5PerCore) && !known(reading?.busyFraction);

export function renderDeviceGatesLine(live) {
  const gates = Object.entries(live).filter(([name, state]) => !silent(name, state)).map(([name, { reason, reading, thresholds }]) => `${name} ${reason ? 'CLOSED' : 'ok'} (${GATE_PARTS[name](reading, thresholds).filter(Boolean).join(', ')})`);
  return gates.length ? `device gates: ${gates.join(' · ')}` : null;
}
