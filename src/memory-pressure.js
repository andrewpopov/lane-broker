import fs from 'node:fs';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { readMemorySample } from './memory.js';

const UNKNOWN = Object.freeze({ vmPressureLevel: null, offloadPercent: null, psiSomeAvg10: null });

const finite = (n) => (Number.isFinite(n) ? n : null);
const percentOf = (part, whole) => (Number.isFinite(part) && Number.isFinite(whole) && whole > 0 ? (100 * part) / whole : null);

export function parsePsiSomeAvg10(text) {
  const avg10 = /^some\b.*\bavg10=([\d.]+)/m.exec(text ?? '')?.[1];
  return avg10 === undefined ? null : finite(Number(avg10));
}

export function parseMeminfoOffloadPercent(text) {
  const kb = (name) => Number(new RegExp(`^${name}:\\s+(\\d+) kB`, 'm').exec(text ?? '')?.[1]);
  return percentOf(kb('SwapTotal') - kb('SwapFree'), kb('MemTotal'));
}

const TEST_HOOK = 'LANE_BROKER_TEST_MEMORY_PRESSURE';

function testReading(value) {
  const [vmPressureLevel, offloadPercent, psiSomeAvg10] = value.split(',').map((v) => (v.trim() === '' ? null : finite(Number(v))));
  return { vmPressureLevel: vmPressureLevel ?? null, offloadPercent: offloadPercent ?? null, psiSomeAvg10: psiSomeAvg10 ?? null };
}

const quietly = (read) => {
  try {
    return read();
  } catch {
    return null;
  }
};

const darwinVmPressureLevel = (exec) =>
  finite(Number(exec('sysctl', ['-n', 'kern.memorystatus_vm_pressure_level'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 2000 }).trim()));

function darwinOffloadPercent(sampleMemory) {
  const sample = sampleMemory();
  return percentOf((sample?.swapUsedBytes ?? NaN) + (sample?.compressorBytes ?? 0), sample?.totalMemoryBytes ?? os.totalmem());
}

// PKG-404: offloadPercent is memory pushed out of RAM (swap used, plus the macOS compressor) as a share of RAM; Linux reads it
// only where PSI is missing. Never throws: an unreadable signal is null.
export function readMemoryPressure({ platform = process.platform, env = process.env, exec = execFileSync, readFile = (f) => fs.readFileSync(f, 'utf8'), sampleMemory = readMemorySample } = {}) {
  if (env[TEST_HOOK] !== undefined) return testReading(env[TEST_HOOK]);
  if (platform === 'darwin') {
    return { vmPressureLevel: quietly(() => darwinVmPressureLevel(exec)), offloadPercent: quietly(() => darwinOffloadPercent(sampleMemory)), psiSomeAvg10: null };
  }
  if (platform === 'linux') {
    const psiSomeAvg10 = quietly(() => parsePsiSomeAvg10(readFile('/proc/pressure/memory')));
    return { vmPressureLevel: null, offloadPercent: psiSomeAvg10 === null ? quietly(() => parseMeminfoOffloadPercent(readFile('/proc/meminfo'))) : null, psiSomeAvg10 };
  }
  return { ...UNKNOWN };
}
