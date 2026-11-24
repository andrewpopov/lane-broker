import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { parseVmStatSwapBytes } from './memory.js';

const finite = (n) => (Number.isFinite(n) ? n : null);
const LINUX_PAGE_BYTES = 4096;
const EXEC_OPTIONS = { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 2000 };

export function parsePsiSomeAvg10(text) {
  const avg10 = /^some\b.*\bavg10=([\d.]+)/m.exec(text ?? '')?.[1];
  return avg10 === undefined ? null : finite(Number(avg10));
}

export function parseProcVmstatSwapBytes(text) {
  const pages = (name) => Number(new RegExp(`^${name} (\\d+)$`, 'm').exec(text ?? '')?.[1]);
  return finite(LINUX_PAGE_BYTES * (pages('pswpin') + pages('pswpout')));
}

const TEST_HOOK = 'LANE_BROKER_TEST_MEMORY_PRESSURE';

function testReading(value, at) {
  const [vmPressureLevel = null, psiSomeAvg10 = null, swapBytes = null] = value.split(',').map((v) => (v.trim() === '' ? null : finite(Number(v))));
  return { vmPressureLevel, psiSomeAvg10, swapBytes, at };
}

const quietly = (read) => {
  try {
    return read();
  } catch {
    return null;
  }
};

// PKG-404: only signals of paging now, never how much is swapped or compressed (BRAIN-273: occupancy outlives the pressure).
// swapBytes is a since-boot counter; the gate derives a rate from two samples. Never throws: an unreadable signal is null.
export function readMemoryPressure({ platform = process.platform, env = process.env, exec = execFileSync, readFile = (f) => fs.readFileSync(f, 'utf8'), now = Date.now() } = {}) {
  if (env[TEST_HOOK] !== undefined) return testReading(env[TEST_HOOK], now);
  if (platform === 'darwin') {
    return {
      vmPressureLevel: quietly(() => finite(Number(exec('sysctl', ['-n', 'kern.memorystatus_vm_pressure_level'], EXEC_OPTIONS).trim()))),
      psiSomeAvg10: null,
      swapBytes: quietly(() => parseVmStatSwapBytes(exec('vm_stat', [], EXEC_OPTIONS))),
      at: now,
    };
  }
  if (platform === 'linux') {
    const psiSomeAvg10 = quietly(() => parsePsiSomeAvg10(readFile('/proc/pressure/memory')));
    return { vmPressureLevel: null, psiSomeAvg10, swapBytes: psiSomeAvg10 === null ? quietly(() => parseProcVmstatSwapBytes(readFile('/proc/vmstat'))) : null, at: now };
  }
  return { vmPressureLevel: null, psiSomeAvg10: null, swapBytes: null, at: now };
}
