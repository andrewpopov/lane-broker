import fs from 'node:fs';
import os from 'node:os';
import { execFileSync } from 'node:child_process';

import { parseVmStatAvailable } from './memory.js';

const GIB = 1024 ** 3;

function readText(file, readFile = fs.readFileSync) {
  try {
    return readFile(file, 'utf8').trim();
  } catch {
    return null;
  }
}

export function parseByteSize(value) {
  if (typeof value === 'number') return Number.isFinite(value) && value > 0 ? Math.round(value) : null;
  const match = String(value ?? '').trim().match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb|kib|mib|gib|tib)?$/i);
  if (!match) return null;
  const amount = Number(match[1]);
  const unit = (match[2] || 'b').toLowerCase();
  const multipliers = {
    b: 1,
    kb: 1000,
    mb: 1000 ** 2,
    gb: 1000 ** 3,
    tb: 1000 ** 4,
    kib: 1024,
    mib: 1024 ** 2,
    gib: GIB,
    tib: 1024 ** 4,
  };
  const bytes = amount * multipliers[unit];
  return Number.isFinite(bytes) && bytes > 0 ? Math.round(bytes) : null;
}

export function parseCpuMax(value) {
  const [quotaRaw, periodRaw] = String(value ?? '').trim().split(/\s+/);
  if (!quotaRaw || quotaRaw === 'max') return null;
  const quota = Number(quotaRaw);
  const period = Number(periodRaw);
  return Number.isFinite(quota) && quota > 0 && Number.isFinite(period) && period > 0 ? quota / period : null;
}

export function parseCpuSet(value) {
  const ranges = String(value ?? '').trim().split(',').filter(Boolean);
  let count = 0;
  for (const range of ranges) {
    const match = range.match(/^(\d+)(?:-(\d+))?$/);
    if (!match) return null;
    const start = Number(match[1]);
    const end = match[2] === undefined ? start : Number(match[2]);
    if (end < start) return null;
    count += end - start + 1;
  }
  return count > 0 ? count : null;
}

function finiteCgroupBytes(value) {
  if (!value || value === 'max') return null;
  const bytes = Number(value);
  // cgroup v1 uses enormous sentinel values for "unlimited".
  return Number.isFinite(bytes) && bytes > 0 && bytes < 2 ** 60 ? bytes : null;
}

function cgroupV2Dirs(readFile) {
  const membership = readText('/proc/self/cgroup', readFile);
  const line = membership?.split(/\r?\n/).find((entry) => entry.startsWith('0::'));
  const relative = line ? line.slice(3).replace(/^\/+/, '') : '';
  const parts = relative.split('/').filter(Boolean);
  const dirs = [];
  for (let length = parts.length; length >= 0; length -= 1) {
    const suffix = parts.slice(0, length).join('/');
    dirs.push(suffix ? `/sys/fs/cgroup/${suffix}` : '/sys/fs/cgroup');
  }
  return [...new Set(dirs)];
}

/** Resources available to this execution environment. WSL is intentionally
 * treated as Linux: cgroup limits win over host-level values when present. */
export function detectResourceCapacity({
  platform = process.platform,
  parallelism = typeof os.availableParallelism === 'function' ? os.availableParallelism() : os.cpus().length,
  totalMemory = os.totalmem(),
  freeMemory = os.freemem(),
  readFile = fs.readFileSync,
  vmStatText = null,
  exec = execFileSync,
} = {}) {
  let cpuCores = Math.max(1, Number(parallelism) || 1);
  let memoryBytes = Math.max(1, Number(totalMemory) || 1);
  let availableMemoryBytes = Math.max(0, Number(freeMemory) || 0);
  const sources = ['os'];

  if (platform === 'darwin') {
    // os.freemem() on macOS counts only free pages, excluding
    // inactive/speculative pages that are just as reclaimable (BRAIN-252) —
    // it reads ~1.8GB "free" on a box that's actually ~80% idle. vm_stat's
    // fuller accounting fixes that. Any failure (missing binary, malformed
    // output, timeout, an implausible result) falls back to freeMemory
    // above — never throws, never leaves availableMemoryBytes null on
    // darwin.
    let text = vmStatText;
    if (text == null) {
      try {
        // 2s bound, stdio ignore-stderr: same shape as the pressure probe
        // in cpu.js's readMemoryInfo — a hung vm_stat must never stall a
        // caller sitting inside the scheduler's global lock. killSignal
        // makes the bound hard: execFileSync's default SIGTERM can be
        // caught/ignored and still leave the caller waiting past timeout.
        text = exec('vm_stat', [], {
          encoding: 'utf8',
          stdio: ['ignore', 'pipe', 'ignore'],
          timeout: 2000,
          killSignal: 'SIGKILL',
        });
      } catch {
        text = null; // vm_stat missing/failed: fall back to freeMemory
      }
    }
    const vmStatAvailableBytes = text != null ? parseVmStatAvailable(text) : null;
    // Sanity-bound the parsed result: it must be a positive number and
    // cannot exceed total memory (a vm_stat body that doesn't match the
    // detected page size, or overlapping counters, could otherwise produce
    // a number larger than the machine has).
    if (Number.isFinite(vmStatAvailableBytes) && vmStatAvailableBytes > 0) {
      availableMemoryBytes = Math.min(vmStatAvailableBytes, memoryBytes);
      sources.push('vm_stat');
    }
  }

  if (platform === 'linux') {
    let foundCpu = false;
    let foundCpuSet = false;
    let foundMemory = false;
    for (const dir of cgroupV2Dirs(readFile)) {
      const cpuQuota = parseCpuMax(readText(`${dir}/cpu.max`, readFile));
      const cpuSet = parseCpuSet(readText(`${dir}/cpuset.cpus.effective`, readFile));
      if (cpuQuota !== null) {
        cpuCores = Math.min(cpuCores, cpuQuota);
        foundCpu = true;
      }
      if (cpuSet !== null) {
        cpuCores = Math.min(cpuCores, cpuSet);
        foundCpuSet = true;
      }

      const memoryMax = finiteCgroupBytes(readText(`${dir}/memory.max`, readFile));
      const memoryCurrent = finiteCgroupBytes(readText(`${dir}/memory.current`, readFile)) ?? 0;
      if (memoryMax !== null) {
        memoryBytes = Math.min(memoryBytes, memoryMax);
        availableMemoryBytes = Math.min(availableMemoryBytes, Math.max(0, memoryMax - memoryCurrent));
        foundMemory = true;
      }
    }
    if (foundCpu) sources.push('cgroup-cpu');
    if (foundCpuSet) sources.push('cpuset');
    if (foundMemory) sources.push('cgroup-memory');
  }

  return { cpuCores, memoryBytes, availableMemoryBytes, source: sources.join('+') };
}

export function resolveTicketResources({ weight, cpuCores, memoryBytes, defaultMemoryBytesPerWeight = GIB }) {
  const cpu = cpuCores ?? weight;
  const memory = memoryBytes ?? Math.max(1, Number(weight) || 1) * defaultMemoryBytesPerWeight;
  return { cpuCores: cpu, memoryBytes: Math.round(memory) };
}

export function leaseResources(lease, cfg) {
  return resolveTicketResources({
    weight: lease.weight,
    cpuCores: lease.resources?.cpuCores,
    memoryBytes: lease.resources?.memoryBytes,
    defaultMemoryBytesPerWeight: cfg.defaultMemoryBytesPerWeight,
  });
}

export function effectiveWeightCapacity(cfg, cpuCores) {
  if (cfg.capacity !== 'auto') return cfg.capacity;
  const budget = Math.max(0, cpuCores - (cfg.cpuReserveCores ?? 0));
  return Math.max(1, Math.floor(budget));
}

export function evaluateMemoryAdmission({ memoryInfo, heldLeases, candidateResources, cfg, now = Date.now() }) {
  if (!memoryInfo || !Number.isFinite(memoryInfo.availableBytes)) {
    return { admit: false, reason: 'memory-unavailable', projectedAvailableBytes: null, memoryBudgetBytes: null };
  }
  const growthReserve = heldLeases.reduce((sum, lease) => {
    const requested = leaseResources(lease, cfg).memoryBytes;
    const fresh = Number.isFinite(lease.observedAt) && now - lease.observedAt <= 30_000;
    const observed = fresh && Number.isFinite(lease.observedMemoryBytes) ? lease.observedMemoryBytes : 0;
    return sum + Math.max(0, requested - observed);
  }, 0);
  const projectedAvailableBytes = memoryInfo.availableBytes - growthReserve - candidateResources.memoryBytes;
  const reserveBytes = Number.isFinite(cfg.memoryReserveBytes) ? cfg.memoryReserveBytes : 0;
  const memoryBudgetBytes = Number.isFinite(memoryInfo.totalBytes)
    ? Math.max(0, memoryInfo.totalBytes - reserveBytes)
    : Infinity;
  const reservedBytes = heldLeases.reduce((sum, lease) => sum + leaseResources(lease, cfg).memoryBytes, 0);
  if (reservedBytes + candidateResources.memoryBytes > memoryBudgetBytes) {
    return { admit: false, reason: 'memory-reservations', projectedAvailableBytes, memoryBudgetBytes };
  }
  if (projectedAvailableBytes < reserveBytes) {
    return { admit: false, reason: 'memory-headroom', projectedAvailableBytes, memoryBudgetBytes };
  }
  return { admit: true, reason: 'ok', projectedAvailableBytes, memoryBudgetBytes };
}
