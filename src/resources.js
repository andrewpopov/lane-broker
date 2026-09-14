import fs from 'node:fs';
import os from 'node:os';
import { execFileSync } from 'node:child_process';

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

/**
 * Parse `vm_stat`'s text into an available-bytes figure. On macOS,
 * `os.freemem()` counts only genuinely free pages — it excludes inactive,
 * speculative, and purgeable pages, all of which macOS deliberately keeps
 * populated with reclaimable data on a healthy machine and reclaims under
 * real pressure. `free + inactive + speculative + purgeable` is the figure
 * that actually reflects what a new process can obtain. The page size is
 * read from the header line rather than assumed (it varies: 4096 on Intel,
 * 16384 on Apple Silicon) — assuming 4096 on a 16384-page-size machine
 * would under-report available memory by 4x. Returns `null` on anything it
 * cannot parse; never throws.
 */
export function parseVmStat(text) {
  const input = String(text ?? '');
  const headerMatch = input.match(/page size of (\d+) bytes/);
  if (!headerMatch) return null;
  const pageSize = Number(headerMatch[1]);
  if (!Number.isFinite(pageSize) || pageSize <= 0) return null;

  const fieldFor = (label) => {
    const match = input.match(new RegExp(`^Pages ${label}:\\s*(\\d+)\\.?\\s*$`, 'm'));
    if (!match) return null;
    const value = Number(match[1]);
    return Number.isFinite(value) && value >= 0 ? value : null;
  };

  const free = fieldFor('free');
  const inactive = fieldFor('inactive');
  const speculative = fieldFor('speculative');
  const purgeable = fieldFor('purgeable');
  if (free === null || inactive === null || speculative === null || purgeable === null) return null;

  return (free + inactive + speculative + purgeable) * pageSize;
}

function detectDarwinAvailableMemory(exec) {
  try {
    const out = exec('vm_stat', [], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 2000,
    });
    return parseVmStat(out);
  } catch {
    return null;
  }
}

/** Resources available to this execution environment. WSL is intentionally
 * treated as Linux: cgroup limits win over host-level values when present. */
export function detectResourceCapacity({
  platform = process.platform,
  parallelism = typeof os.availableParallelism === 'function' ? os.availableParallelism() : os.cpus().length,
  totalMemory = os.totalmem(),
  freeMemory = os.freemem(),
  readFile = fs.readFileSync,
  exec = execFileSync,
} = {}) {
  let cpuCores = Math.max(1, Number(parallelism) || 1);
  let memoryBytes = Math.max(1, Number(totalMemory) || 1);
  let availableMemoryBytes = Math.max(0, Number(freeMemory) || 0);
  const sources = ['os'];

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
  } else if (platform === 'darwin') {
    const vmStatAvailable = detectDarwinAvailableMemory(exec);
    if (vmStatAvailable !== null) {
      availableMemoryBytes = vmStatAvailable;
      sources.push('vm_stat');
    }
    // On any parse/exec failure, availableMemoryBytes stays os.freemem() —
    // wrong on macOS, but a known, already-shipped quantity — and 'source'
    // is left naming only 'os', so it never claims a reading that failed.
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
  // A macOS 'critical' pressure reading is a real, direct signal from the
  // kernel that memory is genuinely short RIGHT NOW — it must win outright,
  // regardless of what the byte arithmetic below says (that arithmetic is
  // itself only ever a best-effort estimate). 'warn' is deliberately NOT
  // a veto here: it fires well before anything is actually short (macOS
  // raises it opportunistically, long before 'critical'), and the byte
  // checks below already account for real headroom via vm_stat — a second,
  // cruder gate on top of that would just re-introduce this same bug
  // (denying everything on a healthy-but-not-idle machine) one step removed.
  if (memoryInfo.macPressure === 'critical') {
    return { admit: false, reason: 'memory-pressure-critical', projectedAvailableBytes: null, memoryBudgetBytes: null };
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
