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

/**
 * `minCpuCores` (BRAIN-360) is the floor an elastic lane accepts; it only
 * appears on the result when it is a real floor, strictly below the claim, so
 * a lane without one resolves to exactly the shape it always had.
 */
export function resolveTicketResources({ weight, cpuCores, minCpuCores, memoryBytes, defaultMemoryBytesPerWeight = GIB }) {
  const cpu = cpuCores ?? weight;
  const memory = memoryBytes ?? Math.max(1, Number(weight) || 1) * defaultMemoryBytesPerWeight;
  const elastic = Number.isFinite(minCpuCores) && minCpuCores > 0 && minCpuCores < cpu;
  return { cpuCores: cpu, ...(elastic ? { minCpuCores } : {}), memoryBytes: Math.round(memory) };
}

/**
 * BRAIN-360: the CPU cores a held lease is CHARGED for -- the elastic grant when
 * admission gave it less than it declared, else its declared claim. Every place
 * that charges a lease's CPU (reservedSum, settled demand, status, a nested run's
 * widening check) reads this, never `lease.resources.cpuCores` directly.
 */
export function leaseCpuCores(lease) {
  return Number.isFinite(lease.grantedCpuCores) ? lease.grantedCpuCores : lease.resources?.cpuCores;
}

/**
 * BRAIN-425: the fields EVERY terminal history row carries, whichever path wrote it: what the run was charged
 * (the elastic grant, else the declaration; read from a lease, ticket or attempt record, anything with `resources`)
 * and `exit`/`signal`, null until the caller's own result says otherwise. Spread it BEFORE the row's own fields.
 */
export function terminalRowDefaults(source) {
  const cpu = leaseCpuCores(source ?? {});
  const memory = source?.resources?.memoryBytes;
  return {
    ...(Number.isFinite(cpu) ? { grantedCpuCores: cpu } : {}),
    ...(Number.isFinite(memory) ? { grantedMemoryBytes: memory } : {}),
    exit: null,
    signal: null,
  };
}

/** remote-probe capability: this runner resolves `minCpuCores` itself and admits at a partial claim. */
export const ELASTIC_CLAIMS_CAPABILITY = 'elastic-claims/1';

/**
 * BRAIN-360: the integer claims an elastic lane may be admitted at when its full `cpuCores` does not
 * fit: `{ hi, lo }`, tried downward from `hi` to `lo`. Grants are integers only: `hi` is the largest
 * integer strictly below the claim that also fits the CPU headroom the full-claim evaluation measured
 * (`headroom` = budget minus everything already projected busy), `lo` is `ceil(minCpuCores)`, so a
 * fractional floor rounds UP and a grant is never below it. The headroom bound is what keeps the walk
 * short whatever the declared claim is. Null when there is no floor or nothing in range.
 */
export function elasticClaimRange({ cpuCores, minCpuCores, headroom }) {
  if (!Number.isFinite(minCpuCores) || !Number.isFinite(cpuCores) || !Number.isFinite(headroom) || !(minCpuCores < cpuCores)) return null;
  const hi = Math.min(Math.ceil(cpuCores) - 1, Math.floor(headroom));
  const lo = Math.ceil(minCpuCores);
  return hi >= lo ? { hi, lo } : null;
}

/**
 * BRAIN-362: an elastic ticket (`resources.minCpuCores`) is never evaluated, charged or granted above
 * what this host's CPU budget can hold: its claim is capped at floor(budget) up front, so every admission
 * path (full fit, a cold sample, the idle exemption, ...) sees the same capped claim and the grant,
 * lease, history and the child's LANE_BROKER_CPU_CORES can never exceed it. A cap that reaches the floor
 * leaves no floor (the claim is then simply that many cores). Only in active mode, where the budget is
 * enforced (checkResourceBudget); a non-elastic claim over budget is refused up front, never capped.
 */
export function capElasticClaim(ticket, cfg, host = detectResourceCapacity()) {
  const min = ticket.resources?.minCpuCores;
  if (cfg.schedulerMode !== 'active' || !Number.isFinite(min)) return ticket;
  const cap = Math.floor(cpuBudgetCores(host, cfg));
  if (!(ticket.resources.cpuCores > cap)) return ticket;
  // A budget that shrank below the floor (or to zero) must never produce a grant under the minimum: leave the ticket as declared;
  // admission denies it (elasticBelowFloor) so it waits like any over-budget claim instead of being admitted below what it asked to run with.
  if (cap < 1 || cap < Math.ceil(min)) return ticket;
  const { minCpuCores, ...claim } = ticket.resources;
  return { ...ticket, resources: { ...claim, cpuCores: cap, ...(minCpuCores < cap ? { minCpuCores } : {}) } };
}

/**
 * BRAIN-362: an elastic ticket whose floor, rounded up, no longer fits floor(current CPU budget) (the budget shrank after
 * preflight). Admission denies it before any exemption (cold sample, idle overshoot) could grant it its full claim;
 * it keeps waiting, since the budget may grow back. Active mode only, like the cap.
 */
export function elasticBelowFloor(ticket, cfg, host = detectResourceCapacity()) {
  const min = ticket.resources?.minCpuCores;
  if (cfg.schedulerMode !== 'active' || !Number.isFinite(min)) return false;
  const cap = Math.floor(cpuBudgetCores(host, cfg));
  return cap < 1 || cap < Math.ceil(min);
}

export function leaseResources(lease, cfg) {
  return resolveTicketResources({
    weight: lease.weight,
    cpuCores: leaseCpuCores(lease),
    memoryBytes: lease.resources?.memoryBytes,
    defaultMemoryBytesPerWeight: cfg.defaultMemoryBytesPerWeight,
  });
}

export function effectiveWeightCapacity(cfg, cpuCores) {
  if (cfg.capacity !== 'auto') return cfg.capacity;
  const budget = Math.max(0, cpuCores - (cfg.cpuReserveCores ?? 0));
  return Math.max(1, Math.floor(budget));
}

/**
 * The CPU core budget for admission on `host` under `globalCfg`: the smaller
 * of "all cores minus the reserve" and "cpuAdmissionPercent of all cores".
 * Extracted so every caller that needs a runner's/host's static CPU budget
 * (`checkResourceBudget` below, and BRAIN-320 S1a's `remote-probe` capacity
 * report) shares this one formula instead of each re-deriving it.
 */
export function cpuBudgetCores(host, globalCfg) {
  return Math.max(
    0,
    Math.min(host.cpuCores - globalCfg.cpuReserveCores, (globalCfg.cpuAdmissionPercent / 100) * host.cpuCores),
  );
}

/**
 * The "requested resources exceed this environment's budget" refusal,
 * extracted so `run.js` and the supervisor's remote-fallback path (BRAIN-319
 * T3b) apply the exact same message and exit code -- a request that would
 * have been refused locally must still be refused once it falls back from a
 * remote runner, not silently admitted because the check only ran once.
 */
export function checkResourceBudget({ resources, globalCfg, host }) {
  const cpuBudget = cpuBudgetCores(host, globalCfg);
  const memoryBudget = Math.max(0, host.memoryBytes - globalCfg.memoryReserveBytes);
  // An elastic claim is granted in whole cores, so its smallest grant is ceil(minCpuCores) and the most it can
  // ever get is floor(budget): a fractional floor whose ceiling passes the budget could never start (BRAIN-362).
  const elasticFloor = Number.isFinite(resources.minCpuCores);
  const smallestClaim = elasticFloor ? Math.ceil(resources.minCpuCores) : resources.cpuCores;
  const grantableBudget = elasticFloor ? Math.floor(cpuBudget) : cpuBudget;
  if (globalCfg.schedulerMode === 'active' && (smallestClaim > grantableBudget || resources.memoryBytes > memoryBudget)) {
    return {
      ok: false,
      exitCode: 64,
      message:
        `lane run: requested resources exceed this environment's budget (` +
        `${smallestClaim}/${cpuBudget.toFixed(2)} CPU cores, ` +
        `${resources.memoryBytes}/${memoryBudget} memory bytes).\n`,
    };
  }
  return { ok: true };
}

/** Exit code of every `localRefused` policy refusal (BRAIN-320). */
export const LOCAL_REFUSED_EXIT_CODE = 69;
/** BRAIN-455: a remote-only lane found no usable runner right now. Retryable, unlike the policy refusal above. */
export const NO_RUNNER_EXIT_CODE = 76;

/**
 * The "this lane is refused for local runs by default" refusal, extracted
 * (BRAIN-320) so `run.js`'s immediate refusal and the supervisor's
 * remote-fallback refusal (a `localRefused` lane whose remote attempt fell
 * back to local, and `--allow-local-sim` was never passed) apply the exact
 * same message and exit code -- one refusal path per reason, same pattern
 * as `checkResourceBudget` above.
 *
 * `laneClass` gates the DSN hint: ROUGE_FLEET_SUBMIT_DSN is rouge's sim-fleet
 * submit DSN, so only a `sim` lane is pointed at it.
 */
export function localSimRefusal(lane, laneClass) {
  // BRAIN-382: name the variable, never print its value -- it is a DSN with a password, and this
  // message lands in agent transcripts.
  let submitHint = '';
  if (laneClass === 'sim') {
    submitHint = process.env.ROUGE_FLEET_SUBMIT_DSN
      ? ' (submit to the fleet instead, via the DSN in ROUGE_FLEET_SUBMIT_DSN)'
      : ' (submit to the fleet instead: set ROUGE_FLEET_SUBMIT_DSN, or pass --allow-local-sim to run here)';
  }
  return {
    ok: false,
    exitCode: LOCAL_REFUSED_EXIT_CODE,
    message: `lane run: lane "${lane}" is refused for local runs by default${submitHint}. Pass --allow-local-sim to override.\n`,
  };
}

/**
 * BRAIN-455: a `localRefused` lane that is remote-eligible runs only on a runner, so "no runner can take it right now" is
 * a retryable fleet condition, not the policy refusal `localSimRefusal` reports.
 */
export function noRunnerRefusal(lane, reason) {
  return {
    ok: false,
    exitCode: NO_RUNNER_EXIT_CODE,
    message:
      `lane run: lane "${lane}" runs only on a remote runner and none can take it now (${reason}). ` +
      `Retry later, or pass --allow-local-sim to run here.\n`,
  };
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

/** BRAIN-403: the whole admission budget an exclusive lease claims (the same two formulas status and admission report). */
export function exclusiveBudget(host, cfg) {
  return { cpuBudget: cpuBudgetCores(host, cfg), memoryBudgetBytes: Math.max(0, host.memoryBytes - cfg.memoryReserveBytes) };
}
