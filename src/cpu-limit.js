import fs from 'node:fs';
import { spawnSync } from 'node:child_process';

/** BRAIN-651 L1: advertised only when `cpuLimit` is not "off" and this host's probe passed. */
export const CPU_LIMIT_CAPABILITY = 'cpu-limit/1';
export const CPU_LIMIT_MODES = ['off', 'prefer', 'require'];
/** BRAIN-695: "measure" records each lease's cgroup memory peak; "enforce" (a hard MemoryMax) is refused at config load until BRAIN-696. */
export const MEMORY_LIMIT_MODES = ['off', 'measure', 'enforce'];
/** BRAIN-695: advertised when CPU is capped per lease (probe passed, cpuLimit on) and memory is not switched off. */
export const ENFORCED_LEASES_CAPABILITY = 'enforced-leases/1';
export const ENFORCEMENT_UNKNOWN = 'unknown (runner older than 0.61.0)';
export const CPU_LIMIT_UNAVAILABLE = 'CPU_LIMIT_UNAVAILABLE';

const PROBE_TIMEOUT_MS = 10_000;
// A scope created with CPUQuota=100% reads back "100000 100000" (quota period); anything else means the controller did not apply it.
// The same scope reads back the memory controller too (line 2): CPU and memory resolve independently, so a host that delegates only
// `cpu` (pitelite) still enforces CPU. A memory file that cannot be read prints "none".
const READBACK = 'cg="/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)"; cat "$cg/cpu.max" 2>/dev/null || echo none; cat "$cg/memory.peak" 2>/dev/null || cat "$cg/memory.current" 2>/dev/null || echo none';
// systemd >= 259 expands $VAR in scope command arguments; --expand-environment=no keeps lane argv byte-for-byte (a host that rejects the flag fails the probe and runs unwrapped).
const PROBE_ARGV = ['systemd-run', '--user', '--scope', '--expand-environment=no', '-p', 'CPUQuota=100%', '-p', 'MemoryAccounting=yes', '--quiet', 'sh', '-c', READBACK];

export class CpuLimitUnavailableError extends Error {
  constructor(reason, requirement = 'cpuLimit is "require"') {
    super(`${CPU_LIMIT_UNAVAILABLE}: ${requirement} but this host cannot enforce a CPU quota (${reason})`);
    this.name = 'CpuLimitUnavailableError';
    this.code = CPU_LIMIT_UNAVAILABLE;
    this.reason = reason;
  }
}

const defaultExec = (file, args) => spawnSync(file, args, { encoding: 'utf8', timeout: PROBE_TIMEOUT_MS });

/**
 * Can the broker's own user create a transient systemd scope with CPUQuota, and does the cgroup show it applied? Independently, can that
 * scope's memory accounting be read? Returns `{cpu: {available, reason}, memory: {available, reason}}`.
 * The readback runs INSIDE the scope (it is gone once its process exits). `exec(file, args)` returns spawnSync's shape.
 */
export function probeCgroupControllers({ platform = process.platform, exec = defaultExec } = {}) {
  const both = (reason) => ({ cpu: { available: false, reason }, memory: { available: false, reason } });
  if (platform !== 'linux') return both('unsupported-platform');
  const [file, ...args] = PROBE_ARGV;
  const res = exec(file, args);
  if (res.error) return both(res.error.code === 'ENOENT' ? 'systemd-run-missing' : `systemd-run-failed: ${res.error.message}`);
  if (res.status !== 0) {
    const detail = String(res.stderr ?? '').trim().split('\n')[0];
    return both(`systemd-run-exit-${res.status}${detail ? `: ${detail}` : ''}`);
  }
  const [cpuLine = '', memoryLine = ''] = String(res.stdout ?? '').trim().split('\n').map((line) => line.trim());
  const match = /^(\d+) (\d+)$/.exec(cpuLine);
  return {
    cpu: !match || match[1] !== match[2] ? { available: false, reason: `cpu-max-not-applied: ${cpuLine || 'empty readback'}` } : { available: true, reason: 'ok' },
    memory: /^\d+$/.test(memoryLine) ? { available: true, reason: 'ok' } : { available: false, reason: `memory-not-readable: ${memoryLine || 'empty readback'}` },
  };
}

/** The CPU half of `probeCgroupControllers`: `{available, reason}`. */
export const probeCpuLimit = (deps = {}) => probeCgroupControllers(deps).cpu;

/** The transient scope's unit name for a lease: deterministic, so a later stop (BRAIN-651 L3) never has to discover it. */
export const cpuScopeUnit = (leaseId) => `lane-${leaseId}.scope`;

/** The smallest CPU grant, in cores, a quota can express (1%): below it a quota would have to exceed the grant. */
export const MIN_CPU_GRANT_CORES = 0.01;

/** The quota for `cores` booked cores: the exact percent (systemd accepts e.g. 150%), floored so it never exceeds the grant. */
export function cpuQuotaPercent(cores) {
  const percent = Math.floor(cores * 100 + 1e-9); // 0.29 * 100 is 28.999999999999996: the epsilon absorbs float error, never a real fraction of a percent
  if (!(percent >= 1)) throw new Error(`cpuQuotaPercent: a grant of ${cores} cores is below the ${MIN_CPU_GRANT_CORES}-core minimum (1%)`);
  return percent;
}

/** The argv that runs `argv` in its own scope capped at `grantCores` cores (named `unit` when given; `measureMemory` turns memory accounting on, with no cap). Spawns nothing. */
export function cpuLimitArgv(grantCores, argv, { unit, measureMemory = false } = {}) {
  if (!Number.isFinite(grantCores) || grantCores <= 0) throw new Error(`cpuLimitArgv: grantCores must be positive and finite, got ${grantCores}`);
  return ['systemd-run', '--user', '--scope', '--expand-environment=no', ...(unit ? [`--unit=${unit}`] : []), '-p', `CPUQuota=${cpuQuotaPercent(grantCores)}%`, ...(measureMemory ? ['-p', 'MemoryAccounting=yes'] : []), '--quiet', '--collect', '--', ...argv];
}

/** Execution is distinct from admission: an exclusive reservation is never reduced by this ceiling. */
export function resolveExecutionCpuCores(grantedCpuCores, maxExecutionCpuCores) {
  if (maxExecutionCpuCores === undefined) return grantedCpuCores;
  if (!Number.isInteger(maxExecutionCpuCores) || maxExecutionCpuCores <= 0) throw new Error('maxExecutionCpuCores must be a positive integer');
  if (!Number.isFinite(grantedCpuCores) || grantedCpuCores <= 0) throw new CpuLimitUnavailableError('no usable CPU grant', 'maxExecutionCpuCores was requested');
  return Math.min(grantedCpuCores, maxExecutionCpuCores);
}

/**
 * BRAIN-651 L2: spawn a lane's child with `spawnFn`, inside a CPUQuota scope when `resolved` (resolveCpuLimit's shape) says the mode
 * is on and the probe passed, on Linux. `systemd-run --scope` execs the command in place, so the pid, process group, stdio, cwd, env and
 * exit status are the bare command's. The quota is the exact booked allocation (BRAIN-695: a fractional grant is no longer rounded up).
 * `resolved.memory.available` additionally turns on memory accounting so the heartbeat can read the scope's peak. Returns `{child, unit}`; `unit` is null when nothing was wrapped.
 */
export function spawnInCpuScope(spawnFn, { resolved, platform = process.platform, leaseId, grantCores, requireQuota = false, cmd, args, options }) {
  const grantTooSmall = Number.isFinite(grantCores) && grantCores > 0 && grantCores < MIN_CPU_GRANT_CORES;
  const enforce = platform === 'linux' && resolved?.mode !== 'off' && resolved?.available === true && Number.isFinite(grantCores) && grantCores >= MIN_CPU_GRANT_CORES;
  const unenforceable = () => new CpuLimitUnavailableError(grantTooSmall ? `grant of ${grantCores} cores is below the ${MIN_CPU_GRANT_CORES}-core minimum` : resolved?.reason ?? 'unsupported-platform', 'maxExecutionCpuCores was requested');
  if (requireQuota && !enforce) throw unenforceable();
  if (grantTooSmall && resolved?.mode === 'require') throw unenforceable();
  if (!enforce) return { child: spawnFn(cmd, args, options), unit: null };
  const unit = cpuScopeUnit(leaseId);
  const [file, ...wrapped] = cpuLimitArgv(grantCores, [cmd, ...args], { unit, measureMemory: resolved.memory?.available === true });
  return { child: spawnFn(file, wrapped, options), unit };
}

/**
 * `{mode, available, reason, memory: {mode, available, reason}}` for status and remote-probe: the CPU result on top, the memory controller's
 * beside it (`cfg.memoryLimit`; absent counts as "off"). Mode "off" never runs the probe; one probe serves both controllers.
 */
export function resolveCpuLimit(cfg, deps = {}) {
  const mode = cfg.cpuLimit;
  const memoryMode = cfg.memoryLimit ?? 'off';
  if (mode === 'off') return { mode, available: false, reason: 'off', memory: { mode: memoryMode, available: false, reason: 'off' } };
  const { cpu, memory } = probeCgroupControllers(deps);
  return { mode, ...cpu, memory: { mode: memoryMode, ...(memoryMode === 'off' ? { available: false, reason: 'off' } : memory) } };
}

/** BRAIN-695: what a lease on this host is held to: `{cpu: "enforced"|"unenforced", memory: "measured"|"off"|"unavailable", reason}`. */
export function enforcementOf(resolved, { platform = process.platform } = {}) {
  const cpu = resolved?.mode !== 'off' && resolved?.available === true ? 'enforced' : 'unenforced';
  const memoryMode = resolved?.memory?.mode ?? 'off';
  const memory = memoryMode === 'off' ? 'off' : cpu === 'enforced' && resolved.memory.available === true ? 'measured' : 'unavailable';
  const reason = platform === 'darwin' ? 'no cgroups on darwin; children get nice only' : cpu === 'unenforced' ? resolved?.reason ?? 'unknown' : memory === 'unavailable' ? resolved.memory.reason : 'ok';
  return { cpu, memory, reason };
}

/** A probe's enforcement as the placement line and `lane status` name it; a runner older than 0.61.0 sends none. */
export function enforcementLabel(enforcement) {
  if (!enforcement || typeof enforcement !== 'object') return ENFORCEMENT_UNKNOWN;
  return `cpu ${enforcement.cpu}, memory ${enforcement.memory}`;
}

/**
 * Startup gate: "require" plus an unavailable probe throws CPU_LIMIT_UNAVAILABLE; "prefer" calls `warn` once and continues
 * (silently on a platform that has no cgroups at all, where nothing can be done about it).
 * Returns the resolved `{mode, available, reason, memory}`.
 */
export function enforceCpuLimitStartup(cfg, { warn = () => {}, ...deps } = {}) {
  const resolved = resolveCpuLimit(cfg, deps);
  if (resolved.mode === 'off' || resolved.available) return resolved;
  if (resolved.mode === 'require') throw new CpuLimitUnavailableError(resolved.reason);
  if (resolved.reason !== 'unsupported-platform') warn(`lane-broker: cpuLimit is "prefer" but CPU quotas are unavailable (${resolved.reason}); running without them\n`);
  return resolved;
}

/** How long a lease release waits for a scope stop before it gives up and releases anyway (BRAIN-651 L3). */
export const CPU_SCOPE_STOP_WAIT_MS = 15_000;
const stopExec = (file, args) => spawnSync(file, args, { encoding: 'utf8', timeout: CPU_SCOPE_STOP_WAIT_MS });

/**
 * BRAIN-651 L3: stop a lane's scope, which signals EVERY process in it (a setsid'd grandchild included; KillMode defaults to
 * control-group: SIGTERM, then SIGKILL after the unit's stop timeout) and returns once the scope is empty. A scope that is not
 * loaded has already ended (`--collect` unloads it when its last process exits), so that counts as stopped.
 * `exec(file, args)` returns spawnSync's shape. Returns `{stopped, reason}`.
 */
export function stopCpuScope(unit, { exec = stopExec } = {}) {
  const res = exec('systemctl', ['--user', 'stop', unit]);
  if (res.error) {
    const reason = res.error.code === 'ENOENT' ? 'systemctl-missing' : res.error.code === 'ETIMEDOUT' ? 'timeout' : `systemctl-failed: ${res.error.message}`;
    return { stopped: false, reason };
  }
  if (res.status === 0) return { stopped: true, reason: 'stopped' };
  const detail = String(res.stderr ?? '').trim().split('\n')[0];
  if (/not loaded|not found/i.test(detail)) return { stopped: true, reason: 'not-loaded' };
  return { stopped: false, reason: `systemctl-exit-${res.status}${detail ? `: ${detail}` : ''}` };
}

/**
 * Stop the scope a lease recorded as `cpuScopeUnit`, before the lease is released. A lease without one (cpuLimit off, macOS)
 * makes no call and returns null. A stop that did not complete is logged through `log` naming the unit, and the caller releases anyway.
 */
export function stopLeaseCpuScope(lease, { exec, log = () => {} } = {}) {
  const unit = lease?.cpuScopeUnit;
  if (!unit) return null;
  const result = stopCpuScope(unit, { exec });
  if (!result.stopped) log(`lane-broker-cpu-scope id=${lease.id} unit=${unit} stop-incomplete reason=${result.reason}; releasing the lease anyway\n`);
  return result;
}

const UNIT_CGROUP_UNDER_USER_MANAGER = (uid, unit) => `/sys/fs/cgroup/user.slice/user-${uid}.slice/user@${uid}.service/app.slice/${unit}`;

/** The scope's cgroup directory: from the child's own /proc/<pid>/cgroup (only when it names `unit`, so a child not yet moved into the scope is never mistaken for it), else the user-manager path. */
export function scopeCgroupDir(unit, { pid, uid = process.getuid?.(), readFile = (p) => fs.readFileSync(p, 'utf8'), exists = fs.existsSync } = {}) {
  try {
    const line = readFile(`/proc/${pid}/cgroup`).split('\n').find((l) => l.startsWith('0::'));
    const rel = line?.slice(3).trim();
    if (rel?.endsWith(`/${unit}`)) return `/sys/fs/cgroup${rel}`;
  } catch {
    // process gone or no /proc: fall through to the deterministic path
  }
  const fallback = uid === undefined ? null : UNIT_CGROUP_UNDER_USER_MANAGER(uid, unit);
  return fallback && exists(fallback) ? fallback : null;
}

/**
 * BRAIN-695: a reader of one lease scope's memory peak in bytes. `read()` returns the scope's `memory.peak`, else the running max of its
 * `memory.current`, or null when nothing is readable (scope not yet entered, already collected, no memory controller). It never throws.
 */
export function scopeMemoryPeakTracker(reader) {
  let peak = null;
  return {
    /** Reads once more and returns the highest value seen so far (null when nothing was ever readable); never throws. */
    sample() {
      try {
        const value = reader?.read() ?? null;
        if (value !== null) peak = Math.max(peak ?? 0, value);
      } catch {
        // best effort: an unreadable scope leaves the previous peak
      }
      return peak;
    },
  };
}

export function scopeMemoryPeakReader(unit, { pid, ...deps } = {}) {
  const readFile = deps.readFile ?? ((p) => fs.readFileSync(p, 'utf8'));
  let dir = null;
  let currentMax = null;
  const num = (file) => {
    try {
      const value = Number(readFile(`${dir}/${file}`).trim());
      return Number.isFinite(value) && value >= 0 ? value : null;
    } catch {
      return null;
    }
  };
  return {
    read() {
      try {
        dir ??= scopeCgroupDir(unit, { pid, ...deps });
        if (!dir) return null;
        const peak = num('memory.peak');
        if (peak !== null) return peak;
        const current = num('memory.current');
        if (current !== null) currentMax = Math.max(currentMax ?? 0, current);
        return currentMax;
      } catch {
        return null;
      }
    },
  };
}
