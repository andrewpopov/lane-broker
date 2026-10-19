import { spawnSync } from 'node:child_process';

/** BRAIN-651 L1: advertised only when `cpuLimit` is not "off" and this host's probe passed. */
export const CPU_LIMIT_CAPABILITY = 'cpu-limit/1';
export const CPU_LIMIT_MODES = ['off', 'prefer', 'require'];
export const CPU_LIMIT_UNAVAILABLE = 'CPU_LIMIT_UNAVAILABLE';

const PROBE_TIMEOUT_MS = 10_000;
// A scope created with CPUQuota=100% reads back "100000 100000" (quota period); anything else means the controller did not apply it.
const READBACK = 'cat "/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/cpu.max"';
const PROBE_ARGV = ['systemd-run', '--user', '--scope', '-p', 'CPUQuota=100%', '--quiet', 'sh', '-c', READBACK];

export class CpuLimitUnavailableError extends Error {
  constructor(reason) {
    super(`${CPU_LIMIT_UNAVAILABLE}: cpuLimit is "require" but this host cannot enforce a CPU quota (${reason})`);
    this.name = 'CpuLimitUnavailableError';
    this.code = CPU_LIMIT_UNAVAILABLE;
    this.reason = reason;
  }
}

const defaultExec = (file, args) => spawnSync(file, args, { encoding: 'utf8', timeout: PROBE_TIMEOUT_MS });

/**
 * Can the broker's own user create a transient systemd scope with CPUQuota, and does the cgroup show it applied?
 * The readback runs INSIDE the scope (it is gone once its process exits). `exec(file, args)` returns spawnSync's shape.
 */
export function probeCpuLimit({ platform = process.platform, exec = defaultExec } = {}) {
  if (platform !== 'linux') return { available: false, reason: 'unsupported-platform' };
  const [file, ...args] = PROBE_ARGV;
  const res = exec(file, args);
  if (res.error) return { available: false, reason: res.error.code === 'ENOENT' ? 'systemd-run-missing' : `systemd-run-failed: ${res.error.message}` };
  if (res.status !== 0) {
    const detail = String(res.stderr ?? '').trim().split('\n')[0];
    return { available: false, reason: `systemd-run-exit-${res.status}${detail ? `: ${detail}` : ''}` };
  }
  const match = /^(\d+) (\d+)$/.exec(String(res.stdout ?? '').trim());
  if (!match || match[1] !== match[2]) return { available: false, reason: `cpu-max-not-applied: ${String(res.stdout ?? '').trim() || 'empty readback'}` };
  return { available: true, reason: 'ok' };
}

/** The transient scope's unit name for a lease: deterministic, so a later stop (BRAIN-651 L3) never has to discover it. */
export const cpuScopeUnit = (leaseId) => `lane-${leaseId}.scope`;

/** The argv that runs `argv` in its own scope capped at `grantCores` cores (named `unit` when given). Spawns nothing. */
export function cpuLimitArgv(grantCores, argv, { unit } = {}) {
  if (!Number.isInteger(grantCores) || grantCores < 1) throw new Error(`cpuLimitArgv: grantCores must be a positive integer, got ${grantCores}`);
  return ['systemd-run', '--user', '--scope', ...(unit ? [`--unit=${unit}`] : []), '-p', `CPUQuota=${grantCores * 100}%`, '--quiet', '--collect', '--', ...argv];
}

/**
 * BRAIN-651 L2: spawn a lane's child with `spawnFn`, inside a CPUQuota scope when `resolved` (resolveCpuLimit's shape) says the mode
 * is on and the probe passed, on Linux. `systemd-run --scope` execs the command in place, so the pid, process group, stdio, cwd, env and
 * exit status are the bare command's. A fractional grant rounds UP: the quota is never below what admission reserved.
 * Returns `{child, unit}`; `unit` is null when nothing was wrapped.
 */
export function spawnInCpuScope(spawnFn, { resolved, platform = process.platform, leaseId, grantCores, cmd, args, options }) {
  const enforce = platform === 'linux' && resolved?.mode !== 'off' && resolved?.available === true && Number.isFinite(grantCores) && grantCores > 0;
  if (!enforce) return { child: spawnFn(cmd, args, options), unit: null };
  const unit = cpuScopeUnit(leaseId);
  const [file, ...wrapped] = cpuLimitArgv(Math.ceil(grantCores), [cmd, ...args], { unit });
  return { child: spawnFn(file, wrapped, options), unit };
}

/** `{mode, available, reason}` for status and remote-probe. Mode "off" never runs the probe. */
export function resolveCpuLimit(cfg, deps = {}) {
  const mode = cfg.cpuLimit;
  if (mode === 'off') return { mode, available: false, reason: 'off' };
  return { mode, ...probeCpuLimit(deps) };
}

/**
 * Startup gate: "require" plus an unavailable probe throws CPU_LIMIT_UNAVAILABLE; "prefer" calls `warn` once and continues.
 * Returns the resolved `{mode, available, reason}`.
 */
export function enforceCpuLimitStartup(cfg, { warn = () => {}, ...deps } = {}) {
  const resolved = resolveCpuLimit(cfg, deps);
  if (resolved.mode === 'off' || resolved.available) return resolved;
  if (resolved.mode === 'require') throw new CpuLimitUnavailableError(resolved.reason);
  warn(`lane-broker: cpuLimit is "prefer" but CPU quotas are unavailable (${resolved.reason}); running without them\n`);
  return resolved;
}
