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
  constructor(reason, requirement = 'cpuLimit is "require"') {
    super(`${CPU_LIMIT_UNAVAILABLE}: ${requirement} but this host cannot enforce a CPU quota (${reason})`);
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
  if (!Number.isFinite(grantCores) || grantCores <= 0) throw new Error(`cpuLimitArgv: grantCores must be positive and finite, got ${grantCores}`);
  return ['systemd-run', '--user', '--scope', ...(unit ? [`--unit=${unit}`] : []), '-p', `CPUQuota=${grantCores * 100}%`, '--quiet', '--collect', '--', ...argv];
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
 * exit status are the bare command's. By default a fractional grant rounds UP, preserving legacy behavior. An explicit execution ceiling uses the exact allocation.
 * Returns `{child, unit}`; `unit` is null when nothing was wrapped.
 */
export function spawnInCpuScope(spawnFn, { resolved, platform = process.platform, leaseId, grantCores, requireQuota = false, cmd, args, options }) {
  const enforce = platform === 'linux' && resolved?.mode !== 'off' && resolved?.available === true && Number.isFinite(grantCores) && grantCores > 0;
  if (requireQuota && !enforce) throw new CpuLimitUnavailableError(resolved?.reason ?? 'unsupported-platform', 'maxExecutionCpuCores was requested');
  if (!enforce) return { child: spawnFn(cmd, args, options), unit: null };
  const unit = cpuScopeUnit(leaseId);
  const [file, ...wrapped] = cpuLimitArgv(requireQuota ? grantCores : Math.ceil(grantCores), [cmd, ...args], { unit });
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
