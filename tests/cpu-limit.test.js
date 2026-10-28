import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { listLeases } from '../src/lease.js';
import { listQueue } from '../src/scheduler.js';
import { paths } from '../src/state.js';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import {
  probeCpuLimit,
  probeCgroupControllers,
  enforcementOf,
  enforcementLabel,
  scopeCgroupDir,
  scopeMemoryPeakReader,
  ENFORCED_LEASES_CAPABILITY,
  ENFORCEMENT_UNKNOWN,
  cpuLimitArgv,
  resolveCpuLimit,
  enforceCpuLimitStartup,
  CpuLimitUnavailableError,
  CPU_LIMIT_CAPABILITY,
} from '../src/cpu-limit.js';
import { advertisedCapabilities } from '../src/capabilities.js';
import { memOver } from '../src/status.js';
import { renderRunnersLines } from '../src/runner-unreachable.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

/** BRAIN-651 L1: capability and probe for per-lane CPUQuota scopes. Nothing here spawns systemd-run: `exec` is stubbed. */

const calls = [];
const execReturning = (res) => (file, args) => {
  calls.push([file, ...args]);
  return res;
};
const okExec = execReturning({ status: 0, stdout: '100000 100000\n', stderr: '' });
const bothExec = execReturning({ status: 0, stdout: '100000 100000\n4096\n', stderr: '' });
const cpuOnlyExec = execReturning({ status: 0, stdout: '100000 100000\nnone\n', stderr: '' });

test('probe: success runs systemd-run --user --scope with CPUQuota=100% and reads cpu.max back inside the scope', () => {
  calls.length = 0;
  assert.deepEqual(probeCpuLimit({ platform: 'linux', exec: okExec }), { available: true, reason: 'ok' });
  const [file, ...args] = calls[0];
  assert.equal(file, 'systemd-run');
  assert.deepEqual(args.slice(0, 8), ['--user', '--scope', '-p', 'CPUQuota=100%', '-p', 'MemoryAccounting=yes', '--quiet', 'sh']);
  assert.match(args.join(' '), /cpu\.max/);
  assert.match(args.join(' '), /memory\.peak/);
});

test('probe: systemd-run missing, a non-zero exit, and a quota that was not applied are each unavailable with a reason', () => {
  const missing = probeCpuLimit({ platform: 'linux', exec: execReturning({ error: Object.assign(new Error('spawn systemd-run ENOENT'), { code: 'ENOENT' }) }) });
  assert.deepEqual(missing, { available: false, reason: 'systemd-run-missing' });
  const failed = probeCpuLimit({ platform: 'linux', exec: execReturning({ status: 1, stdout: '', stderr: 'Failed to connect to bus\nmore\n' }) });
  assert.deepEqual(failed, { available: false, reason: 'systemd-run-exit-1: Failed to connect to bus' });
  const unapplied = probeCpuLimit({ platform: 'linux', exec: execReturning({ status: 0, stdout: 'max 100000\n', stderr: '' }) });
  assert.equal(unapplied.available, false);
  assert.match(unapplied.reason, /^cpu-max-not-applied: max 100000/);
});

test('probe: macOS is unsupported and never execs', () => {
  calls.length = 0;
  assert.deepEqual(probeCpuLimit({ platform: 'darwin', exec: okExec }), { available: false, reason: 'unsupported-platform' });
  assert.equal(calls.length, 0);
});

test('probe: CPU and memory resolve independently (a cpu-only delegation still enforces CPU)', () => {
  assert.deepEqual(probeCgroupControllers({ platform: 'linux', exec: bothExec }), { cpu: { available: true, reason: 'ok' }, memory: { available: true, reason: 'ok' } });
  const cpuOnly = probeCgroupControllers({ platform: 'linux', exec: cpuOnlyExec });
  assert.deepEqual(cpuOnly.cpu, { available: true, reason: 'ok' });
  assert.equal(cpuOnly.memory.available, false);
  assert.match(cpuOnly.memory.reason, /^memory-not-readable: none/);
  const memoryOnly = probeCgroupControllers({ platform: 'linux', exec: execReturning({ status: 0, stdout: 'none\n4096\n', stderr: '' }) });
  assert.equal(memoryOnly.cpu.available, false);
  assert.equal(memoryOnly.memory.available, true);
});

test('cpuLimitArgv: CPUQuota is 100% per booked core, and the original argv follows --', () => {
  for (const [grant, quota] of [[1, '100%'], [2, '200%'], [4, '400%']]) {
    assert.deepEqual(cpuLimitArgv(grant, ['npm', 'test']), ['systemd-run', '--user', '--scope', '-p', `CPUQuota=${quota}`, '--quiet', '--collect', '--', 'npm', 'test']);
  }
  assert.ok(cpuLimitArgv(1.5, ['x']).includes('CPUQuota=150%'), 'exact fractional quota, not rounded up');
  assert.ok(cpuLimitArgv(0.001, ['x']).includes('CPUQuota=1%'), 'at least 1%');
  assert.ok(cpuLimitArgv(2, ['x'], { measureMemory: true }).includes('MemoryAccounting=yes'));
  for (const bad of [0, -1, Infinity, NaN, undefined, '2']) assert.throws(() => cpuLimitArgv(bad, ['x']), /positive and finite/);
});

test('resolveCpuLimit: mode off never probes', () => {
  calls.length = 0;
  assert.deepEqual(resolveCpuLimit({ cpuLimit: 'off' }, { platform: 'linux', exec: okExec }), { mode: 'off', available: false, reason: 'off', memory: { mode: 'off', available: false, reason: 'off' } });
  assert.equal(calls.length, 0);
  const resolved = resolveCpuLimit({ cpuLimit: 'prefer', memoryLimit: 'measure' }, { platform: 'linux', exec: bothExec });
  assert.deepEqual(resolved, { mode: 'prefer', available: true, reason: 'ok', memory: { mode: 'measure', available: true, reason: 'ok' } });
});

test('require plus an unavailable probe refuses with CPU_LIMIT_UNAVAILABLE', () => {
  assert.throws(
    () => enforceCpuLimitStartup({ cpuLimit: 'require' }, { platform: 'darwin' }),
    (err) => err instanceof CpuLimitUnavailableError && err.code === 'CPU_LIMIT_UNAVAILABLE' && /CPU_LIMIT_UNAVAILABLE.*unsupported-platform/.test(err.message),
  );
  assert.equal(enforceCpuLimitStartup({ cpuLimit: 'require' }, { platform: 'linux', exec: okExec }).available, true);
});

test('prefer plus an unavailable probe warns exactly once and continues; off is silent; darwin (no cgroups at all) is silent', () => {
  const warnings = [];
  const missing = { platform: 'linux', exec: execReturning({ error: Object.assign(new Error('x'), { code: 'ENOENT' }) }), warn: (l) => warnings.push(l) };
  const res = enforceCpuLimitStartup({ cpuLimit: 'prefer' }, missing);
  assert.equal(res.available, false);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /systemd-run-missing/);
  enforceCpuLimitStartup({ cpuLimit: 'off' }, { platform: 'darwin', warn: (l) => warnings.push(l) });
  enforceCpuLimitStartup({ cpuLimit: 'prefer' }, { platform: 'darwin', warn: (l) => warnings.push(l) });
  assert.equal(warnings.length, 1);
});

test('enforced-leases/1 needs CPU enforcement and memoryLimit not off', () => {
  const on = { mode: 'prefer', available: true };
  assert.equal(advertisedCapabilities({ memoryLimit: 'measure' }, on).includes(ENFORCED_LEASES_CAPABILITY), true);
  assert.equal(advertisedCapabilities({ memoryLimit: 'off' }, on).includes(ENFORCED_LEASES_CAPABILITY), false);
  assert.equal(advertisedCapabilities({ memoryLimit: 'measure' }, { mode: 'prefer', available: false }).includes(ENFORCED_LEASES_CAPABILITY), false);
  assert.equal(advertisedCapabilities({ memoryLimit: 'measure' }, { mode: 'off', available: false }).includes(ENFORCED_LEASES_CAPABILITY), false);
});

test('enforcementOf: enforced + measured, cpu-only host, memory off, and the darwin reason', () => {
  const full = resolveCpuLimit({ cpuLimit: 'prefer', memoryLimit: 'measure' }, { platform: 'linux', exec: bothExec });
  assert.deepEqual(enforcementOf(full, { platform: 'linux' }), { cpu: 'enforced', memory: 'measured', reason: 'ok' });
  const cpuOnly = resolveCpuLimit({ cpuLimit: 'prefer', memoryLimit: 'measure' }, { platform: 'linux', exec: cpuOnlyExec });
  assert.deepEqual(enforcementOf(cpuOnly, { platform: 'linux' }), { cpu: 'enforced', memory: 'unavailable', reason: 'memory-not-readable: none' });
  const memOff = resolveCpuLimit({ cpuLimit: 'prefer', memoryLimit: 'off' }, { platform: 'linux', exec: bothExec });
  assert.equal(enforcementOf(memOff, { platform: 'linux' }).memory, 'off');
  const mac = resolveCpuLimit({ cpuLimit: 'prefer', memoryLimit: 'measure' }, { platform: 'darwin' });
  assert.deepEqual(enforcementOf(mac, { platform: 'darwin' }), { cpu: 'unenforced', memory: 'unavailable', reason: 'no cgroups on darwin; children get nice only' });
});

test('enforcementLabel names the enforcement, or that the runner is older than 0.61.0', () => {
  assert.equal(enforcementLabel({ cpu: 'enforced', memory: 'measured', reason: 'ok' }), 'cpu enforced, memory measured');
  assert.equal(enforcementLabel(undefined), ENFORCEMENT_UNKNOWN);
  assert.equal(ENFORCEMENT_UNKNOWN, 'unknown (runner older than 0.61.0)');
});

test('RUNNERS lines show each runner\'s last-seen enforcement; absent field renders nothing', () => {
  const base = { state: 'up', unreachableSince: null, lostSettleAt: null, lastOkAt: null, offSchedule: false, readinessGap: null, watcherRunning: true };
  const lines = renderRunnersLines([
    { ...base, name: 'skybox', enforcement: { cpu: 'enforced', memory: 'measured', reason: 'ok' } },
    { ...base, name: 'old', enforcement: null },
    { ...base, name: 'unseen' },
  ]).join('\n');
  assert.match(lines, /skybox .*enforcement: cpu enforced, memory measured/);
  assert.match(lines, /old .*enforcement: unknown \(runner older than 0\.61\.0\)/);
  assert.doesNotMatch(lines.split('\n').find((l) => l.includes('unseen')), /enforcement/);
});

test('scope memory reader: memory.peak when present, else the running max of memory.current, null when unreadable (never throws)', () => {
  const unit = 'lane-abc.scope';
  const files = { '/proc/7/cgroup': `0::/user.slice/user-1.slice/user@1.service/app.slice/${unit}\n` };
  const base = `/sys/fs/cgroup/user.slice/user-1.slice/user@1.service/app.slice/${unit}`;
  const readFile = (p) => {
    if (!(p in files)) throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
    return files[p];
  };
  assert.equal(scopeCgroupDir(unit, { pid: 7, readFile }), base);
  assert.equal(scopeCgroupDir(unit, { pid: 7, readFile: () => '0::/user.slice/other.scope\n', uid: 1, exists: () => false }), null, 'a pid not yet in the scope is not mistaken for it');
  const reader = scopeMemoryPeakReader(unit, { pid: 7, readFile });
  assert.equal(reader.read(), null, 'no memory files yet');
  files[`${base}/memory.current`] = '100\n';
  assert.equal(reader.read(), 100);
  files[`${base}/memory.current`] = '40\n';
  assert.equal(reader.read(), 100, 'running max of memory.current');
  files[`${base}/memory.peak`] = '900\n';
  assert.equal(reader.read(), 900, 'memory.peak wins');
  const gone = scopeMemoryPeakReader(unit, { pid: 8, readFile: () => { throw new Error('gone'); }, uid: 1, exists: () => false });
  assert.equal(gone.read(), null);
});

test('MEM-OVER: only when the measured cgroup peak exceeds the booked memory', () => {
  assert.equal(memOver({ observedCgroupMemoryPeakBytes: 3e9 }, { memoryBytes: 2e9 }), true);
  assert.equal(memOver({ observedCgroupMemoryPeakBytes: 2e9 }, { memoryBytes: 2e9 }), false);
  assert.equal(memOver({}, { memoryBytes: 2e9 }), false);
});

test('cpu-limit/1 is advertised only when mode is not off and the probe passed', () => {
  assert.equal(advertisedCapabilities({}, { mode: 'off', available: false }).includes(CPU_LIMIT_CAPABILITY), false);
  assert.equal(advertisedCapabilities({}, { mode: 'prefer', available: false }).includes(CPU_LIMIT_CAPABILITY), false);
  assert.equal(advertisedCapabilities({}, { mode: 'require', available: true }).includes(CPU_LIMIT_CAPABILITY), true);
  assert.equal(DEFAULT_GLOBAL_CONFIG.cpuLimit, 'prefer', 'BRAIN-695: CPU bookings are enforced by default');
  assert.equal(DEFAULT_GLOBAL_CONFIG.memoryLimit, 'measure');
});

test('config: cpuLimit accepts off/prefer/require and rejects anything else by name', async () => {
  for (const value of ['off', 'prefer', 'require']) {
    const { home, env } = freshEnv();
    writeGlobalConfig(home, { version: 1, cpuLimit: value });
    const res = await laneRun(['capabilities', '--json'], { env });
    assert.equal(res.code, 0, res.stderr);
    assert.equal(JSON.parse(res.stdout).cpuLimit.mode, value);
  }
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, cpuLimit: 'always' });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.match(res.stderr + res.stdout, /"cpuLimit" must be one of "off", "prefer", "require", got "always"/);
});

test('status --json and remote-probe report cpuLimit {mode, available, reason, memory} and the enforcement summary', async () => {
  const { home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, cpuLimit: 'off', memoryLimit: 'off' });
  const off = { mode: 'off', available: false, reason: 'off', memory: { mode: 'off', available: false, reason: 'off' } };
  const status = JSON.parse((await laneRun(['status', '--json'], { env })).stdout);
  assert.deepEqual(status.cpuLimit, off);
  const probe = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.deepEqual(probe.cpuLimit, off);
  assert.equal(probe.enforcement.cpu, 'unenforced');
  assert.equal(probe.enforcement.memory, 'off');
  assert.equal(probe.capabilities.includes(CPU_LIMIT_CAPABILITY), false);
  assert.equal(probe.capabilities.includes(ENFORCED_LEASES_CAPABILITY), false);
  const caps = JSON.parse((await laneRun(['capabilities', '--json'], { env })).stdout);
  assert.deepEqual(caps.enforcement, probe.enforcement);
});

test('default config: cpuLimit prefer and memoryLimit measure; on darwin the enforcement reason says nice only', async () => {
  const { home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1 });
  const caps = JSON.parse((await laneRun(['capabilities', '--json'], { env })).stdout);
  assert.equal(caps.cpuLimit.mode, 'prefer');
  assert.equal(caps.cpuLimit.memory.mode, 'measure');
  if (process.platform === 'darwin') assert.deepEqual(caps.enforcement, { cpu: 'unenforced', memory: 'unavailable', reason: 'no cgroups on darwin; children get nice only' });
});

test('config: memoryLimit accepts off/measure, rejects unknown values by name, and refuses "enforce" (BRAIN-696)', async () => {
  for (const value of ['off', 'measure']) {
    const { home, env } = freshEnv();
    writeGlobalConfig(home, { version: 1, memoryLimit: value });
    const res = await laneRun(['capabilities', '--json'], { env });
    assert.equal(res.code, 0, res.stderr);
    assert.equal(JSON.parse(res.stdout).cpuLimit.memory.mode, value);
  }
  for (const [value, message] of [['always', /"memoryLimit" must be one of "off", "measure", "enforce", got "always"/], ['enforce', /"memoryLimit": "enforce" is not yet supported.*BRAIN-696/]]) {
    const { base, home, env } = freshEnv();
    writeGlobalConfig(home, { version: 1, memoryLimit: value });
    const repoDir = path.join(base, 'repo');
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
    const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
    assert.match(res.stderr + res.stdout, message);
  }
});

test('supervisor: cpuLimit "require" on a host without quotas refuses the run by name (exit 69) and never runs the payload', { skip: process.platform === 'linux' }, async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, cpuLimit: 'require' });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, '-e', 'process.exit(0)'], { env, cwd: repoDir });
  assert.equal(res.code, 69, res.stderr);
  assert.match(res.stderr, /CPU_LIMIT_UNAVAILABLE.*unsupported-platform/);
});

for (const mode of ['off', 'prefer']) {
  test(`EXECUTION_CEILING_REFUSED: cpuLimit ${mode} without enforcement publishes terminal refusal and releases exclusive reservation before payload`, { skip: mode === 'prefer' && process.platform === 'linux' }, async () => {
    const { base, home, state, env } = freshEnv();
    writeGlobalConfig(home, { version: 1, cpuLimit: mode, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
    const repoDir = path.join(base, 'repo');
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, exclusive: true, maxExecutionCpuCores: 6 } } });
    const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, '-e', 'console.log("PAYLOAD_RAN")'], { env, cwd: repoDir });
    assert.equal(res.code, 69, res.stderr);
    assert.match(res.stderr, /CPU_LIMIT_UNAVAILABLE.*maxExecutionCpuCores/);
    assert.doesNotMatch(res.stdout, /PAYLOAD_RAN/);
    const results = fs.readdirSync(paths(state).results).filter((name) => name.endsWith('.json'));
    assert.equal(results.length, 1, 'the supervisor published a terminal result');
    const terminal = JSON.parse(fs.readFileSync(path.join(paths(state).results, results[0]), 'utf8'));
    assert.equal(terminal.exit, 69);
    assert.match(terminal.error, /CPU_LIMIT_UNAVAILABLE.*maxExecutionCpuCores/);
    assert.equal(terminal.startedAt, null);
    assert.deepEqual(listLeases(state), [], 'refusal released the admitted exclusive reservation');
    assert.deepEqual(listQueue(state), [], 'refusal removed the queued ticket');
  });
}
