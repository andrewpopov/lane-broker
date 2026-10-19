import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import {
  probeCpuLimit,
  cpuLimitArgv,
  resolveCpuLimit,
  enforceCpuLimitStartup,
  CpuLimitUnavailableError,
  CPU_LIMIT_CAPABILITY,
} from '../src/cpu-limit.js';
import { advertisedCapabilities } from '../src/capabilities.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

/** BRAIN-651 L1: capability and probe for per-lane CPUQuota scopes. Nothing here spawns systemd-run: `exec` is stubbed. */

const calls = [];
const execReturning = (res) => (file, args) => {
  calls.push([file, ...args]);
  return res;
};
const okExec = execReturning({ status: 0, stdout: '100000 100000\n', stderr: '' });

test('probe: success runs systemd-run --user --scope with CPUQuota=100% and reads cpu.max back inside the scope', () => {
  calls.length = 0;
  assert.deepEqual(probeCpuLimit({ platform: 'linux', exec: okExec }), { available: true, reason: 'ok' });
  const [file, ...args] = calls[0];
  assert.equal(file, 'systemd-run');
  assert.deepEqual(args.slice(0, 6), ['--user', '--scope', '-p', 'CPUQuota=100%', '--quiet', 'sh']);
  assert.match(args.join(' '), /cpu\.max/);
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

test('cpuLimitArgv: CPUQuota is 100% per granted core, and the original argv follows --', () => {
  for (const [grant, quota] of [[1, '100%'], [2, '200%'], [4, '400%']]) {
    assert.deepEqual(cpuLimitArgv(grant, ['npm', 'test']), ['systemd-run', '--user', '--scope', '-p', `CPUQuota=${quota}`, '--quiet', '--collect', '--', 'npm', 'test']);
  }
  for (const bad of [0, 1.5, -1, undefined, '2']) assert.throws(() => cpuLimitArgv(bad, ['x']), /positive integer/);
});

test('resolveCpuLimit: mode off never probes', () => {
  calls.length = 0;
  assert.deepEqual(resolveCpuLimit({ cpuLimit: 'off' }, { platform: 'linux', exec: okExec }), { mode: 'off', available: false, reason: 'off' });
  assert.equal(calls.length, 0);
  assert.deepEqual(resolveCpuLimit({ cpuLimit: 'prefer' }, { platform: 'linux', exec: okExec }), { mode: 'prefer', available: true, reason: 'ok' });
});

test('require plus an unavailable probe refuses with CPU_LIMIT_UNAVAILABLE', () => {
  assert.throws(
    () => enforceCpuLimitStartup({ cpuLimit: 'require' }, { platform: 'darwin' }),
    (err) => err instanceof CpuLimitUnavailableError && err.code === 'CPU_LIMIT_UNAVAILABLE' && /CPU_LIMIT_UNAVAILABLE.*unsupported-platform/.test(err.message),
  );
  assert.equal(enforceCpuLimitStartup({ cpuLimit: 'require' }, { platform: 'linux', exec: okExec }).available, true);
});

test('prefer plus an unavailable probe warns exactly once and continues; off is silent', () => {
  const warnings = [];
  const res = enforceCpuLimitStartup({ cpuLimit: 'prefer' }, { platform: 'darwin', warn: (l) => warnings.push(l) });
  assert.equal(res.available, false);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /unsupported-platform/);
  enforceCpuLimitStartup({ cpuLimit: 'off' }, { platform: 'darwin', warn: (l) => warnings.push(l) });
  assert.equal(warnings.length, 1);
});

test('cpu-limit/1 is advertised only when mode is not off and the probe passed', () => {
  assert.equal(advertisedCapabilities({}, { mode: 'off', available: false }).includes(CPU_LIMIT_CAPABILITY), false);
  assert.equal(advertisedCapabilities({}, { mode: 'prefer', available: false }).includes(CPU_LIMIT_CAPABILITY), false);
  assert.equal(advertisedCapabilities({}, { mode: 'require', available: true }).includes(CPU_LIMIT_CAPABILITY), true);
  assert.equal(DEFAULT_GLOBAL_CONFIG.cpuLimit, 'off');
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

test('status --json and remote-probe report cpuLimit {mode, available, reason}', async () => {
  const { home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1 });
  const status = JSON.parse((await laneRun(['status', '--json'], { env })).stdout);
  assert.deepEqual(status.cpuLimit, { mode: 'off', available: false, reason: 'off' });
  const probe = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.deepEqual(probe.cpuLimit, { mode: 'off', available: false, reason: 'off' });
  assert.equal(probe.capabilities.includes(CPU_LIMIT_CAPABILITY), false);
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
