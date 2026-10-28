import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { makeTmpDir } from './helpers/tmp.js';
import { CpuLimitUnavailableError, spawnInCpuScope, cpuScopeUnit, resolveCpuLimit, resolveExecutionCpuCores } from '../src/cpu-limit.js';
import { childEnv, runningLease } from '../src/supervisor.js';

/** BRAIN-651 L2: a lane child runs inside a CPUQuota scope sized to its admitted grant. Unit tests inject `spawn`; the integration test is Linux-only. */

const ON = { mode: 'prefer', available: true, reason: 'ok' };
const LEASE = '11111111-2222-4333-8444-555555555555';
const OPTIONS = { cwd: '/work', detached: true, stdio: ['ignore', 'pipe', 'pipe'], env: { LANE_BROKER_CPU_CORES: '2' } };

const recordingSpawn = () => {
  const calls = [];
  const fn = (cmd, args, options) => {
    calls.push({ cmd, args, options });
    return { pid: 1 };
  };
  return { fn, calls };
};
const run = (spawnFn, over = {}) => spawnInCpuScope(spawnFn, { resolved: ON, platform: 'linux', leaseId: LEASE, grantCores: 2, cmd: '/usr/bin/nice', args: ['-n', '5', 'npm', 'test'], options: OPTIONS, ...over });

test('CPU_GRANT_WRAPPED: grants 2 and 4 spawn systemd-run with CPUQuota = 100% per core, a lease-named unit, and the nice command inside', () => {
  for (const [grant, quota] of [[2, '200%'], [4, '400%']]) {
    const { fn, calls } = recordingSpawn();
    const { unit } = run(fn, { grantCores: grant });
    assert.equal(unit, `lane-${LEASE}.scope`);
    assert.equal(unit, cpuScopeUnit(LEASE));
    assert.equal(calls.length, 1);
    assert.equal(calls[0].cmd, 'systemd-run');
    assert.deepEqual(calls[0].args, ['--user', '--scope', `--unit=lane-${LEASE}.scope`, '-p', `CPUQuota=${quota}`, '--quiet', '--collect', '--', '/usr/bin/nice', '-n', '5', 'npm', 'test']);
    assert.equal(calls[0].options, OPTIONS, 'cwd, env, detached and stdio are handed through untouched');
  }
});

test('EXECUTION_CEILING: quota and worker budget use min(actual grant, ceiling) without changing the reservation', () => {
  for (const [grant, cap, expected] of [[14.4, 6, 6], [2, 6, 2], [1.5, 6, 1.5]]) {
    const lease = { id: LEASE, key: 'r:release', exclusive: true, resources: { cpuCores: grant }, grantedCpuCores: grant };
    const allocation = resolveExecutionCpuCores(grant, cap);
    const { fn, calls } = recordingSpawn();
    run(fn, { grantCores: allocation, requireQuota: true, options: { ...OPTIONS, env: childEnv(lease, {}, allocation) } });
    assert.equal(allocation, expected);
    assert.ok(calls[0].args.includes(`CPUQuota=${expected * 100}%`));
    assert.equal(calls[0].options.env.LANE_BROKER_CPU_CORES, String(expected));
    assert.equal(lease.resources.cpuCores, grant);
    assert.equal(lease.grantedCpuCores, grant);
    const live = runningLease({ ...lease, executionCpuCores: allocation }, { childPgid: 42, startedAt: 1 });
    assert.equal(live.executionCpuCores, expected);
    assert.equal(live.grantedCpuCores, grant);
  }
  assert.equal(resolveExecutionCpuCores(14.4), 14.4, 'omitted ceiling preserves the original worker budget');
  for (const bad of [0, -1, 1.5, Infinity, NaN, '6', null]) assert.throws(() => resolveExecutionCpuCores(14.4, bad), /positive integer/);
});

test('EXECUTION_CEILING_FAIL_CLOSED: quota off, unsupported or unavailable never spawns a child', () => {
  for (const over of [
    { resolved: { mode: 'off', available: false, reason: 'off' } },
    { resolved: { mode: 'prefer', available: false, reason: 'systemd-run-missing' } },
    { platform: 'darwin' },
    { resolved: undefined },
    { grantCores: undefined },
  ]) {
    const { fn, calls } = recordingSpawn();
    assert.throws(() => run(fn, { requireQuota: true, ...over }), /CPU_LIMIT_UNAVAILABLE.*maxExecutionCpuCores/);
    assert.equal(calls.length, 0);
  }
  assert.throws(() => resolveExecutionCpuCores(undefined, 6), /CPU_LIMIT_UNAVAILABLE/);
});

test('a fractional grant gets its exact quota, not one rounded up to the next core (BRAIN-695: bookings are caps)', () => {
  for (const [grant, quota] of [[1.5, '150%'], [0.5, '50%'], [2.25, '225%']]) {
    const { fn, calls } = recordingSpawn();
    run(fn, { grantCores: grant });
    assert.ok(calls[0].args.includes(`CPUQuota=${quota}`), `${grant} cores -> ${quota}`);
  }
});

test('memory accounting is switched on in the scope only when the memory probe passed', () => {
  const on = recordingSpawn();
  run(on.fn, { resolved: { ...ON, memory: { mode: 'measure', available: true, reason: 'ok' } } });
  assert.ok(on.calls[0].args.includes('MemoryAccounting=yes'));
  assert.ok(!on.calls[0].args.some((a) => /MemoryMax/.test(a)), 'measured, never capped');
  const off = recordingSpawn();
  run(off.fn, { resolved: { ...ON, memory: { mode: 'measure', available: false, reason: 'x' } } });
  assert.ok(!off.calls[0].args.includes('MemoryAccounting=yes'));
  assert.ok(off.calls[0].args.includes('CPUQuota=200%'), 'CPU enforcement does not depend on memory');
});

test('not wrapped when off, when prefer found no support, on darwin, or when there is no usable grant', () => {
  const cases = [
    { resolved: resolveCpuLimit({ cpuLimit: 'off' }) },
    { resolved: { mode: 'prefer', available: false, reason: 'systemd-run-missing' } },
    { resolved: ON, platform: 'darwin' },
    { resolved: undefined },
    { grantCores: undefined },
  ];
  for (const over of cases) {
    const { fn, calls } = recordingSpawn();
    const { unit } = run(fn, over);
    assert.equal(unit, null, JSON.stringify(over));
    assert.deepEqual(calls, [{ cmd: '/usr/bin/nice', args: ['-n', '5', 'npm', 'test'], options: OPTIONS }], JSON.stringify(over));
  }
});

test('the wrapped child keeps its exit status, cwd and env (a stand-in systemd-run execs the command, as --scope does)', async (t) => {
  const dir = makeTmpDir('lane-broker-cpu-scope-', t);
  const seen = path.join(dir, 'seen');
  fs.writeFileSync(path.join(dir, 'systemd-run'), `#!/bin/sh\necho "$@" > "${seen}"\nwhile [ "$1" != "--" ]; do shift; done\nshift\nexec "$@"\n`, { mode: 0o755 });
  const { child, unit } = run(spawn, {
    cmd: 'sh',
    args: ['-c', 'echo "$PWD $LANE_BROKER_CPU_CORES"; exit 7'],
    options: { cwd: fs.realpathSync(dir), stdio: ['ignore', 'pipe', 'pipe'], env: { PATH: `${dir}:${process.env.PATH}`, LANE_BROKER_CPU_CORES: '2' } },
  });
  let out = '';
  child.stdout.on('data', (c) => (out += c));
  const code = await new Promise((resolve) => child.on('exit', resolve));
  assert.equal(code, 7);
  assert.equal(out.trim(), `${fs.realpathSync(dir)} 2`);
  assert.match(fs.readFileSync(seen, 'utf8'), new RegExp(`--unit=${unit} .*CPUQuota=200%`));
});

test('the unit name is recorded on the running lease only when a scope was made', () => {
  const lease = { id: LEASE, grantedCpuCores: 2 };
  const wrapped = runningLease(lease, { childPgid: 42, cpuScopeUnit: cpuScopeUnit(LEASE), startedAt: 1 });
  assert.equal(wrapped.cpuScopeUnit, `lane-${LEASE}.scope`);
  assert.equal(wrapped.childPgid, 42);
  assert.equal(wrapped.grantedCpuCores, 2);
  assert.equal('cpuScopeUnit' in runningLease(lease, { childPgid: 42, cpuScopeUnit: null, startedAt: 1 }), false);
});

const probe = process.platform === 'linux' ? resolveCpuLimit({ cpuLimit: 'require' }) : { available: false, reason: `needs Linux, this is ${process.platform}` };
const SKIP = probe.available ? false : `no CPUQuota scope here: ${probe.reason}`;

// Burns one core in this process and one in a detached grandchild (so a process-group kill could not reach it) for BURN_MS,
// then reports the scope's own cgroup and the cores it used, from cpu.stat.
const BURNER = `
const fs = require('fs');
const { spawn } = require('child_process');
const BURN_MS = 3000;
const burn = (ms) => { const end = Date.now() + ms; while (Date.now() < end); };
if (process.argv[2] === 'grandchild') { burn(BURN_MS); process.exit(0); }
const cgroup = fs.readFileSync('/proc/self/cgroup', 'utf8').trim().split('\\n').pop().split(':').pop();
const usage = () => Number(/usage_usec (\\d+)/.exec(fs.readFileSync('/sys/fs/cgroup' + cgroup + '/cpu.stat', 'utf8'))[1]);
const before = usage();
const startedAt = Date.now();
const grandchild = spawn(process.execPath, [__filename, 'grandchild'], { detached: true, stdio: 'inherit' });
burn(BURN_MS);
grandchild.on('exit', () => {
  const elapsedUs = (Date.now() - startedAt) * 1000;
  console.log(JSON.stringify({ cgroup, cores: (usage() - before) / elapsedUs }));
});
`;

test('CPU_GRANT_ENFORCED: a grant-1 scope holds a CPU burner and its detached grandchild to about one core', { skip: SKIP }, async (t) => {
  const dir = makeTmpDir('lane-broker-cpu-grant-', t);
  const script = path.join(dir, 'burner.js');
  fs.writeFileSync(script, BURNER);
  const leaseId = `cpu-grant-${process.pid}`;
  const { child, unit } = spawnInCpuScope(spawn, {
    resolved: probe,
    leaseId,
    grantCores: 1,
    cmd: process.execPath,
    args: [script],
    options: { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] },
  });
  let out = '';
  let err = '';
  child.stdout.on('data', (c) => (out += c));
  child.stderr.on('data', (c) => (err += c));
  const code = await new Promise((resolve) => child.on('exit', resolve));
  assert.equal(code, 0, err);
  const { cgroup, cores } = JSON.parse(out);
  assert.ok(cgroup.endsWith(`/${unit}`), `the burner ran in ${cgroup}, not in scope ${unit}`);
  // two busy processes would use about 2.0 cores unconstrained
  assert.ok(cores <= 1.2, `scope used ${cores.toFixed(2)} cores at grant 1`);
  assert.ok(cores >= 0.5, `scope used only ${cores.toFixed(2)} cores: the burner did not run`);
});

test('a grant below the minimum cannot be enforced: unwrapped under prefer, CpuLimitUnavailableError under require or maxExecutionCpuCores', () => {
  const { fn, calls } = recordingSpawn();
  const { unit } = run(fn, { grantCores: 0.005 });
  assert.equal(unit, null);
  assert.equal(calls[0].cmd, '/usr/bin/nice');
  assert.throws(() => run(fn, { grantCores: 0.005, resolved: { ...ON, mode: 'require' } }), (e) => e instanceof CpuLimitUnavailableError && /0\.005/.test(e.message));
  assert.throws(() => run(fn, { grantCores: 0.005, requireQuota: true }), CpuLimitUnavailableError);
});
