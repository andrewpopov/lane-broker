import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { stopCpuScope, stopLeaseCpuScope, spawnInCpuScope, cpuScopeUnit, resolveCpuLimit } from '../src/cpu-limit.js';
import { cancelCommand } from '../src/cancel.js';
import { readLease, writeLease, reapIfStale, leaseFile } from '../src/lease.js';
import { paths, bootId } from '../src/state.js';

/** BRAIN-651 L3: cancel, a watchdog kill and orphan recovery stop the lane's whole CPU scope, and the lease is released only after it is empty. */

const UNIT = 'lane-orphan-1.scope';
const DEAD_PID = 2 ** 22 + 12345;
const DEAD_PGID = 2 ** 22 + 54321;

const recordingExec = (result = { status: 0, stdout: '', stderr: '' }, onCall = () => {}) => {
  const calls = [];
  const exec = (file, args) => {
    calls.push([file, ...args]);
    onCall();
    return result;
  };
  return { exec, calls };
};

function orphanedLease(state, over = {}) {
  const lease = { id: 'orphan-1', bootId: bootId(), supervisorPid: DEAD_PID, childPgid: DEAD_PGID, state: 'RUNNING', resultPath: path.join(state, 'orphan-1-result.json'), ...over };
  writeLease(state, lease);
  return lease;
}

async function withState(state, fn) {
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await fn();
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
  }
}

test('stopCpuScope runs `systemctl --user stop <unit>`; exit 0 is stopped, "not loaded" counts as already stopped', () => {
  const ok = recordingExec();
  assert.deepEqual(stopCpuScope(UNIT, { exec: ok.exec }), { stopped: true, reason: 'stopped' });
  assert.deepEqual(ok.calls, [['systemctl', '--user', 'stop', UNIT]]);
  const gone = recordingExec({ status: 5, stdout: '', stderr: `Failed to stop ${UNIT}: Unit ${UNIT} not loaded.\n` });
  assert.deepEqual(stopCpuScope(UNIT, { exec: gone.exec }), { stopped: true, reason: 'not-loaded' });
});

test('stopCpuScope reports a stop that did not complete, with the reason', () => {
  const failed = recordingExec({ status: 1, stdout: '', stderr: 'Failed to connect to bus\nmore\n' });
  assert.deepEqual(stopCpuScope(UNIT, { exec: failed.exec }), { stopped: false, reason: 'systemctl-exit-1: Failed to connect to bus' });
  const withError = (code) => stopCpuScope(UNIT, { exec: () => ({ error: Object.assign(new Error(code), { code }) }) });
  assert.deepEqual(withError('ETIMEDOUT'), { stopped: false, reason: 'timeout' });
  assert.deepEqual(withError('ENOENT'), { stopped: false, reason: 'systemctl-missing' });
});

test('stopLeaseCpuScope: a lease without a unit makes no call; a failed stop is logged naming the unit', () => {
  const none = recordingExec();
  assert.equal(stopLeaseCpuScope({ id: 'x' }, { exec: none.exec }), null);
  assert.equal(none.calls.length, 0);
  const lines = [];
  const failed = recordingExec({ status: 1, stdout: '', stderr: 'boom' });
  assert.equal(stopLeaseCpuScope({ id: 'x', cpuScopeUnit: UNIT }, { exec: failed.exec, log: (l) => lines.push(l) }).stopped, false);
  assert.match(lines.join(''), new RegExp(`unit=${UNIT} stop-incomplete reason=systemctl-exit-1: boom`));
});

test('cancel of an orphaned lease stops its scope by unit, and the lease is released only after the stop returned', async (t) => {
  const { state } = freshEnv();
  const lease = orphanedLease(state, { cpuScopeUnit: UNIT });
  const { exec, calls } = recordingExec(undefined, () => assert.ok(readLease(state, 'orphan-1'), 'the lease is still held while the scope is being stopped'));
  const res = await withState(state, () => cancelCommand(lease.id, { exec }));
  assert.equal(res.exitCode, 0);
  assert.deepEqual(calls, [['systemctl', '--user', 'stop', UNIT]]);
  assert.equal(fs.existsSync(leaseFile(state, 'orphan-1')), false);
  t.diagnostic('lease released after the stop');
});

test('cancel of an orphaned lease whose scope is not loaded releases it; one without a unit makes no call', async () => {
  const gone = freshEnv();
  const withUnit = orphanedLease(gone.state, { cpuScopeUnit: UNIT });
  const notLoaded = recordingExec({ status: 5, stdout: '', stderr: `Unit ${UNIT} not loaded.` });
  assert.equal((await withState(gone.state, () => cancelCommand(withUnit.id, { exec: notLoaded.exec }))).exitCode, 0);
  assert.equal(readLease(gone.state, 'orphan-1'), null);

  const plain = freshEnv();
  const noUnit = orphanedLease(plain.state);
  const none = recordingExec();
  assert.equal((await withState(plain.state, () => cancelCommand(noUnit.id, { exec: none.exec }))).exitCode, 0);
  assert.equal(none.calls.length, 0);
  assert.equal(readLease(plain.state, 'orphan-1'), null);
});

test('orphan reaping stops the scope by unit before it releases the lease; no unit means no call', () => {
  const { state } = freshEnv();
  const lease = orphanedLease(state, { cpuScopeUnit: UNIT });
  const { exec, calls } = recordingExec(undefined, () => assert.ok(readLease(state, 'orphan-1'), 'the lease is still held while the scope is being stopped'));
  assert.equal(reapIfStale(state, lease, bootId(), { exec }), 'reaped');
  assert.deepEqual(calls, [['systemctl', '--user', 'stop', UNIT]]);
  assert.equal(readLease(state, 'orphan-1'), null);

  const plain = freshEnv();
  const none = recordingExec();
  assert.equal(reapIfStale(plain.state, orphanedLease(plain.state), bootId(), { exec: none.exec }), 'reaped');
  assert.equal(none.calls.length, 0);
});

const onLinux = process.platform === 'linux';
const probe = onLinux ? resolveCpuLimit({ cpuLimit: 'require' }) : { available: false, reason: `needs Linux, this is ${process.platform}` };
const SKIP_REAL = probe.available ? false : `no CPUQuota scope here: ${probe.reason}`;

// stand-ins on PATH: systemd-run answers the probe, then execs the command as --scope does; systemctl only records its argv
const FAKE_SYSTEMD_RUN = (probeLine) => `#!/bin/sh\nfor a in "$@"; do [ "$a" = "--" ] && break; [ "$a" = "sh" ] && { echo "${probeLine}"; exit 0; }; done\nwhile [ "$1" != "--" ]; do shift; done\nshift\nexec "$@"\n`;

test('a silence-watchdog kill stops the scope by unit (stand-in systemd-run and systemctl, Linux only)', { skip: onLinux ? false : `needs Linux, this is ${process.platform}` }, async (t) => {
  const { base, home, state, env } = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '4000' });
  const bin = makeTmpDir('lane-broker-cpu-cancel-bin-', t);
  const log = path.join(bin, 'systemctl.log');
  fs.writeFileSync(path.join(bin, 'systemd-run'), FAKE_SYSTEMD_RUN('100000 100000'), { mode: 0o755 });
  fs.writeFileSync(path.join(bin, 'systemctl'), `#!/bin/sh\necho "$@" >> "${log}"\n`, { mode: 0o755 });
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, cpuLimit: 'prefer' });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, silenceTimeoutMs: 2000 } } });
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, '-e', `process.stdout.write('go\\n'); setInterval(() => {}, 1000)`], { env: { ...env, PATH: `${bin}:${env.PATH}` }, cwd: repoDir });
  assert.equal(res.code, 124, res.stderr);
  const [row] = fs.readFileSync(paths(state).history, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
  assert.match(fs.readFileSync(log, 'utf8'), new RegExp(`^--user stop ${cpuScopeUnit(row.id).replace(/\./g, '\\.')}$`, 'm'));
  assert.equal(fs.readdirSync(paths(state).leases).length, 0, 'the lease was released');
});

// A leader that starts a setsid'd grandchild (a different process group and session, so a group kill cannot reach it), then waits.
const SCOPE_HOLDER = `
const fs = require('fs');
const { spawn } = require('child_process');
const grandchild = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: true, stdio: 'ignore' });
grandchild.unref();
const cgroup = fs.readFileSync('/proc/self/cgroup', 'utf8').trim().split('\\n').pop().split(':').pop();
console.log(JSON.stringify({ cgroup, grandchild: grandchild.pid }));
setInterval(() => {}, 1000);
`;

const procAlive = (pid) => {
  try {
    process.kill(pid, 0);
  } catch {
    return false;
  }
  try {
    return !/^\d+ \(.*\) Z/.test(fs.readFileSync(`/proc/${pid}/stat`, 'utf8')); // a zombie awaiting its reaper is dead
  } catch {
    return false;
  }
};

test('CPU_SCOPE_EMPTY_BEFORE_RELEASE: cancel leaves the scope with no process, a detached grandchild included, by the time the lease is gone', { skip: SKIP_REAL }, async (t) => {
  const { state } = freshEnv();
  const dir = makeTmpDir('lane-broker-cpu-cancel-', t);
  const script = path.join(dir, 'holder.js');
  fs.writeFileSync(script, SCOPE_HOLDER);
  const leaseId = `cpu-cancel-${process.pid}`;
  const { child, unit } = spawnInCpuScope(spawn, { resolved: probe, leaseId, grantCores: 1, cmd: process.execPath, args: [script], options: { cwd: dir, detached: true, stdio: ['ignore', 'pipe', 'pipe'] } });
  t.after(() => spawn('systemctl', ['--user', 'stop', unit], { stdio: 'ignore' }));
  const reported = await new Promise((resolve, reject) => {
    let out = '';
    child.stdout.on('data', (c) => {
      out += c;
      if (out.includes('\n')) resolve(JSON.parse(out));
    });
    child.on('exit', () => reject(new Error('the holder exited before reporting')));
  });
  assert.ok(reported.cgroup.endsWith(`/${unit}`), `the holder ran in ${reported.cgroup}, not in scope ${unit}`);
  assert.ok(procAlive(reported.grandchild), 'the detached grandchild is running before the cancel');
  orphanedLease(state, { id: leaseId, childPgid: child.pid, cpuScopeUnit: unit, resultPath: path.join(state, 'result.json') });

  const res = await withState(state, () => cancelCommand(leaseId));
  assert.equal(res.exitCode, 0);
  assert.equal(readLease(state, leaseId), null, 'the lease is released');
  const procs = path.join('/sys/fs/cgroup', reported.cgroup, 'cgroup.procs');
  const left = fs.existsSync(procs) ? fs.readFileSync(procs, 'utf8').trim() : '';
  assert.equal(left, '', `the scope still holds processes: ${left}`);
  assert.equal(procAlive(reported.grandchild), false, 'the detached grandchild survived the cancel');
});
