import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, gitFixture, waitFor } from './helpers.js';
import { tmpDir, setup } from './remote-harness.js';
import { spawn } from 'node:child_process';
import { paths, withLock, bootId } from '../src/state.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { loadGlobalConfig } from '../src/config.js';
import { createAttempt, patchAttemptLocked } from '../src/attempts.js';
import { cancelCommand } from '../src/cancel.js';
import { readLease, writeLease } from '../src/lease.js';
import { foldObservedCpu, integratedCpuSeconds } from '../src/observed.js';

/** BRAIN-425: the shape of a finished run's history.jsonl row, per kind of run. */

const GIB = 1024 ** 3;
const busyCmd = (ms, exit = 0) => [process.execPath, '-e', `const end=Date.now()+${ms};while(Date.now()<end){} process.exit(${exit})`];
const rows = (root) => fs.readFileSync(paths(root).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
const lastRow = (root) => rows(root).at(-1);

function localEnv(sampleMs) {
  const f = freshEnv();
  writeGlobalConfig(f.home, { version: 1, capacity: 100, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs });
  const repoDir = path.join(f.base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 2, memoryBytes: GIB } } });
  return { ...f, repoDir };
}
const run = (f, cmd) => laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...cmd], { env: f.env, cwd: f.repoDir });

test('integratedCpuSeconds closes both ends of the sampled span; undefined with no observation', () => {
  let s;
  for (const [at, cores] of [[1000, 2], [3000, 2]]) s = foldObservedCpu(s, cores, at);
  // area 2 cores x 2s = 4; head: 1s before the first sample at 2 cores = 2; tail: 1s after the last at 2 cores = 2
  assert.equal(integratedCpuSeconds(s, 0, 4000), 8);
  assert.equal(integratedCpuSeconds(s, 1000, 3000), 4, 'no head or tail when the run is exactly the sampled span');
  assert.equal(integratedCpuSeconds(undefined, 0, 1000), undefined);
});

test('local success: exit 0, signal null, the grant, cpuSeconds', async () => {
  const f = localEnv(100);
  const result = await run(f, busyCmd(1200));
  assert.equal(result.code, 0, result.stderr);
  const row = lastRow(f.state);
  assert.equal(row.exit, 0);
  assert.equal(row.signal, null);
  assert.equal(row.grantedCpuCores, 2);
  assert.equal(row.grantedMemoryBytes, GIB);
  assert.ok(row.cpuSeconds > 0.3 && row.cpuSeconds < 5, `cpuSeconds ${row.cpuSeconds}`);
});

test('short run (shorter than sampleMs) still gets observedCpu from the early sample', async () => {
  const f = localEnv(60_000);
  const result = await run(f, busyCmd(1800));
  assert.equal(result.code, 0, result.stderr);
  const row = lastRow(f.state);
  assert.ok(row.observedCpu, 'a run shorter than one heartbeat interval must still be observed');
  assert.ok(row.observedCpu.peak > 0.3, JSON.stringify(row.observedCpu));
  assert.ok(row.cpuSeconds > 0, `cpuSeconds ${row.cpuSeconds}`);
});

test('failed run: the nonzero exit is recorded with signal null', async () => {
  const f = localEnv(100);
  const result = await run(f, busyCmd(100, 3));
  assert.equal(result.code, 3);
  const row = lastRow(f.state);
  assert.equal(row.exit, 3);
  assert.equal(row.signal, null);
  assert.equal(row.grantedCpuCores, 2);
});

test('cancelled run: exit 130, signal null, cancelled true', async () => {
  const f = localEnv(100);
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '30'], { env: f.env, cwd: f.repoDir });
  const id = await waitFor(() => {
    try {
      const name = fs.readdirSync(paths(f.state).leases).find((n) => n.endsWith('.json'));
      const lease = name && readLease(f.state, name.replace(/\.json$/, ''));
      return lease?.childPgid ? lease.id : null;
    } catch {
      return null;
    }
  });
  const closed = new Promise((resolve) => child.on('close', resolve));
  const cancel = await laneRun(['cancel', id], { env: f.env, cwd: f.repoDir });
  assert.equal(cancel.code, 0, cancel.stderr);
  await closed;
  const row = rows(f.state).find((r) => r.id === id);
  assert.equal(row.cancelled, true);
  assert.equal(row.exit, 130);
  assert.equal(row.signal, null);
});

test('remote run: submitter row carries exit, grant, cpuSeconds and remotePhasesMs', async () => {
  const { env, repoDir, state } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 2, memoryBytes: 1048576, remote: true } } });
  gitFixture(['add', '-A'], repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], repoDir);
  const marker = path.join(tmpDir('marker'), 'where');
  const cmd = [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(marker)}, process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local'); const end=Date.now()+1500;while(Date.now()<end){}`];
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...cmd], { env, cwd: repoDir });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const row = lastRow(state);
  assert.equal(row.executor, 'remote');
  assert.equal(row.exit, 0);
  assert.equal(row.signal, null);
  assert.equal(row.grantedCpuCores, 2);
  assert.equal(row.grantedMemoryBytes, 1048576);
  assert.ok(row.cpuSeconds > 0, `cpuSeconds ${row.cpuSeconds}`);
  for (const key of ['uploadMs', 'snapshotMs', 'commandMs']) {
    assert.ok(Number.isFinite(row.remotePhasesMs?.[key]), `remotePhasesMs.${key}: ${JSON.stringify(row.remotePhasesMs)}`);
  }
  assert.ok(row.remotePhasesMs.commandMs >= 1000, JSON.stringify(row.remotePhasesMs));
});

test('firstCores is the first reading, not the whole-span average (4 -> 0 -> 0 cores)', () => {
  let s;
  for (const [at, cores] of [[1000, 4], [2000, 0], [3000, 0]]) s = foldObservedCpu(s, cores, at);
  // head 4 cores x 1s before the first sample + area 4 cores x 1s; the span average (2 cores) would give 6
  assert.equal(integratedCpuSeconds(s, 0, 3000), 8);
});

test('the exit path takes no process-table scan of its own: only the constructor, the early sample and the reap check', async () => {
  const f = localEnv(60_000);
  const shimDir = tmpDir('ps-shim');
  const log = path.join(shimDir, 'ps.log');
  const realPs = '/bin/ps';
  fs.writeFileSync(path.join(shimDir, 'ps'), `#!/bin/sh\necho "$PPID $*" >> ${JSON.stringify(log)}\nexec ${realPs} "$@"\n`, { mode: 0o755 });
  const env = { ...f.env, PATH: `${shimDir}${path.delimiter}${f.env.PATH}` };
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...busyCmd(2500)], { env, cwd: f.repoDir });
  const closed = new Promise((resolve) => child.on('close', resolve));
  const supervisorPid = await waitFor(() => {
    try {
      const name = fs.readdirSync(paths(f.state).leases).find((n) => n.endsWith('.json'));
      return (name && readLease(f.state, name.replace(/\.json$/, ''))?.supervisorPid) || null;
    } catch {
      return null;
    }
  });
  await closed;
  const calls = fs.readFileSync(log, 'utf8').split('\n').filter((l) => Number(l.split(' ')[0]) === supervisorPid && l.includes('pcpu=,rss=,lstart=,command=')).length;
  assert.equal(calls, 3, `supervisor process-table scans: ${calls}\n${fs.readFileSync(log, 'utf8')}`);
});

/** Run `body` with this process's broker root pointed at `state` (the in-process cancel/scheduler paths read it from the env). */
async function withState(state, body) {
  const prev = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await body();
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev;
  }
}
async function deadPid() {
  const child = spawn(process.execPath, ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}
const declared = { cpuCores: 3, memoryBytes: GIB };

test('dead-supervisor dequeue row carries the grant and exit/signal', async () => {
  const f = localEnv(100);
  await withState(f.state, async () => {
    const dead = await deadPid();
    const ticket = (id, supervisorPid, createdAt) => ({ id, key: 'r:default', conflicts: [], weight: 1, resources: declared, supervisorPid, supervisorStart: null, cwd: f.repoDir, cmd: ['true'], logPath: path.join(f.base, `${id}.log`), resultPath: path.join(f.base, `${id}.json`), createdAt });
    await enqueue(f.state, ticket('dead-head', dead, Date.now()));
    const live = ticket('live-second', process.pid, Date.now() + 1);
    await enqueue(f.state, live);
    await tryStart(f.state, live, loadGlobalConfig());
  });
  const row = rows(f.state).find((r) => r.id === 'dead-head');
  assert.equal(row.dequeuedDeadSupervisor, true);
  assert.equal(row.grantedCpuCores, 3);
  assert.equal(row.grantedMemoryBytes, GIB);
  assert.equal(row.exit, null);
  assert.equal(row.signal, null);
});

test('orphan-cancel rows (attempt only, lease + attempt, lease only) carry the grant and exit/signal', async () => {
  const f = localEnv(100);
  await withState(f.state, async () => {
    const dead = await deadPid();
    const orphanAttempt = async (id) => {
      await createAttempt(f.state, id, { resources: declared });
      await withLock(f.state, () => patchAttemptLocked(f.state, id, { supervisor: { pid: dead, startTime: null, bootId: bootId() } }));
    };
    const lease = (id) => ({ id, key: `r:${id}`, resultPath: path.join(f.base, `${id}.json`), bootId: bootId(), supervisorPid: dead, supervisorStart: null, childPgid: null, weight: 1, resources: declared, state: 'running' });
    await orphanAttempt('attempt-only');
    assert.equal((await cancelCommand('attempt-only')).exitCode, 0);
    await orphanAttempt('lease-attempt');
    writeLease(f.state, lease('lease-attempt'));
    assert.equal((await cancelCommand('lease-attempt')).exitCode, 0);
    writeLease(f.state, lease('lease-only'));
    assert.equal((await cancelCommand('lease-only')).exitCode, 0);
  });
  for (const id of ['attempt-only', 'lease-attempt', 'lease-only']) {
    const row = rows(f.state).find((r) => r.id === id);
    assert.equal(row.grantedCpuCores, 3, id);
    assert.equal(row.grantedMemoryBytes, GIB, id);
    assert.ok('exit' in row && 'signal' in row, id);
    assert.equal(row.cancelled, true, id);
  }
});
