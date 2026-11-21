import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { describe, test, after } from 'node:test';
import { makeTmpDir } from './helpers/tmp.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { sleep } from './helpers.js';

const unrelated = [];
after(() => { for (const child of unrelated) try { process.kill(-child.pid, 'SIGKILL'); } catch { /* gone */ } });

const alive = (pid) => { try { process.kill(pid, 0); return true; } catch { return false; } };

/** A live process in its own process group that belongs to no lease: what a reused pid or pgid looks like. */
function strangerProcess() {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: true, stdio: 'ignore' });
  child.unref();
  unrelated.push(child);
  return child;
}

/** A remote root and broker state holding one lane `lane-1` with the given lease fields. */
function world(lease) {
  const state = makeTmpDir('guard-state-');
  const remote = makeTmpDir('guard-remote-');
  fs.mkdirSync(path.join(remote, 'tickets', 'ex.e1'), { recursive: true });
  fs.writeFileSync(path.join(remote, 'tickets', 'ex.e1', 'remote-id'), '5d8c54f7-3540-4c44-9fcf-cbee1c6c5cde');
  const leases = path.join(state, 'leases');
  fs.mkdirSync(leases, { recursive: true });
  const leaseFile = path.join(leases, '5d8c54f7-3540-4c44-9fcf-cbee1c6c5cde.json');
  if (lease) fs.writeFileSync(leaseFile, JSON.stringify({ id: '5d8c54f7-3540-4c44-9fcf-cbee1c6c5cde', key: 'k', resultPath: path.join(state, 'result.json'), descendants: [], ...lease }));
  return { state, remote, leaseFile };
}

async function stopWith(load, w, record = { execId: 'ex.e1' }) {
  const saved = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = w.state;
  try {
    const { forceStopRun } = await load('agent/guard-stop.js');
    return await forceStopRun({ remoteRoot: w.remote, record, deadline: Date.now() + 3000 });
  } finally {
    if (saved === undefined) delete process.env.LANE_BROKER_STATE; else process.env.LANE_BROKER_STATE = saved;
  }
}

const GUARDS = {
  'a supervisor whose start time is unknown is never signalled and the stop is unproven': async (load) => {
    const stranger = strangerProcess();
    const w = world({ supervisorPid: stranger.pid, supervisorStart: null, childPgid: 2 ** 22 - 7 });
    assert.equal(await stopWith(load, w).catch(() => false), false);
    await sleep(300);
    assert.equal(alive(stranger.pid), true, 'the unknown-identity process got no SIGKILL');
  },

  'a pid whose readable start time differs is not the supervisor: no SIGKILL for it': async (load) => {
    const stranger = strangerProcess();
    const w = world({ supervisorPid: stranger.pid, supervisorStart: 'Mon Jan  1 00:00:00 1990', childPgid: 2 ** 22 - 7 });
    await stopWith(load, w).catch(() => false);
    await sleep(300);
    assert.equal(alive(stranger.pid), true, 'the reused pid was left alone');
  },

  'a process group that no live lease member proves is ours is not signalled, and the stop is unproven': async (load) => {
    const stranger = strangerProcess();
    const w = world({ supervisorPid: 2 ** 22 - 9, supervisorStart: 'Mon Jan  1 00:00:00 1990', childPgid: stranger.pid });
    assert.equal(await stopWith(load, w).catch(() => false), false);
    await sleep(300);
    assert.equal(alive(stranger.pid), true, 'the reused pgid got no group signal');
  },

  'a lease that cannot be read is not proof: the read error surfaces and nothing is killed': async (load) => {
    const stranger = strangerProcess();
    const w = world(null);
    fs.mkdirSync(w.leaseFile);
    await assert.rejects(() => stopWith(load, w, { execId: 'ex.e1', pid: stranger.pid }));
    assert.equal(alive(stranger.pid), true, 'killing the publisher alone is never the answer');
  },

  'an absent lease is unproven and signals nothing': async (load) => {
    const stranger = strangerProcess();
    const w = world(null);
    assert.equal(await stopWith(load, w, { execId: 'ex.e1', pid: stranger.pid }), false);
    assert.equal(alive(stranger.pid), true);
  },
};

const CANARIES = [
  ['a supervisor whose start time is unknown is never signalled and the stop is unproven', 'if (!isVerifiedProcess(pid, start)) return false;', ''],
  ['a process group that no live lease member proves is ours is not signalled, and the stop is unproven', 'groupLive && groupHasLeaseMember(lease)', 'groupLive'],
  ['a lease that cannot be read is not proof: the read error surfaces and nothing is killed', 'if (err.code === \'ENOENT\') return null;\n    throw err;', 'return null;'],
];
const FILE_OF = { 'a process group that no live lease member proves is ours is not signalled, and the stop is unproven': 'cancel.js' };

describe('agent forced stop: no signal and no proof without positive evidence', { timeout: 120_000 }, () => {
  for (const [name, guard] of Object.entries(GUARDS)) test(name, () => guard(realSrc));
  for (const [name, from, to] of CANARIES) {
    test(`canary: with "${from.slice(0, 40)}" removed, "${name}" fails on its own assertion`, async (t) => {
      const failure = await assertCanaryFails(`${FILE_OF[name] ? '' : 'agent/'}${FILE_OF[name] ?? 'guard-stop.js'}`, [from, to], (load) => GUARDS[name](load));
      t.diagnostic(`failed as expected: ${failure.message.split('\n')[0]}`);
    });
  }

  test('the workload tree re-reads the supervisor\'s recorded descendants on every sample', async () => {
    const w = world({ supervisorPid: 1, supervisorStart: null, childPgid: 100 });
    const { WorkloadTree } = await realSrc('agent/run-guard.js');
    const row = (pid, ppid, token) => ({ pid, ppid, pgid: pid, token, marked: false, markedLeases: [] });
    const table = [row(100, 1, 't100'), row(200, 1, 't200')];
    const tree = new WorkloadTree({ root: w.state, readLeaseId: () => '5d8c54f7-3540-4c44-9fcf-cbee1c6c5cde' });
    assert.deepEqual(tree.pids(table), [100]);
    const lease = JSON.parse(fs.readFileSync(w.leaseFile, 'utf8'));
    fs.writeFileSync(w.leaseFile, JSON.stringify({ ...lease, descendants: [{ pid: 200, token: 't200' }] }));
    assert.deepEqual(tree.pids(table), [100, 200]);
  });

  test('canary: without the per-sample adopt, the later-recorded worker is missed', async () => {
    const w = world({ supervisorPid: 1, supervisorStart: null, childPgid: 100 });
    const load = await (await import('./helpers/mutant-module.js')).mutateSrcTree('agent/run-guard.js', ['else this.#tracker.adopt(lease.descendants);', '']);
    const { WorkloadTree } = await load('agent/run-guard.js');
    const row = (pid, ppid, token) => ({ pid, ppid, pgid: pid, token, marked: false, markedLeases: [] });
    const table = [row(100, 1, 't100'), row(200, 1, 't200')];
    const tree = new WorkloadTree({ root: w.state, readLeaseId: () => '5d8c54f7-3540-4c44-9fcf-cbee1c6c5cde' });
    tree.pids(table);
    const lease = JSON.parse(fs.readFileSync(w.leaseFile, 'utf8'));
    fs.writeFileSync(w.leaseFile, JSON.stringify({ ...lease, descendants: [{ pid: 200, token: 't200' }] }));
    assert.notDeepEqual(tree.pids(table), [100, 200]);
  });
});
