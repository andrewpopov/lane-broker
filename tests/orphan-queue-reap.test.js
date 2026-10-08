import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { enqueue, tryStart, listQueue, isQueuedSupervisorGone } from '../src/scheduler.js';
import { loadGlobalConfig, resolveTicketConfig } from '../src/config.js';
import { paths } from '../src/state.js';
import { processStartTime } from '../src/lease.js';

// BRAIN-457: a queued ticket whose supervisor died (host restart) is never polled again; it must be reaped by whoever looks next.

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

function setup() {
  const fx = freshEnv();
  writeGlobalConfig(fx.home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, maxRemoteQueue: 2 });
  const repoDir = path.join(fx.base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = fx.home;
  const restore = () => {
    if (prev === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prev;
  };
  const resolved = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
  let n = 0;
  const ticket = (id, supervisor) => {
    n += 1;
    return {
      id,
      key: resolved.key,
      conflicts: resolved.conflicts,
      weight: resolved.weight,
      supervisorStart: null,
      ...supervisor,
      cwd: repoDir,
      cmd: ['true'],
      logPath: path.join(fx.base, `${id}.log`),
      resultPath: path.join(fx.base, `${id}.json`),
      createdAt: Date.now() + n,
    };
  };
  return { ...fx, restore, ticket, globalCfg: loadGlobalConfig() };
}

function assertReaped(fx, id) {
  assert.equal(listQueue(fx.state).find((t) => t && t.id === id), undefined, `${id} must be dequeued`);
  const result = JSON.parse(fs.readFileSync(path.join(fx.base, `${id}.json`), 'utf8'));
  assert.equal(result.exit, 75);
  assert.equal(result.reason, 'supervisor gone before admission (host restart?)');
  const rows = fs.readFileSync(paths(fx.state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.ok(rows.find((r) => r.id === id && r.dequeuedDeadSupervisor), 'history row written');
}

test('a dead-supervisor queued ticket is reaped by the next tryStart of another ticket', async () => {
  const fx = setup();
  try {
    const dead = fx.ticket('orphan', { supervisorPid: await deadPid() });
    const live = fx.ticket('live', { supervisorPid: process.pid });
    await enqueue(fx.state, dead);
    await enqueue(fx.state, live);
    await tryStart(fx.state, live, fx.globalCfg);
    assertReaped(fx, 'orphan');
  } finally {
    fx.restore();
  }
});

test('lane status reaps a dead-supervisor queued ticket', async () => {
  const fx = setup();
  try {
    await enqueue(fx.state, fx.ticket('orphan', { supervisorPid: await deadPid() }));
    const out = await laneRun(['status', '--json'], { env: fx.env });
    assert.equal(out.code, 0, out.stderr);
    assert.equal(JSON.parse(out.stdout).queued.length, 0);
    assertReaped(fx, 'orphan');
  } finally {
    fx.restore();
  }
});

test('remote-probe reaps orphans so they stop counting toward maxRemoteQueue', async () => {
  const fx = setup();
  try {
    for (const id of ['o1', 'o2', 'o3']) await enqueue(fx.state, fx.ticket(id, { supervisorPid: await deadPid() }));
    await enqueue(fx.state, fx.ticket('alive', { supervisorPid: process.pid }));
    const out = await laneRun(['remote-probe'], { env: fx.env });
    assert.equal(out.code, 0, out.stderr);
    const probe = JSON.parse(out.stdout);
    assert.equal(probe.queued, 1, 'only the live ticket counts; 3 orphans would have been over maxRemoteQueue 2');
    for (const id of ['o1', 'o2', 'o3']) assertReaped(fx, id);
  } finally {
    fx.restore();
  }
});

test('a live pid with a mismatched start time counts as dead', async () => {
  const fx = setup();
  try {
    await enqueue(fx.state, fx.ticket('reused', { supervisorPid: process.pid, supervisorStart: 'Thu Jan  1 00:00:00 1970' }));
    await laneRun(['status', '--json'], { env: fx.env });
    assertReaped(fx, 'reused');
  } finally {
    fx.restore();
  }
});

test('a differing stored bootId counts as dead even with a live matching pid', async () => {
  const fx = setup();
  try {
    const start = processStartTime(process.pid);
    await enqueue(fx.state, fx.ticket('rebooted', { supervisorPid: process.pid, supervisorStart: start, bootId: 'some-old-boot' }));
    await laneRun(['status', '--json'], { env: fx.env });
    assertReaped(fx, 'rebooted');
  } finally {
    fx.restore();
  }
});

test('a live supervisor is not reaped', async () => {
  const fx = setup();
  try {
    await enqueue(fx.state, fx.ticket('alive', { supervisorPid: process.pid, supervisorStart: processStartTime(process.pid) }));
    await laneRun(['status', '--json'], { env: fx.env });
    const probe = JSON.parse((await laneRun(['remote-probe'], { env: fx.env })).stdout);
    assert.equal(probe.queued, 1);
    assert.ok(listQueue(fx.state).find((t) => t && t.id === 'alive'));
    assert.equal(fs.existsSync(path.join(fx.base, 'alive.json')), false);
  } finally {
    fx.restore();
  }
});

test('indeterminate liveness (ps cannot run) is not reaped', () => {
  const prevPath = process.env.PATH;
  try {
    process.env.PATH = path.join(freshEnv().base, 'empty');
    const t = { supervisorPid: process.pid, supervisorStart: 'Thu Jan  1 00:00:00 1970' };
    assert.equal(isQueuedSupervisorGone(t, 'b'), false);
  } finally {
    process.env.PATH = prevPath;
  }
});

test('a just-enqueued ticket with no recorded supervisor identity is not reaped by lane status', async () => {
  const fx = setup();
  try {
    const t = fx.ticket('handoff', {});
    delete t.supervisorStart;
    await enqueue(fx.state, t);
    await laneRun(['status', '--json'], { env: fx.env });
    assert.ok(listQueue(fx.state).find((q) => q && q.id === 'handoff'), 'still queued');
    assert.equal(fs.existsSync(path.join(fx.base, 'handoff.json')), false);
  } finally {
    fx.restore();
  }
});

test('a differing bootId is dead only when the current bootId is known too', () => {
  const t = { supervisorPid: process.pid, supervisorStart: processStartTime(process.pid), bootId: 'old' };
  assert.equal(isQueuedSupervisorGone(t, 'new'), true);
  assert.equal(isQueuedSupervisorGone(t, ''), false, 'unknown current boot is indeterminate');
  assert.equal(isQueuedSupervisorGone(t, undefined === null ? 'x' : null), false);
});

test('a result that cannot be published leaves the ticket queued for a retry', async () => {
  const fx = setup();
  try {
    const t = fx.ticket('stuck', { supervisorPid: await deadPid() });
    t.resultPath = '/dev/null';
    await enqueue(fx.state, t);
    await laneRun(['status', '--json'], { env: fx.env });
    assert.ok(listQueue(fx.state).find((q) => q && q.id === 'stuck'), 'not dequeued without a published result');
  } finally {
    fx.restore();
  }
});
