import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { loadGlobalConfig } from '../src/config.js';
import { enqueue, tryStart, restoreQueued, retierQueued, listQueueStrict } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { fenceLegacyQueue } from '../src/migrate.js';
import { paths, atomicWriteJson } from '../src/state.js';
import { collectStatus, renderStatusText } from '../src/status.js';
import { claimFigures } from '../src/agent/claims.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';

/** BRAIN-1217: a zero-byte queue record (an unclean reboot) read as `null` and threw out of status and every claim round. */
async function withUnreadableRecord(fn) {
  const { state, home } = freshEnv();
  const saved = [process.env.LANE_BROKER_HOME, process.env.LANE_BROKER_STATE];
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeGlobalConfig(home, { schedulerMode: 'active', capacity: 10, maxLaneWeight: 4, cpuAdmissionPercent: 100, cpuReserveCores: 1, memoryReserveBytes: 0 });
    await enqueue(state, { id: 'queued', key: 'r:queued', repoId: 'queued', weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' });
    fs.writeFileSync(path.join(paths(state).queue, '000000000099-zero-byte.json'), '');
    await fn({ home });
  } finally {
    for (const [key, value] of [['LANE_BROKER_HOME', saved[0]], ['LANE_BROKER_STATE', saved[1]]]) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test('status counts a zero-byte queue record as unreadable and still lists the readable ones', () => withUnreadableRecord(async () => {
  const status = await collectStatus();
  assert.equal(status.unreadableQueue, 1);
  assert.deepEqual(status.queued.map((t) => t.id), ['queued']);
  assert.match(renderStatusText(status), /1 unreadable queue record/);
}));

test('a claim round offers figures past a zero-byte queue record', () => withUnreadableRecord(async ({ home }) => {
  const offered = await claimFigures({ cfg: loadGlobalConfig({ home }) });
  assert.notEqual(offered, null);
  assert.ok(Number.isFinite(offered.freeWeight));
}));

const submitted = (id) => ({ id, key: `r:${id}`, repoId: id, weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', priorityRequested: 'medium' });
const shadowCfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, conflictSafeBackfill: false };
const sampler = () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * 1024 ** 3, totalBytes: 64 * 1024 ** 3, macPressure: 'normal', source: 'test' });

function fencedRoot() {
  const { state } = freshEnv();
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: Date.now() });
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
  fenceLegacyQueue(state, 'test');
  return state;
}

test('a zero-byte record ahead of a readable ticket is quarantined by the next poll, and the ticket starts', async () => {
  const state = fencedRoot();
  const record = await enqueue(state, submitted('a'), shadowCfg);
  fs.writeFileSync(path.join(paths(state).queue, '000000000000-zero-byte.json'), '');
  assert.throws(() => listQueueStrict(state), /unreadable|not a JSON object|Unexpected end/i, 'precondition: strict reads (gc liveIds) throw on it');

  const result = await tryStart(state, record, shadowCfg, undefined, sampler, undefined, memory);

  assert.equal(result.started, true, `the readable ticket starts: ${JSON.stringify(result)}`);
  assert.deepEqual(fs.readdirSync(paths(state).queueQuarantine), ['000000000000-zero-byte.json']);
  assert.equal(fs.existsSync(path.join(paths(state).queue, '000000000000-zero-byte.json')), false);
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /unreadable-queue-record.*zero-byte.*quarantined/);
  assert.deepEqual(listQueueStrict(state).map((t) => t.id), [], 'strict reads no longer throw');
});

test('status reports a quarantined record and names no ticket as head behind a still-unreadable one', async () => {
  const state = fencedRoot();
  await enqueue(state, submitted('a'), shadowCfg);
  fs.writeFileSync(path.join(paths(state).queue, '000000000000-zero-byte.json'), '');
  const prev = [process.env.LANE_BROKER_HOME, process.env.LANE_BROKER_STATE];
  process.env.LANE_BROKER_STATE = state;
  try {
    const before = await collectStatus();
    assert.equal(before.unreadableQueue, 1);
    assert.equal(before.headBlock, null);
    assert.equal(before.resourceBlock, null);
    fs.mkdirSync(paths(state).queueQuarantine, { recursive: true });
    fs.renameSync(path.join(paths(state).queue, '000000000000-zero-byte.json'), path.join(paths(state).queueQuarantine, '000000000000-zero-byte.json'));
    const after = await collectStatus();
    assert.deepEqual([after.unreadableQueue, after.quarantinedQueue], [0, 1]);
    assert.match(renderStatusText(after), /1 queue record\(s\) in quarantine/);
  } finally {
    if (prev[1] === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prev[1];
    if (prev[0] === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prev[0];
  }
});

function assertDurableWrite(recorder, dir, target) {
  const renamedAt = recorder.indexOf('rename', target);
  const tmpFsyncAt = recorder.events.findIndex((e) => e.op === 'fsync' && e.path?.startsWith(path.join(dir, '.tmp-')));
  assert.ok(renamedAt >= 0 && tmpFsyncAt >= 0 && tmpFsyncAt < renamedAt, `${path.basename(target)} is fsynced before the rename`);
  assert.ok(recorder.indexOf('fsync', dir) > renamedAt, `${path.dirname(target)} is fsynced after the rename`);
}

test('enqueue fsyncs the queue record and the seq counter', async () => {
  const state = fencedRoot();
  const recorder = recordFsyncs();
  let record;
  try {
    record = await enqueue(state, submitted('durable'), shadowCfg);
  } finally {
    recorder.stop();
  }
  assertDurableWrite(recorder, paths(state).queue, path.join(paths(state).queue, `${String(record.seq).padStart(12, '0')}-durable.json`));
  assertDurableWrite(recorder, path.dirname(paths(state).seq), paths(state).seq);
});

test('restoring a withdrawn ticket fsyncs its rewritten queue record', async () => {
  const state = fencedRoot();
  const record = { ...submitted('back'), seq: 7, schedVersion: 2, createdAt: Date.now() };
  const recorder = recordFsyncs();
  try {
    await restoreQueued(state, record);
  } finally {
    recorder.stop();
  }
  assertDurableWrite(recorder, paths(state).queue, path.join(paths(state).queue, '000000000007-back.json'));
});

test('re-tiering a queued ticket fsyncs its rewritten queue record', async () => {
  const state = fencedRoot();
  const record = await enqueue(state, submitted('up'), shadowCfg);
  const file = path.join(paths(state).queue, `${String(record.seq).padStart(12, '0')}-up.json`);
  const recorder = recordFsyncs();
  let result;
  try {
    result = await retierQueued(state, 'up', 'high', shadowCfg);
  } finally {
    recorder.stop();
  }
  assert.equal(result.outcome, 'retiered');
  assertDurableWrite(recorder, paths(state).queue, file);
});
