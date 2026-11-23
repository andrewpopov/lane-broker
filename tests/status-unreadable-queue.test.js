import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { loadGlobalConfig } from '../src/config.js';
import { enqueue } from '../src/scheduler.js';
import { paths } from '../src/state.js';
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

test('an enqueued record is fsynced before its rename and its directory after, so a reboot cannot leave a zero-byte record', () => withUnreadableRecord(async () => {
  const root = process.env.LANE_BROKER_STATE;
  const queueDir = paths(root).queue;
  const recorder = recordFsyncs();
  let record;
  try {
    record = await enqueue(root, { id: 'durable', key: 'r:durable', repoId: 'durable', weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' });
  } finally {
    recorder.stop();
  }
  const target = path.join(queueDir, `${String(record.seq).padStart(12, '0')}-durable.json`);
  const renamedAt = recorder.indexOf('rename', target);
  const tmpFsyncAt = recorder.events.findIndex((e) => e.op === 'fsync' && e.path?.startsWith(path.join(queueDir, '.tmp-')));
  assert.ok(renamedAt >= 0 && tmpFsyncAt >= 0 && tmpFsyncAt < renamedAt, 'the record is fsynced before the rename');
  assert.ok(recorder.indexOf('fsync', queueDir) > renamedAt, 'the queue directory is fsynced after the rename');
}));
