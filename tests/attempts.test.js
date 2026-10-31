import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { freshEnv } from './helpers.js';
import {
  createAttempt,
  readAttempt,
  listAttempts,
  updateAttempt,
  fallbackToLocal,
  publishTerminal,
  supervisorAlive,
} from '../src/attempts.js';
import { paths, atomicWriteFile, processStartTime } from '../src/state.js';

async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

function writeCancelMarker(root, id) {
  const dir = paths(root).cancel;
  fs.mkdirSync(dir, { recursive: true });
  atomicWriteFile(path.join(dir, id), String(Date.now()));
}

test('createAttempt writes a readable record with generation 0, phase probe, executor remote', async () => {
  const { state } = freshEnv();
  const attempt = await createAttempt(state, 'abc', { runner: 'skybox' });
  assert.equal(attempt.generation, 0);
  assert.equal(attempt.executor, 'remote');
  assert.equal(attempt.phase, 'probe');
  assert.equal(attempt.runner, 'skybox');
  assert.equal(attempt.id, 'abc');
  assert.ok(attempt.supervisor.pid === process.pid);

  const read = readAttempt(state, 'abc');
  assert.deepEqual(read, attempt);
});

test('createAttempt records the submitter logPath and cwd (BRAIN-462: a crashed caller finds its remote attempt by --log path)', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'with-log', { logPath: '/tmp/x/shard-2.log', cwd: '/repo' });
  const read = readAttempt(state, 'with-log');
  assert.equal(read.logPath, '/tmp/x/shard-2.log');
  assert.equal(read.cwd, '/repo');
  await createAttempt(state, 'without-log', {});
  const bare = readAttempt(state, 'without-log');
  assert.equal(Object.hasOwn(bare, 'logPath'), false, 'absent when not supplied');
});

test('createAttempt stores the supervisor kernelBootId, and omits it when the host has none', async () => {
  const { state } = freshEnv();
  const saved = process.env.LANE_BROKER_KERNEL_BOOT_ID;
  process.env.LANE_BROKER_KERNEL_BOOT_ID = 'kernel-boot-xyz';
  try {
    assert.equal((await createAttempt(state, 'with-kernel-boot', {})).supervisor.kernelBootId, 'kernel-boot-xyz');
    assert.equal(readAttempt(state, 'with-kernel-boot').supervisor.kernelBootId, 'kernel-boot-xyz');
  } finally {
    if (saved === undefined) delete process.env.LANE_BROKER_KERNEL_BOOT_ID;
    else process.env.LANE_BROKER_KERNEL_BOOT_ID = saved;
  }
});

test('listAttempts lists every record present, and an empty state dir lists none', async () => {
  const { state } = freshEnv();
  assert.deepEqual(listAttempts(state), []);
  await createAttempt(state, 'one', {});
  await createAttempt(state, 'two', { runner: 'mac-grandy' });
  const ids = listAttempts(state).map((a) => a.id).sort();
  assert.deepEqual(ids, ['one', 'two']);
});

test('updateAttempt applies a patch when the generation matches', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'id1', {});
  const result = await updateAttempt(state, 'id1', 0, { phase: 'transfer', runner: 'skybox' });
  assert.equal(result.ok, true);
  assert.equal(result.attempt.phase, 'transfer');
  assert.equal(readAttempt(state, 'id1').phase, 'transfer');
  // generation is untouched by a plain updateAttempt (only fallbackToLocal advances it).
  assert.equal(readAttempt(state, 'id1').generation, 0);
});

test('updateAttempt refuses (the generation fence) when the stored generation no longer matches', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'id2', {});
  // Advance the generation out from under a stale caller.
  await fallbackToLocal(state, 'id2', 'runner down');
  assert.equal(readAttempt(state, 'id2').generation, 1);

  const stale = await updateAttempt(state, 'id2', 0, { phase: 'running' });
  assert.equal(stale.ok, false, 'a writer still holding generation 0 must be refused once the record moved to generation 1');
  assert.equal(readAttempt(state, 'id2').phase, 'queued', 'the refused patch must never have been applied');
});

test('fallbackToLocal advances the generation, sets executor local, phase queued, records the reason', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'id3', { runner: 'skybox' });
  const result = await fallbackToLocal(state, 'id3', 'runner unreachable');
  assert.equal(result.ok, true);
  assert.equal(result.attempt.generation, 1);
  assert.equal(result.attempt.executor, 'local');
  assert.equal(result.attempt.phase, 'queued');
  assert.equal(result.attempt.fallbackReason, 'runner unreachable');
});

test('fallbackToLocal is refused once the ticket has been cancelled', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'id4', {});
  writeCancelMarker(state, 'id4');
  const result = await fallbackToLocal(state, 'id4', 'runner down');
  assert.deepEqual(result, { ok: false, cancelled: true });
  assert.equal(readAttempt(state, 'id4').generation, 0, 'a refused fallback must never advance the generation');
});

test('publishTerminal writes the cancelled outcome when the cancel marker exists, regardless of generation', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'id5', {});
  writeCancelMarker(state, 'id5');
  let written = null;
  const result = await publishTerminal(state, 'id5', 0, (outcome) => {
    written = outcome;
  });
  assert.equal(result.ok, true);
  assert.equal(result.cancelled, true);
  assert.deepEqual(written, { cancelled: true });
  assert.equal(readAttempt(state, 'id5'), null, 'the attempt record must be removed once its terminal outcome is published');
});

test('publishTerminal calls resultWriterFn with the real outcome and removes the record when not cancelled', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'id6', {});
  let written = null;
  const result = await publishTerminal(state, 'id6', 0, (outcome) => {
    written = outcome;
  });
  assert.equal(result.ok, true);
  assert.equal(result.cancelled, false);
  assert.deepEqual(written, { cancelled: false });
  assert.equal(readAttempt(state, 'id6'), null);
});

test('publishTerminal with a stale generation refuses and never calls resultWriterFn', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'id7', {});
  await fallbackToLocal(state, 'id7', 'runner down'); // now generation 1
  let called = false;
  const result = await publishTerminal(state, 'id7', 0, () => {
    called = true;
  });
  assert.equal(result.ok, false);
  assert.equal(called, false);
  assert.ok(readAttempt(state, 'id7'), 'a refused publish must never remove the attempt record');
});

test('publishTerminal on an absent attempt with a cancel marker present never calls the writer and reports alreadyPublished', async () => {
  const { state } = freshEnv();
  writeCancelMarker(state, 'gone1');
  let called = false;
  const result = await publishTerminal(state, 'gone1', 0, () => {
    called = true;
  });
  assert.deepEqual(result, { ok: false, alreadyPublished: true });
  assert.equal(called, false);
});

test('publishTerminal on an absent attempt without a cancel marker reports alreadyPublished and never calls the writer', async () => {
  const { state } = freshEnv();
  let called = false;
  const result = await publishTerminal(state, 'gone2', 0, () => {
    called = true;
  });
  assert.deepEqual(result, { ok: false, alreadyPublished: true });
  assert.equal(called, false);
});

for (const order of [['cancelled', 'plain'], ['plain', 'cancelled']]) {
  test(`two publishTerminal calls for one id (${order.join(' then ')}) call the writer exactly once`, async () => {
    const { state } = freshEnv();
    const id = `twice-${order[0]}`;
    await createAttempt(state, id, {});
    const writes = [];
    const publish = async (kind) => {
      // The cancel marker is durable, so it is already visible to whichever publisher comes second.
      if (kind === 'cancelled') writeCancelMarker(state, id);
      return publishTerminal(state, id, 0, (outcome) => {
        writes.push({ kind, outcome });
      });
    };
    const first = await publish(order[0]);
    const second = await publish(order[1]);
    assert.equal(first.ok, true);
    assert.deepEqual(second, { ok: false, alreadyPublished: true });
    assert.equal(writes.length, 1, 'the second publisher must not overwrite the first result');
    assert.equal(writes[0].kind, order[0]);
  });
}

test('publishTerminal: expect returning false on either branch writes nothing and keeps the attempt', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'exp1', {});
  let called = false;
  const plain = await publishTerminal(state, 'exp1', 0, () => {
    called = true;
  }, { expect: () => false });
  assert.deepEqual(plain, { ok: false, stale: true });
  writeCancelMarker(state, 'exp1');
  const cancelled = await publishTerminal(state, 'exp1', 0, () => {
    called = true;
  }, { expect: () => false });
  assert.deepEqual(cancelled, { ok: false, stale: true });
  assert.equal(called, false);
  assert.ok(readAttempt(state, 'exp1'), 'a stale publish must never remove the attempt record');
});

test('publishTerminal: expect is evaluated against the record read under the lock, and true publishes as before', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'exp2', { runner: 'skybox' });
  let seen = null;
  const result = await publishTerminal(state, 'exp2', 0, () => {}, {
    expect: (current) => {
      seen = current;
      return current.runner === 'skybox';
    },
  });
  assert.equal(result.ok, true);
  assert.equal(seen.id, 'exp2');
  assert.equal(readAttempt(state, 'exp2'), null);
});

test('supervisorAlive is false for a dead pid', async () => {
  const { state } = freshEnv();
  const pid = await deadPid();
  const attempt = await createAttempt(state, 'id8', {});
  attempt.supervisor.pid = pid;
  attempt.supervisor.startTime = null;
  assert.equal(supervisorAlive(attempt), false);
});

test('supervisorAlive is false for a reused pid whose start time no longer matches', async () => {
  const { state } = freshEnv();
  const attempt = await createAttempt(state, 'id9', {});
  attempt.supervisor.pid = process.pid; // genuinely alive
  attempt.supervisor.startTime = 'Thu Jan  1 00:00:00 1970'; // bogus, must not match
  assert.equal(supervisorAlive(attempt), false);
});

test('supervisorAlive is true for the real pid and start time of a live process', async () => {
  const { state } = freshEnv();
  const attempt = await createAttempt(state, 'id10', {});
  const realStart = processStartTime(process.pid);
  attempt.supervisor.pid = process.pid;
  attempt.supervisor.startTime = realStart;
  assert.equal(supervisorAlive(attempt), true);
});

test('supervisorAlive is false for a different boot id, even with a live pid and matching start time', async () => {
  const { state } = freshEnv();
  const attempt = await createAttempt(state, 'id11', {});
  const realStart = processStartTime(process.pid);
  attempt.supervisor.pid = process.pid;
  attempt.supervisor.startTime = realStart;
  attempt.supervisor.bootId = 'a-previous-boot-id';
  assert.equal(supervisorAlive(attempt), false);
});

test('an attempt record survives a simulated reboot (boot id change) rather than being reaped', async () => {
  const { state } = freshEnv();
  const attempt = await createAttempt(state, 'id12', {});
  const prevBootIdEnv = process.env.LANE_BROKER_BOOT_ID;
  process.env.LANE_BROKER_BOOT_ID = 'a-brand-new-boot-id';
  try {
    // Nothing in this module reaps on boot change (unlike lease.js's
    // reapIfStale) -- the record must still be readable, just reported as
    // an orphan via supervisorAlive, for a future reconciler to act on.
    assert.ok(readAttempt(state, 'id12'), 'the attempt record must not have been removed by a boot change alone');
    assert.equal(supervisorAlive({ ...attempt, supervisor: { ...attempt.supervisor } }), false);
  } finally {
    if (prevBootIdEnv === undefined) delete process.env.LANE_BROKER_BOOT_ID;
    else process.env.LANE_BROKER_BOOT_ID = prevBootIdEnv;
  }
});

test('publishTerminal refuses a writer that returns a Promise, in both the cancelled and non-cancelled branch', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'idAsyncA', {});
  await assert.rejects(
    () => publishTerminal(state, 'idAsyncA', 0, () => Promise.resolve()),
    /must be synchronous/,
    'a non-cancelled async writer must be refused',
  );
  // Refused: the attempt must survive, untouched by the rejected call.
  assert.ok(readAttempt(state, 'idAsyncA'), 'a refused publish must never remove the attempt record');

  await createAttempt(state, 'idAsyncB', {});
  const dir = paths(state).cancel;
  fs.mkdirSync(dir, { recursive: true });
  atomicWriteFile(path.join(dir, 'idAsyncB'), String(Date.now()));
  await assert.rejects(
    () => publishTerminal(state, 'idAsyncB', 0, () => Promise.resolve()),
    /must be synchronous/,
    'a cancelled-branch async writer must also be refused',
  );
  assert.ok(readAttempt(state, 'idAsyncB'), 'a refused cancelled-branch publish must never remove the attempt record either');
});
