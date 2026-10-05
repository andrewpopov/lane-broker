import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, laneRun, laneSpawn, waitFor, gitFixture } from './helpers.js';
import { enqueue, tryStart, dequeueSync, listQueue } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { fenceLegacyQueue } from '../src/migrate.js';
import { readLease } from '../src/lease.js';
import { paths, atomicWriteJson, readJsonSafe } from '../src/state.js';

/**
 * BRAIN-380 slice 4: the per-repo high cap (enforced in `enqueue`, under the state-root lock), and the admission audit
 * (lease, history row, decision log). The remote half is in priority-remote.test.js.
 */

const MIN = 60_000;
const T0 = 1_700_000_000_000;
const HERE = new URL('.', import.meta.url).pathname;
const SRC = path.join(HERE, '..', 'src');

const ticket = (id, repoId, priority, extra = {}) => ({
  id,
  key: `${repoId}:${id}`,
  repoId,
  weight: 1,
  cwd: process.cwd(),
  cmd: ['true'],
  supervisorPid: process.pid,
  supervisorStart: null,
  logPath: '/dev/null',
  resultPath: '/dev/null',
  priorityRequested: priority,
  ...extra,
});
const queued = (state) => Object.fromEntries(listQueue(state).map((t) => [t.id, t]));
const cfgWith = (overrides = {}) => ({ ...DEFAULT_GLOBAL_CONFIG, ...overrides });
const tryCfg = (overrides = {}) => cfgWith({ schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, conflictSafeBackfill: false, ...overrides });
const sampler = () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * 1024 ** 3, totalBytes: 64 * 1024 ** 3, macPressure: 'normal', source: 'test' });
const poll = (state, t, cfg) => tryStart(state, t, cfg, undefined, sampler, undefined, memory);
const readLog = (state) => (fs.existsSync(paths(state).admissionLog) ? fs.readFileSync(paths(state).admissionLog, 'utf8') : '');

async function withClock(fn) {
  const real = Date.now;
  const clock = { now: T0 };
  Date.now = () => clock.now;
  try {
    return await fn(clock);
  } finally {
    Date.now = real;
  }
}

const writeFence = (state) => {
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
  fenceLegacyQueue(state, 'test');
};

// ---- the cap, in legacy and in fenced mode ----

for (const mode of ['legacy', 'fenced']) {
  test(`cap (${mode}): the first high of a repo is admitted high, the second is demoted to medium with the flags persisted`, async () => {
    const { state } = freshEnv();
    if (mode === 'fenced') writeFence(state);
    const first = await enqueue(state, ticket('a', 'r1', 'high'), cfgWith());
    const second = await enqueue(state, ticket('b', 'r1', 'high'), cfgWith());
    assert.deepEqual([first.priorityRequested, first.priorityAdmitted, first.priorityDemoted], ['high', 'high', false]);
    assert.deepEqual([second.priorityRequested, second.priorityAdmitted, second.priorityDemoted], ['high', 'medium', true]);
    const onDisk = queued(state);
    assert.deepEqual([onDisk.b.priorityRequested, onDisk.b.priorityAdmitted, onDisk.b.priorityDemoted], ['high', 'medium', true], 'persisted, not just returned');
    assert.equal(onDisk.a.priorityDemoted, false);
  });
}

test('cap: a high from a DIFFERENT repo is not demoted, and a non-high ticket is never counted or demoted', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticket('a', 'r1', 'high'), cfgWith());
  const other = await enqueue(state, ticket('b', 'r2', 'high'), cfgWith());
  const med = await enqueue(state, ticket('c', 'r1', 'medium'), cfgWith());
  const low = await enqueue(state, ticket('d', 'r1', 'low'), cfgWith());
  assert.equal(other.priorityAdmitted, 'high');
  assert.equal(other.priorityDemoted, false);
  assert.deepEqual([med.priorityAdmitted, med.priorityDemoted], ['medium', false]);
  assert.deepEqual([low.priorityAdmitted, low.priorityDemoted], ['low', false]);
});

test('cap: maxQueuedHighPerRepo 0 demotes every high; 2 admits two', async () => {
  const zero = freshEnv();
  for (const id of ['a', 'b']) {
    const record = await enqueue(zero.state, ticket(id, 'r1', 'high'), cfgWith({ maxQueuedHighPerRepo: 0 }));
    assert.deepEqual([record.priorityAdmitted, record.priorityDemoted], ['medium', true], id);
  }
  const two = freshEnv();
  const admitted = [];
  for (const id of ['a', 'b', 'c']) admitted.push((await enqueue(two.state, ticket(id, 'r1', 'high'), cfgWith({ maxQueuedHighPerRepo: 2 }))).priorityAdmitted);
  assert.deepEqual(admitted, ['high', 'high', 'medium']);
});

test('cap: a demoted ticket is not itself a queued high, and the slot frees when the high leaves the queue', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticket('a', 'r1', 'high'), cfgWith());
  assert.equal((await enqueue(state, ticket('b', 'r1', 'high'), cfgWith())).priorityDemoted, true);
  assert.equal((await enqueue(state, ticket('c', 'r1', 'high'), cfgWith())).priorityDemoted, true, 'a is still queued');
  dequeueSync(state, 'a');
  assert.equal((await enqueue(state, ticket('d', 'r1', 'high'), cfgWith())).priorityAdmitted, 'high', 'demoted tickets never held the slot');
});

test('cap: a queued high whose supervisor died does not hold the slot, and enqueue does not reap it', async () => {
  const { state } = freshEnv();
  const dead = spawnSync(process.execPath, ['-e', '0']).pid;
  await enqueue(state, ticket('ghost', 'r1', 'high', { supervisorPid: dead }), cfgWith());
  const next = await enqueue(state, ticket('live', 'r1', 'high'), cfgWith());
  assert.deepEqual([next.priorityAdmitted, next.priorityDemoted], ['high', false]);
  assert.ok(queued(state).ghost, 'enqueue counts liveness without mutating other tickets: reaping is tryStart\'s job');
});

test('cap: two real processes racing on the state-root lock admit exactly one high', async () => {
  for (let round = 0; round < 3; round += 1) {
    const { state, env, base } = freshEnv();
    const go = path.join(base, 'go');
    const script = `
      import fs from 'node:fs';
      import { enqueue } from ${JSON.stringify(path.join(SRC, 'scheduler.js'))};
      import { DEFAULT_GLOBAL_CONFIG } from ${JSON.stringify(path.join(SRC, 'config.js'))};
      const [id, ready, go, parentPid] = process.argv.slice(1);
      fs.writeFileSync(ready, '1');
      while (!fs.existsSync(go)) await new Promise((r) => setTimeout(r, 1));
      const t = { id, key: 'r1:' + id, repoId: 'r1', weight: 1, supervisorPid: Number(parentPid), supervisorStart: null, priorityRequested: 'high' };
      const rec = await enqueue(process.env.LANE_BROKER_STATE, t, DEFAULT_GLOBAL_CONFIG);
      process.stdout.write(JSON.stringify([rec.priorityAdmitted, rec.priorityDemoted]));
    `;
    const run = (id) => {
      const ready = path.join(base, `ready-${id}`);
      const child = spawn(process.execPath, ['--input-type=module', '-e', script, id, ready, go, String(process.pid)], { env });
      let out = '';
      let err = '';
      child.stdout.on('data', (d) => (out += d));
      child.stderr.on('data', (d) => (err += d));
      return { ready, done: new Promise((resolve) => child.on('close', (code) => resolve({ code, out, err }))) };
    };
    const a = run('a');
    const b = run('b');
    await waitFor(() => fs.existsSync(a.ready) && fs.existsSync(b.ready), { timeoutMs: 10_000 });
    fs.writeFileSync(go, '1');
    const results = await Promise.all([a.done, b.done]);
    for (const r of results) assert.equal(r.code, 0, r.err);
    const verdicts = results.map((r) => JSON.parse(r.out));
    assert.equal(verdicts.filter(([admitted]) => admitted === 'high').length, 1, `round ${round}: ${JSON.stringify(verdicts)}`);
    assert.equal(verdicts.filter(([admitted, demoted]) => admitted === 'medium' && demoted === true).length, 1, `round ${round}`);
    const records = Object.values(queued(state));
    assert.equal(records.filter((t) => t.priorityAdmitted === 'high').length, 1, 'persisted state agrees');
  }
});

// ---- the CLI: stderr note and history ----

const NOTE = 'lane run: priority high demoted to medium (repo already has a queued high ticket)';

test('CLI: two concurrent `lane run --priority high` from one repo, one prints the demotion note, and history records it', async () => {
  const { env, home, state } = freshEnv();
  writeGlobalConfig(home, { sampleMs: 100 });
  const repoIn = (name) => {
    const dir = fs.mkdtempSync(path.join(path.dirname(home), `${name}-`));
    gitFixture(['init', '-q'], dir);
    return fs.realpathSync(dir);
  };
  const sameRepo = repoIn('same');
  const otherRepo = repoIn('other');
  assert.equal((await laneRun(['pause', 'hold the queue'], { env })).code, 0);
  const run = (cwd) =>
    new Promise((resolve) => {
      const child = laneSpawn(['run', '--lane', 'default', '--priority', 'high', '--', 'sh', '-c', 'echo tier=$LANE_BROKER_PRIORITY'], { env, cwd });
      let stderr = '';
      let stdout = '';
      child.stderr.on('data', (d) => (stderr += d));
      child.stdout.on('data', (d) => (stdout += d));
      child.on('close', (code) => resolve({ code, stderr, stdout }));
    });
  const sameA = run(sameRepo);
  const sameB = run(sameRepo);
  const other = run(otherRepo);
  try {
    const records = await waitFor(() => (listQueue(state).length === 3 ? listQueue(state) : null), { timeoutMs: 15_000 });
    const verdict = (dir) => {
      const repoId = records.find((t) => t.cwd === dir).repoId;
      return records.filter((t) => t.repoId === repoId).map((t) => [t.priorityAdmitted, t.priorityDemoted]).sort();
    };
    assert.deepEqual(verdict(sameRepo), [['high', false], ['medium', true]]);
    assert.deepEqual(verdict(otherRepo), [['high', false]], 'a different repo is not demoted');
  } finally {
    await laneRun(['resume'], { env });
  }
  const results = await Promise.all([sameA, sameB, other]);
  for (const r of results) assert.equal(r.code, 0, r.stderr);
  assert.deepEqual(results.map((r) => r.stdout.trim()).sort(), ['tier=high', 'tier=high', 'tier=medium'], 'the child sees the ADMITTED tier');
  const notes = results.filter((r) => r.stderr.includes(NOTE));
  assert.equal(notes.length, 1, `exactly one demotion note, got stderr: ${JSON.stringify(results.map((r) => r.stderr))}`);
  assert.equal(results.filter((r) => r.stderr.includes('demoted')).length, 1, 'the note is printed once and only on demotion');

  const rows = fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(rows.length, 3);
  const demotedRows = rows.filter((r) => r.priorityDemoted === true);
  assert.equal(demotedRows.length, 1, 'the demotion is visible in history');
  assert.deepEqual([demotedRows[0].priorityRequested, demotedRows[0].priorityAdmitted], ['high', 'medium']);
  for (const row of rows) {
    assert.equal(typeof row.effectiveRankAtStart, 'number');
    assert.equal(typeof row.scoreAtStart, 'number');
    assert.ok(Number.isFinite(row.waitedMs));
  }
});

// ---- audit: lease, history row, decision log ----

test('audit: the lease carries requested/admitted/demoted, rank and score at start', async () => {
  await withClock(async (clock) => {
    const { state } = freshEnv();
    await enqueue(state, ticket('a', 'r1', 'high'), cfgWith());
    const b = ticket('b', 'r1', 'high', { prioOriginAt: T0 });
    const record = await enqueue(state, b, cfgWith());
    assert.equal(record.priorityDemoted, true);
    dequeueSync(state, 'a');
    clock.now = T0 + 5 * MIN;
    const result = await poll(state, b, tryCfg());
    assert.equal(result.started, true);
    const lease = readLease(state, 'b');
    assert.deepEqual(
      [lease.priorityRequested, lease.priorityAdmitted, lease.priorityDemoted, lease.effectiveRankAtStart],
      ['high', 'medium', true, 1],
    );
    assert.equal(lease.scoreAtStart, 1.5, 'medium 1.0 plus 5 of 20 minutes of age');
  });
});

test('audit: the admission decision line carries the head tier, effective rank and score; a not-head poll writes nothing', async () => {
  await withClock(async (clock) => {
    const { state } = freshEnv();
    writeFence(state);
    const a = ticket('a', 'r1', 'low', { prioOriginAt: T0 });
    const b = ticket('b', 'r2', 'high', { prioOriginAt: T0 });
    await enqueue(state, a, cfgWith());
    await enqueue(state, b, cfgWith());
    clock.now = T0 + 10 * MIN; // a has reached 1.0, b sits at the 2.0 ceiling: b is the head
    const before = readLog(state);
    const headPoll = await poll(state, b, tryCfg({ capacity: 0.5 }));
    assert.equal(headPoll.reason, 'capacity');
    assert.match(readLog(state).slice(before.length), / headTier=high headRank=2 headScore=2\.00 /);
    const afterHead = readLog(state).length;
    const notHead = await poll(state, a, tryCfg());
    assert.equal(notHead.reason, 'not-head');
    assert.equal(readLog(state).length, afterHead, 'a not-head poll stays unlogged');
  });
});

test('audit: a low head that has aged is reported with its tier and the rank it has earned', async () => {
  await withClock(async (clock) => {
    const { state } = freshEnv();
    const a = ticket('a', 'r1', 'low', { prioOriginAt: T0 });
    await enqueue(state, a, cfgWith());
    clock.now = T0 + 10 * MIN;
    await poll(state, a, tryCfg());
    assert.match(readLog(state), / headTier=low headRank=1 headScore=1\.00 /);
    assert.equal(readJsonSafe(path.join(paths(state).leases, 'a.json')).effectiveRankAtStart, 1);
  });
});
