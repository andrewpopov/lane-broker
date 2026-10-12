import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart, readSkipState } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths, atomicWriteJson } from '../src/state.js';
import { buildExpressTable, expressVerdict } from '../src/express.js';
import { collectStatus, renderStatusText } from '../src/status.js';

/**
 * BRAIN-551: express admission. Fixture machine: 10 cores, reserve 1, 100% -> CPU budget 9; weight capacity 6.
 * The head (rouge:default, weight 3) is blocked by a running rouge:sim lease (weight 1) with its conflict skip allowance
 * spent and BRAIN-355 safe backfill off, so ordinary backfill refuses everything and only the express path can start a
 * candidate (the head-protection rule reuses BRAIN-355's cannot-delay proof: running + head + candidate <= capacity).
 */

const GIB = 1024 ** 3;
const LIMIT = 3;
const MINUTE = 60_000;

const baseCfg = (overrides = {}) => ({
  ...DEFAULT_GLOBAL_CONFIG,
  schedulerMode: 'active',
  capacity: 6,
  loadClose: 1000,
  loadOpen: 900,
  loadOpenSamples: 1,
  cpuAdmissionPercent: 100,
  cpuReserveCores: 1,
  admissionCooldownMs: 0,
  conflictSkipLimit: LIMIT,
  conflictSafeBackfill: false, // BRAIN-355 would also start a light ticket; off, only the express path can
  ...overrides,
});

const sampler = () => ({ hostBusyCores: 1, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });
const poll = (state, t, cfg) => tryStart(state, t, cfg, undefined, sampler, undefined, memory);

const ticket = (id, overrides = {}) => ({
  id,
  key: `r:${id}`,
  repoId: 'r',
  lane: 'quick',
  weight: 3,
  cwd: process.cwd(),
  cmd: ['true'],
  supervisorPid: process.pid,
  supervisorStart: null,
  logPath: '/dev/null',
  resultPath: '/dev/null',
  ...overrides,
});

const heldLease = (id, key, weight = 1) => ({ id, key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight, state: LEASE_STATE.RUNNING });

async function blockedHead(state, { headWaitedMs = 0 } = {}) {
  writeLease(state, heldLease('holder', 'rouge:sim'));
  await enqueue(state, ticket('head', { key: 'rouge:default', lane: 'default', conflicts: ['rouge:sim'], prioOriginAt: Date.now() - headWaitedMs }));
  atomicWriteJson(paths(state).conflictSkipState, { headId: 'head', count: LIMIT, blockedSince: Date.now(), loggedPhase: 'exhausted' });
}

/** `runs` successful local runs of `seconds` each, for (r, lane), optionally under a command fingerprint. */
const seedHistory = (state, lane, seconds, runs = 5, cmdFingerprint) => {
  const rows = Array.from({ length: runs }, (_, i) => ({ id: `${lane}-${i}`, repo: 'r', lane, ...(cmdFingerprint ? { cmdFingerprint } : {}), startedAt: 1000 + i, endedAt: 1000 + i + seconds * 1000, runMs: seconds * 1000, exit: 0 }));
  fs.appendFileSync(paths(state).history, rows.map((r) => `${JSON.stringify(r)}\n`).join(''));
};

const readLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

test('1: a short ticket behind a blocked head is admitted when it fits, uncounted, and logged', async () => {
  const { state } = freshEnv();
  await blockedHead(state);
  seedHistory(state, 'quick', 10);
  const quick = ticket('quick1');
  await enqueue(state, quick);

  const result = await poll(state, quick, baseCfg());
  assert.equal(result.started, true);
  assert.equal(readSkipState(state).count, LIMIT, 'an express start is not a skip');
  assert.match(readLog(state), /lane-broker-express event=express-admit candidateId=quick1 headId=head expectedMs=10000 source=lane /);
});

test('2: a long ticket is not express', async () => {
  const { state } = freshEnv();
  await blockedHead(state);
  seedHistory(state, 'quick', 600);
  const slow = ticket('slow1');
  await enqueue(state, slow);

  const result = await poll(state, slow, baseCfg());
  assert.equal(result.started, false);
  assert.equal(result.reason, 'not-head');
});

test('3: no history, or fewer than 3 runs, is not express', async () => {
  const none = freshEnv().state;
  await blockedHead(none);
  const a = ticket('a');
  await enqueue(none, a);
  assert.equal((await poll(none, a, baseCfg())).started, false);

  const few = freshEnv().state;
  await blockedHead(few);
  seedHistory(few, 'quick', 10, 2);
  const b = ticket('b');
  await enqueue(few, b);
  assert.equal((await poll(few, b, baseCfg())).started, false);
});

test('4: the command fingerprint p50 wins over the lane p50, and the lane is the fallback', () => {
  const rows = [];
  const add = (fp, seconds, n) => {
    for (let i = 0; i < n; i += 1) rows.push({ repo: 'r', lane: 'quick', ...(fp ? { cmdFingerprint: fp } : {}), startedAt: 1, endedAt: 1 + seconds * 1000, runMs: seconds * 1000, exit: 0 });
  };
  add('fast', 5, 4);
  add('slow', 900, 4);
  add(null, 5, 3);
  const table = buildExpressTable(rows);
  const cfg = baseCfg();
  const t = (cmdFingerprint) => ({ repoId: 'r', lane: 'quick', cmdFingerprint });
  assert.equal(expressVerdict(table, t('fast'), cfg)?.source, 'command');
  assert.equal(expressVerdict(table, t('slow'), cfg), null, 'a slow command on a mostly fast lane is not express');
  assert.equal(expressVerdict(table, t('unseen'), cfg)?.source, 'lane', 'no command history: falls back to the lane');
  assert.equal(expressVerdict(table, { repoId: 'r', lane: 'other' }, cfg), null);
});

test('5: the rate cap holds, and the window reopens a minute later', async () => {
  const { state } = freshEnv();
  await blockedHead(state);
  seedHistory(state, 'quick', 10);
  const cfg = baseCfg({ expressPerMinute: 2 });
  const tickets = ['q1', 'q2', 'q3'].map((id) => ticket(id, { weight: 1 }));
  for (const t of tickets) await enqueue(state, t);

  assert.equal((await poll(state, tickets[0], cfg)).started, true);
  assert.equal((await poll(state, tickets[1], cfg)).started, true);
  const third = await poll(state, tickets[2], cfg);
  assert.equal(third.started, false);
  assert.equal(third.reason, 'not-head');

  atomicWriteJson(paths(state).expressAdmissions, [Date.now() - 2 * MINUTE, Date.now() - 61_000]);
  assert.equal((await poll(state, tickets[2], cfg)).started, true, 'admissions older than a minute no longer count');
});

test('6: a head that has waited too long is protected: an express ticket that could delay it waits, one that cannot starts', async () => {
  const { state } = freshEnv();
  await blockedHead(state, { headWaitedMs: 20 * MINUTE });
  seedHistory(state, 'quick', 10);
  const cfg = baseCfg();
  const heavy = ticket('heavy', { weight: 3 }); // 1 + head 3 + 3 = 7 > 6: could delay the head
  const light = ticket('light', { weight: 1 }); // 1 + head 3 + 1 = 5 <= 6: cannot
  await enqueue(state, heavy);
  await enqueue(state, light);

  assert.equal((await poll(state, heavy, cfg)).started, false);
  assert.equal((await poll(state, light, cfg)).started, true);

  // the same heavy ticket behind a head inside the limit is admitted
  const young = freshEnv().state;
  await blockedHead(young, { headWaitedMs: 0 });
  seedHistory(young, 'quick', 10);
  const heavy2 = ticket('heavy2', { weight: 3 });
  await enqueue(young, heavy2);
  assert.equal((await poll(young, heavy2, cfg)).started, true);
});

test('7: conflicts, serialization and capacity are still respected', async () => {
  const cfg = baseCfg();

  const conflict = freshEnv().state;
  await blockedHead(conflict);
  seedHistory(conflict, 'quick', 10);
  const c = ticket('c', { weight: 1, conflicts: ['rouge:sim'] });
  await enqueue(conflict, c);
  assert.equal((await poll(conflict, c, cfg)).started, false, 'conflicts with the held lease');

  const serial = freshEnv().state;
  await blockedHead(serial);
  writeLease(serial, heldLease('same-key', 'r:s', 1));
  seedHistory(serial, 'quick', 10);
  const s = ticket('s', { weight: 1 });
  await enqueue(serial, s);
  assert.equal((await poll(serial, s, cfg)).started, false, 'same key as a held lease and maxConcurrent 1');

  const full = freshEnv().state;
  await blockedHead(full);
  writeLease(full, heldLease('big', 'r:big', 3));
  seedHistory(full, 'quick', 10);
  const f = ticket('f', { weight: 3 }); // running 4 + 3 > 6
  await enqueue(full, f);
  assert.equal((await poll(full, f, cfg)).started, false, 'does not fit weight capacity');
});

test('8: expressMaxMs 0 turns express off', async () => {
  const { state } = freshEnv();
  await blockedHead(state);
  seedHistory(state, 'quick', 10);
  const quick = ticket('quick1');
  await enqueue(state, quick);
  assert.equal((await poll(state, quick, baseCfg({ expressMaxMs: 0 }))).started, false);
});

test('9: lane status marks a queued express ticket', async () => {
  const { state, home } = freshEnv();
  const saved = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    seedHistory(state, 'quick', 10);
    seedHistory(state, 'default', 600);
    await enqueue(state, ticket('long1', { lane: 'default' }));
    await enqueue(state, ticket('quick1'));
    const status = await collectStatus();
    assert.deepEqual(status.queued.map((q) => [q.id, q.express]), [['long1', undefined], ['quick1', true]]);
    assert.match(renderStatusText(status), /#2 quick1 .*\[express\]/);
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', saved.home], ['LANE_BROKER_STATE', saved.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
});
