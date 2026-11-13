import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv } from './helpers.js';
import { realSrc, assertCanaryFails } from './helpers/mutant-module.js';

const STATE_JS = 'state.js';
const HOUR_MS = 3_600_000;

/** Plant what a power cut leaves: a lock dir whose owner.json holds `content` and was last written `ageMs` ago. */
function plantLock(state, paths, content, ageMs) {
  const lockDir = paths(state).lock;
  fs.mkdirSync(lockDir, { recursive: true });
  const ownerFile = path.join(lockDir, 'owner.json');
  fs.writeFileSync(ownerFile, content);
  const when = new Date(Date.now() - ageMs);
  fs.utimesSync(ownerFile, when, when);
  return lockDir;
}

async function withBootTime(bootMs, run) {
  const saved = process.env.LANE_BROKER_BOOT_TIME_MS;
  process.env.LANE_BROKER_BOOT_TIME_MS = String(bootMs);
  try {
    return await run();
  } finally {
    if (saved === undefined) delete process.env.LANE_BROKER_BOOT_TIME_MS;
    else process.env.LANE_BROKER_BOOT_TIME_MS = saved;
  }
}

const UNREADABLE = [['a 0-byte owner.json', ''], ['an unparsable owner.json', '{"pid": 12']];

async function reclaimsPreBootOwner(load, content) {
  const { withLock, paths } = await load(STATE_JS);
  const { state } = freshEnv();
  plantLock(state, paths, content, 2 * HOUR_MS);
  let entered = false;
  await withBootTime(Date.now() - HOUR_MS, () => withLock(state, () => { entered = true; }, { timeoutMs: 1000 }));
  assert.equal(entered, true, 'the lock was not acquired');
  const tombs = fs.readdirSync(state).filter((n) => n.startsWith('.lock-tomb-unreadable-'));
  assert.equal(tombs.length, 1, 'the unreadable lock was not moved to a tomb by the CAS');
}

async function keepsPostBootOwner(load, content) {
  const { withLock, paths, LockTimeoutError } = await load(STATE_JS);
  const { state } = freshEnv();
  const lockDir = plantLock(state, paths, content, 0);
  await withBootTime(Date.now() - HOUR_MS, () =>
    assert.rejects(withLock(state, () => {}, { timeoutMs: 200 }), (err) => err instanceof LockTimeoutError && /not provably from a previous boot/.test(err.message)),
  );
  assert.ok(fs.existsSync(lockDir), 'an unreadable owner newer than boot was removed');
}

for (const [label, content] of UNREADABLE) {
  test(`${label} older than this boot is reclaimed through the tomb CAS`, () => reclaimsPreBootOwner(realSrc, content));

  test(`${label} newer than this boot is left alone and the timeout names it`, () => keepsPostBootOwner(realSrc, content));

  test(`canary: without the boot-time check, ${label} newer than boot is wrongly reclaimed`, () =>
    assertCanaryFails(STATE_JS, ['if (stat.mtimeMs >= boot) return null;', ''], (load) => keepsPostBootOwner(load, content)));
}

async function unknownBootMeansNoReclaim(load) {
  const { withLock, paths, LockTimeoutError } = await load(STATE_JS);
  const { state } = freshEnv();
  plantLock(state, paths, '', 2 * HOUR_MS);
  await withBootTime('unknown', () => assert.rejects(withLock(state, () => {}, { timeoutMs: 200 }), LockTimeoutError));
}

test('an unreadable owner is not reclaimed when the boot time is unknown', () => unknownBootMeansNoReclaim(realSrc));

test('canary: if an unknown boot time read as "everything is older", an unreadable owner would be reclaimed', () =>
  assertCanaryFails(STATE_JS, ['Number(override) : null;', 'Number(override) : Infinity;'], unknownBootMeansNoReclaim));

test('bootTimeMs honours the test override and otherwise reports a plausible past instant or null', async () => {
  const { bootTimeMs } = await realSrc(STATE_JS);
  await withBootTime(12345, () => assert.equal(bootTimeMs(), 12345));
  const real = bootTimeMs();
  assert.ok(real === null || (real < Date.now() && real > Date.now() - 3650 * 24 * HOUR_MS), `implausible boot time ${real}`);
});

/** Acquire a lock while recording every fsync and the staging rename into the lock path, in order. */
async function acquisitionEvents(load, { contend = false } = {}) {
  const { withLock, paths } = await load(STATE_JS);
  const { state } = freshEnv();
  if (contend) {
    const lockDir = paths(state).lock;
    fs.mkdirSync(lockDir);
    fs.writeFileSync(path.join(lockDir, 'owner.json'), JSON.stringify({ pid: process.pid, token: 'someone-else', start: null, at: Date.now() }));
  }
  const events = [];
  const { fsyncSync, renameSync, openSync } = fs;
  const opened = new Map();
  const lockDir = paths(state).lock;
  const label = (file) => ({ [path.join(lockDir, 'owner.json')]: 'owner.json', [lockDir]: 'lockDir', [state]: 'parent' })[file] ?? `other:${file}`;
  fs.openSync = (file, ...rest) => {
    const fd = openSync(file, ...rest);
    opened.set(fd, label(String(file)));
    return fd;
  };
  fs.fsyncSync = (fd) => {
    events.push(`fsync:${opened.get(fd)}`);
    return fsyncSync(fd);
  };
  fs.renameSync = (from, to) => {
    if (path.basename(String(from)).startsWith('.lock-stage-')) events.push('rename');
    return renameSync(from, to);
  };
  try {
    await withLock(state, () => {}, { timeoutMs: 150 }).catch((err) => events.push(`threw:${err.name}`));
  } finally {
    fs.fsyncSync = fsyncSync;
    fs.renameSync = renameSync;
    fs.openSync = openSync;
  }
  return events;
}

async function fsyncsOnlyAfterTheRename(load) {
  const events = await acquisitionEvents(load);
  const rename = events.indexOf('rename');
  assert.deepEqual(events.slice(0, rename), [], `nothing may be fsynced before the rename: ${events}`);
  assert.deepEqual(events.slice(rename), ['rename', 'fsync:owner.json', 'fsync:lockDir', 'fsync:parent'], `owner.json, lock/ and its parent must be fsynced after the rename, in that order: ${events}`);
}

async function losersDoNotFsync(load) {
  const events = await acquisitionEvents(load, { contend: true });
  assert.deepEqual(events.filter((e) => e.startsWith('fsync')), [], `a contender that loses the rename must not fsync: ${events}`);
  assert.ok(events.includes('threw:LockTimeoutError'), `the contender should have timed out: ${events}`);
}

test('the winner fsyncs owner.json, lock/ and the parent dir after the rename, nothing before it', () => fsyncsOnlyAfterTheRename(realSrc));

test('a contender polling a held lock fsyncs nothing', () => losersDoNotFsync(realSrc));

for (const target of ["fsyncFile(path.join(lockDir, 'owner.json'));", 'fsyncDirectory(lockDir);', 'fsyncDirectory(path.dirname(lockDir));']) {
  test(`canary: without \`${target}\` the fsync ordering test fails`, () => assertCanaryFails(STATE_JS, [target, ''], fsyncsOnlyAfterTheRename));
}

test('canary: an fsync before the staging rename fails both the ordering and the contender tests', async () => {
  const early = ['fs.renameSync(staging, lockDir);\n      acquired = true;', 'fsyncFile(stagedOwner);\n      fs.renameSync(staging, lockDir);\n      acquired = true;'];
  await assertCanaryFails(STATE_JS, early, fsyncsOnlyAfterTheRename);
  await assertCanaryFails(STATE_JS, early, losersDoNotFsync);
});

function plantStages(state, boot) {
  const mk = (name, ageMs) => {
    const dir = path.join(state, name);
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'owner.json'), '');
    const when = new Date(Date.now() - ageMs);
    fs.utimesSync(dir, when, when);
    return dir;
  };
  return {
    deadPid: mk('.lock-stage-2147483646-aabbccddeeff', 0),
    preBoot: mk(`.lock-stage-${process.pid}-112233445566`, 2 * HOUR_MS),
    livePid: mk(`.lock-stage-${process.pid}-665544332211`, 0),
    unrelated: mk('.lock-stage-notapid', 2 * HOUR_MS),
    boot,
  };
}

async function collectsOnlyUnreachableStages(load) {
  const { __test } = await load(STATE_JS);
  const { state } = freshEnv();
  const s = plantStages(state, Date.now() - HOUR_MS);
  await withBootTime(s.boot, () => __test.gcStages(state));
  assert.equal(fs.existsSync(s.deadPid), false, 'a stage of a dead pid was kept');
  assert.equal(fs.existsSync(s.preBoot), false, 'a stage older than this boot was kept');
  assert.equal(fs.existsSync(s.livePid), true, 'a stage of a live pid newer than boot was removed');
  assert.equal(fs.existsSync(s.unrelated), true, 'a directory that is not a stage was removed');
}

test('stage GC removes a dead pid\'s stage and a pre-boot stage even under a live pid, and keeps a post-boot live-pid stage', () => collectsOnlyUnreachableStages(realSrc));

test('canary: without the liveness check, stage GC removes a live process\'s stage', () =>
  assertCanaryFails(STATE_JS, ['isLockOwnerAlive({ pid: Number(m[1]) })', 'false'], collectsOnlyUnreachableStages));

test('canary: without the boot-time check, stage GC keeps a stage older than boot', () =>
  assertCanaryFails(STATE_JS, ['boot !== null && fs.statSync(doomed).mtimeMs < boot', 'false'], collectsOnlyUnreachableStages));

async function readFailureIsNotAnUnreadableOwner(load) {
  const { withLock, paths, LockTimeoutError } = await load(STATE_JS);
  const { state } = freshEnv();
  const lockDir = plantLock(state, paths, JSON.stringify({ pid: 2147483646, token: 'good', start: null, at: 1 }), 2 * HOUR_MS);
  const { readFileSync } = fs;
  fs.readFileSync = (file, ...rest) => {
    if (String(file).endsWith('owner.json')) throw Object.assign(new Error('too many open files'), { code: 'EMFILE' });
    return readFileSync(file, ...rest);
  };
  try {
    await withBootTime(Date.now() - HOUR_MS, () => assert.rejects(withLock(state, () => {}, { timeoutMs: 200 }), LockTimeoutError));
  } finally {
    fs.readFileSync = readFileSync;
  }
  assert.deepEqual(fs.readdirSync(state).filter((n) => n.startsWith('.lock-tomb-')), [], 'a lock was tombed on the strength of a failed read');
  assert.ok(fs.existsSync(lockDir));
}

test('a failing owner.json read (EMFILE) with a good file is waited out, never tombed as unreadable', () => readFailureIsNotAnUnreadableOwner(realSrc));

test('canary: if a failed read counted as an unreadable owner, a pre-boot lock is tombed on a read error', () =>
  assertCanaryFails(STATE_JS, ['return null; // a failed read (EMFILE, EIO, EACCES, ENOENT) says nothing about the content', "raw = '';"], readFailureIsNotAnUnreadableOwner));

async function misjudgedStageIsNeverPublishedEmpty(load) {
  const { withLock, paths, readJsonSafe, __test } = await load(STATE_JS);
  const { state } = freshEnv();
  const { renameSync, rmSync } = fs;
  let gcRan = false;
  fs.renameSync = (from, to) => {
    if (!gcRan && path.basename(String(from)).startsWith(`.lock-stage-${process.pid}-`)) {
      gcRan = true;
      __test.gcStages(state); // boot time in the future: our own live stage is misjudged as pre-boot
    }
    return renameSync(from, to);
  };
  fs.rmSync = (target, opts) => {
    // a recursive remove is not atomic: it unlinks owner.json before the directory, and may be interrupted there
    if (path.basename(String(target)).startsWith('.lock-stage-')) fs.unlinkSync(path.join(String(target), 'owner.json'));
    throw new Error('interrupted remove');
  };
  try {
    await withBootTime(Date.now() + HOUR_MS, () =>
      withLock(state, () => {
        const owner = readJsonSafe(path.join(paths(state).lock, 'owner.json'));
        assert.equal(owner?.pid, process.pid, 'the lock was published with an empty or foreign owner.json');
      }, { timeoutMs: 1000 }),
    );
  } finally {
    fs.renameSync = renameSync;
    fs.rmSync = rmSync;
  }
  assert.ok(gcRan, 'the GC hook never fired');
}

test('a stage that GC misjudges and half-removes is never renamed empty onto lock/', () => misjudgedStageIsNeverPublishedEmpty(realSrc));

test('canary: if GC removed a stage in place, a misjudged acquirer would publish an empty lock/', () =>
  assertCanaryFails(STATE_JS, ['fs.renameSync(doomed, aside);\n        doomed = aside;', ''], misjudgedStageIsNeverPublishedEmpty));

test('bootTimeMs never returns undefined: a number, or null when unknown', async () => {
  const { bootTimeMs } = await realSrc(STATE_JS);
  for (const value of ['unknown', '', '  ', 'NaN', '123']) {
    await withBootTime(value, () => {
      const t = bootTimeMs();
      assert.ok(t === null || (typeof t === 'number' && !Number.isNaN(t)), `${value}: ${t}`);
    });
  }
  assert.equal(await withBootTime('unknown', () => bootTimeMs()), null);
  assert.equal(await withBootTime('', () => bootTimeMs()), null);
  const saved = process.env.LANE_BROKER_BOOT_TIME_MS;
  delete process.env.LANE_BROKER_BOOT_TIME_MS;
  try {
    const real = bootTimeMs();
    assert.ok(real === null || (typeof real === 'number' && !Number.isNaN(real)), `${real}`);
  } finally {
    if (saved !== undefined) process.env.LANE_BROKER_BOOT_TIME_MS = saved;
  }
});
