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
async function acquisitionEvents(load) {
  const { withLock } = await load(STATE_JS);
  const { state } = freshEnv();
  const events = [];
  const { fsyncSync, renameSync } = fs;
  fs.fsyncSync = (fd) => {
    events.push('fsync');
    return fsyncSync(fd);
  };
  fs.renameSync = (from, to) => {
    if (path.basename(String(from)).startsWith('.lock-stage-')) events.push('rename');
    return renameSync(from, to);
  };
  try {
    await withLock(state, () => {});
  } finally {
    fs.fsyncSync = fsyncSync;
    fs.renameSync = renameSync;
  }
  return events;
}

async function fsyncsAroundTheRename(load) {
  const events = await acquisitionEvents(load);
  const rename = events.indexOf('rename');
  assert.equal(events.slice(0, rename).filter((e) => e === 'fsync').length, 2, `owner.json and staging dir must both be fsynced before the rename: ${events}`);
  assert.ok(events.slice(rename + 1).includes('fsync'), `the lock's parent dir must be fsynced after the rename: ${events}`);
}

test('acquiring fsyncs owner.json and the staging dir before the rename, and the parent dir after', () => fsyncsAroundTheRename(realSrc));

for (const target of ['fsyncFile(stagedOwner);', 'fsyncDirectory(staging);', 'fsyncDirectory(path.dirname(lockDir));']) {
  test(`canary: without \`${target}\` the fsync ordering test fails`, () => assertCanaryFails(STATE_JS, [target, ''], fsyncsAroundTheRename));
}

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

test('stage GC removes stages of dead pids and from before boot, never a live process\'s stage', () => collectsOnlyUnreachableStages(realSrc));

test('canary: without the liveness check, stage GC removes a live process\'s stage', () =>
  assertCanaryFails(STATE_JS, ['!isLockOwnerAlive({ pid: Number(m[1]) })', 'true'], collectsOnlyUnreachableStages));

test('canary: without the boot-time check, stage GC keeps a stage older than boot', () =>
  assertCanaryFails(STATE_JS, ['boot !== null && fs.statSync(full).mtimeMs < boot', 'false'], collectsOnlyUnreachableStages));
