import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, laneRun, BIN, stripLogTimestamps } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { gcRemoteTickets, gcLogsAndResults, maybeDailyGc, REMOTE_TMP_BASE } from '../src/gc.js';
import { enqueue } from '../src/scheduler.js';
import { writeLease } from '../src/lease.js';
import { paths, atomicWriteFile, processStartTime, tombstonePath, writeCancelMarkerFile, writeWithdrawMarkerFile, writeExpireMarkerFile } from '../src/state.js';
import { appendRotatingLog, logAdmissionDecision, ADMISSION_LOG_MAX_BYTES, ADMISSION_LOG_REFRESH_MS } from '../src/admission.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig } from '../src/config.js';
import { serializeHeader } from '../src/remote-stream.js';

/**
 * BRAIN-438: what a finished run leaves behind is collected, what a live one owns never is, and the admission log stops
 * growing per poll. Times are injected (`now`) and file mtimes set explicitly, so nothing here sleeps.
 */

const DAY = 86_400_000;
const RETENTION = DEFAULT_GLOBAL_CONFIG.remoteTicketRetentionMs;
const NOW = Date.now();
const gc = (remoteRoot, state, extra = {}) => gcRemoteTickets({ remoteRoot, brokerRoot: state, retentionMs: RETENTION, now: NOW, ...extra });

function setup() {
  const f = freshEnv();
  const remoteRoot = makeTmpDir('gc-remote-');
  return { ...f, remoteRoot };
}

function setAge(file, ageMs) {
  const t = new Date(NOW - ageMs);
  fs.utimesSync(file, t, t);
}

// The tmp dir lives under /var/tmp (what remote-exec records), outside makeTmpDir's os.tmpdir(); removed at exit even on failure.
const leakedTmpDirs = new Set();
process.on('exit', () => {
  for (const dir of leakedTmpDirs) fs.rmSync(dir, { recursive: true, force: true });
});

/** A pid that existed and is gone. */
function deadPid() {
  const child = spawnSync(process.execPath, ['-e', 'process.exit(0)']);
  return child.pid;
}

/**
 * A ticket directory as `remote-exec` leaves it. `publisher`: 'alive' (this test process), 'dead', or 'none'.
 * `laneId` is the broker-side ticket it enqueued (written as `remote-id`).
 */
function makeTicket(remoteRoot, { id = crypto.randomUUID(), publisher = 'dead', result = true, laneId, work = true, tmp = false, ageMs = 0 } = {}) {
  const dir = path.join(remoteRoot, 'tickets', id);
  fs.mkdirSync(dir, { recursive: true });
  if (publisher !== 'none') {
    const pid = publisher === 'alive' ? process.pid : deadPid();
    fs.writeFileSync(path.join(dir, 'publisher.json'), JSON.stringify({ pid, start: publisher === 'alive' ? processStartTime(pid) : null }));
  }
  if (result) fs.writeFileSync(path.join(dir, 'result.json'), JSON.stringify({ protocol: 1, kind: 'completed', exit: 0 }));
  if (laneId) fs.writeFileSync(path.join(dir, 'remote-id'), laneId);
  if (work) {
    fs.mkdirSync(path.join(dir, 'work'), { recursive: true });
    fs.writeFileSync(path.join(dir, 'work', 'big.bin'), 'x'.repeat(1024));
  }
  let tmpDir = null;
  if (tmp) {
    tmpDir = path.join(REMOTE_TMP_BASE, `lb-gctest-${crypto.randomBytes(6).toString('hex')}`);
    fs.mkdirSync(tmpDir);
    leakedTmpDirs.add(tmpDir);
    fs.writeFileSync(path.join(tmpDir, 'scratch'), 'y');
    fs.writeFileSync(path.join(dir, 'tmp-dir'), tmpDir);
  }
  setAge(dir, ageMs);
  return { id, dir, workDir: path.join(dir, 'work'), tmpDir };
}

const ticketOf = (id) => ({ id, key: `r:${id}`, weight: 1, resources: {}, conflicts: [], supervisorPid: process.pid, supervisorStart: null });

// ---- remote tickets: work dirs ----

test('a dead, finished ticket has its work and tmp dirs removed; its result stays', async () => {
  const { state, remoteRoot } = setup();
  const t = makeTicket(remoteRoot, { tmp: true });
  const summary = await gc(remoteRoot, state);
  assert.equal(summary.work, 1);
  assert.equal(fs.existsSync(t.workDir), false, 'work/ is gone');
  assert.equal(fs.existsSync(t.tmpDir), false, 'the tmp dir is gone');
  assert.ok(fs.existsSync(path.join(t.dir, 'result.json')), 'the result is kept for the submitter');
  assert.deepEqual(fs.readdirSync(path.join(remoteRoot, 'gc-trash')), [], 'nothing is left in the trash');
});

test('a ticket without a terminal result is not touched while young, even with a dead publisher', async () => {
  const { state, remoteRoot } = setup();
  const t = makeTicket(remoteRoot, { result: false });
  await gc(remoteRoot, state);
  assert.ok(fs.existsSync(t.workDir));
});

test('--dry-run reports what it would do and removes nothing', async () => {
  const { state, remoteRoot } = setup();
  const t = makeTicket(remoteRoot);
  const summary = await gc(remoteRoot, state, { dryRun: true });
  assert.equal(summary.work, 1);
  assert.ok(fs.existsSync(t.workDir));
});

// ---- a live ticket is NEVER touched ----

const LIVE_CASES = {
  'its publisher is alive': async () => ({ publisher: 'alive' }),
  'it is leased': async (state) => {
    const laneId = crypto.randomUUID();
    writeLease(state, { ...ticketOf(laneId), supervisorPid: process.pid });
    return { laneId };
  },
  'it is queued': async (state) => {
    const laneId = crypto.randomUUID();
    await enqueue(state, ticketOf(laneId));
    return { laneId };
  },
  'a cancel marker is pending': async (state) => {
    const laneId = crypto.randomUUID();
    writeCancelMarkerFile(state, laneId);
    return { laneId };
  },
  'a withdraw marker is pending': async (state) => {
    const laneId = crypto.randomUUID();
    writeWithdrawMarkerFile(state, laneId);
    return { laneId };
  },
  'an expiry marker is pending': async (state) => {
    const laneId = crypto.randomUUID();
    writeExpireMarkerFile(state, laneId);
    return { laneId };
  },
  'it is tombstoned': async () => ({ tombstone: true }),
};

for (const [why, arrange] of Object.entries(LIVE_CASES)) {
  for (const ageMs of [0, RETENTION + DAY]) {
    test(`a ticket is never touched when ${why} (${ageMs === 0 ? 'fresh' : 'past retention'})`, async () => {
      const { state, remoteRoot } = setup();
      const { tombstone, ...opts } = await arrange(state);
      const t = makeTicket(remoteRoot, { ...opts, ageMs, tmp: true });
      if (tombstone) atomicWriteFile(tombstonePath(remoteRoot, t.id), String(NOW));
      await gc(remoteRoot, state);
      assert.ok(fs.existsSync(t.workDir), 'work/ survives');
      assert.ok(fs.existsSync(t.tmpDir), 'tmp dir survives');
      assert.ok(fs.existsSync(path.join(t.dir, 'result.json')), 'the ticket survives');
      fs.rmSync(t.tmpDir, { recursive: true, force: true });
    });
  }
}

// ---- whole ticket dirs past retention ----

test('a dead ticket directory older than the retention is removed whole; a younger one is kept', async () => {
  const { state, remoteRoot } = setup();
  const old = makeTicket(remoteRoot, { ageMs: RETENTION + DAY, work: false });
  const young = makeTicket(remoteRoot, { ageMs: RETENTION - DAY, work: false });
  const noResult = makeTicket(remoteRoot, { ageMs: RETENTION + DAY, result: false, publisher: 'none', work: false });
  const summary = await gc(remoteRoot, state);
  assert.equal(summary.tickets, 2);
  assert.equal(fs.existsSync(old.dir), false);
  assert.equal(fs.existsSync(noResult.dir), false, 'a crashed exec that never published is collected too');
  assert.ok(fs.existsSync(young.dir));
});

// ---- tombstones outlive any possible late remote-exec ----

function execHeaderOnly(env, remoteRoot, ticketId) {
  const header = { ticketId, generation: 0, repoKey: 'gc-test', lane: 'default', argv: [process.execPath, '-e', ''], relCwd: '' };
  const child = spawn(process.execPath, [BIN, 'remote-exec', '--root', remoteRoot], { env });
  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });
  child.stdin.on('error', () => {});
  child.stdin.end(serializeHeader(header, [], undefined));
  return new Promise((resolve) => child.on('close', (code) => resolve({ code, stderr })));
}

test('a tombstone older than every retention value survives GC, and a late exec is still refused', async () => {
  const f = setup();
  writeGlobalConfig(f.home, {});
  const id = crypto.randomUUID();
  atomicWriteFile(tombstonePath(f.remoteRoot, id), String(NOW));
  setAge(tombstonePath(f.remoteRoot, id), 10 * RETENTION);
  await gc(f.remoteRoot, f.state);
  await gc(f.remoteRoot, f.state, { retentionMs: DAY });
  assert.ok(fs.existsSync(tombstonePath(f.remoteRoot, id)), 'never pruned');
  const { code, stderr } = await execHeaderOnly(f.env, f.remoteRoot, id);
  assert.equal(code, 1, stderr);
  assert.match(stderr, /was cancelled before it arrived/);
});

// ---- GC never deletes outside its root ----

test('a gc-trash that is a symlink to an outside directory is refused and the outside files survive', async () => {
  const { state, remoteRoot } = setup();
  const outside = makeTmpDir('gc-outside-');
  fs.mkdirSync(path.join(outside, 'victim'));
  fs.writeFileSync(path.join(outside, 'victim', 'sentinel'), 'keep');
  fs.symlinkSync(outside, path.join(remoteRoot, 'gc-trash'));
  makeTicket(remoteRoot);
  await assert.rejects(() => gc(remoteRoot, state), /refusing to collect/);
  await assert.rejects(() => gc(remoteRoot, state, { maxTickets: 1 }), /refusing to collect/);
  assert.equal(fs.readFileSync(path.join(outside, 'victim', 'sentinel'), 'utf8'), 'keep');
});

test('a symlinked tickets directory is refused, and symlinks inside gc-trash are never followed', async () => {
  const a = setup();
  const outside = makeTmpDir('gc-outside-');
  fs.writeFileSync(path.join(outside, 'sentinel'), 'keep');
  fs.symlinkSync(outside, path.join(a.remoteRoot, 'tickets'));
  await assert.rejects(() => gc(a.remoteRoot, a.state), /refusing to collect/);
  const b = setup();
  fs.mkdirSync(path.join(b.remoteRoot, 'gc-trash'));
  fs.symlinkSync(outside, path.join(b.remoteRoot, 'gc-trash', 'link'));
  makeTicket(b.remoteRoot);
  await gc(b.remoteRoot, b.state);
  assert.equal(fs.readFileSync(path.join(outside, 'sentinel'), 'utf8'), 'keep');
});

// ---- the per-run bound ----

test('at most maxTickets ticket directories are acted on per run, and repeated runs finish the job', async () => {
  const { state, remoteRoot } = setup();
  const all = Array.from({ length: 5 }, () => makeTicket(remoteRoot));
  const first = await gc(remoteRoot, state, { maxTickets: 2 });
  assert.equal(first.work, 2);
  assert.equal(first.bounded, true);
  assert.equal(all.filter((t) => !fs.existsSync(t.workDir)).length, 2);
  await gc(remoteRoot, state, { maxTickets: 2 });
  const last = await gc(remoteRoot, state, { maxTickets: 2 });
  assert.equal(last.bounded, false);
  assert.equal(all.filter((t) => fs.existsSync(t.workDir)).length, 0);
});

test('at most maxExamined directories are examined per run, and repeated runs cover all of them', async () => {
  const { state, remoteRoot } = setup();
  const all = Array.from({ length: 500 }, () => makeTicket(remoteRoot));
  let runs = 0;
  for (; runs < 20 && all.some((t) => fs.existsSync(t.workDir)); runs += 1) {
    const summary = await gc(remoteRoot, state, { maxExamined: 50 });
    assert.ok(summary.examined <= 50, `examined ${summary.examined}`);
  }
  assert.equal(all.filter((t) => fs.existsSync(t.workDir)).length, 0);
  assert.ok(runs >= 10, 'it took at least ten bounded runs');
});

// ---- logs and results ----

test('logs and results older than the retention are pruned; a live lease or queued ticket keeps its files', async () => {
  const { state } = setup();
  const p = paths(state);
  fs.mkdirSync(p.logs, { recursive: true });
  fs.mkdirSync(p.results, { recursive: true });
  const old = 15 * DAY;
  const make = (id, ageMs) => {
    for (const file of [path.join(p.logs, `${id}.log`), path.join(p.results, `${id}.json`)]) {
      fs.writeFileSync(file, 'x');
      setAge(file, ageMs);
    }
  };
  const [stale, young, leased, queued] = [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()];
  make(stale, old);
  make(young, DAY);
  make(leased, old);
  make(queued, old);
  writeLease(state, ticketOf(leased));
  await enqueue(state, ticketOf(queued));
  const summary = await gcLogsAndResults(state, { retentionMs: DEFAULT_GLOBAL_CONFIG.logRetentionMs, now: NOW });
  assert.deepEqual([summary.logs, summary.results], [1, 1]);
  assert.equal(fs.existsSync(path.join(p.logs, `${stale}.log`)), false);
  assert.equal(fs.existsSync(path.join(p.results, `${stale}.json`)), false);
  for (const id of [young, leased, queued]) {
    assert.ok(fs.existsSync(path.join(p.logs, `${id}.log`)), `${id} log kept`);
    assert.ok(fs.existsSync(path.join(p.results, `${id}.json`)), `${id} result kept`);
  }
});

test('a live lease or queued ticket keeps the custom log path it names, whatever its filename', async () => {
  const { state } = setup();
  const p = paths(state);
  fs.mkdirSync(p.logs, { recursive: true });
  const shared = path.join(p.logs, 'shared.log');
  const queuedLog = path.join(p.logs, 'queued-custom.log');
  const orphan = path.join(p.logs, 'orphan.log');
  for (const f of [shared, queuedLog, orphan]) {
    fs.writeFileSync(f, 'x');
    setAge(f, 30 * DAY);
  }
  writeLease(state, { ...ticketOf(crypto.randomUUID()), logPath: shared });
  await enqueue(state, { ...ticketOf(crypto.randomUUID()), logPath: queuedLog });
  const summary = await gcLogsAndResults(state, { retentionMs: DEFAULT_GLOBAL_CONFIG.logRetentionMs, now: NOW });
  assert.equal(summary.logs, 1);
  assert.ok(fs.existsSync(shared) && fs.existsSync(queuedLog));
  assert.equal(fs.existsSync(orphan), false);
});

test('the daily sweep runs once, is not due again within a day, and is due after one', async () => {
  const { state } = setup();
  const p = paths(state);
  fs.mkdirSync(p.logs, { recursive: true });
  const stale = path.join(p.logs, `${crypto.randomUUID()}.log`);
  fs.writeFileSync(stale, 'x');
  setAge(stale, 30 * DAY);
  const cfg = { ...DEFAULT_GLOBAL_CONFIG };
  assert.equal((await maybeDailyGc(state, cfg, NOW)).logs, 1);
  assert.equal(fs.existsSync(stale), false);
  assert.equal(await maybeDailyGc(state, cfg, NOW + DAY - 1000), null, 'not due within a day');
  assert.notEqual(await maybeDailyGc(state, cfg, NOW + DAY + 1000), null, 'due after a day');
});

test('a sweep that hits its bound leaves the daily marker unclaimed so the next supervisor continues', async () => {
  const { state } = setup();
  const p = paths(state);
  fs.mkdirSync(p.logs, { recursive: true });
  for (let i = 0; i < 3; i += 1) {
    const f = path.join(p.logs, `${crypto.randomUUID()}.log`);
    fs.writeFileSync(f, 'x');
    setAge(f, 30 * DAY);
  }
  const summary = await maybeDailyGc(state, { ...DEFAULT_GLOBAL_CONFIG, logGcMaxFilesPerRun: 2 }, NOW);
  assert.equal(summary.bounded, true);
  assert.equal(fs.readdirSync(p.logs).length, 1);
  assert.notEqual(await maybeDailyGc(state, { ...DEFAULT_GLOBAL_CONFIG, logGcMaxFilesPerRun: 2 }, NOW + 1), null);
  assert.equal(fs.readdirSync(p.logs).length, 0);
});

// ---- the admission log ----

const decision = (candidateId, over = {}) => ({
  candidateId,
  mode: 'active',
  currentDecision: 'start',
  currentReason: 'ok',
  admit: false,
  reason: 'projected-over-budget',
  ...over,
});
const logLines = (state) => stripLogTimestamps(fs.readFileSync(paths(state).admissionLog, 'utf8')).split('\n').filter(Boolean);

test('the admission log writes a candidate only when its decision changes, and prefixes an ISO timestamp', () => {
  const { state } = setup();
  for (let poll = 0; poll < 50; poll += 1) logAdmissionDecision(state, decision('a'));
  assert.equal(logLines(state).length, 1, 'fifty identical polls write one line');
  logAdmissionDecision(state, decision('b'));
  assert.equal(logLines(state).length, 2, 'another candidate is its own pair');
  logAdmissionDecision(state, decision('a', { reason: 'memory' }));
  assert.equal(logLines(state).length, 3, 'a changed reason is written');
  logAdmissionDecision(state, decision('a'));
  assert.equal(logLines(state).length, 4, 'and so is a change back');
  const raw = fs.readFileSync(paths(state).admissionLog, 'utf8').split('\n').filter(Boolean);
  for (const l of raw) assert.match(l, /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z lane-broker-admission candidate=/);
});

test('an unchanged decision is suppressed within five minutes and written again after', () => {
  const { state } = setup();
  logAdmissionDecision(state, decision('a'), NOW);
  logAdmissionDecision(state, decision('a'), NOW + ADMISSION_LOG_REFRESH_MS - 1000);
  assert.equal(logLines(state).length, 1);
  logAdmissionDecision(state, decision('a'), NOW + ADMISSION_LOG_REFRESH_MS + 1000);
  assert.equal(logLines(state).length, 2);
  logAdmissionDecision(state, decision('a'), NOW + ADMISSION_LOG_REFRESH_MS + 2000);
  assert.equal(logLines(state).length, 2, 'the refresh restarts the window');
});

test('an admitted candidate is forgotten, so a later denial of the same id is written again', () => {
  const { state } = setup();
  logAdmissionDecision(state, decision('a'));
  logAdmissionDecision(state, decision('a', { admit: true, reason: 'ok' }));
  logAdmissionDecision(state, decision('a'));
  assert.equal(logLines(state).length, 3);
});

test('the admission log rotates to .1 at the size limit and keeps exactly one old file', () => {
  const dir = makeTmpDir('gc-rotate-');
  const file = path.join(dir, 'admission-decisions.log');
  const line = `${'x'.repeat(40)}\n`;
  for (let i = 0; i < 10; i += 1) appendRotatingLog(file, line, { maxBytes: 200, now: NOW });
  assert.ok(fs.existsSync(`${file}.1`), 'rotated');
  assert.ok(fs.statSync(file).size < 200 + line.length + 30, 'the live file restarted small');
  assert.ok(fs.statSync(`${file}.1`).size >= 200, 'the old file holds what filled it');
  assert.deepEqual(fs.readdirSync(dir).sort(), ['admission-decisions.log', 'admission-decisions.log.1'], 'only one old file is kept');
  assert.equal(ADMISSION_LOG_MAX_BYTES, 10 * 1024 * 1024);
});

// ---- config and CLI ----

test('the retention and bound keys are validated', () => {
  const f = freshEnv();
  const saved = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = f.home;
  try {
    for (const bad of [{ remoteTicketRetentionMs: 3_600_000 }, { logRetentionMs: 'soon' }, { remoteGcMaxTicketsPerRun: 0 }, { remoteGcMaxExaminedPerRun: 0 }, { logGcMaxFilesPerRun: 1.5 }]) {
      writeGlobalConfig(f.home, bad);
      assert.throws(() => loadGlobalConfig(), new RegExp(Object.keys(bad)[0]), JSON.stringify(bad));
    }
    writeGlobalConfig(f.home, { remoteTicketRetentionMs: 2 * DAY, logRetentionMs: 3 * DAY });
    assert.equal(loadGlobalConfig().remoteTicketRetentionMs, 2 * DAY);
    assert.equal(loadGlobalConfig().remoteGcMaxTicketsPerRun, 10);
  } finally {
    if (saved === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = saved;
  }
});

test('lane gc --dry-run prints a summary and changes nothing', async () => {
  const f = setup();
  writeGlobalConfig(f.home, {});
  const t = makeTicket(f.remoteRoot);
  const { code, stdout, stderr } = await laneRun(['gc', '--dry-run', '--root', f.remoteRoot], { env: f.env });
  assert.equal(code, 0, stderr);
  const out = JSON.parse(stdout);
  assert.equal(out.dryRun, true);
  assert.equal(out.remote.work, 1);
  assert.ok(fs.existsSync(t.workDir));
  const real = await laneRun(['gc', '--root', f.remoteRoot], { env: f.env });
  assert.equal(real.code, 0, real.stderr);
  assert.equal(fs.existsSync(t.workDir), false);
});
