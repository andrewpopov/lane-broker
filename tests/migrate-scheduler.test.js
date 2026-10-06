import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, waitFor } from './helpers.js';
import { setup as remoteSetup, tmpDir, markerCmd } from './remote-harness.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { createAttempt, readAttempt } from '../src/attempts.js';
import { writeLease, LEASE_STATE, isPidAlive } from '../src/lease.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { migrateScheduler, migrateSchedulerCommand, runsLaneBroker } from '../src/migrate.js';
import { resolveScheduler, readSchedulerFence } from '../src/fairness.js';
import { bootId, paths, atomicWriteFile, atomicWriteJson, readJsonSafe, MigrationInProgressError } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';

/**
 * BRAIN-380 slice 3: `lane migrate-scheduler`. Every test uses a temporary state root; none touches ~/.cache/lane-broker.
 * The process table is always injected, because the real one holds every other lane-broker process on the machine.
 */

const BIN = fileURLToPath(new URL('../bin/lane.js', import.meta.url));
const SUPERVISOR = fileURLToPath(new URL('../src/supervisor.js', import.meta.url));
const T0 = 1_700_000_000_000;

const noProcesses = () => [];
const pause = (state) => atomicWriteFile(paths(state).pause, 'test');
const paused = () => {
  const { state, ...rest } = freshEnv();
  pause(state);
  return { state, ...rest };
};
const ticket = (id, overrides = {}) => ({
  id,
  key: `r:${id}`,
  weight: 1,
  cwd: process.cwd(),
  cmd: ['true'],
  supervisorPid: process.pid,
  supervisorStart: null,
  logPath: '/dev/null',
  resultPath: '/dev/null',
  ...overrides,
});
const lease = (id, state = LEASE_STATE.RUNNING) => ({ id, key: `r:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state });
const migrate = (state, opts = {}) => migrateScheduler(state, { readProcesses: noProcesses, ...opts });
const failuresOf = (result) => result.checks.flatMap((c) => c.failures);
const migratingExists = (state) => fs.existsSync(paths(state).migrating);

/** Run `fn` with process.stdout/stderr writes captured. */
async function capture(fn) {
  const out = [];
  const err = [];
  const realOut = process.stdout.write;
  const realErr = process.stderr.write;
  process.stdout.write = (chunk) => (out.push(String(chunk)), true);
  process.stderr.write = (chunk) => (err.push(String(chunk)), true);
  try {
    const result = await fn();
    return { result, stdout: out.join(''), stderr: err.join('') };
  } finally {
    process.stdout.write = realOut;
    process.stderr.write = realErr;
  }
}

/** Every file under `dir` with its bytes: a snapshot a "changes nothing" test compares before and after. */
function snapshot(dir) {
  const files = {};
  const walk = (current) => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        files[`${path.relative(dir, full)}/`] = '';
        walk(full);
      } else {
        files[path.relative(dir, full)] = fs.readFileSync(full, 'utf8');
      }
    }
  };
  walk(dir);
  return files;
}

async function deadPid() {
  const child = spawn(process.execPath, ['-e', 'process.exit(0)']);
  const { pid } = child;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

// ---- refusals ----

test('refuses when the broker is not paused, leaving no marker and no cutover files', async () => {
  const { state } = freshEnv();
  const result = await migrate(state);
  assert.equal(result.status, 'refused');
  assert.deepEqual(failuresOf(result), ['the broker is not paused; run `lane pause` first']);
  assert.equal(migratingExists(state), false);
  assert.equal(fs.existsSync(paths(state).fairness), false);
  assert.equal(fs.existsSync(paths(state).schedFence), false);
});

test('refuses with a queued ticket, naming its id, and leaves the broker paused with the marker removed', async () => {
  const { state } = paused();
  await enqueue(state, ticket('queued-one'));
  const result = await migrate(state);
  assert.equal(result.status, 'refused');
  assert.deepEqual(failuresOf(result), ['queue is not empty: 1 queued ticket(s): queued-one']);
  assert.equal(migratingExists(state), false, 'the marker is removed on failure');
  assert.equal(fs.existsSync(paths(state).pause), true, 'the broker stays paused');
  assert.equal(fs.existsSync(paths(state).schedFence), false);
});

for (const leaseState of [LEASE_STATE.RUNNING, LEASE_STATE.ORPHANED]) {
  test(`refuses with a ${leaseState} lease, naming its id`, async () => {
    const { state } = paused();
    writeLease(state, lease('held-lane', leaseState));
    const result = await migrate(state);
    assert.equal(result.status, 'refused');
    assert.deepEqual(failuresOf(result), [`lease held-lane is ${leaseState}`]);
  });
}

test('refuses with a non-terminal remote attempt even though the queue is EMPTY', async () => {
  const { state } = paused();
  await createAttempt(state, 'attempt-only');
  assert.deepEqual(listQueue(state), [], 'the queue cannot see this supervisor');
  const result = await migrate(state);
  assert.equal(result.status, 'refused');
  assert.deepEqual(failuresOf(result), [`attempt attempt-only is still in phase probe (supervisor pid ${process.pid})`]);
  assert.equal(migratingExists(state), false);
});

for (const [kind, dirKey] of [['lease', 'leases'], ['attempt', 'attempts'], ['queue', 'queue']]) {
  test(`an unreadable ${kind} record refuses the migration instead of being skipped`, async () => {
    const { state } = paused();
    const bad = path.join(paths(state)[dirKey], 'broken.json');
    fs.mkdirSync(path.dirname(bad), { recursive: true });
    fs.writeFileSync(bad, '{not json');
    const result = await migrate(state);
    assert.equal(result.status, 'refused');
    const [failure] = failuresOf(result);
    assert.match(failure, new RegExp(`^unreadable ${kind} record ${bad.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}: `));
    assert.equal(fs.existsSync(paths(state).schedFence), false);
  });
}

test('an unreadable record that parses to a non-object is also refused', async () => {
  const { state } = paused();
  fs.mkdirSync(paths(state).leases, { recursive: true });
  fs.writeFileSync(path.join(paths(state).leases, 'null.json'), 'null');
  assert.equal((await migrate(state)).status, 'refused');
});

test('a live lane process blocks the migration whatever its version, but the migrator and its ancestors do not', async () => {
  const { state } = paused();
  const table = [
    { pid: 1, ppid: 0, command: '/sbin/launchd' },
    { pid: 50, ppid: 1, command: 'node /Users/x/node_modules/@andrewpopov/lane-broker/bin/lane.js run --repo r -- npm test' }, // an ancestor: excluded
    { pid: 60, ppid: 50, command: `node ${BIN} migrate-scheduler` }, // the migrator itself
    { pid: 70, ppid: 1, command: 'node /old/0.9.0/lane-broker/src/supervisor.js' },
    { pid: 71, ppid: 1, command: 'node --no-warnings /new/lane-broker/bin/lane.js remote-pipeline /tickets/abc' },
    { pid: 72, ppid: 1, command: 'node --require /x/hook.js /old/lane-broker/bin/lane.js run -- x' }, // the script is not node's first non-flag argument
    { pid: 73, ppid: 1, command: 'vim /new/lane-broker/src/supervisor.js' }, // not running it, but every token is scanned: fail closed
    { pid: 74, ppid: 1, command: 'node /a/server.js --port 1' },
  ];
  const result = await migrateScheduler(state, { readProcesses: () => table, pid: 60 });
  assert.equal(result.status, 'refused');
  assert.deepEqual(
    failuresOf(result).map((f) => f.match(/^lane process pid (\d+) is alive/)?.[1]),
    ['70', '71', '72', '73'],
  );
  assert.match(failuresOf(result)[0], /supervisor\.js$/);
  assert.equal(migratingExists(state), false);
});

test('an unreadable process table refuses rather than assuming no lane process is alive', async () => {
  const { state } = paused();
  const result = await migrateScheduler(state, {
    readProcesses: () => {
      throw new Error('ps: cannot fork');
    },
  });
  assert.equal(result.status, 'refused');
  assert.match(failuresOf(result)[0], /^the process table could not be read \(ps: cannot fork\); refusing/);
});

test('runsLaneBroker recognises the script as the executable or as node\'s first argument, including a `lane` symlink', () => {
  const dir = makeTmpDir('lane-bin-');
  fs.mkdirSync(path.join(dir, 'pkg', 'bin'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'pkg', 'bin', 'lane.js'), '');
  fs.symlinkSync(path.join(dir, 'pkg', 'bin', 'lane.js'), path.join(dir, 'lane'));
  fs.writeFileSync(path.join(dir, 'other'), '');
  fs.symlinkSync(path.join(dir, 'other'), path.join(dir, 'lane-other'));
  assert.equal(runsLaneBroker(`node ${path.join(dir, 'lane')} run -- x`), true);
  assert.equal(runsLaneBroker(`${path.join(dir, 'lane')} status`), true);
  assert.equal(runsLaneBroker('/usr/local/bin/node /a/b/src/supervisor.js'), true);
  assert.equal(runsLaneBroker('node /a/b/bin/lane.js remote-exec --root /r'), true);
  assert.equal(runsLaneBroker('node /gone/lane run'), true, 'an unresolvable lane symlink is treated as lane-broker');
  assert.equal(runsLaneBroker('node /a/server.js'), false);
  assert.equal(runsLaneBroker('grep -r supervisor.js /a'), false);
});

test('another migrator that is alive refuses; a marker left by a dead one is taken over', async () => {
  const { state } = paused();
  const sleeper = spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 30000)']);
  try {
    atomicWriteJson(paths(state).migrating, { pid: sleeper.pid, startedAt: T0 });
    const refused = await migrate(state);
    assert.equal(refused.status, 'refused');
    assert.match(failuresOf(refused)[0], new RegExp(`^another lane migrate-scheduler is running \\(pid ${sleeper.pid}\\)`));
    assert.equal(migratingExists(state), true, 'the other migrator\'s marker is not ours to remove');
  } finally {
    sleeper.kill();
  }
  atomicWriteJson(paths(state).migrating, { pid: await deadPid(), startedAt: T0 });
  assert.equal((await migrate(state)).status, 'migrated');
  assert.equal(migratingExists(state), false);
});

// ---- the cutover ----

test('migrates a drained, paused broker: fairness-v2 empty, a valid fence, still paused, no marker', async () => {
  const { state } = paused();
  const result = await migrate(state);
  assert.equal(result.status, 'migrated');
  assert.deepEqual(readJsonSafe(paths(state).fairness), { version: 2, tickets: {} });
  assert.equal(readSchedulerFence(state).status, 'valid');
  assert.equal(resolveScheduler(state, { log: false }).v2, true);
  assert.equal(migratingExists(state), false);
  assert.equal(fs.existsSync(paths(state).pause), true, 'the operator resumes with `lane resume`');
});

test('archives the three legacy singleton skip files with a timestamp suffix, content intact', async () => {
  const { state } = paused();
  const p = paths(state);
  const files = { [p.conflictSkipState]: { headId: 'a', count: 1 }, [p.capacitySkipState]: { headId: 'b', count: 2 }, [p.resourceSkipState]: { headId: 'c', count: 3 } };
  for (const [file, body] of Object.entries(files)) atomicWriteJson(file, body);
  await migrate(state, { now: () => T0 });
  for (const [file, body] of Object.entries(files)) {
    assert.equal(fs.existsSync(file), false, `${path.basename(file)} is archived`);
    assert.deepEqual(readJsonSafe(`${file}.migrated-${new Date(T0).toISOString().replace(/[:.]/g, '-')}`), body);
  }
});

test('a crash between the two renames leaves fairness-v2 without a fence; a re-run completes; a re-run after success is a no-op', async () => {
  const { state } = paused();
  await assert.rejects(
    migrate(state, {
      afterFairness: () => {
        throw new Error('simulated crash');
      },
    }),
    /simulated crash/,
  );
  assert.equal(fs.existsSync(paths(state).fairness), true);
  assert.equal(readSchedulerFence(state).status, 'missing', 'no commit point yet');
  assert.equal(resolveScheduler(state, { log: false }).v2, false, 'still the legacy scheduler');
  assert.equal(migratingExists(state), false);

  assert.equal((await migrate(state)).status, 'migrated');
  assert.equal(readSchedulerFence(state).status, 'valid');

  const before = snapshot(state);
  const again = await capture(() => migrateSchedulerCommand({ root: state, readProcesses: noProcesses }));
  assert.equal(again.result.exitCode, 0);
  assert.match(again.stdout, /already migrated/);
  assert.deepEqual(snapshot(state), before);
});

test('a marker left behind by a hard crash does not wedge the next run', async () => {
  const { state } = paused();
  atomicWriteJson(paths(state).migrating, { pid: await deadPid(), startedAt: T0 });
  await assert.rejects(enqueue(state, ticket('t')), MigrationInProgressError);
  assert.equal((await migrate(state)).status, 'migrated');
  await enqueue(state, ticket('t'));
});

test('both cutover writes fsync the file before the rename and the directory after it', async (t) => {
  const { state } = paused();
  const events = [];
  const opened = new Map();
  const realOpen = fs.openSync;
  const realFsync = fs.fsyncSync;
  const realRename = fs.renameSync;
  t.mock.method(fs, 'openSync', (file, ...rest) => {
    const fd = realOpen(file, ...rest);
    opened.set(fd, String(file));
    return fd;
  });
  t.mock.method(fs, 'fsyncSync', (fd) => {
    events.push(['fsync', opened.get(fd)]);
    return realFsync(fd);
  });
  t.mock.method(fs, 'renameSync', (from, to) => {
    events.push(['rename', String(to)]);
    return realRename(from, to);
  });
  await migrate(state);
  const p = paths(state);
  for (const target of [p.fairness, p.schedFence]) {
    const at = events.findIndex(([kind, file]) => kind === 'rename' && file === target);
    assert.ok(at > 0, `${path.basename(target)} was renamed into place`);
    const before = events[at - 1];
    assert.equal(before[0], 'fsync');
    assert.equal(path.dirname(before[1]), state, `${path.basename(target)}'s temp file was fsynced first`);
    assert.match(path.basename(before[1]), /^\.tmp-/);
    assert.deepEqual(events[at + 1], ['fsync', state], `the directory was fsynced after renaming ${path.basename(target)}`);
  }
  assert.ok(events.findIndex(([, f]) => f === p.fairness) < events.findIndex(([, f]) => f === p.schedFence), 'fairness-v2 is durable before the fence');
});

test('without the fsync option atomicWriteFile does not fsync (it is only for the migration writes)', async (t) => {
  const { state } = freshEnv();
  const spy = t.mock.method(fs, 'fsyncSync');
  atomicWriteFile(path.join(state, 'plain'), 'x');
  assert.equal(spy.mock.callCount(), 0);
});

// ---- dry run ----

test('--dry-run changes nothing, whether it would refuse or migrate', async () => {
  const { state } = paused();
  await enqueue(state, ticket('queued-one'));
  atomicWriteJson(paths(state).conflictSkipState, { headId: 'a', count: 1 });
  let before = snapshot(state);
  const refused = await capture(() => migrateSchedulerCommand({ root: state, dryRun: true, readProcesses: noProcesses }));
  assert.equal(refused.result.exitCode, 1);
  assert.match(refused.stderr, /refused: queue is not empty: 1 queued ticket\(s\): queued-one/);
  assert.match(refused.stdout, /ok: paused/);
  assert.deepEqual(snapshot(state), before);

  fs.rmSync(paths(state).queue, { recursive: true });
  fs.mkdirSync(paths(state).queue);
  before = snapshot(state);
  const ready = await capture(() => migrateSchedulerCommand({ root: state, dryRun: true, readProcesses: noProcesses }));
  assert.equal(ready.result.exitCode, 0);
  assert.match(ready.stdout, /all preconditions hold; nothing was changed/);
  assert.deepEqual(snapshot(state), before, 'no marker, no fence, no fairness file, no archive');
});

test('the CLI wires the command: not paused exits 1 and says why; an unknown argument exits 2', async () => {
  const { env } = freshEnv();
  const notPaused = await laneRun(['migrate-scheduler', '--dry-run'], { env });
  assert.equal(notPaused.code, 1, notPaused.stdout + notPaused.stderr);
  assert.match(notPaused.stderr, /refused: the broker is not paused; run `lane pause` first/);
  const unknown = await laneRun(['migrate-scheduler', '--wat'], { env });
  assert.equal(unknown.code, 2);
});

// ---- entry points while `migrating` exists ----

const markMigrating = (state) => atomicWriteJson(paths(state).migrating, { pid: process.pid, startedAt: T0 });

test('enqueue and attempt creation refuse while migrating, writing nothing', async () => {
  const { state } = freshEnv();
  markMigrating(state);
  await assert.rejects(enqueue(state, ticket('t')), (err) => err instanceof MigrationInProgressError && err.message === 'scheduler migration in progress');
  await assert.rejects(createAttempt(state, 't'), MigrationInProgressError);
  assert.deepEqual(listQueue(state), []);
  assert.equal(readAttempt(state, 't'), null);
  assert.equal(fs.existsSync(paths(state).seq), false, 'a refused enqueue does not consume a seq');
});

test('lane run refuses with exit 75 while migrating and creates no ticket, queue record or attempt', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  fs.mkdirSync(paths(state).queue, { recursive: true });
  markMigrating(state);
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 75, result.stderr);
  assert.match(result.stderr, /scheduler migration in progress/);
  assert.deepEqual(fs.readdirSync(paths(state).queue), []);
  assert.deepEqual(fs.readdirSync(paths(state).logs), [], 'no ticket was created');
});

test('a supervisor whose admission is refused publishes exit 75 instead of dying with no result', async () => {
  const { state, env } = freshEnv();
  markMigrating(state);
  const resultPath = path.join(paths(state).results, 'sup.json');
  const t = ticket('sup', { cwd: os.tmpdir(), createdAt: Date.now(), resultPath, supervisorPid: undefined });
  const child = spawn(process.execPath, [SUPERVISOR], { env: { ...env, LANE_BROKER_TICKET: Buffer.from(JSON.stringify(t)).toString('base64') } });
  const code = await new Promise((resolve) => child.on('close', resolve));
  assert.equal(code, 75);
  assert.equal(readJsonSafe(resultPath).exit, 75);
  assert.equal(readJsonSafe(resultPath).reason, 'scheduler-migration');
  assert.deepEqual(listQueue(state), []);
});

test('lane remote-exec on a runner refuses with exit 75 before reading the header or creating a ticket directory', async () => {
  const { state, env } = freshEnv();
  markMigrating(state);
  const root = tmpDir('remote-exec-root');
  const result = await laneRun(['remote-exec', '--root', root], { env });
  assert.equal(result.code, 75, result.stderr);
  assert.match(result.stderr, /scheduler migration in progress/);
  assert.equal(fs.existsSync(path.join(root, 'tickets')), false);
});

test('remote dispatch refuses when the marker appears after the attempt exists: exit 75, the attempt is closed, nothing reaches the runner', async () => {
  const { env, state, repoDir } = remoteSetup();
  // The marker appears while the supervisor is probing runners: after createAttempt, before dispatch.
  const wrapDir = tmpDir('probe-marker-bin');
  fs.writeFileSync(
    path.join(wrapDir, 'ssh'),
    `#!/bin/sh\ncase "$*" in *remote-probe*) echo '{}' > ${JSON.stringify(paths(state).migrating)};; esac\nPATH=${JSON.stringify(env.PATH)} exec ${JSON.stringify(path.join(env.PATH.split(path.delimiter)[0], 'ssh'))} "$@"\n`,
    { mode: 0o755 },
  );
  const marker = path.join(tmpDir('marker'), 'where');
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], {
    env: { ...env, PATH: `${wrapDir}${path.delimiter}${env.PATH}` },
    cwd: repoDir,
  });
  assert.equal(result.code, 75, result.stderr);
  assert.match(result.stderr, /scheduler migration in progress/);
  assert.equal(fs.existsSync(marker), false, 'the command never ran, remotely or locally');
  assert.deepEqual(fs.readdirSync(paths(state).attempts), [], 'the attempt record is closed, so it cannot block the migration forever');
});

// ---- quarantine ----

const cfg = () => ({ ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
const poll = (state, t) => tryStart(state, t, cfg(), undefined, () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() }));
const readLog = (state) => (fs.existsSync(paths(state).admissionLog) ? fs.readFileSync(paths(state).admissionLog, 'utf8') : '');

test('a legacy record after the fence is quarantined and logged, never selected, and its owner finds itself gone', async () => {
  const { state } = paused();
  await migrate(state);
  fs.rmSync(paths(state).pause);
  // what an escaped pre-migration supervisor would have written: the older seq, no schedVersion
  const legacy = ticket('legacy', { seq: 1, createdAt: T0 });
  const legacyFile = path.join(paths(state).queue, '000000000001-legacy.json');
  atomicWriteJson(legacyFile, legacy);
  const fresh = ticket('fresh');
  await enqueue(state, fresh);

  const started = await poll(state, fresh);
  assert.equal(started.started, true, 'the fresh ticket starts although the legacy record has the lower seq');
  assert.equal(fs.existsSync(legacyFile), false);
  assert.equal(fs.existsSync(path.join(paths(state).queueQuarantine, '000000000001-legacy.json')), true);
  assert.match(readLog(state), /event=legacy-record-after-fence ticket=legacy file=000000000001-legacy\.json action=quarantined/);
  assert.deepEqual(listQueue(state), []);
  const owner = await poll(state, legacy);
  assert.deepEqual([owner.started, owner.reason, owner.position], [false, 'not-head', null], 'the owner sees its ticket gone, which its supervisor finalizes as cancelled');
});

test('before the fence a record without schedVersion is an ordinary legacy ticket and is not quarantined', async () => {
  const { state } = freshEnv();
  atomicWriteJson(path.join(paths(state).queue, '000000000001-old.json'), ticket('old', { seq: 1 }));
  assert.equal((await poll(state, ticket('old'))).started, true);
  assert.equal(fs.existsSync(paths(state).queueQuarantine), false);
});

test('a supervisor whose queue record is quarantined exits as cancelled: no lease, no run', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  pause(state);
  await migrate(state);
  const ran = path.join(base, 'ran');
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'touch', ran], { env, cwd: repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  const queueDir = paths(state).queue;
  await waitFor(() => fs.readdirSync(queueDir).some((n) => n.includes(id)), { timeoutMs: 30_000 });
  const name = fs.readdirSync(queueDir).find((n) => n.includes(id));
  const record = readJsonSafe(path.join(queueDir, name));
  assert.equal(record.schedVersion, 2);
  const { schedVersion: _dropped, ...legacy } = record;
  atomicWriteJson(path.join(queueDir, name), legacy); // the supervisor's record turns legacy behind the fence
  try {
    await waitFor(() => !isPidAlive(record.supervisorPid), { timeoutMs: 30_000 });
  } finally {
    if (isPidAlive(record.supervisorPid)) process.kill(record.supervisorPid, 'SIGTERM'); // a failing run must not leave the supervisor polling
  }
  assert.equal(fs.existsSync(path.join(paths(state).queueQuarantine, name)), true);
  assert.equal(fs.existsSync(ran), false, 'the quarantined ticket never ran');
  assert.deepEqual(fs.readdirSync(paths(state).leases), []);
});

// ---- end to end ----

test('legacy FIFO, migrate, then a high ticket overtakes medium and low', async () => {
  const { state } = freshEnv();
  const c = { ...cfg(), capacity: 1 };
  const pollWith = (t) => tryStart(state, t, c, undefined, () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() }));
  const finish = (id) => {
    fs.rmSync(path.join(paths(state).leases, `${id}.json`));
  };

  // before: strict FIFO, a high arriving later does not jump the queue
  const first = ticket('first');
  const late = ticket('late', { priorityRequested: 'high' });
  await enqueue(state, first);
  await enqueue(state, late);
  assert.equal((await pollWith(late)).reason, 'not-head');
  assert.equal((await pollWith(first)).started, true);
  finish('first');
  assert.equal((await pollWith(late)).started, true);
  finish('late');

  pause(state);
  assert.equal((await migrate(state)).status, 'migrated');
  fs.rmSync(paths(state).pause);

  // after: a held lane, then low, medium, high queued in that order; the high starts first, then medium, then low
  writeLease(state, lease('holder'));
  const low = ticket('low', { priorityRequested: 'low' });
  const med = ticket('med');
  const high = ticket('high', { priorityRequested: 'high' });
  for (const t of [low, med, high]) await enqueue(state, t);
  finish('holder');
  assert.equal((await pollWith(low)).reason, 'not-head');
  assert.equal((await pollWith(med)).reason, 'not-head');
  assert.equal((await pollWith(high)).started, true);
  finish('high');
  assert.equal((await pollWith(low)).reason, 'not-head');
  assert.equal((await pollWith(med)).started, true);
  finish('med');
  assert.equal((await pollWith(low)).started, true);
});

test('startup validation: an invalid fence, or an invalid present fairness-v2.json behind a valid fence, means legacy mode and the log line', async () => {
  const { state } = paused();
  await migrate(state);
  fs.writeFileSync(paths(state).fairness, '{garbage');
  assert.equal(resolveScheduler(state).v2, false);
  assert.match(readLog(state), /scheduler-fence-invalid reason=fairness-not-json/);
  const again = await capture(() => migrateSchedulerCommand({ root: state, readProcesses: noProcesses }));
  assert.equal(again.result.exitCode, 1, 'a valid fence over a broken fairness file is not reported as migrated');
});
