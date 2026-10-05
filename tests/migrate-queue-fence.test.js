import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { freshEnv, writeGlobalConfig, gitFixture, laneRun, sleep } from './helpers.js';
import { enqueue, tryStart, listQueue, LegacyQueueAfterFenceError } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { migrateScheduler, runsLaneBroker, fenceLegacyQueue } from '../src/migrate.js';
import { readSchedulerFence } from '../src/fairness.js';
import { runCommand } from '../src/run.js';
import { withLock, paths, atomicWriteFile, atomicWriteJson, queueFenced, MigrationInProgressError, QUEUE_FENCE_NOTE } from '../src/state.js';
import { readHwm, effectiveNow } from '../src/priority-clock.js';
import { waitedMs } from '../src/priority.js';

/**
 * BRAIN-380: the cutover fence OLD code fails on by itself. `lane migrate-scheduler` replaces the legacy `queue/`
 * directory with a regular file, so a pre-migration process can never put a ticket in the queue (and old `tryStart`
 * starts only a ticket it finds there). Temporary state roots only.
 */

const REPO = fileURLToPath(new URL('..', import.meta.url));
const BIN = path.join(REPO, 'bin', 'lane.js');
const SRC = path.join(REPO, 'src');
const T0 = 1_700_000_000_000;

const pause = (state) => atomicWriteFile(paths(state).pause, 'test');
const migrate = (state, opts = {}) => migrateScheduler(state, { readProcesses: () => [], ...opts });
const ticket = (id, overrides = {}) => ({ id, key: `r:${id}`, repoId: 'r', weight: 1, cwd: process.cwd(), cmd: ['true'], supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null', ...overrides });
const sampler = () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() });
const memory = () => ({ availableBytes: 64 * 1024 ** 3, totalBytes: 64 * 1024 ** 3, macPressure: 'normal', source: 'test' });
const cfg = () => ({ ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
const poll = (state, t) => tryStart(state, t, cfg(), undefined, sampler, undefined, memory);

function migratedRoot() {
  const f = freshEnv();
  pause(f.state);
  return f;
}

/** main's code, extracted read-only into a temp dir: the "old version" a rollout may still have running. */
function extractOldCode() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-broker-old-'));
  const archive = spawnSync('git', ['-C', REPO, 'archive', 'origin/main'], { maxBuffer: 256 * 1024 * 1024 });
  assert.equal(archive.status, 0, `git archive origin/main: ${archive.stderr}`);
  execFileSync('tar', ['-x', '-C', dir], { input: archive.stdout });
  assert.ok(fs.existsSync(path.join(dir, 'bin', 'lane.js')), 'main has bin/lane.js');
  return dir;
}

function repoWith(base) {
  const dir = fs.mkdtempSync(path.join(base, 'repo-'));
  gitFixture(['init', '-q'], dir);
  return dir;
}

const markerCmd = (marker) => [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(marker)}, 'ran')`];

function runOld(oldDir, { env, cwd, marker }) {
  const child = spawn(process.execPath, [path.join(oldDir, 'bin', 'lane.js'), 'run', '--lane', 'default', '--', ...markerCmd(marker)], { env, cwd, timeout: 20_000 });
  let stderr = '';
  child.stderr.on('data', (d) => (stderr += d));
  const closed = new Promise((resolve) => child.on('close', (code) => resolve({ code, stderr })));
  return { child, closed };
}

// ---- the layout ----

test('the cutover leaves a regular file where old code expects queue/, and new code queues in queue-v2/', async () => {
  const { state } = migratedRoot();
  await enqueue(state, ticket('before')); // a ticket in the legacy directory would block the migration, so drain it
  fs.rmSync(path.join(paths(state).queue, fs.readdirSync(paths(state).queue)[0]));
  const result = await migrate(state);
  assert.equal(result.status, 'migrated');
  const queuePath = path.join(state, 'queue');
  assert.ok(fs.lstatSync(queuePath).isFile(), 'queue is now a regular file');
  assert.equal(fs.readFileSync(queuePath, 'utf8'), QUEUE_FENCE_NOTE);
  assert.match(QUEUE_FENCE_NOTE, /upgrade lane-broker/);
  assert.ok(fs.readdirSync(state).some((n) => n.startsWith('queue.legacy-')), 'the legacy directory is kept aside');
  assert.equal(queueFenced(state), true);
  assert.equal(paths(state).queue, path.join(state, 'queue-v2'));
  assert.equal(readSchedulerFence(state).status, 'valid');
  fs.rmSync(paths(state).pause);
  await enqueue(state, ticket('after'));
  assert.deepEqual(fs.readdirSync(path.join(state, 'queue-v2')).length, 1, 'new code writes to queue-v2');
  assert.deepEqual(listQueue(state).map((t) => t.id), ['after']);
  assert.equal(fs.readFileSync(paths(state).seq, 'utf8').length > 0, true, 'seq stays shared');
});

// ---- REAL old code ----

test('REAL old code (main) cannot be admitted against a migrated root: `lane run` fails and never runs the command', async () => {
  const oldDir = extractOldCode();
  const { state, home, env } = migratedRoot();
  writeGlobalConfig(home, { sampleMs: 100 });
  assert.equal((await migrate(state)).status, 'migrated');
  fs.rmSync(paths(state).pause); // the rollout resumes the broker; if the fence did not hold, old code would now be admitted
  const marker = path.join(path.dirname(state), 'old-ran');
  const { closed } = runOld(oldDir, { env, cwd: repoWith(path.dirname(state)), marker });
  const { code, stderr } = await closed;
  assert.notEqual(code, 0, `old lane run must fail, stderr: ${stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the command never ran');
  assert.deepEqual(fs.existsSync(path.join(state, 'leases')) ? fs.readdirSync(path.join(state, 'leases')) : [], [], 'no lease');
  assert.equal(fs.existsSync(paths(state).history), false, 'no history row');
  assert.deepEqual(fs.readdirSync(path.join(state, 'queue-v2')), [], 'nothing queued');
});

test('REAL old code launched AFTER the process snapshot (the migrator holds the lock) is still refused once the cutover completes', async () => {
  const oldDir = extractOldCode();
  const { state, home, env } = migratedRoot();
  writeGlobalConfig(home, { sampleMs: 100 });
  const marker = path.join(path.dirname(state), 'old-ran');
  let old;
  const result = await migrate(state, {
    // The snapshot (readProcesses) is already taken. An old `lane run` starts now, passes its own mkdir of queue/ (still a
    // directory), and blocks on the lock the migrator holds: exactly the process the snapshot cannot see.
    afterFairness: async () => {
      old = runOld(oldDir, { env, cwd: repoWith(path.dirname(state)), marker });
      await sleep(1500);
      assert.equal(old.child.exitCode, null, 'the old process is alive and waiting, not already failed');
    },
  });
  assert.equal(result.status, 'migrated');
  fs.rmSync(paths(state).pause);
  const { code, stderr } = await old.closed;
  assert.notEqual(code, 0, `stderr: ${stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the command never ran');
  assert.equal(fs.existsSync(paths(state).history), false);
  assert.deepEqual(fs.readdirSync(path.join(state, 'queue-v2')), [], 'the old ticket never reached any queue');
});

// ---- process matching, every token ----

test('runsLaneBroker scans every argv token and resolves symlinks', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-match-'));
  assert.equal(runsLaneBroker('node --require /x/hook.js /old/lane-broker/bin/lane.js run -- x'), true, 'script is not the first non-flag token');
  assert.equal(runsLaneBroker('node --import file:///x/y.mjs --inspect /old/src/supervisor.js'), true);
  assert.equal(runsLaneBroker('node /a/b/src/remote-pipeline.js /t'), true);
  const link = path.join(dir, 'launcher');
  fs.symlinkSync(BIN, link);
  assert.equal(runsLaneBroker(`node --require /x/hook.js ${link} run`), true, 'a symlink with any name that resolves to bin/lane.js');
  assert.equal(runsLaneBroker('node --require /x/hook.js /a/server.js'), false);
  assert.equal(runsLaneBroker('grep -r supervisor.js /a'), false);
});

// ---- crash safety ----

test('a HARD crash right after the fence (SIGKILL, no finally) leaves the marker; a fresh process recovers it', async () => {
  const { state, env } = migratedRoot();
  const script = `
    import { migrateScheduler } from ${JSON.stringify(path.join(SRC, 'migrate.js'))};
    await migrateScheduler(process.env.LANE_BROKER_STATE, { readProcesses: () => [], afterFence: () => process.kill(process.pid, 'SIGKILL') });
  `;
  const crashed = spawnSync(process.execPath, ['--input-type=module', '-e', script], { env });
  assert.equal(crashed.signal, 'SIGKILL', 'the migrator really died mid-migration');
  assert.equal(readSchedulerFence(state).status, 'valid', 'the commit point was reached');
  assert.equal(fs.existsSync(paths(state).migrating), true, 'the marker is stranded');
  await assert.rejects(enqueue(state, ticket('a')), MigrationInProgressError, 'and it refuses every admission');

  const recovered = await laneRun(['migrate-scheduler'], { env });
  assert.equal(recovered.code, 0, recovered.stderr);
  assert.match(recovered.stdout, /already migrated \(recovered\)/);
  assert.equal(fs.existsSync(paths(state).migrating), false, 'the marker is gone');
  await enqueue(state, ticket('b'));
  assert.deepEqual(listQueue(state).map((t) => t.id), ['b']);
  const again = await laneRun(['migrate-scheduler'], { env });
  assert.match(again.stdout, /already migrated \(valid scheduler fence present\)/, 'a second run is the ordinary no-op');
});

test('a crash between the queue fence and the scheduler fence is completed by a re-run', async () => {
  const { state } = migratedRoot();
  await assert.rejects(
    migrate(state, {
      afterFairness: () => {
        fenceLegacyQueue(state, 'early');
        throw new Error('simulated crash');
      },
    }),
    /simulated crash/,
  );
  assert.equal(readSchedulerFence(state).status, 'missing');
  assert.equal(queueFenced(state), true);
  assert.equal((await migrate(state)).status, 'migrated');
  assert.equal(readSchedulerFence(state).status, 'valid');
});

// ---- fence and a non-empty legacy directory ----

test('a valid fence over a non-empty legacy queue/ directory refuses admission with a clear error', async () => {
  const { state } = freshEnv();
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
  fs.mkdirSync(paths(state).queue, { recursive: true });
  fs.writeFileSync(path.join(paths(state).queue, '000000000001-x.json'), JSON.stringify(ticket('x')));
  await assert.rejects(enqueue(state, ticket('a')), (err) => err instanceof LegacyQueueAfterFenceError && /legacy queue directory .* still holds tickets/.test(err.message));
  await assert.rejects(poll(state, ticket('x')), LegacyQueueAfterFenceError);
});

// ---- quarantine ----

test('a quarantine rename that fails leaves the record unselectable: it reads as a barrier, and the failure is logged', async () => {
  const { state } = freshEnv();
  atomicWriteJson(paths(state).schedFence, { version: 2, migratedAt: T0 });
  atomicWriteJson(paths(state).fairness, { version: 2, tickets: {} });
  fenceLegacyQueue(state, 'test');
  const legacy = ticket('legacy'); // no schedVersion: what an escaped old process would write
  fs.writeFileSync(path.join(paths(state).queue, '000000000000-legacy.json'), JSON.stringify({ ...legacy, seq: 0, createdAt: T0 }));
  fs.writeFileSync(paths(state).queueQuarantine, 'a file where the quarantine directory should be, so the rename must fail');
  await enqueue(state, ticket('behind'));
  const own = await poll(state, legacy);
  assert.equal(own.started, false, 'the un-quarantined legacy record is never selected');
  assert.equal((await poll(state, ticket('behind'))).started, false, 'and nothing behind it moves past the barrier');
  assert.equal(fs.existsSync(path.join(state, 'leases', 'legacy.json')), false);
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /legacy-record-after-fence .*action=quarantine-failed/);
});

// ---- lock timeout at ticket creation ----

test('a lock timeout while stamping the origin leaves it null, so enqueue gives the ticket zero age even during a rollback', async () => {
  const { state, env, home } = freshEnv();
  writeGlobalConfig(home, {});
  const repoDir = repoWith(path.dirname(state));
  atomicWriteJson(paths(state).hwm, { hwm: Date.now() + 30 * 60_000 }); // the wall clock stepped back 30 minutes
  // Ticket creation runs in its own process (runCommand prints to stdout, which a node:test file process must not swallow),
  // with a supervisor stub that hands back the ticket it would have been spawned with.
  const script = `
        import { runCommand } from ${JSON.stringify(path.join(SRC, 'run.js'))};
    let ticket;
    await runCommand({
      lane: 'default', cmd: ['true'], cwd: ${JSON.stringify(repoDir)}, detach: true,
      spawnSupervisor: (_exe, _args, opts) => {
        ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
        const fake = new EventEmitter();
        fake.unref = () => {};
        setImmediate(() => fake.emit('spawn'));
        return fake;
      },
    });
    process.stderr.write(JSON.stringify(ticket));
  `;
  const holder = withLock(state, () => sleep(1500));
  await sleep(100);
  const created = spawnSync(process.execPath, ['--input-type=module', '-e', script], { env: { ...env, LANE_BROKER_TEST_LOCK_TIMEOUT_MS: '200' }, encoding: 'utf8' });
  await holder;
  assert.equal(created.status, 0, created.stderr);
  const ticketJson = created.stderr.slice(created.stderr.indexOf('{"id"'));
  const made = JSON.parse(ticketJson);
  assert.equal(made.prioOriginAt, null, 'no unlocked read of the clock');
  const record = await enqueue(state, { ...made, supervisorPid: process.pid });
  assert.equal(record.prioOriginAt, readHwm(state), 'enqueue assigned nowEff under the lock (the mark, mid-rollback)');
  assert.equal(waitedMs(record, effectiveNow(state)), 0);
});
