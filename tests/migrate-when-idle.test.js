import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, waitFor } from './helpers.js';
import { setup as remoteSetup, tmpDir, markerCmd, makeRunner } from './remote-harness.js';
import { enqueue, tryStart, listQueue } from '../src/scheduler.js';
import { createAttempt, readAttempt } from '../src/attempts.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { migrateScheduler, migrateSchedulerCommand, drainMigrateScheduler } from '../src/migrate.js';
import { clearStaleDrainMarker, drainPauseReason } from '../src/state.js';
import { resolveScheduler } from '../src/fairness.js';
import { selectRunner } from '../src/remote-client.js';
import { stampPriorityOrigin } from '../src/priority-clock.js';
import { bootId, paths, atomicWriteFile, atomicWriteJson, readJsonSafe, processStartTime, readDrainMarker, MigrationInProgressError, DRAINING_MESSAGE } from '../src/state.js';

/**
 * BRAIN-380 slice 4: `lane migrate-scheduler --when-idle`. Every test uses a temporary state root; none touches
 * ~/.cache/lane-broker. The process table is injected wherever a drain is expected to reach quiescence, because the
 * real one holds every other lane-broker process on the machine (this suite's own parallel children included).
 */

const SUPERVISOR = fileURLToPath(new URL('../src/supervisor.js', import.meta.url));
const noProcesses = () => [];
const FAST = { pollMs: 10, timeoutMs: 10_000, readProcesses: noProcesses };

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
const pause = (state) => atomicWriteFile(paths(state).pause, 'operator');
const isPaused = (state) => fs.existsSync(paths(state).pause);
const exists = (state, key) => fs.existsSync(paths(state)[key]);
/** A drain marker owned by a live process (this one, unless `pid` says otherwise). */
const markDraining = (state, pid = process.pid) => atomicWriteJson(paths(state).draining, { pid, startTime: processStartTime(pid), startedAt: Date.now() });

async function deadPid() {
  const child = spawn(process.execPath, ['-e', 'process.exit(0)']);
  const { pid } = child;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

const cfg = () => ({ ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
const poll = (state, t) => tryStart(state, t, cfg(), undefined, () => ({ hostBusyCores: 0, cores: 10, stale: false, sampledAt: Date.now() }));

/** A state root whose drain cannot reach quiescence: one RUNNING lease. Returns the state root and env. */
function busyRoot({ prePaused = false } = {}) {
  const { state, env, ...rest } = freshEnv();
  writeLease(state, lease('held-lane'));
  if (prePaused) pause(state);
  return { state, env, ...rest };
}

// ---- entry points while draining ----

test('while draining, enqueue, attempt creation and the ticket-creation stamp refuse with the draining message, writing nothing', async () => {
  const { state } = freshEnv();
  markDraining(state);
  const refusal = (err) => err instanceof MigrationInProgressError && err.message === 'scheduler migration pending (draining)' && err.message === DRAINING_MESSAGE;
  await assert.rejects(enqueue(state, ticket('t')), refusal);
  await assert.rejects(createAttempt(state, 't'), refusal);
  await assert.rejects(stampPriorityOrigin(state), refusal);
  assert.deepEqual(listQueue(state), []);
  assert.equal(readAttempt(state, 't'), null);
  assert.equal(fs.existsSync(paths(state).seq), false, 'a refused enqueue does not consume a seq');
});

test('lane run refuses with exit 75 while draining and creates no ticket, queue record or attempt', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  fs.mkdirSync(paths(state).queue, { recursive: true });
  markDraining(state);
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 75, result.stderr);
  assert.match(result.stderr, /scheduler migration pending \(draining\)/);
  assert.deepEqual(fs.readdirSync(paths(state).queue), []);
  assert.deepEqual(fs.readdirSync(paths(state).logs), [], 'no ticket was created');
});

test('a supervisor whose enqueue is refused while draining publishes exit 75 instead of dying with no result', async () => {
  const { state, env } = freshEnv();
  markDraining(state);
  const resultPath = path.join(paths(state).results, 'sup.json');
  const t = ticket('sup', { cwd: os.tmpdir(), createdAt: Date.now(), resultPath, supervisorPid: undefined });
  const child = spawn(process.execPath, [SUPERVISOR], { env: { ...env, LANE_BROKER_TICKET: Buffer.from(JSON.stringify(t)).toString('base64') } });
  let stderr = '';
  child.stderr.on('data', (d) => (stderr += d));
  const code = await new Promise((resolve) => child.on('close', resolve));
  assert.equal(code, 75);
  assert.match(stderr, /scheduler migration pending \(draining\)/);
  assert.equal(readJsonSafe(resultPath).exit, 75);
  assert.deepEqual(listQueue(state), []);
});

test('lane remote-exec on a runner refuses with exit 75 while draining, before creating a ticket directory', async () => {
  const { state, env } = freshEnv();
  markDraining(state);
  const root = tmpDir('remote-exec-root');
  const result = await laneRun(['remote-exec', '--root', root], { env });
  assert.equal(result.code, 75, result.stderr);
  assert.match(result.stderr, /scheduler migration pending \(draining\)/);
  assert.equal(fs.existsSync(path.join(root, 'tickets')), false);
});

test('remote dispatch refuses when the drain starts after the attempt exists: exit 75, the attempt is closed, nothing reaches the runner', async () => {
  const { env, state, repoDir } = remoteSetup();
  const live = JSON.stringify({ pid: process.pid, startTime: processStartTime(process.pid), startedAt: Date.now() });
  const wrapDir = tmpDir('probe-drain-bin');
  fs.writeFileSync(
    path.join(wrapDir, 'ssh'),
    `#!/bin/sh\ncase "$*" in *remote-probe*) echo '${live}' > ${JSON.stringify(paths(state).draining)};; esac\nPATH=${JSON.stringify(env.PATH)} exec ${JSON.stringify(path.join(env.PATH.split(path.delimiter)[0], 'ssh'))} "$@"\n`,
    { mode: 0o755 },
  );
  const marker = path.join(tmpDir('marker'), 'where');
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], {
    env: { ...env, PATH: `${wrapDir}${path.delimiter}${env.PATH}` },
    cwd: repoDir,
  });
  assert.equal(result.code, 75, result.stderr);
  assert.match(result.stderr, /scheduler migration pending \(draining\)/);
  assert.equal(fs.existsSync(marker), false, 'the command never ran, remotely or locally');
  assert.deepEqual(fs.readdirSync(paths(state).attempts), [], 'the attempt record is closed');
});

test('an already-queued ticket is still admitted and completes while draining', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const gate = path.join(base, 'gate');
  const ran = path.join(base, 'ran');
  const holder = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', `while [ ! -f ${gate} ]; do sleep 0.05; done`], { env, cwd: repoDir });
  await waitFor(() => fs.existsSync(paths(state).leases) && fs.readdirSync(paths(state).leases).length === 1, { timeoutMs: 30_000 });
  const queued = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'touch', ran], { env, cwd: repoDir });
  const queuedClosed = new Promise((resolve) => queued.on('close', resolve));
  const holderClosed = new Promise((resolve) => holder.on('close', resolve));
  await waitFor(() => fs.readdirSync(paths(state).queue).length === 1, { timeoutMs: 30_000 });

  markDraining(state);
  assert.equal(fs.existsSync(ran), false, 'still queued behind the holder');
  fs.writeFileSync(gate, '');
  assert.equal(await holderClosed, 0);
  assert.equal(await queuedClosed, 0, 'the queued ticket was admitted and completed');
  assert.equal(fs.existsSync(ran), true);
  assert.equal(fs.existsSync(paths(state).draining), true, 'the test owns this marker; nothing else removed it');
});

test('an already-queued ticket is admitted by tryStart while draining', async () => {
  const { state } = freshEnv();
  const t = ticket('queued-first');
  await enqueue(state, t);
  markDraining(state);
  assert.equal((await poll(state, t)).started, true);
});

// ---- probe, status, client ----

test('remote-probe reports a draining runner as draining (and paused, for older clients); a stale marker is not draining', async () => {
  const { state, env } = freshEnv();
  markDraining(state);
  const live = await laneRun(['remote-probe'], { env });
  assert.equal(live.code, 0, live.stderr);
  const payload = JSON.parse(live.stdout);
  assert.equal(payload.draining, true);
  assert.equal(payload.paused, true);

  atomicWriteJson(paths(state).draining, { pid: await deadPid(), startTime: 'Mon Jan  1 00:00:00 2024', startedAt: Date.now() });
  const stale = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.equal(stale.draining, false);
  assert.equal(stale.paused, false);
});

test('selectRunner skips a draining runner with reason "draining"', async () => {
  const binDir = tmpDir('draining-ssh');
  const sshBin = path.join(binDir, 'ssh');
  const probe = { protocol: 1, protocols: [1, 2], version: '0.0.0', draining: true, paused: false, queued: 0, running: 0, capacity: {} };
  fs.writeFileSync(sshBin, `#!/usr/bin/env node\nprocess.stdout.write(${JSON.stringify(`${JSON.stringify(probe)}\n`)});\n`);
  fs.chmodSync(sshBin, 0o755);
  const res = await selectRunner([makeRunner({ ssh: 'a' })], { sshBin, deadlineMs: 3000, maxRemoteQueue: 2 });
  assert.equal(res.runner, null);
  assert.deepEqual(res.skipped.map((s) => s.reason), ['draining']);
});

test('lane status shows who is draining and since when; a stale marker is called stale', async () => {
  const { state, env } = freshEnv();
  markDraining(state);
  const live = await laneRun(['status'], { env });
  assert.match(live.stdout, new RegExp(`draining for scheduler migration \\(pid ${process.pid}, since \\d{4}-\\d\\d-\\d\\dT`));
  atomicWriteJson(paths(state).draining, { pid: await deadPid(), startTime: null, startedAt: Date.now() });
  const stale = await laneRun(['status'], { env });
  assert.match(stale.stdout, /drain marker is stale/);
  assert.doesNotMatch(stale.stdout, /draining for scheduler migration/);
});

// ---- the drain itself ----

test('a drain waits for the queue, leases and attempts to clear, then migrates, resumes and clears both markers', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticket('queued-one'));
  writeLease(state, lease('held-lane'));
  atomicWriteJson(path.join(paths(state).attempts, 'att.json'), { id: 'att', phase: 'probe', supervisor: { pid: process.pid } });
  const lines = [];
  let sleeps = 0;
  const result = await drainMigrateScheduler(state, {
    ...FAST,
    log: (line) => lines.push(line),
    sleep: async () => {
      sleeps += 1;
      assert.equal(exists(state, 'draining'), true, 'the marker is held for the whole wait');
      assert.equal(exists(state, 'migrating'), false);
      assert.equal(isPaused(state), false, 'the broker is not paused while waiting: queued tickets can still be admitted');
      await assert.rejects(enqueue(state, ticket('late')), MigrationInProgressError);
      if (sleeps === 1) for (const f of fs.readdirSync(paths(state).queue)) fs.rmSync(path.join(paths(state).queue, f));
      if (sleeps === 2) removeLease(state, 'held-lane');
      if (sleeps === 3) fs.rmSync(path.join(paths(state).attempts, 'att.json'));
    },
  });
  assert.equal(result.status, 'migrated');
  assert.equal(result.resumed, true);
  assert.equal(sleeps, 3);
  assert.deepEqual(lines, [
    'lane migrate-scheduler: draining: queued 1, running 1, attempts 1, lane processes 0',
    'lane migrate-scheduler: draining: queued 0, running 1, attempts 1, lane processes 0',
    'lane migrate-scheduler: draining: queued 0, running 0, attempts 1, lane processes 0',
    'lane migrate-scheduler: draining: queued 0, running 0, attempts 0, lane processes 0',
  ]);
  assert.equal(resolveScheduler(state, { log: false }).v2, true, 'priority is active');
  assert.equal(isPaused(state), false, 'resumed');
  assert.equal(exists(state, 'draining'), false);
  assert.equal(exists(state, 'migrating'), false);
  await enqueue(state, ticket('after')); // intake is open again
});

test('a broker that was already paused stays paused after the drain migrates', async () => {
  const { state } = freshEnv();
  pause(state);
  const result = await drainMigrateScheduler(state, FAST);
  assert.equal(result.status, 'migrated');
  assert.equal(result.resumed, false);
  assert.equal(isPaused(state), true);
  assert.equal(fs.readFileSync(paths(state).pause, 'utf8'), 'operator');
  assert.equal(exists(state, 'draining'), false);
  assert.equal(resolveScheduler(state, { log: false }).v2, true);
});

test('a pre-paused broker with queued tickets warns that they will not start', async () => {
  const { state } = freshEnv();
  pause(state);
  await enqueue(state, ticket('stuck'));
  const lines = [];
  const result = await drainMigrateScheduler(state, { ...FAST, timeoutMs: 100, pollMs: 20, log: (l) => lines.push(l) });
  assert.equal(result.status, 'timeout');
  assert.equal(lines.filter((l) => /paused, so queued tickets will not start/.test(l)).length, 1);
});

for (const prePaused of [false, true]) {
  test(`a timeout clears the drain marker, restores the pause state (${prePaused ? 'was paused' : 'not paused'}) and exits non-zero`, async () => {
    const { state } = busyRoot({ prePaused });
    const result = await migrateSchedulerCommand({ root: state, whenIdle: true, timeoutMs: 150, pollMs: 20, readProcesses: noProcesses });
    assert.equal(result.exitCode, 1);
    assert.equal(exists(state, 'draining'), false, 'marker cleared');
    assert.equal(exists(state, 'migrating'), false);
    assert.equal(isPaused(state), prePaused, 'pause state restored');
    assert.equal(exists(state, 'schedFence'), false, 'not migrated');
    await enqueue(state, ticket('after')); // intake is open again
  });
}

test('the timeout message names what was still busy', async () => {
  const { state } = busyRoot();
  const err = [];
  const realErr = process.stderr.write;
  process.stderr.write = (chunk) => (err.push(String(chunk)), true);
  try {
    await migrateSchedulerCommand({ root: state, whenIdle: true, timeoutMs: 100, pollMs: 20, readProcesses: noProcesses });
  } finally {
    process.stderr.write = realErr;
  }
  const text = err.join('');
  assert.match(text, /not migrated: the broker was not idle after 0\.1s \(queued 0, running 1, attempts 0, lane processes 0\)/);
  assert.match(text, /still busy: lease held-lane is RUNNING/);
});

test('an unreadable record during the drain refuses at once, cleans up and restores the pause state', async () => {
  for (const prePaused of [false, true]) {
    const { state } = freshEnv();
    if (prePaused) pause(state);
    const bad = path.join(paths(state).leases, 'broken.json');
    fs.mkdirSync(path.dirname(bad), { recursive: true });
    fs.writeFileSync(bad, '{not json');
    const result = await drainMigrateScheduler(state, { ...FAST, timeoutMs: 60_000 });
    assert.equal(result.status, 'refused');
    assert.match(result.checks.flatMap((c) => c.failures)[0], /^unreadable lease record /);
    assert.equal(exists(state, 'draining'), false);
    assert.equal(exists(state, 'migrating'), false);
    assert.equal(isPaused(state), prePaused);
    assert.equal(exists(state, 'schedFence'), false);
  }
});

test('an unreadable process table refuses at once instead of waiting for a lane process that may not exist', async () => {
  const { state } = freshEnv();
  const result = await drainMigrateScheduler(state, {
    ...FAST,
    timeoutMs: 60_000,
    readProcesses: () => {
      throw new Error('ps: cannot fork');
    },
  });
  assert.equal(result.status, 'refused');
  assert.match(result.checks[0].failures[0], /^the process table could not be read \(ps: cannot fork\)/);
  assert.equal(exists(state, 'draining'), false);
});

test('a failure inside the migration cleans up: markers cleared, a pause this command made undone', async () => {
  for (const prePaused of [false, true]) {
    const { state } = freshEnv();
    if (prePaused) pause(state);
    await assert.rejects(
      drainMigrateScheduler(state, {
        ...FAST,
        afterFairness: () => {
          assert.equal(isPaused(state), true, 'the migration itself runs paused');
          throw new Error('simulated crash');
        },
      }),
      /simulated crash/,
    );
    assert.equal(exists(state, 'draining'), false);
    assert.equal(exists(state, 'migrating'), false);
    assert.equal(isPaused(state), prePaused);
    assert.equal(exists(state, 'schedFence'), false);
  }
});

test('a broker that turns busy between the quiescence check and the lock goes back to waiting under the same marker', async () => {
  const { state } = freshEnv();
  let reads = 0;
  const lane = { pid: 4242, ppid: 1, command: 'node /x/lane-broker/bin/lane.js status' };
  const readProcesses = () => {
    reads += 1;
    return reads === 2 ? [lane] : []; // quiet at the poll, busy under the lock, then quiet again
  };
  let sleeps = 0;
  const result = await drainMigrateScheduler(state, {
    ...FAST,
    readProcesses,
    sleep: async () => {
      sleeps += 1;
      assert.equal(exists(state, 'draining'), true, 'still draining');
      assert.equal(isPaused(state), false, 'the failed attempt undid its pause');
      assert.equal(exists(state, 'migrating'), false);
    },
  });
  assert.equal(result.status, 'migrated');
  assert.equal(sleeps, 1);
});

test('another live drain is refused and its marker is left alone', async () => {
  const { state } = freshEnv();
  markDraining(state, process.ppid);
  const before = fs.readFileSync(paths(state).draining, 'utf8');
  const result = await drainMigrateScheduler(state, FAST);
  assert.equal(result.status, 'refused');
  assert.match(result.checks[0].failures[0], new RegExp(`--when-idle is draining \\(pid ${process.ppid}\\)`));
  assert.equal(fs.readFileSync(paths(state).draining, 'utf8'), before);
});

// ---- signals and crashes: the real CLI as a child this test owns ----

for (const [signal, code] of [['SIGTERM', 143], ['SIGINT', 130]]) {
  for (const prePaused of [false, true]) {
    test(`${signal} while waiting clears the drain marker and restores the pause state (${prePaused ? 'was paused' : 'not paused'})`, async () => {
      const { state, env } = busyRoot({ prePaused });
      const child = laneSpawn(['migrate-scheduler', '--when-idle'], { env });
      let stdout = '';
      let stderr = '';
      child.stdout.on('data', (d) => (stdout += d));
      child.stderr.on('data', (d) => (stderr += d));
      const closed = new Promise((resolve) => child.on('close', (c, s) => resolve({ c, s })));
      await waitFor(() => exists(state, 'draining') && /draining: queued/.test(stdout), { timeoutMs: 30_000 });
      assert.equal(readDrainMarker(state).pid, child.pid);
      child.kill(signal);
      const { c } = await closed;
      assert.equal(c, code, stderr);
      assert.match(stderr, new RegExp(`interrupted by ${signal}`));
      assert.equal(exists(state, 'draining'), false);
      assert.equal(exists(state, 'migrating'), false);
      assert.equal(isPaused(state), prePaused);
      assert.equal(exists(state, 'schedFence'), false);
    });
  }
}

test('a SIGKILLed drain leaves a marker that is recognised as stale, ignored by intake, cleared by the next lane run and taken over by a re-run', async () => {
  const { state, env } = busyRoot();
  const child = laneSpawn(['migrate-scheduler', '--when-idle'], { env });
  const closed = new Promise((resolve) => child.on('close', resolve));
  await waitFor(() => exists(state, 'draining'), { timeoutMs: 30_000 });
  assert.equal(readDrainMarker(state).state, 'live');
  child.kill('SIGKILL');
  await closed;

  assert.equal(exists(state, 'draining'), true, 'SIGKILL leaves the marker behind');
  assert.equal(readDrainMarker(state).state, 'stale', 'but its owner is gone, so it is stale');
  const status = await laneRun(['status'], { env });
  assert.match(status.stdout, /drain marker is stale/);
  assert.equal(exists(state, 'draining'), true, 'status is read-only');
  await enqueue(state, ticket('not-blocked')); // a stale marker never refuses intake...
  assert.equal(exists(state, 'draining'), true, '...and enqueue does not need to clear it');

  await stampPriorityOrigin(state); // ...and the next `lane run` clears it, under the lock
  assert.equal(exists(state, 'draining'), false);

  // a second kill, then a re-run takes the stale marker over instead of refusing
  markDraining(state, await deadPid());
  const lines = [];
  removeLease(state, 'held-lane');
  fs.rmSync(path.join(paths(state).queue, fs.readdirSync(paths(state).queue)[0]));
  const result = await drainMigrateScheduler(state, { ...FAST, log: (l) => lines.push(l) });
  assert.equal(result.status, 'migrated');
  assert.ok(lines.some((l) => /took over a stale drain marker/.test(l)));
  assert.equal(exists(state, 'draining'), false);
});

test('a marker whose pid was reused by another process (start time differs) is stale', () => {
  const { state } = freshEnv();
  atomicWriteJson(paths(state).draining, { pid: process.pid, startTime: 'Mon Jan  1 00:00:00 1990', startedAt: Date.now() });
  assert.equal(readDrainMarker(state).state, 'stale');
});

test('--when-idle on an already-migrated root exits 0 at once, without a drain marker or a wait', async () => {
  const { state, env } = freshEnv();
  pause(state);
  assert.equal((await migrateScheduler(state, { readProcesses: noProcesses })).status, 'migrated');
  fs.rmSync(paths(state).pause);
  const result = await laneRun(['migrate-scheduler', '--when-idle'], { env });
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /already migrated/);
  assert.equal(exists(state, 'draining'), false);
  assert.equal(isPaused(state), false);
});

test('the CLI validates its flags: --when-idle excludes --dry-run, --timeout needs --when-idle and a valid duration', async () => {
  const { state, env } = freshEnv();
  for (const args of [['--when-idle', '--dry-run'], ['--timeout', '5m'], ['--when-idle', '--timeout', 'soon'], ['--when-idle', '--bogus']]) {
    const result = await laneRun(['migrate-scheduler', ...args], { env });
    assert.equal(result.code, 2, `${args.join(' ')}: ${result.stderr}`);
  }
  assert.equal(exists(state, 'draining'), false);
});

test('the CLI timeout flag is wired: --timeout 1s gives up, exits 1 and clears the marker', async () => {
  const { state, env } = busyRoot();
  const result = await laneRun(['migrate-scheduler', '--when-idle', '--timeout', '1s'], { env });
  assert.equal(result.code, 1, result.stderr);
  assert.match(result.stderr, /not idle after 1s/);
  assert.equal(exists(state, 'draining'), false);
});

// ---- review fixes: pause ownership, remote intake races, unreadable marker, start time ----

const MIGRATE_URL = new URL('../src/migrate.js', import.meta.url).href;

/**
 * A drain in a child process that stops dead at `seam` (`afterFairness`: paused, before the fence; `afterFence`: paused, after
 * the fence), so the test can SIGKILL it at the one moment it owns a pause. Resolves once the child is at the seam.
 */
async function drainKilledAt(state, seam) {
  const ready = path.join(path.dirname(state), `ready-${seam}`);
  const code = `
    import fs from 'node:fs';
    import { drainMigrateScheduler } from ${JSON.stringify(MIGRATE_URL)};
    setInterval(() => {}, 1000);
    const stop = async () => { fs.writeFileSync(process.env.READY, 'x'); await new Promise(() => {}); };
    await drainMigrateScheduler(process.env.ROOT, { pollMs: 10, readProcesses: () => [], ${seam}: stop });
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', code], { env: { ...process.env, ROOT: state, READY: ready } });
  const closed = new Promise((resolve) => child.on('close', resolve));
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 30_000 });
  const pid = child.pid;
  child.kill('SIGKILL');
  await closed;
  return pid;
}

for (const seam of ['afterFairness', 'afterFence']) {
  const fenced = seam === 'afterFence';
  test(`SIGKILL after the drain paused the broker (${fenced ? 'after' : 'before'} the fence): a lane run releases the drain's own pause`, async () => {
    const { state } = freshEnv();
    const pid = await drainKilledAt(state, seam);
    assert.equal(fs.readFileSync(paths(state).pause, 'utf8'), drainPauseReason(pid), 'the drain paused the broker with its own reason');
    assert.equal(readJsonSafe(paths(state).draining).pausedByDrain, true);
    assert.equal(readDrainMarker(state).state, 'stale');

    await assert.rejects(stampPriorityOrigin(state), MigrationInProgressError, 'the stale migrating marker still refuses intake until a re-run');
    assert.equal(isPaused(state), false, 'the dead drain\'s pause is released');
    assert.equal(exists(state, 'draining'), false);
  });

  test(`SIGKILL after the drain paused the broker (${fenced ? 'after' : 'before'} the fence): a re-run resumes it and finishes`, async () => {
    const { state } = freshEnv();
    await drainKilledAt(state, seam);
    const result = await drainMigrateScheduler(state, FAST);
    assert.equal(result.status, fenced ? 'recovered' : 'migrated');
    assert.equal(isPaused(state), false, 'resumed, not mistaken for an operator pause');
    assert.equal(exists(state, 'draining'), false);
    assert.equal(exists(state, 'migrating'), false);
    assert.equal(resolveScheduler(state, { log: false }).v2, true);
  });
}

test('SIGKILL of a drain on an operator-paused broker: recovery never resumes the operator\'s pause', async () => {
  const { state } = freshEnv();
  pause(state);
  await drainKilledAt(state, 'afterFairness');
  assert.equal(readJsonSafe(paths(state).draining).pausedByDrain, undefined, 'the drain did not pause it');
  await assert.rejects(stampPriorityOrigin(state), MigrationInProgressError);
  assert.equal(fs.readFileSync(paths(state).pause, 'utf8'), 'operator');
  const result = await drainMigrateScheduler(state, FAST);
  assert.equal(result.status, 'migrated');
  assert.equal(fs.readFileSync(paths(state).pause, 'utf8'), 'operator');
});

test('a stale marker that claims the pause is not resumed when PAUSE now holds somebody else\'s reason', async () => {
  const { state } = freshEnv();
  const pid = await deadPid();
  atomicWriteJson(paths(state).draining, { pid, startTime: null, startedAt: Date.now(), pausedByDrain: true });
  pause(state); // an operator paused it afterwards
  assert.equal(clearStaleDrainMarker(state), true);
  assert.equal(fs.readFileSync(paths(state).pause, 'utf8'), 'operator');
  assert.equal(exists(state, 'draining'), false);
});

test('remote dispatch: a drain that starts between the intake check and the attempt write is refused, the attempt closed, nothing sent', async () => {
  const { env, state, repoDir } = remoteSetup();
  const marker = path.join(tmpDir('race-marker'), 'where');
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], {
    env: { ...env, LANE_BROKER_TEST_DRAIN_AT: 'dispatch', LANE_BROKER_TEST_DRAIN_PID: String(process.pid) },
    cwd: repoDir,
  });
  assert.equal(exists(state, 'draining'), true, 'the seam fired');
  assert.equal(result.code, 75, result.stderr);
  assert.match(result.stderr, /scheduler migration pending \(draining\)/);
  assert.equal(fs.existsSync(marker), false, 'the command never ran, remotely or locally');
  assert.deepEqual(fs.readdirSync(paths(state).attempts), [], 'the attempt record is closed');
});

test('remote-exec: a drain that starts while the header is being read is refused before a ticket directory or any extraction', async () => {
  const { state, env } = freshEnv();
  const root = tmpDir('race-remote-exec-root');
  const child = laneSpawn(['remote-exec', '--root', root], { env: { ...env, LANE_BROKER_TEST_DRAIN_AT: 'remote-exec', LANE_BROKER_TEST_DRAIN_PID: String(process.pid) } });
  let stderr = '';
  child.stderr.on('data', (d) => (stderr += d));
  const closed = new Promise((resolve) => child.on('close', resolve));
  child.stdin.end(`${JSON.stringify({ ticketId: '123e4567-e89b-42d3-a456-426614174000' })}\n`);
  assert.equal(await closed, 75, stderr);
  assert.equal(exists(state, 'draining'), true, 'the seam fired');
  assert.match(stderr, /scheduler migration pending \(draining\)/);
  assert.equal(fs.existsSync(path.join(root, 'tickets')), false, 'no ticket directory, so nothing was extracted either');
});

for (const [kind, write] of [
  ['garbage', (file) => fs.writeFileSync(file, '{not json')],
  ['a JSON value that is not a marker', (file) => fs.writeFileSync(file, '"hello"')],
  ['a marker without a pid', (file) => fs.writeFileSync(file, '{"startedAt": 1}')],
  ['a directory (unreadable as a file)', (file) => fs.mkdirSync(file)],
]) {
  test(`an unreadable drain marker (${kind}) fails closed: intake refuses, nothing deletes it, a drain will not start over it`, async () => {
    const { state, env } = freshEnv();
    write(paths(state).draining);
    const refusal = (err) => err instanceof MigrationInProgressError && err.message === DRAINING_MESSAGE;
    await assert.rejects(enqueue(state, ticket('t')), refusal);
    await assert.rejects(stampPriorityOrigin(state), refusal, 'the lane-run path would clear a STALE marker, but not this one');
    assert.equal(readDrainMarker(state).state, 'unknown');
    assert.equal(clearStaleDrainMarker(state), false);
    assert.equal(fs.existsSync(paths(state).draining), true, 'still there: it could not be judged');

    const probe = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
    assert.equal(probe.draining, true);
    assert.match((await laneRun(['status'], { env })).stdout, /drain marker .* is unreadable or malformed/);

    const result = await drainMigrateScheduler(state, FAST);
    assert.equal(result.status, 'refused');
    assert.match(result.checks[0].failures[0], /cannot be read or parsed.*remove that file/);
    assert.equal(fs.existsSync(paths(state).draining), true);

    fs.rmSync(paths(state).draining, { recursive: true, force: true }); // what the README tells an operator to do
    await enqueue(state, ticket('after'));
  });
}

test('a drain that cannot read its own start time refuses to start and writes no marker', async () => {
  const { state } = freshEnv();
  const result = await drainMigrateScheduler(state, { ...FAST, pid: await deadPid() });
  assert.equal(result.status, 'refused');
  assert.match(result.checks[0].failures[0], /cannot read this process's start time/);
  assert.equal(exists(state, 'draining'), false);
  assert.equal(isPaused(state), false);
});
