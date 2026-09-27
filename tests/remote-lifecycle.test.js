import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, laneSpawn, waitFor, sleep, writeGlobalConfig } from './helpers.js';
import { tmpDir, setup, markerCmd, detachAndWait, resultOf } from './remote-harness.js';
import { readAttempt } from '../src/attempts.js';
import { readLease, isGroupAlive } from '../src/lease.js';
import { paths } from '../src/state.js';

/**
 * BRAIN-319 T3b-4: `lane status`/`wait`/`cancel` against an attempt record
 * still mid remote-dispatch, or ORPHANED-REMOTE (dead supervisor). Drives
 * the REAL CLI, same fake-ssh transport as tests/remote-dispatch.test.js.
 */

/** A never-exiting remote command that writes `startedMarker` the instant it
 *  starts, so a test can wait for the dispatch to be genuinely in flight
 *  before acting on it (SIGKILL, cancel, status) instead of racing dial. */
function sleeperCmd(startedMarker) {
  return [
    process.execPath,
    '-e',
    `require('fs').writeFileSync(${JSON.stringify(startedMarker)}, 'x'); setInterval(() => {}, 1000);`,
  ];
}

async function startSleepingRemote(env, repoDir) {
  const startedMarker = path.join(tmpDir('started'), 'started');
  const started = await laneRun(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...sleeperCmd(startedMarker)],
    { env, cwd: repoDir },
  );
  assert.equal(started.code, 0, `--detach should not fail: ${started.stderr}`);
  const id = started.stdout.trim();
  await waitFor(() => fs.existsSync(startedMarker), { timeoutMs: 15_000 });
  return { id, startedMarker };
}

test('lane status shows a REMOTE entry for a sleeping remote child, and it counts toward nothing', async () => {
  const { env, state, repoDir } = setup();
  const { id } = await startSleepingRemote(env, repoDir);
  try {
    const result = await laneRun(['status', '--json'], { env });
    assert.equal(result.code, 0, `stderr: ${result.stderr}`);
    const status = JSON.parse(result.stdout);
    const entry = status.remote.find((r) => r.id === id);
    assert.ok(entry, 'expected a REMOTE entry for the sleeping ticket');
    assert.equal(entry.runner, 'skybox');
    assert.equal(entry.phase, 'running');
    assert.equal(entry.orphaned, false);
    assert.ok(entry.elapsedMs >= 0);
    // Never counted toward local capacity/resources: nothing is running or
    // queued locally, and used/reserved stay at zero.
    assert.equal(status.running.length, 0);
    assert.equal(status.queued.length, 0);
    assert.equal(status.used, 0);
    assert.equal(status.resources.reservedCpuCores, 0);
    assert.equal(status.resources.reservedMemoryBytes, 0);

    const text = (await laneRun(['status'], { env })).stdout;
    assert.match(text, /REMOTE:/);
    assert.match(text, new RegExp(id));
  } finally {
    await laneRun(['cancel', id], { env });
  }
});

test('lane wait on a live remote ticket blocks until its result, then returns its real exit', async () => {
  const { env, repoDir } = setup();
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 7)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 7, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
});

test('lane cancel on a live remote ticket exits 130, never runs locally, and the runner\'s own ticket is cancelled', async () => {
  const { env, state, runnerRoot, repoDir } = setup();
  const { id } = await startSleepingRemote(env, repoDir);

  const cancelResult = await laneRun(['cancel', id], { env });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);

  const waited = await laneRun(['wait', id, '--timeout', '15s'], { env });
  assert.equal(waited.code, 130, `stderr: ${waited.stderr}`);
  assert.equal(resultOf(state, id).cancelled, true);
  assert.equal(readAttempt(state, id), null, 'the attempt record must be gone once cancellation is published');

  const ticketsDir = path.join(runnerRoot, 'tickets');
  const [remoteTicketId] = fs.readdirSync(ticketsDir);
  const record = await waitFor(() => {
    const fp = path.join(ticketsDir, remoteTicketId, 'result.json');
    return fs.existsSync(fp) ? JSON.parse(fs.readFileSync(fp, 'utf8')) : null;
  });
  assert.equal(record.kind, 'cancelled');
});

test('SIGKILLing the supervisor mid-remote-dispatch: status shows ORPHANED-REMOTE, wait exits 1 naming it, cancel reconciles, and a second cancel is a no-op', async () => {
  const { env, state, runnerRoot, repoDir } = setup();
  const { id } = await startSleepingRemote(env, repoDir);

  const attempt = readAttempt(state, id);
  assert.ok(attempt, 'expected an attempt record');
  process.kill(attempt.supervisor.pid, 'SIGKILL');
  // Give the OS a moment to actually reap the pid before probing liveness.
  await sleep(300);

  const status = JSON.parse((await laneRun(['status', '--json'], { env })).stdout);
  const entry = status.remote.find((r) => r.id === id);
  assert.ok(entry, 'expected the orphaned attempt to still be reported');
  assert.equal(entry.orphaned, true);

  const statusText = (await laneRun(['status'], { env })).stdout;
  assert.match(statusText, /ORPHANED-REMOTE/);

  const waited = await laneRun(['wait', id, '--timeout', '5s'], { env });
  assert.equal(waited.code, 1, `stderr: ${waited.stderr}`);
  assert.match(waited.stderr, /ORPHANED-REMOTE/);
  assert.match(waited.stderr, new RegExp(`lane cancel ${id}`));

  const cancelResult = await laneRun(['cancel', id], { env });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);
  assert.equal(readAttempt(state, id), null, 'the attempt record must be gone once reconciled');
  const result = resultOf(state, id);
  assert.equal(result.exit, 130);
  assert.equal(result.cancelled, true);

  const ticketsDir = path.join(runnerRoot, 'tickets');
  const [remoteTicketId] = fs.readdirSync(ticketsDir);
  const runnerRecord = await waitFor(() => {
    const fp = path.join(ticketsDir, remoteTicketId, 'result.json');
    return fs.existsSync(fp) ? JSON.parse(fs.readFileSync(fp, 'utf8')) : null;
  });
  assert.equal(runnerRecord.kind, 'cancelled');

  // Idempotent: the attempt (and its lease/queue entry) is gone, so a
  // second cancel is a harmless no-op, not a crash or a double-write.
  const secondCancel = await laneRun(['cancel', id], { env });
  assert.equal(secondCancel.code, 1);
  assert.equal(resultOf(state, id).exit, 130, 'the already-published result must be untouched');
});

test('an attempt record survives a changed boot id and still reports ORPHANED-REMOTE', async () => {
  const { env, state, repoDir } = setup();
  const { id } = await startSleepingRemote(env, repoDir);
  try {
    const bootChangedEnv = { ...env, LANE_BROKER_BOOT_ID: 'a-brand-new-boot-id' };
    const status = JSON.parse((await laneRun(['status', '--json'], { env: bootChangedEnv })).stdout);
    const entry = status.remote.find((r) => r.id === id);
    assert.ok(entry, 'the attempt record must not have been reaped by the boot change alone');
    assert.equal(entry.orphaned, true);
  } finally {
    // The supervisor is genuinely still alive under its REAL boot id -- the
    // ordinary live-supervisor cancel path cleans everything up.
    await laneRun(['cancel', id], { env });
  }
});

// ---- Codex pre-merge BLOCKER #3: Ctrl-C on `lane wait` for a remote attempt ----

/** Like sleeperCmd, but also records WHERE it ran (via LANE_FAKE_RUNNER) to
 *  `whereMarker` -- so a test can prove a local re-run never overwrote it
 *  with 'local' after a remote-run cancellation. */
function sleeperMarkerCmd(startedMarker, whereMarker) {
  return [
    process.execPath,
    '-e',
    `const fs = require('fs');
fs.writeFileSync(${JSON.stringify(whereMarker)}, process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local');
fs.writeFileSync(${JSON.stringify(startedMarker)}, 'x');
setInterval(() => {}, 1000);`,
  ];
}

test('SIGINT to `lane wait` on a detached remote run cancels it: exit 130, runner ticket cancelled, no local re-run', async () => {
  const { env, runnerRoot, repoDir } = setup();
  const startedMarker = path.join(tmpDir('started'), 'started');
  const whereMarker = path.join(tmpDir('where'), 'where');
  const started = await laneRun(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...sleeperMarkerCmd(startedMarker, whereMarker)],
    { env, cwd: repoDir },
  );
  assert.equal(started.code, 0, `--detach should not fail: ${started.stderr}`);
  const id = started.stdout.trim();
  await waitFor(() => fs.existsSync(startedMarker), { timeoutMs: 15_000 });
  assert.equal(fs.readFileSync(whereMarker, 'utf8'), 'remote');

  const waitChild = laneSpawn(['wait', id], { env });
  let waitStderr = '';
  waitChild.stderr.on('data', (d) => {
    waitStderr += d;
  });
  await sleep(500); // let `lane wait` actually attach and poll at least once
  waitChild.kill('SIGINT');
  // Bounded: a broken cancel-forwarding path here must fail this test fast
  // and by name, never hang the whole suite waiting on a `lane wait` that
  // will genuinely never get a result (nothing else will ever cancel it).
  const exitCode = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      waitChild.kill('SIGKILL');
      reject(new Error(`lane wait never exited after SIGINT -- stderr so far: ${waitStderr}`));
    }, 20_000);
    waitChild.on('close', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
  assert.equal(exitCode, 130, `stderr: ${waitStderr}`);
  // Still 'remote': if a local re-run had happened it would have overwritten
  // this with 'local' (or, at minimum, restarted -- there is no other way
  // the file could be rewritten).
  assert.equal(fs.readFileSync(whereMarker, 'utf8'), 'remote', 'a local rerun must never have happened');

  const ticketsDir = path.join(runnerRoot, 'tickets');
  const [remoteTicketId] = fs.readdirSync(ticketsDir);
  const record = await waitFor(() => {
    const fp = path.join(ticketsDir, remoteTicketId, 'result.json');
    return fs.existsSync(fp) ? JSON.parse(fs.readFileSync(fp, 'utf8')) : null;
  });
  assert.equal(record.kind, 'cancelled');
});

// ---- Codex pre-merge SHOULD-FIX #4: queued-fallback cancellation/orphan ----

/** Force a remote-eligible ticket to fall back AND sit QUEUED (never
 *  leased): capacity 1, occupied by a long-running local blocker first. */
async function startQueuedFallback(env, home, state, repoDir) {
  writeGlobalConfig(home, {
    version: 1,
    capacity: 1,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    runners: [{ name: 'skybox', ssh: 'down', shell: 'sh -c', root: tmpDir('blocked-runner-root') }],
  });
  const blocker = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'sleep', '30'], { env, cwd: repoDir });
  assert.equal(blocker.code, 0, `blocker --detach should not fail: ${blocker.stderr}`);
  await waitFor(() => fs.readdirSync(path.join(state, 'leases')).filter((n) => n.endsWith('.json')).length > 0, { timeoutMs: 15_000 });

  const marker = path.join(tmpDir('marker'), 'where');
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], { env, cwd: repoDir });
  assert.equal(started.code, 0, `--detach should not fail: ${started.stderr}`);
  const id = started.stdout.trim();
  await waitFor(() => readAttempt(state, id)?.executor === 'local', { timeoutMs: 15_000 });
  assert.equal(fs.existsSync(path.join(state, 'leases', `${id}.json`)), false, 'must still be queued, not leased, for this test to mean anything');
  return { id, marker, blockerId: blocker.stdout.trim() };
}

test('cancel while a post-fallback ticket sits QUEUED (capacity full): result 130, attempt removed, wait returns 130 not a hang', async () => {
  const { env, home, state, repoDir } = setup({ ssh: 'down' });
  const { id, marker, blockerId } = await startQueuedFallback(env, home, state, repoDir);

  const cancelResult = await laneRun(['cancel', id], { env });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);

  const result = await waitFor(() => resultOf(state, id), { timeoutMs: 15_000 });
  assert.equal(result.exit, 130);
  assert.equal(result.cancelled, true);
  await waitFor(() => readAttempt(state, id) === null, { timeoutMs: 15_000 });
  assert.equal(fs.existsSync(marker), false, 'the child must never have run at all');

  const waited = await laneRun(['wait', id, '--timeout', '10s'], { env });
  assert.equal(waited.code, 130, `stderr: ${waited.stderr} -- must not hang or time out`);

  await laneRun(['cancel', blockerId], { env }); // cleanup: release the blocker's own child group
});

test('supervisor SIGKILLed after fallback (still queued): wait fails fast, naming it, instead of hanging', async () => {
  const { env, home, state, repoDir } = setup({ ssh: 'down' });
  const { id, blockerId } = await startQueuedFallback(env, home, state, repoDir);

  const attempt = readAttempt(state, id);
  assert.ok(attempt, 'expected an attempt record for the still-queued fallback ticket');
  assert.equal(attempt.executor, 'local');
  process.kill(attempt.supervisor.pid, 'SIGKILL');
  await sleep(300);

  const waited = await laneRun(['wait', id, '--timeout', '5s'], { env });
  assert.equal(waited.code, 1, `stderr: ${waited.stderr}`);
  assert.match(waited.stderr, /ORPHANED/);
  assert.match(waited.stderr, new RegExp(`lane cancel ${id}`));

  await laneRun(['cancel', blockerId], { env }); // cleanup: release the blocker's own child group
});

// ---- BRAIN-319 P1 (Codex re-review): admitted (leased) fallback, dead supervisor ----

/** Force a remote-eligible ticket to fall back AND get ADMITTED (leased) --
 *  plenty of default capacity, unlike startQueuedFallback above. The lease
 *  record is written by `tryStart` the instant the ticket is admitted, with
 *  `childPgid: null` (see scheduler.js) -- the supervisor only fills in the
 *  real pid in a SEPARATE, later `writeLease` call, once `spawn()` actually
 *  returns (supervisor.js). Waiting for "a lease exists" alone can return
 *  during that gap, especially under CPU contention where the supervisor's
 *  own spawn is delayed: a caller that snapshots `childPgid` at that point
 *  captures `null` forever, and `isGroupAlive(null)` -- `process.kill(-pgid,
 *  0)` with `pgid` coerced to `-0` -- is `process.kill(0, 0)`, which
 *  targets the CALLER's own process group and so is vacuously always true.
 *  A test polling `!isGroupAlive(lease.childPgid)` on a stale null pgid
 *  then hangs to its own timeout even though the real child died normally.
 *  Wait for the pid to actually land before returning the lease's id. */
async function startAdmittedFallback(env, state, repoDir, argv) {
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...argv], { env, cwd: repoDir });
  assert.equal(started.code, 0, `--detach should not fail: ${started.stderr}`);
  const id = started.stdout.trim();
  await waitFor(() => readAttempt(state, id)?.executor === 'local', { timeoutMs: 15_000 });
  await waitFor(() => Boolean(readLease(state, id)?.childPgid), { timeoutMs: 15_000 });
  return id;
}

test('SIGKILLing a fallback supervisor after its local child has been admitted (child keeps running): lane wait exits 1 fast, naming it', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down' });
  const id = await startAdmittedFallback(env, state, repoDir, ['sleep', '30']);

  const attempt = readAttempt(state, id);
  assert.ok(attempt, 'expected an attempt record for the admitted fallback ticket');
  assert.equal(attempt.executor, 'local');
  const lease = readLease(state, id);
  assert.ok(lease, 'the ticket must be leased (admitted), not just queued, for this test to mean anything');

  process.kill(attempt.supervisor.pid, 'SIGKILL');
  await sleep(300);

  const startedAt = Date.now();
  const waited = await laneRun(['wait', id, '--timeout', '10s'], { env });
  assert.ok(Date.now() - startedAt < 8000, 'must fail fast off the attempt-liveness check, not poll out to the timeout');
  assert.equal(waited.code, 1, `stderr: ${waited.stderr}`);
  assert.match(waited.stderr, /ORPHANED/);
  assert.match(waited.stderr, new RegExp(`lane cancel ${id}`));

  // Cleanup: `lane wait` deliberately never kills or releases the child (that
  // stays `lane cancel`'s job) -- the detached `sleep 30` is still alive
  // under its own process group. Reap it directly so nothing leaks past this
  // test.
  try {
    process.kill(-lease.childPgid, 'SIGKILL');
  } catch {
    // already gone
  }
  await waitFor(() => !isGroupAlive(lease.childPgid), { timeoutMs: 30000 });
});

test('a completed fallback run leaves no attempt record, and a later `lane cancel` on that id is a no-op that does not overwrite the result (no stale lock left behind)', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 7)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 7, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local', 'ssh is down, so this must have run as a local fallback');
  assert.equal(readAttempt(state, id), null, 'a completed fallback run must leave no attempt record');

  const originalResult = resultOf(state, id);
  assert.equal(originalResult.exit, 7);

  // BRAIN-319 P2: process.exit used to happen INSIDE the publishTerminal
  // writer, before withLock's own `finally` (mutex release) ever ran -- the
  // global lock must already be gone by the time the supervisor has exited.
  assert.equal(fs.existsSync(paths(state).lock), false, 'the global lock must not be left held after the supervisor exits');

  const startedAt = Date.now();
  const cancelResult = await laneRun(['cancel', id], { env });
  assert.ok(
    Date.now() - startedAt < 3000,
    'cancel on an unregistered id must return fast -- a still-held lock would force it through the ~15s stale-lock recovery path instead',
  );
  assert.equal(cancelResult.code, 1, 'nothing left to cancel');

  assert.deepEqual(resultOf(state, id), originalResult, 'the already-published result must be untouched');
});

// ---- older bug (pre-dates this branch's fixes): ORPHANED-lease cancel vs. attempt record ----

test('cancelling an ORPHANED admitted fallback (SIGKILLed supervisor, child still running) finalizes through publishTerminal: result 130, attempt gone, second cancel is a byte-identical no-op', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down' });
  const id = await startAdmittedFallback(env, state, repoDir, ['sleep', '30']);

  const attempt = readAttempt(state, id);
  assert.ok(attempt, 'expected an attempt record for the admitted fallback ticket');
  const lease = readLease(state, id);
  assert.ok(lease, 'the ticket must be leased (admitted), not just queued, for this test to mean anything');

  process.kill(attempt.supervisor.pid, 'SIGKILL');
  await sleep(300);

  const cancelResult = await laneRun(['cancel', id], { env });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);

  const result = resultOf(state, id);
  assert.equal(result.exit, 130, 'an attempt-tracked cancellation must report the same exit 130 every other path here does');
  assert.equal(result.cancelled, true);
  assert.equal(readAttempt(state, id), null, 'the attempt record must be gone once reconciled -- the older bug left it stale');
  await waitFor(() => !isGroupAlive(lease.childPgid), { timeoutMs: 30000 });

  const resultPath = path.join(paths(state).results, `${id}.json`);
  const bytesBefore = fs.readFileSync(resultPath);

  const secondCancel = await laneRun(['cancel', id], { env });
  assert.equal(secondCancel.code, 1, 'nothing left to cancel -- lease and attempt are both gone');

  const bytesAfter = fs.readFileSync(resultPath);
  assert.ok(bytesBefore.equals(bytesAfter), 'the already-published result must be byte-identical after a no-op second cancel');
});
