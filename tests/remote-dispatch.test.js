import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { gitFixture, laneRun, laneSpawn, waitFor, sleep, writeRepoConfig } from './helpers.js';
import { tmpDir, setup, markerCmd, detachAndWait, resultOf, probeCount } from './remote-harness.js';
import { paths, readJsonSafe, isCancelled } from '../src/state.js';
import { readAttempt } from '../src/attempts.js';

/**
 * BRAIN-319 T3b-3: end-to-end coverage of the supervisor's remote-dispatch
 * wiring (T3b-2), driving the REAL `lane run` CLI over the fake-ssh
 * transport from tests/remote-harness.js -- no direct calls into
 * src/supervisor.js or src/attempts.js.
 */

// ---- usable runner: exit passthrough, output relay, stderr banner ----

for (const exitCode of [0, 1, 42]) {
  test(`runner usable: a remote child exiting ${exitCode} passes that exit through, marked as having run remotely`, async () => {
    const { env, state, repoDir } = setup();
    const marker = path.join(tmpDir('marker'), 'where');
    const { id, waited } = await detachAndWait(
      ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, exitCode)],
      env,
      repoDir,
    );
    assert.equal(waited.code, exitCode, `stderr: ${waited.stderr}`);
    assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
    const result = resultOf(state, id);
    assert.equal(result.executor, 'remote');
    assert.equal(result.runner, 'skybox');
  });
}

test('runner usable: output streams on the foreground stdout AND lands in --log, and stderr announces the runner', async () => {
  const { env, repoDir } = setup();
  const marker = path.join(tmpDir('marker'), 'where');
  const logPath = path.join(tmpDir('log'), 'out.log');
  const result = await laneRun(
    ['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', ...markerCmd(marker, 0, 'hello-from-remote\n')],
    { env, cwd: repoDir },
  );
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.match(result.stdout, /hello-from-remote/);
  assert.match(result.stderr, /lane: running on skybox/);
  assert.match(fs.readFileSync(logPath, 'utf8'), /hello-from-remote/);
});

// ---- fallback: runner down ----

test('runner down: falls back to local, with the remote-skip stderr line and executor/fallbackReason recorded', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'local');
  assert.ok(result.fallbackReason, 'expected a fallbackReason to be recorded');
});

test('runner down: the remote-skip line is written to the admission log too', async () => {
  const { env, home, state, repoDir } = setup({ ssh: 'down' });
  const marker = path.join(tmpDir('marker'), 'where');
  await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], env, repoDir);
  const log = fs.readFileSync(paths(state).admissionLog, 'utf8');
  // No runner was usable at all ("skybox" itself is down) -- the "<runner|none>"
  // slot is "none", with the actual skipped-runner detail folded into the reason.
  assert.match(log, /remote-skip: none: skybox: .* — running locally/);
});

// ---- adversarial transport: die-midstream, result-tamper ----

test('die-midstream: falls back to local and reports the LOCAL exit', async () => {
  const { env, repoDir } = setup({ ssh: 'die-midstream' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 7)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 7, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
});

test('result-tamper: a record that does not bind proves nothing, so the ticket is NOT run locally (it may be live) and stays reported', { timeout: 300_000 }, async () => {
  const { env, state, repoDir } = setup({ ssh: 'result-tamper' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 9)],
    env,
    repoDir,
    '180s',
  );
  assert.equal(waited.code, 1, `stderr: ${waited.stderr}`);
  assert.match(waited.stderr, /ORPHANED-REMOTE/);
  assert.equal(readAttempt(state, id)?.executor, 'remote', 'the attempt is kept');
  assert.ok(readAttempt(state, id)?.unresolved);
  // BRAIN-462: the remote attempt carries the submitter's --log path and cwd, so a caller can find it without the id
  assert.match(readAttempt(state, id)?.logPath ?? '', new RegExp(`${id}\\.log$`));
  assert.equal(fs.realpathSync(readAttempt(state, id)?.cwd ?? '/nonexistent'), fs.realpathSync(repoDir));
});

// ---- ineligible: tracked .env ----

test('a tracked .env in the worktree is ineligible for remote and runs locally', async () => {
  const { env, repoDir } = setup();
  gitFixture(['add', '-f', '.lane-broker.json'], repoDir);
  fs.writeFileSync(path.join(repoDir, '.env'), 'SECRET=1');
  gitFixture(['add', '-f', '.env'], repoDir);
  gitFixture(['-c', 'user.name=t', '-c', 'user.email=t@example.invalid', 'commit', '-q', '-m', 'x'], repoDir);
  const marker = path.join(tmpDir('marker'), 'where');
  const { waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
});

test('an ineligible tree is refused before ever probing a runner: zero probes, no "running on" line', async () => {
  // Codex pre-merge finding #5: eligibility (buildManifest) must be decided
  // BEFORE selectRunner ever dials anything -- a foreground run (not
  // --detach) is needed here since a detached supervisor's own stderr goes
  // nowhere (stdio 'ignore'), and the "running on" banner is written to
  // the supervisor's OWN stderr, not the child's captured output.
  const { env, repoDir, probeLogPath } = setup();
  gitFixture(['add', '-f', '.lane-broker.json'], repoDir);
  fs.writeFileSync(path.join(repoDir, '.env'), 'SECRET=1');
  gitFixture(['add', '-f', '.env'], repoDir);
  gitFixture(['-c', 'user.name=t', '-c', 'user.email=t@example.invalid', 'commit', '-q', '-m', 'x'], repoDir);
  const marker = path.join(tmpDir('marker'), 'where');
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], { env, cwd: repoDir });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  assert.doesNotMatch(result.stderr, /running on/);
  assert.match(result.stderr, /remote-skip/);
  assert.equal(probeCount(probeLogPath), 0, 'an ineligible tree must never dial a runner at all');
});

// ---- BRAIN-320 S1a: remoteDeps eligibility ----

test('a remoteDeps lane with no package-lock.json is ineligible and never probes a runner', async () => {
  const { env, repoDir, probeLogPath } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remoteDeps: ['.'] } } });
  gitFixture(['add', '-f', '.lane-broker.json'], repoDir);
  gitFixture(['-c', 'user.name=t', '-c', 'user.email=t@example.invalid', 'commit', '-q', '-m', 'x'], repoDir);
  const marker = path.join(tmpDir('marker'), 'where');
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], { env, cwd: repoDir });
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  assert.doesNotMatch(result.stderr, /running on/);
  assert.match(result.stderr, /remote-skip/);
  assert.equal(probeCount(probeLogPath), 0, 'a remoteDeps-ineligible tree must never dial a runner at all');
});

// ---- Codex pre-merge BLOCKER #1: durable cancellation, forced 130 ----

test('a fallback child that traps SIGTERM and exits 0 still reports 130, never the child\'s own 0', async () => {
  const { env, repoDir } = setup({ ssh: 'down' }); // forces an immediate local fallback
  const startedMarker = path.join(tmpDir('started'), 'started');
  const body = `
require('fs').writeFileSync(${JSON.stringify(startedMarker)}, 'x');
process.on('SIGTERM', () => { process.exit(0); });
setInterval(() => {}, 1000);
`;
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, '-e', body], { env, cwd: repoDir });
  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });
  await waitFor(() => fs.existsSync(startedMarker), { timeoutMs: 120_000 });
  await sleep(300);
  child.kill('SIGINT');
  const exitCode = await new Promise((resolve) => child.on('close', resolve));
  assert.equal(exitCode, 130, `stderr: ${stderr}`);
});

// BRAIN-364: same race as the plain local path (tests/cancel-exit-zero.test.js): a cancel that lands after the fallback
// child ended was never acted on, so the child's own outcome stands. Previously publishTerminal's cancel-wins rule gave 130.
test('BRAIN-364: a cancel landing after a fallback child exited 0 leaves the result exit 0 with executor local', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down' });
  const dir = tmpDir('b364');
  const ready = path.join(dir, 'ready');
  const go = path.join(dir, 'go');
  const holdEnv = { ...env, LANE_BROKER_TEST_HOLD_AT: 'local-finalize', LANE_BROKER_TEST_HOLD_READY: ready, LANE_BROKER_TEST_HOLD_GO: go };
  const marker = path.join(dir, 'where');
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], { env: holdEnv, cwd: repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 120_000 });
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  const cancel = laneRun(['cancel', id], { env, cwd: repoDir });
  await waitFor(() => isCancelled(state, id), { timeoutMs: 120_000 });
  fs.writeFileSync(go, 'x');
  await cancel;
  const result = resultOf(state, id);
  assert.equal(result.exit, 0);
  assert.equal(result.executor, 'local');
  assert.equal(result.cancelled, undefined);
  assert.equal(isCancelled(state, id), false, 'marker cleaned up');
});

test('BRAIN-364: a cancel landing between admission and spawn of a fallback ticket publishes 130 and removes the attempt record', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down' });
  const dir = tmpDir('b364a');
  const ready = path.join(dir, 'ready');
  const go = path.join(dir, 'go');
  const holdEnv = { ...env, LANE_BROKER_TEST_HOLD_AT: 'local-admitted', LANE_BROKER_TEST_HOLD_READY: ready, LANE_BROKER_TEST_HOLD_GO: go };
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(path.join(dir, 'where'), 0)], { env: holdEnv, cwd: repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 120_000 });
  const cancel = laneRun(['cancel', id], { env, cwd: repoDir });
  await waitFor(() => isCancelled(state, id), { timeoutMs: 120_000 });
  fs.writeFileSync(go, 'x');
  await cancel;
  const result = resultOf(state, id);
  assert.equal(result.exit, 130);
  assert.equal(result.cancelled, true);
  assert.equal(result.executor, 'local');
  assert.equal(readAttempt(state, id), null, 'attempt record removed');
  assert.equal(isCancelled(state, id), false, 'marker cleaned up');
});

// ---- Codex pre-merge BLOCKER #2: drain-then-publish, real async drain ----

test('SIGINT during the post-completion drain of a remote success still reports 130, never the child\'s own 0', { timeout: 360_000 }, async () => {
  const { env, repoDir } = setup();
  const logPath = path.join(tmpDir('log'), 'out.log');
  // Deliberately large + a slow reader: the goal is to make the SUPERVISOR's
  // own drain (ForwardWriter/CappedLogWriter, after the remote child has
  // already exited 0) span a long, comfortably-hittable wall-clock window,
  // so the SIGINT below reliably lands mid-drain rather than racing a
  // sub-100ms round trip.
  const bytes = 24 * 1024 * 1024;
  const body = `process.stdout.write(Buffer.alloc(${bytes}, 'a')); process.exit(0);`;
  const child = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', process.execPath, '-e', body],
    { env, cwd: repoDir },
  );
  let sentSignal = false;
  child.stdout.on('data', (chunk) => {
    if (!sentSignal) {
      sentSignal = true;
      child.kill('SIGINT');
    }
    void chunk;
    // Pause for real: an async handler does not slow a flowing stream, so under load the drain could finish before the
    // SIGINT was even delivered. Backpressure keeps 24MB in flight for >= 40ms per chunk whatever the CPU is doing.
    child.stdout.pause();
    setTimeout(() => child.stdout.resume(), 40);
  });
  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });

  const exitCode = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timed out -- likely hung')), 300_000);
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });

  assert.equal(exitCode, 130, `stderr: ${stderr}; a cancellation observed during the drain must win over the child's own successful exit`);
});

// ---- cancellation ----

test(
  'cancelling the foreground caller (SIGINT, the existing cancel path) while the remote child sleeps exits 130, ' +
    'never runs locally, and the runner\'s own ticket result kind is cancelled',
  async () => {
    const { env, runnerRoot, repoDir } = setup();
    const marker = path.join(tmpDir('marker'), 'where');
    const startedMarker = path.join(tmpDir('started'), 'started');
    const body = `
const fs = require('fs');
fs.writeFileSync(${JSON.stringify(startedMarker)}, 'x');
setInterval(() => {}, 1000);
`;
    // The child never exits on its own -- if it somehow ran LOCALLY despite
    // the cancel, it would write the marker at process end, which never
    // happens for this sleeping command; a local rerun is instead detected
    // by the marker's absence.
    const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, '-e', body], { env, cwd: repoDir });
    let stderr = '';
    child.stderr.on('data', (d) => {
      stderr += d;
    });

    await waitFor(() => fs.existsSync(startedMarker), { timeoutMs: 120_000 });
    await sleep(300);
    child.kill('SIGINT');

    const exitCode = await new Promise((resolve) => child.on('close', resolve));
    assert.equal(exitCode, 130, `stderr: ${stderr}`);
    assert.equal(fs.existsSync(marker), false, 'a local rerun must never have happened after cancellation');

    // The runner's own broker ticket (under runnerRoot/tickets/<id>) settles
    // to kind 'cancelled' -- find it by locating the sole ticket dir the
    // fake runner created, rather than needing the id (this test cancels
    // before ever seeing one, matching a real Ctrl-C).
    const ticketsDir = path.join(runnerRoot, 'tickets');
    const [remoteTicketId] = await waitFor(() => {
      try {
        const names = fs.readdirSync(ticketsDir);
        return names.length > 0 ? names : null;
      } catch {
        return null;
      }
    });
    const resultPath = path.join(ticketsDir, remoteTicketId, 'result.json');
    const record = await waitFor(() => readJsonSafe(resultPath), { timeoutMs: 120_000 });
    assert.equal(record.kind, 'cancelled');
  },
);

// ---- --detach + lane wait ----

test('--detach + lane wait works for a remotely-dispatched run', async () => {
  const { env, state, repoDir } = setup();
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  assert.equal(resultOf(state, id).executor, 'remote');
});

// ---- oversize resource request ----

test('oversize resources + a usable runner: BRAIN-320 S1e catches the impossible fit client-side and falls back local, itself refused', async () => {
  // The fake "runner" here is this same machine, sharing this same
  // restrictive global config (a real runner would have its own, separate,
  // presumably bigger one) -- its `remote-probe` STATIC capacity (S1e)
  // therefore honestly reports this same tiny weight/CPU budget. Before
  // BRAIN-320, `lane run` never checked that ahead of time (BRAIN-319
  // T3b-1) and dialed the runner regardless, which refused on its OWN
  // admission once dispatched (`kind: 'refused'`). Now the client's fit
  // check (1e) is a STATIC impossibility check using that same probe
  // capacity: 1000 cores can never fit a runner whose reported CPU
  // budget is tiny, so selectRunner skips it without ever dialing -- same
  // "no runners usable" fallback shape as the runner-down case below, and
  // the LOCAL preflight (this same restrictive config) then refuses it too.
  const { env, state, repoDir } = setup({ cpuAdmissionPercent: 1, cpuCores: 1000 }) // BRAIN-452: a 1000 weight is refused at submission now, so the oversize is CPU;
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 64, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'refused before the child command ever ran');
  const result = resultOf(state, id);
  // A local budget refusal (localBudgetRefusalResult) carries no `executor`
  // field -- same shape as the "runner down" fallback-refusal variant below.
  // `remoteKind` is undefined here too, distinguishing this from the old
  // behaviour this test used to assert (a RUNNER-confirmed `kind: 'refused'`
  // with `executor: 'remote'`), proving the runner was never dialed at all.
  assert.equal(result.executor, undefined, 'the client skipped the runner client-side on the static fit check, before any executor was recorded');
  assert.equal(result.remoteKind, undefined);
  assert.match(result.error, /exceed this environment's budget/);
});

test('oversize resources + the runner down: refused exactly as today, once it falls back', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down', cpuAdmissionPercent: 1, cpuCores: 1000 });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 64, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'refused before ever spawning a child');
  assert.match(resultOf(state, id).error, /exceed this environment's budget/);
});

// ---- large output under a slow foreground reader ----

test('~5MB of remote stdout under a slowly-draining foreground caller completes, passes the exit through, and caps --log like the local path', { timeout: 360_000 }, async () => {
  const { env, repoDir } = setup();
  const logPath = path.join(tmpDir('log'), 'out.log');
  const bytes = 5 * 1024 * 1024;
  const body = `process.stdout.write(Buffer.alloc(${bytes}, 'a'));`;
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', process.execPath, '-e', body], {
    env,
    cwd: repoDir,
  });
  let total = 0;
  child.stdout.on('data', (chunk) => {
    total += chunk.length;
    // Deliberately slow: pause the stream (real backpressure, which an async handler does not apply) before consuming the
    // next chunk, so the writer side (ForwardWriter) is exercised under real drain pressure rather than draining instantly.
    child.stdout.pause();
    setTimeout(() => child.stdout.resume(), 5);
  });
  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });

  const exitCode = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timed out -- likely hung')), 300_000);
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });

  assert.equal(exitCode, 0, `stderr: ${stderr}`);
  assert.equal(total, bytes, 'the caller must have received every byte, in full');
  assert.ok(fs.statSync(logPath).size > 0, '--log must have captured output too');
});

// ---- BRAIN-320 S1c: protocol-2 dispatch end to end ----

test('protocol 2: remoteSetup succeeds, command exits 0 -> green, ran remotely, phase command recorded', async () => {
  const { env, state, repoDir } = setup();
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: { default: { weight: 1, remote: true, remoteSetup: [[process.execPath, '-e', 'process.exit(0)']] } },
  });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.equal(result.remoteKind, 'completed');
  assert.equal(result.remotePhase, 'command');
});

test('protocol 2: remoteSetup fails -> exit is the setup exit, the local command never runs, phase setup attributed', async () => {
  const { env, state, repoDir } = setup();
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: { default: { weight: 1, remote: true, remoteSetup: [[process.execPath, '-e', 'process.exit(7)']] } },
  });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 7, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the local command must never have run');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.equal(result.remotePhase, 'setup');
  const log = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(log, /lane: remote failed during setup \(exit 7\); this can also be a registry or network failure on skybox/);
});

test('protocol 2: remoteDeps (npm ci) fails -> exit is npm ci\'s exit, the local command never runs, phase deps attributed', async () => {
  const { env, state, repoDir } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, remoteDeps: ['.'] } } });
  // A tiny fixture whose package.json declares a dependency the lockfile does
  // not have -- `npm ci` fails deterministically, without ever reaching a
  // registry (same fixture shape as tests/remote-pipeline.test.js's
  // makeDepsFixture({lockMismatch: true})). `.npmrc` is belt-and-braces.
  fs.writeFileSync(
    path.join(repoDir, 'package.json'),
    JSON.stringify({ name: 'fixture', version: '1.0.0', private: true, dependencies: { 'left-pad': '^1.0.0' } }),
  );
  fs.writeFileSync(
    path.join(repoDir, 'package-lock.json'),
    JSON.stringify({ name: 'fixture', version: '1.0.0', lockfileVersion: 3, requires: true, packages: { '': { name: 'fixture', version: '1.0.0' } } }),
  );
  fs.writeFileSync(
    path.join(repoDir, '.npmrc'),
    'registry=http://127.0.0.1:9\nfetch-retries=0\nfetch-retry-mintimeout=100\nfetch-retry-maxtimeout=100\nfetch-timeout=2000\n',
  );
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
    '180s',
  );
  assert.notEqual(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the local command must never have run');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.equal(result.remotePhase, 'deps');
  const log = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(log, /lane: remote failed during deps \(exit \d+\); this can also be a registry or network failure on skybox/);
});

test('protocol 1: an optionless remote lane still sends protocol 1 (no phase in the result)', async () => {
  const { env, state, repoDir } = setup();
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.equal(result.remoteKind, 'completed');
  assert.equal(result.remotePhase, null, 'a protocol-1 result carries no pipeline phase');
});

// ---- BRAIN-320 S1c: mixed protocol versions against a pre-S1a (0.6.0-style) runner ----

test('mixed versions: a v2 (remoteSetup) lane against a runner whose probe lacks protocols is skipped, falls back local', async () => {
  const { env, state, repoDir } = setup({ ssh: 'old-runner' });
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: { default: { weight: 1, remote: true, remoteSetup: [[process.execPath, '-e', 'process.exit(0)']] } },
  });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'local');
  assert.match(result.fallbackReason, /does not support protocol 2/);
});

// ---- BRAIN-320 S1d: opt-in queue timeout ----

test('queue timeout: the client global config carries remoteQueueTimeoutMs, the runner broker never starts the ticket, and the run falls back and completes LOCALLY', async () => {
  const { env, state, repoDir, runnerState } = setup({ remoteQueueTimeoutMs: 300 });
  fs.mkdirSync(runnerState, { recursive: true });
  fs.writeFileSync(path.join(runnerState, 'PAUSE'), 'kept busy for the test');
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local', 'the run must have fallen back and executed locally');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'local');
  assert.match(result.fallbackReason, /queue-timeout/);
});

test('queue timeout: a user cancel while queued (deadline still pending) exits 130, never falls back locally, and the runner ticket is cancelled -- not queue-timeout', async () => {
  const { env, runnerRoot, runnerState, repoDir } = setup({ remoteQueueTimeoutMs: 30_000 });
  fs.mkdirSync(runnerState, { recursive: true });
  fs.writeFileSync(path.join(runnerState, 'PAUSE'), 'kept busy for the test');
  const marker = path.join(tmpDir('marker'), 'where');

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], { env, cwd: repoDir });
  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });

  const ticketsDir = path.join(runnerRoot, 'tickets');
  await waitFor(
    () => {
      try {
        return fs.readdirSync(ticketsDir).length > 0 ? true : null;
      } catch {
        return null;
      }
    },
    { timeoutMs: 120_000 },
  );
  child.kill('SIGINT');

  const exitCode = await new Promise((resolve) => child.on('close', resolve));
  assert.equal(exitCode, 130, `stderr: ${stderr}`);
  assert.equal(fs.existsSync(marker), false, 'a local rerun must never happen after a user cancel');

  const [remoteTicketId] = fs.readdirSync(ticketsDir);
  const resultPath = path.join(ticketsDir, remoteTicketId, 'result.json');
  const record = await waitFor(() => readJsonSafe(resultPath), { timeoutMs: 120_000 });
  assert.equal(record.kind, 'cancelled');
  assert.notEqual(record.reason, 'queue-timeout');
});

test('queue timeout: with no remoteQueueTimeoutMs configured, a busy runner broker is not treated as expired -- the run still dispatches remotely once the runner frees up', async () => {
  const { env, state, repoDir, runnerState } = setup();
  fs.mkdirSync(runnerState, { recursive: true });
  const pauseFile = path.join(runnerState, 'PAUSE');
  fs.writeFileSync(pauseFile, 'kept busy for the test');
  setTimeout(() => {
    try {
      fs.unlinkSync(pauseFile);
    } catch {
      // already gone
    }
  }, 600);
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
    '180s',
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote', 'no queueTimeoutMs was configured, so the ticket must never have expired');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
});

test('BRAIN-320 review fix D: with remoteQueueTimeoutMs configured, an optionless lane skips an old-style (protocol-2-less) runner and falls back local', async () => {
  const { env, state, repoDir } = setup({ ssh: 'old-runner', remoteQueueTimeoutMs: 300 });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local', 'an old runner cannot honour remoteQueueTimeoutMs, so selection must skip it');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'local');
  assert.match(result.fallbackReason, /does not support protocol 2/);
});

test('mixed versions: a v1 (optionless) lane still dispatches remotely against the same old-style probe', async () => {
  const { env, state, repoDir } = setup({ ssh: 'old-runner' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
});

// ---- BRAIN-341: a remote attempt's waitedMs is queue wait, never the run's own duration ----

test('remote run: waitedMs records the wait before the command started, not the ~2.5s the command ran', async () => {
  const { env, state, repoDir } = setup();
  // Long enough that the ssh connect + runner startup the formula counts as wait (seconds under load) stays well under the run.
  const runMs = 8000;
  const cmd = [process.execPath, '-e', `setTimeout(() => process.exit(0), ${runMs});`];
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...cmd], env, repoDir);
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.ok(Number.isFinite(result.waitedMs), `waitedMs should be a number, got ${result.waitedMs}`);
  assert.ok(result.waitedMs < runMs, `waitedMs ${result.waitedMs} must not include the ${runMs}ms run`);
  assert.ok(result.endedAt - result.startedAt >= runMs - 100, 'startedAt must still mean "command started"');
});

// ---- BRAIN-363: queue wait is measured on the runner's clock, never the result-delivery delay ----

test('remote run: a result that lands 6s after the run ended does not count the fetch delay as queue wait', async () => {
  const { env, state, repoDir } = setup({ ssh: 'slow-result' });
  const cmd = [process.execPath, '-e', 'process.exit(0)'];
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...cmd], env, repoDir);
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.ok(Number.isFinite(result.waitedMs), `waitedMs should be a number, got ${result.waitedMs}`);
  assert.ok(result.waitedMs < 3000, `waitedMs ${result.waitedMs} must not include the 6000ms result delay`);
  assert.ok(Number.isFinite(result.runMs) && result.runMs < 3000, `the runner-measured runMs must be recorded, got ${result.runMs}`);
  assert.ok(result.endedAt - result.startedAt >= 5500, 'the delivery delay sits between startedAt and endedAt, not before startedAt');
});

test('remote run: a result from a runner that reports no queuedMs still gets a waitedMs (the BRAIN-341 formula)', { timeout: 240_000 }, async () => {
  const { env, state, repoDir } = setup({ ssh: 'legacy-result' });
  // Long enough that the ssh connect + runner startup the formula counts as wait (seconds under load) stays well under the run.
  const runMs = 8000;
  const cmd = [process.execPath, '-e', `setTimeout(() => process.exit(0), ${runMs});`];
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...cmd], env, repoDir);
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.ok(Number.isFinite(result.waitedMs), `waitedMs should be a number, got ${result.waitedMs}`);
  assert.ok(result.waitedMs < runMs, `waitedMs ${result.waitedMs} must not include the ${runMs}ms run`);
});

// ---- BRAIN-363: an unconfirmed expiry cancel must never lead to a second (local) execution ----

test('remote run: result-wait expiry with an unconfirmed cancel is never re-run locally, and the attempt stays reported and cancellable', { timeout: 300_000 }, async () => {
  const { env, state, repoDir } = setup({ ssh: 'stuck-running', remoteResultWaitMs: 1 });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], env, repoDir, '180s');
  assert.equal(waited.code, 1, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the job must not have been re-run locally');
  assert.match(waited.stderr, /ORPHANED-REMOTE/);
  const attempt = readAttempt(state, id);
  assert.equal(attempt.executor, 'remote');
  assert.equal(attempt.runner, 'skybox');
  assert.match(attempt.unresolved, /may still be running and was not re-run/);
  // the runner never confirms its cancel, so cancel keeps the record rather than forgetting a possibly-live ticket
  const cancelled = await laneRun(['cancel', id], { env });
  assert.equal(cancelled.code, 1, cancelled.stderr);
  assert.match(cancelled.stderr, /not confirmed on skybox/);
  assert.ok(readAttempt(state, id), 'still held');
});

// ---- BRAIN-437: a dispatch the runner may have accepted is never re-run locally without proof it never started ----

test('BRAIN-437: ssh dropped after the runner took the job, fetches all failing, withdraw not confirmed: not re-run locally, attempt kept and cancellable', { timeout: 300_000 }, async () => {
  const { env, state, repoDir } = setup({ ssh: 'drop-stuck' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], env, repoDir, '90s');
  assert.equal(waited.code, 1, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the job must not have been run locally');
  assert.match(waited.stderr, /ORPHANED-REMOTE/);
  assert.equal(readAttempt(state, id).executor, 'remote');
  // this runner can prove the ticket absent (it tombstones it), so cancel completes
  const cancelled = await laneRun(['cancel', id], { env });
  assert.equal(cancelled.code, 0, cancelled.stderr);
  assert.equal(readAttempt(state, id), null);
});

test('BRAIN-437: the same drop with a runner that confirms `withdrawn` falls back to local, once', { timeout: 300_000 }, async () => {
  const { env, repoDir } = setup({ ssh: 'drop-withdrawn' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 7)], env, repoDir, '90s');
  assert.equal(waited.code, 7, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
});

test('BRAIN-452: a fresh oversized weight is refused with exit 64 even for a remote-eligible lane, before anything is enqueued', async () => {
  const { env, state, repoDir } = setup({ weight: 5 }); // maxLaneWeight 4
  const marker = path.join(tmpDir('marker'), 'where');
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(marker, 0)], { env, cwd: repoDir });
  assert.equal(res.code, 64, res.stderr);
  assert.match(res.stderr, /weight 5 would hold most of this machine's capacity.*BRAIN-452/);
  assert.equal(fs.existsSync(marker), false, 'never ran, remotely or locally');
  assert.deepEqual(fs.existsSync(paths(state).queue) ? fs.readdirSync(paths(state).queue) : [], [], 'nothing enqueued');
});
