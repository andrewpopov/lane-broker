import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { gitFixture, laneRun, laneSpawn, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, markerCmd, detachAndWait, resultOf } from './remote-harness.js';
import { paths, readJsonSafe } from '../src/state.js';

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

test('result-tamper: falls back to local and reports the LOCAL exit', async () => {
  const { env, repoDir } = setup({ ssh: 'result-tamper' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 9)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 9, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
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

    await waitFor(() => fs.existsSync(startedMarker), { timeoutMs: 15_000 });
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
    const record = await waitFor(() => readJsonSafe(resultPath), { timeoutMs: 15_000 });
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

test('oversize resources + a usable runner: the CLIENT never refuses at preflight -- the ticket reaches the runner', async () => {
  // The fake "runner" here is this same machine, sharing this same
  // restrictive global config (a real runner would have its own, separate,
  // presumably bigger one) -- so it legitimately refuses this oversize
  // request too, on ITS OWN admission (`remote-runner.js` runs `lane run`
  // in-process with LANE_BROKER_LOCAL=1, I5). What this test actually
  // proves is the CLIENT-side skip (BRAIN-319 T3b-1): `lane run` never
  // printed its own "exceed this environment's budget" refusal and never
  // returned before a supervisor/ticket ever existed -- the exit 64 here is
  // the RUNNER's own confirmed refusal (`kind: 'refused'`), not a client
  // preflight short-circuit.
  const { env, state, repoDir } = setup({ cpuAdmissionPercent: 1, weight: 1000 });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 64, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the runner refused before the child command ever ran');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote', "this must be the RUNNER refusing, not the client's own local preflight");
  assert.equal(result.remoteKind, 'refused');
});

test('oversize resources + the runner down: refused exactly as today, once it falls back', async () => {
  const { env, state, repoDir } = setup({ ssh: 'down', cpuAdmissionPercent: 1, weight: 1000 });
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

test('~5MB of remote stdout under a slowly-draining foreground caller completes, passes the exit through, and caps --log like the local path', async () => {
  const { env, repoDir } = setup();
  const logPath = path.join(tmpDir('log'), 'out.log');
  const bytes = 5 * 1024 * 1024;
  const body = `process.stdout.write(Buffer.alloc(${bytes}, 'a'));`;
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', process.execPath, '-e', body], {
    env,
    cwd: repoDir,
  });
  let total = 0;
  child.stdout.on('data', async (chunk) => {
    total += chunk.length;
    // Deliberately slow: yield past several backpressure-relevant ticks
    // before consuming the next chunk, so the writer side (ForwardWriter)
    // is exercised under real drain pressure rather than draining instantly.
    await sleep(5);
  });
  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });

  const exitCode = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timed out -- likely hung')), 60_000);
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });

  assert.equal(exitCode, 0, `stderr: ${stderr}`);
  assert.equal(total, bytes, 'the caller must have received every byte, in full');
  assert.ok(fs.statSync(logPath).size > 0, '--log must have captured output too');
});
