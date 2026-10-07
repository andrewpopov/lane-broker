import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, waitFor } from './helpers.js';
import { paths } from '../src/state.js';

function setup() {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  return { base, home, state, repoDir, env };
}

test('foreground `lane run` streams child stdout/stderr stream-separated to the caller, and still logs both', async () => {
  const { base, repoDir, env } = setup();
  const logPath = path.join(base, 'streamed.log');
  const outMark = 'OUT-MARK-9f31';
  const errMark = 'ERR-MARK-9f31';

  const result = await laneRun(
    ['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', 'sh', '-c', `echo ${outMark}; echo ${errMark} >&2; exit 3`],
    { env, cwd: repoDir },
  );

  assert.equal(result.code, 3, `stderr: ${result.stderr}`);
  assert.match(result.stdout, new RegExp(outMark), 'caller stdout must contain the child\'s stdout line');
  assert.doesNotMatch(result.stdout, new RegExp(errMark), 'caller stdout must not contain the child\'s stderr line');
  assert.match(result.stderr, new RegExp(errMark), 'caller stderr must contain the child\'s stderr line');
  assert.doesNotMatch(result.stderr, new RegExp(outMark), 'caller stderr must not contain the child\'s stdout line');

  const logged = fs.readFileSync(logPath, 'utf8');
  assert.match(logged, new RegExp(outMark), 'log file must still contain the stdout line');
  assert.match(logged, new RegExp(errMark), 'log file must still contain the stderr line');
});

test('a large stdout arrives complete and in order at the caller (no lost tail)', async () => {
  const { base, repoDir, env } = setup();
  const logPath = path.join(base, 'large.log');
  const N = 20000;
  const script = `for (let i = 0; i < ${N}; i++) console.log('line-' + i);`;

  const result = await laneRun(
    ['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', process.execPath, '-e', script],
    { env, cwd: repoDir },
  );

  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  const lines = result.stdout.split('\n').filter((l) => l.length > 0);
  assert.equal(lines.length, N, `expected ${N} lines at the caller, got ${lines.length}`);
  assert.equal(lines[0], 'line-0');
  assert.equal(lines[N - 1], `line-${N - 1}`);
  for (let i = 0; i < N; i++) {
    assert.equal(lines[i], `line-${i}`, `line ${i} out of order or corrupted`);
  }
});

test('--detach still prints exactly the id on stdout; none of the child\'s output leaks to the caller', async () => {
  const { base, repoDir, env } = setup();
  const logPath = path.join(base, 'detached-stdio.log');
  const outMark = 'DETACH-OUT-MARK';

  const result = await laneRun(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--log', logPath, '--', 'sh', '-c', `echo ${outMark}`],
    { env, cwd: repoDir },
  );

  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.match(result.stdout, /^[0-9a-f-]{36}\n$/, 'stdout must be exactly the id, nothing from the child');
  assert.doesNotMatch(result.stdout, new RegExp(outMark), 'the child\'s output must not leak into a --detach caller\'s stdout');
});

test('a caller whose own stdout closes early (e.g. piped into `head`) does not crash `lane run`', async () => {
  const { base, repoDir, env } = setup();
  const logPath = path.join(base, 'epipe-caller.log');
  const N = 20000;
  const script = `for (let i = 0; i < ${N}; i++) console.log('line-' + i);`;

  const child = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', process.execPath, '-e', script],
    { env, cwd: repoDir },
  );

  let stderr = '';
  child.stderr.on('data', (d) => {
    stderr += d;
  });

  // Simulate the downstream of a pipe (`lane run ... | head -1`) closing
  // early: destroy the read end of the lane-run process's own stdout right
  // after its first chunk, so the process's later writes hit EPIPE.
  await new Promise((resolve) => {
    child.stdout.once('data', () => {
      child.stdout.destroy();
      resolve();
    });
  });

  const exitInfo = await new Promise((resolve) => {
    child.on('exit', (code, signal) => resolve({ code, signal }));
  });

  assert.equal(exitInfo.code, 0, `lane run should still exit with the child's real exit code, got ${JSON.stringify(exitInfo)}; stderr: ${stderr}`);
  assert.doesNotMatch(
    stderr,
    /Uncaught|EPIPE|throw err/i,
    `lane run must not crash with an uncaught exception when its own stdout is closed early; stderr: ${stderr}`,
  );
});

test('a caller killed with SIGKILL does not stop the lane; it still finishes and logs its post-kill output', async () => {
  const { base, home, state, repoDir, env } = setup();
  const logPath = path.join(base, 'killed-caller.log');
  const postKillMark = 'POST-KILL-MARK-7c2';

  const child = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'default', '--log', logPath, '--', 'sh', '-c', `sleep 1; echo ${postKillMark}; exit 5`],
    { env, cwd: repoDir },
  );

  const leaseId = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    return names[0] ? names[0].replace(/\.json$/, '') : null;
  });

  child.kill('SIGKILL');
  if (child.exitCode === null && child.signalCode === null) {
    await new Promise((resolve) => child.on('exit', resolve));
  }

  const resultPath = path.join(paths(state).results, `${leaseId}.json`);
  await waitFor(() => fs.existsSync(resultPath), { timeoutMs: 5000 });
  const result = JSON.parse(fs.readFileSync(resultPath, 'utf8'));
  assert.equal(result.exit, 5, `lane must still finish with the child's real exit code; result: ${JSON.stringify(result)}`);

  const logged = fs.readFileSync(logPath, 'utf8');
  assert.match(logged, new RegExp(postKillMark), 'the post-kill output must still reach the log');
  assert.doesNotMatch(logged, /lane-broker supervisor error/, 'the supervisor must not crash when the caller disappears');
});

test('a queued foreground caller\'s stderr carries only its own child\'s output, never broker telemetry (BRAIN-308 follow-up)', async () => {
  const { base, state, repoDir, env } = setup();
  const holderLog = path.join(base, 'holder.log');
  const waiterLog = path.join(base, 'waiter.log');
  const errMark = 'WAITER-ERR-MARK-b41';

  // First foreground run holds the `same/default` key for long enough (several
  // sampleMs=100 polls) that the second run must actually wait in the queue
  // rather than start immediately.
  const holder = laneRun(
    ['run', '--repo', 'same', '--lane', 'default', '--log', holderLog, '--', 'sh', '-c', 'sleep 1.5'],
    { env, cwd: repoDir },
  );

  await waitFor(() => {
    try {
      return fs.readdirSync(paths(state).leases).some((n) => n.endsWith('.json'));
    } catch {
      return false;
    }
  });

  const waiter = laneRun(
    ['run', '--repo', 'same', '--lane', 'default', '--log', waiterLog, '--', 'sh', '-c', `echo ${errMark} >&2`],
    { env, cwd: repoDir },
  );

  const [holderResult, waiterResult] = await Promise.all([holder, waiter]);

  assert.equal(holderResult.code, 0, `holder stderr: ${holderResult.stderr}`);
  assert.equal(waiterResult.code, 0, `waiter stderr: ${waiterResult.stderr}`);
  assert.match(waiterResult.stderr, new RegExp(errMark), 'caller stderr must contain its own child\'s marker');
  assert.doesNotMatch(
    waiterResult.stderr,
    /^\S+Z lane-broker-admission/m,
    `queued caller's stderr must not carry broker admission telemetry: ${waiterResult.stderr}`,
  );
  assert.doesNotMatch(
    waiterResult.stderr,
    /^lane-broker-head-block/m,
    `queued caller's stderr must not carry broker head-block telemetry: ${waiterResult.stderr}`,
  );
  assert.doesNotMatch(
    waiterResult.stderr,
    /^lane-broker-capacity/m,
    `queued caller's stderr must not carry broker capacity telemetry: ${waiterResult.stderr}`,
  );

  const admissionLogged = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(admissionLogged, /^\S+Z lane-broker-admission/m, 'the admission-decisions log file must still receive telemetry');
});
