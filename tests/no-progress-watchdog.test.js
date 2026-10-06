import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { setup, resultOf } from './remote-harness.js';
import { paths } from '../src/state.js';
import { NoProgressWatchdog, formatWindow, noProgressMessage } from '../src/no-progress.js';
import { loadGlobalConfig, loadRepoConfig, reloadGlobalConfig } from '../src/config.js';

/** BRAIN-431: a run with no output AND no CPU for the window is killed (exit 124, reason no-progress). */

const WINDOW_MS = 2000;
const node = (body) => [process.execPath, '-e', body];
const PRINT_THEN_SLEEP = node(`process.stdout.write('started\\n'); setInterval(() => {}, 1000);`);
const BUSY_SILENT = node(`const end = Date.now() + 5000; while (Date.now() < end) {} process.stdout.write('done\\n');`);
const CHATTY = node(`let n = 0; const t = setInterval(() => { process.stdout.write('tick ' + n++ + '\\n'); if (n === 14) { clearInterval(t); } }, 300);`);

function localSetup() {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, noProgressTimeoutMs: WINDOW_MS } } });
  return { env, state, repoDir };
}

const historyOf = (state) =>
  fs
    .readFileSync(paths(state).history, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l));

const run = (setupResult, argv) => laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...argv], { env: setupResult.env, cwd: setupResult.repoDir });

test('local: a command that prints once then sleeps silently is killed with reason=no-progress and exit 124', async () => {
  const s = localSetup();
  const started = Date.now();
  const result = await run(s, PRINT_THEN_SLEEP);
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  assert.ok(Date.now() - started < 30_000, 'killed promptly, not left to run');
  assert.match(result.stderr, /lane run: no progress for 2s \(no output, no CPU\) — killed/);
  const [row] = historyOf(s.state);
  assert.equal(row.exit, 124);
  assert.equal(row.reason, 'no-progress');
  assert.equal(row.executor, 'local');
  assert.match(fs.readFileSync(paths(s.state).admissionLog, 'utf8'), /lane-broker-reap id=\S+ descendants-reaped=\d+ reason=no-progress/);
});

test('local: a silent but CPU-busy command is NOT killed', async () => {
  const s = localSetup();
  const result = await run(s, BUSY_SILENT);
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(historyOf(s.state)[0].reason, undefined);
});

test('local: a command that prints periodically is NOT killed', async () => {
  const s = localSetup();
  const result = await run(s, CHATTY);
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(historyOf(s.state)[0].reason, undefined);
});

test('local: noProgressTimeoutMs 0 disables the watchdog for the lane', async () => {
  const s = localSetup();
  writeRepoConfig(s.repoDir, { version: 1, lanes: { default: { weight: 1, noProgressTimeoutMs: 0 } } });
  const result = await run(s, node(`process.stdout.write('x'); setTimeout(() => {}, 3500);`));
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
});

test('remote: the runner kills a silent command; the submitter gets the clear message, exit 124 and the history reason', async () => {
  const { env, state, repoDir, runnerState } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, noProgressTimeoutMs: WINDOW_MS } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...PRINT_THEN_SLEEP], { env, cwd: repoDir });
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  assert.match(result.stderr + result.stdout, /lane run: no progress for 2s \(no output, no CPU\) — killed/);
  const [row] = historyOf(state);
  assert.equal(row.executor, 'remote');
  assert.equal(row.exit, 124);
  assert.equal(row.reason, 'no-progress');
  assert.match(fs.readFileSync(paths(runnerState).admissionLog, 'utf8'), /lane-broker-reap id=\S+ descendants-reaped=\d+ reason=no-progress/);
  assert.equal(resultOf(state, row.id).exit, 124);
});

test('config: noProgressTimeoutMs defaults to 15 minutes and is validated globally and per lane', (t) => {
  const { base, home } = freshEnv();
  const previousHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  t.after(() => {
    if (previousHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = previousHome;
  });
  assert.equal(reloadGlobalConfig(undefined).noProgressTimeoutMs, 900_000);
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, noProgressTimeoutMs: -5 } } });
  assert.throws(() => loadRepoConfig(repoDir), /noProgressTimeoutMs must be a non-negative integer/);
  writeGlobalConfig(home, { noProgressTimeoutMs: 1.5 });
  assert.throws(() => loadGlobalConfig(), /noProgressTimeoutMs/);
});

test('watchdog unit: CPU activity or an unreadable table is never idle; formatting', () => {
  const rows = (cpuSec) => [{ pid: 7, token: 't', cpuSec }];
  let cpu = 0;
  const busy = new NoProgressWatchdog({ timeoutMs: 1000, now: 0, readRows: () => rows((cpu += 1)) });
  for (let t = 100; t <= 3000; t += 100) assert.equal(busy.stalled([7], t), false);
  const idle = new NoProgressWatchdog({ timeoutMs: 1000, now: 0, readRows: () => rows(5) });
  const verdicts = [];
  for (let t = 100; t <= 1200; t += 100) verdicts.push(idle.stalled([7], t));
  assert.equal(verdicts.indexOf(true) >= 9, true, 'not before the window has elapsed');
  assert.equal(verdicts.at(-1), true);
  idle.noteOutput(1);
  assert.equal(idle.stalled([7], 1300), false, 'output restarts the window');
  const blind = new NoProgressWatchdog({ timeoutMs: 1000, now: 0, readRows: () => { throw new Error('ps failed'); } });
  for (let t = 100; t <= 3000; t += 100) assert.equal(blind.stalled([7], t), false);
  assert.equal(formatWindow(900_000), '15m');
  assert.equal(formatWindow(90_000), '2m');
  assert.equal(noProgressMessage(900_000), 'lane run: no progress for 15m (no output, no CPU) — killed\n');
});
