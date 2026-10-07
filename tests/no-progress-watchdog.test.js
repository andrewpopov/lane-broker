import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, waitFor, sleep, stripLogTimestamps } from './helpers.js';
import { setup, resultOf } from './remote-harness.js';
import { paths } from '../src/state.js';
import { readLease } from '../src/lease.js';
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

test('config: noProgressTimeoutMs defaults to 0 (kill off) and is validated globally and per lane', (t) => {
  const { base, home } = freshEnv();
  const previousHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  t.after(() => {
    if (previousHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = previousHome;
  });
  assert.equal(reloadGlobalConfig(undefined).noProgressTimeoutMs, 0);
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, noProgressTimeoutMs: -5 } } });
  assert.throws(() => loadRepoConfig(repoDir), /noProgressTimeoutMs must be a non-negative integer/);
  writeGlobalConfig(home, { noProgressTimeoutMs: 1.5 });
  assert.throws(() => loadGlobalConfig(), /noProgressTimeoutMs/);
});

const logAllReaps = (state, id) => stripLogTimestamps(fs.readFileSync(paths(state).admissionLog, 'utf8')).split('\n').filter((l) => l.startsWith(`lane-broker-reap id=${id} descendants-reaped=`));

const QUIET_THEN_EXIT = node(`process.stdout.write('started\\n'); setTimeout(() => {}, 3500);`);
// idle 1.2s, a CPU burst, idle 1.2s: no idle streak reaches the 2s window, though the silent total does
const BURST_BETWEEN_IDLES = node(`process.stdout.write('x'); setTimeout(() => { const end = Date.now() + 400; while (Date.now() < end) {} setTimeout(() => {}, 1200); }, 1200);`);
// a silent coordinator that keeps starting short-lived workers (each is a NEW pid in the tree) and uses ~no CPU itself
const WORKER_CHURN = ['sh', '-c', 'echo x; for i in 1 2 3 4 5 6 7 8; do sleep 0.5; done'];

test('default config: a silent run is NEVER killed, but the stall is recorded in status and in the history row', async () => {
  const s = localSetup();
  writeRepoConfig(s.repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...QUIET_THEN_EXIT], { env: s.env, cwd: s.repoDir });
  const seen = await waitFor(async () => {
    const status = JSON.parse((await laneRun(['status', '--json'], { env: s.env })).stdout);
    const stalled = status.running[0]?.noProgressSinceMs;
    return stalled >= 1000 ? stalled : null;
  }, { timeoutMs: 8000, intervalMs: 300 });
  assert.ok(seen >= 1000, 'status reports the running stall');
  await new Promise((resolve) => (child.exitCode !== null ? resolve() : child.on('exit', resolve)));
  assert.equal(child.exitCode, 0);
  const [row] = historyOf(s.state);
  assert.equal(row.reason, undefined);
  assert.ok(row.maxNoProgressMs >= 1500, `longest stretch recorded; got ${row.maxNoProgressMs}`);
});

test('opt-in lane: a CPU burst between two idle stretches resets the window (the kill needs CONSECUTIVE idle time)', async () => {
  const s = localSetup();
  const result = await run(s, BURST_BETWEEN_IDLES);
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(historyOf(s.state)[0].reason, undefined);
});

test('opt-in lane: a silent coordinator spawning short-lived workers is NOT killed (a new pid is progress)', async () => {
  const s = localSetup();
  writeRepoConfig(s.repoDir, { version: 1, lanes: { default: { weight: 1, noProgressTimeoutMs: 1500 } } });
  const result = await run(s, WORKER_CHURN);
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(historyOf(s.state)[0].reason, undefined);
});

test('remote: the submitter\'s GLOBAL timeout applies on the runner, which has none of its own', async () => {
  const { env, home, state, repoDir } = setup();
  const cfg = JSON.parse(fs.readFileSync(path.join(home, 'config.json'), 'utf8'));
  writeGlobalConfig(home, { ...cfg, noProgressTimeoutMs: WINDOW_MS });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...PRINT_THEN_SLEEP], { env, cwd: repoDir });
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  const [row] = historyOf(state);
  assert.equal(row.executor, 'remote');
  assert.equal(row.reason, 'no-progress');
});

test('a cancel that lands during a watchdog kill shares its reap: one reap line, and the cancelled result wins', async (t) => {
  const s = localSetup();
  // ignores TERM, so the watchdog's reap sits in its grace period while the cancel arrives
  const stubborn = node(`process.on('SIGTERM', () => {}); process.stdout.write('started\\n'); setInterval(() => {}, 1000);`);
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...stubborn], { env: s.env, cwd: s.repoDir });
  const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
  t.after(() => child.kill('SIGKILL'));
  const id = await waitFor(() => {
    try {
      return fs.readdirSync(paths(s.state).leases).find((n) => n.endsWith('.json'))?.replace(/\.json$/, '') ?? null;
    } catch {
      return null;
    }
  });
  // the stall is declared once the streak reaches WINDOW_MS; the reap logs only when it finishes, so wait for the lease to be mid-kill
  await waitFor(() => (readLease(s.state, id)?.noProgressSinceMs ?? 0) >= WINDOW_MS - 500, { timeoutMs: 10_000, intervalMs: 100 });
  await sleep(1500);
  const cancelled = await laneRun(['cancel', id], { env: s.env });
  const { code } = await exited;
  assert.equal(cancelled.code, 0, `cancel stderr: ${cancelled.stderr}`);
  assert.equal(code, 130);
  const row = historyOf(s.state).find((r) => r.id === id);
  assert.equal(row.exit, 130);
  assert.equal(row.cancelled, true);
  const reaps = logAllReaps(s.state, id);
  assert.equal(reaps.length, 1, `exactly one reap; got ${JSON.stringify(reaps)}`);
});

// ---- watchdog unit: the pure observation logic, with a fake process table and clock ----

function fakeWatchdog({ timeoutMs = 1000, cpuAt }) {
  let t = 0;
  let table = () => [{ pid: 7, token: 'a', cpuSec: cpuAt(t), childCpuSec: 0 }];
  const dog = new NoProgressWatchdog({ timeoutMs, readRows: () => table(), clock: () => t });
  return { dog, at: (ms) => { t = ms; }, setTable: (fn) => { table = fn; }, now: () => t };
}

test('unit: the first observation only sets the baseline and is never idle, even when the process is busy then or silent', () => {
  const w = fakeWatchdog({ cpuAt: () => 1000 });
  assert.equal(w.dog.observe([7]), 0);
  w.at(500);
  assert.equal(w.dog.observe([7]), 500, 'idle is measured from the baseline, not from spawn');
});

test('unit: a busy interval resets the streak; the kill needs consecutive idle intervals covering the timeout', () => {
  let cpu = 0;
  const w = fakeWatchdog({ cpuAt: () => cpu });
  let idle = w.dog.observe([7]);
  for (let t = 100; t <= 900; t += 100) { w.at(t); idle = w.dog.observe([7]); }
  assert.equal(idle, 900);
  assert.equal(w.dog.shouldKill(idle), false);
  cpu = 0.5; // a burst in the next interval
  w.at(1000);
  assert.equal(w.dog.observe([7]), 0, 'the burst restarts the streak');
  for (let t = 1100; t <= 1900; t += 100) { w.at(t); idle = w.dog.observe([7]); }
  assert.equal(w.dog.shouldKill(idle), false, 'an average over the window would have killed here');
  w.at(2000);
  idle = w.dog.observe([7]);
  assert.equal(w.dog.shouldKill(idle), true);
  assert.equal(w.dog.maxIdleMs, 1000);
});

test('unit: a CPU-silent coordinator that keeps starting short-lived workers is never idle, however long it runs', () => {
  const w = fakeWatchdog({ timeoutMs: 500, cpuAt: () => 5 });
  let worker = 100;
  w.setTable(() => [{ pid: 7, token: 'a', cpuSec: 5, childCpuSec: 0 }, { pid: worker, token: `w${worker}`, cpuSec: 0, childCpuSec: 0 }]);
  w.dog.observe([7, worker]);
  for (let t = 100; t <= 5000; t += 100) {
    worker += 1; // each interval sees a different short-lived worker
    w.at(t);
    const idle = w.dog.observe([7, worker]);
    assert.equal(w.dog.shouldKill(idle), false, `killed at ${t}ms`);
  }
});

test('unit: output, a new pid, a vanished member row, a null scan and an unreadable table each void the window', () => {
  const w = fakeWatchdog({ cpuAt: () => 5 });
  const idleFor = (ms) => { const end = w.now() + ms; for (let t = w.now() + 100; t <= end; t += 100) { w.at(t); w.dog.observe([7]); } };
  w.dog.observe([7]);
  idleFor(500);
  w.dog.noteOutput(1);
  w.at(700);
  assert.equal(w.dog.observe([7]), 0, 'output');
  idleFor(300);
  w.setTable(() => [{ pid: 7, token: 'a', cpuSec: 5, childCpuSec: 0 }, { pid: 8, token: 'b', cpuSec: 0, childCpuSec: 0 }]);
  w.at(1100);
  assert.equal(w.dog.observe([7, 8]), 0, 'a new pid');
  w.at(1200);
  assert.equal(w.dog.observe([7, 8]), 100);
  w.setTable(() => [{ pid: 7, token: 'a', cpuSec: 5, childCpuSec: 0 }]);
  w.at(1300);
  assert.equal(w.dog.observe([7, 8]), 0, 'a known member has no row');
  w.at(1400);
  assert.equal(w.dog.observe([7]), 0, 'and the set shrinking is itself a change');
  w.at(1500);
  assert.equal(w.dog.observe([7]), 100);
  w.at(1600);
  assert.equal(w.dog.observe(null), 0, 'a null descendant scan');
  w.at(1700);
  assert.equal(w.dog.observe([7]), 0, 'the gap voided the baseline');
  w.setTable(() => { throw new Error('ps failed'); });
  w.at(1800);
  assert.equal(w.dog.observe([7]), 0, 'an unreadable process table');
  w.setTable(() => [{ pid: 7, token: 'a', cpuSec: 5, childCpuSec: 0 }]);
  w.at(1900);
  assert.equal(w.dog.observe([7]), 0);
  w.at(2000);
  assert.equal(w.dog.observe([7]), 100);
});

test('unit: reaped children\'s CPU (cutime/cstime) counts as the tree\'s progress; the kill is off at timeout 0', () => {
  let child = 0;
  const dog = new NoProgressWatchdog({ timeoutMs: 0, readRows: () => [{ pid: 7, token: 'a', cpuSec: 1, childCpuSec: child }], clock: () => t });
  let t = 0;
  dog.observe([7]);
  t = 100;
  assert.equal(dog.observe([7]), 100);
  child = 0.2;
  t = 200;
  assert.equal(dog.observe([7]), 0, 'CPU spent by a worker that already exited');
  assert.equal(dog.shouldKill(1e9), false, 'timeout 0 never kills');
});

test('formatting', () => {
  assert.equal(formatWindow(900_000), '15m');
  assert.equal(formatWindow(90_000), '2m');
  assert.equal(noProgressMessage(900_000), 'lane run: no progress for 15m (no output, no CPU) — killed\n');
});
