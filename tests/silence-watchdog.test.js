import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, stripLogTimestamps } from './helpers.js';
import { setup } from './remote-harness.js';
import { paths } from '../src/state.js';
import { NoProgressWatchdog, silenceMessage } from '../src/no-progress.js';
import { reloadGlobalConfig, loadGlobalConfig, loadRepoConfig } from '../src/config.js';

/** BRAIN-576: a run that writes no output for silenceTimeoutMs is killed whatever its CPU does (exit 124, reason silence). */

const WINDOW_MS = 2000;
const node = (body) => ['node', '-e', body];
// prints once, then burns CPU silently: the no-progress watchdog never fires on this
const SPIN_SILENT = node(`process.stdout.write('started\\n'); const end = Date.now() + 20000; while (Date.now() < end) {}`);
// burns CPU the whole time and prints every ~300 ms for ~4.5 s
const SPIN_CHATTY = node(`let last = 0; const end = Date.now() + 4500; while (Date.now() < end) { if (Date.now() - last > 300) { process.stdout.write('tick\\n'); last = Date.now(); } }`);
// silent for longer than the window but not CPU-busy: both watchdogs could fire
const PRINT_THEN_SLEEP = node(`process.stdout.write('started\\n'); setInterval(() => {}, 1000);`);
const SILENT_FOR_4S = node(`setTimeout(() => process.stdout.write('late\\n'), 4000);`);

function localSetup(laneCfg = { silenceTimeoutMs: WINDOW_MS }, globalCfg = {}) {
  const { base, home, state, env } = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '4000' });
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, ...globalCfg });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...laneCfg } } });
  return { env, state, repoDir };
}

const historyOf = (state) =>
  fs
    .readFileSync(paths(state).history, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l));

const run = (s, argv) => laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...argv], { env: s.env, cwd: s.repoDir });

test('local: a silent CPU-spinning command is killed with reason=silence and exit 124', async () => {
  const s = localSetup();
  const started = Date.now();
  const result = await run(s, SPIN_SILENT);
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  assert.ok(Date.now() - started < 30_000, 'killed promptly, not left to run');
  assert.match(result.stderr, /lane run: no output for 2s — killed \(silence watchdog\)/);
  const [row] = historyOf(s.state);
  assert.equal(row.exit, 124);
  assert.equal(row.reason, 'silence');
  assert.equal(row.silenceTimeoutMs, WINDOW_MS);
  assert.equal(row.executor, 'local');
  assert.match(fs.readFileSync(paths(s.state).admissionLog, 'utf8'), /lane-broker-reap id=\S+ descendants-reaped=\d+ reason=silence/);
  assert.match(fs.readFileSync(path.join(paths(s.state).logs, `${row.id}.log`), 'utf8'), /no output for 2s — killed \(silence watchdog\)/);
});

test('local: a CPU-spinning command that keeps printing is NOT killed', async () => {
  const s = localSetup();
  const result = await run(s, SPIN_CHATTY);
  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.equal(historyOf(s.state)[0].reason, undefined);
});

test('local: silenceTimeoutMs 0 never kills, and neither does leaving it unset', async () => {
  for (const laneCfg of [{ silenceTimeoutMs: 0 }, {}]) {
    const s = localSetup(laneCfg);
    const result = await run(s, SILENT_FOR_4S);
    assert.equal(result.code, 0, `stderr: ${result.stderr}`);
    assert.equal(historyOf(s.state)[0].reason, undefined);
  }
});

test('local: the per-lane value beats the global one, in both directions', async () => {
  const lanePermissive = localSetup({ silenceTimeoutMs: 0 }, { silenceTimeoutMs: WINDOW_MS });
  const permissive = await run(lanePermissive, SILENT_FOR_4S);
  assert.equal(permissive.code, 0, `lane 0 must override the global window; stderr: ${permissive.stderr}`);

  const laneStrict = localSetup({ silenceTimeoutMs: WINDOW_MS }, { silenceTimeoutMs: 600_000 });
  const strict = await run(laneStrict, SILENT_FOR_4S);
  assert.equal(strict.code, 124, `lane window must override the global one; stderr: ${strict.stderr}`);
  assert.equal(historyOf(laneStrict.state)[0].reason, 'silence');
});

test('local: the global value applies to a lane that declares none', async () => {
  const s = localSetup({}, { silenceTimeoutMs: WINDOW_MS });
  const result = await run(s, SPIN_SILENT);
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  assert.equal(historyOf(s.state)[0].reason, 'silence');
});

test('local: with both watchdogs armed the first to fire wins and the tree is reaped once', async () => {
  // no-progress needs 6 s of idle, silence 2 s: silence is first
  const s = localSetup({ silenceTimeoutMs: WINDOW_MS, noProgressTimeoutMs: 6000 });
  const result = await run(s, PRINT_THEN_SLEEP);
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  const [row] = historyOf(s.state);
  assert.equal(row.reason, 'silence');
  assert.equal(row.noProgressTimeoutMs, undefined);
  const reaps = stripLogTimestamps(fs.readFileSync(paths(s.state).admissionLog, 'utf8'))
    .split('\n')
    .filter((l) => l.startsWith(`lane-broker-reap id=${row.id} descendants-reaped=`));
  assert.equal(reaps.length, 1, `exactly one reap; got ${JSON.stringify(reaps)}`);
});

test('remote: the runner kills a silent spinner; the submitter gets the message, exit 124 and the history reason', async () => {
  const { env, state, repoDir } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, silenceTimeoutMs: WINDOW_MS } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...SPIN_SILENT], { env, cwd: repoDir });
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  assert.match(result.stderr + result.stdout, /lane run: no output for 2s — killed \(silence watchdog\)/);
  const [row] = historyOf(state);
  assert.equal(row.executor, 'remote');
  assert.equal(row.reason, 'silence');
  assert.equal(row.silenceTimeoutMs, WINDOW_MS);
});

test('config: silenceTimeoutMs defaults to 0 and is validated globally and per lane', (t) => {
  const { base, home } = freshEnv();
  const previousHome = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  t.after(() => {
    if (previousHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = previousHome;
  });
  assert.equal(reloadGlobalConfig(undefined).silenceTimeoutMs, 0);
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, silenceTimeoutMs: -5 } } });
  assert.throws(() => loadRepoConfig(repoDir), /silenceTimeoutMs must be a non-negative integer/);
  writeGlobalConfig(home, { silenceTimeoutMs: 1.5 });
  assert.throws(() => loadGlobalConfig(), /silenceTimeoutMs/);
});

test('unit: silence is measured from start until the first byte, then from the last byte, ignoring CPU', () => {
  let t = 0;
  const dog = new NoProgressWatchdog({ timeoutMs: 0, silenceTimeoutMs: 1000, readRows: () => [], clock: () => t });
  dog.start();
  t = 999;
  assert.equal(dog.shouldKillSilent(), false);
  t = 1000;
  assert.equal(dog.shouldKillSilent(), true);
  dog.noteOutput(5);
  assert.equal(dog.shouldKillSilent(), false, 'a byte restarts the window');
  t = 1999;
  assert.equal(dog.shouldKillSilent(), false);
  t = 2000;
  assert.equal(dog.shouldKillSilent(), true);
  assert.equal(new NoProgressWatchdog({ timeoutMs: 0, readRows: () => [], clock: () => t }).shouldKillSilent(), false, 'off at 0');
});

test('formatting', () => {
  assert.equal(silenceMessage(20 * 60_000), 'lane run: no output for 20m — killed (silence watchdog)\n');
});
