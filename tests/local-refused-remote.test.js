import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { laneRun, writeRepoConfig, gitFixture } from './helpers.js';
import { paths } from '../src/state.js';
import { locallyRefusedNow, localWindowAllowing } from '../src/resources.js';
import { setup as remoteSetup, tmpDir, markerCmd, detachAndWait, resultOf } from './remote-harness.js';

/**
 * BRAIN-320: a `localRefused` lane (rouge's `sim`) that is ALSO
 * remote-eligible must try a remote runner FIRST -- only a fallback to
 * LOCAL execution is refused. Builds on the fake-ssh harness
 * (tests/remote-harness.js) already shared by tests/remote-dispatch.test.js.
 */

/** remoteSetup() writes a `default` lane with `remote: true` but no
 *  `localRefused` -- overwrite the repo config with both flags set. */
function setupLocalRefusedRemote(opts) {
  const ctx = remoteSetup(opts);
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, localRefused: true } } });
  return ctx;
}

test('localRefused + remote + a usable runner: runs REMOTELY, exit from the command', async () => {
  const { env, state, repoDir } = setupLocalRefusedRemote({ ssh: 'normal' });
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

test('localRefused + remote + runner down: exit 76 (no runner, retryable), never ran locally', async () => {
  const { env, state, repoDir } = setupLocalRefusedRemote({ ssh: 'down' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 76, `stderr: ${waited.stderr}`);
  const result = resultOf(state, id);
  assert.match(result.error, /runs only on a remote runner and none can take it now \(skybox: probe exited 255\)/);
  assert.doesNotMatch(result.error, /ROUGE_FLEET_SUBMIT_DSN/);
  assert.equal(fs.existsSync(marker), false, 'the command must never have run locally');
});

test('localRefused + remote + every runner unreachable: exit 76 naming each runner, no DSN variable (BRAIN-455)', async () => {
  const { env, home, state, repoDir } = setupLocalRefusedRemote({ ssh: 'down' });
  const cfg = JSON.parse(fs.readFileSync(path.join(home, 'config.json'), 'utf8'));
  cfg.runners.push({ ...cfg.runners[0], name: 'wintop' });
  fs.writeFileSync(path.join(home, 'config.json'), JSON.stringify(cfg));
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 76, `stderr: ${waited.stderr}`);
  const result = resultOf(state, id);
  assert.match(result.error, /skybox: probe exited 255; wintop: probe exited 255/);
  assert.doesNotMatch(result.error, /ROUGE_FLEET_SUBMIT_DSN/);
  assert.equal(fs.existsSync(marker), false);
});

test('localRefused + remote + runner down + --allow-local-sim: falls back and runs locally', async () => {
  const { env, state, repoDir } = setupLocalRefusedRemote({ ssh: 'down' });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--allow-local-sim', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'local');
});

test('localRefused WITHOUT remote: refused immediately with exit 69, exactly as today', async () => {
  const { env, repoDir } = remoteSetup({ ssh: 'normal' });
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, localRefused: true } } });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 69);
  assert.match(result.stderr, /refused for local runs/);
});

test('localRefused + remote:true, but forced local via --local: refused immediately with exit 69', async () => {
  const { env, repoDir } = setupLocalRefusedRemote({ ssh: 'normal' });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'needs ssh', '--', 'true'], { env, cwd: repoDir });
  assert.equal(result.code, 69);
  assert.match(result.stderr, /refused for local runs/);
});

// ---- BRAIN-509: a ticket with no local fallback carries no queue timeout ----

/** Park a ticket in the paused runner's queue and return its runner-side queue entry (where the header's queueTimeoutMs lands as `startDeadline`). */
async function queuedOnPausedRunner(ctx, extraRunArgs = []) {
  fs.mkdirSync(ctx.runnerState, { recursive: true });
  fs.writeFileSync(path.join(ctx.runnerState, 'PAUSE'), 'kept busy for the test');
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', ...extraRunArgs, '--', ...markerCmd(path.join(tmpDir('marker'), 'where'), 0)], {
    env: ctx.env,
    cwd: ctx.repoDir,
  });
  assert.equal(started.code, 0, `stderr: ${started.stderr}`);
  const queueDir = path.join(ctx.runnerState, 'queue');
  for (let i = 0; i < 100; i++) {
    const entry = fs.existsSync(queueDir) ? fs.readdirSync(queueDir).find((f) => f.endsWith('.json')) : undefined;
    if (entry) {
      try {
        const queued = JSON.parse(fs.readFileSync(path.join(queueDir, entry), 'utf8'));
        await laneRun(['cancel', started.stdout.trim()], { env: ctx.env });
        return queued;
      } catch {
        // mid-write; retry
      }
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  await laneRun(['cancel', started.stdout.trim()], { env: ctx.env });
  assert.fail('the ticket never reached the runner queue');
}

test('BRAIN-509: localRefused + remote sends no queueTimeoutMs even with remoteQueueTimeoutMs configured', async () => {
  const queued = await queuedOnPausedRunner(setupLocalRefusedRemote({ remoteQueueTimeoutMs: 30_000 }));
  assert.equal(queued.startDeadline, undefined);
});

test('BRAIN-509: localRefused + remote + --allow-local-sim DOES send queueTimeoutMs (it can fall back locally)', async () => {
  const queued = await queuedOnPausedRunner(setupLocalRefusedRemote({ remoteQueueTimeoutMs: 30_000 }), ['--allow-local-sim']);
  assert.ok(Number.isFinite(queued.startDeadline), 'the queue timeout must reach the runner');
});

test('BRAIN-509: an ordinary local-eligible remote lane DOES send queueTimeoutMs', async () => {
  const queued = await queuedOnPausedRunner(remoteSetup({ remoteQueueTimeoutMs: 30_000 }));
  assert.ok(Number.isFinite(queued.startDeadline), 'the queue timeout must reach the runner');
});

test('BRAIN-509: a localRefused ticket outlasting remoteQueueTimeoutMs in a busy runner keeps waiting and completes remotely, not exit 69', async () => {
  const ctx = setupLocalRefusedRemote({ remoteQueueTimeoutMs: 300 });
  fs.mkdirSync(ctx.runnerState, { recursive: true });
  const pauseFile = path.join(ctx.runnerState, 'PAUSE');
  fs.writeFileSync(pauseFile, 'kept busy for the test');
  const marker = path.join(tmpDir('marker'), 'where');
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(started.code, 0, `stderr: ${started.stderr}`);
  const id = started.stdout.trim();
  // Only once the ticket is in the runner's queue does the clock that would have expired it run: hold it paused well
  // past the 300 ms timeout from THAT point, so the test cannot pass by the runner merely starting late.
  const queueDir = path.join(ctx.runnerState, 'queue');
  for (let i = 0; i < 100 && !(fs.existsSync(queueDir) && fs.readdirSync(queueDir).some((f) => f.endsWith('.json'))); i++) {
    await new Promise((r) => setTimeout(r, 100));
  }
  assert.ok(fs.readdirSync(queueDir).some((f) => f.endsWith('.json')), 'the ticket never reached the runner queue');
  await new Promise((r) => setTimeout(r, 1500));
  fs.rmSync(pauseFile, { force: true });
  const waited = await laneRun(['wait', id, '--timeout', '60s'], { env: ctx.env });
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  assert.equal(resultOf(ctx.state, id).executor, 'remote');
});

// ---- BRAIN-629: localWindow lets named localRefused lanes run on this machine during a window ----

const ALL_DAYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
/** Always open: two windows that together cover every minute of every day. */
const ALWAYS_OPEN = { timezone: 'UTC', available: [{ days: ALL_DAYS, from: '00:00', to: '23:59' }, { days: ALL_DAYS, from: '23:59', to: '00:00' }] };
/** Closed today: open only on the days other than today (UTC). */
const closedToday = () => ({
  timezone: 'UTC',
  available: [{ days: ALL_DAYS.filter((d) => d !== ALL_DAYS[(new Date().getUTCDay() + 6) % 7]), from: '00:00', to: '23:59' }],
});

function setupWindow({ schedule, lanes, ssh = 'down' }) {
  const ctx = remoteSetup({ ssh });
  writeRepoConfig(ctx.repoDir, {
    version: 1,
    lanes: { e2e: { weight: 1, remote: true, localRefused: true }, train: { weight: 1, remote: true, localRefused: true, maxExecutionCpuCores: 6 }, sim: { weight: 1, remote: true, localRefused: true, class: 'sim' } },
  });
  const cfg = JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8'));
  cfg.localWindow = { schedule, lanes };
  fs.writeFileSync(path.join(ctx.home, 'config.json'), JSON.stringify(cfg));
  return ctx;
}

async function runLane(ctx, lane) {
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', lane, '--detach', '--', ...markerCmd(marker, 0)], ctx.env, ctx.repoDir);
  return { id, waited, marker };
}

test('BRAIN-629: in-window + listed lane, no runner can take it: runs locally, and the log says why', async () => {
  const ctx = setupWindow({ schedule: ALWAYS_OPEN, lanes: ['e2e'] });
  const { id, waited, marker } = await runLane(ctx, 'e2e');
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  assert.equal(resultOf(ctx.state, id).executor, 'local');
  assert.match(fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'), /running locally \(localWindow 00:00-23:59, 23:59-00:00\)/);
});

test('BRAIN-629 + BRAIN-634: a dispatch-only host ignores an open window: listed lane exits 76, never ran', async () => {
  const ctx = setupWindow({ schedule: ALWAYS_OPEN, lanes: ['e2e'] });
  const file = path.join(ctx.home, 'config.json');
  fs.writeFileSync(file, JSON.stringify({ ...JSON.parse(fs.readFileSync(file, 'utf8')), localExecution: 'refused' }));
  const { waited, marker } = await runLane(ctx, 'e2e');
  assert.equal(waited.code, 76, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false);
});

test('BRAIN-629: in-window + unlisted lane (sim): still refused, exit 76, never ran', async () => {
  const ctx = setupWindow({ schedule: ALWAYS_OPEN, lanes: ['e2e'] });
  const { waited, marker } = await runLane(ctx, 'sim');
  assert.equal(waited.code, 76, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false);
});

test('BRAIN-629: out-of-window + listed lane: refused exactly as before (exit 76, never ran)', async () => {
  const ctx = setupWindow({ schedule: closedToday(), lanes: ['e2e'] });
  const { waited, marker } = await runLane(ctx, 'e2e');
  assert.equal(waited.code, 76, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false);
});

test('TPWR-674: in-window lane with an unenforceable CPU ceiling and no runner free: exit 76 (retryable), not 69 CPU_LIMIT_UNAVAILABLE, never ran', async () => {
  const ctx = setupWindow({ schedule: ALWAYS_OPEN, lanes: ['train'] });
  const { id, waited, marker } = await runLane(ctx, 'train');
  assert.equal(waited.code, 76, `stderr: ${waited.stderr}`);
  assert.match(resultOf(ctx.state, id).error, /no runner can take lane "train" now \(skybox: probe exited 255\) and it cannot run locally \(CPU_LIMIT_UNAVAILABLE/);
  assert.equal(fs.existsSync(marker), false);
});

test('BRAIN-629: locallyRefusedNow follows the clock: closed at 00:50, open at 01:00, closed again at 06:01', () => {
  const globalCfg = {
    localWindow: { schedule: { timezone: 'UTC', available: [{ days: ALL_DAYS, from: '01:00', to: '06:00' }] }, lanes: ['e2e'] },
  };
  const e2e = { lane: 'e2e', localRefused: true, allowLocalSim: false };
  const at = (hhmm) => Date.parse(`2026-10-19T${hhmm}:00Z`);
  assert.equal(locallyRefusedNow(e2e, globalCfg, at('00:50')), true);
  assert.equal(locallyRefusedNow(e2e, globalCfg, at('01:00')), false);
  assert.equal(localWindowAllowing(e2e, globalCfg, at('01:00')), '01:00-06:00');
  assert.equal(locallyRefusedNow(e2e, globalCfg, at('05:59')), false);
  assert.equal(locallyRefusedNow(e2e, globalCfg, at('06:01')), true);
  assert.equal(locallyRefusedNow({ ...e2e, lane: 'sim' }, globalCfg, at('03:00')), true, 'an unlisted lane stays refused in the window');
  assert.equal(locallyRefusedNow({ ...e2e, localRefused: false }, globalCfg, at('00:50')), false);
  assert.equal(locallyRefusedNow(e2e, { localWindow: null }, at('03:00')), true);
});

// ---- BRAIN-769: a lane that fell back to a local run prepares its tree first ----

const NPMRC = 'registry=http://127.0.0.1:9\nfetch-retries=0\nfetch-timeout=2000\n';
const REAL_NPM = execFileSync('which', ['npm'], { encoding: 'utf8' }).trim();

/** package.json, package-lock.json and a tarball dependency, built once with real npm offline. */
const depsFixture = (() => {
  let built;
  return () => {
    if (built) return built;
    const dir = tmpDir('brain769-fixture');
    const npmEnv = { ...process.env, HOME: dir, npm_config_cache: path.join(dir, 'cache'), npm_config_userconfig: path.join(dir, 'userrc') };
    const tool = path.join(dir, 'tool');
    const app = path.join(dir, 'app');
    fs.mkdirSync(tool, { recursive: true });
    fs.mkdirSync(app);
    fs.writeFileSync(path.join(tool, 'package.json'), JSON.stringify({ name: 'hello-tool', version: '1.0.0' }));
    execFileSync(REAL_NPM, ['pack', '--silent', '--pack-destination', app], { cwd: tool, env: npmEnv });
    fs.writeFileSync(path.join(app, 'package.json'), JSON.stringify({ name: 'app', version: '1.0.0', private: true, dependencies: { 'hello-tool': 'file:./hello-tool-1.0.0.tgz' } }));
    execFileSync(REAL_NPM, ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: app, env: npmEnv });
    built = Object.fromEntries(['package.json', 'package-lock.json', 'hello-tool-1.0.0.tgz'].map((f) => [f, fs.readFileSync(path.join(app, f))]));
    return built;
  };
})();

/** An ALWAYS_OPEN window harness whose repo is a committed tree with the fixture app and the given lane declarations; no node_modules. */
function setupPrep(laneDecl) {
  const ctx = setupWindow({ schedule: ALWAYS_OPEN, lanes: ['e2e'] });
  for (const [name, content] of Object.entries(depsFixture())) fs.writeFileSync(path.join(ctx.repoDir, name), content);
  fs.writeFileSync(path.join(ctx.repoDir, '.npmrc'), NPMRC);
  fs.mkdirSync(path.join(ctx.repoDir, 'sub'));
  fs.writeFileSync(path.join(ctx.repoDir, 'sub', 'keep'), '');
  writeRepoConfig(ctx.repoDir, {
    version: 1,
    lanes: { e2e: { weight: 1, remote: true, localRefused: true, ...laneDecl }, plain: { weight: 1, remote: true, ...laneDecl } },
  });
  gitFixture(['add', '-A'], ctx.repoDir);
  gitFixture(['commit', '-q', '-m', 'fixture'], ctx.repoDir);
  return ctx;
}

/** A command that records, in `marker`, its cwd and whether the fixture dependency resolves from there. */
const probeCmd = (marker) => [
  'node',
  '-e',
  `const fs = require('fs'); fs.writeFileSync(${JSON.stringify(marker)}, JSON.stringify({ cwd: fs.realpathSync(process.cwd()), dep: require('module').createRequire(process.cwd() + '/').resolve.paths('x').some((p) => fs.existsSync(p + '/hello-tool/package.json')) }))`,
];
const SETUP_MARK = (file) => ['node', '-e', `require('fs').appendFileSync(${JSON.stringify(file)}, 'x')`];

async function runPrepLane(ctx, cmd, { lane = 'e2e', cwd = ctx.repoDir, extra = [] } = {}) {
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', lane, '--detach', ...extra, '--', ...cmd], ctx.env, cwd);
  return { id, waited, log: () => fs.readFileSync(path.join(paths(ctx.state).logs, `${id}.log`), 'utf8') };
}

test('BRAIN-769: a fallback lane in a tree lacking node_modules installs, then runs the command', async () => {
  const ctx = setupPrep({ remoteDeps: ['.'] });
  const marker = path.join(tmpDir('marker'), 'probe.json');
  const { waited } = await runPrepLane(ctx, probeCmd(marker));
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(JSON.parse(fs.readFileSync(marker, 'utf8')).dep, true, 'the dependency was installed before the command ran');
  assert.ok(fs.existsSync(path.join(ctx.repoDir, 'node_modules', 'hello-tool', 'package.json')));
  assert.match(fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'), /preparing tree before local run/);
});

test('BRAIN-769: a failed setup exits 77 with the infrastructure message and the command never runs', async () => {
  const ctx = setupPrep({ remoteDeps: ['.'], remoteSetup: [['node', '-e', 'process.exit(3)']] });
  const marker = path.join(tmpDir('marker'), 'probe.json');
  const { waited, log } = await runPrepLane(ctx, probeCmd(marker));
  assert.equal(waited.code, 77, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(marker), false, 'the command must not run in an unprepared tree');
  assert.match(log(), /lane: local-setup failed \(setup\) — infrastructure, not the command/);
});

test('BRAIN-769: a tree whose dependencies are current is not reinstalled', async () => {
  const ctx = setupPrep({ remoteDeps: ['.'] });
  const first = await runPrepLane(ctx, ['node', '-e', '0']);
  assert.equal(first.waited.code, 0, `stderr: ${first.waited.stderr}`);
  const sentinel = path.join(ctx.repoDir, 'node_modules', 'sentinel');
  fs.writeFileSync(sentinel, '');
  const marker = path.join(tmpDir('marker'), 'probe.json');
  const second = await runPrepLane(ctx, probeCmd(marker));
  assert.equal(second.waited.code, 0, `stderr: ${second.waited.stderr}`);
  assert.ok(fs.existsSync(sentinel), 'npm ci would have removed node_modules');
  assert.equal(JSON.parse(fs.readFileSync(marker, 'utf8')).dep, true);
});

test('BRAIN-769: a stale tree (locked version differs from the installed one) is reinstalled', async () => {
  const ctx = setupPrep({ remoteDeps: ['.'] });
  await runPrepLane(ctx, ['node', '-e', '0']);
  const hiddenFile = path.join(ctx.repoDir, 'node_modules', '.package-lock.json');
  const hidden = JSON.parse(fs.readFileSync(hiddenFile, 'utf8'));
  hidden.packages['node_modules/hello-tool'].version = '0.0.1';
  fs.writeFileSync(hiddenFile, JSON.stringify(hidden));
  const sentinel = path.join(ctx.repoDir, 'node_modules', 'sentinel');
  fs.writeFileSync(sentinel, '');
  const { waited } = await runPrepLane(ctx, ['node', '-e', '0']);
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(sentinel), false, 'npm ci replaced the stale tree');
});

test('BRAIN-769: a setup-only lane runs its setup before the command', async () => {
  const setupLog = path.join(tmpDir('marker'), 'setup.log');
  const ctx = setupPrep({ remoteSetup: [SETUP_MARK(setupLog)] });
  const marker = path.join(tmpDir('marker'), 'probe.json');
  const { waited } = await runPrepLane(ctx, probeCmd(marker));
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(setupLog, 'utf8'), 'x');
  assert.ok(fs.existsSync(marker));
});

test('BRAIN-769: a command started in a subdirectory runs in that subdirectory of the prepared tree', async () => {
  const ctx = setupPrep({ remoteDeps: ['.'] });
  const marker = path.join(tmpDir('marker'), 'probe.json');
  const { waited } = await runPrepLane(ctx, probeCmd(marker), { cwd: path.join(ctx.repoDir, 'sub') });
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  const probe = JSON.parse(fs.readFileSync(marker, 'utf8'));
  assert.equal(probe.cwd, fs.realpathSync(path.join(ctx.repoDir, 'sub')));
  assert.equal(probe.dep, true, 'the root install is visible from the subdirectory');
});

test('BRAIN-769: an explicit --local run does not prepare the tree', async () => {
  const setupLog = path.join(tmpDir('marker'), 'setup.log');
  const ctx = setupPrep({ remoteDeps: ['.'], remoteSetup: [SETUP_MARK(setupLog)] });
  const marker = path.join(tmpDir('marker'), 'probe.json');
  const { waited } = await runPrepLane(ctx, probeCmd(marker), { lane: 'plain', extra: ['--local', '--local-reason', 'needs the lane CLI'] });
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.existsSync(setupLog), false, 'setup must not run');
  assert.equal(fs.existsSync(path.join(ctx.repoDir, 'node_modules')), false, 'nothing was installed');
});

test('BRAIN-769: the command\'s own nonzero exit passes through a prepared tree unchanged', async () => {
  const ctx = setupPrep({ remoteDeps: ['.'] });
  const { waited } = await runPrepLane(ctx, ['node', '-e', 'process.exit(5)']);
  assert.equal(waited.code, 5, `stderr: ${waited.stderr}`);
});
