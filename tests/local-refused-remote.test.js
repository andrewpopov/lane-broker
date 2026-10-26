import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig } from './helpers.js';
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
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'test', '--', 'true'], { env, cwd: repoDir });
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
