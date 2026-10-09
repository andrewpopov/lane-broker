import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig } from './helpers.js';
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
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--', 'true'], { env, cwd: repoDir });
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
  setTimeout(() => fs.rmSync(pauseFile, { force: true }), 2000);
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], ctx.env, ctx.repoDir, '60s');
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  assert.equal(resultOf(ctx.state, id).executor, 'remote');
});
