import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig, waitFor, sleep, freshEnv } from './helpers.js';
import { setup, tmpDir, probeCount, markerCmd, resultOf } from './remote-harness.js';
import { paths, bootId } from '../src/state.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { listQueue } from '../src/scheduler.js';
import { loadGlobalConfig, resolveTicketConfig, ConfigError } from '../src/config.js';

/** BRAIN-442: `remotePolicy: "local-first"` queues locally and only becomes rebind-eligible after `localFirstWaitMs`. */

const TIMEOUT = { timeout: 120_000 };

/**
 * The harness's fake runner shares the client's broker state, so its probe would report the client's own held lease as runner
 * load. Wrap ssh so a probe always reports an idle runner with plenty of headroom (the same trick remote-rebind.test.js uses).
 */
function localFirstSetup({ lane = { remotePolicy: 'local-first' }, rebindMs = 200 } = {}) {
  const ctx = setup({ ssh: 'normal' });
  const binDir = ctx.env.PATH.split(path.delimiter)[0];
  fs.renameSync(path.join(binDir, 'ssh'), path.join(binDir, 'ssh-real'));
  fs.writeFileSync(
    path.join(binDir, 'ssh'),
    `#!/usr/bin/env node
const { spawn, spawnSync } = require('child_process');
const real = ${JSON.stringify(path.join(binDir, 'ssh-real'))};
const argv = process.argv.slice(2);
if ((argv[argv.length - 1] || '').includes('remote-probe')) {
  const res = spawnSync(real, argv, { encoding: 'utf8' });
  const p = JSON.parse(res.stdout.trim());
  p.queued = 0;
  p.running = 0;
  p.headroom = { cpuCores: 64, memoryBytes: 1e12 };
  process.stdout.write(JSON.stringify(p) + '\\n');
  process.exit(res.status == null ? 1 : res.status);
}
const child = spawn(real, argv, { stdio: 'inherit' });
child.on('close', (code) => process.exit(code == null ? 1 : code));
`,
  );
  fs.chmodSync(path.join(binDir, 'ssh'), 0o755);
  const globalPath = path.join(ctx.home, 'config.json');
  fs.writeFileSync(globalPath, JSON.stringify({ ...JSON.parse(fs.readFileSync(globalPath, 'utf8')), remoteRebindIntervalMs: rebindMs }));
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, ...lane } } });
  return ctx;
}

const holdLocalQueue = (ctx) =>
  writeLease(ctx.state, { id: 'held', key: 'x:held', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 4, state: LEASE_STATE.RUNNING });

async function submit(ctx, marker) {
  const started = await laneRun(['run', '--detach', '--lane', 'default', '--', ...markerCmd(marker)], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(started.code, 0, started.stderr);
  return started.stdout.trim();
}

async function finish(ctx, id) {
  const waited = await laneRun(['wait', id, '--timeout', '60s'], { env: ctx.env });
  assert.equal(waited.code, 0, waited.stderr);
  return resultOf(ctx.state, id);
}

const admissionLog = (ctx) => (fs.existsSync(paths(ctx.state).admissionLog) ? fs.readFileSync(paths(ctx.state).admissionLog, 'utf8') : '');

test('local-first with local headroom runs locally and never touches a runner', TIMEOUT, async () => {
  const ctx = localFirstSetup();
  const marker = path.join(tmpDir('lf-marker'), 'm');
  const id = await submit(ctx, marker);
  const result = await finish(ctx, id);
  assert.equal(result.executor ?? 'local', 'local');
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  assert.equal(probeCount(ctx.probeLogPath), 0, 'no ssh probe or exec was made');
  const history = fs.readFileSync(paths(ctx.state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  const row = history.find((r) => r.id === id);
  assert.equal(row.executor, 'local');
  assert.equal(row.localFirst, true);
});

test('local-first held longer than localFirstWaitMs rebinds to the runner', TIMEOUT, async () => {
  const ctx = localFirstSetup({ lane: { remotePolicy: 'local-first', localFirstWaitMs: 1500 } });
  holdLocalQueue(ctx);
  const marker = path.join(tmpDir('lf-marker'), 'm');
  try {
    const started = Date.now();
    const id = await submit(ctx, marker);
    const record = await waitFor(() => listQueue(ctx.state).find((t) => t.id === id), { timeoutMs: 30_000, intervalMs: 50 });
    assert.equal(record.localFirst, true);
    assert.equal(record.remote.fallback.reason, 'local-first');
    const result = await finish(ctx, id);
    assert.equal(result.executor, 'remote');
    assert.ok(Date.now() - started >= 1500, 'it did not move before its wait elapsed');
    assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
    assert.match(admissionLog(ctx), new RegExp(`remote-rebind: ${id}: seq \\d+ -> skybox .*local-first`));
  } finally {
    removeLease(ctx.state, 'held');
  }
});

test('local-first under localFirstWaitMs does not rebind yet, then runs locally once admitted', TIMEOUT, async () => {
  const ctx = localFirstSetup({ lane: { remotePolicy: 'local-first', localFirstWaitMs: 60_000 } });
  holdLocalQueue(ctx);
  const marker = path.join(tmpDir('lf-marker'), 'm');
  let id;
  try {
    id = await submit(ctx, marker);
    await waitFor(() => listQueue(ctx.state).some((t) => t.id === id), { timeoutMs: 30_000, intervalMs: 50 });
    await sleep(2000); // ten rebind intervals
    assert.deepEqual(listQueue(ctx.state).map((t) => t.id), [id], 'still queued locally');
    assert.equal(probeCount(ctx.probeLogPath), 0, 'no runner was probed');
    assert.doesNotMatch(admissionLog(ctx), /remote-rebind/);
  } finally {
    removeLease(ctx.state, 'held');
  }
  const result = await finish(ctx, id);
  assert.notEqual(result.executor, 'remote');
});

test('a lane without remotePolicy still dispatches remote-first', TIMEOUT, async () => {
  const ctx = localFirstSetup({ lane: {} });
  const marker = path.join(tmpDir('lf-marker'), 'm');
  const id = await submit(ctx, marker);
  const result = await finish(ctx, id);
  assert.equal(result.executor, 'remote');
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  assert.ok(probeCount(ctx.probeLogPath) > 0);
});

test('config: remotePolicy and localFirstWaitMs validate, and the global default is 90000', () => {
  const ctx = setup({ ssh: 'normal' });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = ctx.home;
  try {
    assert.equal(loadGlobalConfig().localFirstWaitMs, 90_000);
    const globalPath = path.join(ctx.home, 'config.json');
    const base = JSON.parse(fs.readFileSync(globalPath, 'utf8'));
    for (const bad of [-1, 1.5, '90']) {
      fs.writeFileSync(globalPath, JSON.stringify({ ...base, localFirstWaitMs: bad }));
      assert.throws(() => loadGlobalConfig(), ConfigError);
    }
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
});

test('config: a lane remotePolicy/localFirstWaitMs is validated, resolved, and inherited by an undeclared lane', () => {
  const { base } = freshEnv();
  for (const lane of [{ remotePolicy: 'sometimes' }, { localFirstWaitMs: -1 }, { localFirstWaitMs: '5' }]) {
    const repoDir = path.join(base, `bad-${Math.random()}`);
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane } } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), ConfigError, JSON.stringify(lane));
  }
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'prepush' },
    lanes: { default: { weight: 1 }, prepush: { weight: 1, remote: true, remotePolicy: 'local-first', localFirstWaitMs: 5 } },
  });
  const plain = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' });
  assert.deepEqual([plain.remotePolicy, plain.localFirstWaitMs], ['remote-first', null]);
  const adHoc = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk1' });
  assert.deepEqual([adHoc.remotePolicy, adHoc.localFirstWaitMs], ['local-first', 5]);
});
