import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, waitFor, sleep } from './helpers.js';
import { tmpDir, setup, recordingCmd, linesOf, hasLease, holdLaneLocally, detach, waitResult } from './remote-harness.js';
import { enqueue, tryStart, listQueue, withdrawQueued, restoreQueued, withdrawUnlessAdmissible } from '../src/scheduler.js';
import { readAttempt, createAttempt } from '../src/attempts.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig, ConfigError } from '../src/config.js';
import { paths } from '../src/state.js';

/**
 * A ticket stranded in the local queue rebinds to a runner that is itself queued (after `remoteRebindMinLocalWaitMs` of local
 * residency), through one locked admit-or-withdraw decision; a runner it was ever dispatched to is never revisited.
 */

// ---- scheduler: the local-residency clock and the locked admit-or-withdraw decision ----

const cfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 4, schedulerMode: 'shadow' };
const ticketOf = (id, key = `r:${id}`) => ({ id, key, weight: 1, resources: {}, conflicts: [], supervisorPid: process.pid, supervisorStart: null });
const queuedIds = (state) => listQueue(state).map((t) => t.id);

test('localSince is stamped on enqueue and reset by a restore, which keeps seq, createdAt and the priority origin', async () => {
  const { state } = freshEnv();
  const record = await enqueue(state, { ...ticketOf('a'), createdAt: 1000 });
  assert.ok(Math.abs(record.localSince - Date.now()) < 5000, 'stamped at enqueue, not taken from createdAt');
  await sleep(30);
  const taken = await withdrawQueued(state, 'a');
  await sleep(30);
  await restoreQueued(state, taken);
  const restored = listQueue(state)[0];
  assert.ok(restored.localSince > record.localSince, 'the residency clock restarts');
  assert.equal(restored.seq, record.seq);
  assert.equal(restored.createdAt, 1000);
  assert.equal(restored.prioOriginAt, record.prioOriginAt);
});

test('withdrawUnlessAdmissible: a head that local capacity can take now stays queued', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  assert.deepEqual(await withdrawUnlessAdmissible(state, 'a', cfg, ticketOf('a')), { admissible: true });
  assert.deepEqual(queuedIds(state), ['a']);
});

test('withdrawUnlessAdmissible: a head blocked by a held conflicting lease is withdrawn, and admissible once that lease is gone', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('holder', 'r:same'));
  assert.equal((await tryStart(state, ticketOf('holder', 'r:same'), cfg)).started, true);
  await enqueue(state, ticketOf('a', 'r:same'));
  const decision = await withdrawUnlessAdmissible(state, 'a', cfg, ticketOf('a', 'r:same'));
  assert.equal(decision.record.id, 'a');
  assert.deepEqual(queuedIds(state), []);
  await restoreQueued(state, decision.record);
  fs.rmSync(path.join(paths(state).leases, 'holder.json'), { force: true });
  assert.deepEqual(await withdrawUnlessAdmissible(state, 'a', cfg, ticketOf('a', 'r:same')), { admissible: true });
  assert.deepEqual(queuedIds(state), ['a']);
});

test('withdrawUnlessAdmissible: a ticket with another queued ahead of it is withdrawn; a cancelled one is left alone', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  await enqueue(state, ticketOf('b'));
  const behind = await withdrawUnlessAdmissible(state, 'b', cfg, ticketOf('b'));
  assert.equal(behind.record.id, 'b');
  fs.mkdirSync(paths(state).cancel, { recursive: true });
  fs.writeFileSync(path.join(paths(state).cancel, 'a'), '');
  assert.deepEqual(await withdrawUnlessAdmissible(state, 'a', cfg, ticketOf('a')), { admissible: true }, 'admission is evaluated first');
  assert.deepEqual(queuedIds(state), ['a']);
  const missing = await withdrawUnlessAdmissible(state, 'nobody', cfg, ticketOf('nobody'));
  assert.equal(missing.record, null);
});

test('remoteRebindMinLocalWaitMs defaults to 120000, accepts 0, rejects negative and non-integers', () => {
  const load = (extra) => {
    const { home } = freshEnv();
    writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000, ...extra });
    const prev = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      process.env.LANE_BROKER_HOME = prev;
    }
  };
  assert.equal(load({}).remoteRebindMinLocalWaitMs, 120_000);
  assert.equal(load({ remoteRebindMinLocalWaitMs: 0 }).remoteRebindMinLocalWaitMs, 0);
  for (const bad of [-1, 1.5, '1000']) assert.throws(() => load({ remoteRebindMinLocalWaitMs: bad }), ConfigError);
});

// ---- end to end over the fake-ssh transport ----

/**
 * The fake runner's probe is overridden through a control file: the queue length it reports (the real fake shares the client's own
 * queue, which would always read as busy) and, optionally, a hold that parks the probe until a file appears.
 */
function queuedRunnerCtx({ runners = 1, remoteQueueTimeoutMs, config = {} } = {}) {
  const ctx = setup({ ssh: 'normal', remoteQueueTimeoutMs });
  const binDir = ctx.env.PATH.split(path.delimiter)[0];
  const control = path.join(tmpDir('queued-control'), 'mode.json');
  const execLog = path.join(tmpDir('queued-exec'), 'exec.log');
  fs.writeFileSync(control, JSON.stringify({ queued: 0 }));
  fs.renameSync(path.join(binDir, 'ssh'), path.join(binDir, 'ssh-real'));
  fs.writeFileSync(
    path.join(binDir, 'ssh'),
    `#!/usr/bin/env node
const fs = require('fs');
const { spawn, spawnSync } = require('child_process');
const mode = JSON.parse(fs.readFileSync(${JSON.stringify(control)}, 'utf8'));
const real = ${JSON.stringify(path.join(binDir, 'ssh-real'))};
const argv = process.argv.slice(2);
const command = argv[argv.length - 1] || '';
if (command.includes('remote-exec')) fs.appendFileSync(${JSON.stringify(execLog)}, command + '\\n');
if (command.includes('remote-probe')) {
  if (mode.hold) {
    fs.writeFileSync(mode.hold.ready, '');
    const spin = () => (fs.existsSync(mode.hold.go) ? finish() : setTimeout(spin, 30));
    var finish = () => answer();
    spin();
  } else answer();
  function answer() {
    const res = spawnSync(real, argv, { encoding: 'utf8' });
    const p = JSON.parse(res.stdout.trim());
    p.queued = mode.queued;
    p.running = 0;
    p.headroom = { cpuCores: mode.queued > 0 ? 0 : 64, memoryBytes: 1e12 };
    process.stdout.write(JSON.stringify(p) + '\\n');
    process.exit(res.status == null ? 1 : res.status);
  }
} else {
  const child = spawn(real, argv, { stdio: 'inherit' });
  child.on('close', (code) => process.exit(code == null ? 1 : code));
}
`,
  );
  fs.chmodSync(path.join(binDir, 'ssh'), 0o755);
  const globalPath = path.join(ctx.home, 'config.json');
  const base = JSON.parse(fs.readFileSync(globalPath, 'utf8'));
  const extraRunners = Array.from({ length: runners - 1 }, (_, i) => ({ ...base.runners[0], name: `runner${i + 2}`, root: tmpDir('queued-extra-root') }));
  const write = (patch) => fs.writeFileSync(globalPath, JSON.stringify({ ...JSON.parse(fs.readFileSync(globalPath, 'utf8')), ...patch }));
  write({ remoteRebindIntervalMs: 300, runners: [...base.runners, ...extraRunners], ...config });
  return {
    ...ctx,
    runnerRoots: [ctx.runnerRoot, ...extraRunners.map((r) => r.root)],
    setConfig: write,
    setMode: (mode) => fs.writeFileSync(control, JSON.stringify(mode)),
    admissionLog: () => fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'),
    execCommands: () => (fs.existsSync(execLog) ? fs.readFileSync(execLog, 'utf8').split('\n').filter(Boolean) : []),
  };
}

/** A ticket parked in the local queue because the only runner reports a queue and `maxRemoteQueue` was 0 when it was submitted. */
async function strandedTicket(ctx, ran) {
  ctx.setConfig({ maxRemoteQueue: 0 });
  ctx.setMode({ queued: 1 });
  const hold = await holdLaneLocally(ctx);
  const id = await detach(ctx, recordingCmd(ran));
  await waitFor(() => queuedIds(ctx.state).includes(id), { timeoutMs: 30_000 });
  return { hold, id };
}

test('queued-runner rebind: a ticket stuck locally past remoteRebindMinLocalWaitMs moves to a runner queued exactly maxRemoteQueue deep, and runs once there', { timeout: 120_000 }, async () => {
  const ctx = queuedRunnerCtx({ config: { remoteRebindMinLocalWaitMs: 2500 } });
  const ran = path.join(tmpDir('queued-ran'), 'ran');
  const { hold, id } = await strandedTicket(ctx, ran);
  const stranded = Date.now();
  ctx.setConfig({ maxRemoteQueue: 1 }); // queued 1 == the cap: allowed
  await sleep(1200);
  assert.deepEqual(queuedIds(ctx.state), [id], 'under the minimum local wait nothing moves');
  assert.doesNotMatch(ctx.admissionLog(), /remote-rebind/);
  const result = await waitResult(ctx, id);
  assert.ok(Date.now() - stranded >= 2000, 'not before the minimum local wait');
  assert.equal(result.executor, 'remote');
  assert.deepEqual(linesOf(ran), ['remote'], 'the command ran once, on the runner');
  assert.match(ctx.admissionLog(), new RegExp(`remote-rebind: ${id}: seq \\d+ -> skybox`));
  assert.deepEqual(readAttempt(ctx.state, id)?.dispatchedRunners ?? ['gone'], ['gone'], 'the attempt ended with the ticket');
  hold.release();
  await hold.done;
});

test('queued-runner rebind: never past maxRemoteQueue, and remoteRebindMinLocalWaitMs 0 disables the path', { timeout: 120_000 }, async () => {
  for (const config of [{ remoteRebindMinLocalWaitMs: 300, maxRemoteQueue: 0 }, { remoteRebindMinLocalWaitMs: 0, maxRemoteQueue: 2 }]) {
    const ctx = queuedRunnerCtx();
    const ran = path.join(tmpDir('queued-ran'), 'ran');
    const { hold, id } = await strandedTicket(ctx, ran);
    ctx.setConfig(config);
    await sleep(2500);
    assert.deepEqual(queuedIds(ctx.state), [id], JSON.stringify(config));
    assert.doesNotMatch(ctx.admissionLog(), /remote-rebind/);
    hold.release();
    await hold.done;
    assert.equal((await waitResult(ctx, id)).executor, 'local');
    assert.deepEqual(linesOf(ran), ['local']);
  }
});

test('queued-runner rebind: an idle runner still takes the ticket at once, whatever the local wait', { timeout: 120_000 }, async () => {
  const ctx = queuedRunnerCtx({ config: { remoteRebindMinLocalWaitMs: 600_000 } });
  const ran = path.join(tmpDir('queued-ran'), 'ran');
  const { hold, id } = await strandedTicket(ctx, ran);
  ctx.setMode({ queued: 0 });
  const result = await waitResult(ctx, id);
  assert.equal(result.executor, 'remote');
  assert.deepEqual(linesOf(ran), ['remote']);
  hold.release();
  await hold.done;
});

test('queued-runner rebind: capacity that frees during the probe wins the locked decision; the ticket runs locally and is not withdrawn', { timeout: 120_000 }, async () => {
  const ctx = queuedRunnerCtx({ config: { remoteRebindMinLocalWaitMs: 300 } });
  const ran = path.join(tmpDir('queued-ran'), 'ran');
  const { hold, id } = await strandedTicket(ctx, ran);
  const dir = tmpDir('queued-hold');
  const ready = path.join(dir, 'ready');
  const go = path.join(dir, 'go');
  ctx.setMode({ queued: 1, hold: { ready, go } });
  ctx.setConfig({ maxRemoteQueue: 2 });
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 60_000 });
  hold.release(); // the lane frees while the ticket's supervisor is inside the probe
  await hold.done;
  await waitFor(() => !hasLease(ctx.state), { timeoutMs: 30_000 });
  fs.writeFileSync(go, '');
  const result = await waitResult(ctx, id);
  assert.equal(result.executor, 'local');
  assert.deepEqual(linesOf(ran), ['local']);
  assert.match(ctx.admissionLog(), /local capacity freed during the probe/);
  assert.doesNotMatch(ctx.admissionLog(), new RegExp(`remote-rebind: ${id}: seq \\d+ ->`));
  assert.deepEqual(ctx.execCommands(), [], 'nothing was ever dispatched');
});

test('a runner that timed out the ticket is never revisited: A then B each time out, neither is dispatched twice, and the command runs once, locally', { timeout: 180_000 }, async () => {
  const ctx = queuedRunnerCtx({ runners: 2, remoteQueueTimeoutMs: 300 });
  fs.mkdirSync(ctx.runnerState, { recursive: true });
  fs.writeFileSync(path.join(ctx.runnerState, 'PAUSE'), 'the runner never starts anything');
  const hold = await holdLaneLocally(ctx);
  const ran = path.join(tmpDir('queued-ran'), 'ran');
  const id = await detach(ctx, recordingCmd(ran));
  await waitFor(() => (readAttempt(ctx.state, id)?.dispatchedRunners ?? []).length === 2, { timeoutMs: 90_000 });
  await waitFor(() => queuedIds(ctx.state).includes(id) && ctx.admissionLog().includes('stays in the local queue'), { timeoutMs: 60_000 });
  await sleep(2500); // many rebind intervals: nothing is left to try
  assert.deepEqual(readAttempt(ctx.state, id).dispatchedRunners, ['skybox', 'runner2']);
  const execs = ctx.execCommands();
  assert.equal(execs.length, 2, `two dispatches in total:\n${execs.join('\n')}`);
  assert.ok(ctx.runnerRoots.every((root) => execs.filter((c) => c.includes(root)).length === 1), 'one dispatch per runner');
  assert.deepEqual(linesOf(ran), []);
  hold.release();
  await hold.done;
  assert.equal((await waitResult(ctx, id)).executor, 'local');
  assert.deepEqual(linesOf(ran), ['local'], 'the command executed exactly once overall');
});

test('BRAIN-509: a localRefused ticket never falls back to the local queue, so the queued path has nothing to move', { timeout: 120_000 }, async () => {
  const ctx = queuedRunnerCtx({ config: { remoteRebindMinLocalWaitMs: 300, maxRemoteQueue: 2 } });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, localRefused: true } } });
  ctx.setMode({ queued: 1 });
  const ran = path.join(tmpDir('queued-ran'), 'ran');
  const id = await detach(ctx, recordingCmd(ran));
  const result = await waitResult(ctx, id);
  assert.equal(result.executor, 'remote');
  assert.deepEqual(linesOf(ran), ['remote']);
  assert.doesNotMatch(ctx.admissionLog(), /remote-rebind/);
});
