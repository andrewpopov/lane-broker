import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, waitFor, sleep } from './helpers.js';
import { tmpDir, makeRunner, setup, probeCount } from './remote-harness.js';
import { selectRunner } from '../src/remote-client.js';
import { enqueue, tryStart, listQueue, withdrawQueued, restoreQueued } from '../src/scheduler.js';
import { listLeases } from '../src/lease.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig, ConfigError } from '../src/config.js';
import { paths, readJsonSafe } from '../src/state.js';

/**
 * BRAIN-405: a remote-eligible ticket that fell back to the local queue is moved to a runner that later has real room,
 * and runners are ranked by real headroom and estimated finish.
 */

// ---- selectRunner: headroom and estimated finish (probe answered per destination) ----

function makeProbeMapSsh(probes) {
  const binDir = tmpDir('rebind-ssh');
  const sshBin = path.join(binDir, 'ssh');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
const probes = ${JSON.stringify(probes)};
const rest = [];
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i += 1) { if (argv[i] === '-o') { i += 1; continue; } rest.push(argv[i]); }
const p = probes[rest[0]];
if (!p) process.exit(255);
process.stdout.write(JSON.stringify(p) + '\\n');
`,
  );
  fs.chmodSync(sshBin, 0o755);
  return sshBin;
}

const probe = (extra = {}) => ({ protocol: 1, protocols: [1, 2], version: '0.0.0', paused: false, queued: 0, running: 0, capacity: {}, ...extra });
const room = (cpuCores, memoryBytes = 1e12) => ({ headroom: { cpuCores, memoryBytes } });
const reservation = { weight: 1, cpuCores: 4, memoryBytes: 1e9 };
const runner = (name, speedFactor) => ({ ...makeRunner({ ssh: name }), ...(speedFactor === undefined ? {} : { speedFactor }) });

test('selectRunner: of two runners with room, the earlier estimated finish wins, not the first in config order', async () => {
  const sshBin = makeProbeMapSsh({ slow: probe(room(8)), fast: probe(room(8)) });
  const res = await selectRunner([runner('slow', 1), runner('fast', 4)], { sshBin, deadlineMs: 3000, reservation });
  assert.equal(res.runner.name, 'fast');
});

test('selectRunner: equal estimated finish -> more free CPU, then config order', async () => {
  const sshBin = makeProbeMapSsh({ a: probe(room(6)), b: probe(room(12)), c: probe(room(12)) });
  const res = await selectRunner([runner('a'), runner('b'), runner('c')], { sshBin, deadlineMs: 3000, reservation });
  assert.equal(res.runner.name, 'b');
});

test('selectRunner: a runner without real CPU headroom is not chosen even though its queue is empty', async () => {
  const sshBin = makeProbeMapSsh({ busy: probe(room(1.5)), free: probe(room(8)) });
  const res = await selectRunner([runner('busy', 10), runner('free', 1)], { sshBin, deadlineMs: 3000, reservation });
  assert.equal(res.runner.name, 'free', 'busy is faster on paper but has 1.5 free cores for a 4-core ticket');
  assert.match(res.skipped.find((s) => s.name === 'busy').reason, /no CPU headroom/);
});

test('selectRunner: the only runner lacks headroom -> no runner, so the ticket stays local', async () => {
  const sshBin = makeProbeMapSsh({ busy: probe(room(0)) });
  const res = await selectRunner([runner('busy')], { sshBin, deadlineMs: 3000, reservation });
  assert.equal(res.runner, null);
});

test('selectRunner: headroom falls back to CPU budget minus reserved when the probe has no measurement', async () => {
  const sshBin = makeProbeMapSsh({ full: probe({ capacity: { cpuCores: 8 }, reservedCpuCores: 6 }), open: probe({ capacity: { cpuCores: 8 }, reservedCpuCores: 2 }) });
  const res = await selectRunner([runner('full'), runner('open')], { sshBin, deadlineMs: 3000, reservation });
  assert.equal(res.runner.name, 'open');
});

test('selectRunner: not enough available memory is not room', async () => {
  const sshBin = makeProbeMapSsh({ tight: probe(room(8, 1e6)), roomy: probe(room(8, 1e12)) });
  const res = await selectRunner([runner('tight'), runner('roomy')], { sshBin, deadlineMs: 3000, reservation });
  assert.equal(res.runner.name, 'roomy');
});

test('selectRunner: an unmeasured runner (older probe) still counts as having room', async () => {
  const sshBin = makeProbeMapSsh({ old: probe() });
  const res = await selectRunner([runner('old')], { sshBin, deadlineMs: 3000, reservation });
  assert.equal(res.runner.name, 'old');
});

test('selectRunner: a runner with room is preferred over any runner queue', async () => {
  const sshBin = makeProbeMapSsh({ queued: probe({ queued: 1, ...room(64) }), idle: probe(room(8)) });
  const res = await selectRunner([runner('queued', 100), runner('idle')], { sshBin, deadlineMs: 3000, maxRemoteQueue: 2, reservation });
  assert.equal(res.runner.name, 'idle');
  assert.equal(res.queuedChoice, undefined);
});

// ---- config ----

function loadWith(extra) {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 15, loadOpen: 11, loadOpenSamples: 3, sampleMs: 5000, ...extra });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    return loadGlobalConfig();
  } finally {
    process.env.LANE_BROKER_HOME = prev;
  }
}

test('remoteRebindIntervalMs defaults to 30000, accepts 0, rejects negative and non-integers', () => {
  assert.equal(loadWith({}).remoteRebindIntervalMs, 30_000);
  assert.equal(loadWith({ remoteRebindIntervalMs: 0 }).remoteRebindIntervalMs, 0);
  for (const bad of [-1, 1.5, '30000']) assert.throws(() => loadWith({ remoteRebindIntervalMs: bad }), ConfigError);
});

test('runner speedFactor must be a positive number', () => {
  const runners = (speedFactor) => [{ name: 'a', ssh: 'a', speedFactor }];
  assert.equal(loadWith({ runners: runners(2.5) }).runners[0].speedFactor, 2.5);
  for (const bad of [0, -1, '2', null]) assert.throws(() => loadWith({ runners: runners(bad) }), ConfigError);
});

// ---- the atomic withdraw (scheduler) ----

const cfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 4, schedulerMode: 'shadow' };
const ticketOf = (id, key = `r:${id}`) => ({ id, key, weight: 1, resources: {}, conflicts: [], supervisorPid: process.pid, supervisorStart: null });
const queuedIds = (state) => listQueue(state).map((t) => t.id);
const queuedSeqs = (state) => Object.fromEntries(listQueue(state).map((t) => [t.id, t.seq]));

test('withdraw: a ticket that is still queued is returned with its seq and leaves the queue', async () => {
  const { state } = freshEnv();
  const record = await enqueue(state, ticketOf('a'));
  const taken = await withdrawQueued(state, 'a');
  assert.equal(taken.id, 'a');
  assert.equal(taken.seq, record.seq);
  assert.deepEqual(queuedIds(state), []);
});

test('withdraw: a ticket the scheduler already started is not withdrawn', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  const started = await tryStart(state, ticketOf('a'), cfg);
  assert.equal(started.started, true);
  assert.equal(await withdrawQueued(state, 'a'), null);
  assert.equal(listLeases(state).filter((l) => l.id === 'a').length, 1, 'the lease is untouched');
});

test('withdraw: a ticket that was withdrawn is not started by the scheduler', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  assert.ok(await withdrawQueued(state, 'a'));
  const started = await tryStart(state, ticketOf('a'), cfg);
  assert.equal(started.started, false);
  assert.equal(started.position ?? null, null, 'no longer in the queue');
  assert.equal(listLeases(state).length, 0);
});

test('withdraw: a cancelled ticket is left for its own cancel path', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  fs.mkdirSync(paths(state).cancel, { recursive: true });
  fs.writeFileSync(path.join(paths(state).cancel, 'a'), '');
  assert.equal(await withdrawQueued(state, 'a'), null);
  assert.deepEqual(queuedIds(state), ['a']);
});

test('withdraw racing the scheduler admitting the same ticket: exactly one wins, never both, never neither', async () => {
  const outcomes = { started: 0, withdrawn: 0 };
  for (let round = 0; round < 25; round += 1) {
    const { state } = freshEnv();
    await enqueue(state, ticketOf('a'));
    const [started, withdrawn] = await Promise.all([tryStart(state, ticketOf('a'), cfg), withdrawQueued(state, 'a')]);
    const startedWon = started.started === true;
    const withdrawnWon = withdrawn !== null;
    assert.notEqual(startedWon, withdrawnWon, `round ${round}: started=${startedWon} withdrawn=${withdrawnWon}`);
    assert.deepEqual(queuedIds(state), [], `round ${round}: nobody leaves it queued`);
    assert.equal(listLeases(state).length, startedWon ? 1 : 0, `round ${round}: a lease exists iff the scheduler won`);
    outcomes[startedWon ? 'started' : 'withdrawn'] += 1;
  }
  assert.equal(outcomes.started + outcomes.withdrawn, 25);
});

test('restore: a withdrawn ticket goes back at its ORIGINAL position, ahead of tickets queued after it', async () => {
  const { state } = freshEnv();
  await enqueue(state, ticketOf('a'));
  await enqueue(state, ticketOf('b'));
  await enqueue(state, ticketOf('c'));
  const taken = await withdrawQueued(state, 'b');
  await enqueue(state, ticketOf('d'));
  await restoreQueued(state, taken);
  assert.deepEqual(queuedIds(state), ['a', 'b', 'c', 'd']);
});

// ---- end to end over the fake-ssh transport ----

/**
 * Wrap the harness's fake ssh in a switch driven by a control file, so one test can change what the runner looks like while
 * a ticket sits in the queue: `down` (unreachable), `full` (reachable, no CPU headroom), `ok`, or `fail-exec` (probe fine,
 * every remote-exec dies, so a dispatch is refused).
 */
function controllableRunner() {
  const ctx = setup({ ssh: 'normal' });
  const binDir = ctx.env.PATH.split(path.delimiter)[0];
  const control = path.join(tmpDir('rebind-control'), 'mode');
  fs.writeFileSync(control, 'down');
  fs.renameSync(path.join(binDir, 'ssh'), path.join(binDir, 'ssh-real'));
  fs.writeFileSync(
    path.join(binDir, 'ssh'),
    `#!/usr/bin/env node
const fs = require('fs');
const { spawn, spawnSync } = require('child_process');
const mode = fs.readFileSync(${JSON.stringify(control)}, 'utf8').trim();
const real = ${JSON.stringify(path.join(binDir, 'ssh-real'))};
const argv = process.argv.slice(2);
const command = argv[argv.length - 1] || '';
if (mode === 'down') process.exit(255);
if (command.includes('remote-probe')) {
  const res = spawnSync(real, argv, { encoding: 'utf8' });
  const p = JSON.parse(res.stdout.trim());
  // the fake runner shares the client's broker state, so it would report the client's own queued tickets as its own
  p.queued = 0;
  p.running = 0;
  p.headroom = { cpuCores: mode === 'full' ? 0 : 64, memoryBytes: 1e12 };
  process.stdout.write(JSON.stringify(p) + '\\n');
  process.exit(res.status == null ? 1 : res.status);
}
if (mode === 'fail-exec' && command.includes('remote-exec')) process.exit(255);
const child = spawn(real, argv, { stdio: 'inherit' });
child.on('close', (code) => process.exit(code == null ? 1 : code));
`,
  );
  fs.chmodSync(path.join(binDir, 'ssh'), 0o755);
  const globalPath = path.join(ctx.home, 'config.json');
  const base = JSON.parse(fs.readFileSync(globalPath, 'utf8'));
  fs.writeFileSync(globalPath, JSON.stringify({ ...base, remoteRebindIntervalMs: 300 }));
  return { ...ctx, setMode: (mode) => fs.writeFileSync(control, mode), admissionLog: () => fs.readFileSync(paths(ctx.state).admissionLog, 'utf8').toString() };
}

/** `lane run` of a command that appends where it ran to `file` (a line per execution), and holds until `release` exists if given. */
const recordingCmd = (file, release = null) => [
  process.execPath,
  '-e',
  `const fs = require('fs');
fs.appendFileSync(${JSON.stringify(file)}, (process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local') + '\\n');
${release ? `const t = setInterval(() => { if (fs.existsSync(${JSON.stringify(release)})) { clearInterval(t); process.exit(0); } }, 50);` : ''}`,
];

const linesOf = (file) => (fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split('\n').filter(Boolean) : []);
const hasLease = (state) => fs.existsSync(paths(state).leases) && fs.readdirSync(paths(state).leases).some((n) => n.endsWith('.json'));

/** A local-only holder of the lane's key, so every later ticket on that lane waits in the local queue. */
async function holdLaneLocally(ctx) {
  const dir = tmpDir('rebind-hold');
  const release = path.join(dir, 'release');
  const holder = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--local', '--', ...recordingCmd(path.join(dir, 'ran'), release)], { env: ctx.env, cwd: ctx.repoDir });
  await waitFor(() => hasLease(ctx.state), { timeoutMs: 60_000 });
  return { release: () => fs.writeFileSync(release, ''), holder, done: new Promise((resolve) => holder.on('close', resolve)) };
}

const detach = async (ctx, cmd, { env = ctx.env, extra = [] } = {}) => {
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', ...extra, '--', ...cmd], { env, cwd: ctx.repoDir });
  assert.equal(started.code, 0, started.stderr);
  return started.stdout.trim();
};
const waitResult = async (ctx, id) => {
  const waited = await laneRun(['wait', id, '--timeout', '60s'], { env: ctx.env });
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}\nadmission log:\n${fs.readFileSync(paths(ctx.state).admissionLog, 'utf8')}`);
  return readJsonSafe(path.join(paths(ctx.state).results, `${id}.json`));
};

test('a ticket that fell back to the local queue is rebound once a runner frees up, and runs remotely exactly once', async () => {
  const ctx = controllableRunner();
  const hold = await holdLaneLocally(ctx);
  const ran = path.join(tmpDir('rebind-ran'), 'ran');
  const id = await detach(ctx, recordingCmd(ran));
  await waitFor(() => queuedIds(ctx.state).includes(id), { timeoutMs: 30_000 });
  assert.match(listQueue(ctx.state).find((t) => t.id === id).remote.fallback.reason, /./, 'the fallback keeps the ticket eligible and says why');

  ctx.setMode('ok');
  const result = await waitResult(ctx, id);
  assert.equal(result.executor, 'remote');
  assert.equal(result.runner, 'skybox');
  assert.deepEqual(linesOf(ran), ['remote'], 'the command ran once, on the runner');
  assert.match(ctx.admissionLog(), new RegExp(`remote-rebind: ${id}: seq \\d+ -> skybox`));
  hold.release();
  await hold.done;
});

test('a runner with no CPU headroom does not take the ticket; it waits locally until one has room', async () => {
  const ctx = controllableRunner();
  const hold = await holdLaneLocally(ctx);
  ctx.setMode('full');
  const ran = path.join(tmpDir('rebind-ran'), 'ran');
  const id = await detach(ctx, recordingCmd(ran));
  await waitFor(() => queuedIds(ctx.state).includes(id), { timeoutMs: 30_000 });
  const probesBefore = probeCount(ctx.probeLogPath);
  await waitFor(() => probeCount(ctx.probeLogPath) >= probesBefore + 3, { timeoutMs: 30_000 });
  assert.deepEqual(queuedIds(ctx.state), [id], 'still queued locally despite repeated re-probes');
  assert.deepEqual(linesOf(ran), []);
  ctx.setMode('ok');
  const result = await waitResult(ctx, id);
  assert.equal(result.executor, 'remote');
  hold.release();
  await hold.done;
});

test('a refused rebind keeps the ticket\'s original queue position', async () => {
  const ctx = controllableRunner();
  const hold = await holdLaneLocally(ctx);
  const ran = path.join(tmpDir('rebind-ran'), 'ran');
  const first = await detach(ctx, recordingCmd(ran));
  await waitFor(() => queuedIds(ctx.state).includes(first), { timeoutMs: 30_000 });
  const second = await detach(ctx, recordingCmd(ran));
  await waitFor(() => queuedIds(ctx.state).length === 2, { timeoutMs: 30_000 });
  const original = queuedIds(ctx.state);
  assert.deepEqual(original, [first, second]);
  const originalSeqs = queuedSeqs(ctx.state);

  ctx.setMode('fail-exec');
  await waitFor(() => (ctx.admissionLog().match(/remote-rebind: .* stays in the local queue at seq/g) ?? []).length >= 2, { timeoutMs: 60_000 });
  // every restore puts a ticket back at its own seq; sample until both are back, then compare the order
  await waitFor(() => queuedIds(ctx.state).length === 2, { timeoutMs: 30_000 });
  assert.deepEqual(queuedIds(ctx.state), original, 'the first ticket is still ahead of the second after being refused');
  assert.deepEqual(queuedSeqs(ctx.state), originalSeqs, 'each ticket kept its own sequence number');

  ctx.setMode('down');
  hold.release();
  await hold.done;
  assert.equal((await waitResult(ctx, first)).executor, 'local');
  assert.equal((await waitResult(ctx, second)).executor, 'local');
  assert.deepEqual(linesOf(ran).slice(0, 2), ['local', 'local']);
});

test('--local and LANE_BROKER_LOCAL=1 tickets are never rebound', async () => {
  const ctx = controllableRunner();
  const hold = await holdLaneLocally(ctx);
  ctx.setMode('ok');
  const probesBefore = probeCount(ctx.probeLogPath);
  const flagged = await detach(ctx, recordingCmd(path.join(tmpDir('rebind-ran'), 'ran')), { extra: ['--local'] });
  const enved = await detach(ctx, recordingCmd(path.join(tmpDir('rebind-ran'), 'ran')), { env: { ...ctx.env, LANE_BROKER_LOCAL: '1' } });
  await waitFor(() => queuedIds(ctx.state).length === 2, { timeoutMs: 30_000 });
  await sleep(1500); // five rebind intervals
  assert.deepEqual(queuedIds(ctx.state), [flagged, enved], 'both still queued locally');
  assert.equal(probeCount(ctx.probeLogPath), probesBefore, 'no runner was ever probed for them');
  assert.doesNotMatch(ctx.admissionLog(), /remote-rebind/);
  hold.release();
  await hold.done;
  for (const id of [flagged, enved]) assert.notEqual((await waitResult(ctx, id)).executor, 'remote');
});

test('a running ticket is never rebound', async () => {
  const ctx = controllableRunner(); // runner is down, so the first ticket falls back and starts locally at once
  const dir = tmpDir('rebind-run');
  const release = path.join(dir, 'release');
  const ran = path.join(dir, 'ran');
  const id = await detach(ctx, recordingCmd(ran, release));
  await waitFor(() => linesOf(ran).length === 1 && hasLease(ctx.state), { timeoutMs: 60_000 });
  ctx.setMode('ok');
  const probesBefore = probeCount(ctx.probeLogPath);
  await sleep(1500);
  assert.equal(probeCount(ctx.probeLogPath), probesBefore, 'a running ticket does not look for a runner');
  assert.doesNotMatch(ctx.admissionLog(), /remote-rebind/);
  fs.writeFileSync(release, '');
  const result = await waitResult(ctx, id);
  assert.equal(result.executor, 'local');
  assert.deepEqual(linesOf(ran), ['local']);
});

test('remoteRebindIntervalMs 0 disables rebinding', async () => {
  const ctx = controllableRunner();
  const globalPath = path.join(ctx.home, 'config.json');
  fs.writeFileSync(globalPath, JSON.stringify({ ...JSON.parse(fs.readFileSync(globalPath, 'utf8')), remoteRebindIntervalMs: 0 }));
  const hold = await holdLaneLocally(ctx);
  const id = await detach(ctx, recordingCmd(path.join(tmpDir('rebind-ran'), 'ran')));
  await waitFor(() => queuedIds(ctx.state).includes(id), { timeoutMs: 30_000 });
  ctx.setMode('ok');
  const probesBefore = probeCount(ctx.probeLogPath);
  await sleep(1500);
  assert.equal(probeCount(ctx.probeLogPath), probesBefore);
  assert.deepEqual(queuedIds(ctx.state), [id]);
  hold.release();
  await hold.done;
  assert.equal((await waitResult(ctx, id)).executor, 'local');
});
