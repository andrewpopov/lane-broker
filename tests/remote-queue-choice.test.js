import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, laneRun, waitFor, writeRepoConfig } from './helpers.js';
import { spawn } from 'node:child_process';
import { tmpDir, makeRunner, setup, markerCmd, detachAndWait, resultOf } from './remote-harness.js';
import { selectRunner } from '../src/remote-client.js';
import { couldAdmitNow, enqueue } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfig, ConfigError } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, paths } from '../src/state.js';

/** A fake ssh that answers `remote-probe` per destination from `probes` (name -> payload). */
function makeProbeMapSsh(probes) {
  const binDir = tmpDir('queue-choice-ssh');
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

const probe = (queued, extra = {}) => ({ protocol: 1, protocols: [1, 2], version: '0.0.0', paused: false, queued, running: 0, capacity: {}, ...extra });
const runnersOf = (...names) => names.map((n) => makeRunner({ ssh: n }));

test('selectRunner: an idle runner is preferred over a less-queued earlier one (pass 1 unchanged)', async () => {
  const sshBin = makeProbeMapSsh({ a: probe(1), b: probe(0) });
  const res = await selectRunner(runnersOf('a', 'b'), { sshBin, deadlineMs: 3000, maxRemoteQueue: 2 });
  assert.equal(res.runner.name, 'b');
  assert.equal(res.queuedChoice, undefined);
});

test('selectRunner: all runners queued -> the least-queued one, marked queuedChoice', async () => {
  const sshBin = makeProbeMapSsh({ a: probe(2), b: probe(1), c: probe(2) });
  const res = await selectRunner(runnersOf('a', 'b', 'c'), { sshBin, deadlineMs: 3000, maxRemoteQueue: 2 });
  assert.equal(res.runner.name, 'b');
  assert.equal(res.queuedChoice, true);
  assert.equal(res.probe.queued, 1);
  assert.equal(res.skipped.length, 3, 'skipped still lists every queued runner');
});

test('selectRunner: a queue-depth tie goes to config order', async () => {
  const sshBin = makeProbeMapSsh({ a: probe(1), b: probe(1) });
  const res = await selectRunner(runnersOf('a', 'b'), { sshBin, deadlineMs: 3000, maxRemoteQueue: 2 });
  assert.equal(res.runner.name, 'a');
  assert.equal(res.queuedChoice, true);
});

test('selectRunner: queued above maxRemoteQueue -> no runner, reason names the cap', async () => {
  const sshBin = makeProbeMapSsh({ a: probe(3), b: probe(5) });
  const res = await selectRunner(runnersOf('a', 'b'), { sshBin, deadlineMs: 3000, maxRemoteQueue: 2 });
  assert.equal(res.runner, null);
  assert.equal(res.queuedChoice, undefined);
  assert.match(res.skipped[0].reason, /queued: 3 \(over maxRemoteQueue 2\)/);
});

test('selectRunner: maxRemoteQueue 0 (and unset) behaves exactly as before, reason unannotated', async () => {
  const sshBin = makeProbeMapSsh({ a: probe(1), b: probe(1) });
  for (const opts of [{ maxRemoteQueue: 0 }, {}]) {
    const res = await selectRunner(runnersOf('a', 'b'), { sshBin, deadlineMs: 3000, ...opts });
    assert.equal(res.runner, null);
    assert.deepEqual(res.skipped.map((s) => s.reason), ['queued: 1', 'queued: 1']);
  }
});

test('selectRunner: paused, protocol-2 and capacity skips still exclude a queued runner from queuing', async () => {
  const sshBin = makeProbeMapSsh({
    paused: probe(1, { paused: true }),
    v1: probe(1, { protocols: [1] }),
    small: probe(1, { capacity: { weight: 1 } }),
  });
  const res = await selectRunner(runnersOf('paused', 'v1', 'small'), {
    sshBin,
    deadlineMs: 3000,
    maxRemoteQueue: 2,
    requireProtocol2: true,
    reservation: { weight: 4, cpuCores: 4, memoryBytes: 1 },
  });
  assert.equal(res.runner, null);
  assert.match(res.skipped[0].reason, /paused/);
  assert.match(res.skipped[1].reason, /protocol 2/);
  assert.match(res.skipped[2].reason, /^queued: 1/);
});

test('selectRunner: a queued runner that can never fit the ticket is not a queuedChoice', async () => {
  const sshBin = makeProbeMapSsh({ small: probe(1, { capacity: { weight: 1 } }), big: probe(2, { capacity: { weight: 8 } }) });
  const res = await selectRunner(runnersOf('small', 'big'), {
    sshBin,
    deadlineMs: 3000,
    maxRemoteQueue: 2,
    reservation: { weight: 4, cpuCores: 4, memoryBytes: 1 },
  });
  assert.equal(res.runner.name, 'big');
  assert.equal(res.queuedChoice, true);
});

// ---- couldAdmitNow: the read-only local-admission seam ----

const ticket = { id: 't', key: 'r:default', weight: 1, resources: {}, conflicts: [] };
const cfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 2, schedulerMode: 'shadow' };
const holder = (id, key = 'other:key', weight = 1) =>
  ({ id, key, bootId: bootId(), supervisorPid: process.pid, weight, state: LEASE_STATE.RUNNING, admittedAt: 0 });

test('couldAdmitNow: idle broker admits', async () => {
  const { state } = freshEnv();
  assert.deepEqual(await couldAdmitNow(state, cfg, ticket, () => null), { admit: true, reason: 'ok' });
});

test('couldAdmitNow: denies on capacity, same-key conflict, pause and queue-ahead', async () => {
  const { state } = freshEnv();
  writeLease(state, holder('h1', 'other:key', 2));
  assert.equal((await couldAdmitNow(state, cfg, ticket, () => null)).reason, 'capacity');

  const s2 = freshEnv().state;
  writeLease(s2, holder('h2', ticket.key, 1));
  assert.equal((await couldAdmitNow(s2, cfg, ticket, () => null)).reason, 'conflict');

  const s3 = freshEnv().state;
  fs.writeFileSync(paths(s3).pause, 'maintenance');
  assert.equal((await couldAdmitNow(s3, cfg, ticket, () => null)).reason, 'paused');

  const s4 = freshEnv().state;
  await enqueue(s4, { ...ticket, id: 'ahead', supervisorPid: process.pid, supervisorStart: null });
  assert.equal((await couldAdmitNow(s4, cfg, ticket, () => null)).reason, 'queue-ahead');
});

test('couldAdmitNow: memory-critical pressure denies', async () => {
  const { state } = freshEnv();
  assert.equal((await couldAdmitNow(state, cfg, ticket, () => ({ availableBytes: 1e12, totalBytes: 1e12, macPressure: 'critical' }))).reason, 'memory-critical');
});

// Reaping: crash/reboot leftovers must not read as queue-ahead / conflict / capacity.

async function deadPid() {
  const child = spawn(process.execPath, ['-e', '0'], { stdio: 'ignore' });
  await new Promise((resolve) => child.on('exit', resolve));
  return child.pid;
}

test('couldAdmitNow: a queued record whose supervisor is dead is reaped, not counted as queue-ahead', async () => {
  const { state } = freshEnv();
  await enqueue(state, { ...ticket, id: 'ghost', supervisorPid: await deadPid(), supervisorStart: null });
  assert.deepEqual(await couldAdmitNow(state, cfg, ticket, () => null), { admit: true, reason: 'ok' });
  assert.equal(fs.readdirSync(paths(state).queue).length, 0, 'the dead record was dequeued');
});

test('couldAdmitNow: a lease whose supervisor and group are dead is reaped, not counted as conflict or capacity', async () => {
  const { state } = freshEnv();
  const dead = await deadPid();
  writeLease(state, { ...holder('dead-same-key', ticket.key), supervisorPid: dead, childPgid: dead });
  writeLease(state, { ...holder('dead-heavy', 'other:key', 2), supervisorPid: dead, childPgid: dead });
  assert.deepEqual(await couldAdmitNow(state, cfg, ticket, () => null), { admit: true, reason: 'ok' });
});

test('couldAdmitNow: a lease whose supervisor is dead but whose group is alive still blocks', async () => {
  const { state } = freshEnv();
  const child = spawn('sleep', ['30'], { detached: true, stdio: 'ignore' });
  try {
    writeLease(state, { ...holder('orphan', 'other:key', 2), supervisorPid: await deadPid(), childPgid: child.pid });
    assert.equal((await couldAdmitNow(state, cfg, ticket, () => null)).reason, 'capacity');
  } finally {
    process.kill(-child.pid, 'SIGKILL');
  }
});

// ---- maxRemoteQueue config ----

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

test('maxRemoteQueue defaults to 2, accepts 0, rejects negative and non-integers', () => {
  assert.equal(loadWith({}).maxRemoteQueue, 2);
  assert.equal(loadWith({ maxRemoteQueue: 0 }).maxRemoteQueue, 0);
  for (const bad of [-1, 1.5, '2']) assert.throws(() => loadWith({ maxRemoteQueue: bad }), ConfigError);
});

// ---- supervisor: end to end over the fake-ssh transport ----
// The fake runner's `remote-probe` reads the CLIENT's own broker state (see makeFakeSshBin), so a
// ticket queued locally shows up as `queued: 1` on the runner AND makes local admission say no.

test('supervisor: runner has a queue and local cannot admit -> the ticket is queued on the runner, recorded as queuedAt', async () => {
  const { env, state, repoDir } = setup();
  const globalPath = path.join(env.LANE_BROKER_HOME, 'config.json');
  const cfg = JSON.parse(fs.readFileSync(globalPath, 'utf8'));
  fs.writeFileSync(globalPath, JSON.stringify({ ...cfg, capacity: 1 }));
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true }, hold: { weight: 1 } } });

  const blocker = laneRun(['run', '--repo', 'r', '--lane', 'hold', '--', 'sleep', '4'], { env, cwd: repoDir });
  await waitFor(() => fs.existsSync(paths(state).leases) && fs.readdirSync(paths(state).leases).some((n) => n.endsWith('.json')), { timeoutMs: 60_000 });
  const queued = laneRun(['run', '--repo', 'r', '--lane', 'hold', '--', 'true'], { env, cwd: repoDir });
  await waitFor(() => fs.existsSync(paths(state).queue) && fs.readdirSync(paths(state).queue).length > 0, { timeoutMs: 60_000 });

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
  assert.equal(result.queuedAt, 'skybox(1)');
  await Promise.all([blocker, queued]);
});

test('supervisor: maxRemoteQueue 0 -> a queued runner is skipped and the ticket runs locally (today\'s behaviour)', async () => {
  const { env, state, repoDir } = setup();
  const globalPath = path.join(env.LANE_BROKER_HOME, 'config.json');
  const cfg = JSON.parse(fs.readFileSync(globalPath, 'utf8'));
  fs.writeFileSync(globalPath, JSON.stringify({ ...cfg, capacity: 1, maxRemoteQueue: 0 }));
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true }, hold: { weight: 1 } } });

  const blocker = laneRun(['run', '--repo', 'r', '--lane', 'hold', '--', 'sleep', '3'], { env, cwd: repoDir });
  await waitFor(() => fs.existsSync(paths(state).leases) && fs.readdirSync(paths(state).leases).some((n) => n.endsWith('.json')), { timeoutMs: 60_000 });
  const queued = laneRun(['run', '--repo', 'r', '--lane', 'hold', '--', 'true'], { env, cwd: repoDir });
  await waitFor(() => fs.existsSync(paths(state).queue) && fs.readdirSync(paths(state).queue).length > 0, { timeoutMs: 60_000 });

  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(
    ['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)],
    env,
    repoDir,
  );
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}`);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local');
  const result = resultOf(state, id);
  assert.equal(result.executor, 'local');
  assert.match(result.fallbackReason, /skybox: queued: 1/);
  assert.equal(result.queuedAt, undefined);
  await Promise.all([blocker, queued]);
});
