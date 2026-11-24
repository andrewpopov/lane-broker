import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { setup, markerCmd, tmpDir, resultOf, detachAndWait } from './remote-harness.js';
import { reapSpawned, setupRunner, makeHeader, execRemote, readExecResult } from './helpers/remote-exec-fixture.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { paths } from '../src/state.js';

// PKG-402: every execution says where it ran, in the log and on the result, for local, push and pull alike.

afterEach(reapSpawned);

const MiB = 1024 * 1024;
const HOST = os.hostname();
const LINE = /^lane: executing on (\S+) via (local|push|pull) \(ticket (\S+), cpu (\S+), mem (\S+?)(?:, runner (\S+))?\)$/m;

const parsedLine = (text) => {
  const m = LINE.exec(text);
  return m && { host: m[1], transport: m[2], ticket: m[3], cpu: m[4], mem: m[5], runner: m[6] ?? null };
};

function localRepo() {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, memoryBytes: 256 * MiB } } });
  return { state, env, repoDir };
}

const localScenario = async (load) => {
  const { state, env, repoDir } = localRepo();
  const logPath = path.join(tmpDir('ran-on-log'), 'out.log');
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--cpu', '2', '--log', logPath, '--', 'node', '-e', 'console.log("hi")'], { env, cwd: repoDir, bin: load.laneBin });
  assert.equal(res.code, 0, res.stderr);
  const logged = parsedLine(fs.readFileSync(logPath, 'utf8'));
  assert.ok(logged, `the log names where the lane ran: ${fs.readFileSync(logPath, 'utf8')}`);
  assert.deepEqual(parsedLine(res.stderr), logged, 'the waiting caller sees the same line');
  assert.deepEqual([logged.host, logged.transport, logged.cpu, logged.mem, logged.runner], [HOST, 'local', '2', '256MB', null]);
  assert.ok(fs.readFileSync(logPath, 'utf8').indexOf('lane: executing on') < fs.readFileSync(logPath, 'utf8').indexOf('hi\n'), 'before the command output');
  const shown = await laneRun(['result', logged.ticket, '--json'], { env, bin: load.laneBin });
  assert.equal(shown.code, 0, shown.stderr);
  assert.deepEqual(JSON.parse(shown.stdout).ranOn, { host: HOST, transport: 'local', ticket: logged.ticket, cpuCores: 2, memoryBytes: 256 * MiB, runner: null });
  assert.deepEqual(resultOf(state, logged.ticket).ranOn, JSON.parse(shown.stdout).ranOn);
};

test('local: the log and the waiting caller get one executing-on line, and lane result --json carries the same fields', async () => localScenario(realSrc));

test('canary: without the supervisor writing the line to the log, the local scenario fails by name', async () => {
  await assertCanaryFails('supervisor.js', ['  logWriter.write(ranOnLine(ranOn));\n', ''], localScenario);
});

test('push: the runner supervisor names the runner host, push, the submitter ticket and runner; the submitter result relays it', { timeout: 180_000 }, async () => {
  const { env, state, repoDir } = setup({ cpuCores: 1 });
  const marker = path.join(tmpDir('marker'), 'where');
  const { id, waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], env, repoDir);
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  const log = fs.readFileSync(path.join(paths(state).logs, `${id}.log`), 'utf8');
  const logged = parsedLine(log);
  assert.ok(logged, `the submitter's log carries the runner's line: ${log}`);
  assert.deepEqual(logged, { host: HOST, transport: 'push', ticket: id, cpu: '1', mem: '1024MB', runner: 'skybox' }, 'memory is the default claim, 1 GiB per weight');
  assert.deepEqual(resultOf(state, id).ranOn, { host: HOST, transport: 'push', ticket: id, cpuCores: 1, memoryBytes: 1024 * MiB, runner: 'skybox' });
});

test('pull runner side: remote-exec --epoch records pull, the submitter ticket and the runner name in its line and published result', async () => {
  const ctx = setupRunner();
  const h = makeHeader('process.exit(0)', { cpuCores: 3, minCpuCores: 1, memoryBytes: 64 * MiB });
  const closed = await execRemote(ctx, h, { extraArgs: ['--epoch', '1', '--runner-name', 'wintop', '--claim-cpu-cores', '2'] }).closed;
  assert.equal(closed.code, 0, closed.stderr);
  assert.deepEqual(parsedLine(closed.stderr), { host: HOST, transport: 'pull', ticket: h.ticketId, cpu: '2', mem: '64MB', runner: 'wintop' });
  assert.deepEqual(readExecResult(ctx, `${h.ticketId}.e1`).ranOn, { host: HOST, transport: 'pull', ticket: h.ticketId, cpuCores: 2, memoryBytes: 64 * MiB, runner: 'wintop' });
});

const RUNNER_RAN_ON = { host: 'wintop', transport: 'pull', ticket: 't-1', cpuCores: 2, memoryBytes: null, runner: 'wintop' };

const publicationScenario = async (load) => {
  const { pullFinishedResult } = await load('pull-publication.js');
  const publication = { createdAt: 0, attemptStartedAt: 0, enriched: { id: 't-1' } };
  const settled = pullFinishedResult(publication, { result: { kind: 'completed', ranOn: RUNNER_RAN_ON }, decision: { exit: 0 }, host: 'wintop', acceptedAt: 10 }, null, {});
  assert.deepEqual(settled.ranOn, RUNNER_RAN_ON, 'a pull result relays where the runner ran it');
};

test('pull submitter side: the published result relays the runner ranOn', async () => publicationScenario(realSrc));

test('canary: without the pull publication relaying ranOn, the submitter scenario fails by name', async () => {
  await assertCanaryFails('pull-publication.js', ['    ...relayedRanOn(result),\n', ''], publicationScenario);
});

const relayScenario = async (load) => {
  const { relayedRanOn } = await load('ran-on.js');
  assert.deepEqual(relayedRanOn({ ranOn: { ...RUNNER_RAN_ON, cpuCores: 'lots', extra: 1 } }), { ranOn: { ...RUNNER_RAN_ON, cpuCores: null } });
  for (const bad of [undefined, {}, { ranOn: null }, { ranOn: { ...RUNNER_RAN_ON, transport: 'carrier-pigeon' } }, { ranOn: { ...RUNNER_RAN_ON, host: '' } }, { ranOn: { ...RUNNER_RAN_ON, ticket: 7 } }]) {
    assert.deepEqual(relayedRanOn(bad), {}, `a malformed runner record is dropped: ${JSON.stringify(bad)}`);
  }
};

test('a runner record is relayed only when well-formed, and a malformed figure reads null', async () => relayScenario(realSrc));

test('canary: without the well-formedness check, a malformed runner record is relayed and the relay scenario fails by name', async () => {
  await assertCanaryFails('ran-on.js', ['  if (!named(ranOn?.host) || !RAN_ON_TRANSPORTS.includes(ranOn.transport) || !named(ranOn.ticket)) return {};\n', '  if (!ranOn) return {};\n'], relayScenario);
});

test('the line spells an undeclared memory and cpu as unset and names the runner only when there is one', async () => {
  const { ranOnLine, ranOnRecord } = await realSrc('ran-on.js');
  assert.equal(ranOnLine(ranOnRecord({ host: 'h', transport: 'local', ticket: 't' })), 'lane: executing on h via local (ticket t, cpu unset, mem unset)\n');
  assert.equal(ranOnLine(ranOnRecord({ host: 'h', transport: 'push', ticket: 't', cpuCores: 1.5, memoryBytes: 2 * MiB, runner: 'r' })), 'lane: executing on h via push (ticket t, cpu 1.5, mem 2MB, runner r)\n');
});
