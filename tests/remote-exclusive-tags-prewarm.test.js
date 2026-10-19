import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { selectRunner } from '../src/remote-client.js';
import { remoteSelectOptions, selectionOptionsFor } from '../src/supervisor.js';
import { resolveTicketConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { readinessGap, readinessProbeArgs, parseReadinessArgs } from '../src/runner-readiness.js';
import { runCommand } from '../src/run.js';
import { CAPABILITIES, REMOTE_EXCLUSIVE_CAPABILITY } from '../src/capabilities.js';
import { runnerPrewarmCommand, pickPrewarmLane } from '../src/runner-prewarm.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId, atomicWriteJson } from '../src/state.js';
import { tmpDir, makeRunner } from './remote-harness.js';
import { freshEnv, writeRepoConfig, writeGlobalConfig, laneRun, gitFixture } from './helpers.js';

/** BRAIN-649: `remoteExclusive` (a lane holds a whole runner), `requiresTags`/`runnerTags` (capability filter), `lane runner prewarm`. */

/** A fake ssh answering `remote-probe` per destination from `probes`; `tags` answers `--tags` like the real runner does, for names asked. */
function makeProbeSsh({ probes, tags = {} }) {
  const dir = tmpDir('brain649-ssh');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const probes = ${JSON.stringify(probes)};
const tags = ${JSON.stringify(tags)};
const argv = process.argv.slice(2);
const rest = [];
for (let i = 0; i < argv.length; i += 1) { if (argv[i] === '-o') { i += 1; continue; } rest.push(argv[i]); }
const dest = rest[0];
const m = /--tags ([^ ]+)/.exec((rest[1] || '').replace(/['\\\\]/g, ''));
const probe = { protocol: 1, protocols: [1, 2], paused: false, queued: 0, running: 0, capacity: {}, version: 't', capabilities: ${JSON.stringify(CAPABILITIES)}, ...probes[dest] };
if (m && !probe.old) probe.tags = Object.fromEntries(m[1].split(',').map((n) => [n, (tags[dest] || []).includes(n)]));
process.stdout.write(JSON.stringify(probe) + '\\n');
`,
  );
  fs.chmodSync(bin, 0o755);
  return bin;
}
const runners = (...names) => names.map((n) => makeRunner({ ssh: n }));

// ---- remoteExclusive: placement ----

test('preferIdle: an idle eligible runner is taken over busy and queued ones, whatever the config order', async () => {
  const sshBin = makeProbeSsh({ probes: { busy: { running: 1 }, queued: { queued: 1 }, idle: {} } });
  const res = await selectRunner(runners('busy', 'queued', 'idle'), { sshBin, deadlineMs: 3000, preferIdle: true, maxRemoteQueue: 0 });
  assert.equal(res.runner.name, 'idle');
  assert.equal(res.queuedChoice, undefined);
});

test('preferIdle: with none idle the least-loaded eligible runner is the queued choice, past maxRemoteQueue (not no-runner); control: an ordinary lane gets none', async () => {
  const sshBin = makeProbeSsh({ probes: { deep: { running: 2, queued: 9 }, busy: { running: 3, queued: 0 }, mid: { running: 1, queued: 4 } } });
  const res = await selectRunner(runners('deep', 'busy', 'mid'), { sshBin, deadlineMs: 3000, preferIdle: true, maxRemoteQueue: 2 });
  assert.equal(res.runner.name, 'busy');
  assert.equal(res.queuedChoice, true);
  assert.match(res.skipped.find((s) => s.name === 'busy').reason, /not idle \(running 3, queued 0\).*drain/);
  const tie = await selectRunner(runners('deep', 'mid', 'mid2'), { sshBin: makeProbeSsh({ probes: { deep: { queued: 4 }, mid: { queued: 4, headroom: { cpuCores: 1 } }, mid2: { queued: 4, headroom: { cpuCores: 8 } } } }), deadlineMs: 3000, preferIdle: true, reservation: { weight: 1, cpuCores: 1, memoryBytes: 0 } });
  assert.equal(tie.runner.name, 'mid2', 'equal queues: more free CPU wins');
  const control = await selectRunner(runners('deep', 'busy', 'mid'), { sshBin, deadlineMs: 3000, maxRemoteQueue: 2 });
  assert.equal(control.runner.name, 'busy');
  const controlDeep = await selectRunner(runners('deep'), { sshBin, deadlineMs: 3000, maxRemoteQueue: 2 });
  assert.equal(controlDeep.runner, null);
});

test('preferIdle: a runner missing the tag or the capability is never chosen, even when idle, and with none eligible there is no runner', async () => {
  const sshBin = makeProbeSsh({
    probes: { idleNoTag: {}, idleOld: { capabilities: ['exclusive/1'] }, loaded: { running: 2, queued: 3 } },
    tags: { loaded: ['browser'] },
  });
  const needs = { sshBin, deadlineMs: 3000, preferIdle: true, requireCapabilities: [REMOTE_EXCLUSIVE_CAPABILITY], readinessNeeds: { tags: ['browser'] } };
  const res = await selectRunner(runners('idleNoTag', 'idleOld', 'loaded'), needs);
  assert.equal(res.runner.name, 'loaded');
  assert.equal(res.queuedChoice, true);
  const none = await selectRunner(runners('idleNoTag', 'idleOld'), needs);
  assert.equal(none.runner, null);
  assert.equal(none.queuedChoice, undefined);
});

test('remoteSelectOptions: a remoteExclusive lane prefers an idle runner and requires remote-exclusive/1; rebind and rebalance never take a loaded one; an ordinary lane neither', () => {
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, runners: [] };
  const base = { id: 'x', lane: 'l', repoId: 'r', weight: 1, resources: {}, remote: {} };
  const exclusive = remoteSelectOptions({ ...base, remote: { remoteExclusive: true } }, cfg, tmpDir('opts'));
  assert.equal(exclusive.preferIdle, true);
  assert.ok(exclusive.requireCapabilities.includes(REMOTE_EXCLUSIVE_CAPABILITY));
  const exclusiveEnriched = { ...base, remote: { remoteExclusive: true } };
  for (const kind of ['rebind', 'rebalance']) assert.equal(selectionOptionsFor(kind, exclusiveEnriched, cfg, { root: tmpDir('opts') }).preferIdle, false);
  assert.equal(selectionOptionsFor('place', exclusiveEnriched, cfg, { root: tmpDir('opts') }).preferIdle, true);
  const plain = remoteSelectOptions(base, cfg, tmpDir('opts'));
  assert.equal(plain.preferIdle, false);
  assert.deepEqual(plain.requireCapabilities, []);
  assert.ok(CAPABILITIES.includes(REMOTE_EXCLUSIVE_CAPABILITY));
});

test('a runner that does not advertise remote-exclusive/1 (an older release, which would ignore the flag) is skipped', async () => {
  const sshBin = makeProbeSsh({ probes: { old: { capabilities: ['exclusive/1'] }, current: {} } });
  const res = await selectRunner(runners('old', 'current'), { sshBin, deadlineMs: 3000, preferIdle: true, requireCapabilities: [REMOTE_EXCLUSIVE_CAPABILITY] });
  assert.equal(res.runner.name, 'current');
  assert.match(res.skipped[0].reason, /does not advertise remote-exclusive\/1/);
});

// ---- remoteExclusive: runner-side admission ----

const GIB = 1024 ** 3;
async function submit({ env, cwd, ...options }) {
  let ticket = null;
  const spawnSupervisor = (execPath, args, opts) => {
    ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
    atomicWriteJson(ticket.resultPath, { id: ticket.id, exit: 0, signal: null, startedAt: Date.now(), endedAt: Date.now(), waitedMs: 0 });
    const silent = () => new PassThrough().end();
    return Object.assign(new EventEmitter(), { pid: process.pid, unref() {}, stdout: silent(), stderr: silent() });
  };
  const names = ['LANE_BROKER_HOME', 'LANE_BROKER_STATE', 'LANE_BROKER_LEASE', 'LANE_BROKER_KEY', 'LANE_BROKER_LOCAL', 'LANE_BROKER_PRIORITY'];
  const prev = Object.fromEntries(names.map((n) => [n, process.env[n]]));
  for (const n of names) delete process.env[n];
  process.env.LANE_BROKER_HOME = env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = env.LANE_BROKER_STATE;
  const origWrite = process.stderr.write.bind(process.stderr);
  let stderr = '';
  process.stderr.write = (chunk, ...rest) => {
    stderr += chunk;
    return origWrite(chunk, ...rest);
  };
  try {
    const result = await runCommand({ repo: 'r', lane: 'default', cmd: ['true'], cwd, spawnSupervisor, ...options });
    return { result, ticket, stderr };
  } finally {
    process.stderr.write = origWrite;
    for (const n of names) {
      if (prev[n] === undefined) delete process.env[n];
      else process.env[n] = prev[n];
    }
  }
}

function setup(lane) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane } } });
  gitFixture(['init', '-q'], repoDir);
  return { home, state, env, repoDir };
}

test('runner intake honours remoteExclusive (an exclusive, high ticket) but still ignores a plain exclusive; the submitter side is not made exclusive', async () => {
  const withRemote = setup({ remoteExclusive: true });
  const intake = await submit({ env: withRemote.env, cwd: withRemote.repoDir, configRoot: withRemote.repoDir });
  assert.equal(intake.ticket.exclusive, true);
  assert.equal(intake.ticket.priorityRequested, 'high');
  const submitterSide = await submit({ env: withRemote.env, cwd: withRemote.repoDir });
  assert.equal(Object.hasOwn(submitterSide.ticket, 'exclusive'), false, 'remoteExclusive does not make the local run exclusive');
  const plainExclusive = setup({ exclusive: true });
  const ignored = await submit({ env: plainExclusive.env, cwd: plainExclusive.repoDir, configRoot: plainExclusive.repoDir });
  assert.equal(Object.hasOwn(ignored.ticket, 'exclusive'), false);
});

test('the runner admits the intake ticket only when it holds nothing, then admits nothing else while it runs', async () => {
  const { state, env, repoDir } = setup({ remoteExclusive: true });
  const { ticket: submitted } = await submit({ env, cwd: repoDir, configRoot: repoDir });
  const ticket = { ...submitted, id: 'intake-excl', supervisorPid: process.pid, supervisorStart: null, logPath: '/dev/null', resultPath: '/dev/null' };
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, schedulerMode: 'shadow', capacity: 10, loadClose: 1000, loadOpen: 900, admissionCooldownMs: 0 };
  const lease = (id, extra = {}) => ({ id, key: `r:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING, ...extra });
  writeLease(state, lease('other'));
  await enqueue(state, ticket);
  assert.equal((await tryStart(state, ticket, cfg)).reason, 'exclusive-draining');
  removeLease(state, 'other');
  assert.equal((await tryStart(state, ticket, cfg)).started, true, 'control: it starts once the runner holds nothing');
  const newcomer = { ...ticket, id: 'newcomer', exclusive: undefined, priorityRequested: 'medium', key: 'r:newcomer' };
  await enqueue(state, newcomer);
  assert.equal((await tryStart(state, newcomer, cfg)).reason, 'exclusive-held');
});

// ---- capability tags ----

test('requiresTags: a runner without the tag is not chosen; one declaring it is, whatever the config order', async () => {
  const sshBin = makeProbeSsh({ probes: { bare: {}, full: {} }, tags: { bare: [], full: ['browser'] } });
  const res = await selectRunner(runners('bare', 'full'), { sshBin, deadlineMs: 3000, readinessNeeds: { tags: ['browser'] } });
  assert.equal(res.runner.name, 'full');
  assert.match(res.skipped[0].reason, /missing tag browser/);
  assert.deepEqual(res.readiness, { bare: { check: 'tags', missing: ['browser'] } });
});

test('requiresTags: an older runner that answers no tags is not ready; a lane with no tags sends the probe unchanged', async () => {
  const sshBin = makeProbeSsh({ probes: { old: { old: true } } });
  const res = await selectRunner(runners('old'), { sshBin, deadlineMs: 3000, readinessNeeds: { tags: ['browser'] } });
  assert.equal(res.runner, null);
  assert.match(res.skipped[0].reason, /does not report tags/);
  assert.deepEqual(readinessProbeArgs({ tags: [] }), []);
  assert.deepEqual(readinessProbeArgs({ tags: ['browser'] }), ['--tags', 'browser']);
  assert.deepEqual(parseReadinessArgs(['--tags', 'browser,gpu']).tags, ['browser', 'gpu']);
  assert.equal(readinessGap({ tags: { browser: true } }, { tags: ['browser'] }), null);
});

test('the real runner side: remote-probe --tags answers from the runner config runnerTags', async () => {
  const { home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, runnerTags: ['browser'] });
  const probe = JSON.parse((await laneRun(['remote-probe', '--tags', 'browser,gpu'], { env })).stdout);
  assert.deepEqual(probe.tags, { browser: true, gpu: false });
  const plain = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.equal('tags' in plain, false);
});

test('config: requiresTags merges at repo and lane level, remoteExclusive and prewarm resolve, and bad shapes are config errors', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, requiresTags: ['a'], lanes: { train: { weight: 1, requiresTags: ['browser', 'a'], remoteExclusive: true, prewarm: true }, plain: { weight: 1 } } });
  const train = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'train' });
  assert.deepEqual(train.requiresTags, ['a', 'browser']);
  assert.equal(train.remoteExclusive, true);
  assert.equal(train.prewarm, true);
  const plain = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'plain' });
  assert.deepEqual([plain.requiresTags, plain.remoteExclusive, plain.prewarm], [['a'], false, false]);
  for (const [lane, pattern] of [
    [{ requiresTags: 'browser' }, /requiresTags must be an array/],
    [{ remoteExclusive: 'yes' }, /remoteExclusive must be a boolean/],
    [{ prewarm: 1 }, /prewarm must be a boolean/],
  ]) {
    writeRepoConfig(repoDir, { version: 1, lanes: { train: { weight: 1, ...lane } } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'train' }), (err) => err instanceof ConfigError && pattern.test(err.message));
  }
});

// ---- prewarm ----

function prewarmRepo(lanes) {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes });
  gitFixture(['init', '-q'], repoDir);
  return repoDir;
}
const run = async (args, opts) => {
  const out = [];
  const err = [];
  const result = await runnerPrewarmCommand(args, { out: (t) => out.push(t), err: (t) => err.push(t), ...opts });
  return { ...result, out, err };
};
const SETUP = [['corepack', 'pnpm', 'art:pull']];

test('prewarm: sends the flagged lane to every online runner as `true` at low priority with its remoteSetup', async () => {
  const cwd = prewarmRepo({ train: { weight: 1, remoteSetup: SETUP }, warm: { weight: 1, remoteSetup: SETUP, prewarm: true } });
  const calls = [];
  const dispatch = async (opts) => (calls.push(opts), { outcome: 'confirmed', exitCode: 0 });
  const cfg = { runners: [{ name: 'a', ssh: 'a' }, { name: 'b', ssh: 'b', offline: true }, { name: 'c', ssh: 'c' }] };
  const res = await run([], { cwd, cfg, dispatch });
  assert.equal(res.exitCode, 0, res.err.join('\n'));
  assert.deepEqual(calls.map((c) => c.runner.name), ['a', 'c']);
  assert.deepEqual([calls[0].argv, calls[0].lane, calls[0].priorityRequested, calls[0].remoteSetup], [['true'], 'warm', 'low', SETUP]);
  assert.match(calls[0].ticketId, /^[0-9a-f-]{36}$/);
  assert.notEqual(calls[0].ticketId, calls[1].ticketId);
  assert.equal((await run(['--runner', 'c'], { cwd, cfg, dispatch })).out.length, 1);
});

test('prewarm: a failed warm-up exits 1 and names the runner; --dry-run dispatches nothing', async () => {
  const cwd = prewarmRepo({ warm: { weight: 1, remoteSetup: SETUP, prewarm: true } });
  const cfg = { runners: [{ name: 'a', ssh: 'a' }] };
  let calls = 0;
  const failing = await run([], { cwd, cfg, dispatch: async () => (calls++, { outcome: 'confirmed', exitCode: 1 }) });
  assert.equal(failing.exitCode, 1);
  assert.match(failing.out[0], /^a: not warm \(confirmed exit 1\)/);
  const dry = await run(['--dry-run'], { cwd, cfg, dispatch: async () => (calls++, { outcome: 'confirmed', exitCode: 0 }) });
  assert.equal(dry.exitCode, 0);
  assert.equal(calls, 1, 'only the failing run dispatched');
  assert.match(dry.out[0], /^a: would prewarm lane warm/);
});

test('prewarm: refuses without a flagged lane, with an ambiguous choice, or with nothing to warm', async () => {
  const cfg = { runners: [{ name: 'a', ssh: 'a' }] };
  const dispatch = async () => assert.fail('must not dispatch');
  const none = await run([], { cwd: prewarmRepo({ train: { weight: 1, remoteSetup: SETUP } }), cfg, dispatch });
  assert.deepEqual([none.exitCode, /no lane sets prewarm: true/.test(none.err[0])], [64, true]);
  const two = await run([], { cwd: prewarmRepo({ a: { weight: 1, prewarm: true, remoteSetup: SETUP }, b: { weight: 1, prewarm: true, remoteSetup: SETUP } }), cfg, dispatch });
  assert.match(two.err[0], /several lanes set prewarm: true \(a, b\); pass --lane/);
  const empty = await run([], { cwd: prewarmRepo({ a: { weight: 1, prewarm: true } }), cfg, dispatch });
  assert.match(empty.err[0], /neither remoteDeps nor remoteSetup/);
  assert.equal((await run(['--bogus'], { cfg })).exitCode, 2);
  assert.deepEqual(pickPrewarmLane({ fileLanes: { x: { prewarm: true } } }, 'y'), { error: 'lane "y" does not set prewarm: true' });
});
