import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { CPU_LIMIT_CAPABILITY } from '../src/cpu-limit.js';
import { selectRunner } from '../src/remote-client.js';
import { selectionOptionsFor } from '../src/supervisor.js';
import { resolveTicketConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { readinessGap, readinessProbeArgs, parseReadinessArgs } from '../src/runner-readiness.js';
import { runCommand } from '../src/run.js';
import { CAPABILITIES, EXECUTION_CEILING_CAPABILITY, advertisedCapabilities } from '../src/capabilities.js';
import { runnerPrewarmCommand, pickPrewarmLane } from '../src/runner-prewarm.js';
import { atomicWriteJson } from '../src/state.js';
import { tmpDir, makeRunner } from './remote-harness.js';
import { freshEnv, writeRepoConfig, writeGlobalConfig, laneRun, gitFixture, GENEROUS_PROBE_DEADLINE_MS } from './helpers.js';

/** BRAIN-649: `requiresTags`/`runnerTags` (capability filter), `lane runner prewarm`. */

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

// ---- capability tags ----

test('requiresTags: a runner without the tag is not chosen; one declaring it is, whatever the config order', async () => {
  const sshBin = makeProbeSsh({ probes: { bare: {}, full: {} }, tags: { bare: [], full: ['browser'] } });
  const res = await selectRunner(runners('bare', 'full'), { sshBin, deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: { tags: ['browser'] } });
  assert.equal(res.runner.name, 'full');
  assert.match(res.skipped[0].reason, /missing tag browser/);
  assert.deepEqual(res.readiness, { bare: { check: 'tags', missing: ['browser'] } });
});

test('requiresTags: an older runner that answers no tags is not ready; a lane with no tags sends the probe unchanged', async () => {
  const sshBin = makeProbeSsh({ probes: { old: { old: true } } });
  const res = await selectRunner(runners('old'), { sshBin, deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: { tags: ['browser'] } });
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

test('config: requiresTags merges at repo and lane level, prewarm resolves, and bad shapes are config errors', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, requiresTags: ['a'], lanes: { train: { weight: 1, requiresTags: ['browser', 'a'], prewarm: true }, plain: { weight: 1 } } });
  const train = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'train' });
  assert.deepEqual(train.requiresTags, ['a', 'browser']);
  assert.equal(train.prewarm, true);
  const plain = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'plain' });
  assert.deepEqual([plain.requiresTags, plain.prewarm], [['a'], false]);
  for (const [lane, pattern] of [
    [{ requiresTags: 'browser' }, /requiresTags must be an array/],
    [{ prewarm: 1 }, /prewarm must be a boolean/],
  ]) {
    writeRepoConfig(repoDir, { version: 1, lanes: { train: { weight: 1, ...lane } } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'train' }), (err) => err instanceof ConfigError && pattern.test(err.message));
  }
});

// ---- avoidTags (BRAIN-1072) ----

const AVOID = { avoidTags: ['gpu'] };
const avoidFleet = (probes = {}) => makeProbeSsh({ probes: { wintop: {}, skybox: {}, ...probes }, tags: { wintop: ['gpu'], skybox: [] } });

test('avoidTags: while a runner without the tag is up, the avoided runner is never chosen, idle or not', async () => {
  const idle = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet(), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID });
  assert.equal(idle.runner.name, 'skybox', 'config order alone would pick wintop');
  assert.match(idle.skipped.find((s) => s.name === 'wintop').reason, /avoided tag gpu/);
  const queued = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet({ skybox: { queued: 3 } }), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID, maxRemoteQueue: 6 });
  assert.deepEqual([queued.runner.name, queued.queuedChoice], ['skybox', true], 'an idle avoided runner loses to a queued preferred one');
  const overCap = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet({ skybox: { queued: 9 } }), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID, maxRemoteQueue: 6 });
  assert.equal(overCap.runner, null, 'skybox is up, only over the cap: wintop is still not taken');
  assert.equal(overCap.skipped.find((s) => s.name === 'wintop' && /avoided/.test(s.reason)).candidate, undefined);
});

test('avoidTags: with no runner without the tag up, the avoided runner is the fallback', async () => {
  const down = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet(), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID, downRunners: { skybox: 'unreachable since 22:30' } });
  assert.equal(down.runner.name, 'wintop');
  assert.equal(down.skipped.some((s) => /avoided/.test(s.reason)), false);
  const paused = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet({ skybox: { paused: true } }), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID });
  assert.equal(paused.runner.name, 'wintop');
});

test('avoidTags: an observed-only runner (a rebalance source) counts as up by the same rule, and is never chosen', async () => {
  const queuedThere = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet({ skybox: { queued: 1 } }), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID, observeOnly: ['skybox'] });
  assert.equal(queuedThere.runner, null, 'skybox holds the ticket and is up: wintop stays out');
  const idleThere = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet(), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID, observeOnly: ['skybox'] });
  assert.equal(idleThere.runner, null, 'an observed runner is never the choice, idle or not');
  const pausedThere = await selectRunner(runners('wintop', 'skybox'), { sshBin: avoidFleet({ skybox: { queued: 1, paused: true } }), deadlineMs: GENEROUS_PROBE_DEADLINE_MS, readinessNeeds: AVOID, observeOnly: ['skybox'] });
  assert.equal(pausedThere.runner.name, 'wintop', 'a paused source is not up');
});

// A Sunday 13:00 PDT, and a skybox window that closes 300s later: a 600s run there cannot finish inside it.
const NOW = Date.parse('2026-10-11T20:00:00Z');
const CLOSING_SOON = { timezone: 'America/Los_Angeles', available: [{ days: ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'], from: '06:00', to: '13:05' }] };
const windowFleet = (closing) => [makeRunner({ ssh: 'wintop' }), { ...makeRunner({ ssh: 'skybox' }), ...(closing ? { schedule: CLOSING_SOON } : {}) }];
const WINDOW_OPTS = {
  deadlineMs: GENEROUS_PROBE_DEADLINE_MS,
  now: NOW,
  readinessNeeds: AVOID,
  reservation: { weight: 1, cpuCores: 4, memoryBytes: 1e9 },
  maxRemoteQueue: 6,
  finishModel: { estimateFor: (runner) => ({ ms: runner.name === 'skybox' ? 600_000 : 900_000, tier: 'repo-lane', n: 3 }) },
};
const ROOMY = { headroom: { cpuCores: 8, memoryBytes: 1e12 } };

test('avoidTags: a busy preferred runner whose window closes before the predicted finish is not up, so the avoided runner is chosen', async () => {
  const sshBin = avoidFleet({ wintop: ROOMY, skybox: { headroom: { cpuCores: 0, memoryBytes: 1e12 } } });
  const closing = await selectRunner(windowFleet(true), { ...WINDOW_OPTS, sshBin, busyQueueable: true });
  assert.equal(closing.runner?.name, 'wintop', JSON.stringify(closing.skipped));
  const open = await selectRunner(windowFleet(false), { ...WINDOW_OPTS, sshBin, busyQueueable: true });
  assert.equal(open.runner.name, 'skybox', 'with its window open the busy skybox keeps wintop out');
});

test('avoidTags: a preferred runner queued past maxRemoteQueue whose window closes before the predicted finish is not up', async () => {
  const sshBin = avoidFleet({ wintop: ROOMY, skybox: { queued: 9, ...ROOMY } });
  const closing = await selectRunner(windowFleet(true), { ...WINDOW_OPTS, sshBin });
  assert.equal(closing.runner?.name, 'wintop', JSON.stringify(closing.skipped));
  const open = await selectRunner(windowFleet(false), { ...WINDOW_OPTS, sshBin });
  assert.equal(open.runner, null, 'with its window open the over-cap skybox is up, so wintop is not taken');
});

test('avoidTags: asked with the requiresTags in one --tags flag, and never a readiness gap', () => {
  assert.deepEqual(readinessProbeArgs({ tags: ['browser'], avoidTags: ['gpu', 'browser'] }), ['--tags', 'browser,gpu']);
  assert.deepEqual(readinessProbeArgs({ tags: [], avoidTags: ['gpu'] }), ['--tags', 'gpu']);
  assert.equal(readinessGap({ tags: { browser: true, gpu: true } }, { tags: ['browser'], avoidTags: ['gpu'] }), null);
  assert.equal(readinessGap({}, { tags: [], avoidTags: ['gpu'] }), null, 'an older runner answering no tags is still ready');
  assert.deepEqual(parseReadinessArgs(['--tags', 'browser,gpu']).tags, ['browser', 'gpu']);
});

test('avoidTags: selection options carry the lane avoidTags, and ignoreAvoid (the refusal-deadline rescue) drops them', () => {
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, runners: [] };
  const enriched = { id: 'avoid', lane: 'train', repoId: 'r', weight: 1, resources: { cpuCores: 2 }, remote: { avoidTags: ['gpu'], requiresTags: ['browser'] } };
  const root = tmpDir('avoid-options');
  assert.deepEqual(selectionOptionsFor('rebalance', enriched, cfg, { root }).readinessNeeds.avoidTags, ['gpu']);
  const open = selectionOptionsFor('rebalance', enriched, cfg, { root, ignoreAvoid: true }).readinessNeeds;
  assert.deepEqual([open.avoidTags, open.tags], [[], ['browser']]);
});

test('config: avoidTags resolves per lane, is inherited by a template, and may not name a tag the lane requires', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, undeclaredLanes: { as: 'train' }, lanes: { train: { weight: 1, avoidTags: ['gpu', 'gpu'] }, plain: { weight: 1 } } });
  assert.deepEqual(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'train' }).avoidTags, ['gpu']);
  assert.deepEqual(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'plain' }).avoidTags, []);
  assert.deepEqual(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'adhoc' }).avoidTags, ['gpu']);
  for (const [cfg, pattern] of [
    [{ lanes: { train: { weight: 1, avoidTags: 'gpu' } } }, /avoidTags must be an array/],
    [{ lanes: { train: { weight: 1, requiresTags: ['gpu'], avoidTags: ['gpu'] } } }, /both requires and avoids tag gpu/],
    [{ requiresTags: ['gpu'], lanes: { train: { weight: 1, avoidTags: ['gpu'] } } }, /both requires and avoids tag gpu/],
  ]) {
    writeRepoConfig(repoDir, { version: 1, ...cfg });
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

test('prewarm: BRAIN-926 never sends remoteDepsCacheKeyFilesCoverInstallScripts, because it does not probe the runner', async () => {
  const cwd = prewarmRepo({ warm: { weight: 1, remoteSetup: SETUP, prewarm: true, remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles: ['gen-input.txt'], remoteDepsCacheKeyFilesCoverInstallScripts: true } });
  const calls = [];
  const dispatch = async (opts) => (calls.push(opts), { outcome: 'confirmed', exitCode: 0 });
  const res = await run([], { cwd, cfg: { runners: [{ name: 'a', ssh: 'a' }] }, dispatch });
  assert.equal(res.exitCode, 0, res.err.join('\n'));
  assert.deepEqual(calls[0].remoteDepsCacheKeyFiles, ['gen-input.txt']);
  assert.equal('remoteDepsCacheKeyFilesCoverInstallScripts' in calls[0], false);
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


test('execution ceiling is carried separately in local tickets and remote payloads without changing reservation', async () => {
  const local = setup({ cpuCores: 2, minCpuCores: 1, maxExecutionCpuCores: 1 });
  const capped = await submit({ env: local.env, cwd: local.repoDir });
  assert.equal(capped.result.exitCode, 0);
  assert.equal(capped.ticket.maxExecutionCpuCores, 1);
  assert.equal(capped.ticket.resources.cpuCores, 2);
  assert.equal(capped.ticket.resources.minCpuCores, 1);
  assert.equal(Object.hasOwn(capped.ticket.resources, 'maxExecutionCpuCores'), false);

  const remote = setup({ cpuCores: 2, maxExecutionCpuCores: 3, remote: true });
  writeGlobalConfig(remote.home, { runners: [makeRunner({ ssh: 'unused', root: tmpDir('execution-cap-runner') })] });
  const forwarded = await submit({ env: remote.env, cwd: remote.repoDir });
  assert.equal(forwarded.result.exitCode, 0);
  assert.equal(forwarded.ticket.maxExecutionCpuCores, 3);
  assert.equal(forwarded.ticket.remote.maxExecutionCpuCores, 3);
  assert.equal(forwarded.ticket.remote.cpuCores, 2);

  const ordinary = setup({ cpuCores: 2 });
  const absent = await submit({ env: ordinary.env, cwd: ordinary.repoDir });
  assert.equal(Object.hasOwn(absent.ticket, 'maxExecutionCpuCores'), false);
});

test('execution ceiling header override remains exact and invalid internal overrides fail before submission', async () => {
  const lane = setup({ cpuCores: 2, maxExecutionCpuCores: 1 });
  const overridden = await submit({ env: lane.env, cwd: lane.repoDir, maxExecutionCpuCoresOverride: 3 });
  assert.equal(overridden.ticket.maxExecutionCpuCores, 3);
  assert.equal(overridden.ticket.resources.cpuCores, 2);
  for (const maxExecutionCpuCoresOverride of [0, -1, 1.5, Infinity, null]) {
    const refused = await submit({ env: lane.env, cwd: lane.repoDir, maxExecutionCpuCoresOverride });
    assert.equal(refused.result.exitCode, 64);
    assert.equal(refused.ticket, null);
    assert.match(refused.stderr, /maxExecutionCpuCores must be a positive integer/);
  }
});

test('EXECUTION_CEILING_COMPATIBILITY: every placement path rejects old runners and missing quotas, while uncapped routing is unchanged', async () => {
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, runners: [] };
  const base = { id: 'ceiling', lane: 'release', repoId: 'r', weight: 1, resources: { cpuCores: 14.4 }, remote: {} };
  const sshBin = makeProbeSsh({ probes: {
    oldQuota: { capabilities: [CPU_LIMIT_CAPABILITY] },
    noQuota: { capabilities: [EXECUTION_CEILING_CAPABILITY] },
    supported: { capabilities: [CPU_LIMIT_CAPABILITY, EXECUTION_CEILING_CAPABILITY] },
  } });
  for (const kind of ['place', 'rebind', 'rebalance']) {
    const opts = selectionOptionsFor(kind, { ...base, maxExecutionCpuCores: 6 }, cfg, { root: tmpDir('cap-options') });
    const draining = selectionOptionsFor(kind, { ...base, maxExecutionCpuCores: 6 }, cfg, { root: tmpDir('draining-cap-options'), draining: true, stuckLocally: true });
    assert.ok(draining.requireCapabilities.includes(EXECUTION_CEILING_CAPABILITY), `${kind} draining`);
    assert.ok(draining.requireCapabilities.includes(CPU_LIMIT_CAPABILITY), `${kind} draining`);
    assert.ok(opts.requireCapabilities.includes(EXECUTION_CEILING_CAPABILITY), kind);
    assert.ok(opts.requireCapabilities.includes(CPU_LIMIT_CAPABILITY), kind);
    const selected = await selectRunner(runners('oldQuota', 'noQuota', 'supported'), { ...opts, sshBin });
    assert.equal(selected.runner.name, 'supported', kind);
    assert.match(selected.skipped.find((row) => row.name === 'oldQuota').reason, /execution-cpu-ceiling\/1/);
    assert.match(selected.skipped.find((row) => row.name === 'noQuota').reason, /cpu-limit\/1/);
    const none = await selectRunner(runners('oldQuota', 'noQuota'), { ...opts, sshBin });
    assert.equal(none.runner, null, kind);
    const plain = selectionOptionsFor(kind, base, cfg, { root: tmpDir('uncapped-options') });
    assert.deepEqual(plain.requireCapabilities, []);
    assert.equal((await selectRunner(runners('oldQuota'), { ...plain, sshBin })).runner.name, 'oldQuota');
  }
});

test('EXECUTION_CEILING_ADVERTISEMENT: ceiling support is advertised only with usable quota enforcement', () => {
  for (const quota of [{ mode: 'off', available: false }, { mode: 'off', available: true }, { mode: 'prefer', available: false }]) {
    const caps = advertisedCapabilities({}, quota);
    assert.equal(caps.includes(EXECUTION_CEILING_CAPABILITY), false);
    assert.equal(caps.includes(CPU_LIMIT_CAPABILITY), false);
  }
  const caps = advertisedCapabilities({}, { mode: 'prefer', available: true });
  assert.ok(caps.includes(EXECUTION_CEILING_CAPABILITY));
  assert.ok(caps.includes(CPU_LIMIT_CAPABILITY));
  assert.equal(CAPABILITIES.includes(EXECUTION_CEILING_CAPABILITY), false, 'never advertise unconditionally');
});
