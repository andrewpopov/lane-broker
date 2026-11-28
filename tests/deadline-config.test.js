import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { freshEnv, writeGlobalConfig, writeRepoConfig, gitFixture } from './helpers.js';
import { resolveTicketConfig, loadGlobalConfig, ConfigError, DEFAULT_GLOBAL_CONFIG, resolveDeadline } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { atomicWriteJson, bootId } from '../src/state.js';
import { runCommand } from '../src/run.js';

/** BRAIN-940 R1: the run deadline's config, CLI and resolution. Validation and transport only; nothing enforces. */

const BASE_GLOBAL = { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 };

function withHome(globalExtra, fn) {
  const ctx = freshEnv();
  writeGlobalConfig(ctx.home, { ...BASE_GLOBAL, ...globalExtra });
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = ctx.home;
  try {
    return fn(ctx);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prev;
  }
}

test('global deadlineMs defaults to 6 h and deadlineEnforcement to audit', () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.deadlineMs, 21_600_000);
  assert.equal(DEFAULT_GLOBAL_CONFIG.deadlineEnforcement, 'audit');
  withHome({}, () => assert.deepEqual([loadGlobalConfig().deadlineMs, loadGlobalConfig().deadlineEnforcement], [21_600_000, 'audit']));
});

test('global deadlineMs accepts 60000..86400000 and refuses anything else, 0 and null included', () => {
  for (const ok of [60_000, 86_400_000]) withHome({ deadlineMs: ok }, () => assert.equal(loadGlobalConfig().deadlineMs, ok));
  for (const bad of [59_999, 86_400_001, 0, null, 1.5, '6h', -1]) {
    withHome({ deadlineMs: bad }, () => assert.throws(() => loadGlobalConfig(), (err) => err instanceof ConfigError && /"deadlineMs" must be an integer number of milliseconds from 60000 to 86400000/.test(err.message), String(bad)));
  }
});

test('global deadlineEnforcement is audit or enforce', () => {
  withHome({ deadlineEnforcement: 'enforce' }, () => assert.equal(loadGlobalConfig().deadlineEnforcement, 'enforce'));
  for (const bad of ['on', '', null, true]) withHome({ deadlineEnforcement: bad }, () => assert.throws(() => loadGlobalConfig(), ConfigError, String(bad)));
});

function repoWith(lanes, extra = {}) {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes, ...extra });
  return repoDir;
}

test('lane deadlineMs: absent falls through (null), in range loads, out of range and explicit null/0 are refused', () => {
  const resolve = (lane) => resolveTicketConfig({ cwd: repoWith({ default: { weight: 1, ...lane } }), repo: 'r', lane: 'default' });
  assert.equal(resolve({}).deadlineMs, null);
  for (const ok of [60_000, 86_400_000]) assert.equal(resolve({ deadlineMs: ok }).deadlineMs, ok);
  for (const bad of [59_999, 86_400_001, 0, null, 2.5, '1h']) {
    assert.throws(() => resolve({ deadlineMs: bad }), (err) => err instanceof ConfigError && /lane "default"\.deadlineMs must be an integer number of milliseconds from 60000 to 86400000/.test(err.message), String(bad));
  }
});

test('an undeclaredLanes template passes its deadlineMs to the ad-hoc lane', () => {
  const repoDir = repoWith({ tpl: { weight: 1, deadlineMs: 900_000 }, other: { weight: 1 } }, { undeclaredLanes: { as: 'tpl' } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'adhoc' }).deadlineMs, 900_000);
});

test('resolveDeadline: CLI over lane over global, each with its source', () => {
  assert.deepEqual(resolveDeadline({ cli: 1, lane: 2, global: 3 }), { deadlineMs: 1, deadlineSource: 'cli' });
  assert.deepEqual(resolveDeadline({ cli: null, lane: 2, global: 3 }), { deadlineMs: 2, deadlineSource: 'lane' });
  assert.deepEqual(resolveDeadline({ cli: null, lane: null, global: 3 }), { deadlineMs: 3, deadlineSource: 'global' });
});

function setup({ lane = {}, global = {} } = {}) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { ...BASE_GLOBAL, ...global });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane } } });
  gitFixture(['init', '-q'], repoDir);
  return { home, state, env, repoDir };
}

const ENV_NAMES = ['LANE_BROKER_HOME', 'LANE_BROKER_STATE', 'LANE_BROKER_LEASE', 'LANE_BROKER_KEY', 'LANE_BROKER_LOCAL', 'LANE_BROKER_PRIORITY'];

async function submit(ctx, options, { inherit } = {}) {
  let ticket = null;
  const spawnSupervisor = (execPath, args, opts) => {
    ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
    atomicWriteJson(ticket.resultPath, { id: ticket.id, exit: 0, signal: null, startedAt: Date.now(), endedAt: Date.now(), waitedMs: 0 });
    const silent = () => new PassThrough().end();
    return Object.assign(new EventEmitter(), { pid: process.pid, unref() {}, stdout: silent(), stderr: silent() });
  };
  const prev = Object.fromEntries(ENV_NAMES.map((n) => [n, process.env[n]]));
  for (const n of ENV_NAMES) delete process.env[n];
  Object.assign(process.env, { LANE_BROKER_HOME: ctx.env.LANE_BROKER_HOME, LANE_BROKER_STATE: ctx.env.LANE_BROKER_STATE });
  if (inherit) {
    const key = resolveTicketConfig({ cwd: ctx.repoDir, repo: 'r', lane: 'default' }).key;
    writeLease(ctx.state, { id: 'parent', key, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING, resources: { cpuCores: 2, memoryBytes: 1024 ** 3 } });
    Object.assign(process.env, { LANE_BROKER_LEASE: 'parent', LANE_BROKER_KEY: key });
  }
  let stderr = '';
  const origWrite = process.stderr.write.bind(process.stderr);
  process.stderr.write = (chunk, ...rest) => {
    stderr += chunk;
    return origWrite(chunk, ...rest);
  };
  try {
    const result = await runCommand({ repo: 'r', lane: 'default', cmd: ['true'], cwd: ctx.repoDir, spawnSupervisor, ...options });
    return { result, ticket, stderr };
  } finally {
    process.stderr.write = origWrite;
    for (const n of ENV_NAMES) {
      if (prev[n] === undefined) delete process.env[n];
      else process.env[n] = prev[n];
    }
  }
}

test('the ticket carries the effective deadline and its source: CLI, then lane, then global', async () => {
  const both = setup({ lane: { deadlineMs: 600_000 }, global: { deadlineMs: 7_200_000 } });
  const viaCli = await submit(both, { deadline: '2m' });
  assert.deepEqual([viaCli.ticket.deadlineMs, viaCli.ticket.deadlineSource], [120_000, 'cli']);
  const viaLane = await submit(both, {});
  assert.deepEqual([viaLane.ticket.deadlineMs, viaLane.ticket.deadlineSource], [600_000, 'lane']);
  const viaGlobal = await submit(setup({ global: { deadlineMs: 7_200_000 } }), {});
  assert.deepEqual([viaGlobal.ticket.deadlineMs, viaGlobal.ticket.deadlineSource], [7_200_000, 'global']);
  const dflt = await submit(setup(), {});
  assert.deepEqual([dflt.ticket.deadlineMs, dflt.ticket.deadlineSource], [21_600_000, 'global']);
});

test('--deadline outside 60s..24h, or not a duration, exits 64 and never enqueues', async () => {
  const ctx = setup();
  for (const spec of ['59s', '25h', 'soon', '', '0']) {
    const bad = await submit(ctx, { deadline: spec });
    assert.equal(bad.result.exitCode, 64, spec);
    assert.equal(bad.ticket, null, spec);
    assert.match(bad.stderr, /--deadline/, spec);
  }
});

test('--deadline on a reentrant run exits 64 and runs nothing; without it the same run proceeds', async () => {
  const ctx = setup();
  const refused = await submit(ctx, { deadline: '2m' }, { inherit: true });
  assert.equal(refused.result.exitCode, 64);
  assert.match(refused.stderr, /--deadline is refused on a run inside a lease/);
  const allowed = await submit(setup(), {}, { inherit: true });
  assert.equal(allowed.result.exitCode, 0);
});
