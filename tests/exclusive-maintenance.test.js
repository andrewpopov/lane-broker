import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, waitFor, gitFixture, BIN } from './helpers.js';
import { resolveTicketConfig, ConfigError } from '../src/config.js';
import { readLease } from '../src/lease.js';
import { paths, atomicWriteJson } from '../src/state.js';
import { runCommand } from '../src/run.js';
import { oversizedWeightMessage, remoteExclusiveDeprecationMessage, formatDeadline } from '../src/exclusive.js';

/** BRAIN-695 S3: exclusive is a maintenance class with a purpose and a hard deadline; remoteExclusive is deprecated. */

const TEST_DEADLINE_MS = 1500; // below the public minimum, injected at the supervisor only
const TRAP_EXIT_ZERO = `trap 'exit 0' TERM; echo ready; while :; do sleep 0.1; done`;

function setup({ lane = {}, extraEnv = {} } = {}) {
  const { base, home, state, env } = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '4000', ...extraEnv });
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane }, plain: { weight: 1 } } });
  gitFixture(['init', '-q'], repoDir);
  return { base, home, state, env, repoDir };
}

const historyOf = (state) => fs.readFileSync(paths(state).history, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
const EXCL = ['--exclusive', '--exclusive-purpose', 'upgrade', '--exclusive-deadline', '1m'];
const runArgs = (extra, cmd, lane = 'default') => ['run', '--repo', 'r', '--lane', lane, ...extra, '--', ...cmd];
const bash = (script) => ['bash', '-c', script];

async function detached(ctx, extra, cmd) {
  const result = await laneRun(runArgs([...extra, '--detach'], cmd), { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(result.code, 0, result.stderr);
  const id = result.stderr.match(/detached (\S+?);/)?.[1];
  assert.ok(id, `no id in: ${result.stderr}`);
  const lease = await waitFor(() => {
    const l = readLease(ctx.state, id);
    return l && l.childPgid ? l : null;
  });
  return { id, lease, result };
}

const waitReady = (lease) => waitFor(() => (fs.existsSync(lease.logPath) && fs.readFileSync(lease.logPath, 'utf8').includes('ready') ? true : null));

// ---- config validation ----

test('config: exclusivePurpose and exclusiveDeadlineMs are validated with messages naming the lane and key', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  const resolve = () => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'up' });
  const refuses = (laneCfg, pattern) => {
    writeRepoConfig(repoDir, { version: 1, lanes: { up: { weight: 1, exclusive: true, ...laneCfg } } });
    assert.throws(resolve, (err) => err instanceof ConfigError && pattern.test(err.message), JSON.stringify(laneCfg));
  };
  for (const bad of ['deploy', '', 3, null]) refuses({ exclusivePurpose: bad }, /lane "up"\.exclusivePurpose must be "upgrade" or "calibration"/);
  for (const bad of [59_999, 7_200_001, 90_000.5, '20m', null, 0]) refuses({ exclusiveDeadlineMs: bad }, /lane "up"\.exclusiveDeadlineMs must be an integer number of milliseconds from 60000 to 7200000/);
  writeRepoConfig(repoDir, { version: 1, lanes: { up: { weight: 1, exclusive: true, exclusivePurpose: 'calibration', exclusiveDeadlineMs: 60_000 }, other: { weight: 1 } }, undeclaredLanes: { as: 'up' } });
  const ok = resolve();
  assert.equal(ok.exclusivePurpose, 'calibration');
  assert.equal(ok.exclusiveDeadlineMs, 60_000);
  const adhoc = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'adhoc' });
  assert.equal(adhoc.exclusivePurpose, null, 'not inherited through an undeclaredLanes template');
  assert.equal(adhoc.exclusiveDeadlineMs, null);
  assert.equal(formatDeadline(7_200_000), '2h');
  assert.equal(formatDeadline(90_000), '90s');
  assert.equal(formatDeadline(1500), '1500ms');
});

test('oversizedWeightMessage points at cpuCores/memoryBytes and no longer recommends exclusive', () => {
  const message = oversizedWeightMessage(9, 4);
  assert.match(message, /cpuCores/);
  assert.match(message, /memoryBytes/);
  assert.doesNotMatch(message, /exclusive/);
});

// ---- lane run: resolution, refusals, warnings (fake supervisor seam) ----

async function submit(ctx, options) {
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
  process.env.LANE_BROKER_HOME = ctx.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = ctx.env.LANE_BROKER_STATE;
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
    for (const n of names) {
      if (prev[n] === undefined) delete process.env[n];
      else process.env[n] = prev[n];
    }
  }
}

test('the ticket carries purpose and deadline from the CLI or the lane config; the CLI wins; ms integers and 30s/20m/2h forms parse', async () => {
  const configured = setup({ lane: { exclusive: true, exclusivePurpose: 'calibration', exclusiveDeadlineMs: 120_000 } });
  const fromConfig = await submit(configured, {});
  assert.equal(fromConfig.stderr, '');
  assert.equal(fromConfig.ticket.exclusivePurpose, 'calibration');
  assert.equal(fromConfig.ticket.exclusiveDeadlineMs, 120_000);
  const overridden = await submit(configured, { exclusivePurpose: 'upgrade', exclusiveDeadline: '20m' });
  assert.equal(overridden.ticket.exclusivePurpose, 'upgrade');
  assert.equal(overridden.ticket.exclusiveDeadlineMs, 1_200_000);
  const plain = setup();
  for (const [spec, ms] of [['60000', 60_000], ['90s', 90_000], ['2h', 7_200_000]]) {
    assert.equal((await submit(plain, { exclusive: true, exclusivePurpose: 'upgrade', exclusiveDeadline: spec })).ticket.exclusiveDeadlineMs, ms, spec);
  }
});

test('purpose or deadline without exclusive exits 64 naming the key; bad values exit 64 and never enqueue', async () => {
  const lanes = setup({ lane: { exclusivePurpose: 'upgrade' } });
  const purpose = await submit(lanes, {});
  assert.equal(purpose.result.exitCode, 64);
  assert.match(purpose.stderr, /lane "default": exclusivePurpose is set but the run is not exclusive/);
  assert.equal(purpose.ticket, null);
  const deadlineConfig = setup({ lane: { exclusiveDeadlineMs: 60_000 } });
  const deadline = await submit(deadlineConfig, {});
  assert.equal(deadline.result.exitCode, 64);
  assert.match(deadline.stderr, /lane "default": exclusiveDeadlineMs is set but the run is not exclusive/);
  const plain = setup();
  const cliDeadline = await submit(plain, { exclusiveDeadline: '20m' });
  assert.equal(cliDeadline.result.exitCode, 64);
  assert.match(cliDeadline.stderr, /exclusiveDeadlineMs is set but the run is not exclusive/);
  for (const options of [{ exclusivePurpose: 'deploy' }, { exclusiveDeadline: '59s' }, { exclusiveDeadline: '3h' }, { exclusiveDeadline: 'soon' }, { exclusiveDeadline: '' }, { exclusivePurpose: '' }]) {
    const bad = await submit(plain, { exclusive: true, ...options });
    assert.equal(bad.result.exitCode, 64, JSON.stringify(options));
    assert.equal(bad.ticket, null, JSON.stringify(options));
    assert.match(bad.stderr, /--exclusive-(purpose|deadline)/, JSON.stringify(options));
  }
});

test('exclusive without purpose or deadline warns and stays exclusive; with both it is silent', async () => {
  const ctx = setup();
  const bare = await submit(ctx, { exclusive: true });
  assert.match(bare.stderr, /exclusive without --exclusive-purpose\/--exclusive-deadline is deprecated; refused from 0\.62\.0/);
  assert.equal(bare.ticket.exclusive, true, 'never silently downgraded');
  assert.equal(bare.ticket.exclusivePurpose, undefined);
  const half = await submit(ctx, { exclusive: true, exclusivePurpose: 'upgrade' });
  assert.match(half.stderr, /is deprecated; refused from 0\.62\.0/);
  assert.equal(half.ticket.exclusive, true);
  const full = await submit(ctx, { exclusive: true, exclusivePurpose: 'upgrade', exclusiveDeadline: '1m' });
  assert.doesNotMatch(full.stderr, /deprecated/);
  assert.equal(full.ticket.exclusive, true);
});

test('remoteExclusive: still honoured, with the one deprecation line on stderr (and none for a lane without it)', async () => {
  const ctx = setup({ lane: { remoteExclusive: true } });
  const run = await submit(ctx, {});
  const expected = 'lane-broker: lane "default": "remoteExclusive" is deprecated: it holds a whole runner. Book cpuCores/memoryBytes honestly instead; leases are CPU-enforced (BRAIN-695). Refused from 0.62.0.\n';
  assert.equal(remoteExclusiveDeprecationMessage('default') + '\n', expected);
  assert.equal(run.stderr.split(expected).length - 1, 1, `exactly once: ${run.stderr}`);
  assert.equal(run.result.exitCode, 0, 'behaviour unchanged');
  const clean = await submit(ctx, { lane: 'plain' });
  assert.doesNotMatch(clean.stderr, /remoteExclusive/);
});

// ---- the supervisor: deadline expiry ----

test('deadline expiry ends the run with 124 even when the command traps TERM and exits 0; the lease is released and the row says so', async () => {
  const ctx = setup({ extraEnv: { LANE_BROKER_TEST_EXCLUSIVE_DEADLINE_MS: String(TEST_DEADLINE_MS) } });
  const started = Date.now();
  const result = await laneRun(runArgs(EXCL, bash(TRAP_EXIT_ZERO)), { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  assert.ok(Date.now() - started < 30_000);
  assert.match(result.stderr, /exclusive deadline 1500ms expired/);
  const [row] = historyOf(ctx.state);
  assert.equal(row.exit, 124);
  assert.equal(row.exclusive, true);
  assert.equal(row.exclusiveDeadlineExpired, true);
  assert.equal(row.reason, 'exclusive-deadline');
  assert.match(fs.readFileSync(path.join(paths(ctx.state).logs, `${row.id}.log`), 'utf8'), /exclusive deadline 1500ms expired/);
  assert.deepEqual(fs.readdirSync(paths(ctx.state).leases).filter((n) => n.endsWith('.json')), [], 'lease released');
  assert.match(fs.readFileSync(paths(ctx.state).admissionLog, 'utf8'), /lane-broker-reap id=\S+ .*reason=exclusive-deadline/);
});

test('a command that finishes inside its deadline is untouched (exit 0, no expiry flag)', async () => {
  const ctx = setup({ extraEnv: { LANE_BROKER_TEST_EXCLUSIVE_DEADLINE_MS: '10000' } });
  const result = await laneRun(runArgs(EXCL, bash('echo ok')), { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(result.code, 0, result.stderr);
  assert.equal(historyOf(ctx.state)[0].exclusiveDeadlineExpired, undefined);
});

test('an explicit lane cancel before expiry keeps 130, not 124', async () => {
  const ctx = setup({ extraEnv: { LANE_BROKER_TEST_EXCLUSIVE_DEADLINE_MS: '20000' } });
  const { id, lease } = await detached(ctx, EXCL, bash(TRAP_EXIT_ZERO));
  await waitReady(lease);
  const waiter = laneRun(['wait', id], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal((await laneRun(['cancel', id], { env: ctx.env, cwd: ctx.repoDir })).code, 0);
  assert.equal((await waiter).code, 130);
  const [row] = historyOf(ctx.state);
  assert.equal(row.exit, 130);
  assert.equal(row.exclusiveDeadlineExpired, undefined);
});

test('--detach keeps the deadline: the detached supervisor ends the run at 124, and lane status shows purpose and time left while it runs', async () => {
  const ctx = setup({ extraEnv: { LANE_BROKER_TEST_EXCLUSIVE_DEADLINE_MS: '6000' } });
  const { id, lease } = await detached(ctx, EXCL, bash(TRAP_EXIT_ZERO));
  assert.equal(lease.exclusivePurpose, 'upgrade');
  assert.ok(lease.exclusiveDeadlineAt - lease.startedAt === 6000, 'the deadline starts at spawn');
  await waitReady(lease);
  const status = await laneRun(['status'], { env: ctx.env, cwd: ctx.repoDir });
  assert.match(status.stdout, /\[exclusive upgrade, deadline in \d+s\]/);
  const json = JSON.parse((await laneRun(['status', '--json'], { env: ctx.env, cwd: ctx.repoDir })).stdout);
  const held = json.running.find((r) => r.id === id);
  assert.equal(held.exclusivePurpose, 'upgrade');
  assert.ok(held.exclusiveDeadlineRemainingMs > 0 && held.exclusiveDeadlineRemainingMs <= 6000);
  const waited = await laneRun(['wait', id], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(waited.code, 124, waited.stderr);
  assert.equal(historyOf(ctx.state)[0].exclusiveDeadlineExpired, true);
});

test('a nested same-lane run lives inside the parent: the parent deadline ends both, with no second timer or reset', async () => {
  const ctx = setup({ extraEnv: { LANE_BROKER_TEST_EXCLUSIVE_DEADLINE_MS: String(TEST_DEADLINE_MS) } });
  const pidFile = path.join(ctx.base, 'nested.pid');
  const script = `node ${JSON.stringify(BIN)} run --repo r --lane default --exclusive -- bash -c 'echo $$ > ${pidFile}; while :; do sleep 0.1; done'`;
  const result = await laneRun(runArgs(EXCL, bash(script)), { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(result.code, 124, `stderr: ${result.stderr}`);
  assert.equal(historyOf(ctx.state).length, 1, 'the nested run never became a ticket of its own');
  const nestedPid = Number(fs.readFileSync(pidFile, 'utf8'));
  assert.throws(() => process.kill(nestedPid, 0), { code: 'ESRCH' }, 'the nested workload is gone');
  assert.doesNotMatch(result.stderr, /deprecated/, 'a nested run does not warn again');
});
