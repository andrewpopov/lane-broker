import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { freshEnv, writeGlobalConfig, writeRepoConfig, gitFixture } from './helpers.js';
import { runCommand } from '../src/run.js';
import { atomicWriteJson } from '../src/state.js';
import { repoIdentity, sanitizeKey } from '../src/config.js';

const RUNNERS = [{ name: 'skybox', ssh: 'skybox-runner' }];

/**
 * Run `runCommand` against a fake supervisor that never actually spawns a
 * process: it decodes the base64 `LANE_BROKER_TICKET` env the real
 * supervisor would receive, immediately writes a green result so
 * `runCommand`'s poll loop returns right away, and hands the decoded ticket
 * back to the caller (BRAIN-319 T3b-1's only observable seam onto the ticket
 * payload -- same pattern as tests/run-not-found-grace.test.js).
 */
async function captureTicket({ env, cwd, local, localReason, lane = 'default', extraEnv = {} }) {
  let capturedTicket = null;
  let spawnCalled = false;
  const spawnSupervisor = (execPath, args, opts) => {
    spawnCalled = true;
    capturedTicket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
    atomicWriteJson(capturedTicket.resultPath, {
      id: capturedTicket.id,
      exit: 0,
      signal: null,
      startedAt: Date.now(),
      endedAt: Date.now(),
      waitedMs: 0,
    });
    const silent = () => new PassThrough().end();
    return Object.assign(new EventEmitter(), { pid: process.pid, unref() {}, stdout: silent(), stderr: silent() });
  };

  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  const prevLease = process.env.LANE_BROKER_LEASE;
  const prevLocal = process.env.LANE_BROKER_LOCAL;
  const prevLocalReason = process.env.LANE_BROKER_LOCAL_REASON;
  process.env.LANE_BROKER_HOME = env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = env.LANE_BROKER_STATE;
  if (extraEnv.LANE_BROKER_LEASE !== undefined) process.env.LANE_BROKER_LEASE = extraEnv.LANE_BROKER_LEASE;
  else delete process.env.LANE_BROKER_LEASE;
  if (extraEnv.LANE_BROKER_LOCAL !== undefined) process.env.LANE_BROKER_LOCAL = extraEnv.LANE_BROKER_LOCAL;
  else delete process.env.LANE_BROKER_LOCAL;
  if (extraEnv.LANE_BROKER_LOCAL_REASON !== undefined) process.env.LANE_BROKER_LOCAL_REASON = extraEnv.LANE_BROKER_LOCAL_REASON;
  else delete process.env.LANE_BROKER_LOCAL_REASON;
  let stderr = '';
  const origWrite = process.stderr.write.bind(process.stderr);
  process.stderr.write = (chunk, ...rest) => {
    stderr += chunk;
    return origWrite(chunk, ...rest);
  };
  try {
    const result = await runCommand({
      repo: 'r',
      lane,
      cmd: ['true'],
      cwd,
      local,
      localReason,
      spawnSupervisor,
    });
    return { result, ticket: capturedTicket, spawnCalled, stderr };
  } finally {
    process.stderr.write = origWrite;
    if (prevHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prevHome;
    if (prevState === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = prevState;
    if (prevLease === undefined) delete process.env.LANE_BROKER_LEASE;
    else process.env.LANE_BROKER_LEASE = prevLease;
    if (prevLocal === undefined) delete process.env.LANE_BROKER_LOCAL;
    else process.env.LANE_BROKER_LOCAL = prevLocal;
    if (prevLocalReason === undefined) delete process.env.LANE_BROKER_LOCAL_REASON;
    else process.env.LANE_BROKER_LOCAL_REASON = prevLocalReason;
  }
}

function setup({ runners, remote } = {}) {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 2,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    ...(runners !== undefined ? { runners } : {}),
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...(remote !== undefined ? { remote } : {}) } } });
  gitFixture(['init', '-q'], repoDir);
  return { base, home, env, repoDir };
}

const NO_REMOTE_CASES = [
  ['no runners configured', { runners: undefined, remote: true }, {}],
  ['lane not opted into remote', { runners: RUNNERS, remote: false }, {}],
  ['lane has no remote field at all', { runners: RUNNERS, remote: undefined }, {}],
  ['--local forces local', { runners: RUNNERS, remote: true }, { local: true, localReason: 'needs ssh' }],
  ['LANE_BROKER_LOCAL=1 forces local', { runners: RUNNERS, remote: true }, { extraEnv: { LANE_BROKER_LOCAL: '1', LANE_BROKER_LOCAL_REASON: 'needs ssh' } }],
  ['an inherited LANE_BROKER_LEASE forces local', { runners: RUNNERS, remote: true }, { extraEnv: { LANE_BROKER_LEASE: 'some-ancestor-id' } }],
];

for (const [label, cfg, opts] of NO_REMOTE_CASES) {
  test(`ticket payload carries no "remote" key when ineligible: ${label}`, async () => {
    const { env, repoDir } = setup(cfg);
    const { result, ticket, spawnCalled, stderr } = await captureTicket({ env, cwd: repoDir, ...opts });
    assert.equal(result.exitCode, 0, `stderr: ${stderr}`);
    assert.ok(spawnCalled, 'supervisor should have been spawned');
    assert.ok(ticket, 'ticket should have been captured');
    assert.equal(Object.prototype.hasOwnProperty.call(ticket, 'remote'), false, 'I6: ineligible ticket must carry no "remote" key at all');
  });
}

const IGNORED = /lane: --local ignored for remote-eligible lane default: pass --local-reason/;

test('BRAIN-547: --local without a reason on a remote-eligible lane is ignored with a warning and stays remote', async () => {
  const { env, repoDir } = setup({ runners: RUNNERS, remote: true });
  const { result, ticket, stderr } = await captureTicket({ env, cwd: repoDir, local: true });
  assert.equal(result.exitCode, 0, `stderr: ${stderr}`);
  assert.match(stderr, IGNORED);
  assert.ok(ticket.remote, 'ticket must still be remote-eligible');
  assert.equal(ticket.localReason, undefined);
});

test('BRAIN-547: LANE_BROKER_LOCAL=1 without a reason is ignored the same way', async () => {
  const { env, repoDir } = setup({ runners: RUNNERS, remote: true });
  const { ticket, stderr } = await captureTicket({ env, cwd: repoDir, extraEnv: { LANE_BROKER_LOCAL: '1' } });
  assert.match(stderr, IGNORED);
  assert.ok(ticket.remote);
});

test('BRAIN-547: --local with a reason stays local and the ticket carries localReasonText', async () => {
  const { env, repoDir } = setup({ runners: RUNNERS, remote: true });
  const { ticket, stderr } = await captureTicket({ env, cwd: repoDir, local: true, localReason: 'needs ssh' });
  assert.doesNotMatch(stderr, /ignored/);
  assert.equal(Object.prototype.hasOwnProperty.call(ticket, 'remote'), false);
  assert.equal(ticket.localReason, 'forced-flag');
  assert.equal(ticket.localReasonText, 'needs ssh');
});

test('BRAIN-547: a reason alone (flag or env) implies local, env origin is forced-env', async () => {
  const { env, repoDir } = setup({ runners: RUNNERS, remote: true });
  const flag = await captureTicket({ env, cwd: repoDir, localReason: 'needs gh' });
  assert.equal(Object.prototype.hasOwnProperty.call(flag.ticket, 'remote'), false);
  assert.equal(flag.ticket.localReason, 'forced-flag');
  const viaEnv = await captureTicket({ env, cwd: repoDir, extraEnv: { LANE_BROKER_LOCAL: '1', LANE_BROKER_LOCAL_REASON: 'needs gh' } });
  assert.equal(Object.prototype.hasOwnProperty.call(viaEnv.ticket, 'remote'), false);
  assert.equal(viaEnv.ticket.localReason, 'forced-env');
  assert.equal(viaEnv.ticket.localReasonText, 'needs gh');
  const envOnly = await captureTicket({ env, cwd: repoDir, extraEnv: { LANE_BROKER_LOCAL_REASON: 'needs gh' } });
  assert.equal(Object.prototype.hasOwnProperty.call(envOnly.ticket, 'remote'), false);
});

test('BRAIN-547: --local on a lane that is not remote-eligible is unchanged and silent', async () => {
  const { env, repoDir } = setup({ runners: RUNNERS, remote: false });
  const { result, stderr } = await captureTicket({ env, cwd: repoDir, local: true });
  assert.equal(result.exitCode, 0);
  assert.doesNotMatch(stderr, /ignored/);
});

test('an allowed undeclared lane is never remote-eligible even when runners are configured', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 4,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    runners: RUNNERS,
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: 'allow',
    lanes: { default: { weight: 1 }, prepush: { weight: 2, remote: true } },
  });
  gitFixture(['init', '-q'], repoDir);

  const { result, ticket, spawnCalled, stderr } = await captureTicket({ env, cwd: repoDir, lane: 'zirk788' });
  assert.equal(result.exitCode, 0, `stderr: ${stderr}`);
  assert.ok(spawnCalled, 'supervisor should have been spawned');
  assert.ok(ticket, 'ticket should have been captured');
  assert.equal(Object.prototype.hasOwnProperty.call(ticket, 'remote'), false, 'an allowed undeclared lane must never be remote-eligible');
});

// BRAIN-325: this is the seam through which the remote runner side resolves
// an undeclared lane too -- `lane remote-exec` snapshots `.lane-broker.json`
// and calls this same `runCommand` -> `resolveTicketConfig` path with
// `configRoot` pointed at the snapshot work dir (src/remote-runner.js), so
// there is no separate "remote-side" resolver to keep in sync.
test('an "as"-resolved undeclared lane inherits remote eligibility (weight, cpuCores, memoryBytes) from its template', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 4,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    runners: RUNNERS,
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'prepush' },
    lanes: {
      default: { weight: 1 },
      prepush: { weight: 2, cpuCores: 1.5, memoryBytes: 1073741824, remote: true, remoteDeps: ['.'] },
    },
  });
  gitFixture(['init', '-q'], repoDir);

  const { result, ticket, spawnCalled, stderr } = await captureTicket({ env, cwd: repoDir, lane: 'zirk812' });
  assert.equal(result.exitCode, 0, `stderr: ${stderr}`);
  assert.ok(spawnCalled, 'supervisor should have been spawned');
  assert.ok(ticket, 'ticket should have been captured');
  assert.ok(ticket.remote, 'an "as"-resolved undeclared lane inheriting remote:true must be remote-eligible');
  assert.equal(ticket.remote.weight, 2);
  assert.equal(ticket.remote.cpuCores, 1.5);
  assert.equal(ticket.remote.memoryBytes, 1073741824);
});

test('ticket payload carries a "remote" block, with the documented shape, when every condition is met', async () => {
  const { env, repoDir } = setup({ runners: RUNNERS, remote: true });
  const { result, ticket, stderr } = await captureTicket({ env, cwd: repoDir });
  assert.equal(result.exitCode, 0, `stderr: ${stderr}`);
  assert.ok(ticket.remote, 'expected ticket.remote to be present');
  assert.equal(ticket.remote.worktreeRoot, fs.realpathSync(repoDir));
  assert.equal(ticket.remote.relCwd, '');
  // repoKey is resolved.repoId: the realpath'd git common dir, stable across
  // every worktree of this repo (config.js's repoIdentity()).
  assert.equal(ticket.remote.repoKey, sanitizeKey(repoIdentity(repoDir)));
  assert.equal(ticket.remote.weight, 1);
  assert.equal(ticket.remote.cpuCores, ticket.resources.cpuCores);
  assert.equal(ticket.remote.memoryBytes, ticket.resources.memoryBytes);
});

test('a subdirectory cwd inside the worktree resolves relCwd relative to the worktree root', async () => {
  const { env, repoDir } = setup({ runners: RUNNERS, remote: true });
  const subdir = path.join(repoDir, 'sub', 'dir');
  fs.mkdirSync(subdir, { recursive: true });
  const { result, ticket, stderr } = await captureTicket({ env, cwd: subdir });
  assert.equal(result.exitCode, 0, `stderr: ${stderr}`);
  assert.ok(ticket.remote, 'expected ticket.remote to be present');
  assert.equal(ticket.remote.relCwd, path.join('sub', 'dir'));
});

test('oversize resource request is NOT refused at preflight when remote-eligible', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 2,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    schedulerMode: 'active',
    cpuAdmissionPercent: 1,
    cpuReserveCores: 0,
    runners: RUNNERS,
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, cpuCores: 1000, remote: true } } }); // BRAIN-452: oversize via CPU, a 1000 weight is refused at submission
  gitFixture(['init', '-q'], repoDir);

  const { result, ticket, stderr } = await captureTicket({ env, cwd: repoDir });
  assert.equal(result.exitCode, 0, `expected no preflight refusal; stderr: ${stderr}`);
  assert.doesNotMatch(stderr, /exceed this environment's budget/);
  assert.ok(ticket.remote, 'expected the oversize request to still be remote-eligible');
});

test('the same oversize resource request is refused exactly as today when ineligible (no runners)', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, {
    version: 1,
    capacity: 2,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    schedulerMode: 'active',
    cpuAdmissionPercent: 1,
    cpuReserveCores: 0,
  });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1000 } } });
  gitFixture(['init', '-q'], repoDir);

  const { result, spawnCalled, stderr } = await captureTicket({ env, cwd: repoDir });
  assert.equal(result.exitCode, 64, `stderr: ${stderr}`);
  assert.match(stderr, /exceed this environment's budget/);
  assert.equal(spawnCalled, false, 'refused at preflight: the supervisor must never be spawned');
});
