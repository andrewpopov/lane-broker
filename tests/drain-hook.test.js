import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, laneRun } from './helpers.js';
import { drainHookCommand } from '../src/agent/drain-hook.js';
import { claimBlock, drainsOf, isDraining } from '../src/agent/claims.js';
import { ExecutionTable, ensureAgentDirs, newExecutionRecord } from '../src/agent/store.js';
import { paths, readDrainMarkers, readRemoteDrain, writeRemoteDrain } from '../src/state.js';
import { randomUUID } from 'node:crypto';
import os from 'node:os';

const EXPIRES = '2099-01-01T00:00:00.000Z';
const env = (extra = {}) => ({ BOSUN_DRAIN_ID: 'd1', BOSUN_DRAIN_MODE: 'interrupt', BOSUN_DRAIN_REASON: 'deploy', BOSUN_DRAIN_EXPIRES_AT: EXPIRES, ...extra });
const markerFile = (state) => path.join(paths(state).drainOwners, 'bosun.json');

function hookWorld() {
  const { state, home, base, env: processEnv } = freshEnv();
  const remoteRoot = path.join(base, 'remote');
  const agent = { state: 'running', drains: [] };
  const observe = (id = 'd1') => { agent.drains = [{ owner: 'bosun', id, applied: JSON.parse(fs.readFileSync(markerFile(state), 'utf8')).applied }]; };
  const remote = { left: false };
  const run = async (verb, vars = env(), extra = {}) => {
    const errors = [];
    const { exitCode } = await drainHookCommand([verb], {
      remoteRoot, env: vars, loadConfig: () => ({ stateDir: state, brokerHome: home }),
      readStatus: () => agent, remoteWork: async () => remote.left, err: { write: (s) => errors.push(s) }, ...extra,
    });
    return { exitCode, errors };
  };
  return { state, remoteRoot, agent, remote, run, processEnv, observe };
}

const putRecord = (remoteRoot, subphase) => {
  ensureAgentDirs(remoteRoot);
  const claim = { id: randomUUID(), epoch: 1, token: randomUUID(), repo_key: 'r', lane: 'default', lane_key: 'k', binding: {}, header: {}, snapshot: {} };
  new ExecutionTable(remoteRoot).put({ ...newExecutionRecord(claim), subphase });
};

test('--apply writes the bosun marker with drain id, mode and expiry, and a second apply changes only the application nonce', async () => {
  const { state, run } = hookWorld();
  assert.equal((await run('--apply')).exitCode, 0);
  const first = JSON.parse(fs.readFileSync(markerFile(state), 'utf8'));
  assert.equal(first.drainId, 'd1');
  assert.equal(first.mode, 'interrupt');
  assert.equal(first.expiresAt, Date.parse(EXPIRES));
  assert.equal(first.owner, 'bosun');
  assert.equal((await run('--apply')).exitCode, 0);
  const { applied: firstApplied, ...firstRest } = first;
  const { applied: secondApplied, ...secondRest } = JSON.parse(fs.readFileSync(markerFile(state), 'utf8'));
  assert.deepEqual(secondRest, firstRest);
  assert.ok(firstApplied && secondApplied && firstApplied !== secondApplied, 'every application carries a fresh nonce');
});

test('--lift removes the bosun marker, twice, and leaves the human marker alone', async () => {
  const { state, run } = hookWorld();
  await writeRemoteDrain(state, 'human drain');
  await run('--apply');
  assert.equal((await run('--lift')).exitCode, 0);
  assert.equal((await run('--lift')).exitCode, 0);
  assert.deepEqual(readDrainMarkers(state).map((m) => m.owner), ['human']);
});

test('--status is 1 with no marker or another id, 75 before the agent has observed the id, and 0 after', async () => {
  const { agent, run, observe } = hookWorld();
  assert.equal((await run('--status')).exitCode, 1, 'no marker');
  await run('--apply', env({ BOSUN_DRAIN_ID: 'other' }));
  assert.equal((await run('--status')).exitCode, 1, 'another id');
  await run('--apply');
  assert.equal((await run('--status')).exitCode, 75, 'status lists no drains yet');
  agent.drains = [{ owner: 'bosun', id: 'other', applied: 'x' }];
  assert.equal((await run('--status')).exitCode, 75, 'status lists another id');
  observe();
  assert.equal((await run('--status')).exitCode, 0);
});

test('--status counts a host with no live agent as observed', async () => {
  const { agent, run } = hookWorld();
  await run('--apply');
  Object.assign(agent, { state: 'no-owner', drains: undefined });
  assert.equal((await run('--status')).exitCode, 0);
});

test('interrupt is 0 with a RUNNING record; wait is 75 until every record is DONE and no remote work is left', async () => {
  const { remoteRoot, remote, run, observe } = hookWorld();
  putRecord(remoteRoot, 'RUNNING');
  await run('--apply');
  observe();
  assert.equal((await run('--status')).exitCode, 0, 'interrupt ignores records');
  await run('--apply', env({ BOSUN_DRAIN_MODE: 'wait' }));
  observe();
  const wait = env({ BOSUN_DRAIN_MODE: 'wait' });
  assert.equal((await run('--status', wait)).exitCode, 75, 'a RUNNING record');
  const table = new ExecutionTable(remoteRoot);
  table.put({ ...table.all()[0], subphase: 'DONE' });
  remote.left = true;
  assert.equal((await run('--status', wait)).exitCode, 75, 'a remote lease is still running or queued');
  remote.left = false;
  assert.equal((await run('--status', wait)).exitCode, 0);
});

test('wait with no agent directory at all has no records, so only remote work holds it', async () => {
  const { run, observe } = hookWorld();
  const wait = env({ BOSUN_DRAIN_MODE: 'wait' });
  await run('--apply', wait);
  observe();
  assert.equal((await run('--status', wait)).exitCode, 0);
});

test('a missing or invalid drain variable is a usage error (2) and writes no marker', async () => {
  const { state, run } = hookWorld();
  for (const bad of [{ BOSUN_DRAIN_ID: '' }, { BOSUN_DRAIN_MODE: 'now' }, { BOSUN_DRAIN_EXPIRES_AT: 'soon' }]) {
    assert.equal((await run('--apply', env(bad))).exitCode, 2, JSON.stringify(bad));
  }
  assert.equal(fs.existsSync(markerFile(state)), false);
});

test('the verb is top-level: lane drain-hook with no verb exits 2, naming the verbs', async () => {
  const { processEnv } = hookWorld();
  const result = await laneRun(['drain-hook'], { env: processEnv });
  assert.equal(result.code, 2);
  assert.match(result.stderr, /exactly one of --apply, --lift, --status/);
});

test('a marker carrying drainId and mode still drains and blocks claims, and an older-shape reader sees reason and since', async () => {
  const { state, run, processEnv } = hookWorld();
  await run('--apply');
  assert.equal(isDraining(state), true);
  assert.equal(claimBlock({ schedule: null, stopClaimingBeforeCloseMs: 0, draining: () => isDraining(state), owners: () => ['bosun'] }), 'draining (bosun)');
  assert.deepEqual(drainsOf(state), [{ owner: 'bosun', id: 'd1', applied: JSON.parse(fs.readFileSync(markerFile(state), 'utf8')).applied }]);
  assert.equal(readRemoteDrain(state).reason, 'deploy');
  const probe = JSON.parse((await laneRun(['remote-probe'], { env: processEnv })).stdout);
  assert.equal(probe.draining, true);
  assert.equal(probe.remoteDrain.reason, 'deploy');
});

test('--status is 1 when the marker holds the id in another mode: a wait marker never answers an interrupt query', async () => {
  const { remoteRoot, run, observe } = hookWorld();
  putRecord(remoteRoot, 'RUNNING');
  await run('--apply', env({ BOSUN_DRAIN_MODE: 'wait' }));
  observe();
  assert.equal((await run('--status', env({ BOSUN_DRAIN_MODE: 'interrupt' }))).exitCode, 1);
});

test('a reapplied id is not acknowledged by the earlier application: 75 until the agent reports the new nonce', async () => {
  const { run, observe } = hookWorld();
  await run('--apply');
  observe();
  assert.equal((await run('--status')).exitCode, 0);
  await run('--lift');
  await run('--apply');
  assert.equal((await run('--status')).exitCode, 75, 'the old nonce is not this application');
  observe();
  assert.equal((await run('--status')).exitCode, 0);
});

test('a marker that is replaced, reapplied or lifted while the remote work was being read never yields a stale 0', async () => {
  const wait = env({ BOSUN_DRAIN_MODE: 'wait' });
  for (const [change, expected] of [
    [(run) => run('--apply', wait), 75],
    [(run) => run('--apply', env({ BOSUN_DRAIN_ID: 'd2', BOSUN_DRAIN_MODE: 'wait' })), 1],
    [(run) => run('--apply', env({ BOSUN_DRAIN_MODE: 'interrupt' })), 1],
    [(run) => run('--lift'), 1],
  ]) {
    const { run, observe } = hookWorld();
    await run('--apply', wait);
    observe();
    const status = await run('--status', wait, { remoteWork: async () => { await change(run); return false; } });
    assert.equal(status.exitCode, expected, change.toString());
  }
});

test('an expired marker is not in force: --status is 1', async () => {
  const { run } = hookWorld();
  await run('--apply', env({ BOSUN_DRAIN_EXPIRES_AT: '2000-01-01T00:00:00.000Z' }));
  assert.equal((await run('--status')).exitCode, 1);
});

test('an unreadable agent config is an error exit that is neither 0 nor 75, for every verb', async () => {
  const { run } = hookWorld();
  const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'drain-hook-noconfig-'));
  for (const verb of ['--apply', '--lift', '--status']) {
    const { exitCode, errors } = await run(verb, env(), { configDir: empty, loadConfig: undefined });
    assert.equal(exitCode, 1, verb);
    assert.match(errors.join(''), /lane drain-hook:/);
  }
  fs.rmSync(empty, { recursive: true, force: true });
});

const writeJson = (file, value) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(value)); };
const leaseOf = (state, id, running = true) => writeJson(path.join(paths(state).leases, `${id}.json`), { id, state: running ? 'RUNNING' : 'DONE' });
const submittedBy = (remoteRoot, ticket, id) => {
  fs.mkdirSync(path.join(remoteRoot, 'tickets', ticket), { recursive: true });
  fs.writeFileSync(path.join(remoteRoot, 'tickets', ticket, 'remote-id'), id);
};

test('wait with the real remote-work reader: a remote lease or queued ticket holds it, a local one and a DONE lease do not', async () => {
  const { state, remoteRoot, run, observe } = hookWorld();
  const wait = env({ BOSUN_DRAIN_MODE: 'wait' });
  await run('--apply', wait);
  observe();
  const real = { remoteWork: undefined };
  assert.equal((await run('--status', wait, real)).exitCode, 0, 'nothing running');
  leaseOf(state, 'local-1');
  leaseOf(state, 'remote-done', false);
  submittedBy(remoteRoot, 'sub-1', 'remote-done');
  assert.equal((await run('--status', wait, real)).exitCode, 0, 'a local lease and a finished remote one');
  leaseOf(state, 'remote-1');
  submittedBy(remoteRoot, 'sub-2', 'remote-1');
  assert.equal((await run('--status', wait, real)).exitCode, 75, 'a running remote lease');
  fs.rmSync(path.join(paths(state).leases, 'remote-1.json'));
  writeJson(path.join(paths(state).queue, '000000000001-remote-1.json'), { id: 'remote-1' });
  assert.equal((await run('--status', wait, real)).exitCode, 75, 'a queued remote ticket');
});

test('wait fails closed when ticket ownership cannot be read: any lease or queued ticket then counts as work left', async () => {
  const wait = env({ BOSUN_DRAIN_MODE: 'wait' });
  for (const [what, damage] of [
    ['tickets is not a directory', (remoteRoot) => fs.writeFileSync(path.join(remoteRoot, 'tickets'), 'x')],
    ['a remote-id that cannot be read', (remoteRoot) => fs.mkdirSync(path.join(remoteRoot, 'tickets', 'sub-1', 'remote-id'), { recursive: true })],
  ]) {
    const { state, remoteRoot, run, observe } = hookWorld();
    await run('--apply', wait);
    observe();
    leaseOf(state, 'local-1');
    fs.mkdirSync(remoteRoot, { recursive: true });
    damage(remoteRoot);
    assert.equal((await run('--status', wait, { remoteWork: undefined })).exitCode, 75, what);
  }
});
