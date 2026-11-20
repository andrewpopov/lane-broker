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

const EXPIRES = '2099-01-01T00:00:00.000Z';
const env = (extra = {}) => ({ BOSUN_DRAIN_ID: 'd1', BOSUN_DRAIN_MODE: 'interrupt', BOSUN_DRAIN_REASON: 'deploy', BOSUN_DRAIN_EXPIRES_AT: EXPIRES, ...extra });
const markerFile = (state) => path.join(paths(state).drainOwners, 'bosun.json');

function hookWorld() {
  const { state, home, base, env: processEnv } = freshEnv();
  const remoteRoot = path.join(base, 'remote');
  const agent = { state: 'running', drains: [] };
  const remote = { left: false };
  const run = async (verb, vars = env(), extra = {}) => {
    const errors = [];
    const { exitCode } = await drainHookCommand([verb], {
      remoteRoot, env: vars, loadConfig: () => ({ stateDir: state, brokerHome: home }),
      readStatus: () => agent, remoteWork: async () => remote.left, err: { write: (s) => errors.push(s) }, ...extra,
    });
    return { exitCode, errors };
  };
  return { state, remoteRoot, agent, remote, run, processEnv };
}

const putRecord = (remoteRoot, subphase) => {
  ensureAgentDirs(remoteRoot);
  const claim = { id: randomUUID(), epoch: 1, token: randomUUID(), repo_key: 'r', lane: 'default', lane_key: 'k', binding: {}, header: {}, snapshot: {} };
  new ExecutionTable(remoteRoot).put({ ...newExecutionRecord(claim), subphase });
};

test('--apply writes the bosun marker with drain id, mode and expiry, and a second apply leaves the same marker', async () => {
  const { state, run } = hookWorld();
  assert.equal((await run('--apply')).exitCode, 0);
  const first = JSON.parse(fs.readFileSync(markerFile(state), 'utf8'));
  assert.equal(first.drainId, 'd1');
  assert.equal(first.mode, 'interrupt');
  assert.equal(first.expiresAt, Date.parse(EXPIRES));
  assert.equal(first.owner, 'bosun');
  assert.equal((await run('--apply')).exitCode, 0);
  assert.deepEqual(JSON.parse(fs.readFileSync(markerFile(state), 'utf8')), first);
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
  const { agent, run } = hookWorld();
  assert.equal((await run('--status')).exitCode, 1, 'no marker');
  await run('--apply', env({ BOSUN_DRAIN_ID: 'other' }));
  assert.equal((await run('--status')).exitCode, 1, 'another id');
  await run('--apply');
  assert.equal((await run('--status')).exitCode, 75, 'status lists no drains yet');
  agent.drains = [{ owner: 'bosun', id: 'other' }];
  assert.equal((await run('--status')).exitCode, 75, 'status lists another id');
  agent.drains = [{ owner: 'bosun', id: 'd1' }];
  assert.equal((await run('--status')).exitCode, 0);
});

test('--status counts a host with no live agent as observed', async () => {
  const { agent, run } = hookWorld();
  await run('--apply');
  Object.assign(agent, { state: 'no-owner', drains: undefined });
  assert.equal((await run('--status')).exitCode, 0);
});

test('interrupt is 0 with a RUNNING record; wait is 75 until every record is DONE and no remote work is left', async () => {
  const { remoteRoot, agent, remote, run } = hookWorld();
  agent.drains = [{ owner: 'bosun', id: 'd1' }];
  putRecord(remoteRoot, 'RUNNING');
  await run('--apply');
  assert.equal((await run('--status')).exitCode, 0, 'interrupt ignores records');
  await run('--apply', env({ BOSUN_DRAIN_MODE: 'wait' }));
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
  const { agent, run } = hookWorld();
  agent.drains = [{ owner: 'bosun', id: 'd1' }];
  const wait = env({ BOSUN_DRAIN_MODE: 'wait' });
  await run('--apply', wait);
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
  assert.deepEqual(drainsOf(state), [{ owner: 'bosun', id: 'd1' }]);
  assert.equal(readRemoteDrain(state).reason, 'deploy');
  const probe = JSON.parse((await laneRun(['remote-probe'], { env: processEnv })).stdout);
  assert.equal(probe.draining, true);
  assert.equal(probe.remoteDrain.reason, 'deploy');
});
