import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { resolveTicketConfig, ConfigError } from '../src/config.js';
import { historyRow } from '../src/supervisor.js';
import { freshEnv, laneRun, writeRepoConfig } from './helpers.js';
import { paths } from '../src/state.js';

// BRAIN-752
const GOOD = 'needs ssh to pitelite';
const BAD = 'worktree-local-dist';

function repoWith(lanes) {
  const { base, env, state } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes });
  return { repoDir, env, state };
}

const lastRow = (state) => JSON.parse(fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').pop());

async function runLane(lanes, lane) {
  const { repoDir, env, state } = repoWith(lanes);
  const res = await laneRun(['run', '--repo', 'r', '--lane', lane, '--', 'true'], { env, cwd: repoDir });
  assert.equal(res.code, 0, res.stderr);
  return { res, row: lastRow(state) };
}

test('config resolves a declared localReason, and null when absent or blank', () => {
  const { repoDir } = repoWith({ default: { weight: 1, localReason: ` ${GOOD} ` }, plain: { weight: 1 }, blank: { weight: 1, localReason: '  ' } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).localReason, GOOD);
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'plain' }).localReason, null);
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'blank' }).localReason, null);
});

test('an undeclared lane inherits its template localReason', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, undeclaredLanes: { as: 'tmpl' }, lanes: { default: { weight: 1 }, tmpl: { weight: 1, localReason: GOOD } } });
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'adhoc' }).localReason, GOOD);
});

test('config rejects a non-string localReason', () => {
  for (const bad of [true, 3]) {
    const { repoDir } = repoWith({ default: { weight: 1, localReason: bad } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), ConfigError);
  }
});

test('config-local lane with an unaccepted reason is stamped and warned about', async () => {
  const { res, row } = await runLane({ default: { weight: 1, localReason: BAD } }, 'default');
  assert.equal(row.configLocal, true);
  assert.equal(row.configLocalReason, BAD);
  assert.equal(row.configLocalAccepted, false);
  assert.equal(row.localReason, undefined);
  assert.match(res.stderr, /warning: lane default runs on this machine by config/);
  assert.match(res.stderr, new RegExp(BAD));
});

test('config-local lane with no declared reason is stamped unaccepted and warned about', async () => {
  const { res, row } = await runLane({ default: { weight: 1 } }, 'default');
  assert.equal(row.configLocal, true);
  assert.equal(row.configLocalAccepted, false);
  assert.equal('configLocalReason' in row, false);
  assert.match(res.stderr, /declares no localReason/);
});

test('config-local lane with an accepted reason is stamped accepted and not warned about', async () => {
  const { res, row } = await runLane({ default: { weight: 1, localReason: GOOD } }, 'default');
  assert.deepEqual([row.configLocal, row.configLocalReason, row.configLocalAccepted], [true, GOOD, true]);
  assert.doesNotMatch(res.stderr, /warning/);
});

for (const lane of ['prepush', 'prepush-web']) {
  test(`${lane} lane is not stamped or warned about`, async () => {
    const { res, row } = await runLane({ default: { weight: 1 }, [lane]: { weight: 1 } }, lane);
    assert.equal('configLocal' in row, false);
    assert.doesNotMatch(res.stderr, /warning/);
  });
}

test('a remote:true lane run locally is not stamped', async () => {
  const { res, row } = await runLane({ default: { weight: 1, remote: true } }, 'default');
  assert.equal('configLocal' in row, false);
  assert.doesNotMatch(res.stderr, /warning/);
});

test('historyRow carries the config-local fields on a local row only', () => {
  const ticket = { id: 'a', key: 'r:default', repoId: 'r', lane: 'default', configLocal: true, configLocalReason: BAD, configLocalAccepted: false };
  const local = historyRow(ticket, { exit: 0 });
  assert.deepEqual([local.configLocal, local.configLocalReason, local.configLocalAccepted], [true, BAD, false]);
  const remote = historyRow(ticket, { exit: 0, executor: 'remote' });
  assert.equal('configLocal' in remote, false);
  assert.equal('configLocal' in historyRow({ ...ticket, configLocal: undefined }, { exit: 0 }), false);
});
