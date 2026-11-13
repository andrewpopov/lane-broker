import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { laneRun, BIN } from './helpers.js';
import { isExecutionId, executionIdOf, parseExecutionId, tombstonePath } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';
import { reapSpawned, setupRunner, makeHeader, execRemote, readExecResult, ticketPath } from './helpers/remote-exec-fixture.js';

/** BRAIN-1035 S2a (T1): `remote-exec --epoch N` names the ticket dir, tombstone and tmp owner `<uuid>.e<N>`; without it nothing changes. */

afterEach(reapSpawned);

const UUID = '11111111-2222-3333-4444-555555555555';

test('execution ids: a uuid or uuid.e<positive int> only, and they parse back to the logical ticket', () => {
  assert.equal(executionIdOf(UUID, 2), `${UUID}.e2`);
  assert.deepEqual(parseExecutionId(`${UUID}.e12`), { ticketId: UUID, epoch: 12 });
  assert.deepEqual(parseExecutionId(UUID), { ticketId: UUID, epoch: null });
  for (const bad of [`${UUID}.e0`, `${UUID}.e02`, `${UUID}.e`, `${UUID}.x2`, `../${UUID}`, `${UUID}.e1/..`, 'x', null, 7]) {
    assert.equal(isExecutionId(bad), false, String(bad));
    assert.equal(parseExecutionId(bad), null, String(bad));
  }
  assert.throws(() => executionIdOf(UUID, 0));
  assert.throws(() => executionIdOf('not-a-uuid', 1));
  assert.throws(() => executionIdOf(UUID, 1e15), 'a 16-digit epoch would name a dir the readers skip');
  assert.equal(isExecutionId(executionIdOf(UUID, 999_999_999_999_999)), true);
});

test('the same ticket at e1 then e2 on one host gives two dirs and two results carrying their epoch; a tombstone of e1 does not block e2', async () => {
  const ctx = setupRunner();
  const h = makeHeader('process.exit(0)');
  const tombstoned = `${h.ticketId}.e1`;
  fs.mkdirSync(path.dirname(tombstonePath(ctx.root, tombstoned)), { recursive: true });
  fs.writeFileSync(tombstonePath(ctx.root, tombstoned), '1');
  const refused = await execRemote(ctx, h, { extraArgs: ['--epoch', '1'] }).closed;
  assert.equal(refused.code, 1, refused.stderr);
  assert.equal(fs.existsSync(path.join(ctx.root, 'tickets', tombstoned)), false, 'a tombstoned execution is never created');

  assert.equal((await execRemote(ctx, h, { extraArgs: ['--epoch', '2'] }).closed).code, 0);
  assert.equal((await execRemote(ctx, h, { extraArgs: ['--epoch', '3'] }).closed).code, 0);
  for (const epoch of [2, 3]) {
    const result = readExecResult(ctx, `${h.ticketId}.e${epoch}`);
    assert.equal(result.kind, 'completed');
    assert.equal(result.epoch, epoch);
    assert.equal(result.ticketId, h.ticketId, 'the result binds to the logical ticket uuid');
  }
  assert.equal(fs.existsSync(path.join(ctx.root, 'tickets', h.ticketId)), false, 'no bare-uuid dir for an epoch run');
  const tmpDir = fs.readFileSync(ticketPath(ctx, `${h.ticketId}.e2`, 'tmp-dir'), 'utf8').trim();
  assert.ok(tmpDir.length > 0);
});

test('without --epoch the dir is the bare uuid and the result carries none of the new fields', async () => {
  const ctx = setupRunner();
  const h = makeHeader('process.exit(0)');
  assert.equal((await execRemote(ctx, h).closed).code, 0);
  const pushResult = readExecResult(ctx, h.ticketId);
  assert.equal(pushResult.kind, 'completed');
  for (const key of ['epoch', 'neverStarted', 'stopProven', 'shutdownProof']) assert.equal(key in pushResult, false, key);

  const h2 = makeHeader('process.exit(0)');
  assert.equal((await execRemote(ctx, h2, { extraArgs: ['--epoch', '1'] }).closed).code, 0);
  const { epoch, ...rest } = readExecResult(ctx, `${h2.ticketId}.e1`);
  assert.equal(epoch, 1);
  assert.deepEqual(Object.keys(rest), Object.keys(pushResult), 'an epoch result is a push result plus `epoch`');
});

test('remote-result, remote-cancel, remote-artifacts-release accept an execution id; remote-withdraw stays uuid-only; a bad epoch flag is refused', async () => {
  const ctx = setupRunner();
  const h = makeHeader('process.exit(0)');
  const execId = `${h.ticketId}.e4`;
  await execRemote(ctx, h, { extraArgs: ['--epoch', '4'] }).closed;
  const served = JSON.parse((await laneRun(['remote-result', execId, '--root', ctx.root], { env: ctx.env })).stdout);
  assert.equal(served.epoch, 4);
  const cancel = await laneRun(['remote-cancel', execId, '--root', ctx.root], { env: ctx.env });
  assert.equal(cancel.code, 0, cancel.stderr);
  assert.equal(JSON.parse(cancel.stdout).cancelConfirmed, true);
  assert.equal((await laneRun(['remote-artifacts-release', execId, '--root', ctx.root], { env: ctx.env })).code, 0);
  assert.equal((await laneRun(['remote-withdraw', execId, '--root', ctx.root], { env: ctx.env })).code, 2);
  assert.equal((await laneRun(['remote-exec', '--root', ctx.root, '--epoch', '0'], { env: ctx.env })).code, 2);
  assert.equal((await laneRun(['remote-exec', '--root', ctx.root, '--epoch'], { env: ctx.env })).code, 2);
  const sixteen = spawnSync(process.execPath, [BIN, 'remote-exec', '--root', ctx.root, '--epoch', '1000000000000000'], { env: ctx.env, input: '', timeout: 20_000 });
  assert.equal(sixteen.status, 2, 'a 16-digit epoch is refused');
});

test('--runner-name and --runner-speed-factor override the header fields and are validated by the header rules', async () => {
  const ctx = setupRunner();
  const seen = path.join(makeTmpDir('exec-env-'), 'env.json');
  const probe = `require('fs').writeFileSync(${JSON.stringify(seen)}, JSON.stringify([process.env.LANE_BROKER_RUNNER, process.env.LANE_BROKER_RUNNER_SPEED_FACTOR]))`;
  const h = makeHeader(probe, { runnerName: 'submitter-guess', runnerSpeedFactor: 1 });
  assert.equal((await execRemote(ctx, h, { extraArgs: ['--runner-name', 'wintop', '--runner-speed-factor', '1.5'] }).closed).code, 0);
  assert.deepEqual(JSON.parse(fs.readFileSync(seen, 'utf8')), ['wintop', '1.5']);

  const bad = makeHeader('process.exit(0)');
  await execRemote(ctx, bad, { extraArgs: ['--runner-speed-factor', '9'] }).closed;
  assert.equal(readExecResult(ctx, bad.ticketId).reason, 'invalid runnerSpeedFactor');
  const badName = makeHeader('process.exit(0)');
  await execRemote(ctx, badName, { extraArgs: ['--runner-name', 'bad name!'] }).closed;
  assert.equal(readExecResult(ctx, badName.ticketId).reason, 'invalid runnerName');
});
