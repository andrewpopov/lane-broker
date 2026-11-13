import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import crypto from 'node:crypto';
import { laneRun, gitFixture } from './helpers.js';
import { setup, tmpDir, markerCmd } from './remote-harness.js';
import { readHistoryRows } from '../src/suggest.js';
import { paths } from '../src/state.js';
import { pipeSnapshot } from '../src/remote-client.js';
import { dispatchOnlyRefusal } from '../src/resources.js';

/** BRAIN-1050: an ssh transfer that broke is exit 76 (merge-train waits on it) but is `failureKind: 'transport'`, not a capacity refusal. */

const fakeSsh = () => Object.assign(new EventEmitter(), { stdin: new PassThrough(), kill() {} });

const dispatchOnlyRun = (ctx) => {
  const file = path.join(ctx.home, 'config.json');
  fs.writeFileSync(file, JSON.stringify({ ...JSON.parse(fs.readFileSync(file, 'utf8')), localExecution: 'refused' }));
  return laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(path.join(tmpDir('marker'), 'where'), 0)], { env: ctx.env, cwd: ctx.repoDir });
};

test('pipeSnapshot tags both ssh transfer failures as transport', async () => {
  const stdinBroke = fakeSsh();
  const pending = pipeSnapshot(stdinBroke, new PassThrough(), 5000);
  stdinBroke.stdin.emit('error', new Error('EPIPE'));
  assert.deepEqual(await pending, { ok: false, reason: 'ssh stdin error', failureKind: 'transport' });

  const closedEarly = fakeSsh();
  const closing = pipeSnapshot(closedEarly, new PassThrough(), 5000);
  closedEarly.emit('close');
  assert.deepEqual(await closing, { ok: false, reason: 'ssh closed before transfer finished', failureKind: 'transport' });
});

test('pipeSnapshot leaves a non-transport failure (a snapshot stream error) untagged', async () => {
  const stream = new PassThrough();
  const pending = pipeSnapshot(fakeSsh(), stream, 5000);
  stream.destroy(new Error('boom'));
  const result = await pending;
  assert.equal(result.ok, false);
  assert.equal('failureKind' in result, false);
});

test('a dispatch-only host whose ssh transfer breaks exits 76 with failureKind transport in the history row, the result and the message', async () => {
  const ctx = setup({ ssh: 'exec-exits-at-once' });
  fs.writeFileSync(path.join(ctx.repoDir, 'big.bin'), crypto.randomBytes(4_000_000));
  gitFixture(['add', '-f', 'big.bin'], ctx.repoDir);
  const res = await dispatchOnlyRun(ctx);
  assert.equal(res.code, 76, res.stderr);
  assert.match(res.stderr, /\(transport: ssh stdin error\)/);

  const row = readHistoryRows(paths(ctx.state).history).at(-1);
  assert.equal(row.exit, 76);
  assert.equal(row.failureKind, 'transport');
  const resultsDir = paths(ctx.state).results;
  const results = fs.readdirSync(resultsDir).map((f) => JSON.parse(fs.readFileSync(path.join(resultsDir, f), 'utf8')));
  assert.deepEqual(results.map((r) => [r.exit, r.failureKind]), [[76, 'transport']]);
});

test('a no-runner refusal that is not a transport failure carries no failureKind', async () => {
  const refusal = dispatchOnlyRefusal('default', { retryable: true, reason: 'no runner has room' });
  assert.equal(refusal.exitCode, 76);
  assert.equal('failureKind' in refusal, false);
  assert.doesNotMatch(refusal.message, /transport/);

  const ctx = setup({ ssh: 'down' });
  const res = await dispatchOnlyRun(ctx);
  assert.equal(res.code, 76, res.stderr);
  const row = readHistoryRows(paths(ctx.state).history).at(-1);
  assert.equal('failureKind' in row, false);
});
