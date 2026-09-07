import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, laneRun } from './helpers.js';
import { paths, processStartTime } from '../src/state.js';

function plantLiveLock(state) {
  const lockDir = paths(state).lock;
  fs.mkdirSync(lockDir, { recursive: true });
  const start = processStartTime(process.pid);
  fs.writeFileSync(
    path.join(lockDir, 'owner.json'),
    JSON.stringify({ pid: process.pid, token: 'planted-live-holder', start: start === undefined ? null : start, at: Date.now() }),
  );
}

test('lane status still prints the sections and fails loudly when the lock cannot be taken', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  // Our own process is a live owner (it never dies), so the 5s lock wait
  // inside statusCommand must time out rather than take over the lock.
  plantLiveLock(state);

  const result = await laneRun(['status'], { env: { ...process.env, LANE_BROKER_HOME: home, LANE_BROKER_STATE: state } });

  assert.equal(result.code, 1, `expected exit 1; got ${result.code}, stderr: ${result.stderr}`);
  assert.match(result.stderr, /could not take the broker lock within 5s/);
  assert.match(result.stdout, /capacity:/, 'the read-only view must still be rendered');
  assert.match(result.stdout, /QUEUE/, 'the read-only view must still be rendered');
}, { timeout: 15_000 });

test('lane status --json also prints the lock diagnostic on stderr, not only lockError in the payload', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  plantLiveLock(state);

  const result = await laneRun(['status', '--json'], { env: { ...process.env, LANE_BROKER_HOME: home, LANE_BROKER_STATE: state } });

  assert.equal(result.code, 1, `expected exit 1; got ${result.code}, stderr: ${result.stderr}`);
  assert.match(result.stderr, /could not take the broker lock within 5s/, '--json mode must warn on stderr too');
  const payload = JSON.parse(result.stdout);
  assert.equal(payload.capacity, 4);
  assert.ok(payload.lockError && /timed out/.test(payload.lockError.message), 'lockError must be in the JSON payload');
}, { timeout: 15_000 });

test('lane status on a fresh empty state prints the sections and exits 0', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });

  const result = await laneRun(['status'], { env: { ...process.env, LANE_BROKER_HOME: home, LANE_BROKER_STATE: state } });

  assert.equal(result.code, 0, `expected exit 0; got ${result.code}, stderr: ${result.stderr}`);
  assert.match(result.stdout, /capacity:/);
  assert.match(result.stdout, /QUEUE/);
});
