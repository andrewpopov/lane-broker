import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { freshEnv } from './helpers.js';
import { paths, withLock, __test } from '../src/state.js';
import { queuedPollDelayMs } from '../src/supervisor.js';

const { createBusyBackoff } = __test;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function countStagingDirs() {
  const original = fs.mkdirSync;
  const counter = { count: 0, stop: () => (fs.mkdirSync = original) };
  fs.mkdirSync = (target, ...rest) => {
    if (path.basename(String(target)).startsWith('.lock-stage-')) counter.count += 1;
    return original(target, ...rest);
  };
  return counter;
}

test('contenders of a held lock create no staging dirs while its owner is alive', async () => {
  const { state } = freshEnv();
  const waiters = [];
  let stagedWhileHeld;
  await withLock(state, async () => {
    const counter = countStagingDirs();
    try {
      for (let i = 0; i < 20; i += 1) waiters.push(withLock(state, () => undefined, { timeoutMs: 30_000 }));
      await sleep(1000);
      stagedWhileHeld = counter.count;
    } finally {
      counter.stop();
    }
  });
  await Promise.all(waiters);
  assert.equal(stagedWhileHeld, 0, 'staging dirs created while the lock dir was held by a live owner');
});

test('a contender still acquires a lock whose owner is dead', async () => {
  const { state } = freshEnv();
  const dead = spawn(process.execPath, ['-e', 'process.exit(0)']);
  await new Promise((resolve) => dead.on('exit', resolve));
  fs.mkdirSync(paths(state).lock, { recursive: true });
  fs.writeFileSync(path.join(paths(state).lock, 'owner.json'), JSON.stringify({ pid: dead.pid, token: 'dead', start: null, at: Date.now() }));
  const got = await withLock(state, () => 'acquired', { timeoutMs: 5000 });
  assert.equal(got, 'acquired');
});

test('busy backoff doubles from pollMs, is capped at 250ms, and is fully jittered', () => {
  const ceilings = (random) => {
    const backoff = createBusyBackoff(25, random);
    return Array.from({ length: 7 }, () => backoff.next('holder-a'));
  };
  assert.deepEqual(ceilings(() => 1), [25, 50, 100, 200, 250, 250, 250]);
  assert.deepEqual(ceilings(() => 0.5), [12.5, 25, 50, 100, 125, 125, 125]);
  assert.deepEqual(ceilings(() => 0), [0, 0, 0, 0, 0, 0, 0]);
});

test('busy backoff restarts when the holder changes or on reset', () => {
  const backoff = createBusyBackoff(25, () => 1);
  assert.deepEqual([backoff.next('a'), backoff.next('a'), backoff.next('a')], [25, 50, 100]);
  assert.equal(backoff.next('b'), 25, 'a new holder restarts the ladder');
  assert.equal(backoff.next('b'), 50);
  backoff.reset();
  assert.equal(backoff.next('b'), 25, 'reset restarts the ladder');
});

test('deep-queued waiters poll admission at most 4x sampleMs, jittered +-20%', () => {
  const notHead = (position) => ({ started: false, reason: 'not-head', position });
  const at = (started, random) => queuedPollDelayMs(started, 5000, () => random);
  assert.equal(at(notHead(1), 0.5), 5000);
  assert.equal(at(notHead(8), 0.5), 5000);
  assert.equal(at(notHead(9), 0.5), 10_000);
  assert.equal(at(notHead(30), 0.5), 20_000);
  assert.equal(at(notHead(500), 0.5), 20_000);
  assert.ok(Math.abs(at(notHead(30), 0) - 16_000) < 1e-6);
  assert.ok(Math.abs(at(notHead(30), 1) - 24_000) < 1e-6);
  assert.ok(Math.abs(at(notHead(1), 0) - 4000) < 1e-6);
  assert.ok(Math.abs(at(notHead(1), 1) - 6000) < 1e-6);
  assert.equal(at({ started: false, reason: 'capacity' }, 0.5), 5000);
  assert.equal(at({ started: false, reason: 'not-head', position: null }, 0.5), 5000);
});
