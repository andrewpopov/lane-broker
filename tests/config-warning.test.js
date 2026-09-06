import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { paths, readJsonSafe } from '../src/state.js';
import { recordConfigReloadError, clearConfigReloadWarning } from '../src/supervisor.js';

/**
 * recordConfigReloadError/clearConfigReloadWarning back the operational
 * visibility for a persistently broken global config (surfaced via
 * `lane status`): a single message must not spam stderr on every poll, but a
 * distinct message -- or a resolve-then-fail cycle -- must be announced
 * again. `lastConfigErrorMessage` is process-global by design (matching one
 * real supervisor process), so each test clears it first rather than
 * asserting a call count across the whole file.
 */
function captureStderr(fn) {
  const original = process.stderr.write.bind(process.stderr);
  const lines = [];
  process.stderr.write = (chunk) => {
    lines.push(String(chunk));
    return true;
  };
  try {
    fn();
  } finally {
    process.stderr.write = original;
  }
  return lines;
}

test('a persistent config failure is announced once, not once per call', () => {
  const { state } = freshEnv();
  clearConfigReloadWarning(state); // reset any leakage from a prior test in this file
  const err = new Error('config.json: invalid JSON (persistent-test-a)');

  const first = captureStderr(() => recordConfigReloadError(state, err));
  assert.equal(first.length, 1, 'the first occurrence of a message must be announced');
  const warningAfterFirst = readJsonSafe(paths(state).configWarning);
  assert.ok(warningAfterFirst, 'the failure must be persisted so `lane status` can show it');
  assert.equal(warningAfterFirst.message, err.message);

  const second = captureStderr(() => recordConfigReloadError(state, err));
  assert.equal(second.length, 0, 'a repeat of the same message must not be announced again');

  const third = captureStderr(() => recordConfigReloadError(state, err));
  assert.equal(third.length, 0, 'still deduplicated on a third repeat');

  const warningAfterThird = readJsonSafe(paths(state).configWarning);
  assert.equal(warningAfterThird.firstAt, warningAfterFirst.firstAt, 'firstAt is preserved across repeats of the same message');
  assert.ok(warningAfterThird.lastAt >= warningAfterFirst.lastAt, 'lastAt still advances even while deduplicated');
});

test('a distinct message is announced again even while a prior one was already recorded', () => {
  const { state } = freshEnv();
  clearConfigReloadWarning(state);
  const errA = new Error('config.json: invalid JSON (persistent-test-b-a)');
  const errB = new Error('config.json: "loadOpen" must be less than "loadClose" (persistent-test-b-b)');

  captureStderr(() => recordConfigReloadError(state, errA));
  const onB = captureStderr(() => recordConfigReloadError(state, errB));
  assert.equal(onB.length, 1, 'a new, distinct failure message must be announced');

  const warning = readJsonSafe(paths(state).configWarning);
  assert.equal(warning.message, errB.message, 'the persisted record reflects the latest failure');
});

test('clearConfigReloadWarning removes the persisted record and re-arms the announcement', () => {
  const { state } = freshEnv();
  clearConfigReloadWarning(state);
  const err = new Error('config.json: invalid JSON (persistent-test-c)');

  captureStderr(() => recordConfigReloadError(state, err));
  assert.ok(fs.existsSync(paths(state).configWarning));

  clearConfigReloadWarning(state);
  assert.equal(fs.existsSync(paths(state).configWarning), false, 'a resolved config must clear the visible warning');

  // Once cleared, the same message failing again is a new occurrence, not a
  // continuation of the old one -- it must be announced again.
  const again = captureStderr(() => recordConfigReloadError(state, err));
  assert.equal(again.length, 1, 're-announced after the warning was cleared');
});
