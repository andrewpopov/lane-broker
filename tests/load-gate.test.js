import test from 'node:test';
import assert from 'node:assert/strict';
import { updateGateState } from '../src/load.js';

const CFG = { loadClose: 15, loadOpen: 11, loadOpenSamples: 3 };

test('load gate closes immediately when load exceeds loadClose', () => {
  const s = updateGateState(null, 20, CFG);
  assert.equal(s.closed, true);
});

test('load gate stays open below loadClose when never closed', () => {
  const s = updateGateState(null, 12, CFG);
  assert.equal(s.closed, false);
});

test('load gate requires loadOpenSamples consecutive samples under loadOpen to reopen', () => {
  let s = updateGateState(null, 20, CFG); // closes
  assert.equal(s.closed, true);
  s = updateGateState(s, 5, CFG); // 1 of 3
  assert.equal(s.closed, true);
  s = updateGateState(s, 5, CFG); // 2 of 3
  assert.equal(s.closed, true);
  s = updateGateState(s, 5, CFG); // 3 of 3 -> reopen
  assert.equal(s.closed, false);
});

test('a sample between loadOpen and loadClose resets the consecutive-under counter without reopening', () => {
  let s = updateGateState(null, 20, CFG); // closed
  s = updateGateState(s, 5, CFG); // 1 of 3 under
  s = updateGateState(s, 13, CFG); // between open/close -> reset counter, stays closed
  assert.equal(s.closed, true);
  assert.equal(s.consecutiveUnder, 0);
  s = updateGateState(s, 5, CFG);
  s = updateGateState(s, 5, CFG);
  assert.equal(s.closed, true); // only 2 of 3 so far
  s = updateGateState(s, 5, CFG);
  assert.equal(s.closed, false); // 3 of 3
});

test('a spike back above loadClose while reopening re-closes and resets the counter', () => {
  let s = updateGateState(null, 20, CFG);
  s = updateGateState(s, 5, CFG);
  s = updateGateState(s, 5, CFG);
  s = updateGateState(s, 20, CFG); // spike re-closes
  assert.equal(s.closed, true);
  assert.equal(s.consecutiveUnder, 0);
});

test('a threshold change mid-countdown restarts the count under the new thresholds rather than blending them', () => {
  let s = updateGateState(null, 20, CFG); // closes
  s = updateGateState(s, 5, CFG); // 1 of 3 under the original loadOpen (11)
  s = updateGateState(s, 5, CFG); // 2 of 3
  assert.equal(s.consecutiveUnder, 2);
  // Another supervisor reloads a raised loadOpen mid-countdown. A stale
  // supervisor still using the old fingerprint must not get to treat this
  // sample as "3 of 3" and reopen the gate under a config nobody installed.
  const RAISED = { loadClose: 40, loadOpen: 30, loadOpenSamples: 3 };
  s = updateGateState(s, 5, RAISED);
  assert.equal(s.consecutiveUnder, 1, 'the countdown must restart at 1, not continue to 3');
  assert.equal(s.closed, true, 'a threshold change alone must never reopen the gate');
});

test('a threshold change does not reopen a gate on its own even when the counter had already reached the sample count', () => {
  let s = updateGateState(null, 20, CFG); // closes
  s = updateGateState(s, 5, CFG);
  s = updateGateState(s, 5, CFG);
  s = updateGateState(s, 5, CFG); // 3 of 3 -> reopens under CFG
  assert.equal(s.closed, false);
  // Re-close it, then verify a bare threshold change (no new sample beyond
  // the reset) cannot itself flip `closed`.
  s = updateGateState(s, 50, CFG); // closes again
  const RAISED = { loadClose: 40, loadOpen: 30, loadOpenSamples: 3 };
  s = updateGateState(s, 20, RAISED); // between old and new open thresholds
  assert.equal(s.closed, true, 'closed must reflect the load sample and countdown, never the fingerprint change alone');
  assert.equal(s.consecutiveUnder, 1);
});
