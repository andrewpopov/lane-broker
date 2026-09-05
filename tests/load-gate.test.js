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
