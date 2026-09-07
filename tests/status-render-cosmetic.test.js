import test from 'node:test';
import assert from 'node:assert/strict';
import { renderStatusText } from '../src/status.js';

function baseStatus(overrides = {}) {
  return {
    capacity: 4,
    used: 0,
    paused: null,
    configWarning: null,
    loadGate: {
      closed: false,
      lastLoad: null,
      sampleAgeMs: null,
      consecutiveUnder: 0,
      loadClose: 15,
      loadOpen: 11,
      loadOpenSamples: 3,
    },
    running: [],
    queued: [],
    ...overrides,
  };
}

test('lane status renders a "no sample yet" line before the first gate sample exists', () => {
  const text = renderStatusText(baseStatus());
  assert.match(text, /load gate: open \(no sample yet — samples are taken when a ticket reaches the queue head\)/);
  assert.doesNotMatch(text, /load \?/);
});

test('lane status renders the normal gate line once a sample exists', () => {
  const text = renderStatusText(
    baseStatus({ loadGate: { closed: false, lastLoad: 4.2, sampleAgeMs: 1000, consecutiveUnder: 0, loadClose: 15, loadOpen: 11, loadOpenSamples: 3 } }),
  );
  assert.match(text, /load gate: open \(load 4\.2, sampled 1s ago/);
});
