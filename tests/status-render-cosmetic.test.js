import test from 'node:test';
import assert from 'node:assert/strict';
import { renderStatusText } from '../src/status.js';

function baseStatus(overrides = {}) {
  return {
    capacity: 4,
    used: 0,
    paused: null,
    configWarning: null,
    headBlock: null,
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

// --- BRAIN-249: a stalled queue must say so, instead of an hours-old gate
// sample rendering as ordinary current state. ---

test('lane status flags a stale gate sample instead of rendering it as current', () => {
  const text = renderStatusText(
    baseStatus({
      loadGate: {
        closed: false,
        lastLoad: 12.35,
        sampleAgeMs: 173 * 60 * 1000, // the live BRAIN-249 incident: ~173m old
        consecutiveUnder: 0,
        loadClose: 15,
        loadOpen: 11,
        loadOpenSamples: 3,
      },
    }),
  );
  assert.match(text, /load gate: open \(load 12\.35, sampled 173m0s ago.*STALE/);
  assert.match(text, /samples are taken when a ticket reaches the queue head/);
});

test('lane status does not flag a fresh gate sample as stale', () => {
  const text = renderStatusText(
    baseStatus({ loadGate: { closed: false, lastLoad: 4.2, sampleAgeMs: 1000, consecutiveUnder: 0, loadClose: 15, loadOpen: 11, loadOpenSamples: 3 } }),
  );
  assert.doesNotMatch(text, /STALE/);
});

test('lane status names the head, the blocking lease, and that backfill is refused while inside the grace period', () => {
  const text = renderStatusText(
    baseStatus({
      headBlock: {
        headId: 'head-abc',
        blockingLeaseId: 'holder-xyz',
        blockingKey: 'rouge:sim',
        skipCount: 3,
        skipLimit: 3,
        graceMs: 600_000,
        blockedMs: 60_000,
        refused: true,
      },
    }),
  );
  assert.match(text, /queue stalled: head head-abc is blocked by lease holder-xyz \(key rouge:sim\)/);
  assert.match(text, /skip allowance exhausted \(3\/3\)/);
  assert.match(text, /backfill refused for 9m0s more/);
});

test('lane status reports the grace period lapsed and backfill resumed once it has', () => {
  const text = renderStatusText(
    baseStatus({
      headBlock: {
        headId: 'head-abc',
        blockingLeaseId: 'holder-xyz',
        blockingKey: 'rouge:sim',
        skipCount: 3,
        skipLimit: 3,
        graceMs: 600_000,
        blockedMs: 700_000,
        refused: false,
      },
    }),
  );
  assert.match(text, /queue: head head-abc is still blocked by lease holder-xyz \(key rouge:sim\)/);
  assert.match(text, /grace period has lapsed — backfill resumed past it/);
});
