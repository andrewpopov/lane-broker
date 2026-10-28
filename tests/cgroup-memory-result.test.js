import test from 'node:test';
import assert from 'node:assert/strict';
import { buildResult } from '../src/remote-runner.js';
import { relayedUsage } from '../src/remote-relay.js';
import { observedLeaseFields } from '../src/observed.js';
import { scopeMemoryPeakTracker } from '../src/cpu-limit.js';

/** BRAIN-695: the scope's measured memory peak travels runner result -> submitter republication, and bad values are dropped, not thrown on. */

const header = { protocol: 1, ticketId: 't', generation: 1 };

test('buildResult carries observedCgroupMemoryPeakBytes into the runner result', () => {
  const result = buildResult(header, '/nonexistent-ticket-dir', { kind: 'completed', exit: 0, observedCgroupMemoryPeakBytes: 123456 });
  assert.equal(result.observedCgroupMemoryPeakBytes, 123456);
});

test('relayedUsage republishes a well-formed cgroup peak and drops a malformed one', () => {
  assert.equal(relayedUsage({ observedCgroupMemoryPeakBytes: 987654 }).observedCgroupMemoryPeakBytes, 987654);
  assert.equal('observedCgroupMemoryPeakBytes' in relayedUsage({ observedCgroupMemoryPeakBytes: 'lots' }), false);
  assert.equal('observedCgroupMemoryPeakBytes' in relayedUsage({}), false);
});

test('the RSS peak and the cgroup peak are separate fields (the fixture differs on purpose)', () => {
  const usage = relayedUsage({ observedRssPeakBytes: 100, observedCgroupMemoryPeakBytes: 900 });
  assert.equal(usage.observedRssPeakBytes, 100);
  assert.equal(usage.observedCgroupMemoryPeakBytes, 900);
  assert.equal(typeof observedLeaseFields, 'function');
});

test('scopeMemoryPeakTracker keeps the highest read, so a spike seen only at exit-time is the published peak', () => {
  const values = [100, null, 900, 400];
  const tracker = scopeMemoryPeakTracker({ read: () => values.shift() });
  assert.equal(tracker.sample(), 100);
  assert.equal(tracker.sample(), 100, 'an unreadable sample keeps the previous peak');
  assert.equal(tracker.sample(), 900, 'the exit-time read is higher');
  assert.equal(tracker.sample(), 900, 'a lower later read never lowers it');
});

test('scopeMemoryPeakTracker: a reader that throws or returns null, or no reader, leaves the peak null', () => {
  assert.equal(scopeMemoryPeakTracker({ read: () => { throw new Error('gone'); } }).sample(), null);
  assert.equal(scopeMemoryPeakTracker({ read: () => null }).sample(), null);
  assert.equal(scopeMemoryPeakTracker(null).sample(), null);
});
