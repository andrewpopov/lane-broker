import test from 'node:test';
import assert from 'node:assert/strict';
import { buildResult } from '../src/remote-runner.js';
import { relayedUsage } from '../src/remote-relay.js';
import { observedLeaseFields } from '../src/observed.js';

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
