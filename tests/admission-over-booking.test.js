// BRAIN-695 S2: admission judges a new lane against the machine's REAL usage when running leases use more than they booked.
// A lane a little over its own booking on an idle host is harmless; a host at 80% real use under 50% of bookings must refuse a 25% request.
import test from 'node:test';
import assert from 'node:assert/strict';
import { evaluateCpuAdmission, OBSERVATION_TTL_MS } from '../src/admission.js';
import { evaluateMemoryAdmission } from '../src/resources.js';

const GiB = 1024 ** 3;
const now = Date.now();
const OPEN = { cpuGateState: { closed: false }, cooldownBlocked: false };
const CFG = { cpuAdmissionPercent: 100, cpuReserveCores: 0 };

function admit({ cores, hostBusyCores, heldLeases, candidateWeight, candidateRef, cfg = CFG }) {
  return evaluateCpuAdmission({ cpuSample: { cores, hostBusyCores, stale: false }, heldLeases, candidateWeight, candidateRef, ...OPEN, cfg, now });
}

// two leases booking 5 cores each (10 of 20 = 50%) that really use 8 each (16 = 80%)
const overBooked = (extra = {}, observedAt = now) => [
  { id: 'a', weight: 5, observedCpuCores: 8, observedAt, ...extra },
  { id: 'b', weight: 5, observedCpuCores: 8, observedAt, ...extra },
];

test('booked 50% using 80% denies a 25% request', () => {
  const result = admit({ cores: 20, hostBusyCores: 16, heldLeases: overBooked(), candidateWeight: 5 });
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'projected-over-budget');
  assert.equal(result.externalBusy, 0);
  assert.equal(result.projectedBusy, 21, 'observed 16 + candidate 5, not booked 10 + candidate 5');
});

test('a lane over its own booking on an idle host does not block', () => {
  const heldLeases = [{ id: 'a', weight: 4, observedCpuCores: 5, observedAt: now }];
  const result = admit({ cores: 20, hostBusyCores: 5, heldLeases, candidateWeight: 4 });
  assert.equal(result.admit, true);
  assert.equal(result.projectedBusy, 9);
});

test('memory used above booking shrinks headroom', () => {
  const heldLeases = [1, 2].map((n) => ({ id: String(n), weight: 1, resources: { cpuCores: 1, memoryBytes: 25 * GiB }, observedMemoryBytes: 40 * GiB, observedAt: now }));
  const result = evaluateMemoryAdmission({
    memoryInfo: { totalBytes: 100 * GiB, availableBytes: 20 * GiB },
    heldLeases,
    candidateResources: { cpuCores: 1, memoryBytes: 25 * GiB },
    cfg: { defaultMemoryBytesPerWeight: GiB, memoryReserveBytes: 0 },
    now,
  });
  assert.equal(result.reason, 'memory-headroom', 'bookings (50 + 25) fit the total; only real availability refuses');
  assert.equal(result.admit, false);
});

test('stale observation falls back to host busy', () => {
  const stale = now - (OBSERVATION_TTL_MS + 1);
  const result = admit({ cores: 20, hostBusyCores: 16, heldLeases: overBooked({}, stale), candidateWeight: 5 });
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'projected-over-budget');
  assert.equal(result.externalBusy, 16, 'unobserved leases leave the host busy as external load');
});

test('documented exception: outranked lower-priority lease is discounted', () => {
  // INTENTIONAL (BRAIN-627): leases the kernel runs behind the candidate (nice 19 > 0, tier low < high) yield to it, so only
  // (1 - preemptibleShare) of their real 16 cores is charged: 3.2 + 5 = 8.2 <= 20. The same numbers admit-deny flip on priority alone.
  const cfg = { ...CFG, preemptibleNiceMin: 10, preemptibleShare: 0.8 };
  const heldLeases = overBooked({ nice: 19, priorityAdmitted: 'low' });
  const candidateRef = { nice: 0, priorityAdmitted: 'high' };
  const result = admit({ cores: 20, hostBusyCores: 16, heldLeases, candidateWeight: 5, candidateRef, cfg });
  assert.equal(result.admit, true);
  assert.ok(Math.abs(result.projectedBusy - 8.2) < 1e-9, `projectedBusy ${result.projectedBusy}`);
  const equalRank = admit({ cores: 20, hostBusyCores: 16, heldLeases, candidateWeight: 5, candidateRef: { nice: 0, priorityAdmitted: 'low' }, cfg });
  assert.equal(equalRank.admit, false, 'no discount without outranking');
});
