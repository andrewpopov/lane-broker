import test from 'node:test';
import assert from 'node:assert/strict';
import {
  detectResourceCapacity,
  evaluateMemoryAdmission,
  parseByteSize,
  parseCpuMax,
  parseCpuSet,
  resolveTicketResources,
} from '../src/resources.js';

test('resource parsers accept human sizes, fractional quotas, and CPU sets', () => {
  assert.equal(parseByteSize('1.5GiB'), 1.5 * 1024 ** 3);
  assert.equal(parseByteSize('512mb'), 512_000_000);
  assert.equal(parseByteSize('nope'), null);
  assert.equal(parseCpuMax('150000 100000'), 1.5);
  assert.equal(parseCpuMax('max 100000'), null);
  assert.equal(parseCpuSet('0-3,6,8-9'), 7);
});

test('Linux capacity honors cgroup v2 CPU, cpuset, and memory limits', () => {
  const files = new Map([
    ['/sys/fs/cgroup/cpu.max', '250000 100000'],
    ['/sys/fs/cgroup/cpuset.cpus.effective', '0-3'],
    ['/sys/fs/cgroup/memory.max', String(8 * 1024 ** 3)],
    ['/sys/fs/cgroup/memory.current', String(3 * 1024 ** 3)],
  ]);
  const snapshot = detectResourceCapacity({
    platform: 'linux',
    parallelism: 16,
    totalMemory: 32 * 1024 ** 3,
    freeMemory: 20 * 1024 ** 3,
    readFile: (file) => {
      if (!files.has(file)) throw new Error('missing');
      return files.get(file);
    },
  });
  assert.equal(snapshot.cpuCores, 2.5);
  assert.equal(snapshot.memoryBytes, 8 * 1024 ** 3);
  assert.equal(snapshot.availableMemoryBytes, 5 * 1024 ** 3);
});

test('Linux capacity follows nested cgroup v2 membership and ancestor limits', () => {
  const GiB = 1024 ** 3;
  const files = new Map([
    ['/proc/self/cgroup', '0::/user.slice/lane.scope\n'],
    ['/sys/fs/cgroup/user.slice/lane.scope/cpu.max', '200000 100000'],
    ['/sys/fs/cgroup/user.slice/lane.scope/cpuset.cpus.effective', '4-7'],
    ['/sys/fs/cgroup/user.slice/lane.scope/memory.max', String(6 * GiB)],
    ['/sys/fs/cgroup/user.slice/lane.scope/memory.current', String(2 * GiB)],
    ['/sys/fs/cgroup/user.slice/memory.max', String(8 * GiB)],
    ['/sys/fs/cgroup/user.slice/memory.current', String(5 * GiB)],
  ]);
  const snapshot = detectResourceCapacity({
    platform: 'linux',
    parallelism: 16,
    totalMemory: 32 * GiB,
    freeMemory: 20 * GiB,
    readFile: (file) => {
      if (!files.has(file)) throw new Error('missing');
      return files.get(file);
    },
  });
  assert.equal(snapshot.cpuCores, 2);
  assert.equal(snapshot.memoryBytes, 6 * GiB);
  assert.equal(snapshot.availableMemoryBytes, 3 * GiB, 'the tighter ancestor remaining-memory limit wins');
});

test('ticket resources preserve weight as CPU and memory compatibility estimates', () => {
  assert.deepEqual(resolveTicketResources({ weight: 2, defaultMemoryBytesPerWeight: 1024 }), {
    cpuCores: 2,
    memoryBytes: 2048,
  });
  assert.deepEqual(resolveTicketResources({ weight: 2, cpuCores: 1.5, memoryBytes: 4096 }), {
    cpuCores: 1.5,
    memoryBytes: 4096,
  });
});

test('memory admission accounts for reservations and live available headroom', () => {
  const GiB = 1024 ** 3;
  const cfg = { defaultMemoryBytesPerWeight: GiB, memoryReserveBytes: GiB };
  const result = evaluateMemoryAdmission({
    memoryInfo: { totalBytes: 8 * GiB, availableBytes: 5 * GiB },
    heldLeases: [{ weight: 2, resources: { cpuCores: 2, memoryBytes: 2 * GiB }, observedMemoryBytes: GiB, observedAt: Date.now() }],
    candidateResources: { cpuCores: 2, memoryBytes: 3 * GiB },
    cfg,
  });
  assert.equal(result.admit, true);
  assert.equal(result.projectedAvailableBytes, GiB);
});
