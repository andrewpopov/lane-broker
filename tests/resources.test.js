import test from 'node:test';
import assert from 'node:assert/strict';
import {
  detectResourceCapacity,
  evaluateMemoryAdmission,
  parseByteSize,
  parseCpuMax,
  parseCpuSet,
  parseVmStat,
  resolveTicketResources,
} from '../src/resources.js';

const GiB = 1024 ** 3;

// Verbatim sample from BRAIN-253's observed incident: page size 16384,
// available = (107717 + 1173404 + 10276 + 17396) * 16384 bytes ≈ 19.97 GiB.
const VM_STAT_16K = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                   107717.
Pages active:                                1174123.
Pages inactive:                              1173404.
Pages speculative:                             10276.
Pages throttled:                                   0.
Pages wired down:                             249487.
Pages purgeable:                               17396.
`;

const VM_STAT_4K = `Mach Virtual Memory Statistics: (page size of 4096 bytes)
Pages free:                                   200000.
Pages active:                                 500000.
Pages inactive:                               300000.
Pages speculative:                             50000.
Pages throttled:                                   0.
Pages wired down:                             100000.
Pages purgeable:                               20000.
`;

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

test('parseVmStat sums free+inactive+speculative+purgeable, using the page size from the header', () => {
  const bytes = parseVmStat(VM_STAT_16K);
  const expected = (107717 + 1173404 + 10276 + 17396) * 16384;
  assert.equal(bytes, expected);
  assert.ok(Math.abs(bytes / GiB - 19.97) < 0.01, `expected ~19.97 GiB, got ${bytes / GiB} GiB`);
});

test('parseVmStat reads the page size instead of assuming 4096', () => {
  const bytes4k = parseVmStat(VM_STAT_4K);
  const expected4k = (200000 + 300000 + 50000 + 20000) * 4096;
  assert.equal(bytes4k, expected4k);
  // Re-parsing the 16K fixture as if page size were hardcoded to 4096 would
  // produce a wildly different (4x smaller) figure than the real parse.
  const bytes16k = parseVmStat(VM_STAT_16K);
  assert.notEqual(bytes16k, (107717 + 1173404 + 10276 + 17396) * 4096);
});

test('parseVmStat returns null on unparseable input instead of throwing', () => {
  assert.equal(parseVmStat(''), null);
  assert.equal(parseVmStat('Pages free: 100.'), null, 'missing page-size header');
  assert.equal(
    parseVmStat('Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 100.\n'),
    null,
    'truncated: missing inactive/speculative/purgeable fields',
  );
  assert.equal(
    parseVmStat(
      'Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: notanumber.\nPages inactive: 1.\nPages speculative: 1.\nPages purgeable: 1.\n',
    ),
    null,
    'non-numeric page count',
  );
  assert.equal(parseVmStat(undefined), null);
  assert.equal(parseVmStat(null), null);
});

test('detectResourceCapacity on darwin uses vm_stat when it parses, and names the source', () => {
  const snapshot = detectResourceCapacity({
    platform: 'darwin',
    parallelism: 12,
    totalMemory: 48 * GiB,
    freeMemory: 1.67 * GiB,
    exec: () => VM_STAT_16K,
  });
  const expected = (107717 + 1173404 + 10276 + 17396) * 16384;
  assert.equal(snapshot.availableMemoryBytes, expected);
  assert.ok(snapshot.source.includes('vm_stat'), `expected source to name vm_stat, got ${snapshot.source}`);
});

test('detectResourceCapacity on darwin falls back to os.freemem() when exec throws, and does not claim vm_stat', () => {
  const snapshot = detectResourceCapacity({
    platform: 'darwin',
    parallelism: 12,
    totalMemory: 48 * GiB,
    freeMemory: 1.67 * GiB,
    exec: () => {
      throw new Error('spawnSync vm_stat ENOENT');
    },
  });
  assert.equal(snapshot.availableMemoryBytes, 1.67 * GiB);
  assert.ok(!snapshot.source.includes('vm_stat'), `expected source not to name vm_stat, got ${snapshot.source}`);
});

test('detectResourceCapacity on linux never calls the darwin exec', () => {
  let called = false;
  const spyExec = () => {
    called = true;
    return VM_STAT_16K;
  };
  const files = new Map([
    ['/sys/fs/cgroup/cpu.max', '250000 100000'],
    ['/sys/fs/cgroup/memory.max', String(8 * GiB)],
    ['/sys/fs/cgroup/memory.current', String(3 * GiB)],
  ]);
  detectResourceCapacity({
    platform: 'linux',
    parallelism: 16,
    totalMemory: 32 * GiB,
    freeMemory: 20 * GiB,
    readFile: (file) => {
      if (!files.has(file)) throw new Error('missing');
      return files.get(file);
    },
    exec: spyExec,
  });
  assert.equal(called, false, 'the darwin vm_stat exec must never be invoked on linux');
});

test('evaluateMemoryAdmission admits a default-sized (2 GiB) candidate given real macOS headroom and normal pressure', () => {
  const cfg = { defaultMemoryBytesPerWeight: GiB, memoryReserveBytes: 2 * GiB };
  const result = evaluateMemoryAdmission({
    memoryInfo: { totalBytes: 48 * GiB, availableBytes: 19.97 * GiB, macPressure: 'normal' },
    heldLeases: [],
    candidateResources: { cpuCores: 2, memoryBytes: 2 * GiB },
    cfg,
  });
  assert.equal(result.admit, true, `expected admission, got deny:${result.reason}`);
});

test('evaluateMemoryAdmission denies on macOS critical pressure with its own distinct reason, even when bytes look fine', () => {
  const cfg = { defaultMemoryBytesPerWeight: GiB, memoryReserveBytes: GiB };
  const result = evaluateMemoryAdmission({
    memoryInfo: { totalBytes: 48 * GiB, availableBytes: 40 * GiB, macPressure: 'critical' },
    heldLeases: [],
    candidateResources: { cpuCores: 1, memoryBytes: GiB },
    cfg,
  });
  assert.equal(result.admit, false);
  assert.equal(result.reason, 'memory-pressure-critical');
});

test('evaluateMemoryAdmission does not veto on macOS warn pressure alone; the byte checks still apply', () => {
  const cfg = { defaultMemoryBytesPerWeight: GiB, memoryReserveBytes: 2 * GiB };
  const admitted = evaluateMemoryAdmission({
    memoryInfo: { totalBytes: 48 * GiB, availableBytes: 19.97 * GiB, macPressure: 'warn' },
    heldLeases: [],
    candidateResources: { cpuCores: 2, memoryBytes: 2 * GiB },
    cfg,
  });
  assert.equal(admitted.admit, true, `expected 'warn' alone not to deny, got deny:${admitted.reason}`);

  const denied = evaluateMemoryAdmission({
    memoryInfo: { totalBytes: 48 * GiB, availableBytes: 2.5 * GiB, macPressure: 'warn' },
    heldLeases: [],
    candidateResources: { cpuCores: 2, memoryBytes: 2 * GiB },
    cfg,
  });
  assert.equal(denied.admit, false, "'warn' still lets the ordinary byte-headroom check deny when headroom is actually tight");
  assert.equal(denied.reason, 'memory-headroom');
});
