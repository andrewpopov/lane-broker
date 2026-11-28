import test from 'node:test';
import assert from 'node:assert/strict';
import { readMemoryInfo } from '../src/cpu.js';
import {
  detectResourceCapacity,
  parseTestHostMemory,
  evaluateMemoryAdmission,
  parseByteSize,
  parseCpuMax,
  parseCpuSet,
  resolveTicketResources,
} from '../src/resources.js';

delete process.env.LANE_BROKER_TEST_HOST_CORES;
delete process.env.LANE_BROKER_TEST_HOST_MEMORY;

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

test('LANE_BROKER_TEST_HOST_CORES replaces the machine core count and the cgroup CPU limits, never a caller\'s explicit parallelism, and leaves memory alone', () => {
  const files = new Map([
    ['/sys/fs/cgroup/cpu.max', '100000 100000'],
    ['/sys/fs/cgroup/cpuset.cpus.effective', '0'],
    ['/sys/fs/cgroup/memory.max', String(8 * 1024 ** 3)],
  ]);
  const readFile = (file) => {
    if (!files.has(file)) throw new Error('missing');
    return files.get(file);
  };
  const snapshot = detectResourceCapacity({
    platform: 'linux',
    totalMemory: 32 * 1024 ** 3,
    readFile,
    testHostCores: 12,
  });
  assert.equal(snapshot.cpuCores, 12);
  assert.equal(snapshot.memoryBytes, 8 * 1024 ** 3);
  assert.equal(detectResourceCapacity({ platform: 'linux', parallelism: 3, readFile, testHostCores: 12 }).cpuCores, 3);
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

const SAMPLE_VM_STAT_TEXT = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                              105036.
Pages active:                           1191718.
Pages inactive:                         1180377.
Pages speculative:                        10655.
Pages throttled:                              0.
Pages wired down:                        227360.
Pages purgeable:                          30601.
`;

test('macOS available memory sums free+inactive+speculative pages from vm_stat, excluding purgeable (BRAIN-252)', () => {
  const snapshot = detectResourceCapacity({
    platform: 'darwin',
    parallelism: 8,
    totalMemory: 48 * 1024 ** 3,
    // Deliberately different from the vm_stat-derived figure, so the
    // assertion below cannot pass by the fixture accidentally agreeing
    // with itself.
    freeMemory: 1.8 * 1024 ** 3,
    vmStatText: SAMPLE_VM_STAT_TEXT,
  });
  assert.equal(snapshot.availableMemoryBytes, 16384 * (105036 + 1180377 + 10655));
  assert.ok(snapshot.source.split('+').includes('vm_stat'));
});

test('macOS falls back to os.freemem when vm_stat output is malformed', () => {
  const snapshot = detectResourceCapacity({
    platform: 'darwin',
    parallelism: 8,
    totalMemory: 48 * 1024 ** 3,
    freeMemory: 1.8 * 1024 ** 3,
    vmStatText: 'not vm_stat output at all',
  });
  assert.equal(snapshot.availableMemoryBytes, 1.8 * 1024 ** 3);
  assert.equal(snapshot.source, 'os');
});

test('macOS falls back to os.freemem when the vm_stat exec throws', () => {
  const snapshot = detectResourceCapacity({
    platform: 'darwin',
    parallelism: 8,
    totalMemory: 48 * 1024 ** 3,
    freeMemory: 1.8 * 1024 ** 3,
    exec: () => {
      throw new Error('vm_stat not found');
    },
  });
  assert.equal(snapshot.availableMemoryBytes, 1.8 * 1024 ** 3);
  assert.equal(snapshot.source, 'os');
});

test('macOS clamps an implausibly large vm_stat result to total memory', () => {
  const snapshot = detectResourceCapacity({
    platform: 'darwin',
    parallelism: 8,
    // Small enough that the sample's ~21.2GB parsed figure exceeds it.
    totalMemory: 4 * 1024 ** 3,
    freeMemory: 1 * 1024 ** 3,
    vmStatText: SAMPLE_VM_STAT_TEXT,
  });
  assert.equal(snapshot.availableMemoryBytes, 4 * 1024 ** 3);
  assert.ok(snapshot.source.split('+').includes('vm_stat'));
});

test('macOS execs vm_stat with a hard 2s bound (timeout + killSignal)', () => {
  let capturedArgs = null;
  detectResourceCapacity({
    platform: 'darwin',
    parallelism: 8,
    totalMemory: 48 * 1024 ** 3,
    freeMemory: 1.8 * 1024 ** 3,
    exec: (cmd, args, options) => {
      capturedArgs = { cmd, args, options };
      return SAMPLE_VM_STAT_TEXT;
    },
  });
  assert.equal(capturedArgs.cmd, 'vm_stat');
  assert.equal(capturedArgs.options.timeout, 2000);
  assert.equal(capturedArgs.options.killSignal, 'SIGKILL');
});

test('Linux ignores vmStatText entirely, even when it is complete and would parse to a different value', () => {
  const snapshot = detectResourceCapacity({
    platform: 'linux',
    parallelism: 8,
    totalMemory: 32 * 1024 ** 3,
    freeMemory: 20 * 1024 ** 3,
    // Complete, valid vm_stat text that would parse to 16384*(105036+1180377+10655)
    // = ~21.23GB, distinct from the 20GiB freeMemory below — proves the
    // platform gate actually excludes this path on Linux rather than the
    // assertion coincidentally matching either value.
    vmStatText: SAMPLE_VM_STAT_TEXT,
    readFile: () => {
      throw new Error('missing');
    },
  });
  assert.equal(snapshot.availableMemoryBytes, 20 * 1024 ** 3);
  assert.equal(snapshot.source, 'os');
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

test('BRAIN-385: cgroup inactive_file page cache counts as available, not used', () => {
  const GiB = 1024 ** 3;
  const files = new Map([
    ['/sys/fs/cgroup/memory.max', String(10 * GiB)],
    ['/sys/fs/cgroup/memory.current', String(Math.round(2.7 * GiB))],
    ['/sys/fs/cgroup/memory.stat', `anon ${Math.round(0.134 * GiB)}\nfile ${Math.round(2.5 * GiB)}\ninactive_file ${Math.round(1.6 * GiB)}\nactive_file ${Math.round(0.86 * GiB)}\n`],
  ]);
  const snapshot = detectResourceCapacity({
    platform: 'linux', parallelism: 4, totalMemory: 32 * GiB, freeMemory: 30 * GiB,
    readFile: (file) => { if (files.has(file)) return files.get(file); throw new Error('ENOENT'); },
  });
  assert.equal(Math.round((snapshot.availableMemoryBytes / GiB) * 10) / 10, 8.9); // 10 - (2.7 - 1.6)
});

test('LANE_BROKER_TEST_HOST_MEMORY replaces total and available memory from os, vm_stat and cgroups, and leaves cores alone', () => {
  const GiB = 1024 ** 3;
  const hostMemory = parseTestHostMemory(`${64 * GiB},${48 * GiB}`);
  const cgroup = (file) => ({ '/sys/fs/cgroup/memory.max': String(2 * GiB), '/sys/fs/cgroup/memory.current': String(GiB) })[file] ?? (() => { throw new Error('missing'); })();
  const linux = detectResourceCapacity({ platform: 'linux', parallelism: 5, totalMemory: 8 * GiB, freeMemory: GiB, readFile: cgroup, testHostMemory: hostMemory });
  assert.deepEqual([linux.cpuCores, linux.memoryBytes, linux.availableMemoryBytes, linux.source], [5, 64 * GiB, 48 * GiB, 'test-memory']);
  const darwin = detectResourceCapacity({ platform: 'darwin', parallelism: 5, totalMemory: 8 * GiB, freeMemory: GiB, vmStatText: 'Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 10.\n', testHostMemory: hostMemory });
  assert.deepEqual([darwin.memoryBytes, darwin.availableMemoryBytes, darwin.source], [64 * GiB, 48 * GiB, 'test-memory']);
});

test('a malformed LANE_BROKER_TEST_HOST_MEMORY is no override', () => {
  for (const raw of ['', '1', '1,', ',1', '0,5', '-1,5', '1.5,2', '1,2,3', 'a,b', ' 1,2', undefined]) {
    assert.equal(parseTestHostMemory(raw), null, JSON.stringify(raw));
  }
  assert.deepEqual(parseTestHostMemory('100,40'), { totalBytes: 100, availableBytes: 40 });
  const real = detectResourceCapacity({ platform: 'linux', totalMemory: 1000, freeMemory: 400, readFile: () => { throw new Error('missing'); }, testHostMemory: parseTestHostMemory('junk') });
  assert.deepEqual([real.memoryBytes, real.availableMemoryBytes, real.source], [1000, 400, 'os']);
});

test('the env hook reaches readMemoryInfo, the default memoryReader of tryStart', () => {
  process.env.LANE_BROKER_TEST_HOST_MEMORY = '1000,250';
  try {
    const info = readMemoryInfo(() => '1');
    // A runner inside a CPU-limited cgroup also reports cgroup-cpu/cpuset; only the memory sources are pinned.
    const memorySources = info.source.split('+').filter((s) => s.includes('memory'));
    assert.deepEqual([info.totalBytes, info.availableBytes, memorySources], [1000, 250, ['test-memory']]);
  } finally {
    delete process.env.LANE_BROKER_TEST_HOST_MEMORY;
  }
});
