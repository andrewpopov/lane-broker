import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import {
  sampleHostCpu,
  computeBusyCores,
  readMemoryInfo,
  parseProcessTable,
  selectLeaseTree,
  observeLeaseTree,
  observedGroupCpuCores,
  observedGroupMemoryBytes,
} from '../src/cpu.js';

/** Build os.cpus()-shaped fixtures from just {idle, total} per core. */
function fakeCpus(cores) {
  return cores.map(({ idle, total }) => ({
    model: 'test',
    speed: 0,
    times: { user: total - idle, nice: 0, sys: 0, idle, irq: 0 },
  }));
}

test('computeBusyCores reports stale with no prior snapshot', () => {
  const snapshot = { at: 1000, cpus: [{ idle: 100, total: 200 }] };
  const result = computeBusyCores(null, snapshot);
  assert.equal(result.stale, true);
  assert.equal(result.hostBusyCores, null);
});

test('computeBusyCores reports stale when the core count changes between snapshots', () => {
  const prev = { at: 0, cpus: [{ idle: 0, total: 100 }] };
  const snapshot = { at: 1000, cpus: [{ idle: 0, total: 200 }, { idle: 0, total: 200 }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, true);
});

test('computeBusyCores sums per-core busy fractions from the delta between two snapshots', () => {
  const prev = { at: 0, cpus: [{ idle: 1000, total: 2000 }, { idle: 1000, total: 2000 }] };
  // core0: totalDelta=1000, idleDelta=200 -> 80% busy; core1: totalDelta=1000, idleDelta=1000 -> 0% busy
  const snapshot = { at: 1000, cpus: [{ idle: 1200, total: 3000 }, { idle: 2000, total: 3000 }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, false);
  assert.ok(Math.abs(result.hostBusyCores - 0.8) < 1e-9, `expected ~0.8, got ${result.hostBusyCores}`);
});

test('computeBusyCores is stale when no core advanced (duplicate sample, e.g. two polls too close together)', () => {
  const snapshot = { at: 0, cpus: [{ idle: 500, total: 1000 }] };
  const result = computeBusyCores(snapshot, snapshot);
  assert.equal(result.stale, true);
  assert.equal(result.hostBusyCores, null);
});

test('computeBusyCores treats a zero-elapsed gap (two samples in the same millisecond) as a legitimate reading, not stale', () => {
  // Date.now() has ~1ms resolution, so two back-to-back real calls can
  // easily land in the same millisecond. Only a NEGATIVE gap (clock went
  // backwards) is invalid, not a zero one.
  const prev = { at: 1000, cpus: [{ idle: 1000, total: 2000 }] };
  const snapshot = { at: 1000, cpus: [{ idle: 1200, total: 3000 }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, false);
  assert.ok(Math.abs(result.hostBusyCores - 0.8) < 1e-9);
});

test('computeBusyCores treats a negative elapsed gap (clock went backwards) as stale', () => {
  const prev = { at: 1000, cpus: [{ idle: 1000, total: 2000 }] };
  const snapshot = { at: 999, cpus: [{ idle: 1200, total: 3000 }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, true);
});

test('sampleHostCpu: the first call ever (no persisted sidecar) is stale', () => {
  const { state } = freshEnv();
  const result = sampleHostCpu(state, fakeCpus([{ idle: 1000, total: 2000 }]));
  assert.equal(result.stale, true);
  assert.equal(result.hostBusyCores, null);
  assert.equal(result.cores, 1);
});

test('sampleHostCpu: a second, later call diffs against the persisted first snapshot', () => {
  const { state } = freshEnv();
  sampleHostCpu(state, fakeCpus([{ idle: 1000, total: 2000 }, { idle: 1000, total: 2000 }]));
  const result = sampleHostCpu(state, fakeCpus([{ idle: 1200, total: 3000 }, { idle: 2000, total: 3000 }]));
  assert.equal(result.stale, false);
  assert.ok(Math.abs(result.hostBusyCores - 0.8) < 1e-9);
});

test('sampleHostCpu: a stale (non-advancing) repeat sample never reports a fabricated busy figure', () => {
  const { state } = freshEnv();
  const cpus = fakeCpus([{ idle: 500, total: 1000 }]);
  sampleHostCpu(state, cpus);
  const result = sampleHostCpu(state, cpus); // identical -> no core advanced
  assert.equal(result.stale, true);
  assert.equal(result.hostBusyCores, null);
});

test('computeBusyCores treats a snapshot older than the max sample gap as stale, even with matching core count and advancing counters', () => {
  // A gap this large (well past MAX_SAMPLE_GAP_MS = 60_000) would otherwise
  // average over hours of history and could mask a live spike (Codex review
  // finding #3).
  const prev = { at: 0, cpus: [{ idle: 1000, total: 2000 }] };
  const snapshot = { at: 3 * 60 * 60 * 1000, cpus: [{ idle: 1200, total: 3000 }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, true);
  assert.equal(result.hostBusyCores, null);
});

test('computeBusyCores treats a snapshot with a non-numeric `at` as stale rather than computing a garbage gap', () => {
  const prev = { at: 'not-a-number', cpus: [{ idle: 1000, total: 2000 }] };
  const snapshot = { at: 1000, cpus: [{ idle: 1200, total: 3000 }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, true);
});

test('computeBusyCores skips a malformed per-core entry instead of propagating a NaN busy figure', () => {
  const prev = { at: 0, cpus: [{ idle: 1000, total: 2000 }, {}] }; // second core: missing idle/total
  const snapshot = { at: 1000, cpus: [{ idle: 1200, total: 3000 }, { idle: 'oops', total: 'oops' }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, false, 'the one well-formed core still yields a usable reading');
  assert.ok(Math.abs(result.hostBusyCores - 0.8) < 1e-9, `expected ~0.8 from the good core alone, got ${result.hostBusyCores}`);
  assert.ok(Number.isFinite(result.hostBusyCores), 'must never be NaN');
});

test('computeBusyCores is stale (not NaN) when every core entry is malformed', () => {
  const prev = { at: 0, cpus: [{}] };
  const snapshot = { at: 1000, cpus: [{ idle: 'oops', total: 'oops' }] };
  const result = computeBusyCores(prev, snapshot);
  assert.equal(result.stale, true);
  assert.equal(result.hostBusyCores, null);
});

test('sampleHostCpu never throws when the sidecar write fails (e.g. the state root does not exist / is unwritable)', () => {
  // Point at a path that cannot be created as a directory (its parent is a
  // file, not a directory), so atomicWriteJson's mkdir+write must fail.
  const { base } = freshEnv();
  const blocker = `${base}/blocker-file`;
  fs.writeFileSync(blocker, 'not a directory');
  const brokenRoot = `${blocker}/state`; // can't mkdir under a file
  assert.doesNotThrow(() => {
    const result = sampleHostCpu(brokenRoot, fakeCpus([{ idle: 100, total: 200 }]));
    assert.equal(result.stale, true);
  });
});

// ps -A -o pid=,ppid=,pgid=,pcpu=,rss= fixture. Lane pgid is 100; its npm child 101 spawns
// workers with `detached: true`, so 102/103 sit in their OWN groups (as in production) and 104 is
// a grandchild of 102. 200/201 are an unrelated tree; 300 is an orphan reparented to init.
const PS_FIXTURE = [
  '  100     1   100   0.0   1000',
  '  101   100   100  10.0   2000',
  '  102   101   746 150.0   4000',
  '  103   101 25854 100.0   8000',
  '  104   102   746  50.0   1000',
  '  200     1   200 400.0 999999',
  '  201   200   200 100.0 999999',
  '  300     1   300  90.0 999999',
  '',
].join('\n');

test('parseProcessTable skips blank and malformed rows', () => {
  const rows = parseProcessTable('  1 0 1 2.5 100\n\ngarbage\n1 2 3\n');
  assert.deepEqual(rows, [{ pid: 1, ppid: 0, pgid: 1, pcpu: 2.5, rss: 100 }]);
  assert.deepEqual(parseProcessTable(''), []);
});

test('selectLeaseTree includes detached descendants in other process groups and excludes unrelated trees (BRAIN-353)', () => {
  const tree = selectLeaseTree(parseProcessTable(PS_FIXTURE), 100);
  assert.equal(tree.cores, 3.1); // (0 + 10 + 150 + 100 + 50) / 100
  assert.equal(tree.memoryBytes, 16000 * 1024);
});

test('selectLeaseTree is null for an id with no process in the group, never a fabricated zero', () => {
  assert.equal(selectLeaseTree(parseProcessTable(PS_FIXTURE), 999), null);
  assert.equal(selectLeaseTree([], 100), null);
});

test('selectLeaseTree terminates on a ppid cycle and an orphan chain', () => {
  const rows = parseProcessTable('10 12 10 100 1\n11 10 11 100 1\n12 11 12 100 1\n13 9999 13 100 1\n');
  assert.equal(selectLeaseTree(rows, 10).cores, 3); // 10,11,12 form a cycle; orphan 13 stays out
});

test('selectLeaseTree excludes a nested lease (inner supervisor pid, inner child group) but keeps the outer tree', () => {
  const rows = parseProcessTable(
    [
      '100 1 100 10.0 100', // outer lane child
      '110 100 100 10.0 100', // inner `lane run` client, outer group
      '111 110 111 20.0 100', // inner supervisor (stopPid), own group
      '112 111 112 300.0 100', // inner child group (stopPgid)
      '113 112 113 100.0 100', // inner detached worker, descends from stopped pids
      '120 100 120 50.0 100', // outer's own detached worker
    ].join('\n'),
  );
  const stops = { stopPids: new Set([111]), stopPgids: new Set([112]) };
  assert.equal(selectLeaseTree(rows, 100, stops).cores, 0.7);
  assert.equal(selectLeaseTree(rows, 100).cores, 4.9);
});

test('selectLeaseTree ignores its own pgid in stopPgids', () => {
  const rows = parseProcessTable('100 1 100 10.0 100\n101 100 101 20.0 100');
  assert.equal(selectLeaseTree(rows, 100, { stopPgids: new Set([100]) }).cores, 0.3);
});

test('observeLeaseTree reads real descendants, including a detached grandchild in its own process group', async () => {
  const { spawn } = await import('node:child_process');
  const spin = 'const e=Date.now()+3000;while(Date.now()<e){}';
  const mid = `require('child_process').spawn(process.execPath,['-e',${JSON.stringify(spin)}],{detached:true,stdio:'ignore'}).unref();setTimeout(()=>{},3000)`;
  const child = spawn(process.execPath, ['-e', mid], { detached: true, stdio: 'ignore' });
  try {
    await new Promise((r) => setTimeout(r, 1500));
    const tree = observeLeaseTree(child.pid);
    assert.ok(tree, 'lease tree observed');
    assert.ok(tree.cores > 0.3, `expected the detached busy grandchild in the sum, got ${tree.cores}`);
  } finally {
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch {}
    // the detached grandchild is outside the killed group; it exits on its own after ~3s
  }
});

test('observedGroupMemoryBytes is bounded and fails safely', () => {
  let options;
  assert.equal(
    observedGroupMemoryBytes(123, (_cmd, _args, passedOptions) => {
      options = passedOptions;
      return '1 0 123 0.0 512\n';
    }),
    512 * 1024,
  );
  assert.equal(options.timeout, 2000);
  assert.equal(observedGroupMemoryBytes(123, () => { throw new Error('timeout'); }), null);
});

test('observedGroupCpuCores returns null for a falsy pgid without shelling out', () => {
  assert.equal(observedGroupCpuCores(null), null);
  assert.equal(observedGroupCpuCores(0), null);
});

// BRAIN-207 Codex pre-merge review: both probes must be bounded (timeout:
// 2000 passed to execFileSync) so a hung `ps`/`sysctl` can never hold the
// supervisor heartbeat (observedGroupCpuCores) or the global admission lock
// (readMemoryInfo, called from inside tryStart) open indefinitely. A timed-
// out exec throws (Node surfaces ETIMEDOUT the same way any other
// execFileSync failure throws), so injecting a throwing exec is exactly
// what a real timeout looks like from the caller's side.

test('observedGroupCpuCores passes a 2000ms timeout bound to exec, so a hung `ps` cannot stall the heartbeat forever', () => {
  let capturedOptions = null;
  const spyExec = (cmd, args, options) => {
    capturedOptions = options;
    return '1234 1 1234 10.0 100\n';
  };
  observedGroupCpuCores(1234, spyExec);
  assert.equal(capturedOptions.timeout, 2000);
});

test('readMemoryInfo passes a 2000ms timeout bound to exec, so a hung `sysctl` cannot stall the admission lock forever', { skip: process.platform !== 'darwin' }, () => {
  let capturedOptions = null;
  const spyExec = (cmd, args, options) => {
    capturedOptions = options;
    return '1';
  };
  readMemoryInfo(spyExec);
  assert.equal(capturedOptions.timeout, 2000);
});

test('observedGroupCpuCores: an exec that throws (simulating a ps timeout) yields null, not a crash', () => {
  const timingOutExec = () => {
    const err = new Error('spawnSync ps ETIMEDOUT');
    err.code = 'ETIMEDOUT';
    throw err;
  };
  assert.equal(observedGroupCpuCores(1234, timingOutExec), null);
});

test('readMemoryInfo: an exec that throws (simulating a sysctl timeout) yields a non-denying reading, not a crash', () => {
  const timingOutExec = () => {
    const err = new Error('spawnSync sysctl ETIMEDOUT');
    err.code = 'ETIMEDOUT';
    throw err;
  };
  const info = readMemoryInfo(timingOutExec);
  assert.equal(info.macPressure, null, 'a timed-out probe degrades to null, same as any other sysctl failure');
  assert.notEqual(info.macPressure, 'critical');
});

test('readMemoryInfo never throws and reports a numeric availableBytes', () => {
  const info = readMemoryInfo();
  assert.equal(typeof info.availableBytes, 'number');
  assert.ok(info.availableBytes >= 0);
  // macPressure is best-effort: a string on macOS when the probe succeeds, null otherwise.
  assert.ok(info.macPressure === null || typeof info.macPressure === 'string');
});
