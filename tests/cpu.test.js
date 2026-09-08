import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { freshEnv } from './helpers.js';
import { sampleHostCpu, computeBusyCores, readMemoryInfo, parseGroupCpuOutput, observedGroupCpuCores } from '../src/cpu.js';

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

test('parseGroupCpuOutput: an all-blank/whitespace ps output is null, not a fabricated zero (BRAIN-207)', () => {
  // Number('') === 0, so the blank line must be filtered BEFORE the Number()
  // coercion, not after -- otherwise "nothing usable" reads as "a real
  // process at exactly 0% CPU" instead of null (no observation).
  assert.equal(parseGroupCpuOutput('\n'), null);
  assert.equal(parseGroupCpuOutput('   \n  \n'), null);
  assert.equal(parseGroupCpuOutput(''), null);
});

test('parseGroupCpuOutput sums usable rows into a cores-busy fraction, ignoring blank lines', () => {
  assert.equal(parseGroupCpuOutput(' 12.5\n 37.5\n\n'), 0.5);
});

test('parseGroupCpuOutput treats non-numeric garbage as no usable rows', () => {
  assert.equal(parseGroupCpuOutput('garbage'), null);
});

test('observedGroupCpuCores returns null for a falsy pgid without shelling out', () => {
  assert.equal(observedGroupCpuCores(null), null);
  assert.equal(observedGroupCpuCores(0), null);
});

test('readMemoryInfo never throws and reports a numeric availableBytes', () => {
  const info = readMemoryInfo();
  assert.equal(typeof info.availableBytes, 'number');
  assert.ok(info.availableBytes >= 0);
  // macPressure is best-effort: a string on macOS when the probe succeeds, null otherwise.
  assert.ok(info.macPressure === null || typeof info.macPressure === 'string');
});
