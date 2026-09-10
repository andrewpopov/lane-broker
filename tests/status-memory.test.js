import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv, writeGlobalConfig, writeMemoryFile, laneRun } from './helpers.js';
import { renderStatusText } from '../src/status.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

// BRAIN-211: a memory/swap headroom reading in `lane status`. Detection, not
// admission -- see memory.js and scheduler.js's own (separate, pre-existing)
// macPressure brake for why this must never become a second one.

test('lane status renders the memory line with an injected exhausted reading', async () => {
  const { home, state, base } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const memoryFile = writeMemoryFile(base, 42 * 1024 * 1024 * 1024, 44 * 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024);

  const result = await laneRun(['status'], {
    env: { ...process.env, LANE_BROKER_HOME: home, LANE_BROKER_STATE: state, LANE_BROKER_MEMORY_FILE: memoryFile },
  });

  assert.equal(result.code, 0, `expected exit 0; got ${result.code}, stderr: ${result.stderr}`);
  assert.match(result.stdout, /memory: EXHAUSTED/);
  assert.match(result.stdout, /\[informational\]/);
  assert.doesNotMatch(result.stdout, /undefined/);
  assert.doesNotMatch(result.stdout, /NaN/);
});

test('lane status renders the memory line with an injected healthy reading', async () => {
  const { home, state, base } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const memoryFile = writeMemoryFile(base, 100 * 1024 * 1024, 2048 * 1024 * 1024, 50 * 1024 * 1024);

  const result = await laneRun(['status'], {
    env: { ...process.env, LANE_BROKER_HOME: home, LANE_BROKER_STATE: state, LANE_BROKER_MEMORY_FILE: memoryFile },
  });

  assert.equal(result.code, 0, `expected exit 0; got ${result.code}, stderr: ${result.stderr}`);
  assert.match(result.stdout, /memory: HEALTHY/);
});

test('renderStatusText degrades gracefully when the memory sample is unavailable (null), never crashing or printing undefined/NaN', () => {
  const status = {
    capacity: 4,
    used: 0,
    paused: null,
    configWarning: null,
    loadGate: { closed: false, lastLoad: null, sampleAgeMs: null, consecutiveUnder: 0, loadClose: 15, loadOpen: 11, loadOpenSamples: 3 },
    memory: null,
    running: [],
    queued: [],
  };
  const text = renderStatusText(status);
  assert.match(text, /memory: unavailable/);
  assert.match(text, /\[informational\]/);
  assert.doesNotMatch(text, /undefined/);
  assert.doesNotMatch(text, /NaN/);
});

test('renderStatusText degrades gracefully when status.memory is missing entirely (not just null)', () => {
  const status = {
    capacity: 4,
    used: 0,
    paused: null,
    configWarning: null,
    loadGate: { closed: false, lastLoad: null, sampleAgeMs: null, consecutiveUnder: 0, loadClose: 15, loadOpen: 11, loadOpenSamples: 3 },
    running: [],
    queued: [],
  };
  const text = renderStatusText(status);
  assert.match(text, /memory: unavailable/);
  assert.doesNotMatch(text, /undefined/);
  assert.doesNotMatch(text, /NaN/);
});

// The important negative control: an exhausted memory reading, injected
// exactly the way `lane status` would sample it, must never affect a real
// admission decision. This is the test that proves the reading is a display,
// not a gate -- see the BRAIN-211 canary note in the implementer's report
// for how this was confirmed to actually fail when wired into tryStart.
test('negative control: an exhausted memory reading (swap ~100% used) never denies admission', async () => {
  const { state, base } = freshEnv();
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 };
  const memoryFile = writeMemoryFile(base, 44 * 1024 * 1024 * 1024, 44 * 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
  const prev = process.env.LANE_BROKER_MEMORY_FILE;
  process.env.LANE_BROKER_MEMORY_FILE = memoryFile;

  const ticket = {
    id: 'candidate-1',
    key: 'repo/candidate',
    weight: 1,
    conflicts: [],
    supervisorPid: process.pid,
    supervisorStart: null,
    cwd: '/tmp',
    cmd: ['true'],
    logPath: '/tmp/log',
    resultPath: '/tmp/result',
  };

  try {
    await enqueue(state, ticket);
    // loadSampler -> 0 (open gate), cpuSampler default, memoryReader default
    // (the pre-existing macPressure brake) -> null on this host in the
    // common case, which never denies either; the point of this test is
    // LANE_BROKER_MEMORY_FILE specifically, which nothing in tryStart reads.
    const result = await tryStart(state, ticket, cfg, () => 0);
    assert.equal(result.started, true, 'an exhausted BRAIN-211 swap reading must never deny a start -- it is informational only');
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_MEMORY_FILE;
    else process.env.LANE_BROKER_MEMORY_FILE = prev;
  }
});
