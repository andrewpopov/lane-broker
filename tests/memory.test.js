import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {
  parseSwapUsage,
  parseVmStat,
  classifyMemorySample,
  readMemorySample,
  SWAP_TIGHT_PCT,
  SWAP_EXHAUSTED_PCT,
} from '../src/memory.js';

test('parseSwapUsage reads total/used out of real sysctl-shaped text', () => {
  const parsed = parseSwapUsage('total = 2048.00M  used = 512.00M  free = 1536.00M  (encrypted)');
  assert.equal(parsed.swapTotalBytes, 2048 * 1024 * 1024);
  assert.equal(parsed.swapUsedBytes, 512 * 1024 * 1024);
});

test('parseSwapUsage returns null on unrecognized text rather than fabricating a number', () => {
  assert.equal(parseSwapUsage('not sysctl output'), null);
  assert.equal(parseSwapUsage(''), null);
  assert.equal(parseSwapUsage(undefined), null);
});

test('parseVmStat reads page size and compressor pages out of real vm_stat-shaped text', () => {
  const text = [
    'Mach Virtual Memory Statistics: (page size of 16384 bytes)',
    'Pages free:                              123456.',
    'Pages occupied by compressor:            4096.',
    '',
  ].join('\n');
  const parsed = parseVmStat(text);
  assert.equal(parsed.pageSize, 16384);
  assert.equal(parsed.compressorPages, 4096);
});

test('parseVmStat returns both fields null when either is missing/malformed', () => {
  assert.deepEqual(parseVmStat('garbage'), { pageSize: null, compressorPages: null });
  assert.deepEqual(parseVmStat('Mach Virtual Memory Statistics: (page size of 16384 bytes)'), {
    pageSize: null,
    compressorPages: null,
  });
});

test('classifyMemorySample: healthy well below the tight threshold', () => {
  const sample = { swapUsedBytes: 100 * 1024 * 1024, swapTotalBytes: 2048 * 1024 * 1024, compressorBytes: 50 * 1024 * 1024, sampledAt: 1 };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'healthy');
  assert.ok(c.swapUsedPct < SWAP_TIGHT_PCT);
});

test('classifyMemorySample: tight between the two thresholds', () => {
  const sample = { swapUsedBytes: 1200 * 1024 * 1024, swapTotalBytes: 2048 * 1024 * 1024, compressorBytes: null, sampledAt: 1 };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'tight');
  assert.ok(c.swapUsedPct >= SWAP_TIGHT_PCT && c.swapUsedPct < SWAP_EXHAUSTED_PCT);
});

test('classifyMemorySample: exhausted at/above the exhausted threshold (the 2026-09-09 incident shape)', () => {
  // 42GB swap used out of a 44GB total -- the actual incident figures.
  const sample = { swapUsedBytes: 42 * 1024 * 1024 * 1024, swapTotalBytes: 44 * 1024 * 1024 * 1024, compressorBytes: 8 * 1024 * 1024 * 1024, sampledAt: 1 };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'exhausted');
  assert.ok(c.swapUsedPct >= SWAP_EXHAUSTED_PCT);
});

test('classifyMemorySample: null sample yields null classification, never a fabricated 0%', () => {
  assert.equal(classifyMemorySample(null), null);
  assert.equal(classifyMemorySample(undefined), null);
  assert.equal(classifyMemorySample({ swapUsedBytes: NaN, swapTotalBytes: NaN }), null);
});

test('readMemorySample honors LANE_BROKER_MEMORY_FILE, with values a real sampler would not plausibly produce', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-broker-memtest-'));
  const file = path.join(dir, 'memory');
  // A distinctive, implausible fixture -- these exact byte counts could not
  // come from a real sysctl/vm_stat call, so a sampler that silently ignores
  // the override and falls through to the real host reading cannot pass this
  // by coincidence.
  const FIXTURE_USED = 123456789;
  const FIXTURE_TOTAL = 987654321;
  const FIXTURE_COMPRESSOR = 13579111;
  fs.writeFileSync(file, `${FIXTURE_USED},${FIXTURE_TOTAL},${FIXTURE_COMPRESSOR}`);

  const prev = process.env.LANE_BROKER_MEMORY_FILE;
  process.env.LANE_BROKER_MEMORY_FILE = file;
  try {
    const sample = readMemorySample();
    assert.equal(sample.swapUsedBytes, FIXTURE_USED);
    assert.equal(sample.swapTotalBytes, FIXTURE_TOTAL);
    assert.equal(sample.compressorBytes, FIXTURE_COMPRESSOR);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_MEMORY_FILE;
    else process.env.LANE_BROKER_MEMORY_FILE = prev;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('readMemorySample falls through to the real reading when the override file is missing/malformed', () => {
  const prev = process.env.LANE_BROKER_MEMORY_FILE;
  process.env.LANE_BROKER_MEMORY_FILE = path.join(os.tmpdir(), 'lane-broker-memtest-does-not-exist');
  try {
    // Just confirm it doesn't throw and doesn't return the (nonexistent) override.
    const sample = readMemorySample();
    assert.ok(sample === null || typeof sample.swapUsedBytes === 'number');
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_MEMORY_FILE;
    else process.env.LANE_BROKER_MEMORY_FILE = prev;
  }
});

test('readMemorySample never throws on a real host (macOS: a real sample; elsewhere: null)', () => {
  const prev = process.env.LANE_BROKER_MEMORY_FILE;
  delete process.env.LANE_BROKER_MEMORY_FILE;
  try {
    const sample = readMemorySample();
    if (process.platform === 'darwin') {
      assert.ok(sample === null || typeof sample.swapUsedBytes === 'number');
    } else {
      assert.equal(sample, null);
    }
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_MEMORY_FILE;
    else process.env.LANE_BROKER_MEMORY_FILE = prev;
  }
});

test('readMemorySample: a throwing exec degrades to null, never a crash', { skip: process.platform !== 'darwin' }, () => {
  const prev = process.env.LANE_BROKER_MEMORY_FILE;
  delete process.env.LANE_BROKER_MEMORY_FILE;
  try {
    const throwingExec = () => {
      throw new Error('spawnSync sysctl ETIMEDOUT');
    };
    assert.equal(readMemorySample(throwingExec), null);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_MEMORY_FILE;
    else process.env.LANE_BROKER_MEMORY_FILE = prev;
  }
});

// Codex pre-merge review (BRAIN-211): Number('') is 0, not NaN, so a
// field-count-correct but EMPTY override was accepted as a real 0-of-0
// sample and rendered "HEALTHY 0.0%" — silently contradicting this
// function's documented fall-through-on-malformed contract. A fixture that
// fabricates a healthy reading is worse than one that fails outright: it
// would let a future test pass for entirely the wrong reason.
test('readMemorySample does not accept blank override fields as a zeroed sample', () => {
  const prev = process.env.LANE_BROKER_MEMORY_FILE;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-broker-memblank-'));
  try {
    for (const blank of [',,', ' , , ', '1,,3']) {
      const file = path.join(dir, 'memory');
      fs.writeFileSync(file, blank);
      process.env.LANE_BROKER_MEMORY_FILE = file;
      const sample = readMemorySample();
      // Either it fell through to the real host reading, or (non-darwin) it
      // returned null. What it must NEVER do is hand back the blanks as a
      // 0-of-0 sample that classifies as healthy.
      const fabricated =
        sample && sample.swapTotalBytes === 0 && sample.swapUsedBytes === 0 && sample.compressorBytes === 0;
      assert.equal(fabricated, false, `blank override ${JSON.stringify(blank)} was accepted as a zeroed sample`);
    }
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_MEMORY_FILE;
    else process.env.LANE_BROKER_MEMORY_FILE = prev;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// Codex also flagged the absence of a zero-total fixture. classifyMemorySample
// guards the division, but nothing pinned that it stays guarded.
test('classifyMemorySample: a zero swap total does not divide by zero or emit NaN', () => {
  const c = classifyMemorySample({ swapUsedBytes: 0, swapTotalBytes: 0, compressorBytes: 0, sampledAt: 1 });
  assert.ok(Number.isFinite(c.swapUsedPct), 'swapUsedPct must be finite, never NaN');
  assert.equal(c.swapUsedPct, 0);
  assert.equal(c.level, 'healthy');
});
