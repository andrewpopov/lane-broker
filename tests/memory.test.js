import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {
  parseSwapUsage,
  parseVmStat,
  parseVmStatAvailable,
  classifyMemorySample,
  readMemorySample,
  AVAILABLE_TIGHT_PCT,
  AVAILABLE_EXHAUSTED_PCT,
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

test('parseVmStatAvailable reads free+inactive+speculative out of real vm_stat-shaped text', () => {
  const text = [
    'Mach Virtual Memory Statistics: (page size of 16384 bytes)',
    'Pages free:                              105036.',
    'Pages inactive:                          1180377.',
    'Pages speculative:                       10655.',
    '',
  ].join('\n');
  assert.equal(parseVmStatAvailable(text), 16384 * (105036 + 1180377 + 10655));
});

const GiB = 1024 * 1024 * 1024;

// BRAIN-273: the whole point of this table is rows 1 and 3, where a
// swap-based classifier and an availability-based classifier disagree.
// Row 1 uses the REAL measured incident numbers from the ticket, not
// invented ones: swap 25494/26624MB (95.8%), available 17.06GiB of 48GiB.
test('classifyMemorySample: high swap + high availability is healthy (the BRAIN-273 incident shape, real numbers)', () => {
  const sample = {
    swapUsedBytes: 25494 * 1024 * 1024,
    swapTotalBytes: 26624 * 1024 * 1024,
    compressorBytes: 24833 * 1024 * 1024,
    availableBytes: 17.06 * GiB,
    totalMemoryBytes: 48 * GiB,
    sampledAt: 1,
  };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'healthy');
  assert.ok(c.swapUsedPct > 90, 'sanity: this fixture really is high-swap');
  assert.ok(c.availablePct > AVAILABLE_TIGHT_PCT);
});

test('classifyMemorySample: high swap + low availability is exhausted', () => {
  const sample = {
    swapUsedBytes: 25494 * 1024 * 1024,
    swapTotalBytes: 26624 * 1024 * 1024,
    compressorBytes: 24833 * 1024 * 1024,
    availableBytes: 0.5 * GiB,
    totalMemoryBytes: 48 * GiB,
    sampledAt: 1,
  };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'exhausted');
  assert.ok(c.availablePct <= AVAILABLE_EXHAUSTED_PCT);
});

test('classifyMemorySample: low swap + low availability is exhausted (availability drives the verdict, not swap)', () => {
  const sample = {
    swapUsedBytes: 0.05 * GiB,
    swapTotalBytes: 1 * GiB,
    compressorBytes: null,
    availableBytes: 0.5 * GiB,
    totalMemoryBytes: 48 * GiB,
    sampledAt: 1,
  };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'exhausted');
  assert.ok(c.swapUsedPct < AVAILABLE_EXHAUSTED_PCT, 'sanity: this fixture really is low-swap');
});

test('classifyMemorySample: low swap + high availability is healthy', () => {
  const sample = {
    swapUsedBytes: 0.1 * GiB,
    swapTotalBytes: 1 * GiB,
    compressorBytes: null,
    availableBytes: 17 * GiB,
    totalMemoryBytes: 48 * GiB,
    sampledAt: 1,
  };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'healthy');
});

test('classifyMemorySample: tight between the two availability thresholds', () => {
  const sample = {
    swapUsedBytes: 1 * GiB,
    swapTotalBytes: 2 * GiB,
    compressorBytes: null,
    availableBytes: 5 * GiB, // 5/48 = ~10.4%, between AVAILABLE_EXHAUSTED_PCT (8) and AVAILABLE_TIGHT_PCT (15)
    totalMemoryBytes: 48 * GiB,
    sampledAt: 1,
  };
  const c = classifyMemorySample(sample);
  assert.equal(c.level, 'tight');
});

test('classifyMemorySample: missing availability yields null (unavailable), never a swap-derived guess', () => {
  // Same swap numbers as the incident fixture above, but no availability
  // signal at all -- this must NOT fall back to classifying off swap.
  assert.equal(
    classifyMemorySample({
      swapUsedBytes: 25494 * 1024 * 1024,
      swapTotalBytes: 26624 * 1024 * 1024,
      compressorBytes: 24833 * 1024 * 1024,
      sampledAt: 1,
    }),
    null,
  );
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
      const fabricated = Boolean(
        sample && sample.swapTotalBytes === 0 && sample.swapUsedBytes === 0 && sample.compressorBytes === 0,
      );
      assert.equal(fabricated, false, `blank override ${JSON.stringify(blank)} was accepted as a zeroed sample`);
    }
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_MEMORY_FILE;
    else process.env.LANE_BROKER_MEMORY_FILE = prev;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// Codex also flagged the absence of a zero-total fixture. classifyMemorySample
// guards the division, but nothing pinned that it stays guarded. Swap is now
// display-context only, but a zero swap total must still render as 0%, never
// NaN, alongside a real availability-derived verdict.
test('classifyMemorySample: a zero swap total does not divide by zero or emit NaN', () => {
  const c = classifyMemorySample({
    swapUsedBytes: 0,
    swapTotalBytes: 0,
    compressorBytes: 0,
    availableBytes: 17 * GiB,
    totalMemoryBytes: 48 * GiB,
    sampledAt: 1,
  });
  assert.ok(Number.isFinite(c.swapUsedPct), 'swapUsedPct must be finite, never NaN');
  assert.equal(c.swapUsedPct, 0);
  assert.equal(c.level, 'healthy');
});

test('classifyMemorySample: a zero total memory yields null rather than dividing by zero', () => {
  assert.equal(
    classifyMemorySample({ swapUsedBytes: 0, swapTotalBytes: 0, compressorBytes: 0, availableBytes: 0, totalMemoryBytes: 0, sampledAt: 1 }),
    null,
  );
});
