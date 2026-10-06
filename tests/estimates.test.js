import test from 'node:test';
import assert from 'node:assert/strict';
import {
  INITIAL_SIGMA, Z90, emptyEstimator, observeCompleted, observeCensored, p50Of, p90Of, sigmaOf, recordObservation, estimateFor, classDefault,
  bucketOf, BUCKETS, seedFactor, factorFor, observeFactor, rebaseAnchor, rescaleEstimator, runningResidual, addRemaining, remainingMinutes, ANCHOR_STALE_MS,
} from '../src/estimates.js';

const near = (a, b, eps = 1e-6) => assert.ok(Math.abs(a - b) < eps, `${a} !~ ${b}`);
const fold = (xs, f = observeCompleted) => xs.reduce(f, emptyEstimator());

test('1. estimator: first observation seeds mu with the stated initial sigma', () => {
  const s = observeCompleted(emptyEstimator(), 100);
  near(p50Of(s), 100);
  near(sigmaOf(s), INITIAL_SIGMA);
  near(p90Of(s), 100 * Math.exp(Z90 * INITIAL_SIGMA));
  assert.equal(s.n, 1);
});

test('1. estimator: EWMA alpha 0.1 moves mu a tenth of the way in ln space', () => {
  const s = fold([100, 200]);
  near(s.mu, Math.log(100) + 0.1 * Math.log(2));
});

test('1. estimator: update ratio is winsorised to [0.2, 5]', () => {
  const hi = fold([100, 1e9]);
  near(hi.mu, Math.log(100) + 0.1 * Math.log(5));
  const lo = fold([100, 1e-6]);
  near(lo.mu, Math.log(100) + 0.1 * Math.log(0.2));
});

test('1. estimator: variance shrinks on repeated identical runs and grows on a spread', () => {
  assert.ok(fold([100, 100, 100, 100]).variance < INITIAL_SIGMA ** 2);
  assert.ok(fold([100, 400, 100, 400, 100]).variance > fold([100, 100, 100, 100, 100]).variance);
});

test('2. censored: repeated timeouts raise p50 and never lower it', () => {
  let s = fold([100, 100, 100]);
  let prev = p50Of(s);
  for (const elapsed of [150, 400, 90, 400, 50, 1000]) {
    s = observeCensored(s, elapsed);
    assert.ok(p50Of(s) >= prev, `p50 fell from ${prev} to ${p50Of(s)} on a ${elapsed}s censored run`);
    prev = p50Of(s);
  }
  near(p50Of(s), 1000);
  assert.equal(s.censored, 6);
});

test('2. censored: a shorter censored run than the estimate leaves it untouched', () => {
  const s = fold([300]);
  near(p50Of(observeCensored(s, 10)), 300);
});

test('2. censored: a first censored observation seeds a lower bound', () => {
  near(p50Of(observeCensored(emptyEstimator(), 500)), 500);
});

test('2. cancelled and never-started are ignored: non-positive durations change nothing', () => {
  const s = fold([100]);
  assert.deepEqual(observeCensored(s, 0), s);
  assert.deepEqual(observeCompleted(s, 0), s);
});

test('3. units: ref_s = wall_s / factor, wall = ref_s x factor applied once', () => {
  const refS = 240 / 2;
  const s = observeCompleted(emptyEstimator(), refS);
  near(p50Of(s), 120);
  near(p50Of(s) * 2, 240);
});

const JOB = { template: 'rouge:sim', class_id: 'sim', throughAct: 3, armSetHash: 'a1', codeFamily: 'v7' };

test('4. cold start: exact key', () => {
  const store = recordObservation(new Map(), JOB, { refS: 100 });
  const e = estimateFor(store, JOB);
  assert.equal(e.est_source, 'exact');
  near(e.est_p50_s, 100);
});

test('4. cold start: each step down the chain, widening sigma x1.5 compounding', () => {
  const store = recordObservation(new Map(), JOB, { refS: 100 });
  const sigma = INITIAL_SIGMA;
  const steps = [
    ['no-code-family', { ...JOB, codeFamily: 'v8' }, 1],
    ['no-arm-set', { ...JOB, codeFamily: 'v8', armSetHash: 'zz' }, 2],
    ['no-through-act', { ...JOB, codeFamily: 'v8', armSetHash: 'zz', throughAct: 9 }, 3],
    ['runner-wide', { ...JOB, codeFamily: 'v8', armSetHash: 'zz', throughAct: 9, class_id: 'other' }, 4],
  ];
  for (const [source, job, level] of steps) {
    const e = estimateFor(store, job);
    assert.equal(e.est_source, source);
    near(e.est_p50_s, 100);
    near(e.est_p90_s, 100 * Math.exp(Z90 * sigma * 1.5 ** level));
  }
});

test('4. cold start: class default is p50 = class p75, p90 = 2 x p50, with 300/900 placeholders', () => {
  const none = new Map();
  assert.deepEqual(estimateFor(none, JOB, { workClass: 'test' }), { est_p50_s: 300, est_p90_s: 600, est_source: 'class-default' });
  assert.deepEqual(estimateFor(none, JOB, { workClass: 'sim' }), { est_p50_s: 900, est_p90_s: 1800, est_source: 'class-default' });
  assert.deepEqual(estimateFor(none, JOB, { workClass: 'test', classP75: 80 }), { est_p50_s: 80, est_p90_s: 160, est_source: 'class-default' });
  assert.deepEqual(classDefault('sim'), { p50: 900, p90: 1800 });
});

test('4. cold start: a submitter hint is clamped to at least the class default', () => {
  const low = estimateFor(new Map(), JOB, { workClass: 'test', hint: { p50: 5, p90: 6 } });
  assert.equal(low.est_p50_s, 300);
  assert.equal(low.est_p90_s, 600);
  const high = estimateFor(new Map(), JOB, { workClass: 'test', hint: { p50: 1000, p90: 1500 } });
  assert.equal(high.est_p50_s, 1000);
  assert.equal(high.est_p90_s, 2000);
});

test('5. buckets by ref-s: <120, 120-600, >600', () => {
  assert.deepEqual([119.9, 120, 600, 600.1].map(bucketOf), ['short', 'medium', 'medium', 'long']);
});

test('5. factors: seeded from calibration, refined by EWMA of wall / est_p50 per host, class and bucket', () => {
  let f = seedFactor({}, 'skybox', 'test', 'short', 2);
  assert.equal(factorFor(f, 'skybox', 'test', 'short'), 2);
  f = observeFactor(f, 'skybox', 'test', 60, 180); // observed 3.0
  near(factorFor(f, 'skybox', 'test', 'short'), 2.1);
  assert.equal(factorFor(f, 'skybox', 'test', 'long'), 1);
  assert.equal(factorFor(f, 'skybox', 'sim', 'short'), 1);
});

test('5. factors: a censored observation into a MISSING cell cannot lower it below the default 1', () => {
  const f = observeFactor({}, 'skybox', 'test', 100, 20, { censored: true });
  assert.ok(factorFor(f, 'skybox', 'test', 'short') >= 1);
  assert.equal(factorFor(observeFactor({}, 'skybox', 'test', 100, 300, { censored: true }), 'skybox', 'test', 'short'), 3);
});

test('5. factors: censored runs only raise a factor', () => {
  let f = seedFactor({}, 'skybox', 'test', 'short', 2);
  f = observeFactor(f, 'skybox', 'test', 60, 70, { censored: true }); // 1.17 < 2
  assert.equal(factorFor(f, 'skybox', 'test', 'short'), 2);
  f = observeFactor(f, 'skybox', 'test', 60, 300, { censored: true }); // 5 > 2
  assert.equal(factorFor(f, 'skybox', 'test', 'short'), 5);
});

test('5. mac-grandy is pinned at 1.00: never seeded, refined, or read as anything else', () => {
  let f = seedFactor({}, 'mac-grandy', 'test', 'short', 3);
  f = observeFactor(f, 'mac-grandy', 'test', 60, 600);
  assert.equal(factorFor(f, 'mac-grandy', 'test', 'short'), 1);
  assert.equal(f['mac-grandy'], undefined);
  assert.equal(factorFor({ 'mac-grandy': { test: { short: { factor: 9, n: 1 } } } }, 'mac-grandy', 'test', 'short'), 1);
});

const cell = (factor) => ({ factor, n: 5 });
const NOW = 100 * 24 * 3600 * 1000;

test('5. anchor: a calibrated mac-grandy keeps the anchor and rescales nothing', () => {
  const factors = { skybox: { test: { short: cell(2) } } };
  const r = rebaseAnchor({ factors, calibratedAt: { 'mac-grandy': NOW - 1000, skybox: NOW }, now: NOW });
  assert.equal(r.moved, false);
  assert.equal(r.anchor, 'mac-grandy');
  assert.equal(r.factors, factors);
});

const OLD_ANCHOR_STALE = { 'mac-grandy': NOW - ANCHOR_STALE_MS - 1 };

test('5. anchor: 30 days without calibration moves it to the median host, rescaling per (class, bucket) cell', () => {
  const factors = { a: { test: { short: cell(1.5) } }, b: { test: { short: cell(2) } }, c: { test: { short: cell(4) } } };
  const calibratedAt = { ...OLD_ANCHOR_STALE, a: NOW, b: NOW, c: NOW };
  const r = rebaseAnchor({ factors, calibratedAt, now: NOW });
  assert.equal(r.moved, true);
  assert.equal(r.anchor, 'b');
  assert.deepEqual(r.refScales, { test: { short: 2 } });
  assert.deepEqual(r.unanchored, []);
  near(r.factors.a.test.short.factor, 0.75);
  near(r.factors.c.test.short.factor, 2);
  assert.equal(r.factors.b, undefined);
  near(p50Of(rescaleEstimator(observeCompleted(emptyEstimator(), 100), 'test', r.refScales)), 200);
  assert.equal(rebaseAnchor({ factors: r.factors, calibratedAt: { ...calibratedAt, b: NOW }, anchor: 'b', now: NOW }).moved, false);
});

test('5. anchor: the old anchor stays readable after the rebase (real per-cell value, not an unreadable aggregate)', () => {
  const factors = { b: { test: { short: cell(2), long: cell(4) } } };
  const r = rebaseAnchor({ factors, calibratedAt: { ...OLD_ANCHOR_STALE, b: NOW }, now: NOW });
  assert.equal(r.anchor, 'b');
  assert.equal(factorFor(r.factors, 'mac-grandy', 'test', 'short', { anchor: 'b' }), 0.5);
  assert.equal(factorFor(r.factors, 'mac-grandy', 'test', 'long', { anchor: 'b' }), 0.25);
  assert.equal(factorFor(r.factors, 'b', 'test', 'long', { anchor: 'b' }), 1);
});

test('5. anchor: new-anchor bucket differences are preserved, not collapsed to one scalar (short 1, long 4)', () => {
  const factors = { b: { test: { short: cell(1), long: cell(4) } } };
  const r = rebaseAnchor({ factors, calibratedAt: { ...OLD_ANCHOR_STALE, b: NOW }, now: NOW });
  // 100 ref-s short job on mac-grandy: 100 s before; after, its ref is 100 x 1 and the factor 1
  near(100 * r.refScales.test.short * factorFor(r.factors, 'mac-grandy', 'test', 'short', { anchor: 'b' }), 100);
  // 1000 ref-s long job on mac-grandy: 1000 s before; after, 4000 ref-s x 0.25
  near(1000 * r.refScales.test.long * factorFor(r.factors, 'mac-grandy', 'test', 'long', { anchor: 'b' }), 1000);
});

test('5. anchor: a cell the new anchor lacks is left unscaled and reported as unanchored', () => {
  const factors = { a: { test: { short: cell(2) }, sim: { long: cell(3) } }, z: { test: { short: cell(5) } } };
  const r = rebaseAnchor({ factors, calibratedAt: { ...OLD_ANCHOR_STALE, a: NOW, z: NOW }, now: NOW });
  assert.equal(r.anchor, 'a');
  assert.deepEqual(r.unanchored, []);
  const r2 = rebaseAnchor({ factors: { a: { test: { short: cell(2) } }, z: { test: { short: cell(5) }, sim: { long: cell(3) } } }, calibratedAt: { ...OLD_ANCHOR_STALE, a: NOW, z: NOW }, now: NOW });
  assert.equal(r2.anchor, 'a');
  assert.deepEqual(r2.unanchored, [['sim', 'long']]);
  assert.equal(r2.factors.z.sim.long.factor, 3);
  assert.equal(r2.refScales.sim, undefined);
});

test('5. anchor: UNIT PRESERVATION over random factor tables: predicted wall seconds unchanged on every host', () => {
  let seed = 12345;
  const rnd = () => ((seed = (seed * 1664525 + 1013904223) % 4294967296) / 4294967296);
  const hosts = ['a', 'b', 'c', 'd'];
  const classes = ['test', 'sim'];
  for (let trial = 0; trial < 200; trial += 1) {
    const factors = {};
    for (const h of hosts) for (const c of classes) for (const b of BUCKETS) if (rnd() < 0.8) factors[h] = { ...factors[h], [c]: { ...factors[h]?.[c], [b]: cell(0.3 + rnd() * 5) } };
    const calibratedAt = { ...OLD_ANCHOR_STALE, ...Object.fromEntries(hosts.map((h) => [h, NOW])) };
    const r = rebaseAnchor({ factors, calibratedAt, now: NOW });
    if (!r.moved) continue;
    const own = factors[r.anchor];
    for (const h of ['mac-grandy', ...hosts]) {
      for (const [c, b] of Object.entries(own).flatMap(([cc, bb]) => Object.keys(bb).map((x) => [cc, x]))) {
        const refOld = 10 + rnd() * 1000;
        const wallOld = refOld * factorFor(factors, h, c, b);
        const wallNew = refOld * r.refScales[c][b] * factorFor(r.factors, h, c, b, { anchor: r.anchor });
        near(wallNew, wallOld, 1e-9 * Math.max(1, wallOld));
      }
    }
  }
});

test('5. anchor: a host stale for more than 30 days is not a candidate; with none left nothing moves', () => {
  const factors = { a: { test: { short: cell(2) } } };
  const r = rebaseAnchor({ factors, calibratedAt: { a: NOW - ANCHOR_STALE_MS - 5 }, now: NOW });
  assert.equal(r.moved, false);
});

test('6. overrun: the spec worked example (factor 2, 120 s elapsed)', () => {
  const r = runningResidual({ elapsedWallS: 120, factor: 2, p50: 20, p90: 40, maxMs: 3_600_000 });
  assert.equal(r.overrun, true);
  assert.equal(r.elapsedRef, 60);
  assert.equal(r.residualRef, 30);
  assert.equal(r.residualWallS, 60);
});

test('6. overrun: capped at (max_ms / 1000 - elapsed_wall) / factor, max_ms in milliseconds', () => {
  const r = runningResidual({ elapsedWallS: 120, factor: 2, p50: 20, p90: 40, maxMs: 140_000 });
  assert.equal(r.residualRef, 10); // (140 - 120) / 2, not 0.5 x 60
  assert.equal(r.residualWallS, 20);
  assert.equal(runningResidual({ elapsedWallS: 120, factor: 2, p50: 20, p90: 40, maxMs: 100_000 }).residualRef, 0);
});

test('6. not overrun within p90: remaining p50, not flagged', () => {
  const r = runningResidual({ elapsedWallS: 20, factor: 2, p50: 30, p90: 60, maxMs: 1_000_000 });
  assert.equal(r.overrun, false);
  assert.equal(r.residualRef, 20);
});

test('7. remaining work: bucketed by the job p50, queued p50s plus running residuals', () => {
  let rem = {};
  rem = addRemaining(rem, 'test', 30, 30);
  rem = addRemaining(rem, 'test', 30, 10); // a running residual in the same bucket
  rem = addRemaining(rem, 'test', 900, 900);
  assert.deepEqual(rem, { test: { short: 40, long: 900 } });
});

test('7. remaining minutes: each bucket converted with its OWN factor, never one factor for the group', () => {
  const rem = { test: { short: 60, long: 600 } };
  const factors = { skybox: { test: { short: cell(1), long: cell(3) } } };
  const minutes = remainingMinutes(rem, (wc, b) => factorFor(factors, 'skybox', wc, b));
  near(minutes, (60 * 1 + 600 * 3) / 60);
  const oneFactor = ((60 + 600) * 1) / 60;
  assert.notEqual(minutes, oneFactor);
});
