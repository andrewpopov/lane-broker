/**
 * BRAIN-408 slice A (spec 5.7): the PURE estimates engine. No I/O and no clock reads (callers pass `now`).
 *
 * Every duration here is in REFERENCE seconds ("ref-s", the anchor host's slot = factor 1.00):
 * `ref_s = wall_s / factor`, and wall time is `ref_s * factor`, applied once at the last step.
 */

export const ALPHA = 0.1;
export const RATIO_MIN = 0.2;
export const RATIO_MAX = 5;
/** z-score of the 90th percentile of a normal. */
export const Z90 = 1.28;
/**
 * Sigma (in ln-ref-s) given to a key's first observation: 0.6 makes the seeded p90 e^(1.28 * 0.6) = 2.16x the p50,
 * wide enough that one lucky run does not look certain, narrow enough that it still orders work.
 */
export const INITIAL_SIGMA = 0.6;
/** Each cold-start fallback step widens sigma by this factor, compounding per step. */
export const COLD_START_SIGMA_WIDEN = 1.5;
export const CLASS_DEFAULT_PLACEHOLDER_S = { test: 300, sim: 900 };
/** Bucket upper bounds in ref-s: <120, 120-600, >600. */
export const BUCKETS = ['short', 'medium', 'long'];
export const ANCHOR_HOST = 'mac-grandy';
export const ANCHOR_STALE_MS = 30 * 24 * 3600 * 1000;

const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
const isNum = (v) => typeof v === 'number' && Number.isFinite(v);

/* ---------- estimator state ---------- */

/** `{ n, mu, variance, censored }`; mu = ln(ref-s). Plain data so a later slice can persist it as is. */
export function emptyEstimator() {
  return { n: 0, mu: 0, variance: INITIAL_SIGMA ** 2, censored: 0 };
}

export const sigmaOf = (state) => Math.sqrt(state.variance);
export const p50Of = (state) => Math.exp(state.mu);
export const p90Of = (state, sigma = sigmaOf(state)) => Math.exp(state.mu + Z90 * sigma);

/** A completed observation: ratio to the current p50 winsorised to [0.2, 5], then EWMA of mu and its variance. */
export function observeCompleted(state, refS) {
  if (!(refS > 0)) return state;
  if (state.n === 0) return { n: 1, mu: Math.log(refS), variance: INITIAL_SIGMA ** 2, censored: 0 };
  const ratio = clamp(refS / p50Of(state), RATIO_MIN, RATIO_MAX);
  const delta = Math.log(ratio);
  return {
    n: state.n + 1,
    mu: state.mu + ALPHA * delta,
    variance: (1 - ALPHA) * (state.variance + ALPHA * delta * delta),
    censored: state.censored,
  };
}

/**
 * A censored observation (max_ms timeout, wedged kill, interruption after running) is a LOWER BOUND:
 * the estimate becomes max(current, elapsed). It can raise an estimate, never lower it.
 */
export function observeCensored(state, elapsedRefS) {
  if (!(elapsedRefS > 0)) return state;
  if (state.n === 0) return { n: 1, mu: Math.log(elapsedRefS), variance: INITIAL_SIGMA ** 2, censored: 1 };
  return { ...state, n: state.n + 1, mu: Math.max(state.mu, Math.log(elapsedRefS)), censored: state.censored + 1 };
}

/**
 * Re-express a work class's estimator after the anchor moved: multiply its ref-s by the rebase's `refScales[class][bucket]`
 * for the bucket its CURRENT p50 falls in; a bucket the rebase left unanchored is not scaled.
 */
export function rescaleEstimator(state, workClass, refScales) {
  if (state.n === 0) return state;
  const scale = refScales[workClass]?.[bucketOf(p50Of(state))] ?? 1;
  return { ...state, mu: state.mu + Math.log(scale) };
}

/* ---------- keys, cold start ---------- */

/** Fallback order, narrowest first; each entry names which job-key parts it keeps. */
export const KEY_LEVELS = [
  { source: 'exact', parts: ['template', 'class_id', 'throughAct', 'armSetHash', 'codeFamily'] },
  { source: 'no-code-family', parts: ['template', 'class_id', 'throughAct', 'armSetHash'] },
  { source: 'no-arm-set', parts: ['template', 'class_id', 'throughAct'] },
  { source: 'no-through-act', parts: ['template', 'class_id'] },
  { source: 'runner-wide', parts: ['template'] },
];
export const CLASS_DEFAULT_SOURCE = 'class-default';

export const keyAt = (level, jobKey) => KEY_LEVELS[level].parts.map((p) => `${p}=${jobKey[p] ?? ''}`).join('|');

/** The estimator store is a Map<levelKey, state>. Every observation updates every level, so a coarser level always has the data. */
export function recordObservation(store, jobKey, { refS, censored = false }) {
  for (let level = 0; level < KEY_LEVELS.length; level += 1) {
    const k = `${level}:${keyAt(level, jobKey)}`;
    const cur = store.get(k) ?? emptyEstimator();
    store.set(k, censored ? observeCensored(cur, refS) : observeCompleted(cur, refS));
  }
  return store;
}

/** `{ p50, p90 }` of the class default: p50 = class p75 (placeholder with no data), p90 = 2 x p50. */
export function classDefault(workClass, classP75) {
  const p50 = isNum(classP75) && classP75 > 0 ? classP75 : (CLASS_DEFAULT_PLACEHOLDER_S[workClass] ?? CLASS_DEFAULT_PLACEHOLDER_S.test);
  return { p50, p90: 2 * p50 };
}

/**
 * `{ est_p50_s, est_p90_s, est_source }` in ref-s: the narrowest key with data, sigma widened x1.5 per step
 * down the chain; the class default when none has. A submitter hint is only ever a floor-clamped suggestion:
 * it can raise the class default, never lower it.
 */
export function estimateFor(store, jobKey, { workClass = 'test', classP75, hint } = {}) {
  for (let level = 0; level < KEY_LEVELS.length; level += 1) {
    const state = store.get(`${level}:${keyAt(level, jobKey)}`);
    if (!state || state.n === 0) continue;
    const sigma = sigmaOf(state) * COLD_START_SIGMA_WIDEN ** level;
    return { est_p50_s: p50Of(state), est_p90_s: p90Of(state, sigma), est_source: KEY_LEVELS[level].source };
  }
  const d = classDefault(workClass, classP75);
  const p50 = Math.max(d.p50, isNum(hint?.p50) ? hint.p50 : 0);
  const p90 = Math.max(d.p90, isNum(hint?.p90) ? hint.p90 : 0, 2 * p50);
  return { est_p50_s: p50, est_p90_s: p90, est_source: CLASS_DEFAULT_SOURCE };
}

/* ---------- speed factors ---------- */

export function bucketOf(refS) {
  if (refS < 120) return 'short';
  if (refS <= 600) return 'medium';
  return 'long';
}

/** factors: { [host]: { [workClass]: { [bucket]: { factor, n } } } } */
export const emptyFactors = () => ({});

/** Seed a cell from calibration (a time multiplier, >1 slower). The anchor host is never seeded: it is 1.00. */
export function seedFactor(factors, host, workClass, bucket, factor, { anchor = ANCHOR_HOST } = {}) {
  if (host === anchor || !(factor > 0)) return factors;
  return setCell(factors, host, workClass, bucket, { factor, n: 0 });
}

function setCell(factors, host, workClass, bucket, cell) {
  return { ...factors, [host]: { ...factors[host], [workClass]: { ...factors[host]?.[workClass], [bucket]: cell } } };
}

/** The factor for a cell; the anchor is pinned at 1.00 regardless of what is stored, an unknown cell is 1.0. */
export function factorFor(factors, host, workClass, bucket, { anchor = ANCHOR_HOST } = {}) {
  if (host === anchor) return 1;
  return factors[host]?.[workClass]?.[bucket]?.factor ?? 1;
}

/**
 * Refine a cell from one finished job: EWMA of wall / (est_p50 in ref-s), ratio winsorised around the current
 * factor. A censored run (wall is only a lower bound) only raises it. The anchor is never refined.
 */
export function observeFactor(factors, host, workClass, estP50RefS, wallS, { censored = false, anchor = ANCHOR_HOST } = {}) {
  if (host === anchor || !(estP50RefS > 0) || !(wallS > 0)) return factors;
  const bucket = bucketOf(estP50RefS);
  const cell = factors[host]?.[workClass]?.[bucket];
  const observed = wallS / estP50RefS;
  // a missing cell reads as the default 1, so a censored lower bound may only raise it
  if (!cell) return setCell(factors, host, workClass, bucket, { factor: censored ? Math.max(1, observed) : observed, n: 1 });
  const bounded = clamp(observed, cell.factor * RATIO_MIN, cell.factor * RATIO_MAX);
  const next = censored ? Math.max(cell.factor, observed) : cell.factor + ALPHA * (bounded - cell.factor);
  return setCell(factors, host, workClass, bucket, { factor: next, n: cell.n + 1 });
}

const geoMean = (xs) => Math.exp(xs.reduce((sum, x) => sum + Math.log(x), 0) / xs.length);
const hostCells = (hostFactors) => Object.values(hostFactors ?? {}).flatMap((byBucket) => Object.values(byBucket).map((c) => c.factor));
const cellsOf = (hostFactors) => Object.entries(hostFactors ?? {}).flatMap(([wc, byBucket]) => Object.keys(byBucket).map((b) => [wc, b]));

/**
 * The 30-day anchor rule, pure. `calibratedAt` is { [host]: ms of its last calibration }. While the anchor has a
 * calibration within 30 days nothing changes. Otherwise the anchor becomes the median host A (by geometric-mean factor,
 * among hosts calibrated within 30 days) and, PER (class, bucket) cell A has: every host's factor is divided by
 * factor_old[A] (a host with no cell there counts as the default 1, so the old anchor reads 1 / factor_old[A]), A's own
 * cells become exactly 1 (A is pinned, so they are dropped), and `refScales[class][bucket] = factor_old[A]` is what stored
 * ref-s estimates of that class and bucket multiply by (see rescaleEstimator). Predicted wall time ref-s x factor is
 * thereby preserved cell by cell. A cell A lacks is left alone and listed in `unanchored` as [class, bucket]: no value is guessed.
 */
export function rebaseAnchor({ factors, calibratedAt, anchor = ANCHOR_HOST, now }) {
  const unchanged = { anchor, factors, refScales: {}, unanchored: [], moved: false };
  const fresh = (h) => isNum(calibratedAt[h]) && now - calibratedAt[h] <= ANCHOR_STALE_MS;
  if (fresh(anchor)) return unchanged;
  const candidates = Object.keys(factors)
    .filter((h) => h !== anchor && fresh(h) && hostCells(factors[h]).length > 0)
    .map((h) => ({ host: h, rank: geoMean(hostCells(factors[h])) }))
    .sort((a, b) => a.rank - b.rank || (a.host < b.host ? -1 : 1));
  if (candidates.length === 0) return unchanged;
  const newAnchor = candidates[Math.floor((candidates.length - 1) / 2)].host;
  const own = factors[newAnchor];
  const refScales = {};
  for (const [wc, byBucket] of Object.entries(own)) refScales[wc] = Object.fromEntries(Object.entries(byBucket).map(([b, c]) => [b, c.factor]));
  const hosts = new Set([...Object.keys(factors), anchor]);
  hosts.delete(newAnchor);
  const rebased = {};
  for (const host of hosts) {
    let next = { ...factors[host] };
    for (const [wc, b] of cellsOf(own)) {
      const cur = factors[host]?.[wc]?.[b];
      next = { ...next, [wc]: { ...next[wc], [b]: { factor: (cur?.factor ?? 1) / refScales[wc][b], n: cur?.n ?? 0 } } };
    }
    rebased[host] = next;
  }
  const unanchored = [...new Set([...hosts].flatMap((h) => cellsOf(factors[h]).filter(([wc, b]) => !own[wc]?.[b]).map(([wc, b]) => `${wc}/${b}`)))].sort().map((k) => k.split('/'));
  return { anchor: newAnchor, factors: rebased, refScales, unanchored, moved: true };
}

/* ---------- overrun, remaining work ---------- */

/**
 * A running job's residual. Within its p90 it is whatever of the p50 is left. Past p90 it is flagged `overrun` and
 * its residual is 0.5 x elapsed_ref ref-s, capped at (max_ms / 1000 - elapsed_wall_s) / factor (`maxMs` is
 * MILLISECONDS), then converted to wall once: factor 2, 120 s elapsed -> elapsed_ref 60, residual 30 ref-s, 60 s wall.
 */
export function runningResidual({ elapsedWallS, factor, p50, p90, maxMs }) {
  const elapsedRef = elapsedWallS / factor;
  if (!(elapsedRef > p90)) {
    const residualRef = Math.max(p50 - elapsedRef, 0);
    return { overrun: false, elapsedRef, residualRef, residualWallS: residualRef * factor };
  }
  const cap = isNum(maxMs) ? Math.max((maxMs / 1000 - elapsedWallS) / factor, 0) : Infinity;
  const residualRef = Math.min(0.5 * elapsedRef, cap);
  return { overrun: true, elapsedRef, residualRef, residualWallS: residualRef * factor };
}

/** rem_ref: { [workClass]: { [bucket]: ref-s } }, bucketed by the job's own p50. Add a queued p50 or a running residual. */
export function addRemaining(rem, workClass, bucketRefS, amountRefS) {
  const bucket = bucketOf(bucketRefS);
  return { ...rem, [workClass]: { ...rem[workClass], [bucket]: (rem[workClass]?.[bucket] ?? 0) + amountRefS } };
}

/** Minutes of work on a host: EACH bucket converted with its own factor, `sum(rem_ref x factor) / 60`. */
export function remainingMinutes(rem, factorOf) {
  let wallS = 0;
  for (const [workClass, byBucket] of Object.entries(rem)) for (const [bucket, refS] of Object.entries(byBucket)) wallS += refS * factorOf(workClass, bucket);
  return wallS / 60;
}
