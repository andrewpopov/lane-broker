import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  GUARD, HEAVY, listTestFiles, selectFiles, resolveConcurrency, weighFiles, orderLongestFirst, predictMakespan, parseFileTimings,
} from '../scripts/test-tier.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const universe = listTestFiles(here);
const weights = JSON.parse(fs.readFileSync(path.join(here, '..', 'scripts', 'test-weights.json'), 'utf8'));
const MAX_FILE_SECONDS = 400;

test('pr and heavy partition every test file except the guard, each exactly once', () => {
  const pr = selectFiles('pr', universe);
  const heavy = selectFiles('heavy', universe);
  assert.deepEqual(pr.filter((name) => heavy.includes(name)), []);
  assert.deepEqual([...pr, ...heavy].sort(), universe.filter((name) => name !== GUARD));
  assert.equal(new Set([...pr, ...heavy]).size, pr.length + heavy.length);
});

test('the all tier is the whole universe minus the guard, which no tier pools', () => {
  assert.deepEqual(selectFiles('all', universe), universe.filter((name) => name !== GUARD));
  for (const tier of ['pr', 'heavy', 'all']) assert.ok(!selectFiles(tier, universe).includes(GUARD), tier);
  assert.ok(universe.includes(GUARD));
});

test('every heavy file exists', () => {
  for (const name of HEAVY) assert.ok(universe.includes(name), name);
  assert.deepEqual(selectFiles('heavy', universe).sort(), [...HEAVY].sort());
});

test('an unknown tier is refused', () => {
  assert.throws(() => selectFiles('nightly', universe), /unknown tier/);
});

test('ordering is longest-first and a file with no weight weighs the median', () => {
  const ordered = orderLongestFirst(['a', 'b', 'c', 'd', 'new'], { a: 1, b: 9, c: 5, d: 3 });
  assert.deepEqual(ordered.map((entry) => entry.file), ['b', 'c', 'new', 'd', 'a']);
  assert.equal(weighFiles(['new'], { a: 1, b: 9, c: 5, d: 3 })[0].seconds, 4);
});

test('the makespan prediction is a greedy longest-first simulation plus the guard', () => {
  const ordered = orderLongestFirst(['a', 'b', 'c', 'd'], { a: 8, b: 5, c: 4, d: 3 });
  assert.equal(predictMakespan(ordered, 1), 20 + 1);
  assert.equal(predictMakespan(ordered, 2), 11 + 1);
  assert.equal(predictMakespan(ordered, 9), 8 + 1);
});

test('concurrency is the floor of the lane CPU grant, else the flag, else 1', () => {
  assert.equal(resolveConcurrency(undefined, { LANE_BROKER_CPU_CORES: '4.5' }), 4);
  assert.equal(resolveConcurrency(undefined, { LANE_BROKER_CPU_CORES: '4' }), 4);
  assert.equal(resolveConcurrency(undefined, {}), 1);
  assert.equal(resolveConcurrency(undefined, { LANE_BROKER_CPU_CORES: 'lots' }), 1);
  assert.equal(resolveConcurrency(undefined, { LANE_BROKER_CPU_CORES: '0.5' }), 1);
  assert.equal(resolveConcurrency('3', { LANE_BROKER_CPU_CORES: '8' }), 3);
  assert.equal(resolveConcurrency('x', { LANE_BROKER_CPU_CORES: '8' }), 8);
});

test('per-file timing lines parse back into weights', () => {
  assert.deepEqual(parseFileTimings('noise\ntest-tier file a.test.js 12.5s\ntest-tier total 13.0s\n'), { 'a.test.js': 12.5 });
});

test('every weight names an existing test file, so a stale weight fails', () => {
  assert.deepEqual(Object.keys(weights).filter((name) => !universe.includes(name)), []);
});

test(`no single file is weighted over ${MAX_FILE_SECONDS}s, so none becomes the critical path again`, () => {
  const heavySet = new Set(HEAVY);
  const tooLong = Object.entries(weights).filter(([name, seconds]) => seconds > MAX_FILE_SECONDS && !heavySet.has(name));
  assert.deepEqual(tooLong, []);
});
