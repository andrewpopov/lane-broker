import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { shardFiles, parseShard } from '../scripts/test-files.js';

const LAST = 'tests/zz-tmp-leak-guard.test.js';
const files = ['tests/a.test.js', 'tests/b.test.js', 'tests/c.test.js', 'tests/d.test.js', 'tests/e.test.js', LAST];
const durations = { 'tests/a.test.js': 100, 'tests/b.test.js': 60, 'tests/c.test.js': 50, 'tests/d.test.js': 10, [LAST]: 1 };

test('the shards cover every file exactly once, apart from the leak guard, which ends each shard', () => {
  const shards = [1, 2, 3].map((i) => shardFiles(files, durations, i, 3));
  for (const shard of shards) assert.equal(shard.at(-1), LAST);
  const dealt = shards.flatMap((shard) => shard.slice(0, -1)).sort();
  assert.deepEqual(dealt, files.filter((f) => f !== LAST));
});

test('files go longest first to the lightest shard; an unmeasured file counts as the median', () => {
  // a(100) -> 1; b(60) -> 2; c(50) -> 2 (60 < 100); e(median 50) -> 1 (100 < 110); d(10) -> 2 (110 < 150)
  assert.deepEqual(shardFiles(files, durations, 1, 2), ['tests/a.test.js', 'tests/e.test.js', LAST]);
  assert.deepEqual(shardFiles(files, durations, 2, 2), ['tests/b.test.js', 'tests/c.test.js', 'tests/d.test.js', LAST]);
});

test('one shard is the whole suite in name order', () => {
  assert.deepEqual(shardFiles(files, durations, 1, 1), files);
});

test('TRAIN_SHARD is i/N with 1 <= i <= N; unset means the whole suite', () => {
  assert.deepEqual(parseShard(undefined), { shard: 1, total: 1 });
  assert.deepEqual(parseShard('2/4'), { shard: 2, total: 4 });
  for (const bad of ['0/4', '5/4', '2', 'a/b', '1/0']) assert.throws(() => parseShard(bad), /TRAIN_SHARD/);
});

test('tests/durations.json names only test files that exist', () => {
  const durationsFile = JSON.parse(fs.readFileSync(new URL('./durations.json', import.meta.url), 'utf8'));
  const missing = Object.keys(durationsFile).filter((f) => !fs.existsSync(new URL(`../${f}`, import.meta.url)));
  assert.deepEqual(missing, []);
});
