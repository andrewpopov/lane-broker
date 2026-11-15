import test from 'node:test';
import assert from 'node:assert/strict';
import { timeScale, scaledMs } from './helpers.js';

const speed = (v) => ({ LANE_BROKER_RUNNER_SPEED_FACTOR: v });
const override = (v, extra = {}) => ({ LANE_BROKER_TEST_TIME_SCALE: v, ...extra });

test('timeScale: a slow runner stretches budgets, anything else leaves them alone', () => {
  const table = [
    [{}, 1],
    [speed(''), 1],
    [speed('abc'), 1],
    [speed('0'), 1],
    [speed('-1'), 1],
    [speed('0.5'), 2],
    [speed('1'), 1],
    [speed('4'), 1],
    [speed('5'), 1],
    [override('3'), 3],
    [override('3', speed('0.5')), 6],
    [override('', speed('0.5')), 2],
    [override('abc'), 1],
    [override('0'), 1],
    [override('-2'), 1],
  ];
  for (const [env, expected] of table) assert.equal(timeScale(env), expected, JSON.stringify(env));
});

test('scaledMs: rounds a stretched budget up to a whole millisecond', () => {
  assert.equal(scaledMs(1000, {}), 1000);
  assert.equal(scaledMs(1001, speed('0.5')), 2002);
  assert.equal(scaledMs(1000, speed('4')), 1000);
  assert.equal(scaledMs(3, override('1.1')), 4);
});
