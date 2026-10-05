import test from 'node:test';
import assert from 'node:assert/strict';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { score, effectiveRank, orderQueue, originOrNow, waitedMs } from '../src/priority.js';

// BRAIN-380 slice 1: pure scoring and ordering on a fake clock. Nothing here touches the disk or Date.now.

const MIN = 60_000;
const cfg = DEFAULT_GLOBAL_CONFIG;
const ticket = (id, seq, tier, origin = 0) => ({ id, seq, priorityAdmitted: tier, prioOriginAt: origin });
const ids = (queue) => queue.map((t) => (t === null ? null : t.id));

test('defaults: score is tier plus age, one tier per 10 minutes, capped at the high-tier weight', () => {
  const fresh = (tier) => score(ticket('t', 1, tier), 0, cfg);
  assert.deepEqual([fresh('low'), fresh('medium'), fresh('high')], [0, 1, 2]);
  assert.equal(score(ticket('t', 1, 'low'), 10 * MIN, cfg), 1);
  assert.equal(score(ticket('t', 1, 'low'), 20 * MIN, cfg), 2);
  assert.equal(score(ticket('t', 1, 'low'), 90 * MIN, cfg), 2, 'aging stops at the ceiling');
  assert.equal(score(ticket('t', 1, 'high'), 20 * MIN, cfg), 2, 'a high never scores above the ceiling');
});

test('boundary 9:59 / 10:00 / 20:00: a low ticket reaches medium, then high, and seq decides each tie', () => {
  const order = (nowEff, fresh) => ids(orderQueue([ticket('low', 1, 'low', 0), { ...ticket('fresh', 2, fresh, nowEff) }], nowEff, cfg));
  assert.deepEqual(order(9 * MIN + 59_000, 'medium'), ['fresh', 'low'], 'at 9:59 the low ticket is still behind a fresh medium');
  assert.deepEqual(order(10 * MIN, 'medium'), ['low', 'fresh'], 'at 10:00 it ties a fresh medium and wins on its lower seq');
  assert.deepEqual(order(19 * MIN + 59_000, 'high'), ['fresh', 'low'], 'at 19:59 it is still behind a fresh high');
  assert.deepEqual(order(20 * MIN, 'high'), ['low', 'fresh'], 'at 20:00 it ties a fresh high and wins on its lower seq');
  const rank = (nowEff) => effectiveRank(ticket('low', 1, 'low', 0), nowEff, cfg);
  assert.deepEqual([rank(9 * MIN + 59_000), rank(10 * MIN), rank(20 * MIN), rank(80 * MIN)], [0, 1, 2, 2], 'display rank steps once per aging period and caps at high');
});

test('the ceiling tie: a low aged 20 minutes against a fresh high is decided by seq, in either direction', () => {
  const nowEff = 20 * MIN;
  assert.deepEqual(ids(orderQueue([ticket('low', 1, 'low', 0), ticket('high', 2, 'high', nowEff)], nowEff, cfg)), ['low', 'high']);
  assert.deepEqual(ids(orderQueue([ticket('high', 1, 'high', nowEff), ticket('low', 2, 'low', 0)], nowEff, cfg)), ['high', 'low']);
});

test('sustained high arrivals never pass an aged low that has the lowest seq', () => {
  // Two highs arrive every minute and one ticket starts every minute, so a backlog of aged highs builds up.
  let queue = [ticket('low', 1, 'low', 0)];
  let seq = 1;
  let startedLowAt = null;
  for (let minute = 1; minute <= 40 && startedLowAt === null; minute += 1) {
    const nowEff = minute * MIN;
    for (let i = 0; i < 2; i += 1) {
      seq += 1;
      queue.push(ticket(`high-${seq}`, seq, 'high', nowEff));
    }
    const [head, ...rest] = orderQueue(queue, nowEff, cfg);
    queue = rest;
    if (head.id === 'low') startedLowAt = minute;
  }
  assert.equal(startedLowAt, 20, 'the low ticket starts the minute it reaches the ceiling, not later and not earlier');
});

test('a ticket without a trustworthy origin starts at zero age; an origin ahead of the clock is not trusted', () => {
  assert.equal(originOrNow(undefined, 5000), 5000);
  assert.equal(originOrNow(NaN, 5000), 5000);
  assert.equal(originOrNow(9000, 5000), 5000, 'an origin in the future of the priority clock is distrusted');
  assert.equal(originOrNow(3000, 5000), 3000);
  assert.equal(waitedMs({ priorityAdmitted: 'low' }, 7 * MIN), 0, 'no origin, no age, whatever createdAt says');
  assert.equal(waitedMs({ priorityAdmitted: 'low', createdAt: 0 }, 7 * MIN), 0, 'createdAt never feeds priority age');
});

test('orderQueue treats an unreadable record as a barrier: nothing moves across it', () => {
  const raw = [ticket('a', 1, 'low'), ticket('b', 2, 'high'), null, ticket('c', 4, 'low'), ticket('d', 5, 'high')];
  assert.deepEqual(ids(orderQueue(raw, 0, cfg)), ['b', 'a', null, 'd', 'c'], 'each run is reordered on its own, the null stays where it was');
  assert.deepEqual(ids(orderQueue([null, ticket('x', 1, 'low'), ticket('y', 2, 'high')], 0, cfg)), [null, 'y', 'x'], 'a null head stays the head');
  assert.deepEqual(ids(orderQueue([], 0, cfg)), []);
});

test('equal scores order by seq, and a record without seq falls back to its position', () => {
  assert.deepEqual(ids(orderQueue([ticket('b', 2, 'medium'), ticket('a', 1, 'medium')], 0, cfg)), ['a', 'b']);
  assert.deepEqual(ids(orderQueue([{ id: 'p', priorityAdmitted: 'medium' }, { id: 'q', priorityAdmitted: 'medium' }], 0, cfg)), ['p', 'q']);
});
