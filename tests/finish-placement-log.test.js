import test from 'node:test';
import assert from 'node:assert/strict';
import { finishPlacementText } from '../src/supervisor.js';

// BRAIN-899 A3: the `finish-placement` line says what every runner did -- scored, merely queued, or excluded -- and what was chosen.

const score = (name, over = {}) => ({ name, finishMs: 360_000, waitMs: 100_000, runMs: 200_000, tier: 'repo-lane', n: 3, idle: false, queued: 1, slots: 2, ...over });
const runner = (name) => ({ name });
const FAST = 'fast 360s (wait 100s, run 200s, repo-lane n=3, q=1 slots=2)';

test('a scored decision names every candidate with its queue depth and slot count, then the winner', () => {
  const picked = { runner: runner('fast'), queuedChoice: true, scores: [score('fast'), score('slow', { finishMs: 560_000, waitMs: 0, runMs: 500_000, idle: true, queued: 0, slots: 1 })], skipped: [] };
  assert.equal(
    finishPlacementText(picked),
    `${FAST} | slow 560s (wait 0s, run 500s, repo-lane n=3, q=0 slots=1) -> fast (queued)`,
  );
});

test('a runner still in the running for the choice is not an exclusion; the ones really left out are named with their reason', () => {
  const picked = {
    runner: runner('slow'),
    scores: [score('slow', { idle: true, queued: 0, slots: 1, waitMs: 0, runMs: 500_000, finishMs: 560_000 })],
    skipped: [
      { name: 'fast', reason: 'queued: 1', candidate: true },
      { name: 'down', reason: 'unreachable for 10m' },
      { name: 'paused', reason: 'paused' },
    ],
  };
  assert.equal(
    finishPlacementText(picked),
    'slow 560s (wait 0s, run 500s, repo-lane n=3, q=0 slots=1) [excluded: down: unreachable for 10m; paused: paused] -> slow',
  );
});

test('the winner is never listed as an exclusion, however it was reached', () => {
  const picked = { runner: runner('busy'), queuedChoice: true, skipped: [{ name: 'busy', reason: 'no CPU headroom (0.00 free, need 4)' }] };
  assert.equal(finishPlacementText(picked), 'unscored -> busy (queued)', 'the BRAIN-338 busy fallback won on a skip reason');
});

test('an unranked decision says unscored, and a decision with no runner says none', () => {
  assert.equal(finishPlacementText({ runner: runner('skybox'), skipped: [] }), 'unscored -> skybox');
  assert.equal(
    finishPlacementText({ runner: null, skipped: [{ name: 'skybox', reason: 'outside schedule' }] }),
    'unscored [excluded: skybox: outside schedule] -> none',
  );
  assert.equal(finishPlacementText({ runner: null, scores: [score('fast')], skipped: [] }), `${FAST} -> none`, 'a scored field with no winner is still a decision');
});

test('the prefix names the path that made the decision', () => {
  assert.equal(finishPlacementText({ runner: runner('b'), scores: [score('fast')], skipped: [] }, 'rebalance: '), `rebalance: ${FAST} -> b`);
  assert.equal(finishPlacementText({ runner: null, skipped: [{ name: 'b', reason: 'paused' }] }, 'rebalance: queued: '), 'rebalance: queued: unscored [excluded: b: paused] -> none');
});

test('a selection with no field to judge at all is not a decision and logs nothing', () => {
  for (const picked of [null, undefined, {}, { runner: null }, { runner: null, scores: [], skipped: [] }]) {
    assert.equal(finishPlacementText(picked), null, JSON.stringify(picked));
  }
});
