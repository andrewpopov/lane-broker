import test from 'node:test';
import assert from 'node:assert/strict';
import { fingerprintOf } from '../src/state.js';

/**
 * fingerprintOf is now an exported, shared helper (src/load.js and
 * src/admission.js both use it), not a private template literal — so it
 * needs to actually satisfy what its name promises: encode type as well as
 * value, and join unambiguously so an embedded delimiter can't forge a
 * different tuple. Today's callers only ever pass validated finite numbers,
 * so none of this was reachable in production before — these tests are for
 * the next caller.
 */

test('fingerprintOf is stable: the same tuple produces the same fingerprint across calls', () => {
  assert.equal(fingerprintOf(90, 70, 3), fingerprintOf(90, 70, 3));
  assert.equal(fingerprintOf('a', 'b'), fingerprintOf('a', 'b'));
});

test('fingerprintOf is type-sensitive: a number and its string form must not collide', () => {
  assert.notEqual(fingerprintOf(1), fingerprintOf('1'));
});

test('fingerprintOf is delimiter-safe: a value containing the delimiter cannot forge a different tuple', () => {
  assert.notEqual(fingerprintOf('1|2', 3), fingerprintOf(1, '2|3'));
});

test('fingerprintOf keeps its existing numeric-tuple output stable (no persisted-state reset for today\'s callers)', () => {
  // Pinned to the exact previous (pre-fix) output for pure numeric tuples,
  // since the caller comment promises this stays byte-identical.
  assert.equal(fingerprintOf(90, 70, 3), '90|70|3');
  assert.equal(fingerprintOf(15, 11, 3), '15|11|3');
});
