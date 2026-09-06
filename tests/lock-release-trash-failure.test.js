import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { withLock, paths, readJsonSafe } from '../src/state.js';
import { freshEnv } from './helpers.js';

/**
 * A failing trash-directory cleanup must never make the release path fall
 * back to deleting `lockDir` directly: by the time the rename-away has
 * succeeded, a contender may already have re-acquired that same path, and an
 * unconditional `rmSync(lockDir)` in the catch would delete a live lock that
 * is no longer ours.
 */
test('a failing trash cleanup after a successful rename-away does not touch a contender\'s re-acquired lock', async () => {
  const { state } = freshEnv();
  const lockDir = paths(state).lock;
  const ownerFile = path.join(lockDir, 'owner.json');

  const origRmSync = fs.rmSync;
  let sawTrashCleanupAttempt = false;
  const contenderToken = crypto.randomUUID();

  fs.rmSync = (target, opts) => {
    const base = path.basename(String(target));
    if (base.startsWith('.lock-release-') && !sawTrashCleanupAttempt) {
      sawTrashCleanupAttempt = true;
      // Simulate a contender's withLock() winning the race and re-acquiring
      // the now-free lockDir path *before* our trash cleanup runs.
      fs.mkdirSync(lockDir, { recursive: true });
      fs.writeFileSync(ownerFile, JSON.stringify({ pid: 999999, token: contenderToken, start: null, at: Date.now() }));
      throw new Error('simulated trash cleanup failure');
    }
    return origRmSync(target, opts);
  };

  try {
    await withLock(state, () => {
      // Lock held; nothing to do. The release path runs in withLock's finally.
    });
  } finally {
    fs.rmSync = origRmSync;
  }

  assert.equal(sawTrashCleanupAttempt, true, 'the test did not actually exercise the trash-cleanup failure path');

  const owner = readJsonSafe(ownerFile);
  assert.ok(owner, 'the contender\'s lock directory must still exist after our failed trash cleanup');
  assert.equal(owner.token, contenderToken, 'the contender\'s owner token must be untouched by our release path');
});
