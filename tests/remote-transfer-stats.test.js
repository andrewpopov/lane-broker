import test from 'node:test';
import assert from 'node:assert/strict';
import { snapshotLine, relayedTransfer } from '../src/remote-relay.js';

/** BRAIN-642 slice D: the per-dispatch snapshot log line and the history fields. */

test('remote-transfer-stats: full line with gzip, cache and exclude', () => {
  const stats = { mode: 'gzip', treeBytes: 238_100_000, sentBytes: 41_000_000, cacheFiles: 8788, cachedFiles: 8730 };
  assert.equal(snapshotLine('skybox', stats, { count: 0 }), 'lane: snapshot -> skybox: tree 238.1 MB, sent 41.0 MB (gzip), cache 8730/8788 files (99.3%), excluded 0\n');
});

test('remote-transfer-stats: plain transfer without cache or exclude omits those parts', () => {
  const stats = { mode: 'plain', treeBytes: 5_000_000, sentBytes: 5_000_000, cacheFiles: 0, cachedFiles: 0 };
  assert.equal(snapshotLine('wintop', stats, undefined), 'lane: snapshot -> wintop: tree 5.0 MB, sent 5.0 MB\n');
});

test('remote-transfer-stats: history fields carry client bytes and runner blob counts, optional and validated', () => {
  const fields = relayedTransfer({ treeBytes: 10, sentBytes: 4 }, { blobStats: { hits: 3, misses: 1, corrupt: 0, published: 1 } });
  assert.deepEqual(fields, { snapshotRawBytes: 10, snapshotSentBytes: 4, blobHits: 3, blobMisses: 1, blobCorrupt: 0 });
  assert.deepEqual(relayedTransfer(null, {}), {});
  assert.deepEqual(relayedTransfer({ treeBytes: -1, sentBytes: 'x' }, { blobStats: { hits: 1.5 } }), {});
});
