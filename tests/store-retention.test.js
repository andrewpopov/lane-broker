import test from 'node:test';
import assert from 'node:assert/strict';
import { startStore, fakeClock, snapshotOf, publish, sha } from './store-harness.js';
import { sweep } from '../src/store/retention.js';

const DAY = 24 * 3600 * 1000;

test('retention keeps pinned and still-referenced objects and sweeps expired ones', async (t) => {
  const clock = fakeClock();
  const srv = await startStore({ clock });
  t.after(() => srv.close());
  const shared = { 'shared.txt': 'shared by both' };
  const pinned = snapshotOf({ ...shared, 'pinned.txt': 'only in the pinned job' });
  const expiring = snapshotOf({ ...shared, 'expiring.txt': 'only in the expiring job' });
  const recent = snapshotOf({ 'recent.txt': 'terminal only 2 days ago' });
  await publish(srv, 'pinned', pinned);
  await publish(srv, 'expiring', expiring);
  await publish(srv, 'recent', recent);
  const stray = Buffer.from('uploaded, never referenced');
  await srv.submit.putBlob(sha(stray), stray);

  await srv.admin.json('PUT', '/jobs/pinned/terminal');
  await srv.admin.json('PUT', '/pins/pinned');
  await srv.admin.json('PUT', '/jobs/expiring/terminal');
  clock.advance(12 * DAY);
  await srv.admin.json('PUT', '/jobs/recent/terminal');
  clock.advance(2 * DAY + 1000);

  // t = +14 d: `pinned` and `expiring` are past 14 d terminal, `recent` is not; the stray blob is past 24 h.
  const result = sweep(srv.store);
  assert.deepEqual(result.expiredManifests, ['expiring']);
  assert.equal(srv.store.readManifest('pinned') !== null, true, 'pinned manifest kept');
  assert.equal(srv.store.readManifest('recent') !== null, true, 'not yet 14 d past terminal');
  for (const [h] of pinned.blobs) assert.equal(srv.store.hasBlob(h), true, 'pinned job blobs kept (incl. the one it shares)');
  assert.equal(srv.store.hasBlob(sha('only in the expiring job')), false, 'blob referenced only by the expired manifest is swept');
  assert.equal(srv.store.hasBlob(sha(stray)), false, 'unreferenced blob older than 24 h is swept');
  for (const [h] of recent.blobs) assert.equal(srv.store.hasBlob(h), true);

  await srv.admin.json('DELETE', '/pins/pinned');
  assert.deepEqual(sweep(srv.store).expiredManifests, ['pinned'], 'once unpinned it expires');
});

test('a fresh unreferenced blob survives the 24 h grace window, and a re-upload restarts it', async (t) => {
  const clock = fakeClock();
  const srv = await startStore({ clock });
  t.after(() => srv.close());
  const data = Buffer.from('in flight');
  await srv.submit.putBlob(sha(data), data);
  clock.advance(23 * 3600 * 1000);
  assert.equal(sweep(srv.store).blobsDeleted, 0);
  assert.equal(srv.store.hasBlob(sha(data)), true);
});

test('above 80% of the cap the oldest terminal, unpinned groups are evicted first', async (t) => {
  const clock = fakeClock();
  const srv = await startStore({ clock, capBytes: 3000 });
  t.after(() => srv.close());
  const mk = (name, fill) => snapshotOf({ [name]: String(fill).repeat(700) });
  const [a, b, c] = [mk('a', 1), mk('b', 2), mk('c', 3)];
  await publish(srv, 'old', a);
  await publish(srv, 'mid', b);
  await publish(srv, 'live', c); // never terminal: must never be evicted
  await srv.admin.json('PUT', '/jobs/old/terminal');
  clock.advance(1000);
  await srv.admin.json('PUT', '/jobs/mid/terminal');
  clock.advance(2 * 3600 * 1000);
  const result = sweep(srv.store);
  assert.deepEqual(result.evictedManifests, ['old']);
  assert.equal(srv.store.readManifest('mid') !== null, true);
  assert.equal(srv.store.readManifest('live') !== null, true);
  assert.ok(srv.store.bytes < 2400);
});
