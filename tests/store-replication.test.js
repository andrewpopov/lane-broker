import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { startStore, fakeClock, snapshotOf, publish, sha } from './store-harness.js';
import { Replicator, readReplicationState, replicationLag } from '../src/store/replicate.js';
import { compareStores } from '../src/store/compare.js';
import { readJournal } from '../src/store/journal.js';
import { blobRelPath } from '../src/store/ids.js';

async function pair() {
  const clock = fakeClock();
  const primary = await startStore({ clock });
  const replica = await startStore({ clock });
  return { clock, primary, replica, close: () => Promise.all([primary.close(), replica.close()]) };
}

const put = (srv, text) => srv.submit.putBlob(sha(text), Buffer.from(text));

/** A replica client whose verify step can be overridden, to model a replica that cannot confirm what it stored. */
function wrap(real, overrides) {
  return {
    putObject: (...a) => (overrides.putObject ?? real.putObject.bind(real))(...a),
    verifyObject: (...a) => (overrides.verifyObject ?? real.verifyObject.bind(real))(...a),
  };
}

test('replicates in journal order and verifies every object at the replica', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  const snap = snapshotOf({ 'a.txt': 'aaa', 'b.txt': 'bbb' });
  await publish(p.primary, 'job-1', snap);
  const shipped = [];
  const r = new Replicator({ store: p.primary.store, replica: p.replica.replicaPeer, now: p.clock.now, onShipped: (e) => shipped.push(e.seq) });
  const result = await r.runOnce();
  assert.deepEqual({ ok: result.ok, shipped: result.shipped, seq: result.replicatedSeq }, { ok: true, shipped: 3, seq: 3 });
  assert.deepEqual(shipped, [1, 2, 3]);
  for (const h of snap.blobs.keys()) assert.equal(p.replica.store.hasBlob(h), true);
  assert.deepEqual((await p.replica.replicaPeer.getManifest('job-1')).manifestHash, snap.manifest.manifestHash);
  assert.equal(replicationLag(p.primary.store, p.clock.now()).oldestUnreplicatedAgeSeconds, 0);
});

test('the age metric is the age of the first unreplicated entry, 0 once drained, and is served at /metrics', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'one');
  p.clock.advance(600_000);
  await put(p.primary, 'two');
  p.clock.advance(60_000);
  assert.equal(replicationLag(p.primary.store, p.clock.now()).oldestUnreplicatedAgeSeconds, 660);
  const metrics = async () => (await (await fetch(`${p.primary.url}/metrics`)).text());
  assert.match(await metrics(), /^oldest_unreplicated_object_age_seconds 660$/m);
  await new Replicator({ store: p.primary.store, replica: p.replica.replicaPeer, now: p.clock.now }).runOnce();
  assert.match(await metrics(), /^oldest_unreplicated_object_age_seconds 0$/m);
  assert.equal(replicationLag(p.primary.store, p.clock.now()).oldestUnreplicatedAgeSeconds, 0);
});

test('the journal resumes after a crash mid-replication: the persisted watermark is where the next round starts', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  for (const x of ['a', 'b', 'c', 'd']) await put(p.primary, x);
  let verifies = 0;
  const crashing = wrap(p.replica.replicaPeer, {
    verifyObject: (rel) => {
      verifies += 1;
      if (verifies === 3) throw new Error('simulated crash after the replica stored seq 3, before the watermark moved');
      return p.replica.replicaPeer.verifyObject(rel);
    },
  });
  const first = await new Replicator({ store: p.primary.store, replica: crashing, now: p.clock.now }).runOnce();
  assert.equal(first.ok, false);
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, 2);
  assert.equal(readReplicationState(p.primary.root).failedRounds, 1);

  const puts = [];
  const resumed = wrap(p.replica.replicaPeer, { putObject: (e, f) => { puts.push(e.seq); return p.replica.replicaPeer.putObject(e, f); } });
  const second = await new Replicator({ store: p.primary.store, replica: resumed, now: p.clock.now }).runOnce();
  assert.equal(second.ok, true);
  assert.deepEqual(puts, [3, 4], 'seq 1-2 are not re-shipped; seq 3 (stored but unconfirmed) is');
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, 4);
  assert.equal(readReplicationState(p.primary.root).failedRounds, 0);
  for (const x of ['a', 'b', 'c', 'd']) assert.equal(p.replica.store.hasBlob(sha(x)), true);
});

test('the watermark never advances past an object the replica could not verify', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  for (const x of ['a', 'b', 'c']) await put(p.primary, x);
  const liar = wrap(p.replica.replicaPeer, {
    verifyObject: async (rel) => (rel.endsWith(sha('b')) ? { sha256: sha('corrupted'), size: 1 } : p.replica.replicaPeer.verifyObject(rel)),
  });
  const result = await new Replicator({ store: p.primary.store, replica: liar, now: p.clock.now }).runOnce();
  assert.equal(result.ok, false);
  assert.match(result.error, /replica verification failed for seq 2/);
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, 1, 'stops before seq 2 and never skips ahead to seq 3');
  p.clock.advance(120_000);
  assert.ok(replicationLag(p.primary.store, p.clock.now()).oldestUnreplicatedAgeSeconds >= 120, 'the recovery point keeps ageing');

  const missing = wrap(p.replica.replicaPeer, { verifyObject: async () => null });
  const again = await new Replicator({ store: p.primary.store, replica: missing, now: p.clock.now }).runOnce();
  assert.equal(again.ok, false);
  assert.equal(readReplicationState(p.primary.root).replicatedSeq, 1);
  assert.equal(readReplicationState(p.primary.root).failedRounds, 2);
});

test('an object created mid-replication is a higher seq and is not missed', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'first');
  await put(p.primary, 'second');
  let injected = false;
  const r = new Replicator({
    store: p.primary.store,
    replica: p.replica.replicaPeer,
    now: p.clock.now,
    onShipped: async (entry) => {
      if (entry.seq === 1 && !injected) {
        injected = true;
        await put(p.primary, 'created while the round was running');
      }
    },
  });
  const result = await r.runOnce();
  assert.equal(result.replicatedSeq, 3);
  assert.equal(p.replica.store.hasBlob(sha('created while the round was running')), true);
  assert.equal(readJournal(p.primary.root).length, 3);
});

test('full compare finds a planted divergence: missing, corrupted and unjournaled objects', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  for (const x of ['keep', 'lost', 'rotted']) await put(p.primary, x);
  const r = new Replicator({ store: p.primary.store, replica: p.replica.replicaPeer, now: p.clock.now });
  await r.runOnce();
  const cmp = () => compareStores({ store: p.primary.store, replica: p.replica.replicaPeer, deep: true });
  assert.equal((await cmp()).ok, true);

  fs.rmSync(path.join(p.replica.root, blobRelPath(sha('lost'))));
  fs.writeFileSync(path.join(p.replica.root, blobRelPath(sha('rotted'))), 'ROTT'); // same size, different bytes
  const orphan = Buffer.from('written, then crashed before the journal append');
  const orphanPath = path.join(p.primary.root, blobRelPath(sha(orphan)));
  fs.mkdirSync(path.dirname(orphanPath), { recursive: true });
  fs.writeFileSync(orphanPath, orphan);

  const diff = await cmp();
  assert.equal(diff.ok, false);
  assert.deepEqual(diff.missingAtReplica.sort(), [blobRelPath(sha('lost')), blobRelPath(sha(orphan))].sort());
  assert.deepEqual(diff.mismatched, [blobRelPath(sha('rotted'))]);
  assert.deepEqual(diff.notInJournal, [blobRelPath(sha(orphan))]);

  // repair path: re-journal the unrecorded object, replicate, compare again (the corrupt replica copy needs a manual delete)
  await p.primary.admin.rejournal([...diff.notInJournal]);
  fs.rmSync(path.join(p.replica.root, blobRelPath(sha('rotted'))));
  await p.primary.admin.rejournal([blobRelPath(sha('lost')), blobRelPath(sha('rotted'))]);
  assert.equal((await r.runOnce()).ok, true);
  assert.equal((await cmp()).ok, true);
});
