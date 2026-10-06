import { listObjects } from './objects.js';

/**
 * Weekly backstop (spec 8.2), run inside the serving process: compare the primary's on-disk objects with the replica's
 * listing and with the journal. `notInJournal`: objects a crash left unrecorded. `missingAtReplica`/`mismatched`:
 * anything the watermark claims but the replica does not hold (`deep` re-hashes both sides). `lostAtPrimary`: journaled
 * objects that are absent from the primary's disk (accidental loss; the replica's copy is the only one left).
 */
export async function compareStores({ store, replica, deep = false }) {
  const local = await listObjects(store.root, { deep });
  const remote = new Map((await replica.list({ deep })).map((o) => [o.path, o]));
  const state = store.journal.objectState();
  const present = new Set(local.map((o) => o.path));
  const diff = { missingAtReplica: [], mismatched: [], extraAtReplica: [], notInJournal: [], lostAtPrimary: [] };
  for (const o of local) {
    const r = remote.get(o.path);
    if (!r) diff.missingAtReplica.push(o.path);
    else if (r.size !== o.size || (deep && r.sha256 !== o.sha256)) diff.mismatched.push(o.path);
    remote.delete(o.path);
    const st = state.get(o.path);
    if (st !== 'put' && st !== 'intent') diff.notInJournal.push(o.path);
  }
  for (const [rel, st] of state) if (st === 'put' && !present.has(rel)) diff.lostAtPrimary.push(rel);
  diff.extraAtReplica = [...remote.keys()];
  diff.ok = !diff.missingAtReplica.length && !diff.mismatched.length && !diff.notInJournal.length && !diff.lostAtPrimary.length;
  return diff;
}
