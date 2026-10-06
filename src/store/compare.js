import path from 'node:path';
import { listObjects } from './objects.js';
import { readJournal } from './journal.js';

/**
 * Weekly backstop (spec 8.2): compare the primary's on-disk objects with the replica's listing and with the
 * journal. `notInJournal` finds objects a crash left unrecorded (the replicator can never see them);
 * `missingAtReplica`/`mismatched` find anything the watermark claims but the replica does not hold; `deep`
 * re-hashes both sides instead of trusting sizes. `replica.list({deep})` returns `[{path, size, sha256?}]`.
 */
export async function compareStores({ root, replica, deep = false }) {
  const local = await listObjects(path.resolve(root), { deep });
  const remote = new Map((await replica.list({ deep })).map((o) => [o.path, o]));
  const journaled = new Set(readJournal(root).map((r) => r.path));
  const diff = { missingAtReplica: [], mismatched: [], extraAtReplica: [], notInJournal: [] };
  for (const o of local) {
    const r = remote.get(o.path);
    if (!r) diff.missingAtReplica.push(o.path);
    else if (r.size !== o.size || (deep && r.sha256 !== o.sha256)) diff.mismatched.push(o.path);
    remote.delete(o.path);
    if (!journaled.has(o.path)) diff.notInJournal.push(o.path);
  }
  diff.extraAtReplica = [...remote.keys()];
  diff.ok = !diff.missingAtReplica.length && !diff.mismatched.length && !diff.notInJournal.length;
  return diff;
}
