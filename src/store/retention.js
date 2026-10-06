import fs from 'node:fs';
import path from 'node:path';
import { parseObjectPath, blobRelPath } from './ids.js';

/** Spec 8.3 defaults. */
export const RETENTION_DEFAULTS = Object.freeze({
  manifestAfterTerminalMs: 14 * 24 * 3600 * 1000, // manifests: until the group is terminal + 14 d
  unreferencedBlobMs: 24 * 3600 * 1000, // blobs: while referenced; unreferenced for 24 h, then swept
  evictAtFraction: 0.8, // above this share of the cap, evict oldest terminal groups
});

function jobs(store) {
  const dir = path.join(store.root, 'manifests');
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).map((n) => parseObjectPath(`manifests/${n}`)).filter(Boolean).map((p) => p.job);
}

function removeJob(store, job) {
  fs.rmSync(store.abs(`manifests/${job}.json`), { force: true });
  fs.rmSync(store.abs(`meta/${job}.json`), { force: true });
  store.jobBlobCache.delete(job);
}

function referencedBlobs(store) {
  const referenced = new Set();
  for (const job of jobs(store)) for (const sha of store.jobBlobs(job) ?? []) referenced.add(sha);
  return referenced;
}

function sweepBlobs(store, now, graceMs) {
  const referenced = referencedBlobs(store);
  let deleted = 0;
  const walk = (dir) => {
    for (const name of fs.existsSync(dir) ? fs.readdirSync(dir) : []) {
      const full = path.join(dir, name);
      const st = fs.lstatSync(full);
      if (st.isDirectory()) walk(full);
      else if (!referenced.has(name) && now - st.mtimeMs >= graceMs) {
        fs.unlinkSync(full);
        store.bytes -= st.size;
        deleted += 1;
      }
    }
  };
  walk(path.join(store.root, 'blobs'));
  return deleted;
}

/**
 * One retention pass. A pinned job (an in-use snapshot) is never expired or evicted, so its blobs stay
 * referenced. Deletions are not journaled: the journal records commits, and a replica keeps what it was sent
 * until its own sweep (its terminal marks are separate state).
 */
export function sweep(store, { now = store.now(), ...overrides } = {}) {
  const cfg = { ...RETENTION_DEFAULTS, ...overrides };
  const expired = jobs(store).filter((job) => {
    const m = store.meta(job);
    return !m.pinned && m.terminalAt != null && now - m.terminalAt >= cfg.manifestAfterTerminalMs;
  });
  expired.forEach((job) => removeJob(store, job));
  let blobsDeleted = sweepBlobs(store, now, cfg.unreferencedBlobMs);

  const evicted = [];
  if (Number.isFinite(store.capBytes)) {
    const candidates = jobs(store)
      .map((job) => ({ job, ...store.meta(job) }))
      .filter((m) => !m.pinned && m.terminalAt != null)
      .sort((a, b) => a.terminalAt - b.terminalAt);
    while (store.bytes >= store.capBytes * cfg.evictAtFraction && candidates.length) {
      const { job } = candidates.shift();
      const owned = store.jobBlobs(job) ?? new Set();
      removeJob(store, job);
      evicted.push(job);
      // Space pressure: free the evicted group's blobs now (no grace), unless another group still references them.
      const stillReferenced = referencedBlobs(store);
      for (const sha of owned) {
        if (stillReferenced.has(sha)) continue;
        const size = store.blobSize(sha);
        if (size === null) continue;
        fs.unlinkSync(store.abs(blobRelPath(sha)));
        store.bytes -= size;
        blobsDeleted += 1;
      }
    }
  }
  return { expiredManifests: expired, evictedManifests: evicted, blobsDeleted, bytes: store.bytes };
}
