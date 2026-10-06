import fs from 'node:fs';
import path from 'node:path';
import { parseObjectPath, blobRelPath, manifestRelPath } from './ids.js';

/** Spec 8.3 defaults. */
export const RETENTION_DEFAULTS = Object.freeze({
  manifestAfterTerminalMs: 14 * 24 * 3600 * 1000, // manifests: until the group is terminal + 14 d
  unreferencedBlobMs: 24 * 3600 * 1000, // blobs: while referenced; unreferenced for 24 h, then swept
  evictAtFraction: 0.8, // above this share of the cap, evict oldest terminal groups
});

function jobs(store) {
  const dir = path.join(store.root, 'manifests');
  return fs.readdirSync(dir).map((n) => parseObjectPath(`manifests/${n}`)).filter(Boolean).map((p) => p.job);
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
    for (const name of fs.readdirSync(dir)) {
      const full = path.join(dir, name);
      const st = fs.lstatSync(full);
      if (st.isDirectory()) walk(full);
      else if (st.isFile() && !referenced.has(name) && now - st.mtimeMs >= graceMs && store.deleteObject(blobRelPath(name))) deleted += 1;
    }
  };
  walk(path.join(store.root, 'blobs'));
  return deleted;
}

/**
 * One retention pass. Every deletion goes through `store.deleteObject` (intent, unlink, delete). A pinned job is never
 * expired or evicted; a non-terminal job's manifest and blobs are always kept.
 */
export function sweep(store, { now = store.now(), ...overrides } = {}) {
  // A replica's retention is local and time-based: it never applies the primary's deletions. It sweeps a manifest only when
  // the replicated state says terminal and unpinned AND the replica received it more than the grace ago, and an
  // unreferenced blob only after the same grace, so a manifest still in flight can never lose its blobs.
  const cfg = { ...RETENTION_DEFAULTS, ...(store.replicaMode ? { manifestAfterTerminalMs: store.replicaGraceMs, unreferencedBlobMs: store.replicaGraceMs } : {}), ...overrides };
  const expired = jobs(store).filter((job) => {
    const m = store.meta(job);
    if (m.pinned || m.terminalAt == null) return false;
    return now - (store.replicaMode ? m.registeredAt : m.terminalAt) >= cfg.manifestAfterTerminalMs;
  });
  expired.forEach((job) => store.deleteObject(manifestRelPath(job)));
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
      store.deleteObject(manifestRelPath(job));
      evicted.push(job);
      // Space pressure: free the evicted group's blobs now (no grace), unless another group still references them.
      const stillReferenced = referencedBlobs(store);
      for (const sha of owned) if (!stillReferenced.has(sha) && store.deleteObject(blobRelPath(sha))) blobsDeleted += 1;
    }
  }
  return { expiredManifests: expired, evictedManifests: evicted, blobsDeleted, bytes: store.bytes };
}
