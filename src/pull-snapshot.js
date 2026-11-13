import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { once } from 'node:events';
import { finished } from 'node:stream/promises';
import { repoIdentity, DEFAULT_GLOBAL_CONFIG } from './config.js';
import { blobNamespace, isCacheable } from './remote-blobs.js';
import { BLOB_CACHE_MIN_BYTES, snapshotPipeline } from './remote-client.js';
import { TRANSFER_GZIP } from './remote-gzip.js';
import { readVerifiedFile } from './remote-stream.js';
import { atomicWriteJson, paths, readJsonSafe } from './state.js';

const DAY_MS = 24 * 3_600_000;
export const DEFAULT_BLOB_CONCURRENCY = 8;

export const blobIndexFile = (root) => path.join(paths(root).pull, 'blob-index.json');
export const blobKey = (ns, sha256) => `blobs/${ns}/${sha256}`;
export const streamKey = (ticketId, generation, sha256) => `streams/${ticketId}-${generation}-${sha256}.gz`;

const readBlobIndex = (root) => readJsonSafe(blobIndexFile(root)) ?? {};

function recordBlobUpload(root, indexKey, uploadedAt, refreshAfterMs) {
  const index = readBlobIndex(root);
  const live = Object.entries(index).filter(([, at]) => uploadedAt - at <= refreshAfterMs);
  atomicWriteJson(blobIndexFile(root), { ...Object.fromEntries(live), [indexKey]: uploadedAt });
}

async function mapBounded(items, limit, fn) {
  let next = 0;
  let failed = false;
  const worker = async () => {
    while (!failed && next < items.length) {
      const item = items[next++];
      try {
        await fn(item);
      } catch (err) {
        failed = true;
        throw err;
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
}

/** HEAD's Last-Modified is the age when the store reports one, then the local upload index; with neither the blob counts as old. */
async function blobIsFresh({ s3, bucket, ns, blob, root, now, refreshAfterMs, deadlineMs }) {
  const head = await s3.headObject(bucket, blobKey(ns, blob.sha256), { deadlineMs });
  if (!head.exists || head.size !== blob.size) return false;
  const uploadedAt = head.lastModified ?? readBlobIndex(root)[`${ns}/${blob.sha256}`];
  return Number.isFinite(uploadedAt) && now() - uploadedAt <= refreshAfterMs;
}

async function uploadBlob({ s3, bucket, ns, blob, entry, worktreeRoot, spool, root, now, refreshAfterMs, deadlineMs, putDeadlineMs }) {
  if (await blobIsFresh({ s3, bucket, ns, blob, root, now, refreshAfterMs, deadlineMs })) return 'skipped';
  const file = path.join(spool, `blob-${blob.sha256}`);
  fs.writeFileSync(file, readVerifiedFile(worktreeRoot, entry), { flag: 'wx' });
  await s3.putObjectFromFile(bucket, blobKey(ns, blob.sha256), file, { deadlineMs: putDeadlineMs });
  fs.unlinkSync(file);
  recordBlobUpload(root, `${ns}/${blob.sha256}`, now(), refreshAfterMs);
  return 'uploaded';
}

async function spoolStream(stream, file) {
  const hash = crypto.createHash('sha256');
  const out = fs.createWriteStream(file, { flags: 'wx' });
  let bytes = 0;
  try {
    for await (const chunk of stream) {
      hash.update(chunk);
      bytes += chunk.length;
      if (!out.write(chunk)) await once(out, 'drain');
    }
    out.end();
    await finished(out);
  } catch (err) {
    out.destroy();
    throw err;
  }
  return { sha256: hash.digest('hex'), bytes };
}

/**
 * Uploads what a pull ticket's runner will fetch and returns `{snapshot, header, stats}`. `snapshot` is the descriptor the queue pins,
 * `{key, sha256, bytes, blobs}` with `key` relative to `bucket`; `blobs` lists exactly the hashes encoded as cached frames. `header` is the
 * given header plus `repoIdentity` when the repository has one.
 */
export async function uploadSnapshot({
  s3, bucket = DEFAULT_GLOBAL_CONFIG.lanePull.rustfs.snapBucket, root, worktreeRoot, header, manifest, ticketId, generation,
  blobRefreshAfterDays = DEFAULT_GLOBAL_CONFIG.lanePull.blobRefreshAfterDays, blobCacheMinBytes = BLOB_CACHE_MIN_BYTES,
  blobConcurrency = DEFAULT_BLOB_CONCURRENCY, gzipLevel = DEFAULT_GLOBAL_CONFIG.remoteSnapshotGzipLevel, now = Date.now, deadlineMs, putDeadlineMs,
}) {
  const identity = repoIdentity(worktreeRoot);
  const stamped = identity ? { ...header, repoIdentity: identity } : header;
  const candidates = identity ? manifest.entries.filter(isCacheable) : [];
  const cacheable = candidates.reduce((sum, e) => sum + e.size, 0) >= blobCacheMinBytes ? candidates : [];
  const ns = identity ? blobNamespace(identity) : null;
  const entryByHash = new Map(cacheable.map((e) => [e.sha256, e]));
  const blobs = [...entryByHash.values()].map((e) => ({ ns, sha256: e.sha256, size: e.size })).sort((a, b) => (a.sha256 < b.sha256 ? -1 : 1));
  const spool = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-pull-snapshot-'));
  const refreshAfterMs = blobRefreshAfterDays * DAY_MS;
  try {
    const outcomes = [];
    await mapBounded(blobs, blobConcurrency, async (blob) => {
      outcomes.push(await uploadBlob({
        s3, bucket, ns, blob, entry: entryByHash.get(blob.sha256), worktreeRoot, spool, root, now, refreshAfterMs, deadlineMs, putDeadlineMs,
      }));
    });
    const cached = new Set(cacheable.map((e) => e.path));
    const { stream, stats } = snapshotPipeline({ worktreeRoot, header: stamped, manifest, cached, transferCache: cached.size > 0, transfer: TRANSFER_GZIP, gzipLevel });
    const file = path.join(spool, 'stream.gz');
    const { sha256, bytes } = await spoolStream(stream, file);
    const key = streamKey(ticketId, generation, sha256);
    await s3.putObjectFromFile(bucket, key, file, { deadlineMs: putDeadlineMs });
    return {
      snapshot: { key, sha256, bytes, blobs },
      header: stamped,
      stats: { ...stats, blobsUploaded: outcomes.filter((o) => o === 'uploaded').length, blobsSkipped: outcomes.filter((o) => o === 'skipped').length },
    };
  } finally {
    fs.rmSync(spool, { recursive: true, force: true });
  }
}
