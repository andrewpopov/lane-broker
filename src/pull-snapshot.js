import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
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

export async function mapBounded(items, limit, fn) {
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
  const settled = await Promise.allSettled(Array.from({ length: Math.min(limit, items.length) }, worker));
  const failure = settled.find((r) => r.status === 'rejected');
  if (failure) throw failure.reason;
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

async function spoolStream(stream, out) {
  const hash = crypto.createHash('sha256');
  let bytes = 0;
  const tap = new Transform({
    transform(chunk, _encoding, done) {
      hash.update(chunk);
      bytes += chunk.length;
      done(null, chunk);
    },
  });
  await pipeline(stream, tap, out);
  return { sha256: hash.digest('hex'), bytes };
}

export const withRepoIdentity = (header, worktreeRoot) => {
  const identity = repoIdentity(worktreeRoot);
  return identity ? { ...header, repoIdentity: identity } : header;
};

/**
 * Uploads what a pull ticket's runner will fetch and returns `{snapshot, stats}`. `snapshot` is the descriptor the queue pins,
 * `{key, sha256, bytes, blobs}` with `key` relative to `bucket`; `blobs` lists exactly the hashes encoded as cached frames. `header` is used
 * as given: the caller stamps `repoIdentity` (`withRepoIdentity`) before the envelope stores it, so the stream and the ticket agree.
 */
export async function uploadSnapshot({
  s3, bucket = DEFAULT_GLOBAL_CONFIG.lanePull.rustfs.snapBucket, root, worktreeRoot, header, manifest, ticketId, generation,
  blobRefreshAfterDays = DEFAULT_GLOBAL_CONFIG.lanePull.blobRefreshAfterDays, blobCacheMinBytes = BLOB_CACHE_MIN_BYTES,
  blobConcurrency = DEFAULT_BLOB_CONCURRENCY, gzipLevel = DEFAULT_GLOBAL_CONFIG.remoteSnapshotGzipLevel, now = Date.now, deadlineMs, putDeadlineMs,
  openSpool = (file) => fs.createWriteStream(file, { flags: 'wx' }),
}) {
  const identity = header.repoIdentity;
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
    const { stream, stats } = snapshotPipeline({ worktreeRoot, header, manifest, cached, transferCache: cached.size > 0, transfer: TRANSFER_GZIP, gzipLevel });
    const file = path.join(spool, 'stream.gz');
    const { sha256, bytes } = await spoolStream(stream, openSpool(file));
    const key = streamKey(ticketId, generation, sha256);
    await s3.putObjectFromFile(bucket, key, file, { deadlineMs: putDeadlineMs });
    return {
      snapshot: { key, sha256, bytes, blobs },
      stats: { ...stats, blobsUploaded: outcomes.filter((o) => o === 'uploaded').length, blobsSkipped: outcomes.filter((o) => o === 'skipped').length },
    };
  } finally {
    fs.rmSync(spool, { recursive: true, force: true });
  }
}
