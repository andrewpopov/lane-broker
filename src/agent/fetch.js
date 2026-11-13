import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { fsyncDirectory, fsyncFile } from '../state.js';
import { createStrictGunzip } from '../remote-gzip.js';
import { makeReader, readHeaderLine, MAX_HEADER_BYTES } from '../remote-stream.js';
import { HAVE_LEASE_TTL_MS, acquireBlobLeases, blobNamespace, hasBlob, publishBlob } from '../remote-blobs.js';
import { brokerLock } from '../remote-runner.js';
import { blobKey } from '../pull-snapshot.js';
import { S3Error } from '../s3-client.js';
import { spoolPath } from './store.js';

/** The store did not answer as a store does (connection, timeout, a status): worth another try until the fetch budget is spent. */
export class TransportError extends Error {}

/** The store answered and the object is absent or wrong, or the stream does not describe this ticket: the ticket finishes `rejected` with `reason`. */
export class VerdictError extends Error {}

export const GET_DEADLINE_MS = 5 * 60_000;
export const SNAP_PROBE_KEY = '.probe';

export function hashFile(file) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    let bytes = 0;
    fs.createReadStream(file).on('data', (chunk) => { hash.update(chunk); bytes += chunk.length; }).on('error', reject).on('end', () => resolve({ sha256: hash.digest('hex'), bytes }));
  });
}

// The runner's key has no List, so a store may answer a missing key 403. A 403 means "missing" only while the readable probe object still reads: the key then authenticates and may read.
async function probeReads(s3, bucket, dest, signal) {
  const probe = `${dest}.probe`;
  try {
    return (await s3.getObjectToFile(bucket, SNAP_PROBE_KEY, probe, { deadlineMs: GET_DEADLINE_MS, signal })).exists;
  } catch {
    return false;
  } finally {
    fs.rmSync(probe, { force: true });
  }
}

/** One GET to `dest`: `{exists, sha256, size}`, with a missing key (404, or a 403 the probe explains) reported as `exists: false`. */
async function getObject(s3, bucket, key, dest, { signal }) {
  try {
    return await s3.getObjectToFile(bucket, key, dest, { deadlineMs: GET_DEADLINE_MS, signal });
  } catch (err) {
    if (signal?.aborted || !(err instanceof S3Error)) throw err;
    if (err.status === 403 && await probeReads(s3, bucket, dest, signal)) return { exists: false };
    throw new TransportError(err.message);
  }
}

/** The ticket's stream into `dest`, checked against the pinned size and sha256, then made durable. */
export async function downloadVerified(s3, bucket, { key, sha256, bytes }, dest, { signal }) {
  const got = await getObject(s3, bucket, key, dest, { signal });
  if (!got.exists) throw new VerdictError(`stream-missing: ${key}`);
  if (got.size !== bytes || got.sha256 !== sha256) {
    fs.rmSync(dest, { force: true });
    throw new VerdictError(`stream-corrupt: got ${got.size} bytes sha256 ${got.sha256}, the ticket pins ${bytes} bytes sha256 ${sha256}`);
  }
  fsyncFile(dest);
  fsyncDirectory(path.dirname(dest));
}

/** The stream's header line, read through the same strict gunzip `remote-exec` uses; only the first line is consumed. */
export async function readSnapshotHeader(file, { compressedBytes }) {
  const gunzip = createStrictGunzip({ maxDecodedBytes: Number.MAX_SAFE_INTEGER, maxCompressedBytes: compressedBytes });
  const source = fs.createReadStream(file);
  pipeline(source, gunzip.input).catch(() => {});
  try {
    const read = await readHeaderLine(makeReader(gunzip.output), MAX_HEADER_BYTES);
    if (!read.ok) throw new VerdictError(`stream-header: ${read.reason}`);
    return read.header;
  } finally {
    source.destroy();
    gunzip.output.destroy();
  }
}

/** Why the stream's own header is not the ticket the queue bound this execution to, or null. */
export function bindingProblem(header, { id, claim: { binding } }) {
  const manifestHash = header?.manifest?.manifestHash;
  const expected = [['ticketId', header?.ticketId, id], ['ticketId', header?.ticketId, binding.ticketId], ['generation', header?.generation, binding.generation],
    ['manifestHash', manifestHash, binding.manifestHash], ['protocol', header?.protocol ?? 1, binding.protocol]];
  const bad = expected.find(([, got, want]) => got !== want);
  return bad ? `binding-mismatch: ${bad[0]} is ${JSON.stringify(bad[1])}, the ticket binds ${JSON.stringify(bad[2])}` : null;
}

/** A TTL lease on the blobs a pulled stream names, so eviction cannot take one between the prefetch and `remote-exec`'s extraction. */
export class BlobLease {
  #release = null;

  constructor(root, hashes) {
    Object.assign(this, { root, hashes });
  }

  async renew() {
    const next = await acquireBlobLeases(this.root, this.hashes, brokerLock, { ttlMs: HAVE_LEASE_TTL_MS });
    const previous = this.#release;
    this.#release = next;
    await previous?.();
  }

  async release() {
    const previous = this.#release;
    this.#release = null;
    await previous?.();
  }
}

/**
 * Make every blob the stream was encoded without present and intact in this runner's cache. An absent object is `cache-miss` and a hash
 * mismatch `cache-corrupt`, both VerdictErrors that finish the ticket `rejected`; an unreachable store is a TransportError.
 */
export async function prefetchBlobs({ s3, bucket, root, record, header, signal }) {
  const blobs = record.claim.snapshot.blobs ?? [];
  if (blobs.length === 0) return;
  if (typeof header.repoIdentity !== 'string' || header.repoIdentity === '') throw new VerdictError('binding-mismatch: the header names no repoIdentity but the ticket lists blobs');
  const ns = blobNamespace(header.repoIdentity);
  for (const blob of blobs) {
    if (blob.ns !== ns) throw new VerdictError(`binding-mismatch: blob ${blob.sha256} is in namespace ${blob.ns}, the header's repoIdentity is ${ns}`);
    if (hasBlob(root, ns, blob.sha256, blob.size) === 'hit') continue;
    const tmp = `${spoolPath(root, record.execId)}.blob`;
    try {
      const got = await getObject(s3, bucket, blobKey(ns, blob.sha256), tmp, { signal });
      if (!got.exists) throw new VerdictError('cache-miss: 1');
      if (got.size !== blob.size || got.sha256 !== blob.sha256) throw new VerdictError('cache-corrupt: 1');
      const published = publishBlob(root, ns, { type: 'file', size: blob.size, sha256: blob.sha256 }, fs.readFileSync(tmp));
      if (published === 'skipped' || published === 'not-cached') throw new VerdictError('cache-miss: 1');
    } finally {
      fs.rmSync(tmp, { force: true });
    }
  }
}
