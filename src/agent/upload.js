import fs from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { encodeArtifacts } from '../remote-artifacts.js';
import { DEFAULT_S3_PUT_DEADLINE_MS } from '../s3-client.js';

/** Where a runner's outputs live: its own host prefix, so its RustFS key can write nothing else. Both sides derive this; it is never transmitted. */
export const outputKey = (host, ticketId, epoch, name) => `${host}/${ticketId}/e${epoch}/${name}`;
export const outProbeKey = (host) => `${host}/.probe`;

export const artifactsDirOf = (remoteRoot, execId) => path.join(path.resolve(remoteRoot), 'tickets', execId, 'artifacts');

/**
 * Signed PUT of the full log and, when the lane stored any, the artifacts stream (spooled to `artifactsFile` first, so the PUT has a size and
 * a hash). Each PUT gets what is left of the caller's budget as its deadline. Returns the hashes and sizes `ld_finish` carries.
 */
export async function uploadOutputs({ s3, bucket, host, record, logFile, artifactsDir, artifactsFile, signal, remainingMs = () => DEFAULT_S3_PUT_DEADLINE_MS }) {
  const put = (name, file) => s3.putObjectFromFile(bucket, outputKey(host, record.id, record.epoch, name), file, { signal, deadlineMs: Math.max(1, remainingMs()) });
  const log = await put('log', logFile);
  const outputs = { logSha256: log.sha256, logBytes: log.size };
  const artifacts = encodeArtifacts(artifactsDir, record.id);
  if (artifacts === null) return outputs;
  await pipeline(artifacts, fs.createWriteStream(artifactsFile));
  const stored = await put('artifacts', artifactsFile);
  return { ...outputs, artifactsSha256: stored.sha256, artifactsBytes: stored.size };
}
