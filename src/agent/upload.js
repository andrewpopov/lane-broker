import fs from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { encodeArtifacts } from '../remote-artifacts.js';

/** Where a runner's outputs live: its own host prefix, so its RustFS key can write nothing else. Both sides derive this; it is never transmitted. */
export const outputKey = (host, ticketId, epoch, name) => `${host}/${ticketId}/e${epoch}/${name}`;
export const outProbeKey = (host) => `${host}/.probe`;

export const artifactsDirOf = (remoteRoot, execId) => path.join(path.resolve(remoteRoot), 'tickets', execId, 'artifacts');

/**
 * Signed PUT of the full log and, when the lane stored any, the artifacts stream (spooled to `artifactsFile` first, so the PUT has a size and
 * a hash). Returns the hashes and sizes `ld_finish` carries.
 */
export async function uploadOutputs({ s3, bucket, host, record, logFile, artifactsDir, artifactsFile, signal }) {
  const put = (name, file) => s3.putObjectFromFile(bucket, outputKey(host, record.id, record.epoch, name), file, { signal });
  const log = await put('log', logFile);
  const outputs = { logSha256: log.sha256, logBytes: log.size };
  const artifacts = encodeArtifacts(artifactsDir, record.id);
  if (artifacts === null) return outputs;
  await pipeline(artifacts, fs.createWriteStream(artifactsFile));
  const stored = await put('artifacts', artifactsFile);
  return { ...outputs, artifactsSha256: stored.sha256, artifactsBytes: stored.size };
}
