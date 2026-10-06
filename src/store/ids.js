/** Identifier shapes for lane-store. Every id that becomes a path component is matched here first, so a
 *  path can only be built from a validated sha or job id (no `..`, no separators, no leading dot). */
export const SHA256_RE = /^[0-9a-f]{64}$/;
export const JOB_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function isSha256(s) {
  return typeof s === 'string' && SHA256_RE.test(s);
}

export function isJobId(s) {
  return typeof s === 'string' && JOB_ID_RE.test(s);
}

/** Store-relative path of a blob: sha256 with a 2-level fan-out (`ab/cd/<sha>`). */
export function blobRelPath(sha) {
  return `blobs/${sha.slice(0, 2)}/${sha.slice(2, 4)}/${sha}`;
}

export function manifestRelPath(job) {
  return `manifests/${job}.json`;
}

/** Inverse of blobRelPath/manifestRelPath; null for anything that is not exactly one of those shapes. */
export function parseObjectPath(rel) {
  if (typeof rel !== 'string') return null;
  const blob = /^blobs\/([0-9a-f]{2})\/([0-9a-f]{2})\/([0-9a-f]{64})$/.exec(rel);
  if (blob && blob[3].startsWith(blob[1] + blob[2])) return { kind: 'blob', sha: blob[3] };
  const manifest = /^manifests\/(.+)\.json$/.exec(rel);
  if (manifest && isJobId(manifest[1])) return { kind: 'manifest', job: manifest[1] };
  return null;
}
