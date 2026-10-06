import { manifestHashOf } from '../remote-manifest.js';
import { encodeSnapshot, extractSnapshot } from '../remote-stream.js';

/**
 * Receiver side of a detached submit (spec 4.2/8.1): turn a registered manifest plus its blobs into a working
 * directory. There is deliberately no validation here of its own: the blobs are replayed as the same framed
 * stream a remote runner receives and handed to `extractSnapshot`, so path shape, symlink-escape and
 * ancestor-symlink refusal, size/exec/target agreement and the final `verifyManifestNoGit` hash pass are the one
 * implementation in remote-manifest.js / remote-stream.js. `readBlob(sha) -> Buffer` is the caller's cache or
 * fetch. Returns `{ok:true}` or `{ok:false, reason}`; never writes outside `destDir`.
 */
export async function materializeSnapshot({ manifest, destDir, readBlob, limits }) {
  if (!manifest || !Array.isArray(manifest.entries)) return { ok: false, reason: 'malformed manifest' };
  if (manifestHashOf(manifest.entries) !== manifest.manifestHash) return { ok: false, reason: 'manifest hash does not match its entries' };
  const stream = encodeSnapshot(null, {}, manifest.entries, async (entry) => {
    const bytes = await readBlob(entry.sha256);
    if (bytes.length !== entry.size) throw new Error(`blob size differs from manifest: ${entry.path}`); // keeps the frame length honest; the hash is checked by extractSnapshot
    return bytes;
  });
  return extractSnapshot(stream, destDir, manifest, limits);
}
