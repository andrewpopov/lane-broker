import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { createStore } from '../src/store/server.js';
import { createHmacVerifier, signToken } from '../src/store/auth.js';
import { StoreClient } from '../src/store/client.js';
import { manifestHashOf } from '../src/remote-manifest.js';

export const SECRET = 'test-secret-not-for-production';
export const sha = (data) => crypto.createHash('sha256').update(data).digest('hex');

/** A controllable clock shared by a store and the test, so retention and ages never depend on wall time. */
export function fakeClock(start = 1_700_000_000_000) {
  const clock = { t: start, now: () => clock.t, advance(ms) { clock.t += ms; } };
  return clock;
}

export function token(claims, { clock } = {}) {
  const now = clock ? clock.now() : Date.now();
  return signToken(SECRET, { exp: Math.floor(now / 1000) + 365 * 24 * 3600, ...claims });
}

/** One in-process lane-store on a disposable dir, bound to loopback. Tests run two of these (primary + replica). */
export async function startStore({ clock = fakeClock(), root, ...opts } = {}) {
  const dir = root ?? fs.mkdtempSync(path.join(os.tmpdir(), 'lane-store-test-'));
  const s = await createStore({ root: dir, verifier: createHmacVerifier(SECRET, { now: clock.now }), now: clock.now, ...opts });
  const addr = await s.listen('127.0.0.1', 0);
  const url = `http://127.0.0.1:${addr.port}`;
  const client = (claims) => new StoreClient({ baseUrl: url, token: token(claims, { clock }) });
  return {
    ...s,
    root: dir,
    url,
    clock,
    client,
    admin: client({ role: 'admin' }),
    replicaPeer: client({ role: 'replica' }),
    submit: client({ role: 'submit' }),
    reader: (job) => client({ role: 'read', job }),
  };
}

/** A manifest of in-memory files: `{name: content}` -> `{manifest, blobs: Map(sha -> Buffer)}`. */
export function snapshotOf(files, symlinks = {}) {
  const blobs = new Map();
  const entries = [];
  for (const [name, content] of Object.entries(files)) {
    const buf = Buffer.from(content);
    blobs.set(sha(buf), buf);
    entries.push({ path: name, type: 'file', exec: false, size: buf.length, sha256: sha(buf) });
  }
  for (const [name, target] of Object.entries(symlinks)) entries.push({ path: name, type: 'symlink', target });
  return { manifest: { manifestHash: manifestHashOf(entries), entries }, blobs };
}

export async function publish(srv, job, snap) {
  for (const [h, buf] of snap.blobs) await srv.submit.putBlob(h, buf);
  await srv.submit.putManifest(job, snap.manifest);
}

export function tmpDir(prefix = 'lane-store-dest') {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
}
