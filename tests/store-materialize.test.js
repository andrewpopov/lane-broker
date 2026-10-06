import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { startStore, snapshotOf, publish, sha, tmpDir } from './store-harness.js';
import { materializeSnapshot } from '../src/store/materialize.js';
import { manifestHashOf } from '../src/remote-manifest.js';

const fromMap = (blobs) => async (h) => {
  const b = blobs.get(h);
  if (!b) throw new Error(`no blob ${h}`);
  return b;
};

function manifestOf(entries) {
  return { manifestHash: manifestHashOf(entries), entries };
}

const file = (p, content) => ({ path: p, type: 'file', exec: false, size: Buffer.byteLength(content), sha256: sha(content) });

async function refused(entries, blobs, pattern) {
  const parent = tmpDir('materialize-parent');
  const dest = path.join(parent, 'work');
  const res = await materializeSnapshot({ manifest: manifestOf(entries), destDir: dest, readBlob: fromMap(blobs) });
  assert.equal(res.ok, false, 'must be refused');
  assert.match(res.reason, pattern);
  const stray = fs.readdirSync(parent).filter((n) => n !== 'work');
  assert.deepEqual(stray, [], 'nothing written beside the destination');
  return { parent, dest };
}

test('a stored snapshot materializes into a working dir, files and in-root symlinks intact', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const snap = snapshotOf({ 'src/a.js': 'console.log(1)', 'README.md': 'hi' }, { 'link-to-readme': 'README.md' });
  await publish(srv, 'job-1', snap);
  const reader = srv.reader('job-1');
  const manifest = await reader.getManifest('job-1');
  const dest = path.join(tmpDir(), 'work');
  const res = await materializeSnapshot({ manifest, destDir: dest, readBlob: (h) => reader.getBlob(h) });
  assert.deepEqual(res, { ok: true });
  assert.equal(fs.readFileSync(path.join(dest, 'src/a.js'), 'utf8'), 'console.log(1)');
  assert.equal(fs.readlinkSync(path.join(dest, 'link-to-readme')), 'README.md');
});

test('path-traversal entries are refused at the receiver', async () => {
  const content = 'payload';
  const blobs = new Map([[sha(content), Buffer.from(content)]]);
  for (const bad of ['../escape.txt', 'a/../../escape.txt', '/etc/escape.txt', 'a//b.txt', './x.txt']) {
    await refused([file(bad, content)], blobs, /invalid path|absolute path/);
  }
});

test('a symlink that escapes the root, or is later written through, is refused at the receiver', async () => {
  const content = 'payload';
  const blobs = new Map([[sha(content), Buffer.from(content)]]);
  await refused([{ path: 'link', type: 'symlink', target: '/etc' }], blobs, /symlink target absolute/);
  await refused([{ path: 'a/link', type: 'symlink', target: '../../outside' }], blobs, /symlink target escapes/);
  await refused([{ path: 'link', type: 'symlink', target: 'sub' }, file('link/evil.txt', content)], blobs, /ancestor is symlink/);
});

test('a blob whose bytes do not match the manifest hash is refused', async () => {
  const good = 'trusted!';
  const blobs = new Map([[sha(good), Buffer.from('tampered')]]); // same length, wrong content
  await refused([file('f.txt', good)], blobs, /hash mismatch/);
  const short = new Map([[sha(good), Buffer.from('short')]]);
  await refused([file('f.txt', good)], short, /size differs|file body|size mismatch/);
});

test('a manifest whose claimed hash does not match its entries, and an existing destination, are refused', async () => {
  const content = 'x';
  const blobs = new Map([[sha(content), Buffer.from(content)]]);
  const entries = [file('f.txt', content)];
  const res = await materializeSnapshot({ manifest: { manifestHash: sha('forged'), entries }, destDir: path.join(tmpDir(), 'w'), readBlob: fromMap(blobs) });
  assert.deepEqual(res, { ok: false, reason: 'manifest hash does not match its entries' });
  const exists = await materializeSnapshot({ manifest: manifestOf(entries), destDir: tmpDir(), readBlob: fromMap(blobs) });
  assert.deepEqual(exists, { ok: false, reason: 'destDir already exists' });
});
