import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { gitFixture } from './helpers.js';
import { buildManifest, manifestHashOf } from '../src/remote-manifest.js';
import { encodeSnapshot, extractSnapshot } from '../src/remote-stream.js';

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
}

function tmpRepo() {
  const dir = tmpDir('remote-stream-repo');
  gitFixture(['init', '-q'], dir);
  return dir;
}

/** Build a Readable from an ordered list of chunks (Buffer or utf8 string). */
function streamOf(chunks) {
  async function* gen() {
    for (const c of chunks) yield Buffer.isBuffer(c) ? c : Buffer.from(c, 'utf8');
  }
  return Readable.from(gen());
}

function line(obj) {
  return `${JSON.stringify(obj)}\n`;
}

test('round trip: encode -> extract -> verify ok, including exec bit and a symlink', async () => {
  const src = tmpRepo();
  fs.writeFileSync(path.join(src, 'plain.txt'), 'hello world');
  const exe = path.join(src, 'run.sh');
  fs.writeFileSync(exe, '#!/bin/sh\necho hi\n');
  fs.chmodSync(exe, 0o755);
  fs.writeFileSync(path.join(src, 'target.txt'), 'target content');
  fs.symlinkSync('target.txt', path.join(src, 'link.txt'));
  gitFixture(['add', '.'], src);
  gitFixture(['commit', '-q', '-m', 'init'], src);

  const manifest = buildManifest(src);
  const streamReadable = encodeSnapshot(src, { ticketId: 't1' }, manifest.entries);

  const base = tmpDir('remote-stream-extract');
  const dest = path.join(base, 'dest');
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.deepEqual(result, { ok: true });

  assert.equal(fs.readFileSync(path.join(dest, 'plain.txt'), 'utf8'), 'hello world');
  assert.equal(fs.statSync(path.join(dest, 'run.sh')).mode & 0o777, 0o755);
  assert.equal(fs.readlinkSync(path.join(dest, 'link.txt')), 'target.txt');
});

test('destDir already existing is refused', async () => {
  const src = tmpRepo();
  fs.writeFileSync(path.join(src, 'a.txt'), 'a');
  gitFixture(['add', 'a.txt'], src);
  gitFixture(['commit', '-q', '-m', 'init'], src);
  const manifest = buildManifest(src);

  const base = tmpDir('remote-stream-existing');
  const dest = path.join(base, 'dest');
  fs.mkdirSync(dest);
  fs.writeFileSync(path.join(dest, 'sentinel'), 'do-not-touch');

  const streamReadable = encodeSnapshot(src, {}, manifest.entries);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /already exists/);
  assert.equal(fs.readFileSync(path.join(dest, 'sentinel'), 'utf8'), 'do-not-touch');
});

// ---- adversarial extractor inputs, each hand-built against a small real manifest ----

function smallManifest() {
  const src = tmpRepo();
  fs.writeFileSync(path.join(src, 'a.txt'), 'aaaa');
  gitFixture(['add', 'a.txt'], src);
  gitFixture(['commit', '-q', '-m', 'init'], src);
  return { src, manifest: buildManifest(src) };
}

function freshDest(prefix) {
  const base = tmpDir(prefix);
  return { base, dest: path.join(base, 'dest') };
}

async function assertNothingOutsideDest(base, dest) {
  const names = fs.readdirSync(base);
  assert.deepEqual(names.filter((n) => n !== 'dest'), [], `nothing besides dest/ was created in ${base}`);
  // dest may or may not exist depending on how early extraction failed.
  void dest;
}

test('adversarial: unlisted path is rejected before writing', async () => {
  const { manifest } = smallManifest();
  const { base, dest } = freshDest('remote-stream-adv-unlisted');
  const body = Buffer.from('xx');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'not-in-manifest.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /^unlisted path:/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: duplicate path is rejected', async () => {
  const { manifest } = smallManifest();
  const { base, dest } = freshDest('remote-stream-adv-dup');
  const body = Buffer.from('aaaa');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /^duplicate path:/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: ../ path traversal is rejected', async () => {
  const { manifest } = smallManifest();
  const { base, dest } = freshDest('remote-stream-adv-dotdot');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: '../escape.txt', type: 'file', exec: false, size: 0 }),
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /invalid path|unlisted path/);
  await assertNothingOutsideDest(base, dest);
  assert.equal(fs.existsSync(path.join(base, 'escape.txt')), false);
});

test('adversarial: absolute path is rejected', async () => {
  const { manifest } = smallManifest();
  const { base, dest } = freshDest('remote-stream-adv-abs');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: '/etc/evil.txt', type: 'file', exec: false, size: 0 }),
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /^absolute path:/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: a frame nested under a manifest symlink ancestor is rejected', async () => {
  const entries = [
    { path: 'link', type: 'symlink', target: 'somewhere' },
    { path: 'link/evil.txt', type: 'file', exec: false, size: 2, sha256: 'x' },
  ];
  const manifest = { entries, manifestHash: manifestHashOf(entries) };
  const { base, dest } = freshDest('remote-stream-adv-ancestor');
  const body = Buffer.from('xx');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'link', type: 'symlink', target: 'somewhere' }),
    line({ path: 'link/evil.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /ancestor is symlink/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: a symlink target escaping destDir is rejected even when it matches the manifest', async () => {
  const entries = [{ path: 'evil', type: 'symlink', target: '../../../../etc/passwd' }];
  const manifest = { entries, manifestHash: manifestHashOf(entries) };
  const { base, dest } = freshDest('remote-stream-adv-symescape');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'evil', type: 'symlink', target: '../../../../etc/passwd' }),
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /symlink target escapes/);
  assert.equal(fs.existsSync(path.join(dest, 'evil')), false);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: size mismatch vs manifest is rejected', async () => {
  const { manifest } = smallManifest();
  const { base, dest } = freshDest('remote-stream-adv-sizemismatch');
  const body = Buffer.from('a'); // manifest says size 4
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /^size mismatch:/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: an oversize frame beyond limits.maxFileBytes is rejected', async () => {
  const entries = [{ path: 'big.txt', type: 'file', exec: false, size: 1000, sha256: 'x' }];
  const manifest = { entries, manifestHash: manifestHashOf(entries) };
  const { base, dest } = freshDest('remote-stream-adv-oversize');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'big.txt', type: 'file', exec: false, size: 1000 }),
    Buffer.alloc(1000, 'x'),
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, { maxFileBytes: 100 });
  assert.equal(result.ok, false);
  assert.match(result.reason, /^oversize frame:/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: a truncated stream (missing terminator/body) is rejected', async () => {
  const { manifest } = smallManifest();
  const { base, dest } = freshDest('remote-stream-adv-truncated');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'a.txt', type: 'file', exec: false, size: 4 }),
    Buffer.from('aa'), // only 2 of 4 bytes, then stream ends
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /truncated stream/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: trailing garbage after the terminator is rejected', async () => {
  const { manifest } = smallManifest();
  const { base, dest } = freshDest('remote-stream-adv-trailing');
  const body = Buffer.from('aaaa');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
    'garbage-after-terminator\n',
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /trailing bytes/);
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: one corrupted byte in a file body passes framing but fails the final hash re-verify', async () => {
  const { manifest } = smallManifest(); // a.txt = "aaaa", sha256 of that
  const { base, dest } = freshDest('remote-stream-adv-corrupt');
  const body = Buffer.from('aaab'); // same length, different bytes -> size matches, hash won't
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /^hash mismatch: a\.txt$/);
  // The corrupted file IS written inside dest (framing accepted it) -- that's expected;
  // the invariant is that nothing was written OUTSIDE dest.
  await assertNothingOutsideDest(base, dest);
});

test('adversarial: an early terminator (missing manifest entry) is rejected', async () => {
  const entries = [
    { path: 'a.txt', type: 'file', exec: false, size: 4, sha256: 'x' },
    { path: 'b.txt', type: 'file', exec: false, size: 4, sha256: 'y' },
  ];
  const manifest = { entries, manifestHash: manifestHashOf(entries) };
  const { base, dest } = freshDest('remote-stream-adv-earlyend');
  const body = Buffer.from('aaaa');
  const streamReadable = streamOf([
    line({ protocol: 1, manifest: { entries: manifest.entries, manifestHash: manifest.manifestHash } }),
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }), // b.txt never sent
  ]);
  const result = await extractSnapshot(streamReadable, dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /missing entry/);
  await assertNothingOutsideDest(base, dest);
});
