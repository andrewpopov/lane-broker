import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { gitFixture, freshEnv, writeRepoConfig } from './helpers.js';
import { buildManifest, checkSymlinkEscape, RemoteIneligibleError, manifestHashOf } from '../src/remote-manifest.js';
import { encodeSnapshot, extractSnapshot } from '../src/remote-stream.js';
import { resolveTicketConfig, ConfigError } from '../src/config.js';
import { makeTmpDir } from './helpers/tmp.js';

function repoWith(files, links) {
  const dir = makeTmpDir('remote-omit-symlinks-');
  gitFixture(['init', '-q'], dir);
  for (const [rel, body] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
    fs.writeFileSync(path.join(dir, rel), body);
  }
  for (const [rel, target] of Object.entries(links)) {
    fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
    fs.symlinkSync(target, path.join(dir, rel));
  }
  gitFixture(['add', '.'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  return dir;
}

const paths = (m) => m.entries.map((e) => e.path);

test('flag off: an escaping symlink makes the tree ineligible, as before', () => {
  const dir = repoWith({ 'a.txt': 'a' }, { 'projects/x/CLAUDE.md': '../../../sibling/CLAUDE.md' });
  assert.throws(() => buildManifest(dir), (e) => e instanceof RemoteIneligibleError && e.message === 'symlink target escapes root: projects/x/CLAUDE.md');
  assert.throws(() => buildManifest(dir, { omitEscapingSymlinks: false }), /symlink target escapes root: projects\/x\/CLAUDE\.md/);
});

test('flag off: an absolute symlink target still refuses', () => {
  const dir = repoWith({ 'a.txt': 'a' }, { abs: '/etc/hosts' });
  assert.throws(() => buildManifest(dir), /symlink target is absolute: abs/);
});

test('flag on: relative and absolute escaping symlinks are omitted, the rest stays, and they are reported', () => {
  const dir = repoWith(
    { 'a.txt': 'a', 'real/f.txt': 'f' },
    { 'projects/x/CLAUDE.md': '../../../sibling/CLAUDE.md', abs: '/etc/hosts', 'inside.txt': 'a.txt' },
  );
  const m = buildManifest(dir, { omitEscapingSymlinks: true });
  assert.deepEqual(paths(m), ['a.txt', 'inside.txt', 'real/f.txt']);
  assert.deepEqual(m.omittedSymlinks, ['abs', 'projects/x/CLAUDE.md']);
  assert.equal(m.manifestHash, manifestHashOf(m.entries));
});

test('flag on, nothing escapes: no omittedSymlinks key and the same manifest as without the flag', () => {
  const dir = repoWith({ 'a.txt': 'a' }, { 'inside.txt': 'a.txt' });
  const on = buildManifest(dir, { omitEscapingSymlinks: true });
  assert.equal('omittedSymlinks' in on, false);
  assert.deepEqual(on, buildManifest(dir));
});

test('flag on: a walk that passes through another symlink entry still refuses', () => {
  // a/b/x -> ../.. is lexically fine; a/b/s -> x/../y steps through x first
  const dir = repoWith({}, { 'a/b/x': '../..', 'a/b/s': 'x/../y' });
  assert.throws(() => buildManifest(dir, { omitEscapingSymlinks: true }), /passes through a symlink entry\): a\/b\/s/);
});

test('the ancestor-is-a-symlink refusal is not an escape the flag can omit', () => {
  // git cannot index a path beneath a symlink entry (D/F conflict), so buildManifest cannot reach this case from a
  // real repo; the check is exercised directly. buildManifest only omits the two plain-escape reasons.
  assert.throws(
    () => checkSymlinkEscape('d/link', '../../out', new Set(['d', 'd/link'])),
    (e) => e instanceof RemoteIneligibleError && e.reason === 'symlink target escapes root (ancestor is a symlink)',
  );
});

test('runner side: extraction rejects an escaping symlink that arrives in a manifest, flag or no flag', async () => {
  const entries = [{ path: 'evil', type: 'symlink', target: '../../../../etc/passwd' }];
  const manifest = { entries, manifestHash: manifestHashOf(entries) };
  const base = makeTmpDir('remote-omit-symlinks-extract-');
  const dest = path.join(base, 'dest');
  const frames = [
    JSON.stringify({ protocol: 1, manifest, remoteOmitEscapingSymlinks: true }),
    JSON.stringify({ path: 'evil', type: 'symlink', target: '../../../../etc/passwd' }),
    JSON.stringify({ end: true }),
  ].map((l) => `${l}\n`);
  const result = await extractSnapshot(Readable.from(frames.map((f) => Buffer.from(f))), dest, manifest, {});
  assert.equal(result.ok, false);
  assert.match(result.reason, /symlink target escapes/);
  assert.equal(fs.existsSync(path.join(dest, 'evil')), false);
});

test('a snapshot built with the flag extracts cleanly on the runner', async () => {
  const dir = repoWith({ 'a.txt': 'a' }, { 'projects/x/CLAUDE.md': '../../../sibling/CLAUDE.md' });
  const manifest = buildManifest(dir, { omitEscapingSymlinks: true });
  const base = makeTmpDir('remote-omit-symlinks-roundtrip-');
  const dest = path.join(base, 'dest');
  const result = await extractSnapshot(encodeSnapshot(dir, { ticketId: 't1' }, manifest.entries), dest, manifest, {});
  assert.deepEqual(result, { ok: true });
  assert.equal(fs.existsSync(path.join(dest, 'a.txt')), true);
  assert.equal(fs.existsSync(path.join(dest, 'projects/x/CLAUDE.md')), false);
});

test('config: remoteOmitEscapingSymlinks defaults to false, must be a boolean, and an ad-hoc lane inherits it', () => {
  const { base } = freshEnv();
  const bad = path.join(base, 'repo-omit-bad');
  writeRepoConfig(bad, { version: 1, lanes: { default: { weight: 2, remoteOmitEscapingSymlinks: 'yes' } } });
  assert.throws(() => resolveTicketConfig({ cwd: bad, repo: 'x', lane: 'default' }), ConfigError);

  const ok = path.join(base, 'repo-omit-ok');
  writeRepoConfig(ok, { version: 1, lanes: { default: { weight: 2 }, t: { weight: 2, remote: true, remoteOmitEscapingSymlinks: true } }, undeclaredLanes: { as: 't' } });
  assert.equal(resolveTicketConfig({ cwd: ok, repo: 'x', lane: 'default' }).remoteOmitEscapingSymlinks, false);
  assert.equal(resolveTicketConfig({ cwd: ok, repo: 'x', lane: 't' }).remoteOmitEscapingSymlinks, true);
  assert.equal(resolveTicketConfig({ cwd: ok, repo: 'x', lane: 'adhoc' }).remoteOmitEscapingSymlinks, true, 'an ad-hoc lane inherits it');
});
