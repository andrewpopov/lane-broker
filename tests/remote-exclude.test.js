import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { gitFixture, writeRepoConfig } from './helpers.js';
import { buildManifest, RemoteIneligibleError } from '../src/remote-manifest.js';
import { encodeSnapshot, extractSnapshot } from '../src/remote-stream.js';
import { resolveTicketConfig, ConfigError } from '../src/config.js';
import {
  remoteExcludeShapeProblem,
  remoteExcludePreflight,
  protectedConfigPaths,
  remoteExcludeWarnings,
  MAX_REMOTE_EXCLUDE_PATTERNS,
} from '../src/remote-exclude.js';
import { makeTmpDir } from './helpers/tmp.js';

function repoWith(files, { untracked = {} } = {}) {
  const dir = makeTmpDir('remote-exclude-');
  gitFixture(['init', '-q'], dir);
  const write = (rel, body) => {
    fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
    fs.writeFileSync(path.join(dir, rel), body);
  };
  for (const [rel, body] of Object.entries(files)) write(rel, body);
  gitFixture(['add', '.'], dir);
  gitFixture(['commit', '-q', '-m', 'init'], dir);
  for (const [rel, body] of Object.entries(untracked)) write(rel, body);
  return dir;
}

const paths = (m) => m.entries.map((e) => e.path);
const FILES = { 'a.txt': 'a', 'art/big.png': 'x'.repeat(1000), 'art/sub/more.png': 'y'.repeat(500), 'src/b.js': 'b' };

test('remote-exclude: excluded path absent from the manifest, the stream and the runner workdir', async () => {
  const dir = repoWith(FILES, { untracked: { 'art/untracked.png': 'u' } });
  const manifest = buildManifest(dir, { exclude: ['art/'] });
  assert.deepEqual(paths(manifest), ['a.txt', 'src/b.js']);
  assert.equal(manifest.excluded.count, 3);
  assert.equal(manifest.excluded.bytes, 1501);

  const chunks = [];
  for await (const c of encodeSnapshot(dir, { ticketId: 't1' }, manifest.entries)) chunks.push(Buffer.from(c));
  assert.equal(Buffer.concat(chunks).includes('art/big.png'), false, 'the stream never names an excluded path');

  const dest = path.join(makeTmpDir('remote-exclude-dest-'), 'dest');
  const result = await extractSnapshot(encodeSnapshot(dir, { ticketId: 't1' }, manifest.entries), dest, manifest, {});
  assert.deepEqual(result, { ok: true });
  assert.equal(fs.existsSync(path.join(dest, 'a.txt')), true);
  assert.equal(fs.existsSync(path.join(dest, 'art')), false);
});

test('an excluded denylisted file is out of consideration, not an ineligibility refusal', () => {
  const dir = repoWith({ ...FILES, '.env': 'SECRET=1' });
  assert.throws(() => buildManifest(dir), (e) => e instanceof RemoteIneligibleError);
  assert.deepEqual(paths(buildManifest(dir, { exclude: ['.env'] })), ['a.txt', 'art/big.png', 'art/sub/more.png', 'src/b.js']);
});

test('glob semantics: * stays in one segment, ** spans segments, a bare file pattern does not match a directory contents', () => {
  const dir = repoWith(FILES);
  assert.deepEqual(paths(buildManifest(dir, { exclude: ['art/*.png'] })), ['a.txt', 'art/sub/more.png', 'src/b.js']);
  assert.deepEqual(paths(buildManifest(dir, { exclude: ['**/*.png'] })), ['a.txt', 'src/b.js']);
  assert.deepEqual(paths(buildManifest(dir, { exclude: ['art'] })), ['a.txt', 'art/big.png', 'art/sub/more.png', 'src/b.js']);
});

test('an old-style lane without remoteExclude is unchanged: same manifestHash, no excluded key', () => {
  const dir = repoWith(FILES);
  const plain = buildManifest(dir);
  assert.equal('excluded' in plain, false);
  assert.deepEqual(buildManifest(dir, { exclude: null }), plain);
  assert.equal(buildManifest(dir, { exclude: ['nothing-here/'] }).manifestHash, plain.manifestHash);
});

test('dispatch and the rebalance unchanged-check build the same manifest from the same exclude', () => {
  const dir = repoWith(FILES);
  const opts = { omitEscapingSymlinks: false, exclude: ['art/'] };
  assert.equal(buildManifest(dir, opts).manifestHash, buildManifest(dir, opts).manifestHash);
  assert.notEqual(buildManifest(dir, opts).manifestHash, buildManifest(dir, { omitEscapingSymlinks: false }).manifestHash);
});

test('every buildManifest call site in the supervisor and client passes the lane exclude', () => {
  const root = path.resolve(import.meta.dirname, '../src');
  for (const [file, sites] of [['supervisor.js', 2], ['remote-client.js', 1]]) {
    const calls = fs.readFileSync(path.join(root, file), 'utf8').split('\n').filter((l) => /\bbuildManifest\(/.test(l) && !l.includes('import'));
    assert.equal(calls.length, sites, `${file} call sites`);
    for (const line of calls) assert.match(line, /exclude:/, `${file}: ${line.trim()}`);
  }
});

test('a pattern matching nothing warns; a matching one reports count, size and per-pattern counts', () => {
  const dir = repoWith(FILES);
  const m = buildManifest(dir, { exclude: ['art/', 'typo/'] });
  assert.deepEqual(m.excluded.unmatched, ['typo/']);
  const lines = remoteExcludeWarnings(m.excluded);
  assert.equal(lines[0], 'lane: remote snapshot excludes 2 files / 0.0 MB (art/: 2)\n');
  assert.match(lines[1], /"typo\/" matched no files/);
  assert.deepEqual(remoteExcludeWarnings(undefined), []);
});

function configWith(lane, extra = {}) {
  const dir = makeTmpDir('remote-exclude-config-');
  writeRepoConfig(dir, { version: 1, lanes: { default: { weight: 2, ...lane } }, ...extra });
  return dir;
}
const resolve = (dir, lane = 'default') => resolveTicketConfig({ cwd: dir, repo: 'x', lane });

test('config: valid remoteExclude resolves, absent resolves to null, an ad-hoc lane inherits it', () => {
  assert.equal(resolve(configWith({})).remoteExclude, null);
  assert.deepEqual(resolve(configWith({ remoteExclude: ['art/', '**/*.psd'] })).remoteExclude, ['art/', '**/*.psd']);
  const dir = configWith({ remoteExclude: ['art/'] }, { undeclaredLanes: { as: 'default' } });
  assert.deepEqual(resolve(dir, 'adhoc').remoteExclude, ['art/']);
});

test('config: patterns covering .lane-broker.json, a remoteDeps package.json or lockfile are refused', () => {
  for (const [lane, pattern] of [
    [{}, '.lane-broker.json'],
    [{}, '.*'],
    [{}, '**'],
    [{ remoteDeps: ['.'] }, 'package-lock.json'],
    [{ remoteDeps: ['.'] }, '*.json'],
    [{ remoteDeps: ['web'] }, 'web/'],
    [{ remoteDeps: ['web'] }, 'web/package.json'],
    [{ remoteDeps: ['web'] }, '**/npm-shrinkwrap.json'],
  ]) {
    assert.throws(() => resolve(configWith({ ...lane, remoteExclude: [pattern] })), (e) => e instanceof ConfigError && /remoteExclude/.test(e.message), `${pattern} with ${JSON.stringify(lane)}`);
  }
});

const preflight = (dir, patterns, relCwd = '', remoteDeps = null) => remoteExcludePreflight(patterns, { worktreeRoot: dir, relCwd, remoteDeps });
const CFG = JSON.stringify({ version: 1, lanes: { default: { weight: 1 } } });

test('preflight: a nested config that governs the invocation directory is protected while the root one survives', () => {
  const dir = repoWith({ '.lane-broker.json': CFG, 'apps/api/.lane-broker.json': CFG, 'apps/api/x.js': 'x', 'apps/web/y.js': 'y' });
  assert.match(preflight(dir, ['apps/api/.lane-broker.json'], 'apps/api'), /governs this run/);
  assert.match(preflight(dir, ['apps/**/.lane-broker.json'], 'apps/api'), /apps\/api\/\.lane-broker\.json/);
  assert.equal(preflight(dir, ['apps/api/.lane-broker.json'], ''), null, 'from the root the nested file does not govern');
  assert.match(preflight(dir, ['.lane-broker.json'], 'apps/api'), /would exclude \.lane-broker\.json/);
});

test('preflight: invocation from different subdirectories protects different config paths', () => {
  const dir = repoWith({ '.lane-broker.json': CFG, 'apps/api/.lane-broker.json': CFG, 'apps/api/src/z.js': 'z', 'apps/web/y.js': 'y' });
  assert.deepEqual(protectedConfigPaths(dir, 'apps/api/src'), ['apps/api/.lane-broker.json', '.lane-broker.json']);
  assert.deepEqual(protectedConfigPaths(dir, 'apps/web'), ['.lane-broker.json']);
  assert.deepEqual(protectedConfigPaths(dir, ''), ['.lane-broker.json']);
  assert.equal(preflight(dir, ['apps/api/.lane-broker.json'], 'apps/web'), null);
});

test('preflight: an exclusion that empties the invocation directory is refused, by surviving files not by name', () => {
  const dir = repoWith({ '.lane-broker.json': CFG, 'web/app/a.js': 'a', 'web/app/b.js': 'b', 'other.js': 'o' });
  assert.match(preflight(dir, ['web/'], 'web/app'), /no files in the lane's working directory web\/app/);
  assert.match(preflight(dir, ['web/app/*.js'], 'web/app'), /no files/);
  assert.match(preflight(dir, ['**/*.js'], 'web/app'), /no files/);
  assert.equal(preflight(dir, ['web/app/a.js'], 'web/app'), null);
  assert.equal(preflight(dir, ['other.js'], 'web/app'), null);
});

test('preflight: remoteDeps files are protected', () => {
  const dir = repoWith({ '.lane-broker.json': CFG, 'web/package.json': '{}', 'web/package-lock.json': '{}', 'web/a.js': 'a' });
  assert.match(preflight(dir, ['web/package-lock.json'], 'web', ['web']), /package-lock\.json/);
});

test('limits: 65 patterns, negation, absolute, .. and malformed patterns are refused', () => {
  const ok = Array.from({ length: MAX_REMOTE_EXCLUDE_PATTERNS }, (_, i) => `d${i}/`);
  assert.equal(remoteExcludeShapeProblem(ok), null);
  assert.match(remoteExcludeShapeProblem([...ok, 'one-more/']), /more than 64/);
  assert.match(remoteExcludeShapeProblem(['!x']), /negation/);
  assert.match(remoteExcludeShapeProblem(['/abs']), /absolute/);
  assert.match(remoteExcludeShapeProblem(['../x']), /\.\./);
  assert.match(remoteExcludeShapeProblem(['a/../x']), /\.\./);
  assert.match(remoteExcludeShapeProblem(['x'.repeat(257)]), /longer than 256/);
  assert.equal(remoteExcludeShapeProblem(['x'.repeat(256)]), null);
  assert.match(remoteExcludeShapeProblem(['a**b']), /whole segment/);
  assert.match(remoteExcludeShapeProblem(['a//b']), /empty/);
  assert.match(remoteExcludeShapeProblem(['a', 'a']), /duplicated/);
  assert.match(remoteExcludeShapeProblem([]), /non-empty/);
  assert.match(remoteExcludeShapeProblem('art/'), /non-empty array/);
  assert.match(remoteExcludeShapeProblem([5]), /strings/);
  assert.throws(() => resolve(configWith({ remoteExclude: ['!x'] })), ConfigError);
});
