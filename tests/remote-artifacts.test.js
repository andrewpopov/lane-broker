import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { isValidRemoteArtifactsShape, resolveTicketConfig, ConfigError } from '../src/config.js';
import {
  artifactMatches,
  collectArtifacts,
  encodeArtifacts,
  installArtifacts,
  pruneStaleArtifacts,
  receiveArtifacts,
} from '../src/remote-artifacts.js';
import { makeReader } from '../src/remote-stream.js';
import { freshEnv, writeRepoConfig } from './helpers.js';
import { makeGitWorktree } from './remote-harness.js';

const LIMITS = { maxFileBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };
const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
const tmp = (prefix) => fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));

function workDirWith(files) {
  const dir = tmp('artifacts-work');
  for (const [rel, content] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
    fs.writeFileSync(path.join(dir, rel), content);
  }
  return dir;
}

function collect(workDir, patterns, limits = LIMITS) {
  const dest = path.join(tmp('artifacts-dest'), 'artifacts');
  return { dest, res: collectArtifacts(workDir, patterns, dest, limits) };
}

function stream(chunks) {
  return makeReader(Readable.from(chunks.map((c) => (Buffer.isBuffer(c) ? c : Buffer.from(c)))));
}

const frame = (obj) => `${JSON.stringify(obj)}\n`;

/** A well-formed artifact stream for `{path: content}`, with a hook to corrupt the header entries. */
function artifactStream(ticketId, files, { mutateEntry } = {}) {
  const entries = Object.entries(files).map(([p, c]) => ({ path: p, size: Buffer.byteLength(c), sha256: sha(Buffer.from(c)) }));
  const headerEntries = entries.map((e) => (mutateEntry ? mutateEntry(e) : e));
  const chunks = [frame({ protocol: 1, ticketId, files: headerEntries })];
  for (const e of headerEntries) chunks.push(frame(e), files[e.path] ?? '');
  chunks.push(frame({ end: true }));
  return stream(chunks);
}

// ---- config shape ----

test('remoteArtifacts shape: accepts canonical paths and simple globs, refuses traversal, absolute paths and bad globs', () => {
  assert.equal(isValidRemoteArtifactsShape(['artifacts/test-lane/default-latest.json', 'out/*.json', 'reports/**/summary.txt']), true);
  for (const bad of [[], '../x', ['../x'], ['a/../x'], ['/etc/passwd'], ['a//b'], ['a/'], ['./a'], ['a/**b'], ['a', 'a'], [''], [1]]) {
    assert.equal(isValidRemoteArtifactsShape(bad), false, JSON.stringify(bad));
  }
});

test('a repo config with a bad remoteArtifacts or remoteArtifactsOn is a ConfigError', () => {
  const { base } = freshEnv();
  for (const lane of [{ remoteArtifacts: ['../x'] }, { remoteArtifacts: 'x' }, { remoteArtifactsOn: 'sometimes' }]) {
    const repoDir = path.join(base, `repo-${Math.random()}`);
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane } } });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), ConfigError, JSON.stringify(lane));
  }
});

test('remoteArtifacts and remoteArtifactsOn resolve with the policy defaulting to success, and an undeclared lane inherits both from its template', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    undeclaredLanes: { as: 'prepush' },
    lanes: { default: { weight: 1 }, prepush: { weight: 1, remote: true, remoteArtifacts: ['a/b.json'], remoteArtifactsOn: 'always' }, plain: { weight: 1, remoteArtifacts: ['x'] } },
  });
  const declared = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'plain' });
  assert.deepEqual(declared.remoteArtifacts, ['x']);
  assert.equal(declared.remoteArtifactsOn, 'success');
  const adHoc = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'zirk1' });
  assert.deepEqual(adHoc.remoteArtifacts, ['a/b.json']);
  assert.equal(adHoc.remoteArtifactsOn, 'always');
  assert.equal(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).remoteArtifacts, null);
});

test('glob matching: * stays inside a segment, ** spans segments (including none)', () => {
  assert.equal(artifactMatches('out/*.json', 'out/a.json'), true);
  assert.equal(artifactMatches('out/*.json', 'out/sub/a.json'), false);
  assert.equal(artifactMatches('out/**/a.json', 'out/a.json'), true);
  assert.equal(artifactMatches('out/**/a.json', 'out/x/y/a.json'), true);
  assert.equal(artifactMatches('a.b', 'aXb'), false, 'a dot is a literal');
});

// ---- runner side: collect ----

test('collect: an exact declared file is stored with its sha256, and a declared file that was never produced is reported missing', () => {
  const work = workDirWith({ 'artifacts/test-lane/default-latest.json': '{"ok":true}' });
  const { dest, res } = collect(work, ['artifacts/test-lane/default-latest.json', 'artifacts/never.json']);
  assert.equal(res.ok, true);
  assert.equal(res.count, 1);
  assert.deepEqual(res.missing, ['artifacts/never.json']);
  const manifest = JSON.parse(fs.readFileSync(path.join(dest, 'manifest.json'), 'utf8'));
  assert.equal(manifest.files[0].sha256, sha(Buffer.from('{"ok":true}')));
  assert.equal(fs.readFileSync(path.join(dest, 'files/artifacts/test-lane/default-latest.json'), 'utf8'), '{"ok":true}');
});

test('collect: a glob matches files across one segment only and ** across many', () => {
  const work = workDirWith({ 'out/a.json': '1', 'out/b.json': '2', 'out/c.txt': '3', 'out/deep/d.json': '4' });
  assert.deepEqual(collect(work, ['out/*.json']).res.count, 2);
  assert.deepEqual(collect(work, ['out/**/*.json']).res.count, 3);
});

test('collect: a symlink is refused, even one pointing at a file inside the work dir', () => {
  const work = workDirWith({ 'real.txt': 'x' });
  fs.symlinkSync('real.txt', path.join(work, 'link.txt'));
  const { res, dest } = collect(work, ['link.txt']);
  assert.equal(res.ok, false);
  assert.match(res.reason, /symlink/);
  assert.equal(fs.existsSync(dest), false, 'a refusal leaves nothing stored');
});

test('collect: a symlink matched by a glob refuses the whole set', () => {
  const work = workDirWith({ 'out/a.json': '1', 'real.txt': 'x' });
  fs.symlinkSync('../real.txt', path.join(work, 'out/b.json'));
  const { res } = collect(work, ['out/*.json']);
  assert.equal(res.ok, false);
  assert.match(res.reason, /symlink/);
});

test('collect: a file reached through a symlinked directory that resolves outside the work dir is refused (realpath containment)', () => {
  const outside = workDirWith({ 'secret.txt': 'top secret' });
  const work = workDirWith({});
  fs.symlinkSync(outside, path.join(work, 'linkdir'));
  const { res } = collect(work, ['linkdir/secret.txt']);
  assert.equal(res.ok, false);
  assert.match(res.reason, /outside/);
});

test('collect: a traversal pattern that slipped past validation is refused at collection, not read', () => {
  const parent = workDirWith({ 'outside.txt': 'nope', 'work/in.txt': 'yes' });
  const { res } = collect(path.join(parent, 'work'), ['../outside.txt']);
  assert.equal(res.ok, false);
  assert.match(res.reason, /outside/);
});

test('collect: a non-regular file (fifo) is refused', () => {
  const work = workDirWith({});
  execFileSync('mkfifo', [path.join(work, 'pipe')]);
  const { res } = collect(work, ['pipe']);
  assert.equal(res.ok, false);
  assert.match(res.reason, /not a regular file/);
});

test('collect: the per-file cap, the total cap and the count cap each refuse', () => {
  const work = workDirWith({ 'a.bin': 'x'.repeat(100), 'b.bin': 'y'.repeat(100), 'c.bin': 'z'.repeat(100) });
  const perFile = collect(work, ['a.bin'], { ...LIMITS, maxFileBytes: 99 }).res;
  assert.equal(perFile.ok, false);
  assert.match(perFile.reason, /per-file cap/);
  const total = collect(work, ['*.bin'], { ...LIMITS, maxTotalBytes: 250 }).res;
  assert.equal(total.ok, false);
  assert.match(total.reason, /total cap/);
  const count = collect(work, ['*.bin'], { ...LIMITS, maxCount: 2 }).res;
  assert.equal(count.ok, false);
  assert.match(count.reason, /count cap/);
  assert.equal(collect(work, ['*.bin']).res.ok, true, 'the same files pass under roomy caps');
});

// ---- the stream: runner encode -> submitter receive ----

test('stream: what collect stores, encode frames and receive verifies round-trips byte for byte', async () => {
  const id = crypto.randomUUID();
  const work = workDirWith({ 'out/a.json': 'alpha', 'out/b.json': 'beta' });
  const { dest } = collect(work, ['out/*.json']);
  const reader = makeReader(encodeArtifacts(dest, id));
  const got = await receiveArtifacts(reader, { ticketId: id, patterns: ['out/*.json'], limits: LIMITS });
  assert.equal(got.ok, true, got.reason);
  assert.deepEqual(got.files.map((f) => [f.path, f.bytes.toString()]), [['out/a.json', 'alpha'], ['out/b.json', 'beta']]);
});

test('receive: a sha256 mismatch is refused', async () => {
  const id = crypto.randomUUID();
  const chunks = [
    frame({ protocol: 1, ticketId: id, files: [{ path: 'a.json', size: 5, sha256: sha(Buffer.from('alpha')) }] }),
    frame({ path: 'a.json', size: 5, sha256: sha(Buffer.from('alpha')) }),
    'ALPHA', // same length, different bytes
    frame({ end: true }),
  ];
  const got = await receiveArtifacts(stream(chunks), { ticketId: id, patterns: ['a.json'], limits: LIMITS });
  assert.equal(got.ok, false);
  assert.match(got.reason, /sha256 mismatch/);
});

test('receive: a traversal, absolute, or undeclared path from the runner is refused', async () => {
  const id = crypto.randomUUID();
  for (const [bad, pattern] of [['../evil.txt', '*'], ['/etc/passwd', '*'], ['a/../../evil', '*'], ['other.txt', 'a.json']]) {
    const got = await receiveArtifacts(artifactStream(id, { [bad]: 'x' }), { ticketId: id, patterns: [pattern], limits: LIMITS });
    assert.equal(got.ok, false, bad);
    assert.match(got.reason, /invalid artifact path|was not declared/, bad);
  }
});

test('receive: the submitter enforces its own per-file, total and count caps, and a stream for another ticket is refused', async () => {
  const id = crypto.randomUUID();
  const files = { 'a.bin': 'x'.repeat(10), 'b.bin': 'y'.repeat(10) };
  const run = (limits, ticketId = id) => receiveArtifacts(artifactStream(id, files), { ticketId, patterns: ['*.bin'], limits });
  assert.match((await run({ ...LIMITS, maxFileBytes: 9 })).reason, /per-file cap/);
  assert.match((await run({ ...LIMITS, maxTotalBytes: 15 })).reason, /total cap/);
  assert.match((await run({ ...LIMITS, maxCount: 1 })).reason, /count cap/);
  assert.match((await run(LIMITS, crypto.randomUUID())).reason, /does not bind/);
});

test('receive: a truncated stream is refused', async () => {
  const id = crypto.randomUUID();
  const entry = { path: 'a.json', size: 5, sha256: sha(Buffer.from('alpha')) };
  const got = await receiveArtifacts(stream([frame({ protocol: 1, ticketId: id, files: [entry] }), frame(entry), 'al']), { ticketId: id, patterns: ['a.json'], limits: LIMITS });
  assert.equal(got.ok, false);
  assert.match(got.reason, /truncated/);
});

// ---- submitter side: install ----

const worktreeWithTracked = makeGitWorktree;

const file = (p, c) => ({ path: p, size: c.length, sha256: sha(Buffer.from(c)), bytes: Buffer.from(c) });

test('install: writes each file at its relative path, creating parent directories', () => {
  const wt = worktreeWithTracked({ 'src.txt': 'x' });
  const out = installArtifacts(wt, [file('artifacts/test-lane/default-latest.json', '{"ok":1}')], ['artifacts/test-lane/default-latest.json']);
  assert.deepEqual(out, { written: ['artifacts/test-lane/default-latest.json'], refused: [] });
  assert.equal(fs.readFileSync(path.join(wt, 'artifacts/test-lane/default-latest.json'), 'utf8'), '{"ok":1}');
  assert.deepEqual(fs.readdirSync(path.join(wt, 'artifacts/test-lane')), ['default-latest.json'], 'no temp file left behind');
});

test('install: a tracked file is overwritten only when listed by exact path, never through a glob', () => {
  const wt = worktreeWithTracked({ 'out/tracked.json': 'old', 'out/other.json': 'old' });
  const viaGlob = installArtifacts(wt, [file('out/tracked.json', 'new')], ['out/*.json']);
  assert.equal(viaGlob.written.length, 0);
  assert.match(viaGlob.refused[0].reason, /git tracks/);
  assert.equal(fs.readFileSync(path.join(wt, 'out/tracked.json'), 'utf8'), 'old');

  const viaExact = installArtifacts(wt, [file('out/tracked.json', 'new')], ['out/tracked.json']);
  assert.deepEqual(viaExact.written, ['out/tracked.json']);
  assert.equal(fs.readFileSync(path.join(wt, 'out/tracked.json'), 'utf8'), 'new');
});

test('install: an untracked file is overwritten through a glob', () => {
  const wt = worktreeWithTracked({ 'src.txt': 'x' });
  fs.mkdirSync(path.join(wt, 'out'));
  fs.writeFileSync(path.join(wt, 'out/a.json'), 'old');
  assert.deepEqual(installArtifacts(wt, [file('out/a.json', 'new')], ['out/*.json']).written, ['out/a.json']);
  assert.equal(fs.readFileSync(path.join(wt, 'out/a.json'), 'utf8'), 'new');
});

test('install: traversal and absolute paths are refused and nothing is written outside the worktree', () => {
  const parent = tmp('artifacts-parent');
  const wt = path.join(parent, 'wt');
  fs.mkdirSync(wt);
  execFileSync('git', ['init', '-q'], { cwd: wt });
  const out = installArtifacts(wt, [file('../escaped.txt', 'x'), file('/tmp/lane-artifact-abs.txt', 'x')], ['*']);
  assert.equal(out.written.length, 0);
  assert.deepEqual(out.refused.map((r) => r.reason), ['invalid path', 'invalid path']);
  assert.equal(fs.existsSync(path.join(parent, 'escaped.txt')), false);
});

test('install: a destination that is a symlink, or sits under a symlinked directory leading outside, is refused', () => {
  const outside = tmp('artifacts-outside');
  const wt = worktreeWithTracked({ 'src.txt': 'x' });
  fs.writeFileSync(path.join(outside, 'target.txt'), 'untouched');
  fs.symlinkSync(path.join(outside, 'target.txt'), path.join(wt, 'leaf.txt'));
  fs.symlinkSync(outside, path.join(wt, 'linkdir'));
  const out = installArtifacts(wt, [file('leaf.txt', 'pwn'), file('linkdir/new.txt', 'pwn')], ['leaf.txt', 'linkdir/new.txt']);
  assert.equal(out.written.length, 0);
  assert.match(out.refused[0].reason, /symlink/);
  assert.match(out.refused[1].reason, /outside the worktree/);
  assert.equal(fs.readFileSync(path.join(outside, 'target.txt'), 'utf8'), 'untouched');
  assert.equal(fs.existsSync(path.join(outside, 'new.txt')), false);
});

test('install: one refused file does not stop the others', () => {
  const wt = worktreeWithTracked({ 'out/tracked.json': 'old' });
  const out = installArtifacts(wt, [file('out/new.json', 'n'), file('out/tracked.json', 'x')], ['out/*.json']);
  assert.deepEqual(out.written, ['out/new.json']);
  assert.equal(out.refused.length, 1);
});

// ---- runner housekeeping ----

test('prune: stored artifacts older than a day are removed, recent ones kept', () => {
  const tickets = tmp('artifacts-tickets');
  for (const name of ['old', 'recent']) fs.mkdirSync(path.join(tickets, name, 'artifacts'), { recursive: true });
  const dayAndABit = Date.now() - 25 * 60 * 60_000;
  fs.utimesSync(path.join(tickets, 'old', 'artifacts'), dayAndABit / 1000, dayAndABit / 1000);
  pruneStaleArtifacts(tickets);
  assert.equal(fs.existsSync(path.join(tickets, 'old', 'artifacts')), false);
  assert.equal(fs.existsSync(path.join(tickets, 'recent', 'artifacts')), true);
});
