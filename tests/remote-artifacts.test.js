import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
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
import { makeTmpDir } from './helpers/tmp.js';

const LIMITS = { maxFileBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };
const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
const tmp = (prefix) => makeTmpDir(`${prefix}-`);

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
  for (const bad of [[], '../x', ['../x'], ['a/../x'], ['/etc/passwd'], ['a//b'], ['a/'], ['./a'], ['a/**b'], ['a/b', 'a/b'], [''], [1], ['*.json'], ['**/*.json'], ['stamp.json'], ['*/x.json']]) {
    assert.equal(isValidRemoteArtifactsShape(bad), false, JSON.stringify(bad));
  }
});

test('a repo config with a bad remoteArtifacts or remoteArtifactsOn is a ConfigError', () => {
  const { base } = freshEnv();
  for (const lane of [{ remoteArtifacts: ['../x'] }, { remoteArtifacts: 'x' }, { remoteArtifacts: ['**/*.json'] }, { remoteArtifactsOn: 'sometimes' }]) {
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
    lanes: { default: { weight: 1 }, prepush: { weight: 1, remote: true, remoteArtifacts: ['a/b.json'], remoteArtifactsOn: 'always' }, plain: { weight: 1, remoteArtifacts: ['x/y.json'] } },
  });
  const declared = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'plain' });
  assert.deepEqual(declared.remoteArtifacts, ['x/y.json']);
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
    frame({ protocol: 1, ticketId: id, files: [{ path: 'out/a.json', size: 5, sha256: sha(Buffer.from('alpha')) }] }),
    frame({ path: 'out/a.json', size: 5, sha256: sha(Buffer.from('alpha')) }),
    'ALPHA', // same length, different bytes
    frame({ end: true }),
  ];
  const got = await receiveArtifacts(stream(chunks), { ticketId: id, patterns: ['out/a.json'], limits: LIMITS });
  assert.equal(got.ok, false);
  assert.match(got.reason, /sha256 mismatch/);
});

test('receive: a traversal, absolute, undeclared or protected path from the runner is refused', async () => {
  const id = crypto.randomUUID();
  for (const [bad, pattern, reason] of [
    ['../evil.txt', 'out/*', /invalid artifact path/],
    ['/etc/passwd', 'out/*', /invalid artifact path/],
    ['a/../../evil', 'out/*', /invalid artifact path/],
    ['other/x.txt', 'out/*', /was not declared/],
    ['x.txt', 'out/*', /was not declared/],
    ['.git/config', '.git/*', /protected/],
  ]) {
    const got = await receiveArtifacts(artifactStream(id, { [bad]: 'x' }), { ticketId: id, patterns: [pattern], limits: LIMITS });
    assert.equal(got.ok, false, bad);
    assert.match(got.reason, reason, bad);
  }
});

test('receive: the submitter enforces its own per-file, total and count caps, and a stream for another ticket is refused', async () => {
  const id = crypto.randomUUID();
  const files = { 'out/a.bin': 'x'.repeat(10), 'out/b.bin': 'y'.repeat(10) };
  const run = (limits, ticketId = id) => receiveArtifacts(artifactStream(id, files), { ticketId, patterns: ['out/*.bin'], limits });
  assert.match((await run({ ...LIMITS, maxFileBytes: 9 })).reason, /per-file cap/);
  assert.match((await run({ ...LIMITS, maxTotalBytes: 15 })).reason, /total cap/);
  assert.match((await run({ ...LIMITS, maxCount: 1 })).reason, /count cap/);
  assert.match((await run(LIMITS, crypto.randomUUID())).reason, /does not bind/);
});

test('receive: a stream speaking an unknown protocol version is refused', async () => {
  const id = crypto.randomUUID();
  for (const protocol of [999, 2, undefined]) {
    const entry = { path: 'out/a.json', size: 1, sha256: sha(Buffer.from('a')) };
    const got = await receiveArtifacts(stream([frame({ protocol, ticketId: id, files: [entry] }), frame(entry), 'a', frame({ end: true })]), { ticketId: id, patterns: ['out/a.json'], limits: LIMITS });
    assert.equal(got.ok, false, String(protocol));
    assert.match(got.reason, /unsupported artifact stream protocol/);
  }
});

test('receive: a truncated stream is refused', async () => {
  const id = crypto.randomUUID();
  const entry = { path: 'out/a.json', size: 5, sha256: sha(Buffer.from('alpha')) };
  const got = await receiveArtifacts(stream([frame({ protocol: 1, ticketId: id, files: [entry] }), frame(entry), 'al']), { ticketId: id, patterns: ['out/a.json'], limits: LIMITS });
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

test('install: a git-tracked file is never overwritten, whether declared by glob or by its exact path', () => {
  const wt = worktreeWithTracked({ 'out/tracked.json': 'old', 'out/other.json': 'old' });
  for (const patterns of [['out/*.json'], ['out/tracked.json']]) {
    const out = installArtifacts(wt, [file('out/tracked.json', 'new')], patterns);
    assert.equal(out.written.length, 0, JSON.stringify(patterns));
    assert.match(out.refused[0].reason, /git tracks/);
    assert.equal(fs.readFileSync(path.join(wt, 'out/tracked.json'), 'utf8'), 'old');
  }
});

for (const denied of ['.git/config', '.github/workflows/ci.yml', '.githooks/pre-push', 'sub/.git', '.gitmodules', 'package.json', 'package-lock.json', 'npm-shrinkwrap.json', 'pkg/node_modules/x/index.js', 'out/.env', 'out/.env.local', '.npmrc', 'out/.yarnrc.yml', 'out/yarn.lock', 'out/Cargo.lock', '.lane-broker.json', 'out/PACKAGE.JSON']) {
  test(`install: a protected path (${denied}) is refused even when a declared pattern names it`, () => {
    const wt = worktreeWithTracked({ 'src.txt': 'x' });
    const dir = path.dirname(denied) === '.' ? 'out' : path.dirname(denied);
    const out = installArtifacts(wt, [file(denied, 'pwn')], [denied, `${dir}/*`]);
    assert.equal(out.written.length, 0);
    assert.match(out.refused[0].reason, /protected/);
    if (denied === '.git/config') assert.doesNotMatch(fs.readFileSync(path.join(wt, '.git/config'), 'utf8'), /pwn/);
    assert.equal(fs.existsSync(path.join(wt, denied)), denied === '.git/config');
  });
}

test('install: a path outside the literal directory of every declared pattern is refused', () => {
  const wt = worktreeWithTracked({ 'src.txt': 'x' });
  const out = installArtifacts(wt, [file('elsewhere/a.json', 'x'), file('out/a.json', 'x')], ['out/*.json']);
  assert.deepEqual(out.written, ['out/a.json']);
  assert.match(out.refused[0].reason, /literal directory/);
});

test('install: an alias directory (symlink inside the worktree) cannot be used to overwrite a tracked file', () => {
  const wt = worktreeWithTracked({ 'real/tracked.json': 'old' });
  fs.symlinkSync('real', path.join(wt, 'alias'));
  const out = installArtifacts(wt, [file('alias/tracked.json', 'pwn')], ['alias/*.json']);
  assert.equal(out.written.length, 0);
  assert.match(out.refused[0].reason, /symlink/);
  assert.equal(fs.readFileSync(path.join(wt, 'real/tracked.json'), 'utf8'), 'old');
});

test('install: if git cannot say what is tracked, nothing is written', () => {
  const notRepo = tmp('artifacts-not-a-repo');
  const out = installArtifacts(notRepo, [file('out/a.json', 'x')], ['out/*.json']);
  assert.equal(out.written.length, 0);
  assert.match(out.refused[0].reason, /whether git tracks/);
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
  const out = installArtifacts(wt, [file('../escaped.txt', 'x'), file('/tmp/lane-artifact-abs.txt', 'x')], ['out/*']);
  assert.equal(out.written.length, 0);
  assert.deepEqual(out.refused.map((r) => r.reason), ['invalid path', 'invalid path']);
  assert.equal(fs.existsSync(path.join(parent, 'escaped.txt')), false);
});

test('install: a destination that is a symlink, or sits under a symlinked directory leading outside, is refused', () => {
  const outside = tmp('artifacts-outside');
  const wt = worktreeWithTracked({ 'src.txt': 'x' });
  fs.writeFileSync(path.join(outside, 'target.txt'), 'untouched');
  fs.mkdirSync(path.join(wt, 'out'));
  fs.symlinkSync(path.join(outside, 'target.txt'), path.join(wt, 'out/leaf.txt'));
  fs.symlinkSync(outside, path.join(wt, 'linkdir'));
  const out = installArtifacts(wt, [file('out/leaf.txt', 'pwn'), file('linkdir/new.txt', 'pwn')], ['out/*.txt', 'linkdir/*.txt']);
  assert.equal(out.written.length, 0);
  assert.match(out.refused[0].reason, /symlink/);
  assert.match(out.refused[1].reason, /symlink/);
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

// ---- more runner-side collection ----

test('collect: a protected path is refused (a glob cannot sweep up .git or package.json)', () => {
  const work = workDirWith({ 'out/ok.json': '1', 'out/package.json': '{}' });
  const { res } = collect(work, ['out/*.json']);
  assert.equal(res.ok, false);
  assert.match(res.reason, /protected path/);
  const dotGit = collect(workDirWith({ '.git/config': 'x' }), ['.git/config']).res;
  assert.equal(dotGit.ok, false);
  assert.match(dotGit.reason, /protected path/);
});

test('collect: zero matching files creates nothing on disk', () => {
  const { dest, res } = collect(workDirWith({ 'out/a.txt': '1' }), ['out/*.json']);
  assert.equal(res.ok, true);
  assert.equal(res.count, 0);
  assert.equal(fs.existsSync(dest), false);
});

test('collect: the walk stops at the count cap instead of listing every match first', () => {
  const work = workDirWith(Object.fromEntries(Array.from({ length: 30 }, (_, i) => [`out/f${i}.bin`, 'x'])));
  const res = collect(work, ['out/*.bin'], { ...LIMITS, maxCount: 5 }).res;
  assert.equal(res.ok, false);
  assert.match(res.reason, /count cap/);
});
