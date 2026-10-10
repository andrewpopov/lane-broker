import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { freshEnv, writeGlobalConfig, BIN, laneRun, sleep, waitFor } from './helpers.js';
import { manifestHashOf, verifyManifestNoGit } from '../src/remote-manifest.js';
import { encodeSnapshot, serializeHeader, MAX_HEADER_BYTES } from '../src/remote-stream.js';
import { sanitizeKey } from '../src/config.js';
import { paths, readJsonSafe, processStartTime } from '../src/state.js';
import { writeLease } from '../src/lease.js';
import { enqueue } from '../src/scheduler.js';
import { remoteResultCommand } from '../src/remote-runner.js';
import { makeTmpDir } from './helpers/tmp.js';

function tmpDir(prefix) {
  return makeTmpDir(`${prefix}-`);
}

/** Build a fresh source dir on disk matching `files` (relPath -> utf8 content), and the
 *  manifest entries `encodeSnapshot` needs for it. No git involved -- remote-runner.js's own
 *  tests exercise the wire format and the run, not manifest-building (see remote-manifest.test.js
 *  for that). */
function makeSnapshotSource(files) {
  const dir = tmpDir('remote-runner-src');
  const entries = [];
  for (const [rel, content] of Object.entries(files)) {
    const abs = path.join(dir, rel);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    const buf = Buffer.from(content, 'utf8');
    fs.writeFileSync(abs, buf);
    entries.push({
      path: rel,
      type: 'file',
      exec: false,
      size: buf.length,
      sha256: crypto.createHash('sha256').update(buf).digest('hex'),
    });
  }
  return { dir, entries };
}

function makeHeader(overrides = {}) {
  return {
    ticketId: crypto.randomUUID(),
    generation: 0,
    repoKey: 'remote-runner-test-repo',
    lane: 'default',
    argv: [process.execPath, '-e', 'process.exit(0)'],
    relCwd: '',
    ...overrides,
  };
}

/**
 * `freshEnv()` (helpers.js) writes no config.json, so `loadGlobalConfig`
 * falls back to `DEFAULT_GLOBAL_CONFIG`'s `schedulerMode: 'active'` --
 * meaning admission for every remote-exec'd ticket here is gated by the
 * REAL host's own load average, not by anything these tests control. On a
 * quiet machine that's invisible; on a machine busy running unrelated
 * parallel test suites, the load gate can legitimately stay closed for a
 * long time, and a test whose only failure mode should be "the fix is
 * wrong" instead reads as "the suite hung". Force 'shadow' (informational
 * only, never denies a start) the same way every other integration test in
 * this repo already does via `writeGlobalConfig` -- these tests exercise
 * remote-exec's own logic, not the host admission gate.
 */
function freshShadowEnv(extra) {
  const f = freshEnv(extra);
  writeGlobalConfig(f.home, {});
  return f;
}

function line(obj) {
  return `${JSON.stringify(obj)}\n`;
}

function streamOf(chunks) {
  async function* gen() {
    for (const c of chunks) yield Buffer.isBuffer(c) ? c : Buffer.from(c, 'utf8');
  }
  return Readable.from(gen());
}

/** Spawn the real `bin/lane.js remote-exec --root <root>`, feeding `stream` as stdin.
 *  `cwd`, when given, lets a test exercise a RELATIVE `root` from a known directory
 *  (BRAIN-320 review fix A). */
function spawnRemoteExec(stream, { env, root, cwd }) {
  const child = spawn(process.execPath, [BIN, 'remote-exec', '--root', root], cwd ? { env, cwd } : { env });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (d) => {
    stdout += d;
  });
  child.stderr.on('data', (d) => {
    stderr += d;
  });
  stream.pipe(child.stdin);
  // Subscribe to 'close' at spawn time: a test that awaits something else
  // first (e.g. remote-cancel) can otherwise attach its listener after the
  // event already fired and wait forever.
  closed.set(child, new Promise((resolve) => child.on('close', (code) => resolve(code))));
  return { child, out: () => stdout, err: () => stderr };
}

const closed = new WeakMap();
const CLOSE_TIMEOUT_MS = 60_000;

function waitClose(child) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`remote-exec did not close within ${CLOSE_TIMEOUT_MS}ms`)), CLOSE_TIMEOUT_MS);
  });
  return Promise.race([closed.get(child), timeout]).finally(() => clearTimeout(timer));
}

async function getResult(ticketId, root, env) {
  const { code, stdout } = await laneRun(['remote-result', ticketId, '--root', root], { env });
  assert.equal(code, 0);
  return JSON.parse(stdout.trim());
}

async function runOne(header, entries, { env, root, src, cwd }) {
  const stream = encodeSnapshot(src, header, entries);
  const { child, err } = spawnRemoteExec(stream, { env, root, cwd });
  const code = await waitClose(child);
  return { code, err: err() };
}

// ---- happy path: exit codes pass straight through as "completed" ----

test('a child exiting 0 is reported completed exit 0', async () => {
  const { env, state } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader();

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);
  assert.equal(result.signal, null);
  void state;
});

for (const exitCode of [1, 42, 75, 255]) {
  test(`a child exiting ${exitCode} is reported completed exit ${exitCode}`, async () => {
    const { env } = freshShadowEnv();
    const root = tmpDir('remote-exec-root');
    const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
    const header = makeHeader({ argv: [process.execPath, '-e', `process.exit(${exitCode})`] });

    const { code } = await runOne(header, entries, { env, root, src });
    assert.equal(code, 0);

    const result = await getResult(header.ticketId, root, env);
    assert.equal(result.kind, 'completed');
    assert.equal(result.exit, exitCode);
  });
}

// ---- BRAIN-320 follow-up: the snapshot header is raised to MAX_HEADER_BYTES ----

test('a manifest large enough that its header exceeds the old 1 MB cap (but stays under MAX_HEADER_BYTES) still completes', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const files = {};
  for (let i = 0; i < 9000; i += 1) {
    files[`f${String(i).padStart(6, '0')}.txt`] = 'x';
  }
  const { dir: src, entries } = makeSnapshotSource(files);
  const header = makeHeader();

  const headerBytes = Buffer.byteLength(serializeHeader(header, entries), 'utf8');
  assert.ok(headerBytes > 1_000_000, `expected a header over 1 MB, got ${headerBytes}`);
  assert.ok(headerBytes < MAX_HEADER_BYTES, `expected a header under MAX_HEADER_BYTES, got ${headerBytes}`);

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);
});

// ---- adversarial / malformed input ----

test('a corrupted file body fails the post-extraction hash re-verify and is rejected', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const ticketId = crypto.randomUUID();
  const entries = [{ path: 'a.txt', type: 'file', exec: false, size: 4, sha256: crypto.createHash('sha256').update('aaaa').digest('hex') }];
  const header = {
    protocol: 1,
    ticketId,
    generation: 0,
    repoKey: 'r',
    lane: 'default',
    argv: [process.execPath, '-e', 'process.exit(0)'],
    relCwd: '',
    manifest: { entries, manifestHash: manifestHashOf(entries) },
  };
  const body = Buffer.from('aaab'); // same length, different bytes
  const stream = streamOf([
    line(header),
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
  ]);
  const { child } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0); // a result WAS written

  const result = await getResult(ticketId, root, env);
  assert.equal(result.kind, 'rejected');
  assert.match(result.reason, /hash mismatch/);
});

test('an unsupported protocol version is rejected before any child runs', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const markerFile = path.join(tmpDir('remote-exec-marker'), 'marker');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({
    protocol: 3,
    argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(markerFile)}, '1')`],
  });

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'rejected');
  assert.match(result.reason, /protocol/);
  assert.equal(fs.existsSync(markerFile), false, 'the child must never have run');
});

// BRAIN-320 S1b: a protocol-1 header must never carry remoteDeps/remoteSetup.
test('a protocol-1 header carrying remoteDeps is rejected before any child runs', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const markerFile = path.join(tmpDir('remote-exec-marker'), 'marker');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello', 'package-lock.json': '{}' });
  const header = makeHeader({
    protocol: 1,
    remoteDeps: ['.'],
    argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(markerFile)}, '1')`],
  });

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'rejected');
  assert.match(result.reason, /protocol 1/);
  assert.equal(fs.existsSync(markerFile), false, 'the child must never have run');
});

// BRAIN-320 S1b: a plain protocol-1 header (no remoteDeps/remoteSetup) keeps
// today's exact result shape -- no `phase` field at all (I6).
test('a plain protocol-1 header result has no phase field', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ protocol: 1 });

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.protocol, 1);
  assert.equal(result.kind, 'completed');
  assert.equal('phase' in result, false, 'protocol-1 results must never carry a phase field');
});

// BRAIN-320 S1b: shape validation for a protocol-2 header's remoteDeps/remoteSetup.
test('a protocol-2 header with an invalid remoteDeps shape is rejected', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ protocol: 2, remoteDeps: [] }); // empty array is invalid shape

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'rejected');
  assert.match(result.reason, /remoteDeps/);
});

test('a protocol-2 header with an invalid remoteSetup shape is rejected', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ protocol: 2, remoteSetup: [[]] }); // empty argv is invalid shape

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'rejected');
  assert.match(result.reason, /remoteSetup/);
});

// BRAIN-320 S1b (1c): proves remoteExecCommand actually WIRES the runner-side
// re-check (validateRemoteDeps against the manifest, re-run after extraction)
// rather than merely defining it -- a manifest whose lockfile dir also has an
// npm-shrinkwrap.json passes header-shape validation (remoteDeps: ['.'] is a
// valid shape) and extraction (both are real files on disk), so only the 1c
// re-check itself can catch this before the pipeline ever runs.
test('a protocol-2 header with remoteDeps whose dir has a shrinkwrap alongside the lockfile is rejected, and the pipeline never runs', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const markerFile = path.join(tmpDir('remote-exec-marker'), 'marker');
  const { dir: src, entries } = makeSnapshotSource({
    'package-lock.json': '{}',
    'npm-shrinkwrap.json': '{}',
  });
  const header = makeHeader({
    protocol: 2,
    remoteDeps: ['.'],
    argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(markerFile)}, '1')`],
  });

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'rejected');
  assert.match(result.reason, /shrinkwrap/);
  assert.equal(result.phase, null, 'no phase was ever recorded -- the pipeline never started');
  assert.equal(fs.existsSync(markerFile), false, 'the command must never have run');
});

// BRAIN-321: an unreadable runner global config must not kill protocol-2 intake mid-flight: a refused result, exit 64, cleaned up.
test('protocol-2 intake with an unreadable global config publishes a refused result (exit 64) and never runs the command', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const markerFile = path.join(tmpDir('remote-exec-marker'), 'marker');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ protocol: 2, argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(markerFile)}, '1')`] });
  fs.writeFileSync(path.join(env.LANE_BROKER_HOME, 'config.json'), JSON.stringify({ version: 1 }));
  fs.chmodSync(env.LANE_BROKER_HOME, 0o000);
  try {
    const { code } = await runOne(header, entries, { env, root, src });
    assert.equal(code, 0);
    fs.chmodSync(env.LANE_BROKER_HOME, 0o755);
    const result = await getResult(header.ticketId, root, env);
    assert.equal(result.kind, 'refused');
    assert.equal(result.exit, 64);
    assert.match(result.reason, /unreadable/);
    assert.equal(fs.existsSync(markerFile), false);
  } finally {
    fs.chmodSync(env.LANE_BROKER_HOME, 0o755);
  }
});

// BRAIN-320 S1b: a plain protocol-2 header (no remoteDeps/remoteSetup) still
// runs the caller's argv, through the pipeline, ending at phase "command".
test('a plain protocol-2 header (no remoteDeps/remoteSetup) runs the command and reports phase command', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ protocol: 2, argv: [process.execPath, '-e', 'process.exit(0)'] });

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.protocol, 2);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);
  assert.equal(result.phase, 'command');
});

// BRAIN-320 review fix A: a relative --root must resolve against remote-exec's
// OWN cwd once, up front -- not get re-derived against the protocol-2
// pipeline child's DIFFERENT cwd (the extracted work dir), or the pipeline
// can't find its own pipeline.json.
test('a relative --root still completes phase command (protocol 2)', async () => {
  const { env } = freshShadowEnv();
  const base = tmpDir('remote-exec-relroot-base');
  const rootAbs = path.join(base, 'remote-root');
  fs.mkdirSync(rootAbs, { recursive: true });
  const relRoot = path.relative(base, rootAbs);
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ protocol: 2, argv: [process.execPath, '-e', 'process.exit(0)'] });

  const { code, err } = await runOne(header, entries, { env, root: relRoot, src, cwd: base });
  assert.equal(code, 0, `stderr: ${err}`);

  const result = await getResult(header.ticketId, rootAbs, env);
  assert.equal(result.protocol, 2);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);
  assert.equal(result.phase, 'command');
});

// BRAIN-320 review fix C: git's own repo-local env vars (GIT_DIR here) must
// never reach a deps/setup/command child -- remote-exec already scrubs
// LANE_BROKER_* off its own process.env at the top; this proves the same
// scrub now covers git's vars too, using the same list `scrubbedGitEnv`
// (remote-manifest.js) derives.
test('GIT_DIR set in the runner\'s own env is unset by the time a remoteSetup step runs', async () => {
  const probeFile = path.join(tmpDir('remote-exec-probe'), 'gitdir');
  const { env } = freshShadowEnv({ GIT_DIR: '/nonexistent/bogus-git-dir', PROBE_FILE: probeFile });
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({
    protocol: 2,
    remoteSetup: [[process.execPath, '-e', "require('fs').writeFileSync(process.env.PROBE_FILE, process.env.GIT_DIR || '')"]],
    argv: [process.execPath, '-e', 'process.exit(0)'],
  });

  const { code, err } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0, `stderr: ${err}`);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed', `reason: ${result.reason}`);
  assert.equal(fs.readFileSync(probeFile, 'utf8'), '', 'GIT_DIR must have been unset before the setup child ran');
});

test('an invalid ticketId (path traversal) is refused with nothing written on disk outside root', async () => {
  const { env } = freshShadowEnv();
  // `root` gets its own private parent directory (rather than the shared
  // system temp dir tmpDir() would otherwise put it directly under) so that
  // `path.dirname(root)` -- where a `../escaped` traversal would land -- is
  // private to this test: an unrelated stray file left there by a parallel
  // run/session can never make the assertion below fail.
  const privateParent = tmpDir('remote-exec-root-parent');
  const root = path.join(privateParent, 'root');
  fs.mkdirSync(root);
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ ticketId: '../escaped' });

  const { code, err } = await runOne(header, entries, { env, root, src });
  assert.notEqual(code, 0);
  assert.match(err, /invalid ticketId/);
  assert.equal(fs.existsSync(path.join(root, 'escaped')), false);
  assert.equal(fs.existsSync(path.join(privateParent, 'escaped')), false);
});

test('a pre-existing ticket directory is refused untouched, with no result written', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader();
  const ticketDir = path.join(root, 'tickets', header.ticketId);
  fs.mkdirSync(ticketDir, { recursive: true });
  fs.writeFileSync(path.join(ticketDir, 'sentinel'), 'do-not-touch');

  const { code, err } = await runOne(header, entries, { env, root, src });
  assert.notEqual(code, 0);
  assert.match(err, /already exists/);
  assert.equal(fs.readFileSync(path.join(ticketDir, 'sentinel'), 'utf8'), 'do-not-touch');
  assert.equal(fs.existsSync(path.join(ticketDir, 'result.json')), false);
});

test('an undeclared lane in the snapshot\'s own .lane-broker.json is refused', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({
    '.lane-broker.json': JSON.stringify({ version: 1, lanes: { default: { weight: 1 } } }),
  });
  const header = makeHeader({ lane: 'no-such-lane' });

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'refused');
  // BRAIN-319 T3a: the recorded exit is the actual preflight refusal code
  // (64 = ConfigError, matching src/run.js's "unknown lane" refusal) --
  // a remote-client caller trusts this as the real exit status (see
  // src/remote-client.js's isGreen/dispatchRemote), so it must never be null.
  assert.equal(result.exit, 64);
});

// ---- fresh admission: I4 ----

test('a real matching lease forwarded via LANE_BROKER_LEASE/KEY is NOT reused (I4): a fresh ticket is admitted with LANE_BROKER_LOCAL=1', async () => {
  const { env, state } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const probeFile = path.join(tmpDir('remote-exec-probe'), 'probe.json');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({
    argv: [
      process.execPath,
      '-e',
      `require('fs').writeFileSync(process.env.PROBE_FILE, JSON.stringify({lease:process.env.LANE_BROKER_LEASE||null,key:process.env.LANE_BROKER_KEY||null,local:process.env.LANE_BROKER_LOCAL||null}))`,
    ],
  });

  // Plant a REAL lease matching resolved.key -- run.js's reentrancy check
  // (see its own comment: a forged env var with no matching lease record
  // already falls through harmlessly) only takes the reentrant path when
  // there IS a matching lease on disk. Generous weight/resources so the
  // "must not widen the inherited lease" refusal never fires either --
  // without the env scrub, this WOULD be taken as a legitimate ancestor
  // lease and the command would run directly, inheriting these env vars
  // unchanged and never touching the local broker's admission at all.
  const forgedLeaseId = crypto.randomUUID();
  const resolvedKey = `${sanitizeKey(header.repoKey)}:${sanitizeKey(header.lane)}`;
  writeLease(state, {
    id: forgedLeaseId,
    key: resolvedKey,
    weight: 1_000_000,
    resources: { cpuCores: 1_000_000, memoryBytes: 1_000_000_000_000 },
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: process.pid,
    startedAt: Date.now(),
    heartbeatAt: Date.now(),
  });

  const spawnEnv = { ...env, LANE_BROKER_LEASE: forgedLeaseId, LANE_BROKER_KEY: resolvedKey, PROBE_FILE: probeFile };
  const { code } = await runOne(header, entries, { env: spawnEnv, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);

  const probe = JSON.parse(fs.readFileSync(probeFile, 'utf8'));
  assert.notEqual(probe.lease, forgedLeaseId, 'child must not inherit the forged ancestor lease id');
  assert.equal(probe.lease, result.remoteLaneId, 'child sees the NEW broker ticket id instead');
  assert.ok(probe.key.startsWith(`${sanitizeKey(header.repoKey)}:`), `key ${probe.key} should be derived from repoKey`);
  assert.equal(probe.local, '1');
});

// BRAIN-380: a remote-exec ticket's tier comes only from the dispatch header (none yet, so medium). The runner shell's own
// LANE_BROKER_PRIORITY -- valid or not -- must neither set the tier nor fail the run.
for (const shellValue of ['high', 'low', 'not-a-tier']) {
  test(`a remote-exec ticket ignores the runner shell's LANE_BROKER_PRIORITY=${shellValue}: medium, and its child sees medium`, async () => {
    const { env } = freshShadowEnv();
    const root = tmpDir('remote-exec-root');
    const probeFile = path.join(tmpDir('remote-exec-probe'), 'probe.json');
    const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
    const header = makeHeader({
      argv: [process.execPath, '-e', `require('fs').writeFileSync(process.env.PROBE_FILE, JSON.stringify({prio:process.env.LANE_BROKER_PRIORITY||null}))`],
    });
    const { code, err } = await runOne(header, entries, { env: { ...env, LANE_BROKER_PRIORITY: shellValue, PROBE_FILE: probeFile }, root, src });
    assert.equal(code, 0, err);
    assert.equal(JSON.parse(fs.readFileSync(probeFile, 'utf8')).prio, 'medium');
  });
}

// BRAIN-319 T3: reworked to not depend on `lane cancel`'s SIGTERM grace
// window (that wait is real, host-load-dependent time -- it was the source
// of an observed flake under heavy ambient machine load). A bounded sleep
// that exits on its own removes cancellation from this test entirely; the
// separate cancel tests below still cover cancellation itself.
test('the lease key is derived from repoKey, not the ticket directory path, even with the synthetic git repo present', async () => {
  const { env, state } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: [process.execPath, '-e', 'setTimeout(() => process.exit(0), 3000)'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });

  const foundKey = await waitFor(() => {
    const leasesDir = paths(state).leases;
    let names;
    try {
      names = fs.readdirSync(leasesDir);
    } catch {
      return null;
    }
    for (const n of names) {
      const lease = readJsonSafe(path.join(leasesDir, n));
      if (lease && lease.key && lease.key.startsWith(`${sanitizeKey(header.repoKey)}:`)) return lease.key;
    }
    return null;
  });
  assert.ok(foundKey.startsWith(`${sanitizeKey(header.repoKey)}:`));

  const code = await waitClose(child);
  assert.equal(code, 0);
  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);
});

// ---- synthetic git repo (BRAIN-319 T3) ----

test('a child inside the synthetic git repo sees the work dir as its toplevel', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const realRoot = fs.realpathSync(root);
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: ['git', 'rev-parse', '--show-toplevel'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child, out } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const expectedWorkDir = path.join(realRoot, 'tickets', header.ticketId, 'work');
  assert.equal(out().trim(), expectedWorkDir);
});

test('git status --porcelain inside the synthetic repo is clean right after the snapshot commit', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello', 'b.txt': 'world' });
  const header = makeHeader({ argv: ['git', 'status', '--porcelain'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child, out } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0);
  assert.equal(out().trim(), '');
});

test('a .gitignore in the snapshot still applies inside the synthetic repo to a file the run creates (git check-ignore)', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({
    '.gitignore': 'build-out/\n',
    'a.txt': 'hello',
  });
  const header = makeHeader({ argv: ['sh', '-c', 'mkdir -p build-out && echo x > build-out/o.txt && git check-ignore build-out/o.txt'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0);
  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0, 'git check-ignore exits 0 exactly when the path is ignored');
});

test('a snapshot file that matches .gitignore (tracked in the source repo anyway) is tracked in the synthetic repo', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({
    '.gitignore': 'art/generated/*\n',
    'art/generated/a.png': 'png',
    'b.txt': 'world',
  });
  const header = makeHeader({ argv: ['git', 'ls-files', '--error-unmatch', 'art/generated/a.png'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0);
  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0, 'git ls-files lists the force-tracked file, as the source repo does');
});

// Least-invasive coverage for "a tree mutation between the synthetic commit
// and the post-git re-verify is caught": rather than trying to race
// remote-exec's own subprocess (inherently timing-dependent and would only
// prove "usually caught"), this exercises verifyManifestNoGit directly --
// the exact function remote-runner.js calls with { ignoreRootGit: true }
// after git-init -- against a tree that has both a `.git` dir (as if
// git-init already ran) AND a mutated tracked file, confirming ignoreRootGit
// ignores only `.git` and still catches the real mismatch.
test('verifyManifestNoGit with ignoreRootGit still catches a tree mutation, ignoring only .git', () => {
  const dir = tmpDir('remote-runner-postgit-verify');
  fs.writeFileSync(path.join(dir, 'a.txt'), 'hello');
  const entries = [
    {
      path: 'a.txt',
      type: 'file',
      exec: false,
      size: 5,
      sha256: crypto.createHash('sha256').update('hello').digest('hex'),
    },
  ];
  const manifest = { entries, manifestHash: manifestHashOf(entries) };

  fs.mkdirSync(path.join(dir, '.git'));
  fs.writeFileSync(path.join(dir, '.git', 'HEAD'), 'ref: refs/heads/main\n');
  assert.deepEqual(verifyManifestNoGit(dir, manifest, { ignoreRootGit: true }), { ok: true });

  fs.writeFileSync(path.join(dir, 'a.txt'), 'HELLO'); // same length, mutated content
  const result = verifyManifestNoGit(dir, manifest, { ignoreRootGit: true });
  assert.equal(result.ok, false);
  assert.match(result.reason, /^hash mismatch: a\.txt$/);
});

// ---- relCwd ----

test('relCwd is honoured: the child runs with cwd = work dir + relCwd', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const probeFile = path.join(tmpDir('remote-exec-probe'), 'cwd.txt');
  const { dir: src, entries } = makeSnapshotSource({ 'sub/.keep': 'x' });
  const header = makeHeader({
    relCwd: 'sub',
    argv: [process.execPath, '-e', "require('fs').writeFileSync(process.env.PROBE_FILE, process.cwd())"],
  });

  const spawnEnv = { ...env, PROBE_FILE: probeFile };
  const { code } = await runOne(header, entries, { env: spawnEnv, root, src });
  assert.equal(code, 0);

  // macOS: os.tmpdir() is under a /var symlink to /private/var; process.cwd() in the child
  // reports the realpath, so realpath our own expectation the same way before comparing.
  const expectedCwd = path.join(fs.realpathSync(root), 'tickets', header.ticketId, 'work', 'sub');
  assert.equal(path.resolve(fs.readFileSync(probeFile, 'utf8')), path.resolve(expectedCwd));
});

// ---- cancellation (C5) ----

test('remote-cancel fired before the child is confirmed running still results in "cancelled", and its marker never appears', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const markerFile = path.join(tmpDir('remote-exec-marker'), 'marker');
  // A few files (not just one) widen the window between "ticket directory
  // exists" and "extraction has finished / remote-id is written", so the
  // poll below reliably lands before remote-id -- exercising remote-exec's
  // own post-remote-id cancelled-marker check rather than the (unrelated)
  // broker-level cancel remote-cancel also attempts once remote-id exists.
  const { dir: src, entries } = makeSnapshotSource({
    'a.txt': 'hello'.repeat(1000),
    'b.txt': 'world'.repeat(1000),
    'c.txt': 'more'.repeat(1000),
  });
  // Generous delay so cancellation reliably wins the race even under real
  // machine contention (parallel test files/processes) -- the invariant
  // under test is "cancel wins", not "cancel wins by exactly this much".
  const header = makeHeader({
    argv: [
      process.execPath,
      '-e',
      `setTimeout(() => require('fs').writeFileSync(${JSON.stringify(markerFile)}, '1'), 4000)`,
    ],
  });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });

  // Cancel the instant the ticket directory exists -- BEFORE `remote-id` can
  // possibly have been written yet (extraction of the snapshot into `work`
  // has to happen first). remote-cancel's own broker-level cancel branch is
  // a no-op here (there is no remote-id yet for it to act on): the only
  // thing that can still stop this child from running is remote-exec's own
  // post-remote-id "is it already marked cancelled" check.
  await waitFor(() => fs.existsSync(path.join(root, 'tickets', header.ticketId)), { intervalMs: 1 });
  await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });

  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'cancelled');
  await sleep(5000); // outlast the child's own 4s delay, if it somehow ran anyway
  assert.equal(fs.existsSync(markerFile), false);
});

test('remote-cancel fired against a confirmed-running (leased) child also results in "cancelled"', async () => {
  const { env, state } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: [process.execPath, '-e', 'setInterval(() => {}, 1000)'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });

  await waitFor(() => {
    const leasesDir = paths(state).leases;
    let names;
    try {
      names = fs.readdirSync(leasesDir);
    } catch {
      return false;
    }
    return names.some((n) => {
      const lease = readJsonSafe(path.join(leasesDir, n));
      return lease && lease.key && lease.key.startsWith(`${sanitizeKey(header.repoKey)}:`);
    });
  });

  await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'cancelled');
});

test('remote-cancel against a stopped (SIGSTOP) supervisor reports cancelRequested true and cancelConfirmed false (BRAIN-319 P3)', async () => {
  const { env, state } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: [process.execPath, '-e', 'setInterval(() => {}, 1000)'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });

  let leaseRecord;
  await waitFor(() => {
    const leasesDir = paths(state).leases;
    let names;
    try {
      names = fs.readdirSync(leasesDir);
    } catch {
      return false;
    }
    return names.some((n) => {
      const lease = readJsonSafe(path.join(leasesDir, n));
      if (lease && lease.key && lease.key.startsWith(`${sanitizeKey(header.repoKey)}:`)) {
        leaseRecord = lease;
        return true;
      }
      return false;
    });
  });

  // The ticket IS registered (a real lease exists) but its supervisor cannot
  // react to SIGTERM while stopped -- cancelCommand's own grace-period wait
  // times out and reports failure, so `cancelConfirmed` must be false even
  // though `cancelRequested` (the durable marker write) always succeeds.
  // `registered` reflects the (unlocked, informational-only) read that DID
  // see the lease.
  process.kill(leaseRecord.supervisorPid, 'SIGSTOP');
  try {
    const cancelResult = await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });
    assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);
    const parsed = JSON.parse(cancelResult.stdout.trim());
    assert.equal(parsed.cancelRequested, true);
    assert.equal(parsed.cancelConfirmed, false);
    assert.equal(parsed.registered, true);
  } finally {
    // Resume it so nothing leaks past this test -- the pending SIGTERM
    // cancelCommand already sent gets handled the instant it's running again.
    process.kill(leaseRecord.supervisorPid, 'SIGCONT');
  }

  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'cancelled');
});

test('remote-cancel after remote-id but before the ticket is registered is confirmed, and the ticket never runs (BRAIN-319 P3 follow-up, BRAIN-438 window)', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const pauseFile = path.join(tmpDir('remote-exec-pause'), 'go');
  const ranFile = path.join(tmpDir('remote-exec-ran'), 'ran');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(ranFile)}, 'x')`] });

  const stream = encodeSnapshot(src, header, entries);
  const spawnEnv = { ...env, LANE_BROKER_TEST_PAUSE_AFTER_TICKET_ID: pauseFile };
  const { child } = spawnRemoteExec(stream, { env: spawnEnv, root });

  // remote-id exists, but onTicketCreated is paused (pauseFile doesn't exist yet) -- run.js has not spawned the supervisor, so
  // this id is registered NOWHERE in the local broker (no queue entry, no lease, no attempt record) at the moment remote-cancel runs.
  const remoteIdPath = path.join(root, 'tickets', header.ticketId, 'remote-id');
  await waitFor(() => fs.existsSync(remoteIdPath));

  const cancelResult = await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);
  const parsed = JSON.parse(cancelResult.stdout.trim());
  assert.equal(parsed.cancelRequested, true);
  assert.equal(parsed.registered, false);
  assert.equal(parsed.cancelConfirmed, true, 'nothing is registered and the broker marker refuses any later start');

  fs.mkdirSync(path.dirname(pauseFile), { recursive: true });
  fs.writeFileSync(pauseFile, '1');
  const code = await waitClose(child);
  assert.equal(code, 0);
  assert.equal((await getResult(header.ticketId, root, env)).kind, 'cancelled');
  assert.equal(fs.existsSync(ranFile), false, 'the command never ran');
});

test('remote-cancel after the ticket dir exists but BEFORE remote-id is written is confirmed, the exec refuses to enqueue, and nothing runs', async () => {
  const { env, base } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const ready = path.join(base, 'hold-ready');
  const go = path.join(base, 'hold-go');
  const ranFile = path.join(tmpDir('remote-exec-ran'), 'ran');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(ranFile)}, 'x')`] });

  const stream = encodeSnapshot(src, header, entries);
  const spawnEnv = { ...env, LANE_BROKER_TEST_HOLD_AT: 'remote-exec-before-id', LANE_BROKER_TEST_HOLD_READY: ready, LANE_BROKER_TEST_HOLD_GO: go };
  const { child } = spawnRemoteExec(stream, { env: spawnEnv, root });
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 30_000 });
  const ticketDir = path.join(root, 'tickets', header.ticketId);
  assert.ok(fs.existsSync(ticketDir), 'the ticket dir exists');
  assert.equal(fs.existsSync(path.join(ticketDir, 'remote-id')), false, 'but remote-id does not yet');

  const cancelResult = await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);
  const parsed = JSON.parse(cancelResult.stdout.trim());
  assert.equal(parsed.cancelConfirmed, true, 'the exec will see the marker under the same lock and refuse');
  assert.equal(parsed.registered, false);

  fs.writeFileSync(go, 'x');
  const code = await waitClose(child);
  assert.equal(code, 0);
  assert.equal((await getResult(header.ticketId, root, env)).kind, 'cancelled');
  assert.equal(fs.existsSync(ranFile), false, 'the command never ran');
});

// A read that FAILS is an unknown, never a proof of absence: it must leave the cancel unconfirmed.
async function remoteCancelWith(arrange) {
  const { env, state } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const header = makeHeader();
  const ticketDir = path.join(root, 'tickets', header.ticketId);
  fs.mkdirSync(ticketDir, { recursive: true });
  const laneId = crypto.randomUUID();
  arrange({ ticketDir, state, laneId });
  const out = await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });
  assert.equal(out.code, 0, out.stderr);
  return JSON.parse(out.stdout.trim());
}

test('remote-cancel: a remote-id that exists but cannot be read is UNCONFIRMED, not "no remote-id"', async () => {
  const parsed = await remoteCancelWith(({ ticketDir, laneId }) => {
    fs.writeFileSync(path.join(ticketDir, 'remote-id'), laneId);
    fs.chmodSync(path.join(ticketDir, 'remote-id'), 0o000);
  });
  assert.equal(parsed.cancelConfirmed, false);
});

test('remote-cancel: a genuinely absent remote-id is still confirmed', async () => {
  const parsed = await remoteCancelWith(() => {});
  assert.equal(parsed.cancelConfirmed, true);
});

test('remote-cancel: an unreadable lease record leaves the cancel unconfirmed (both the first check and the post-failure re-check)', async () => {
  const parsed = await remoteCancelWith(({ ticketDir, state, laneId }) => {
    fs.writeFileSync(path.join(ticketDir, 'remote-id'), laneId);
    fs.mkdirSync(paths(state).leases, { recursive: true });
    const lease = path.join(paths(state).leases, `${laneId}.json`);
    fs.writeFileSync(lease, JSON.stringify({ id: laneId, supervisorPid: process.pid }));
    fs.chmodSync(lease, 0o000);
  });
  assert.equal(parsed.cancelConfirmed, false);
  assert.equal(parsed.cancelRequested, true, 'the broker marker is still written');
});

test('remote-cancel: a ticket with a remote-id and no broker records at all is confirmed', async () => {
  const parsed = await remoteCancelWith(({ ticketDir, laneId }) => fs.writeFileSync(path.join(ticketDir, 'remote-id'), laneId));
  assert.equal(parsed.cancelConfirmed, true);
});

// BRAIN-320 review fix B: a direct runner-side `lane cancel <id>` (the
// BROKER-level cancel, never touching the ticket-local `cancelled` marker
// remote-cancel writes) landing between tryStart's expiry and the
// supervisor's finalize must still win -- the published result must be
// 'cancelled', not 'queue-timeout'.
test('a broker-level lane cancel landing after a queue-timeout expiry, before finalize, wins: the LOCAL BROKER\'s own published result is cancelled, not queue-timeout', async () => {
  const { env: baseEnv, home, state } = freshEnv();
  writeGlobalConfig(home, { schedulerMode: 'shadow', sampleMs: 30 });
  // Keep the local broker permanently paused so the ticket sits queued
  // (never admitted) past its own startDeadline -- tryStart's expiry check
  // runs regardless of pause state (it's evaluated before the pause check),
  // so this reliably drives 'queue-timeout' without ever actually starting.
  fs.mkdirSync(state, { recursive: true });
  fs.writeFileSync(path.join(state, 'PAUSE'), 'kept busy for the test');

  const pauseFile = path.join(tmpDir('remote-exec-pause'), 'go');
  const env = { ...baseEnv, LANE_BROKER_TEST_PAUSE_AFTER_QUEUE_TIMEOUT: pauseFile };
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ protocol: 2, queueTimeoutMs: 200, argv: [process.execPath, '-e', 'process.exit(0)'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });

  const remoteIdPath = path.join(root, 'tickets', header.ticketId, 'remote-id');
  await waitFor(() => fs.existsSync(remoteIdPath));
  const remoteLaneId = fs.readFileSync(remoteIdPath, 'utf8').trim();

  // Give the supervisor time to actually observe the expiry (queueTimeoutMs
  // plus a few sampleMs polls) and reach the pause -- then fire the
  // BROKER-level cancel directly against the local broker, never through
  // `lane remote-cancel` (which also writes remote-exec's own ticket-local
  // marker, a separate, independent signal remote-exec's own kind
  // derivation already covers -- this test targets the LOCAL BROKER's own
  // published result instead, which is exactly what finalizeQueuedAndExit
  // decides).
  await sleep(500);
  const cancelResult = await laneRun(['cancel', remoteLaneId], { env });
  assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);

  fs.mkdirSync(path.dirname(pauseFile), { recursive: true });
  fs.writeFileSync(pauseFile, '1');

  const code = await waitClose(child);
  assert.equal(code, 0);

  const structured = readJsonSafe(path.join(paths(state).results, `${remoteLaneId}.json`));
  assert.ok(structured, 'the local broker must have published a result for the raced ticket');
  assert.equal(structured.cancelled, true);
  assert.notEqual(structured.reason, 'queue-timeout');
});

test('remote-cancel fired in the window between remote-id being recorded and the broker registering it still stops the child (BRAIN-319)', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const markerFile = path.join(tmpDir('remote-exec-marker'), 'marker');
  const pauseFile = path.join(tmpDir('remote-exec-pause'), 'go');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({
    argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(markerFile)}, '1')`],
  });

  const stream = encodeSnapshot(src, header, entries);
  const spawnEnv = { ...env, LANE_BROKER_TEST_PAUSE_AFTER_TICKET_ID: pauseFile };
  const { child } = spawnRemoteExec(stream, { env: spawnEnv, root });

  // Wait until remote-id exists: onTicketCreated has already written it and
  // already found the ticket-local `cancelled` marker unset, and is now
  // paused (pauseFile doesn't exist yet) -- well before run.js has even
  // spawned the supervisor, let alone before the supervisor has enqueued or
  // leased this same id with the local broker.
  const remoteIdPath = path.join(root, 'tickets', header.ticketId, 'remote-id');
  await waitFor(() => fs.existsSync(remoteIdPath));

  await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });

  // Release the pause: onTicketCreated's own ticket-local check already ran
  // and missed the cancel (it arrived after), so run.js proceeds to spawn
  // the supervisor exactly as if nothing had happened. Only the broker's own
  // cancel marker -- written by remote-cancel above, re-checked by
  // scheduler.js's tryStart under its admission lock right before the ticket
  // would be admitted -- can still stop the child from ever running.
  fs.mkdirSync(path.dirname(pauseFile), { recursive: true });
  fs.writeFileSync(pauseFile, '1');

  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'cancelled');
  await sleep(500); // outlast any window where the child could still be starting
  assert.equal(fs.existsSync(markerFile), false, 'the child must never have run');
});

// ---- remote-result / remote-probe ----

test('remote-result reports {missing:true} for an unknown ticket, and the real result once it exists', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');

  const missing = await getResult(crypto.randomUUID(), root, env);
  assert.deepEqual(missing, { protocol: 1, missing: true, state: 'gone' });

  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader();
  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);
  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.ticketId, header.ticketId);
  assert.equal(result.kind, 'completed');
});

// ---- remote-result state (BRAIN-339) ----

/** A ticket dir on a fresh runner root. `publisher` (default: this live test process) is written
 *  as publisher.json unless null; `remoteId` as remote-id unless false. Broker state dirs are
 *  created by a first remote-result call. */
async function stateFixture({ publisher = 'self', remoteId = true } = {}) {
  const { env, state } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const ticketId = crypto.randomUUID();
  const laneId = crypto.randomUUID();
  const ticketDir = path.join(root, 'tickets', ticketId);
  fs.mkdirSync(ticketDir, { recursive: true });
  if (remoteId) fs.writeFileSync(path.join(ticketDir, 'remote-id'), laneId);
  if (publisher) {
    const record = publisher === 'self' ? { pid: process.pid, start: processStartTime(process.pid) } : publisher;
    fs.writeFileSync(path.join(ticketDir, 'publisher.json'), JSON.stringify(record));
  }
  await getResult(ticketId, root, env);
  return { env, state, root, ticketId, laneId, ticketDir };
}

function deadPid() {
  return Number(spawnSync(process.execPath, ['-e', 'process.stdout.write(String(process.pid))']).stdout.toString());
}

test('remote-result state: a live publisher that is not queued is running', async () => {
  const f = await stateFixture();
  assert.deepEqual(await getResult(f.ticketId, f.root, f.env), { protocol: 1, missing: true, state: 'running', phase: 'preparing' });
});

test('remote-result state: a live publisher whose lane is queued is queued', async () => {
  const f = await stateFixture();
  await enqueue(f.state, { id: f.laneId, key: 'k:default', weight: 1, supervisorPid: process.pid });
  assert.equal((await getResult(f.ticketId, f.root, f.env)).state, 'queued');
});

test('remote-result state: a live publisher with no remote-id yet (pre-admission) is running', async () => {
  const f = await stateFixture({ remoteId: false });
  assert.equal((await getResult(f.ticketId, f.root, f.env)).state, 'running');
});

test('remote-result state: a dead publisher pid is gone', async () => {
  const pid = deadPid();
  const f = await stateFixture({ publisher: { pid, start: 'Mon Jan  1 00:00:00 2024' } });
  assert.equal((await getResult(f.ticketId, f.root, f.env)).state, 'gone');
});

test('remote-result state: a publisher pid reused by another process (start-time mismatch) is gone', async () => {
  const f = await stateFixture({ publisher: { pid: process.pid, start: 'Mon Jan  1 00:00:00 2001' } });
  assert.equal((await getResult(f.ticketId, f.root, f.env)).state, 'gone');
});

test('remote-result state: no publisher.json (older runner) is gone', async () => {
  const f = await stateFixture({ publisher: null });
  assert.equal((await getResult(f.ticketId, f.root, f.env)).state, 'gone');
});

test('remote-exec writes publisher.json naming its own process and start time', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader();
  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });
  const remoteExecPid = child.pid;
  const publisherFile = path.join(root, 'tickets', header.ticketId, 'publisher.json');
  await waitClose(child);
  const publisher = JSON.parse(fs.readFileSync(publisherFile, 'utf8'));
  assert.equal(publisher.pid, remoteExecPid);
  assert.equal(publisher.bootId, undefined);
  assert.equal(typeof publisher.start, 'string');
});

test('remote-result: a result.json that lands between the first read and the liveness check is returned, not gone', async () => {
  const f = await stateFixture({ publisher: { pid: deadPid(), start: 'Mon Jan  1 00:00:00 2024' } });
  const record = { protocol: 1, kind: 'completed', exit: 0 };
  let reads = 0;
  let out = '';
  const realWrite = process.stdout.write;
  process.stdout.write = (chunk) => {
    out += chunk;
    return true;
  };
  try {
    await remoteResultCommand(f.ticketId, { root: f.root, readResult: () => (reads++ === 0 ? null : record) });
  } finally {
    process.stdout.write = realWrite;
  }
  assert.equal(reads, 2);
  assert.deepEqual(JSON.parse(out.trim()), record);
});

test('remote-exec survives SIGHUP (a dropped ssh session) and still publishes the command exit', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: [process.execPath, '-e', 'setTimeout(() => process.exit(7), 2000)'] });
  const { child } = spawnRemoteExec(encodeSnapshot(src, header, entries), { env, root });
  const ticketDir = path.join(root, 'tickets', header.ticketId);
  await waitFor(() => fs.existsSync(path.join(ticketDir, 'remote-id')), { timeoutMs: 30_000 });
  child.kill('SIGHUP');
  assert.equal(await waitClose(child), 0);
  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 7);
});

test('remote-result state: a present result.json is returned as-is, with no state', async () => {
  const f = await stateFixture();
  fs.writeFileSync(path.join(f.ticketDir, 'result.json'), JSON.stringify({ protocol: 1, kind: 'completed' }));
  assert.deepEqual(await getResult(f.ticketId, f.root, f.env), { protocol: 1, kind: 'completed' });
});

test('remote-probe reports the expected shape', async () => {
  const { env } = freshShadowEnv();
  const { code, stdout } = await laneRun(['remote-probe'], { env });
  assert.equal(code, 0);
  const payload = JSON.parse(stdout.trim());
  assert.equal(payload.protocol, 1);
  assert.equal(typeof payload.version, 'string');
  assert.equal(typeof payload.paused, 'boolean');
  assert.ok(Number.isInteger(payload.queued));
  assert.ok(Number.isInteger(payload.running));
});

test('remote-probe reports protocols and static capacity (BRAIN-320 S1a/1d/1e)', async () => {
  // Active mode (freshEnv writes no config.json): CPU/memory budgets are enforced, so reported.
  const { env } = freshEnv();
  const { code, stdout } = await laneRun(['remote-probe'], { env });
  assert.equal(code, 0);
  const payload = JSON.parse(stdout.trim());
  assert.equal(payload.protocol, 1, 'protocol stays 1 for an old client');
  assert.deepEqual(payload.protocols, [1, 2]);
  assert.ok(payload.capacity && typeof payload.capacity === 'object');
  assert.ok(Number.isFinite(payload.capacity.weight) && payload.capacity.weight > 0);
  assert.ok(Number.isFinite(payload.capacity.cpuCores) && payload.capacity.cpuCores >= 0);
  assert.ok(Number.isFinite(payload.capacity.memoryBytes) && payload.capacity.memoryBytes > 0);
  assert.ok(Number.isFinite(payload.capacity.memoryReserveBytes) && payload.capacity.memoryReserveBytes >= 0);
});

test('remote-probe reports no CPU/memory capacity in shadow mode, where admission never enforces it', async () => {
  const { env } = freshShadowEnv();
  const { code, stdout } = await laneRun(['remote-probe'], { env });
  assert.equal(code, 0);
  const { capacity } = JSON.parse(stdout.trim());
  assert.ok(Number.isFinite(capacity.weight) && capacity.weight > 0);
  assert.equal(capacity.cpuCores, null);
  assert.equal(capacity.memoryBytes, null);
  assert.equal(capacity.memoryReserveBytes, null);
});

// ---- cleanup ----

test('the per-ticket work dir is removed after the result is written, but result.json is kept', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader();
  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const ticketDir = path.join(root, 'tickets', header.ticketId);
  assert.equal(fs.existsSync(path.join(ticketDir, 'work')), false);
  assert.equal(fs.existsSync(path.join(ticketDir, 'result.json')), true);
});

// ---- BRAIN-374: per-ticket TMPDIR ----

function tmpProbeArgv(file) {
  return [
    process.execPath,
    '-e',
    `const fs = require('fs'); fs.writeFileSync(${JSON.stringify(file)}, JSON.stringify({TMPDIR: process.env.TMPDIR, TMP: process.env.TMP, TEMP: process.env.TEMP, mode: fs.statSync(process.env.TMPDIR).mode & 0o777})); fs.writeFileSync(require('path').join(process.env.TMPDIR, 'scratch'), 'x'); process.exit(Number(process.env.PROBE_EXIT || 0))`,
  ];
}

test('a protocol-1 argv sees TMPDIR/TMP/TEMP = one 0700 /var/tmp/lb-* dir, and the dir is removed after success', async () => {
  const probeFile = path.join(tmpDir('remote-exec-probe'), 'env.json');
  const { env } = freshShadowEnv({ TMPDIR: os.tmpdir() });
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: tmpProbeArgv(probeFile) });
  const { code, err } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0, `stderr: ${err}`);

  const ticketDir = path.join(root, 'tickets', header.ticketId);
  const seen = JSON.parse(fs.readFileSync(probeFile, 'utf8'));
  const expected = seen.TMPDIR;
  assert.ok(expected.startsWith('/var/tmp/lb-'), `TMPDIR ${expected} must be a /var/tmp/lb-* dir`);
  assert.notEqual(expected, os.tmpdir(), 'the ambient TMPDIR is overridden');
  assert.deepEqual(seen, { TMPDIR: expected, TMP: expected, TEMP: expected, mode: 0o700 });
  assert.equal(fs.existsSync(expected), false);
  assert.equal(fs.existsSync(path.join(ticketDir, 'result.json')), true);
});

test('the per-ticket tmp dir is removed after a failing command, scratch files included', async () => {
  const probeFile = path.join(tmpDir('remote-exec-probe'), 'env.json');
  const { env } = freshShadowEnv({ PROBE_EXIT: '3' });
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({ argv: tmpProbeArgv(probeFile) });
  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  assert.equal(fs.existsSync(probeFile), true, 'the command ran (and wrote into tmp)');
  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.exit, 3);
  const { TMPDIR: seenTmp } = JSON.parse(fs.readFileSync(probeFile, 'utf8'));
  assert.ok(seenTmp.startsWith('/var/tmp/lb-'), `TMPDIR ${seenTmp} must be a /var/tmp/lb-* dir`);
  assert.equal(fs.existsSync(seenTmp), false);
});

test('the per-ticket tmp dir is removed after an early extract/verify rejection', async () => {
  // No command runs on this path, so a preload hook records the mkdtemp the runner made.
  const mkdtempLog = path.join(tmpDir('remote-exec-mkdtemp'), 'log');
  const hook = path.join(tmpDir('remote-exec-hook'), 'hook.cjs');
  fs.writeFileSync(
    hook,
    `const fs = require('fs'); const orig = fs.mkdtempSync; fs.mkdtempSync = (...a) => { const d = orig.apply(fs, a); fs.appendFileSync(${JSON.stringify(mkdtempLog)}, d + '\\n'); return d; };`,
  );
  const { env } = freshShadowEnv({ NODE_OPTIONS: `--require=${hook}` });
  const root = tmpDir('remote-exec-root');
  const ticketId = crypto.randomUUID();
  const entries = [{ path: 'a.txt', type: 'file', exec: false, size: 4, sha256: crypto.createHash('sha256').update('aaaa').digest('hex') }];
  const header = {
    protocol: 1,
    ticketId,
    generation: 0,
    repoKey: 'r',
    lane: 'default',
    argv: [process.execPath, '-e', 'process.exit(0)'],
    relCwd: '',
    manifest: { entries, manifestHash: manifestHashOf(entries) },
  };
  const body = Buffer.from('aaab');
  const stream = streamOf([
    line(header),
    line({ path: 'a.txt', type: 'file', exec: false, size: body.length }),
    body,
    line({ end: true }),
  ]);
  const { child } = spawnRemoteExec(stream, { env, root });
  assert.equal(await waitClose(child), 0);

  const ticketDir = path.join(root, 'tickets', ticketId);
  assert.equal((await getResult(ticketId, root, env)).kind, 'rejected');
  const made = fs.readFileSync(mkdtempLog, 'utf8').split('\n').filter((d) => d.startsWith('/var/tmp/lb-'));
  assert.equal(made.length, 1, 'the runner created exactly one /var/tmp/lb-* dir before extraction');
  assert.equal(fs.existsSync(made[0]), false);
  assert.equal(fs.existsSync(path.join(ticketDir, 'work')), false);
});

// ---- BRAIN-398: remoteArtifacts in the dispatch header ----

for (const [label, fields] of [
  ['a traversal path', { remoteArtifacts: ['../outside.txt'] }],
  ['an absolute path', { remoteArtifacts: ['/etc/passwd'] }],
  ['a non-array', { remoteArtifacts: 'a.json' }],
  ['an unknown policy', { remoteArtifacts: ['a.json'], remoteArtifactsOn: 'sometimes' }],
]) {
  test(`a header whose remoteArtifacts is ${label} is rejected before anything runs`, async () => {
    const { env } = freshShadowEnv();
    const root = tmpDir('remote-exec-root');
    const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
    const header = makeHeader({ ...fields });
    const { code } = await runOne(header, entries, { env, root, src });
    assert.equal(code, 0);
    const result = await getResult(header.ticketId, root, env);
    assert.equal(result.kind, 'rejected');
    assert.match(result.reason, /invalid remoteArtifacts/);
  });
}

test('lane remote-artifacts refuses a ticket id that is not a uuid, and reports nothing stored for a real one', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const bad = await laneRun(['remote-artifacts', '../x', '--root', root], { env });
  assert.equal(bad.code, 2);
  const none = await laneRun(['remote-artifacts', crypto.randomUUID(), '--root', root], { env });
  assert.equal(none.code, 1);
  assert.equal(none.stdout, '');
});
