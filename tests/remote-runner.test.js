import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { Readable } from 'node:stream';
import { freshEnv, writeGlobalConfig, BIN, laneRun, sleep, waitFor } from './helpers.js';
import { manifestHashOf, verifyManifestNoGit } from '../src/remote-manifest.js';
import { encodeSnapshot } from '../src/remote-stream.js';
import { sanitizeKey } from '../src/config.js';
import { paths, readJsonSafe } from '../src/state.js';
import { writeLease } from '../src/lease.js';

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
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

/** Spawn the real `bin/lane.js remote-exec --root <root>`, feeding `stream` as stdin. */
function spawnRemoteExec(stream, { env, root }) {
  const child = spawn(process.execPath, [BIN, 'remote-exec', '--root', root], { env });
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

async function runOne(header, entries, { env, root, src }) {
  const stream = encodeSnapshot(src, header, entries);
  const { child, err } = spawnRemoteExec(stream, { env, root });
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
    protocol: 2,
    argv: [process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(markerFile)}, '1')`],
  });

  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'rejected');
  assert.match(result.reason, /protocol/);
  assert.equal(fs.existsSync(markerFile), false, 'the child must never have run');
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

test('a .gitignore in the snapshot still applies inside the synthetic repo (git check-ignore)', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-exec-root');
  const { dir: src, entries } = makeSnapshotSource({
    '.gitignore': 'ignored.txt\n',
    'ignored.txt': 'x',
    'a.txt': 'hello',
  });
  const header = makeHeader({ argv: ['git', 'check-ignore', 'ignored.txt'] });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0, 'git check-ignore exits 0 exactly when the path is ignored');
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
  process.kill(leaseRecord.supervisorPid, 'SIGSTOP');
  try {
    const cancelResult = await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });
    assert.equal(cancelResult.code, 0, `stderr: ${cancelResult.stderr}`);
    const parsed = JSON.parse(cancelResult.stdout.trim());
    assert.equal(parsed.cancelRequested, true);
    assert.equal(parsed.cancelConfirmed, false);
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
  assert.deepEqual(missing, { protocol: 1, missing: true });

  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader();
  const { code } = await runOne(header, entries, { env, root, src });
  assert.equal(code, 0);
  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.ticketId, header.ticketId);
  assert.equal(result.kind, 'completed');
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
