import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { Readable } from 'node:stream';
import { freshEnv, writeGlobalConfig, BIN, laneRun, sleep, waitFor } from './helpers.js';
import { encodeSnapshot } from '../src/remote-stream.js';

/**
 * BRAIN-320 S1b (1b/1h): end-to-end pipeline tests against REAL `npm`, no
 * network. A tiny fixture package with a `postinstall` lifecycle script
 * proves real npm/lifecycle-script semantics (a fake npm could not) --
 * `.npmrc`'s `registry=http://127.0.0.1:9` is a belt-and-braces guard: the
 * fixture's lockfile has no registry dependency at all (only `hasInstallScript`),
 * so `npm ci` never has a reason to reach a registry regardless.
 */

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
}

function freshShadowEnv(extra) {
  const f = freshEnv(extra);
  writeGlobalConfig(f.home, {});
  return f;
}

/** Build a fresh source dir on disk matching `files` (relPath -> utf8 content), and the
 *  manifest entries `encodeSnapshot` needs for it -- same shape as remote-runner.test.js's
 *  own copy of this helper. */
function makeSnapshotSource(files) {
  const dir = tmpDir('remote-pipeline-src');
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

/** A tiny npm package fixture: `package.json` + `package-lock.json` (lockfileVersion 3,
 *  no registry dependency) + a `postinstall` lifecycle script that reports its own
 *  environment and completion via absolute, externally-supplied marker file paths (env
 *  vars) -- never paths inside the snapshot's work dir, since remote-exec deletes that
 *  dir (`cleanupWork`) the instant the result is written, before a test could ever read
 *  a marker left inside it. `lockMismatch` makes `npm ci` fail (package.json declares a
 *  dependency the lockfile doesn't have) without ever touching a registry.
 */
function makeDepsFixture({ lockMismatch = false } = {}) {
  const pkg = {
    name: 'fixture',
    version: '1.0.0',
    private: true,
    scripts: { postinstall: 'node postinstall.js' },
    ...(lockMismatch ? { dependencies: { 'left-pad': '^1.0.0' } } : {}),
  };
  const lock = {
    name: 'fixture',
    version: '1.0.0',
    lockfileVersion: 3,
    requires: true,
    packages: { '': { name: 'fixture', version: '1.0.0', hasInstallScript: true } },
  };
  const postinstall = [
    "const fs = require('fs');",
    'if (process.env.POSTINSTALL_ENV_FILE) {',
    '  fs.writeFileSync(process.env.POSTINSTALL_ENV_FILE, JSON.stringify({',
    "    npm_config_test_leak: process.env.npm_config_test_leak || null,",
    '    NODE_ENV: process.env.NODE_ENV || null,',
    '  }));',
    '}',
    'function finish() {',
    '  if (process.env.POSTINSTALL_RAN_FILE) fs.writeFileSync(process.env.POSTINSTALL_RAN_FILE, "1");',
    '}',
    'const sleepMs = Number(process.env.POSTINSTALL_SLEEP_MS || 0);',
    'if (sleepMs > 0) {',
    '  if (process.env.POSTINSTALL_PID_FILE) fs.writeFileSync(process.env.POSTINSTALL_PID_FILE, String(process.pid));',
    '  setTimeout(finish, sleepMs);',
    '} else {',
    '  finish();',
    '}',
  ].join('\n');
  return {
    'package.json': JSON.stringify(pkg),
    'package-lock.json': JSON.stringify(lock),
    // Registry pointed at an unreachable local port with retries/timeouts cut
    // to near-zero: a fixture with a real dependency (lockMismatch) makes npm
    // actually attempt to resolve it, and without these it can retry/back off
    // for many seconds before failing -- these bounds turn "unreachable" into
    // a fast ECONNREFUSED instead of a slow hang, while still proving no
    // fetch ever SUCCEEDS. A fixture with no real dependency (the default)
    // never has a reason to reach the registry at all.
    '.npmrc': 'registry=http://127.0.0.1:9\nfetch-retries=0\nfetch-retry-mintimeout=100\nfetch-retry-maxtimeout=100\nfetch-timeout=2000\n',
    'postinstall.js': postinstall,
  };
}

/** `node -e <script>` argv that exits 1 unless `requireFile` (an absolute path) exists,
 *  then writes `writeFile` (if given) and exits 0. Used as the command-phase argv to prove
 *  ordering (deps/setup really did finish before the command ran). */
function checkerArgv({ requireFile, writeFile } = {}) {
  const script = [
    "const fs = require('fs');",
    requireFile ? `if (!fs.existsSync(${JSON.stringify(requireFile)})) { process.exit(1); }` : '',
    writeFile ? `fs.writeFileSync(${JSON.stringify(writeFile)}, '1');` : '',
    'process.exit(0);',
  ].join('\n');
  return [process.execPath, '-e', script];
}

function makeHeader(overrides = {}) {
  return {
    protocol: 2,
    ticketId: crypto.randomUUID(),
    generation: 0,
    repoKey: 'remote-pipeline-test-repo',
    lane: 'default',
    relCwd: '',
    argv: [process.execPath, '-e', 'process.exit(0)'],
    ...overrides,
  };
}

const closed = new WeakMap();
const CLOSE_TIMEOUT_MS = 60_000;

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
  closed.set(child, new Promise((resolve) => child.on('close', (code) => resolve(code))));
  return { child, out: () => stdout, err: () => stderr };
}

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

function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

// ---- deps success ----

test('deps success: postinstall runs before the command, phase ends at command, exit 0', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-pipeline-root');
  const markerDir = tmpDir('remote-pipeline-markers');
  const ranFile = path.join(markerDir, 'postinstall-ran');
  const commandFile = path.join(markerDir, 'command-ran');
  const { dir: src, entries } = makeSnapshotSource(makeDepsFixture());
  const header = makeHeader({
    remoteDeps: ['.'],
    argv: checkerArgv({ requireFile: ranFile, writeFile: commandFile }),
  });

  const spawnEnv = { ...env, POSTINSTALL_RAN_FILE: ranFile };
  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env: spawnEnv, root });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.protocol, 2);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);
  assert.equal(result.phase, 'command');
  assert.equal(fs.existsSync(commandFile), true, 'the command ran, and saw the postinstall marker already there');
});

// ---- npm ci failure ----

test('npm ci failure: completed, nonzero exit, phase deps, command never runs', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-pipeline-root');
  const markerDir = tmpDir('remote-pipeline-markers');
  const commandFile = path.join(markerDir, 'command-ran');
  const { dir: src, entries } = makeSnapshotSource(makeDepsFixture({ lockMismatch: true }));
  const header = makeHeader({
    remoteDeps: ['.'],
    argv: checkerArgv({ writeFile: commandFile }),
  });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.notEqual(result.exit, 0);
  assert.equal(result.phase, 'deps');
  assert.equal(fs.existsSync(commandFile), false, 'the command must never have run');
});

// ---- setup failure ----

test('setup failure: completed, nonzero exit, phase setup, command never runs', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-pipeline-root');
  const markerDir = tmpDir('remote-pipeline-markers');
  const commandFile = path.join(markerDir, 'command-ran');
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });
  const header = makeHeader({
    remoteSetup: [[process.execPath, '-e', 'process.exit(7)']],
    argv: checkerArgv({ writeFile: commandFile }),
  });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 7);
  assert.equal(result.phase, 'setup');
  assert.equal(fs.existsSync(commandFile), false, 'the command must never have run');
});

// ---- protocol 2 without remoteDeps but with remoteSetup ----

test('protocol 2 without remoteDeps but with remoteSetup works (no deps phase)', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-pipeline-root');
  const markerDir = tmpDir('remote-pipeline-markers');
  const setupFile = path.join(markerDir, 'setup-ran');
  const header = makeHeader({
    remoteSetup: [[process.execPath, '-e', `require('fs').writeFileSync(${JSON.stringify(setupFile)}, '1')`]],
    argv: checkerArgv({ requireFile: setupFile }),
  });
  const { dir: src, entries } = makeSnapshotSource({ 'a.txt': 'hello' });

  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env, root });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);
  assert.equal(result.phase, 'command');
});

// ---- env scrub ----

test('scrub: an ambient npm_config_*/NODE_ENV in the runner env is not visible to deps, but is visible to command', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-pipeline-root');
  const markerDir = tmpDir('remote-pipeline-markers');
  const envFile = path.join(markerDir, 'postinstall-env.json');
  const commandEnvFile = path.join(markerDir, 'command-env.json');
  const { dir: src, entries } = makeSnapshotSource(makeDepsFixture());
  const header = makeHeader({
    remoteDeps: ['.'],
    argv: [
      process.execPath,
      '-e',
      `require('fs').writeFileSync(process.env.COMMAND_ENV_FILE, JSON.stringify({npm_config_test_leak: process.env.npm_config_test_leak || null, NODE_ENV: process.env.NODE_ENV || null}))`,
    ],
  });

  const spawnEnv = {
    ...env,
    POSTINSTALL_ENV_FILE: envFile,
    COMMAND_ENV_FILE: commandEnvFile,
    npm_config_test_leak: 'leaked',
    NODE_ENV: 'leaked-env',
  };
  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env: spawnEnv, root });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'completed');
  assert.equal(result.exit, 0);

  const postinstallEnv = JSON.parse(fs.readFileSync(envFile, 'utf8'));
  assert.equal(postinstallEnv.npm_config_test_leak, null, 'the deps phase must not see the ambient npm_config_* var');
  assert.equal(postinstallEnv.NODE_ENV, null, 'the deps phase must not see the ambient NODE_ENV');

  const commandEnv = JSON.parse(fs.readFileSync(commandEnvFile, 'utf8'));
  assert.equal(commandEnv.npm_config_test_leak, 'leaked', 'the command phase inherits the runner env unscrubbed');
  assert.equal(commandEnv.NODE_ENV, 'leaked-env', 'the command phase inherits the runner env unscrubbed');
});

// ---- cancel during deps ----

test('cancel during deps: kind cancelled, and the sleeping postinstall process is gone', async () => {
  const { env } = freshShadowEnv();
  const root = tmpDir('remote-pipeline-root');
  const markerDir = tmpDir('remote-pipeline-markers');
  const pidFile = path.join(markerDir, 'postinstall-pid');
  const ranFile = path.join(markerDir, 'postinstall-ran');
  const { dir: src, entries } = makeSnapshotSource(makeDepsFixture());
  const header = makeHeader({ remoteDeps: ['.'] });

  const spawnEnv = { ...env, POSTINSTALL_PID_FILE: pidFile, POSTINSTALL_RAN_FILE: ranFile, POSTINSTALL_SLEEP_MS: '30000' };
  const stream = encodeSnapshot(src, header, entries);
  const { child } = spawnRemoteExec(stream, { env: spawnEnv, root });

  await waitFor(() => fs.existsSync(pidFile), { timeoutMs: 30_000 });
  const postinstallPid = Number(fs.readFileSync(pidFile, 'utf8').trim());
  assert.ok(isPidAlive(postinstallPid), 'the postinstall process must be running before cancel');

  await laneRun(['remote-cancel', header.ticketId, '--root', root], { env });
  const code = await waitClose(child);
  assert.equal(code, 0);

  const result = await getResult(header.ticketId, root, env);
  assert.equal(result.kind, 'cancelled');

  await waitFor(() => !isPidAlive(postinstallPid), { timeoutMs: 20_000 });
  assert.equal(fs.existsSync(ranFile), false, 'postinstall must never have completed');
});
