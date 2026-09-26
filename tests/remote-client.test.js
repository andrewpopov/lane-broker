import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, BIN, laneRun, waitFor, sleep } from './helpers.js';
import { shellQuote, buildRemoteCommand, selectRunner, dispatchRemote, isGreen } from '../src/remote-client.js';

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
}

/** freshEnv() + shadow scheduler config, same reasoning as remote-runner.test.js's own
 *  freshShadowEnv: these tests exercise the client/protocol, not the host admission gate. */
function freshShadowEnv(extra) {
  const f = freshEnv(extra);
  writeGlobalConfig(f.home, {});
  return f;
}

/**
 * Build a temp bin dir containing:
 *   - `ssh`: the fake transport. Ignores `-o KEY=VALUE` pairs; reads the
 *     destination and the single trailing command string, and (mode-
 *     dependent, selected by destination name) either runs that command
 *     string with `sh -c` locally, or simulates an unreachable/dying/lying
 *     runner.
 *   - `lane`: a shim that execs THIS worktree's `bin/lane.js` with node --
 *     so a command string of the form `lane remote-exec --root ...`
 *     resolves correctly once this dir is prepended to PATH.
 * Returns { binDir, sshBin }; callers put `binDir` on PATH (via `env`) and
 * pass `sshBin` explicitly to dispatchRemote/selectRunner rather than
 * relying on PATH order for the ssh binary itself.
 */
function makeFakeSshBin() {
  const binDir = tmpDir('fake-ssh-bin');
  const laneShim = path.join(binDir, 'lane');
  fs.writeFileSync(
    laneShim,
    `#!/usr/bin/env node
const { spawnSync } = require('child_process');
const res = spawnSync(process.execPath, [${JSON.stringify(BIN)}, ...process.argv.slice(2)], { stdio: 'inherit' });
process.exit(res.status == null ? 1 : res.status);
`,
  );
  fs.chmodSync(laneShim, 0o755);

  const sshBin = path.join(binDir, 'ssh');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
const { spawn } = require('child_process');

const argv = process.argv.slice(2);
const rest = [];
for (let i = 0; i < argv.length; i += 1) {
  if (argv[i] === '-o') { i += 1; continue; }
  rest.push(argv[i]);
}
const destination = rest[0];
const commandString = rest[1];

function exitNow(code) {
  process.exitCode = code;
  process.exit(code);
}

if (destination === 'down') {
  exitNow(255);
}

if (destination === 'die-midstream') {
  let received = 0;
  const LIMIT = 64;
  let done = false;
  const finish = () => {
    if (done) return;
    done = true;
    exitNow(255);
  };
  process.stdin.on('data', (chunk) => {
    received += chunk.length;
    if (received >= LIMIT) finish();
  });
  process.stdin.on('end', finish);
  process.stdin.on('error', finish);
} else if (destination === 'result-tamper' && commandString.includes('remote-result')) {
  const child = spawn('sh', ['-c', commandString], { stdio: ['ignore', 'pipe', 'inherit'] });
  let out = '';
  child.stdout.on('data', (d) => { out += d; });
  child.on('close', () => {
    let record;
    try {
      record = JSON.parse(out.trim());
    } catch {
      process.stdout.write(out);
      exitNow(0);
      return;
    }
    if (record && !record.missing) record.manifestHash = 'tampered-hash-0000';
    process.stdout.write(\`\${JSON.stringify(record)}\\n\`);
    exitNow(0);
  });
} else {
  // Deliberately NOT 'inherit' for the grandchild's stdout/stderr: a real
  // ssh client's own local stdio pipes are never shared with the remote
  // job, so killing the local ssh process alone always closes them
  // immediately. 'inherit' here would instead share this script's own
  // stdout/stderr fds with the grandchild -- so if the grandchild (the
  // actual remote job) outlives THIS script (SIGKILLed by an abort), the
  // pipe back to the real caller never sees EOF and 'close' never fires,
  // a hang this script must not reproduce. Relaying through its own
  // separate pipes instead means this script's exit alone closes what it
  // owns, regardless of what the grandchild keeps doing.
  const child = spawn('sh', ['-c', commandString], { stdio: ['pipe', 'pipe', 'pipe'] });
  process.stdin.pipe(child.stdin);
  child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);
  child.on('close', (code) => exitNow(code == null ? 1 : code));
  child.on('error', () => exitNow(1));
}
`,
  );
  fs.chmodSync(sshBin, 0o755);

  return { binDir, sshBin };
}

function makeRunner({ ssh, root, binDir, name = ssh }) {
  return { name, ssh, root: root ?? tmpDir('remote-client-root'), shell: 'sh -c' };
}

/** Env for dispatchRemote/selectRunner: freshShadowEnv's isolated broker home/state, with
 *  `binDir` prepended to PATH so the remote command string's `lane` resolves to the shim. */
function clientEnv(binDir, extra) {
  const { env, home, state } = freshShadowEnv(extra);
  return { env: { ...env, PATH: `${binDir}${path.delimiter}${env.PATH}` }, home, state };
}

/** Build a fresh git-tracked worktree source dir; buildManifest requires `git ls-files`. */
function makeGitWorktree(files) {
  const dir = tmpDir('remote-client-src');
  execFileSync('git', ['-c', 'init.defaultBranch=main', 'init', '-q'], { cwd: dir });
  for (const [rel, content] of Object.entries(files)) {
    const abs = path.join(dir, rel);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, content);
  }
  execFileSync('git', ['add', '-A'], { cwd: dir });
  execFileSync(
    'git',
    ['-c', 'user.name=t', '-c', 'user.email=t@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '-q', '-m', 'x'],
    { cwd: dir },
  );
  return dir;
}

function makeDispatchArgs(overrides = {}) {
  return {
    ticketId: crypto.randomUUID(),
    generation: 0,
    repoKey: 'remote-client-test-repo',
    lane: 'default',
    weight: undefined,
    cpuCores: undefined,
    memoryBytes: undefined,
    relCwd: '',
    argv: [process.execPath, '-e', 'process.exit(0)'],
    ...overrides,
  };
}

// ---- end-to-end dispatch: happy path ----

test('dispatchRemote: a completed exit 0 is confirmed, streaming stdout through the callback', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });
  let stdout = '';

  const result = await dispatchRemote({
    ...makeDispatchArgs({ argv: [process.execPath, '-e', "process.stdout.write('hi-from-remote\\n')"] }),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    onStdout: (d) => {
      stdout += d;
    },
  });

  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 0);
  assert.equal(result.result.kind, 'completed');
  assert.match(stdout, /hi-from-remote/);
});

for (const exitCode of [1, 42, 255]) {
  test(`dispatchRemote: a child exiting ${exitCode} is confirmed with that exact exitCode`, async () => {
    const { binDir, sshBin } = makeFakeSshBin();
    const { env } = clientEnv(binDir);
    const src = makeGitWorktree({ 'a.txt': 'hello' });
    const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });

    const result = await dispatchRemote({
      ...makeDispatchArgs({ argv: [process.execPath, '-e', `process.exit(${exitCode})`] }),
      runner,
      worktreeRoot: src,
      sshBin,
      env,
    });

    assert.equal(result.outcome, 'confirmed');
    assert.equal(result.exitCode, exitCode);
  });
}

test('dispatchRemote: an undeclared lane in the snapshot\'s own .lane-broker.json is confirmed with the refusal exit 64', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({
    '.lane-broker.json': JSON.stringify({ version: 1, lanes: { default: { weight: 1 } } }),
  });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });

  const result = await dispatchRemote({
    ...makeDispatchArgs({ lane: 'no-such-lane' }),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
  });

  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 64);
  assert.equal(result.result.kind, 'refused');
});

test('dispatchRemote: relCwd is honoured end to end', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'sub/.keep': 'x' });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });
  let stdout = '';

  const result = await dispatchRemote({
    ...makeDispatchArgs({
      relCwd: 'sub',
      argv: [process.execPath, '-e', 'process.stdout.write(require("path").basename(process.cwd()))'],
    }),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    onStdout: (d) => {
      stdout += d;
    },
  });

  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 0);
  assert.equal(stdout, 'sub');
});

// ---- ineligible: nothing spawned ----

test('dispatchRemote: a tracked .env is ineligible, and nothing is spawned', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ '.env': 'SECRET=1', 'a.txt': 'hello' });
  // A destination that would hang forever if anything were actually dialed --
  // proves nothing was spawned, rather than merely that it failed fast.
  const runner = makeRunner({ ssh: 'must-not-be-dialed', root: tmpDir('remote-exec-root') });

  const before = Date.now();
  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    deadlines: { transferMs: 30_000 },
  });
  const elapsedMs = Date.now() - before;

  assert.equal(result.outcome, 'ineligible');
  assert.match(result.reason, /denylisted secret path/);
  assert.ok(elapsedMs < 5000, `should return immediately, took ${elapsedMs}ms`);
});

// ---- adversarial transport ----

test('dispatchRemote: ssh dying mid-transfer (die-midstream) is unconfirmed', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello'.repeat(1000), 'b.txt': 'world'.repeat(1000) });
  const runner = makeRunner({ ssh: 'die-midstream', root: tmpDir('remote-exec-root') });

  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    deadlines: { transferMs: 10_000, resultMs: 5000, resultAttempts: 2 },
  });

  assert.equal(result.outcome, 'unconfirmed');
});

test('dispatchRemote: a runner returning a result with the wrong manifestHash (result-tamper) is unconfirmed', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const runner = makeRunner({ ssh: 'result-tamper', root: tmpDir('remote-exec-root') });

  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    deadlines: { resultMs: 5000, resultAttempts: 2 },
  });

  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /did not bind/);
});

// ---- cancellation ----

test('dispatchRemote: abort during a sleeping remote child resolves cancelled, and the ticket\'s own result kind becomes cancelled', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const root = tmpDir('remote-exec-root');
  const runner = makeRunner({ ssh: 'normal', root });
  const ticketId = crypto.randomUUID();
  const controller = new AbortController();

  const dispatchPromise = dispatchRemote({
    ...makeDispatchArgs({ ticketId, argv: [process.execPath, '-e', 'setInterval(() => {}, 1000)'] }),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    abortSignal: controller.signal,
  });

  // Wait until the remote ticket directory exists (extraction has started/finished) before
  // firing the abort, so this exercises "cancel a run actually in flight", not a race with dial.
  await waitFor(() => fs.existsSync(path.join(root, 'tickets', ticketId)), { timeoutMs: 15_000 });
  await sleep(300);
  controller.abort();

  const result = await dispatchPromise;
  assert.equal(result.outcome, 'cancelled');

  const final = await waitFor(
    async () => {
      const { stdout } = await laneRun(['remote-result', ticketId, '--root', root], { env });
      const parsed = JSON.parse(stdout.trim());
      return parsed.kind === 'cancelled' ? parsed : null;
    },
    { timeoutMs: 15_000 },
  );
  assert.equal(final.kind, 'cancelled');
});

// ---- selectRunner ----

test('selectRunner: sequential order, skips an unreachable runner and picks the next usable one', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const runners = [makeRunner({ ssh: 'down' }), makeRunner({ ssh: 'normal', name: 'second' })];

  const { runner, skipped } = await selectRunner(runners, { sshBin, env, deadlineMs: 3000 });

  assert.equal(runner.name, 'second');
  assert.equal(skipped.length, 1);
  assert.equal(skipped[0].name, runners[0].name);
});

test('selectRunner: a paused runner is skipped', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  await laneRun(['pause', 'maintenance'], { env });
  const runners = [makeRunner({ ssh: 'normal' })];

  const { runner, skipped } = await selectRunner(runners, { sshBin, env, deadlineMs: 3000 });

  assert.equal(runner, null);
  assert.equal(skipped.length, 1);
  assert.match(skipped[0].reason, /paused/);
});

test('selectRunner: a runner with a queued ticket is skipped', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { home, state, env } = freshEnv();
  const globalConfig = { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 };
  writeGlobalConfig(home, globalConfig);
  const repoDir = tmpDir('remote-client-queue-repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const clientPath = { env: { ...env, PATH: `${binDir}${path.delimiter}${env.PATH}` } };

  // Occupy the only capacity slot so a second ticket is stuck queued, same
  // pattern as tests/queued-cancel.test.js.
  const STARTUP_TIMEOUT_MS = Math.max(60_000, 30 * globalConfig.sampleMs);
  const blockerPromise = laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sleep', '5'], { env: clientPath.env, cwd: repoDir });
  await waitFor(
    () => {
      try {
        return fs.readdirSync(path.join(state, 'leases')).filter((n) => n.endsWith('.json')).length > 0;
      } catch {
        return false;
      }
    },
    { timeoutMs: STARTUP_TIMEOUT_MS },
  );
  const queuedPromise = laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env: clientPath.env, cwd: repoDir });
  await waitFor(
    () => {
      try {
        return fs.readdirSync(path.join(state, 'queue')).length > 0;
      } catch {
        return false;
      }
    },
    { timeoutMs: STARTUP_TIMEOUT_MS },
  );

  const runners = [makeRunner({ ssh: 'normal' })];
  const { runner, skipped } = await selectRunner(runners, { sshBin, env: clientPath.env, deadlineMs: 3000 });

  assert.equal(runner, null);
  assert.ok(skipped.length >= 1);
  assert.match(skipped[0].reason, /queued/);

  await Promise.all([blockerPromise, queuedPromise]);
});

test('selectRunner: a protocol-2 runner is skipped', async () => {
  const { binDir } = makeFakeSshBin();
  // A second fake ssh whose "normal" path answers remote-probe honestly is not enough here --
  // build a bespoke fake ssh that always answers protocol 2 regardless of subcommand.
  const sshBin = path.join(binDir, 'ssh-protocol-mismatch');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
process.stdout.write(JSON.stringify({ protocol: 2, version: '0.0.0', paused: false, queued: 0, running: 0 }) + '\\n');
process.exit(0);
`,
  );
  fs.chmodSync(sshBin, 0o755);
  const { env } = clientEnv(binDir);
  const runners = [makeRunner({ ssh: 'normal' })];

  const { runner, skipped } = await selectRunner(runners, { sshBin, env, deadlineMs: 3000 });

  assert.equal(runner, null);
  assert.match(skipped[0].reason, /protocol/);
});

test('selectRunner: none usable returns {runner:null, skipped:[...]}', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const runners = [makeRunner({ ssh: 'down' }), makeRunner({ ssh: 'down', name: 'second' })];

  const { runner, skipped } = await selectRunner(runners, { sshBin, env, deadlineMs: 3000 });

  assert.equal(runner, null);
  assert.equal(skipped.length, 2);
});

// ---- shellQuote ----

test('shellQuote round-trips spaces, quotes, $, backticks and newlines through a real shell', () => {
  const cases = [
    'plain',
    'has spaces',
    `has "double" quotes`,
    `has 'single' quotes`,
    'has $DOLLAR and $(command)',
    'has `backticks`',
    'has\nnewlines\nin it',
    "mix: $ ` ' \" \n end",
  ];
  for (const value of cases) {
    const out = execFileSync('sh', ['-c', `printf '%s' ${shellQuote(value)}`], { encoding: 'utf8' });
    assert.equal(out, value, `round-trip failed for ${JSON.stringify(value)}`);
  }
});

test('dispatchRemote: weird argv (spaces/quotes/$/backticks/newlines) reaches the remote child byte-identical', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });
  const weird = ['has spaces', 'has "double"', "has 'single'", 'has $DOLLAR', 'has `backtick`', 'has\nnewline'];

  let stdout = '';
  const result = await dispatchRemote({
    ...makeDispatchArgs({
      argv: [process.execPath, '-e', 'process.stdout.write(JSON.stringify(process.argv.slice(1)))', ...weird],
    }),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    onStdout: (d) => {
      stdout += d;
    },
  });

  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 0);
  assert.deepEqual(JSON.parse(stdout), weird);
});

// ---- buildRemoteCommand ----

test('buildRemoteCommand includes --root only when the runner configures one', () => {
  const withoutRoot = buildRemoteCommand({ name: 'r', ssh: 'r' }, 'remote-probe');
  assert.doesNotMatch(withoutRoot, /--root/);
  const withRoot = buildRemoteCommand({ name: 'r', ssh: 'r', root: '/srv/lane-broker/remote' }, 'remote-result', ['abc']);
  assert.match(withRoot, /--root/);
  assert.match(withRoot, /\/srv\/lane-broker\/remote/);
});

// ---- isGreen ----

test('isGreen: true only for a bound, completed, unsignalled, exact zero exit', () => {
  const expected = { ticketId: 't1', generation: 0, manifestHash: 'h1' };
  const green = { protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 0, signal: null };
  assert.equal(isGreen(green, expected), true);

  assert.equal(isGreen({ ...green, exit: 1 }, expected), false, 'nonzero exit');
  assert.equal(isGreen({ ...green, signal: 'SIGTERM' }, expected), false, 'signalled');
  assert.equal(isGreen({ ...green, kind: 'refused' }, expected), false, 'wrong kind');
  assert.equal(isGreen({ ...green, ticketId: 'other' }, expected), false, 'ticketId mismatch');
  assert.equal(isGreen({ ...green, generation: 1 }, expected), false, 'generation mismatch');
  assert.equal(isGreen({ ...green, manifestHash: 'other' }, expected), false, 'manifestHash mismatch');
  assert.equal(isGreen({ ...green, protocol: 2 }, expected), false, 'protocol mismatch');
  assert.equal(isGreen({ ...green, exit: '0' }, expected), false, 'non-integer exit');
  assert.equal(isGreen(null, expected), false, 'null result');
});
