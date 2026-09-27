import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, BIN } from './helpers.js';

/**
 * The fake-ssh transport harness shared by tests/remote-client.test.js
 * (unit-level dispatchRemote/selectRunner calls) and tests/remote-dispatch.test.js
 * (end-to-end `lane run` CLI calls). Extracted so there is exactly one fake
 * transport, never two independently-drifting copies.
 */

export function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
}

/** freshEnv() + shadow scheduler config: these tests exercise the client/protocol
 *  (or, in remote-dispatch.test.js, the supervisor's remote-dispatch wiring), not the
 *  host admission gate. */
export function freshShadowEnv(extra) {
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
 *     runner. The "remote" child is spawned with `LANE_FAKE_RUNNER=1` in its
 *     environment, so a test command can prove it actually ran through this
 *     fake transport (vs. a local fallback's plain `spawn`, which never sees
 *     that variable) by checking `process.env.LANE_FAKE_RUNNER`.
 *   - `lane`: a shim that execs THIS worktree's `bin/lane.js` with node --
 *     so a command string of the form `lane remote-exec --root ...`
 *     resolves correctly once this dir is prepended to PATH.
 * Returns { binDir, sshBin }; callers put `binDir` on PATH (via `env`) and
 * pass `sshBin` explicitly to dispatchRemote/selectRunner rather than
 * relying on PATH order for the ssh binary itself.
 */
export function makeFakeSshBin() {
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
  const child = spawn('sh', ['-c', commandString], { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, LANE_FAKE_RUNNER: '1' } });
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

export function makeRunner({ ssh, root, name = ssh }) {
  return { name, ssh, root: root ?? tmpDir('remote-client-root'), shell: 'sh -c' };
}

/** Env for dispatchRemote/selectRunner (or a real `lane run` CLI invocation): freshShadowEnv's
 *  isolated broker home/state, with `binDir` prepended to PATH so the remote command string's
 *  `lane` resolves to the shim AND the supervisor's own default `sshBin: 'ssh'` resolves to the
 *  fake transport. */
export function clientEnv(binDir, extra) {
  const { env, home, state } = freshShadowEnv(extra);
  return { env: { ...env, PATH: `${binDir}${path.delimiter}${env.PATH}` }, home, state };
}

/** Build a fresh git-tracked worktree source dir; buildManifest requires `git ls-files`. */
export function makeGitWorktree(files) {
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

export function makeDispatchArgs(overrides = {}) {
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
