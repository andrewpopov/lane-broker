import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, gitFixture, laneRun, BIN } from './helpers.js';
import { paths, readJsonSafe } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';

/**
 * The fake-ssh transport harness shared by tests/remote-client.test.js
 * (unit-level dispatchRemote/selectRunner calls) and tests/remote-dispatch.test.js
 * (end-to-end `lane run` CLI calls). Extracted so there is exactly one fake
 * transport, never two independently-drifting copies.
 */

export function tmpDir(prefix) {
  return makeTmpDir(`${prefix}-`);
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
  // A real runner is a SEPARATE machine with its own `~/.cache/lane-broker`
  // -- its own internal `lane run` (inside `lane remote-exec`) never shares
  // queue/lease state with the client that dispatched to it. This same-
  // machine fake transport would otherwise leak the two together (both
  // sides inherit the same LANE_BROKER_HOME/STATE through the spawn chain),
  // which would make a remote attempt's inner ticket show up as a real
  // local lease on the CLIENT's own `lane status` -- exactly the
  // cross-contamination BRAIN-319 T3b-4's "never counted toward local
  // capacity" tests exist to catch. Only `remote-exec` gets this override
  // (below): `remote-probe`/`remote-result`/`remote-cancel` intentionally
  // keep reading the shared state, since existing tests (e.g. "a runner
  // with a queued ticket is skipped") rely on the client's own queue/lease
  // doubling as the probed runner's busy state.
  const runnerHome = tmpDir('fake-runner-home');
  const runnerState = tmpDir('fake-runner-state');
  // BRAIN-319 T3b-5 (finding #5): a durable log of every `remote-probe` this
  // fake ssh sees, one destination name per line -- lets a test assert "no
  // probe happened at all" for an ineligible tree, not just "no dispatch."
  const probeLogDir = tmpDir('fake-probe-log');
  const probeLogPath = path.join(probeLogDir, 'probes.log');
  fs.writeFileSync(probeLogPath, '');
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
const fs = require('fs');
const { spawn } = require('child_process');

const argv = process.argv.slice(2);
const rest = [];
for (let i = 0; i < argv.length; i += 1) {
  if (argv[i] === '-o') { i += 1; continue; }
  rest.push(argv[i]);
}
const destination = rest[0];
const commandString = rest[1];

if (commandString && commandString.includes('remote-probe')) {
  try { fs.appendFileSync(${JSON.stringify(probeLogPath)}, destination + '\\n'); } catch {}
}

function exitNow(code) {
  process.exitCode = code;
  process.exit(code);
}

if (destination === 'old-runner' && commandString && commandString.includes('remote-probe')) {
  // BRAIN-320 S1c (1d): simulate a pre-S1a runner (0.6.0) whose remote-probe
  // response has no 'protocols' field at all -- the client must skip it for
  // a protocol-2-needing lane, but still use it for an optionless (protocol
  // 1) lane, exactly as it did before 'protocols' existed.
  process.stdout.write(JSON.stringify({ protocol: 1, paused: false, queued: 0, running: 0, version: '0.6.0', capacity: {} }) + '\\n');
  exitNow(0);
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
} else if (destination === 'no-artifacts' && commandString.includes('remote-probe')) {
  // BRAIN-398: a runner built before artifacts/1 -- its probe simply lacks the capability.
  const child = spawn('sh', ['-c', commandString], { stdio: ['ignore', 'pipe', 'inherit'] });
  let out = '';
  child.stdout.on('data', (d) => { out += d; });
  child.on('close', () => {
    const probe = JSON.parse(out.trim());
    probe.capabilities = probe.capabilities.filter((c) => !c.startsWith('artifacts/'));
    process.stdout.write(\`\${JSON.stringify(probe)}\\n\`);
    exitNow(0);
  });
} else if ((destination === 'slow-result' || destination === 'legacy-result') && commandString.includes('remote-result')) {
  // BRAIN-363: 'slow-result' lands the result well after the run ended (a recovery / poll delay); 'legacy-result' relays a
  // result from a runner that predates \`queuedMs\`.
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
    if (destination === 'legacy-result') delete record.queuedMs;
    const relay = () => {
      process.stdout.write(\`\${JSON.stringify(record)}\\n\`);
      exitNow(0);
    };
    if (destination === 'slow-result' && record && !record.missing) setTimeout(relay, 6000);
    else relay();
  });
} else if (destination === 'stuck-running' && /remote-(exec|result|cancel)/.test(commandString)) {
  // BRAIN-363: a runner whose job never finishes and whose remote-cancel replies without confirming it.
  if (commandString.includes('remote-exec')) {
    process.stdin.resume();
    process.stdin.on('end', () => exitNow(255));
  } else if (commandString.includes('remote-result')) {
    process.stdout.write(\`\${JSON.stringify({ protocol: 1, missing: true, state: 'running' })}\\n\`);
    exitNow(0);
  } else {
    process.stdout.write(\`\${JSON.stringify({ protocol: 1, cancelConfirmed: false })}\\n\`);
    exitNow(0);
  }
} else if ((destination === 'drop-withdrawn' || destination === 'drop-stuck') && /remote-(exec|result|withdraw)/.test(commandString)) {
  // BRAIN-437: a runner that took the snapshot and then lost the connection, and whose result fetches all fail. Its
  // remote-withdraw answers 'withdrawn' (drop-withdrawn: it never started) or 'not-queued' (drop-stuck: it may have).
  if (commandString.includes('remote-exec')) {
    process.stdin.resume();
    process.stdin.on('end', () => exitNow(255));
  } else if (commandString.includes('remote-result')) {
    exitNow(1);
  } else {
    const ticketId = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.exec(commandString)[0];
    process.stdout.write(\`\${JSON.stringify({ protocol: 1, ticketId, action: destination === 'drop-withdrawn' ? 'withdrawn' : 'not-queued' })}\\n\`);
    exitNow(0);
  }
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
  // remote-cancel needs the SAME isolated state remote-exec's inner ticket
  // lives in (it calls cancelCommand(remoteLaneId) internally, which
  // resolves the real lease to actually kill via LANE_BROKER_STATE) --
  // remote-probe/remote-result intentionally keep sharing the client's
  // state (see the comment above makeFakeSshBin).
  const remoteExecEnv = commandString.includes('remote-exec') || commandString.includes('remote-cancel')
    ? { LANE_BROKER_HOME: ${JSON.stringify(runnerHome)}, LANE_BROKER_STATE: ${JSON.stringify(runnerState)} }
    : {};
  const child = spawn('sh', ['-c', commandString], {
    stdio: ['pipe', 'pipe', 'pipe'],
    env: { ...process.env, LANE_FAKE_RUNNER: '1', ...remoteExecEnv },
  });
  process.stdin.pipe(child.stdin);
  child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);
  child.on('close', (code) => exitNow(code == null ? 1 : code));
  child.on('error', () => exitNow(1));
}
`,
  );
  fs.chmodSync(sshBin, 0o755);

  // BRAIN-320 S1d: exposed so a test can reach into the fake runner's OWN
  // broker state directly (e.g. pausing it, or pre-seeding a lease) to make
  // it durably busy for a `remote-exec` dispatch, without affecting the
  // client's own state (see the cross-contamination comment above).
  return { binDir, sshBin, probeLogPath, runnerHome, runnerState };
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

// ---------------------------------------------------------------------------
// CLI-level helpers, shared by tests/remote-dispatch.test.js and
// tests/remote-lifecycle.test.js: both drive the REAL `lane run` CLI (not
// dispatchRemote/selectRunner directly) over this same fake-ssh transport.
// ---------------------------------------------------------------------------

/** One global config (runners + admission knobs) and one repo config (lane
 *  `remote: true`) per test, sharing a fresh fake-ssh binDir. */
export function setup({ ssh = 'normal', cpuAdmissionPercent, weight = 1, remoteQueueTimeoutMs, remoteResultWaitMs } = {}) {
  const { binDir, probeLogPath, runnerHome, runnerState } = makeFakeSshBin();
  const runnerRoot = tmpDir('remote-dispatch-runner-root');
  const { home, state, env } = freshEnv();
  const cfg = {
    version: 1,
    capacity: 4,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    sampleMs: 100,
    runners: [{ name: 'skybox', ssh, shell: 'sh -c', root: runnerRoot }],
  };
  if (cpuAdmissionPercent !== undefined) {
    cfg.schedulerMode = 'active';
    cfg.cpuAdmissionPercent = cpuAdmissionPercent;
    cfg.cpuReserveCores = 0;
  }
  // BRAIN-320 S1d: opt-in on the CLIENT machine's own global config -- the
  // runner's own broker (a separate LANE_BROKER_HOME, see makeFakeSshBin's
  // doc comment) gets its own fast-poll config below regardless, so a small
  // queueTimeoutMs here doesn't just wait out the runner's 5s default
  // sampleMs before ever observing the expiry.
  if (remoteQueueTimeoutMs !== undefined) cfg.remoteQueueTimeoutMs = remoteQueueTimeoutMs;
  if (remoteResultWaitMs !== undefined) cfg.remoteResultWaitMs = remoteResultWaitMs;
  writeGlobalConfig(home, cfg);
  writeGlobalConfig(runnerHome, { sampleMs: 50, capacity: 4 });
  const repoDir = tmpDir('remote-dispatch-repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight, remote: true } } });
  gitFixture(['init', '-q'], repoDir);
  return { env: { ...env, PATH: `${binDir}${path.delimiter}${env.PATH}` }, home, state, repoDir, runnerRoot, probeLogPath, runnerHome, runnerState };
}

/** Number of `remote-probe` calls logged so far (see makeFakeSshBin's `probeLogPath`). */
export function probeCount(probeLogPath) {
  try {
    return fs.readFileSync(probeLogPath, 'utf8').split('\n').filter(Boolean).length;
  } catch {
    return 0;
  }
}

/** A `[cmd, ...args]` argv that reports where it ran (via LANE_FAKE_RUNNER,
 *  only ever set by the fake ssh's spawned remote child) by writing 'remote'
 *  or 'local' to `markerPath`, optionally after emitting `stdoutText`, then
 *  exits `exitCode`. */
export function markerCmd(markerPath, exitCode = 0, stdoutText = null) {
  const body = `
const fs = require('fs');
${stdoutText ? `process.stdout.write(${JSON.stringify(stdoutText)});` : ''}
fs.writeFileSync(${JSON.stringify(markerPath)}, process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local');
process.exit(${exitCode});
`;
  return [process.execPath, '-e', body];
}

export async function detachAndWait(args, env, cwd, waitTimeout = '30s') {
  const started = await laneRun(args, { env, cwd });
  assert.equal(started.code, 0, `--detach itself should not fail: ${started.stderr}`);
  const id = started.stdout.trim();
  // Bounded: a ticket that should have been refused (or otherwise never
  // completes) must fail this test fast and by name, never hang the whole
  // suite waiting on a `lane wait` that has nothing to wait for.
  const waited = await laneRun(['wait', id, '--timeout', waitTimeout], { env });
  return { id, waited };
}

export function resultOf(state, id) {
  return readJsonSafe(path.join(paths(state).results, `${id}.json`));
}
