import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, gitFixture, laneRun, laneSpawn, waitFor, BIN } from './helpers.js';
import { paths, readJsonSafe } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';
import { CAPABILITIES } from '../src/capabilities.js';

/**
 * The fake-ssh transport harness shared by tests/remote-client.test.js
 * (unit-level dispatchRemote/selectRunner calls) and tests/remote-dispatch.test.js
 * (end-to-end `lane run` CLI calls). Extracted so there is exactly one fake
 * transport, never two independently-drifting copies.
 */

/** The snapshot header a client sent over `remote-exec`'s stdin, from the raw stdin bytes a test's ssh wrapper saved: plain JSON line for a
 *  legacy runner, a gzip stream (BRAIN-642, snapshot-gzip/1) for the default path. */
export function readCapturedHeader(file) {
  let bytes = fs.readFileSync(file);
  if (bytes[0] === 0x1f && bytes[1] === 0x8b) bytes = zlib.gunzipSync(bytes);
  return JSON.parse(bytes.subarray(0, bytes.indexOf(10)).toString('utf8'));
}

export function tmpDir(prefix) {
  return makeTmpDir(`${prefix}-`);
}

/** README "Testing hooks": a 200 ms first result-poll backoff (production 5 s) and a 1 s cancel grace (production 10 s). */
export const FAST_WAITS = { LANE_BROKER_TEST_RESULT_POLL_MS: '200', LANE_BROKER_TEST_CANCEL_GRACE_MS: '1000' };

/** freshEnv() + shadow scheduler config: these tests exercise the client/protocol
 *  (or, in remote-dispatch.test.js, the supervisor's remote-dispatch wiring), not the
 *  host admission gate. */
export function freshShadowEnv(extra) {
  const f = freshEnv({ ...FAST_WAITS, ...extra });
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
  // BRAIN-642: the bytes ssh's stdin carried for a `remote-exec` to 'tee' / 'strict-legacy', and what a strict-legacy runner objected to.
  const teePath = path.join(probeLogDir, 'exec-stdin.bin');
  const violationsPath = path.join(probeLogDir, 'legacy-violations.log');
  fs.writeFileSync(violationsPath, '');
  // BRAIN-642 slice C2: the stdin of the last `remote-have`, and every `remote-exec` command string (one per line), in order.
  const haveLogPath = path.join(probeLogDir, 'have-stdin.json');
  const execCmdsPath = path.join(probeLogDir, 'exec-cmds.log');
  fs.writeFileSync(execCmdsPath, '');
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

if (commandString && commandString.includes('remote-exec')) {
  try { fs.appendFileSync(${JSON.stringify(execCmdsPath)}, commandString + '\\n'); } catch {}
}

// BRAIN-642 C2: env knobs. LANE_FAKE_HAVE = fail | hang; LANE_FAKE_AFTER_HAVE = delete:<dir> | corrupt:<dir> | mutate:<file>, applied
// once the runner has answered a remote-have (the window between the have-answer and the exec).
function afterHave() {
  const spec = process.env.LANE_FAKE_AFTER_HAVE;
  if (!spec) return;
  const at = spec.indexOf(':');
  const kind = spec.slice(0, at);
  const target = spec.slice(at + 1);
  const flip = (file) => { const b = fs.readFileSync(file); b[0] ^= 0xff; fs.writeFileSync(file, b); };
  const walk = (dir) => fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => e.name.startsWith('.') ? [] : e.isDirectory() ? walk(dir + '/' + e.name) : [dir + '/' + e.name]);
  try {
    if (kind === 'delete') for (const f of walk(target)) fs.rmSync(f, { force: true });
    else if (kind === 'corrupt') for (const f of walk(target)) { fs.chmodSync(f, 0o600); flip(f); }
    else if (kind === 'mutate') flip(target);
  } catch {}
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
} else if (commandString && commandString.includes('remote-have') && process.env.LANE_FAKE_HAVE === 'fail') {
  exitNow(255);
} else if (commandString && commandString.includes('remote-have') && process.env.LANE_FAKE_HAVE === 'hang') {
  setInterval(() => {}, 1000);
} else if (commandString && commandString.includes('remote-have')) {
  const chunks = [];
  process.stdin.on('data', (c) => chunks.push(c));
  const child = spawn('sh', ['-c', commandString], { stdio: ['pipe', 'pipe', 'inherit'], env: { ...process.env, LANE_FAKE_RUNNER: '1', LANE_BROKER_HOME: ${JSON.stringify(runnerHome)}, LANE_BROKER_STATE: ${JSON.stringify(runnerState)} } });
  process.stdin.pipe(child.stdin);
  child.stdout.pipe(process.stdout);
  child.on('close', (code) => {
    try { fs.writeFileSync(${JSON.stringify(haveLogPath)}, Buffer.concat(chunks)); } catch {}
    if (code === 0) afterHave();
    exitNow(code == null ? 1 : code);
  });
} else if (((destination === 'no-artifacts' || destination === 'queued-one' || destination === 'strict-legacy') || process.env.LANE_FAKE_CAPS_DROP) && commandString.includes('remote-probe')) {
  // BRAIN-398: a runner built before artifacts/1 -- its probe simply lacks the capability.
  // BRAIN-455: 'queued-one' reports one queued ticket whatever the client's own queue holds, so local admission stays free.
  const child = spawn('sh', ['-c', commandString], { stdio: ['ignore', 'pipe', 'inherit'] });
  let out = '';
  child.stdout.on('data', (d) => { out += d; });
  child.on('close', () => {
    const probe = JSON.parse(out.trim());
    if (destination === 'no-artifacts') probe.capabilities = probe.capabilities.filter((c) => !c.startsWith('artifacts/'));
    else if (destination === 'strict-legacy') probe.capabilities = probe.capabilities.filter((c) => !c.startsWith('snapshot-gzip/') && !c.startsWith('blob-cache/'));
    else if (destination === 'queued-one') probe.queued = 1;
    // BRAIN-642 C2: LANE_FAKE_CAPS_DROP=snapshot-gzip/,blob-cache/ drops capabilities by prefix on any destination
    for (const prefix of (process.env.LANE_FAKE_CAPS_DROP || '').split(',').filter(Boolean)) probe.capabilities = probe.capabilities.filter((c) => !c.startsWith(prefix));
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
} else if (/^drop-/.test(destination) && /remote-(exec|result|withdraw)/.test(commandString)) {
  // BRAIN-437: a runner that took the snapshot and then lost the connection, and whose result fetches all fail. Its
  // remote-withdraw answers 'withdrawn' (drop-withdrawn: it never started), 'not-queued' (drop-stuck, drop-exit1: it may have) or
  // 'no-such-ticket' + tombstoned (drop-gone: a finished ticket the runner's GC has since removed). remote-exec closes with 255
  // (the connection dropped), or with 1 (drop-exit1: the remote command failed in a way that says nothing about a ticket).
  if (commandString.includes('remote-exec')) {
    process.stdin.resume();
    process.stdin.on('end', () => exitNow(destination === 'drop-exit1' ? 1 : 255));
  } else if (commandString.includes('remote-result')) {
    exitNow(1);
  } else {
    const ticketId = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.exec(commandString)[0];
    const reply = { protocol: 1, ticketId, action: { 'drop-withdrawn': 'withdrawn', 'drop-gone': 'no-such-ticket' }[destination] ?? 'not-queued' };
    if (destination === 'drop-gone') reply.tombstoned = true;
    process.stdout.write(\`\${JSON.stringify(reply)}\\n\`);
    exitNow(0);
  }
} else if (destination === 'no-read' && commandString.includes('remote-exec')) {
  // BRAIN-642: a runner session that never reads stdin, so the client's pipeline stalls mid-transfer under backpressure.
  setInterval(() => {}, 1000);
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
  if ((destination === 'tee' || destination === 'strict-legacy') && commandString.includes('remote-exec')) {
    const chunks = [];
    let flushed = false;
    const flush = () => {
      if (flushed) return;
      flushed = true;
      const bytes = Buffer.concat(chunks);
      fs.writeFileSync(${JSON.stringify(teePath)}, bytes);
      if (destination !== 'strict-legacy') return;
      const violations = [];
      if (commandString.includes('--transfer')) violations.push('--transfer flag in argv');
      if (bytes.length > 0 && bytes[0] !== 0x7b) violations.push('first stdin byte is not {');
      else if (bytes.length > 0) {
        // header line, then per frame: a line, then size body bytes for a file; stop at {end:true}
        let pos = 0;
        const nextLine = () => {
          const nl = bytes.indexOf(10, pos);
          if (nl === -1) return null;
          const line = bytes.subarray(pos, nl).toString('utf8');
          pos = nl + 1;
          return line;
        };
        try {
          const header = JSON.parse(nextLine());
          if ('transfer' in header) violations.push('transfer key in header');
          for (let line = nextLine(); line !== null; line = nextLine()) {
            const frame = JSON.parse(line);
            if (frame.end === true) break;
            if ('body' in frame) violations.push('body key in frame ' + frame.path);
            if (frame.type === 'file') pos += frame.size;
          }
        } catch (err) {
          violations.push('unparsable legacy stream: ' + err.message);
        }
      }
      for (const v of violations) fs.appendFileSync(${JSON.stringify(violationsPath)}, v + '\\n');
    };
    process.stdin.on('data', (c) => chunks.push(c));
    process.stdin.on('end', flush);
    child.on('close', flush);
  }
  if (destination === 'corrupt-gzip' && commandString.includes('remote-exec')) {
    // BRAIN-642: a transport bug that damages the first byte, so the runner never gets a usable header.
    let first = true;
    process.stdin.on('data', (c) => {
      if (first) { first = false; c = Buffer.from(c); c[0] ^= 0xff; }
      child.stdin.write(c);
    });
    process.stdin.on('end', () => child.stdin.end());
  } else {
    process.stdin.pipe(child.stdin);
  }
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
  return { binDir, sshBin, probeLogPath, runnerHome, runnerState, teePath, violationsPath, haveLogPath, execCmdsPath };
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
export function setup({ ssh = 'normal', cpuAdmissionPercent, weight = 1, cpuCores, remoteQueueTimeoutMs, remoteResultWaitMs, globalExtra = {} } = {}) {
  const { binDir, probeLogPath, runnerHome, runnerState, teePath, violationsPath, haveLogPath, execCmdsPath } = makeFakeSshBin();
  const runnerRoot = tmpDir('remote-dispatch-runner-root');
  const { home, state, env } = freshEnv(FAST_WAITS);
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
  Object.assign(cfg, globalExtra);
  writeGlobalConfig(home, cfg);
  writeGlobalConfig(runnerHome, { sampleMs: 50, capacity: 4 });
  const repoDir = tmpDir('remote-dispatch-repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight, ...(cpuCores !== undefined ? { cpuCores } : {}), remote: true, remotePolicy: 'remote-first' } } });
  // remote-first: this harness exercises remote mechanics; speed-aware placement (BRAIN-548) has its own tests
  // and would otherwise keep a ticket local once the lane has local history.
  gitFixture(['init', '-q'], repoDir);
  return { env: { ...env, PATH: `${binDir}${path.delimiter}${env.PATH}` }, home, state, repoDir, runnerRoot, probeLogPath, runnerHome, runnerState, teePath, violationsPath, haveLogPath, execCmdsPath };
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
  return ['node', '-e', body];
}

export async function detachAndWait(args, env, cwd, waitTimeout = '180s') {
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

// ---- shared by tests/remote-rebind*.test.js: a ticket held in the local queue behind a local-only holder ----

/** `lane run` of a command that appends where it ran to `file` (a line per execution), and holds until `release` exists if given. */
export const recordingCmd = (file, release = null) => [
  'node',
  '-e',
  `const fs = require('fs');
fs.appendFileSync(${JSON.stringify(file)}, (process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local') + '\\n');
${release ? `const t = setInterval(() => { if (fs.existsSync(${JSON.stringify(release)})) { clearInterval(t); process.exit(0); } }, 50);` : ''}`,
];

export const linesOf = (file) => (fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split('\n').filter(Boolean) : []);
export const hasLease = (state) => fs.existsSync(paths(state).leases) && fs.readdirSync(paths(state).leases).some((n) => n.endsWith('.json'));

/** A local-only holder of the lane's key, so every later ticket on that lane waits in the local queue. */
export async function holdLaneLocally(ctx) {
  const dir = tmpDir('rebind-hold');
  const release = path.join(dir, 'release');
  const holder = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'needs ssh', '--', ...recordingCmd(path.join(dir, 'ran'), release)], { env: ctx.env, cwd: ctx.repoDir });
  await waitFor(() => hasLease(ctx.state), { timeoutMs: 60_000 });
  return { release: () => fs.writeFileSync(release, ''), holder, done: new Promise((resolve) => holder.on('close', resolve)) };
}

export const detach = async (ctx, cmd, { env = ctx.env, extra = [] } = {}) => {
  const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', ...extra, '--', ...cmd], { env, cwd: ctx.repoDir });
  assert.equal(started.code, 0, started.stderr);
  return started.stdout.trim();
};
export const waitResult = async (ctx, id) => {
  const waited = await laneRun(['wait', id, '--timeout', '60s'], { env: ctx.env });
  assert.equal(waited.code, 0, `stderr: ${waited.stderr}\nadmission log:\n${fs.readFileSync(paths(ctx.state).admissionLog, 'utf8')}`);
  return readJsonSafe(path.join(paths(ctx.state).results, `${id}.json`));
};

// ---- BRAIN-436/520: several named real-broker runners behind one fake ssh ----

export const IDLE = { protocol: 1, protocols: [1, 2], paused: false, queued: 0, running: 0, version: 'test', capacity: {}, capabilities: CAPABILITIES, headroom: { cpuCores: 8, memoryBytes: 1e12 } };

/**
 * Replace the harness's single fake ssh with one that fronts several named runners. Each runner is a real broker with its own
 * state (so `remote-result` on A reports A's real queue). Probes are synthetic, read from `probes.json` on every call (a test may
 * rewrite it, or add `delayMs`). Every call is logged as `<subcommand> <runner>`.
 */
export function installTwoRunnerSsh(binDir, runners) {
  const dir = tmpDir('rebalance-fake');
  const logPath = path.join(dir, 'calls.log');
  const probesPath = path.join(dir, 'probes.json');
  fs.writeFileSync(logPath, '');
  fs.writeFileSync(probesPath, JSON.stringify(Object.fromEntries(Object.keys(runners).map((n) => [n, IDLE]))));
  fs.writeFileSync(path.join(dir, 'runners.json'), JSON.stringify(runners));
  fs.writeFileSync(
    path.join(binDir, 'ssh'),
    `#!/usr/bin/env node
const fs = require('fs');
const { spawn } = require('child_process');
const argv = process.argv.slice(2);
const rest = [];
for (let i = 0; i < argv.length; i += 1) { if (argv[i] === '-o') { i += 1; continue; } rest.push(argv[i]); }
const [dest, cmd] = rest;
const sub = (/remote-(probe|exec|result|cancel|withdraw|artifacts-release|artifacts)/.exec(cmd) || [])[1];
fs.appendFileSync(${JSON.stringify(logPath)}, sub + ' ' + dest + '\\n');
const done = (code) => { fs.appendFileSync(${JSON.stringify(logPath)}, 'end-' + sub + ' ' + dest + ' ' + code + '\\n'); process.exitCode = code; process.exit(code); };
if (sub === 'probe') {
  const probe = JSON.parse(fs.readFileSync(${JSON.stringify(probesPath)}, 'utf8'))[dest];
  if (!probe) done(255);
  setTimeout(() => { process.stdout.write(JSON.stringify(probe) + '\\n'); done(0); }, probe.delayMs || 0);
} else {
  const r = JSON.parse(fs.readFileSync(${JSON.stringify(path.join(dir, 'runners.json'))}, 'utf8'))[dest];
  if (!r) done(255);
  const flag = ${JSON.stringify(path.join(dir, 'withdrew-'))} + dest;
  if (sub === 'exec' && r.execFails) done(255);
  if (sub === 'cancel' && r.cancelFails) done(1);
  if (sub === 'result' && r.resultDownAfterWithdraw && fs.existsSync(flag)) done(1);
  if (sub === 'withdraw' && r.withdrawReplyLost) fs.writeFileSync(flag, '1');
  if (sub === 'result' && r.resultPatch) {
    // a test-only rewrite of the runner's real record (a null value deletes the key), e.g. a record with no \`reason\` or a wrong binding
    const patched = spawn('sh', ['-c', cmd], { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, LANE_FAKE_RUNNER: dest, LANE_BROKER_HOME: r.home, LANE_BROKER_STATE: r.state } });
    let out = '';
    patched.stdout.on('data', (d) => { out += d; });
    patched.stderr.pipe(process.stderr);
    patched.on('close', (code) => {
      try {
        const rec = JSON.parse(out.trim());
        if (!rec.missing) for (const [k, v] of Object.entries(r.resultPatch)) { if (v === null) delete rec[k]; else rec[k] = v; }
        process.stdout.write(JSON.stringify(rec) + '\\n');
      } catch { process.stdout.write(out); }
      done(code == null ? 1 : code);
    });
    return;
  }
  const lost = sub === 'withdraw' && r.withdrawReplyLost;
  const child = spawn('sh', ['-c', cmd], { stdio: ['pipe', lost ? 'ignore' : 'pipe', 'pipe'], env: { ...process.env, LANE_FAKE_RUNNER: dest, LANE_BROKER_HOME: r.home, LANE_BROKER_STATE: r.state } });
  process.stdin.pipe(child.stdin);
  if (!lost) child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);
  child.on('close', (code) => done(lost ? 1 : code == null ? 1 : code));
  child.on('error', () => done(1));
}
`,
  );
  fs.chmodSync(path.join(binDir, 'ssh'), 0o755);
  return {
    calls: () => fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean),
    count: (entry) => fs.readFileSync(logPath, 'utf8').split('\n').filter((l) => l === entry).length,
    probesPath,
    setProbe: (name, probe) => {
      const all = JSON.parse(fs.readFileSync(probesPath, 'utf8'));
      all[name] = probe;
      fs.writeFileSync(probesPath, JSON.stringify(all));
    },
  };
}

/** Runners `names` (each a real broker) under the harness `setup()`; every runner in `paused` keeps any ticket queued. */
export function rebalanceSetup({ names = ['a', 'b'], paused = ['a'], rebalance = {}, runnerOpts = {}, configOpts = {}, localPaused = false } = {}) {
  const base = setup();
  const binDir = base.env.PATH.split(path.delimiter)[0];
  const runners = {};
  const globalRunners = [];
  for (const name of names) {
    const home = tmpDir(`rb-${name}-home`);
    const state = tmpDir(`rb-${name}-state`);
    writeGlobalConfig(home, { sampleMs: 50, capacity: 4 });
    if (paused.includes(name)) fs.writeFileSync(path.join(state, 'PAUSE'), 'kept queued for the test');
    runners[name] = { home, state, pause: path.join(state, 'PAUSE'), ...(runnerOpts[name] ?? {}) };
    globalRunners.push({ name, ssh: name, shell: 'sh -c', root: tmpDir(`rb-${name}-root`), ...(configOpts[name] ?? {}) });
  }
  const fake = installTwoRunnerSsh(binDir, runners);
  writeGlobalConfig(base.home, {
    version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100,
    runners: globalRunners,
    remoteRebalanceMinQueuedMs: 400,
    remoteRebalanceIntervalMs: 150,
    remoteRebalanceCooldownMs: 0,
    remoteRebalanceMaxMoves: 1,
    ...rebalance,
  });
  if (localPaused) fs.writeFileSync(path.join(base.state, 'PAUSE'), 'keep the local queue closed');
  const runsFile = path.join(tmpDir('rb-runs'), 'runs');
  const cmd = ['node', '-e', `require('fs').appendFileSync(${JSON.stringify(runsFile)}, process.env.LANE_FAKE_RUNNER + '\\n')`];
  const cmdArgv = cmd;
  const runs = () => (fs.existsSync(runsFile) ? fs.readFileSync(runsFile, 'utf8').split('\n').filter(Boolean) : []);
  const unpause = (name) => fs.rmSync(runners[name].pause, { force: true });
  const start = async () => {
    const started = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...cmd], { env: base.env, cwd: base.repoDir });
    assert.equal(started.code, 0, started.stderr);
    return started.stdout.trim();
  };
  const finish = (id, timeout = '60s') => laneRun(['wait', id, '--timeout', timeout], { env: base.env });
  return { ...base, fake, runs, unpause, start, finish, runners, globalRunners, binDir, cmdArgv };
}

export const T = { timeout: 180_000 };

/**
 * Wait until the exec on runner `name` has started AND enqueued its ticket (recorded `remote-id`). The ssh call starting is not the
 * ticket existing: runner a can only confirm a cancel once its exec has enqueued the ticket, and under load that lagged the
 * cancel, so `remote-cancel` answered "not confirmed on a" (a race in these tests, not in the broker).
 */
export async function execArrived(ctx, name) {
  await waitFor(() => ctx.fake.count(`exec ${name}`) === 1, { timeoutMs: 30_000 });
  const tickets = path.join(ctx.globalRunners.find((r) => r.name === name).root, 'tickets');
  await waitFor(() => fs.existsSync(tickets) && fs.readdirSync(tickets).some((t) => fs.existsSync(path.join(tickets, t, 'remote-id'))), { timeoutMs: 60_000 });
}
