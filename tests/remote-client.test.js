import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, waitFor, sleep } from './helpers.js';
import { tmpDir, makeFakeSshBin, makeRunner, clientEnv, makeGitWorktree, makeDispatchArgs } from './remote-harness.js';
import { shellQuote, buildRemoteCommand, selectRunner, dispatchRemote, remoteWithdraw, isGreen, needsProtocol2, classifyRemoteResult } from '../src/remote-client.js';
import { MAX_HEADER_BYTES } from '../src/remote-stream.js';
import { buildManifest } from '../src/remote-manifest.js';

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

test('dispatchRemote: a manifest whose header would exceed MAX_HEADER_BYTES is ineligible before dialing ssh', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  // A destination that would hang forever if anything were actually dialed --
  // proves the precheck ran before spawn, not merely that dispatch failed fast.
  const runner = makeRunner({ ssh: 'must-not-be-dialed', root: tmpDir('remote-exec-root') });

  // Enough manifest entries, with long-enough paths, that the serialized
  // header alone exceeds MAX_HEADER_BYTES -- no real files need to back
  // these entries on disk, since the precheck runs before encodeSnapshot
  // ever reads a file body.
  const sha256 = crypto.createHash('sha256').update('x').digest('hex');
  const entries = [];
  for (let i = 0; i < 130_000; i += 1) {
    entries.push({ path: `f${String(i).padStart(7, '0')}.txt`, type: 'file', exec: false, size: 1, sha256 });
  }

  const before = Date.now();
  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner,
    worktreeRoot: src,
    manifest: { entries },
    sshBin,
    env,
    deadlines: { transferMs: 30_000 },
  });
  const elapsedMs = Date.now() - before;

  assert.equal(result.outcome, 'ineligible');
  assert.match(result.reason, /snapshot header is \d+ bytes, over the runner limit of \d+/);
  const reportedBytes = Number(result.reason.match(/snapshot header is (\d+) bytes/)[1]);
  assert.ok(reportedBytes > MAX_HEADER_BYTES, `expected the reported size to exceed MAX_HEADER_BYTES, got ${reportedBytes}`);
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

// ---- queue timeout (BRAIN-320 S1d, opt-in) ----

/** Fast-poll shadow config for the fake runner's OWN broker (a separate
 *  `LANE_BROKER_HOME`/`LANE_BROKER_STATE` from the client's, see
 *  makeFakeSshBin's own doc comment) -- 5s default sampleMs would make a
 *  small queueTimeoutMs take a full extra poll cycle to observe. */
function fastRunnerConfig(runnerHome) {
  writeGlobalConfig(runnerHome, { sampleMs: 50, capacity: 4 });
}

test('dispatchRemote: a runner-side ticket that never starts (broker paused) is unconfirmed with reason queue-timeout', async () => {
  const { binDir, sshBin, runnerHome, runnerState } = makeFakeSshBin();
  fastRunnerConfig(runnerHome);
  fs.mkdirSync(runnerState, { recursive: true });
  fs.writeFileSync(path.join(runnerState, 'PAUSE'), 'kept busy for the test');
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });

  const result = await dispatchRemote({
    ...makeDispatchArgs(),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    queueTimeoutMs: 300,
    deadlines: { resultMs: 5000, resultAttempts: 5 },
  });

  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /queue-timeout/);
});

test('dispatchRemote: without queueTimeoutMs, a runner-side ticket stuck behind a paused broker just keeps waiting (never expires)', async () => {
  const { binDir, sshBin, runnerHome, runnerState } = makeFakeSshBin();
  fastRunnerConfig(runnerHome);
  fs.mkdirSync(runnerState, { recursive: true });
  fs.writeFileSync(path.join(runnerState, 'PAUSE'), 'kept busy for the test');
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });

  // No queueTimeoutMs passed at all (I6: absent by default). Unpause shortly
  // after dispatch starts, well past where a 300ms timeout would have fired
  // in the sibling test above, and confirm the ticket still completes
  // normally instead of having been expired out from under it.
  setTimeout(() => {
    try {
      fs.unlinkSync(path.join(runnerState, 'PAUSE'));
    } catch {
      // already gone
    }
  }, 600);

  const result = await dispatchRemote({
    ...makeDispatchArgs({ argv: [process.execPath, '-e', 'process.exit(0)'] }),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    deadlines: { resultMs: 5000, resultAttempts: 5 },
  });

  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 0);
});

test('dispatchRemote: a ticket that starts just before its queueTimeoutMs deadline runs to completion (kind completed)', async () => {
  const { binDir, sshBin, runnerHome, runnerState } = makeFakeSshBin();
  fastRunnerConfig(runnerHome);
  fs.mkdirSync(runnerState, { recursive: true });
  const pauseFile = path.join(runnerState, 'PAUSE');
  fs.writeFileSync(pauseFile, 'kept busy for the test');
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });

  // Unpause WELL before the queueTimeoutMs deadline -- the ticket must be
  // admitted and run to completion, never expired, once it started (BRAIN-320
  // 1f: "cancellation stays consistent -- a ticket that started just before
  // the deadline runs to completion").
  setTimeout(() => {
    try {
      fs.unlinkSync(pauseFile);
    } catch {
      // already gone
    }
  }, 100);

  const result = await dispatchRemote({
    ...makeDispatchArgs({ argv: [process.execPath, '-e', 'process.exit(0)'] }),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    queueTimeoutMs: 2000,
    deadlines: { resultMs: 5000, resultAttempts: 5 },
  });

  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 0);
  assert.equal(result.result.kind, 'completed');
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

// ---- BRAIN-319 review finding #3: an encode error must not hang dispatch ----

test('dispatchRemote: a file changed between manifest and encode resolves unconfirmed within a bound (never hangs)', async () => {
  const { binDir, sshBin } = makeFakeSshBin();
  const { env } = clientEnv(binDir);
  const src = makeGitWorktree({ 'a.txt': 'aaaa' });
  const runner = makeRunner({ ssh: 'normal', root: tmpDir('remote-exec-root') });

  const dispatchPromise = dispatchRemote({
    ...makeDispatchArgs(),
    runner,
    worktreeRoot: src,
    sshBin,
    env,
    deadlines: { transferMs: 5000, resultMs: 2000, resultAttempts: 1 },
  });
  // dispatchRemote runs synchronously (buildManifest included) up to its first await, so by
  // the time this statement runs the manifest has already been built from the ORIGINAL
  // content; the encode generator hasn't read the file yet (it only does so once the stream
  // is actually pumped, on a later tick). Mutating here reproduces "changed between manifest
  // and encode" deterministically, without a fragile timing race.
  fs.writeFileSync(path.join(src, 'a.txt'), 'bbbb');

  const result = await Promise.race([
    dispatchPromise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('dispatchRemote hung past 15s bound')), 15_000)),
  ]);

  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /changed content|snapshot stream error/);
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

// ---- BRAIN-320 S1a: needsProtocol2 / runner-fit ----

/** A bespoke fake ssh that answers `remote-probe` (and anything else) with a fixed JSON payload,
 *  same pattern as the "a protocol-2 runner is skipped" test above but parameterized. */
function makeFixedProbeSsh(binDir, name, payload) {
  const sshBin = path.join(binDir, name);
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
process.stdout.write(${JSON.stringify(JSON.stringify(payload))} + '\\n');
process.exit(0);
`,
  );
  fs.chmodSync(sshBin, 0o755);
  return sshBin;
}

test('selectRunner: a runner lacking protocol 2 is skipped for a remoteDeps lane, but used for an optionless lane', async () => {
  const { binDir } = makeFakeSshBin();
  const sshBin = makeFixedProbeSsh(binDir, 'ssh-v1-only', {
    protocol: 1,
    protocols: [1],
    version: '0.0.0',
    paused: false,
    queued: 0,
    running: 0,
  });
  const { env } = clientEnv(binDir);
  const runners = [makeRunner({ ssh: 'normal' })];

  const needsV2 = await selectRunner(runners, { sshBin, env, deadlineMs: 3000, requireProtocol2: true });
  assert.equal(needsV2.runner, null);
  assert.match(needsV2.skipped[0].reason, /protocol 2/);

  const optionless = await selectRunner(runners, { sshBin, env, deadlineMs: 3000, requireProtocol2: false });
  assert.equal(optionless.runner.name, runners[0].name);
});

test('selectRunner: a runner offering protocol 2 is used for a remoteDeps lane', async () => {
  const { binDir } = makeFakeSshBin();
  const sshBin = makeFixedProbeSsh(binDir, 'ssh-v2', {
    protocol: 1,
    protocols: [1, 2],
    version: '0.0.0',
    paused: false,
    queued: 0,
    running: 0,
  });
  const { env } = clientEnv(binDir);
  const runners = [makeRunner({ ssh: 'normal' })];

  const { runner, skipped } = await selectRunner(runners, { sshBin, env, deadlineMs: 3000, requireProtocol2: true });
  assert.equal(runner.name, runners[0].name);
  assert.equal(skipped.length, 0);
});

test('selectRunner: a runner whose capacity can never fit the ticket is skipped and the next runner is used', async () => {
  const { binDir } = makeFakeSshBin();
  const tooSmallPayload = {
    protocol: 1,
    protocols: [1, 2],
    version: '0.0.0',
    paused: false,
    queued: 0,
    running: 0,
    capacity: { weight: 1, cpuCores: 1, memoryBytes: 2_000_000_000, memoryReserveBytes: 0 },
  };
  const bigEnoughPayload = {
    protocol: 1,
    protocols: [1, 2],
    version: '0.0.0',
    paused: false,
    queued: 0,
    running: 0,
    capacity: { weight: 100, cpuCores: 64, memoryBytes: 64_000_000_000, memoryReserveBytes: 0 },
  };
  const smallSsh = makeFixedProbeSsh(binDir, 'ssh-too-small', tooSmallPayload);
  const bigSsh = makeFixedProbeSsh(binDir, 'ssh-big-enough', bigEnoughPayload);
  // selectRunner takes exactly one sshBin per call, so exercise the fit skip
  // against a too-small runner first, then confirm a big-enough runner is
  // accepted with the exact same reservation -- proving the skip is about
  // capacity, not something else about the fixture.
  const { env } = clientEnv(binDir);
  const reservation = { weight: 2, cpuCores: 4, memoryBytes: 8_000_000_000 };

  const tooSmall = await selectRunner([makeRunner({ ssh: 'normal' })], { sshBin: smallSsh, env, deadlineMs: 3000, reservation });
  assert.equal(tooSmall.runner, null);
  assert.match(tooSmall.skipped[0].reason, /capacity/);

  const bigEnough = await selectRunner([makeRunner({ ssh: 'normal' })], { sshBin: bigSsh, env, deadlineMs: 3000, reservation });
  assert.equal(bigEnough.runner.name, 'normal');
});

test('selectRunner: temporary pressure (queued > 0) is unaffected by the fit check -- still skipped by the existing rule', async () => {
  const { binDir } = makeFakeSshBin();
  const sshBin = makeFixedProbeSsh(binDir, 'ssh-queued', {
    protocol: 1,
    protocols: [1, 2],
    version: '0.0.0',
    paused: false,
    queued: 1,
    running: 1,
    capacity: { weight: 100, cpuCores: 64, memoryBytes: 64_000_000_000, memoryReserveBytes: 0 },
  });
  const { env } = clientEnv(binDir);
  const { runner, skipped } = await selectRunner([makeRunner({ ssh: 'normal' })], {
    sshBin,
    env,
    deadlineMs: 3000,
    reservation: { weight: 1, cpuCores: 1, memoryBytes: 1_000_000_000 },
  });
  assert.equal(runner, null);
  assert.match(skipped[0].reason, /queued/);
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

test('isGreen: true only for a bound, completed, unsignalled, exact zero exit (protocol 1)', () => {
  const expected = { protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1' };
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

// ---- needsProtocol2 ----

test('needsProtocol2: true iff remoteDeps or remoteSetup is a non-empty array', () => {
  assert.equal(needsProtocol2({}), false);
  assert.equal(needsProtocol2({ remoteDeps: null, remoteSetup: null }), false);
  assert.equal(needsProtocol2({ remoteDeps: [], remoteSetup: [] }), false, 'empty arrays do not count');
  assert.equal(needsProtocol2({ remoteDeps: ['.'] }), true);
  assert.equal(needsProtocol2({ remoteSetup: [['npm', 'run', 'build']] }), true);
  assert.equal(needsProtocol2(undefined), false);
});

// ---- classifyRemoteResult (BRAIN-320 S1c: the single classifier) ----

test('classifyRemoteResult: protocol 2, exit 0/no signal/phase command -> confirmed green', () => {
  const expected = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  const record = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 0, signal: null, phase: 'command' };
  const result = classifyRemoteResult(record, expected);
  assert.deepEqual(result, { outcome: 'confirmed', result: record, exitCode: 0, phase: 'command', green: true });
  assert.equal(isGreen(record, expected), true);
});

// Canary target #1: "a completed/0 result with phase deps is never green" (spec 1h).
test('classifyRemoteResult: canary -- completed/exit 0 with phase deps is never green', () => {
  const expected = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  const record = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 0, signal: null, phase: 'deps' };
  const result = classifyRemoteResult(record, expected);
  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /exit 0 outside phase command/);
  assert.equal(isGreen(record, expected), false);
});

test('classifyRemoteResult: protocol 2, nonzero exit with phase deps or setup -> confirmed red, never falls back', () => {
  const expected = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  for (const phase of ['deps', 'setup']) {
    const record = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 1, signal: null, phase };
    const result = classifyRemoteResult(record, expected);
    assert.deepEqual(result, { outcome: 'confirmed', result: record, exitCode: 1, phase, green: false });
  }
});

test('classifyRemoteResult: protocol 2, nonzero exit with phase command -> confirmed red (as today)', () => {
  const expected = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  const record = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 3, signal: null, phase: 'command' };
  const result = classifyRemoteResult(record, expected);
  assert.deepEqual(result, { outcome: 'confirmed', result: record, exitCode: 3, phase: 'command', green: false });
});

test('classifyRemoteResult: protocol 2, phase missing/unrecognized -> unconfirmed, never green', () => {
  const expected = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  for (const phase of [undefined, null, 'bogus']) {
    const record = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 0, signal: null, phase };
    const result = classifyRemoteResult(record, expected);
    assert.equal(result.outcome, 'unconfirmed');
    assert.match(result.reason, /unrecognized phase/);
  }
});

test('classifyRemoteResult: protocol 2, a signal reported alongside exit 0 -> unconfirmed, never green', () => {
  const expected = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  const record = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 0, signal: 'SIGTERM', phase: 'command' };
  const result = classifyRemoteResult(record, expected);
  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /signal alongside exit 0/);
});

test('classifyRemoteResult: a protocol-2 record does not bind against a protocol-1 expectation, and vice versa', () => {
  const expectedV2 = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  const recordV1 = { protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 0, signal: null };
  assert.equal(classifyRemoteResult(recordV1, expectedV2).outcome, 'unconfirmed');

  const expectedV1 = { protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  const recordV2 = { protocol: 2, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'completed', exit: 0, signal: null, phase: 'command' };
  assert.equal(classifyRemoteResult(recordV2, expectedV1).outcome, 'unconfirmed');
});

test('classifyRemoteResult: protocol 1 classification is unchanged (refused/cancelled/completed)', () => {
  const expected = { protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1' };
  assert.deepEqual(
    classifyRemoteResult({ protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'refused', exit: 64 }, expected).exitCode,
    64,
  );
  assert.equal(
    classifyRemoteResult({ protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'cancelled' }, expected).exitCode,
    130,
  );
  assert.equal(
    classifyRemoteResult({ protocol: 1, ticketId: 't1', generation: 0, manifestHash: 'h1', kind: 'unfinished' }, expected).outcome,
    'unconfirmed',
  );
});

// ---- ssh drops mid-job: wait for the runner's result (BRAIN-339) ----

/** A scripted ssh: `remote-exec` exits at once (the dropped session), `remote-result` replays
 *  `results` in order (the last one repeats), `remote-cancel` is logged. Returns the ssh path
 *  plus a log of the remote subcommands seen. */
function makeScriptedSsh(results) {
  const dir = tmpDir('scripted-ssh');
  const logPath = path.join(dir, 'calls.log');
  const scriptPath = path.join(dir, 'script.json');
  const countPath = path.join(dir, 'result-count');
  const cancelReplyPath = path.join(dir, 'cancel-reply.json');
  const afterCancelPath = path.join(dir, 'after-cancel.json');
  fs.writeFileSync(cancelReplyPath, JSON.stringify({ protocol: 1, cancelConfirmed: true }));
  fs.writeFileSync(scriptPath, JSON.stringify(results));
  fs.writeFileSync(countPath, '0');
  const sshBin = path.join(dir, 'ssh');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
const fs = require('fs');
const cmd = process.argv.slice(2).filter((a) => !a.startsWith('-o') && a !== '-o').join(' ');
if (cmd.includes('remote-exec')) {
  fs.appendFileSync(${JSON.stringify(logPath)}, 'exec\\n');
  process.stdin.resume();
  process.stdin.on('end', () => process.exit(255));
} else if (cmd.includes('remote-result')) {
  fs.appendFileSync(${JSON.stringify(logPath)}, 'result\\n');
  const script = JSON.parse(fs.readFileSync(${JSON.stringify(scriptPath)}, 'utf8'));
  if (fs.existsSync(${JSON.stringify(afterCancelPath)}) && fs.readFileSync(${JSON.stringify(logPath)}, 'utf8').includes('cancel')) {
    process.stdout.write(fs.readFileSync(${JSON.stringify(afterCancelPath)}, 'utf8') + '\\n');
    process.exit(0);
  }
  const n = Number(fs.readFileSync(${JSON.stringify(countPath)}, 'utf8'));
  fs.writeFileSync(${JSON.stringify(countPath)}, String(n + 1));
  const entry = script[Math.min(n, script.length - 1)];
  if (entry === null) process.exit(1);
  process.stdout.write(JSON.stringify(entry) + '\\n');
} else if (cmd.includes('remote-cancel')) {
  fs.appendFileSync(${JSON.stringify(logPath)}, 'cancel\\n');
  const reply = fs.readFileSync(${JSON.stringify(cancelReplyPath)}, 'utf8');
  if (reply !== 'NONE') process.stdout.write(reply + '\\n');
}
`,
  );
  fs.chmodSync(sshBin, 0o755);
  const calls = () => fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean);
  return { sshBin, calls, scriptPath, cancelReplyPath, afterCancelPath };
}

/** A fake clock: `sleep` advances `now` instead of waiting; `slept` records every wait. */
function makeFakeClock() {
  let t = 1_000_000;
  const slept = [];
  return {
    now: () => t,
    sleep: async (ms) => {
      slept.push(ms);
      t += ms;
    },
    slept,
  };
}

/** A result the client accepts as bound to this dispatch (same ticketId/generation/manifestHash). */
function boundResult(args, { manifestHash }) {
  return { protocol: 1, ticketId: args.ticketId, generation: args.generation, kind: 'completed', exit: 0, signal: null, phase: 'command', manifestHash };
}

async function dispatchWithScript(script, { abortSignal, clock = makeFakeClock(), extra = {}, deadlines, cancelReply, afterCancel, abortWhen } = {}) {
  const scripted = makeScriptedSsh([]);
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const args = makeDispatchArgs();
  const { manifestHash } = buildManifest(src);
  fs.writeFileSync(
    scripted.scriptPath,
    JSON.stringify(script.map((r) => (r === 'RESULT' ? boundResult(args, { manifestHash }) : r))),
  );
  if (cancelReply !== undefined) fs.writeFileSync(scripted.cancelReplyPath, cancelReply === 'NONE' ? 'NONE' : JSON.stringify(cancelReply));
  if (afterCancel) fs.writeFileSync(scripted.afterCancelPath, JSON.stringify(afterCancel === 'RESULT' ? boundResult(args, { manifestHash }) : afterCancel));
  const result = await dispatchRemote({
    ...args,
    runner: makeRunner({ ssh: 'scripted', root: tmpDir('scripted-runner-root') }),
    worktreeRoot: src,
    sshBin: scripted.sshBin,
    env: process.env,
    now: clock.now,
    sleep: clock.sleep,
    abortSignal: abortWhen ? { get aborted() { return fs.existsSync(path.join(path.dirname(scripted.sshBin), 'calls.log')) && abortWhen(scripted.calls()); }, addEventListener() {}, removeEventListener() {} } : abortSignal,
    deadlines: { resultMs: 5000, resultAttempts: 3, ...deadlines },
    ...extra,
  });
  return { result, clock, calls: scripted.calls() };
}

test('dispatchRemote: state running, running, then a result is confirmed with that result (backoff 5s then 10s)', async () => {
  const { result, clock, calls } = await dispatchWithScript([
    { protocol: 1, missing: true, state: 'running' },
    { protocol: 1, missing: true, state: 'running' },
    'RESULT',
  ]);
  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 0);
  assert.equal(result.result.kind, 'completed');
  assert.deepEqual(clock.slept, [5000, 10_000]);
  assert.equal(calls.filter((c) => c === 'result').length, 3);
});

test('dispatchRemote: the poll backoff grows to a 30s cap', async () => {
  const { clock } = await dispatchWithScript(
    [...Array(8).fill({ protocol: 1, missing: true, state: 'queued' }), 'RESULT'],
  );
  assert.deepEqual(clock.slept, [5000, 10_000, 20_000, 30_000, 30_000, 30_000, 30_000, 30_000]);
});

test('dispatchRemote: failed fetches back off between attempts, then running, then a result is confirmed', async () => {
  const { result, clock } = await dispatchWithScript([null, null, { protocol: 1, missing: true, state: 'running' }, 'RESULT']);
  assert.equal(result.outcome, 'confirmed');
  assert.deepEqual(clock.slept, [5000, 10_000, 20_000]);
});

test('dispatchRemote: state gone is unconfirmed after one fetch, without waiting', async () => {
  const { result, clock, calls } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'gone' }]);
  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /^result not available after retries/);
  assert.deepEqual(clock.slept, []);
  assert.equal(calls.filter((c) => c === 'result').length, 1);
});

test('dispatchRemote: an older runner reporting no state gets exactly resultAttempts fetches, no waiting', async () => {
  const { result, clock, calls } = await dispatchWithScript([{ protocol: 1, missing: true }]);
  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /^result not available after retries/);
  assert.deepEqual(clock.slept, []);
  assert.equal(calls.filter((c) => c === 'result').length, 3);
});

test('dispatchRemote: still running past remoteResultWaitMs is unconfirmed with its own reason', async () => {
  const { result, clock } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], {
    extra: { resultWaitMs: 60_000 },
  });
  assert.equal(result.outcome, 'unconfirmed');
  assert.match(result.reason, /still running after waiting 60000ms/);
  assert.ok(clock.slept.reduce((a, b) => a + b, 0) >= 60_000);
});

test('dispatchRemote: result-wait expiry sends remote-cancel before returning unconfirmed, and says so (BRAIN-363)', async () => {
  const { result, calls } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], {
    extra: { resultWaitMs: 60_000 },
  });
  assert.equal(result.outcome, 'unconfirmed');
  assert.ok(calls.includes('cancel'), `expected a remote-cancel call before the outcome returned, saw ${calls.join(',')}`);
  assert.match(result.reason, /still running after waiting 60000ms for its result; remote copy cancelled/);
});

test('dispatchRemote: an unconfirmed remote-cancel (cancelConfirmed false, or an old runner with no reply) must not be re-run locally (BRAIN-363)', async () => {
  for (const cancelReply of [{ protocol: 1, cancelConfirmed: false }, 'NONE']) {
    const { result } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], { extra: { resultWaitMs: 60_000 }, cancelReply });
    assert.equal(result.outcome, 'unconfirmed');
    assert.equal(result.mayStillBeRunning, true, JSON.stringify(cancelReply));
    assert.match(result.reason, /may still be running and was not re-run/);
  }
  const { result } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], { extra: { resultWaitMs: 60_000 } });
  assert.equal(result.mayStillBeRunning, undefined, 'a confirmed cancel is safe to fall back from');
});

test('dispatchRemote: an unconfirmed expiry cancel keeps mayStillBeRunning for an invalid late record (stale generation, manifest mismatch, incomplete) (BRAIN-363)', async () => {
  const stale = { protocol: 1, ticketId: 'someone-else', generation: 999, kind: 'completed', exit: 0, signal: null, phase: 'command', manifestHash: 'nope' };
  for (const afterCancel of [stale, { protocol: 1, kind: 'completed', exit: 0 }]) {
    const { result } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], {
      extra: { resultWaitMs: 60_000 },
      cancelReply: { protocol: 1, cancelConfirmed: false },
      afterCancel,
    });
    assert.equal(result.outcome, 'unconfirmed');
    assert.equal(result.mayStillBeRunning, true, JSON.stringify(afterCancel));
  }
  const confirmedCancel = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], { extra: { resultWaitMs: 60_000 }, afterCancel: stale });
  assert.equal(confirmedCancel.result.outcome, 'unconfirmed');
  assert.equal(confirmedCancel.result.mayStillBeRunning, undefined, 'a confirmed cancel stays safe to fall back from');
});

test('dispatchRemote: an abort during the expiry cancel or the late fetch is cancelled, not unconfirmed (BRAIN-363)', async () => {
  const running = [{ protocol: 1, missing: true, state: 'running' }];
  const duringCancel = await dispatchWithScript(running, { extra: { resultWaitMs: 60_000 }, cancelReply: { protocol: 1, cancelConfirmed: false }, abortWhen: (c) => c.includes('cancel') });
  assert.equal(duringCancel.result.outcome, 'cancelled');
  assert.equal(duringCancel.calls.at(-1), 'cancel', 'no late fetch after the abort');
  const duringFetch = await dispatchWithScript(running, { extra: { resultWaitMs: 60_000 }, cancelReply: { protocol: 1, cancelConfirmed: false }, abortWhen: (c) => c.includes('cancel') && c.at(-1) === 'result' });
  assert.equal(duringFetch.result.outcome, 'cancelled');
});

test('dispatchRemote: a result that lands during the expiry cancel is used, not discarded (BRAIN-363)', async () => {
  const { result, calls } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], {
    extra: { resultWaitMs: 60_000 },
    cancelReply: { protocol: 1, cancelConfirmed: false },
    afterCancel: 'RESULT',
  });
  assert.ok(calls.includes('cancel'));
  assert.equal(result.outcome, 'confirmed');
  assert.equal(result.exitCode, 0);
});

test('dispatchRemote: a state-gone or no-state runner is not sent a remote-cancel (nothing is running)', async () => {
  const { calls } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'gone' }]);
  assert.ok(!calls.includes('cancel'), `unexpected remote-cancel: ${calls.join(',')}`);
});

test('dispatchRemote: abort during the result wait is cancelled and remote-cancel is attempted', async () => {
  const ac = new AbortController();
  const clock = makeFakeClock();
  const realSleep = clock.sleep;
  clock.sleep = async (ms) => {
    await realSleep(ms);
    ac.abort();
  };
  const { result, calls } = await dispatchWithScript([{ protocol: 1, missing: true, state: 'running' }], {
    abortSignal: ac.signal,
    clock,
  });
  assert.equal(result.outcome, 'cancelled');
  assert.ok(calls.includes('cancel'), `expected a remote-cancel call, saw ${calls.join(',')}`);
});

// ---- BRAIN-436: moving a ticket that sits queued; BRAIN-437: a dropped dispatch is never re-run without proof ----

/**
 * A scripted runner for the rebalance tests. `remote-exec` stays open for `execMs` (0: closes as soon as the snapshot is
 * in) unless it is killed; `remote-result` replays `results` (last repeats; an entry may be `{delayMs, record}`; null is a failed
 * fetch); `remote-withdraw` replays `withdraws` the same way (null: a lost reply). Every subcommand is logged.
 */
function makeRebalanceSsh({ results, withdraws = [], execMs = 0 }) {
  const dir = tmpDir('rebalance-ssh');
  const logPath = path.join(dir, 'calls.log');
  const scriptPath = path.join(dir, 'script.json');
  fs.writeFileSync(scriptPath, JSON.stringify({ results, withdraws }));
  fs.writeFileSync(path.join(dir, 'result-count'), '0');
  fs.writeFileSync(path.join(dir, 'withdraw-count'), '0');
  const sshBin = path.join(dir, 'ssh');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
const fs = require('fs');
const cmd = process.argv.slice(2).filter((a) => !a.startsWith('-o') && a !== '-o').join(' ');
const log = (w) => fs.appendFileSync(${JSON.stringify(logPath)}, w + '\\n');
const next = (list, countFile) => {
  const n = Number(fs.readFileSync(countFile, 'utf8'));
  fs.writeFileSync(countFile, String(n + 1));
  return list[Math.min(n, list.length - 1)];
};
const { results, withdraws } = JSON.parse(fs.readFileSync(${JSON.stringify(scriptPath)}, 'utf8'));
const ticketId = (/'([0-9a-f-]{36})'/.exec(cmd) || [])[1];
if (cmd.includes('remote-exec')) {
  log('exec');
  process.stdin.resume();
  process.stdin.on('end', () => setTimeout(() => process.exit(255), ${execMs}));
} else if (cmd.includes('remote-result')) {
  log('result');
  const entry = next(results, ${JSON.stringify(path.join(dir, 'result-count'))});
  if (entry === null) process.exit(1);
  const send = () => process.stdout.write(JSON.stringify(entry.record ?? entry) + '\\n');
  if (entry.delayMs) setTimeout(send, entry.delayMs); else send();
} else if (cmd.includes('remote-withdraw')) {
  log('withdraw');
  const entry = next(withdraws, ${JSON.stringify(path.join(dir, 'withdraw-count'))});
  if (entry === undefined || entry === null) process.exit(1);
  process.stdout.write(JSON.stringify({ protocol: 1, ticketId, action: entry.action }) + '\\n');
} else if (cmd.includes('remote-cancel')) {
  log('cancel');
  process.stdout.write(JSON.stringify({ protocol: 1, cancelConfirmed: true }) + '\\n');
}
`,
  );
  fs.chmodSync(sshBin, 0o755);
  return { sshBin, calls: () => (fs.existsSync(logPath) ? fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean) : []) };
}

const phaseRecord = (phase) => ({ protocol: 1, missing: true, state: phase === 'queued' ? 'queued' : 'running', phase });
const TARGET = { name: 'B', ssh: 'b' };

async function dispatchRebalance({ results, withdraws, execMs, rebalance, canWithdraw = true, clock = makeFakeClock(), abortSignal, abortWhen, src = makeGitWorktree({ 'a.txt': 'hello' }) }) {
  const ssh = makeRebalanceSsh({ results: [], withdraws, execMs });
  const args = makeDispatchArgs();
  const { manifestHash } = buildManifest(src);
  const bound = boundResult(args, { manifestHash });
  const expand = (r) => (r === 'RESULT' ? bound : r === 'WITHDRAWN' ? { ...bound, kind: 'unfinished', reason: 'withdrawn' } : r?.record === 'WITHDRAWN' ? { ...r, record: { ...bound, kind: 'unfinished', reason: 'withdrawn' } } : r);
  fs.writeFileSync(path.join(path.dirname(ssh.sshBin), 'script.json'), JSON.stringify({ results: results.map(expand), withdraws: withdraws ?? [] }));
  const result = await dispatchRemote({
    ...args,
    runner: makeRunner({ ssh: 'scripted', root: tmpDir('scripted-runner-root') }),
    worktreeRoot: src,
    sshBin: ssh.sshBin,
    env: process.env,
    now: clock.now,
    sleep: clock.sleep,
    abortSignal: abortWhen ? { get aborted() { return abortWhen(ssh.calls()); }, addEventListener() {}, removeEventListener() {} } : abortSignal,
    canWithdraw,
    deadlines: { resultMs: 5000, resultAttempts: 3 },
    rebalance: rebalance && { minQueuedMs: 30_000, intervalMs: 10_000, pickTarget: () => TARGET, ...rebalance },
  });
  return { result, calls: ssh.calls(), clock };
}

const queued = phaseRecord('queued');
const preparing = phaseRecord('preparing');

test('rebalance: a ticket queued past minQueuedMs with a target on offer is withdrawn and moved, never started', { timeout: 60_000 }, async () => {
  const { result, calls } = await dispatchRebalance({ results: [preparing, queued], withdraws: [{ action: 'withdrawn' }], execMs: 20_000, rebalance: {} });
  assert.equal(result.outcome, 'moved', JSON.stringify(result));
  assert.equal(result.neverStarted, true);
  assert.equal(result.to, TARGET);
  assert.equal(result.from.name, 'scripted');
  assert.ok(result.queuedMs >= 30_000, `queuedMs ${result.queuedMs}`);
  assert.equal(calls.filter((c) => c === 'withdraw').length, 1);
  assert.ok(!calls.includes('cancel'), 'a move never uses remote-cancel');
});

test('rebalance: an admitted ticket is never moved, and never withdrawn', { timeout: 60_000 }, async () => {
  const { result, calls } = await dispatchRebalance({
    results: [queued, phaseRecord('admitted'), queued, queued, queued, queued, queued, queued, 'RESULT'].map((r, i, all) => (i === all.length - 1 ? 'RESULT' : r)),
    withdraws: [{ action: 'withdrawn' }],
    execMs: 1500,
    rebalance: { minQueuedMs: 5_000 },
  });
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.ok(!calls.includes('withdraw'), `unexpected withdraw: ${calls.join(',')}`);
});

test('rebalance: preparing then a long queued still moves (the watcher does not stop at preparing)', { timeout: 60_000 }, async () => {
  const { result, calls } = await dispatchRebalance({
    results: [preparing, preparing, preparing, queued],
    withdraws: [{ action: 'withdrawn' }],
    execMs: 20_000,
    rebalance: { minQueuedMs: 20_000 },
  });
  assert.equal(result.outcome, 'moved', JSON.stringify(result));
  assert.ok(calls.filter((c) => c === 'result').length >= 6, calls.join(','));
});

test('rebalance: a withdraw that is not `withdrawn` leaves the ticket on its runner (positive proof only)', { timeout: 60_000 }, async () => {
  for (const action of ['started', 'not-queued', 'cancelled', 'finished', 'no-such-ticket', 'unknown', null]) {
    const { result } = await dispatchRebalance({ results: [queued, queued, queued, queued, queued, 'RESULT'], withdraws: [action === null ? null : { action }], execMs: 1500, rebalance: { minQueuedMs: 5_000 } });
    assert.equal(result.outcome, 'confirmed', `${action}: ${JSON.stringify(result)}`);
  }
});

test('rebalance: a lost withdraw reply is resolved by the runner\'s own bound withdrawn record -- moved, exactly one run', { timeout: 60_000 }, async () => {
  const { result, calls } = await dispatchRebalance({ results: [queued, queued, queued, 'WITHDRAWN'], withdraws: [null], execMs: 20_000, rebalance: { minQueuedMs: 20_000 } });
  assert.equal(result.outcome, 'moved', JSON.stringify(result));
  assert.equal(result.neverStarted, true);
  assert.equal(calls.filter((c) => c === 'exec').length, 1);
  assert.equal(calls.filter((c) => c === 'withdraw').length, 1, 'the lost reply was not retried once the record proved it');
});

test('rebalance: a withdrawn record that does not bind to this ticket is not proof', { timeout: 60_000 }, async () => {
  const stale = { protocol: 1, ticketId: 'someone-else', generation: 999, kind: 'unfinished', reason: 'withdrawn', manifestHash: 'nope' };
  const { result } = await dispatchRebalance({ results: [queued, queued, queued, queued, stale, stale, 'RESULT'], withdraws: [null], execMs: 1500, rebalance: { minQueuedMs: 20_000 } });
  assert.equal(result.outcome, 'unconfirmed', JSON.stringify(result));
  assert.notEqual(result.neverStarted, true);
});

test('rebalance: a record that arrives after the session closed is ignored by the watcher (stale epoch), and the normal path reads its own', { timeout: 60_000 }, async () => {
  // The watcher's fetch is slow and lands after ssh has closed; it carries a withdrawn record that would read as a move.
  const { result } = await dispatchRebalance({
    results: [queued, queued, queued, queued, { delayMs: 2000, record: 'WITHDRAWN' }, 'RESULT'],
    withdraws: [null],
    execMs: 600,
    rebalance: { minQueuedMs: 20_000, intervalMs: 10_000 },
  });
  assert.notEqual(result.outcome, 'moved', JSON.stringify(result));
});

test('rebalance: no withdraw capability means no watcher, no withdraw call, and never a remote-cancel', { timeout: 60_000 }, async () => {
  const { result, calls } = await dispatchRebalance({ results: ['RESULT'], withdraws: [{ action: 'withdrawn' }], execMs: 500, canWithdraw: false, rebalance: {} });
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.deepEqual(calls.filter((c) => c === 'withdraw' || c === 'cancel'), []);
});

test('rebalance: a cancel that lands while the move is being decided stops it before any withdraw', { timeout: 60_000 }, async () => {
  const ac = new AbortController();
  const { result, calls } = await dispatchRebalance({
    results: [queued],
    withdraws: [{ action: 'withdrawn' }],
    execMs: 20_000,
    abortSignal: ac.signal,
    rebalance: { minQueuedMs: 5_000, beforeWithdraw: async () => ac.abort() },
  });
  assert.equal(result.outcome, 'cancelled', JSON.stringify(result));
  assert.ok(!calls.includes('withdraw'), `unexpected withdraw: ${calls.join(',')}`);
});

test('remoteWithdraw: parses a bound reply; any nonzero exit, timeout or garbage is unknown', { timeout: 60_000 }, async () => {
  const id = crypto.randomUUID();
  const withdraw = async (reply) => {
    const ssh = makeRebalanceSsh({ results: [], withdraws: [reply] });
    return remoteWithdraw(makeRunner({ ssh: 'scripted', root: tmpDir('r') }), id, { sshBin: ssh.sshBin, deadlineMs: 5000 });
  };
  assert.deepEqual(await withdraw({ action: 'withdrawn' }), { action: 'withdrawn' });
  assert.deepEqual(await withdraw(null), { action: 'unknown' });
  const slow = makeRebalanceSsh({ results: [] });
  assert.deepEqual(await remoteWithdraw(makeRunner({ ssh: 'x', root: tmpDir('r') }), id, { sshBin: slow.sshBin, deadlineMs: 5000 }), { action: 'unknown' }, 'no reply at all (empty script)');
});

test('BRAIN-437: a dispatch the runner took, ssh dropped and every fetch failed, is mayStillBeRunning (no capability, or withdraw not confirmed)', { timeout: 60_000 }, async () => {
  for (const [canWithdraw, withdraws] of [[false, [{ action: 'withdrawn' }]], [true, [{ action: 'not-queued' }]], [true, [null]], [true, [{ action: 'started' }]]]) {
    const { result, calls } = await dispatchRebalance({ results: [null], withdraws, execMs: 0, canWithdraw, rebalance: null });
    assert.equal(result.outcome, 'unconfirmed', JSON.stringify(result));
    assert.equal(result.mayStillBeRunning, true, `${canWithdraw} ${JSON.stringify(withdraws)}`);
    assert.notEqual(result.neverStarted, true);
    assert.equal(calls.includes('withdraw'), canWithdraw, calls.join(','));
  }
});

test('BRAIN-437: the same dropped dispatch with a `withdrawn` answer is neverStarted -- the local fallback is allowed', { timeout: 60_000 }, async () => {
  const { result } = await dispatchRebalance({ results: [null], withdraws: [{ action: 'withdrawn' }], execMs: 0, rebalance: null });
  assert.equal(result.outcome, 'unconfirmed');
  assert.equal(result.neverStarted, true);
  assert.equal(result.mayStillBeRunning, undefined);
});

test('rebalance: an async pickTarget resolving null never withdraws (the target is awaited)', { timeout: 60_000 }, async () => {
  const { result, calls } = await dispatchRebalance({ results: [queued, queued, queued, queued, queued, 'RESULT'], withdraws: [{ action: 'withdrawn' }], execMs: 1500, rebalance: { minQueuedMs: 5_000, pickTarget: async () => null } });
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.ok(!calls.includes('withdraw'), `unexpected withdraw: ${calls.join(',')}`);
});

test('rebalance: an async pickTarget resolves to the runner itself in `moved.to`, not a Promise', { timeout: 60_000 }, async () => {
  const { result } = await dispatchRebalance({ results: [queued], withdraws: [{ action: 'withdrawn' }], execMs: 20_000, rebalance: { pickTarget: async () => TARGET } });
  assert.equal(result.outcome, 'moved', JSON.stringify(result));
  assert.equal(result.to, TARGET);
});

test('rebalance: pickTarget throwing or beforeWithdraw rejecting is a failed tick -- no move, and dispatchRemote still resolves from the runner', { timeout: 60_000 }, async () => {
  for (const rebalance of [{ pickTarget: () => { throw new Error('boom'); } }, { beforeWithdraw: async () => { throw new Error('boom'); } }]) {
    const { result, calls } = await dispatchRebalance({ results: [queued, queued, queued, queued, queued, 'RESULT'], withdraws: [{ action: 'withdrawn' }], execMs: 1500, rebalance: { minQueuedMs: 5_000, ...rebalance } });
    assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
    assert.ok(!calls.includes('withdraw'), calls.join(','));
    assert.equal(calls.filter((c) => c === 'exec').length, 1);
  }
});

test('BRAIN-437: a cancel arriving during the post-drop withdraw still sends remote-cancel', { timeout: 60_000 }, async () => {
  for (const action of ['started', 'not-queued', null]) {
    const { result, calls } = await dispatchRebalance({ results: [null], withdraws: [action === null ? null : { action }], execMs: 0, rebalance: null, abortWhen: (c) => c.includes('withdraw') });
    assert.equal(result.outcome, 'cancelled', JSON.stringify(result));
    assert.ok(calls.includes('cancel'), `${action}: ${calls.join(',')}`);
  }
});

test('rebalance: a runner that reads `gone` before its publisher record exists is still watched, and then moves', { timeout: 60_000 }, async () => {
  const { result } = await dispatchRebalance({ results: [phaseRecord('gone'), phaseRecord('gone'), queued], withdraws: [{ action: 'withdrawn' }], execMs: 20_000, rebalance: { minQueuedMs: 10_000 } });
  assert.equal(result.outcome, 'moved', JSON.stringify(result));
});

test('rebalance: afterWithdraw hears every withdraw that did not take (unknown included), and none that did', { timeout: 60_000 }, async () => {
  for (const [reply, heard] of [[{ action: 'started' }, ['started']], [null, ['unknown', 'unknown']], [{ action: 'withdrawn' }, []]]) {
    const actions = [];
    await dispatchRebalance({ results: [queued, queued, queued, 'RESULT'], withdraws: [reply], execMs: reply?.action === 'withdrawn' ? 20_000 : 2500, rebalance: { minQueuedMs: 5_000, afterWithdraw: (a) => actions.push(a) } });
    assert.deepEqual(actions.slice(0, heard.length), heard, JSON.stringify(reply));
    if (!heard.length) assert.deepEqual(actions, []);
  }
});
