import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { selectRunner } from '../src/remote-client.js';
import { remoteSelectOptions } from '../src/supervisor.js';
import { loadGlobalConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths, atomicWriteJson, ensureStateDirs } from '../src/state.js';
import { writeLease } from '../src/lease.js';
import { LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { tmpDir } from './remote-harness.js';
import { freshEnv, writeGlobalConfig, BIN, waitFor } from './helpers.js';

/** BRAIN-602: a busy but healthy runner must not read as unavailable. */

const MIN = 60_000;
const WINTOP = { name: 'wintop', ssh: 'wintop-runner' };

// ---- runner side: the probe does not queue behind the broker lock ------------------------------------------------

const STATE_URL = new URL('../src/state.js', import.meta.url).href;

/** A separate process that takes the broker lock and holds it until `<dir>/go` exists. */
async function holdBrokerLock(state, dir) {
  const script = path.join(dir, 'hold.mjs');
  fs.writeFileSync(
    script,
    `import fs from 'node:fs';
import { withLock } from ${JSON.stringify(STATE_URL)};
await withLock(${JSON.stringify(state)}, async () => {
  fs.writeFileSync(${JSON.stringify(path.join(dir, 'ready'))}, '');
  while (!fs.existsSync(${JSON.stringify(path.join(dir, 'go'))})) await new Promise((r) => setTimeout(r, 20));
});
`,
  );
  const child = spawn(process.execPath, [script], { stdio: 'ignore', env: { ...process.env, LANE_BROKER_STATE: state } });
  await waitFor(() => fs.existsSync(path.join(dir, 'ready')), { timeoutMs: 10_000 });
  return {
    release: async () => {
      fs.writeFileSync(path.join(dir, 'go'), '');
      await new Promise((resolve) => (child.exitCode !== null ? resolve() : child.once('exit', resolve)));
    },
  };
}

function runProbe(env) {
  return new Promise((resolve) => {
    const started = Date.now();
    const child = spawn(process.execPath, [BIN, 'remote-probe'], { env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => (stdout += d));
    child.stderr.on('data', (d) => (stderr += d));
    child.on('exit', (code) => resolve({ code, stdout, stderr, ms: Date.now() - started }));
  });
}

test('remote-probe while another process holds the broker lock answers within ~1 s, marked lockless, with the counts from the files', async () => {
  const { home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1 });
  ensureStateDirs(state);
  writeLease(state, { id: 'held', key: 'r:default', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING });
  atomicWriteJson(path.join(paths(state).queue, '000000000001-waiting.json'), { id: 'waiting', key: 'r:default', repoId: 'r', lane: 'default', weight: 1, seq: 1, enqueuedAt: Date.now() });
  const holder = await holdBrokerLock(state, tmpDir('probe-lock-holder'));
  try {
    const res = await runProbe(env);
    assert.equal(res.code, 0, res.stderr);
    assert.ok(res.ms < 2000, `probe took ${res.ms} ms behind a held lock`);
    const probe = JSON.parse(res.stdout);
    assert.equal(probe.stale, true);
    assert.equal(probe.snapshot, 'lockless');
    assert.equal(probe.running, 1);
    assert.equal(probe.queued, 1);
  } finally {
    await holder.release();
  }
  const after = JSON.parse((await runProbe(env)).stdout);
  assert.equal('stale' in after, false, 'an uncontended probe is not marked stale');
  assert.equal('snapshot' in after, false);
});

// ---- submitter side -----------------------------------------------------------------------------------------------

/** A fake ssh: probe call N sleeps `hangMs` when N <= `hangFirst`, otherwise answers like an idle runner. */
function slowSsh({ hangFirst, hangMs }) {
  const dir = tmpDir('slow-ssh');
  const counter = path.join(dir, 'count');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(counter, '');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const fs = require('fs');
fs.appendFileSync(${JSON.stringify(counter)}, 'x');
const n = fs.readFileSync(${JSON.stringify(counter)}, 'utf8').length;
const answer = () => { process.stdout.write(JSON.stringify({ protocol: 1, protocols: [1, 2], paused: false, queued: 0, running: 0, capacity: {}, stale: true, snapshot: 'lockless' }) + '\\n'); process.exit(0); };
if (n <= ${hangFirst}) setTimeout(answer, ${hangMs}); else answer();
`,
  );
  fs.chmodSync(bin, 0o755);
  return { bin, calls: () => fs.readFileSync(counter, 'utf8').length };
}

const beat = (root, lastOkAt, at = Date.now(), unreachable = {}) => atomicWriteJson(paths(root).runnerUnreachable, { ...unreachable, __heartbeat: { at, runners: { wintop: { lastOkAt, lastPingAt: at } } } });
const REFUSED = { repoId: 'r', lane: 'sim', remote: {}, resources: {}, weight: 1, localRefused: true };
const CFG = { ...DEFAULT_GLOBAL_CONFIG, runners: [WINTOP] };

test('remoteProbeDeadlineMs: defaults to 10 s, is validated, and bounds the probe', async () => {
  const { home } = freshEnv();
  const load = (extra) => {
    writeGlobalConfig(home, { version: 1, ...extra });
    return loadGlobalConfig();
  };
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    assert.equal(load({}).remoteProbeDeadlineMs, 10_000);
    assert.equal(load({ remoteProbeDeadlineMs: 20_000 }).remoteProbeDeadlineMs, 20_000);
    assert.equal(load({ remoteProbeDeadlineMs: 2_147_483_647 }).remoteProbeDeadlineMs, 2_147_483_647);
    for (const bad of [0, -1, 1.5, '10000', 2_147_483_648]) assert.throws(() => load({ remoteProbeDeadlineMs: bad }), ConfigError, String(bad));
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prev;
  }
  const root = tmpDir('probe-deadline-opts');
  const options = remoteSelectOptions({ repoId: 'r', lane: 't', remote: {}, resources: {}, weight: 1 }, { ...CFG, remoteProbeDeadlineMs: 321 }, root);
  assert.equal(options.deadlineMs, 321, 'the supervisor path hands the configured deadline to selectRunner');

  const ssh = slowSsh({ hangFirst: 1, hangMs: 1500 });
  const tight = await selectRunner([WINTOP], { sshBin: ssh.bin, deadlineMs: 300 });
  assert.equal(tight.runner, null);
  assert.equal(tight.skipped[0].reason, 'probe timed out');
  const slack = slowSsh({ hangFirst: 1, hangMs: 1500 });
  const loose = await selectRunner([WINTOP], { sshBin: slack.bin, deadlineMs: 5000 });
  assert.equal(loose.runner?.name, 'wintop', 'a longer deadline lets the same slow answer through');
});

test('a localRefused lane retries a timed-out probe once and places on a runner the heartbeat vouches for', async () => {
  const root = tmpDir('probe-retry');
  beat(root, Date.now() - MIN);
  const options = { ...remoteSelectOptions(REFUSED, CFG, root), deadlineMs: 1500 };
  assert.deepEqual(options.retryTimedOut, ['wintop']);
  const ssh = slowSsh({ hangFirst: 1, hangMs: 4000 });
  const res = await selectRunner(CFG.runners, { ...options, sshBin: ssh.bin });
  assert.equal(res.runner?.name, 'wintop');
  assert.equal(ssh.calls(), 2);

  const twice = slowSsh({ hangFirst: 2, hangMs: 4000 });
  const refused = await selectRunner(CFG.runners, { ...options, sshBin: twice.bin });
  assert.equal(refused.runner, null, 'a second timeout is final');
  assert.equal(twice.calls(), 2, 'only one retry');
  assert.equal(refused.skipped[0].reason, 'probe timed out');
});

test('no retry when the heartbeat does not vouch for the runner, or when the lane can run locally', async () => {
  const run = async (enriched, seedBeat) => {
    const root = tmpDir('probe-noretry');
    seedBeat?.(root);
    const options = { ...remoteSelectOptions(enriched, CFG, root), deadlineMs: 1500 };
    const ssh = slowSsh({ hangFirst: 5, hangMs: 4000 });
    const res = await selectRunner(CFG.runners, { ...options, sshBin: ssh.bin });
    return { res, calls: ssh.calls() };
  };
  const stale = await run(REFUSED, (root) => beat(root, Date.now() - 10 * MIN));
  assert.equal(stale.calls, 1, 'last ok ping 10 min ago: the runner is not known to be up');
  assert.equal(stale.res.skipped[0].reason, 'probe timed out');
  const never = await run(REFUSED, (root) => beat(root, null));
  assert.equal(never.calls, 1, 'never answered a ping');
  const noWatcher = await run(REFUSED, null);
  assert.equal(noWatcher.calls, 1, 'no heartbeat at all');
  const deadWatcher = await run(REFUSED, (root) => beat(root, Date.now() - MIN, Date.now() - 10 * MIN));
  assert.equal(deadWatcher.calls, 1, 'a watcher that stopped ticking cannot vouch');
  const failedPing = await run(REFUSED, (root) => beat(root, Date.now() - MIN, Date.now(), { wintop: Date.now() - 30_000 }));
  assert.equal(failedPing.calls, 1, 'a recent ok ping followed by a failed one (clock running) does not grant the retry');
  const local = await run({ ...REFUSED, localRefused: false }, (root) => beat(root, Date.now() - MIN));
  assert.equal(local.calls, 1, 'a lane with a local fallback does not wait for a second probe');
});

test('a runner the heartbeat has marked down past runnerDownAfterMs gets no probe and no retry', async () => {
  const root = tmpDir('probe-down');
  const now = Date.now();
  atomicWriteJson(paths(root).runnerUnreachable, { wintop: now - 10 * MIN, __heartbeat: { at: now, runners: { wintop: { lastOkAt: now - 10 * MIN, lastPingAt: now } } } });
  const options = remoteSelectOptions(REFUSED, CFG, root);
  const ssh = slowSsh({ hangFirst: 5, hangMs: 4000 });
  const res = await selectRunner(CFG.runners, { ...options, sshBin: ssh.bin });
  assert.equal(res.runner, null);
  assert.equal(ssh.calls(), 0);
});

test('with no deadline passed, the default 10 s applies: a probe answering after 8 s is accepted, and the answer may be lockless', async () => {
  const ssh = slowSsh({ hangFirst: 1, hangMs: 8000 });
  const res = await selectRunner([WINTOP], { sshBin: ssh.bin });
  assert.equal(res.runner?.name, 'wintop');
  assert.equal(res.probe.snapshot, 'lockless', 'a lockless answer is a usable answer');
});
