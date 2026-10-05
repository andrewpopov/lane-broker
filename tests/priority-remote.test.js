import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { laneRun, writeGlobalConfig, waitFor, BIN, freshEnv } from './helpers.js';
import { setup, tmpDir, markerCmd } from './remote-harness.js';
import { paths, bootId } from '../src/state.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { listQueue } from '../src/scheduler.js';
import { encodeSnapshot } from '../src/remote-stream.js';
import { remotePriorityFrom, sanitizeAccruedMs, PRIORITY_CAPABILITY } from '../src/priority.js';

/**
 * BRAIN-380 slice 4, §6: priority across the remote hop. The submitter sends the tier it asked for and the wait it
 * has accrued on its OWN priority clock; the runner anchors that wait on ITS clock and applies ITS cap. Runs the real
 * `lane run` CLI over the fake-ssh harness; `ssh` here is a thin wrapper that (a) delays the probe so the ticket
 * visibly accrues wait before dispatch, (b) records the exec header, and (c) gives the runner a skewed clock.
 */

const MIN = 60_000;
const CLOCK_STUB = new URL('./fixtures/clock-offset.mjs', import.meta.url).href;

function wrapperSsh({ realSsh, captureFile, probeDelayMs, runnerClockOffsetMs }) {
  const dir = tmpDir('prio-ssh-wrapper');
  const file = path.join(dir, 'ssh');
  fs.writeFileSync(
    file,
    `#!/usr/bin/env node
const fs = require('fs');
const { spawn } = require('child_process');
const argv = process.argv.slice(2);
const cmd = argv[argv.length - 1] || '';
const run = (extraEnv, stdinBuf) =>
  new Promise((resolve) => {
    const child = spawn(${JSON.stringify(realSsh)}, argv, { stdio: [stdinBuf ? 'pipe' : 'inherit', 'inherit', 'inherit'], env: { ...process.env, ...extraEnv } });
    if (stdinBuf) child.stdin.end(stdinBuf);
    child.on('close', (code) => resolve(code == null ? 1 : code));
  });
(async () => {
  let code;
  if (cmd.includes('remote-probe') && ${probeDelayMs} > 0) await new Promise((r) => setTimeout(r, ${probeDelayMs}));
  if (cmd.includes('remote-exec')) {
    const chunks = [];
    for await (const c of process.stdin) chunks.push(c);
    const buf = Buffer.concat(chunks);
    fs.writeFileSync(${JSON.stringify(captureFile)}, buf.subarray(0, buf.indexOf(10)));
    code = await run({ NODE_OPTIONS: '--import=${CLOCK_STUB}', LANE_TEST_CLOCK_OFFSET_MS: '${runnerClockOffsetMs}' }, buf);
  } else {
    code = await run({});
  }
  process.exit(code);
})();
`,
  );
  fs.chmodSync(file, 0o755);
  return dir;
}

function remoteSetup({ ssh = 'normal', probeDelayMs = 0, runnerClockOffsetMs = 0, runnerConfig = {} } = {}) {
  const ctx = setup({ ssh });
  const realSsh = ctx.env.PATH.split(path.delimiter)[0] + '/ssh';
  const captureFile = path.join(tmpDir('prio-capture'), 'exec-header.json');
  const wrapperDir = wrapperSsh({ realSsh, captureFile, probeDelayMs, runnerClockOffsetMs });
  writeGlobalConfig(ctx.runnerHome, { sampleMs: 50, capacity: 4, priorityAgingMs: 60_000, ...runnerConfig });
  const env = { ...ctx.env, PATH: `${wrapperDir}${path.delimiter}${ctx.env.PATH}` };
  const header = () => JSON.parse(fs.readFileSync(captureFile, 'utf8'));
  const runnerHistory = () =>
    fs.existsSync(paths(ctx.runnerState).history)
      ? fs.readFileSync(paths(ctx.runnerState).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l))
      : [];
  return { ...ctx, env, header, runnerHistory };
}

const noopCmd = [process.execPath, '-e', '0'];

// ---- capability and the pure validators ----

test('remote-probe advertises priority/1 next to elastic-claims/1', () => {
  const { env } = freshEnv();
  const res = spawnSync(process.execPath, [BIN, 'remote-probe'], { env, encoding: 'utf8' });
  assert.equal(res.status, 0, res.stderr);
  const { capabilities } = JSON.parse(res.stdout);
  assert.ok(capabilities.includes(PRIORITY_CAPABILITY));
  assert.ok(capabilities.includes('elastic-claims/1'));
  assert.equal(PRIORITY_CAPABILITY, 'priority/1');
});

test('validators: an unknown tier is medium; accrued wait must be an integer in [0, 24h], otherwise 0', () => {
  for (const tier of ['high', 'medium', 'low']) assert.equal(remotePriorityFrom({ priorityRequested: tier }).priority, tier);
  for (const bad of ['urgent', 'HIGH', '', null, undefined, 2, {}, ['high']]) assert.equal(remotePriorityFrom({ priorityRequested: bad }).priority, 'medium', String(bad));
  assert.equal(remotePriorityFrom({}).priority, 'medium');
  assert.equal(remotePriorityFrom(undefined).accruedMs, 0);
  for (const ok of [0, 1, 90_000, 86_400_000]) assert.equal(sanitizeAccruedMs(ok), ok);
  for (const bad of [-1, 1.5, 86_400_001, Infinity, -Infinity, NaN, '60000', null, undefined, {}, [5]]) assert.equal(sanitizeAccruedMs(bad), 0, String(bad));
});

// ---- the submitter: header fields, and anchoring on the runner under a skewed clock ----

for (const skewMs of [10 * MIN, -10 * MIN]) {
  const direction = skewMs > 0 ? 'ahead of' : 'behind';
  test(`remote: with the runner's clock ${direction} the submitter's by 10 minutes, the runner anchors the accrued wait on its own clock`, async () => {
    const ctx = remoteSetup({ probeDelayMs: 2500, runnerClockOffsetMs: skewMs });
    const result = await laneRun(['run', '--lane', 'default', '--priority', 'low', '--', ...noopCmd], { env: ctx.env, cwd: ctx.repoDir });
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stderr, /lane: running on skybox/);

    const header = ctx.header();
    assert.equal(header.priorityRequested, 'low', 'the tier BEFORE any cap');
    assert.ok(Number.isInteger(header.priorityAccruedMs), 'an integer');
    assert.ok(header.priorityAccruedMs >= 2000 && header.priorityAccruedMs < 30_000, `the wait the submitter saw (probe delay 2.5s): ${header.priorityAccruedMs}`);
    assert.equal(header.prioOriginAt, undefined, 'no raw submitter timestamp rides in the header');

    const [row] = ctx.runnerHistory();
    assert.deepEqual([row.priorityRequested, row.priorityAdmitted, row.priorityDemoted], ['low', 'low', false]);
    // low: score = 2 * waited / 120000 (aging 60s, age cap 120s). The submitter's 2.5s+ counts, a 10 minute skew must not.
    assert.ok(row.scoreAtStart > 0.03, `the accrued wait was applied: ${row.scoreAtStart}`);
    assert.ok(row.scoreAtStart < 0.8, `the skew was not: ${row.scoreAtStart}`);
  });
}

test('remote: the runner applies ITS OWN cap (the submitter would have admitted the high)', async () => {
  const ctx = remoteSetup({ runnerConfig: { maxQueuedHighPerRepo: 0 } });
  const result = await laneRun(['run', '--lane', 'default', '--priority', 'high', '--', ...noopCmd], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(result.code, 0, result.stderr);
  assert.equal(ctx.header().priorityRequested, 'high', 'the header carries the tier before any cap');
  const [row] = ctx.runnerHistory();
  assert.deepEqual([row.priorityRequested, row.priorityAdmitted, row.priorityDemoted], ['high', 'medium', true]);
});

test('remote: an old runner (no priority/1 capability) is still selected, still sent the fields, and the run is unchanged', async () => {
  const ctx = remoteSetup({ ssh: 'old-runner' });
  const marker = path.join(tmpDir('marker'), 'where');
  const result = await laneRun(['run', '--lane', 'default', '--', ...markerCmd(marker, 0)], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(result.code, 0, result.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote', 'runner selection does not depend on the capability');
  const header = ctx.header();
  assert.equal(header.priorityRequested, 'medium');
  assert.ok(Number.isInteger(header.priorityAccruedMs) && header.priorityAccruedMs >= 0);
});

// ---- the runner: validation of what arrives, and the runner shell's own env ----

function remoteExec(header, { env, root }) {
  const src = tmpDir('prio-remote-src');
  const body = Buffer.from('hello');
  fs.writeFileSync(path.join(src, 'a.txt'), body);
  const entries = [{ path: 'a.txt', type: 'file', exec: false, size: body.length, sha256: crypto.createHash('sha256').update(body).digest('hex') }];
  const full = { ticketId: crypto.randomUUID(), generation: 0, repoKey: 'prio-remote-repo', lane: 'default', argv: noopCmd, relCwd: '', ...header };
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [BIN, 'remote-exec', '--root', root], { env });
    let err = '';
    child.stderr.on('data', (d) => (err += d));
    encodeSnapshot(src, full, entries).pipe(child.stdin);
    child.on('close', (code) => resolve({ code, err }));
  });
}

function runnerEnvAndRoot(extraEnv = {}) {
  const f = freshEnv(extraEnv);
  writeGlobalConfig(f.home, { sampleMs: 50, priorityAgingMs: 60_000 });
  return { env: f.env, state: f.state, root: tmpDir('prio-remote-root') };
}
const historyRows = (state) => fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l));

test('runner: invalid priority fields fall back to medium and zero accrued wait', async () => {
  const { env, state, root } = runnerEnvAndRoot();
  const run = await remoteExec({ priorityRequested: 'urgent', priorityAccruedMs: 86_400_001 }, { env, root });
  assert.equal(run.code, 0, run.err);
  const [row] = historyRows(state);
  assert.deepEqual([row.priorityRequested, row.priorityAdmitted], ['medium', 'medium']);
  assert.ok(row.scoreAtStart < 1.5, `an out-of-range wait is ignored, not clamped (a 24h wait would score the 2.0 ceiling): ${row.scoreAtStart}`);
});

test('runner: a valid accrued wait is anchored, and the requested tier is honoured', async () => {
  const { env, state, root } = runnerEnvAndRoot();
  const run = await remoteExec({ priorityRequested: 'low', priorityAccruedMs: 60_000 }, { env, root });
  assert.equal(run.code, 0, run.err);
  const [row] = historyRows(state);
  assert.equal(row.priorityAdmitted, 'low');
  assert.ok(row.scoreAtStart >= 1.0 && row.scoreAtStart < 1.5, `a low that arrived with 60s of 120s accrued: ${row.scoreAtStart}`);
});

test('runner: absent fields mean medium, and the runner shell\'s own LANE_BROKER_PRIORITY is ignored', async () => {
  const { env, state, root } = runnerEnvAndRoot({ LANE_BROKER_PRIORITY: 'high' });
  const run = await remoteExec({}, { env, root });
  assert.equal(run.code, 0, run.err);
  const [row] = historyRows(state);
  assert.deepEqual([row.priorityRequested, row.priorityAdmitted, row.priorityDemoted], ['medium', 'medium', false]);
});

// ---- fallback to local: the origin is the local one, nothing added ----

test('fallback to local: the local ticket enqueues with its OWN origin, the remote wait is not added on top', async () => {
  const ctx = remoteSetup({ ssh: 'result-tamper', probeDelayMs: 1500 });
  // hold the LOCAL queue with a full-capacity lease (a pause would also make the probe report the runner paused: the harness shares state)
  writeLease(ctx.state, { id: 'held', key: 'x:held', bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 4, state: LEASE_STATE.RUNNING });
  const started = await laneRun(['run', '--detach', '--lane', 'default', '--priority', 'low', '--', ...noopCmd], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  try {
    const record = await waitFor(() => listQueue(ctx.state).find((t) => t.id === id), { timeoutMs: 60_000, intervalMs: 100 });
    assert.ok(ctx.header().priorityAccruedMs >= 1500, 'the remote attempt did carry the accrued wait');
    assert.ok(Math.abs(record.createdAt - record.prioOriginAt) < 500, `origin ${record.prioOriginAt} is the local creation instant ${record.createdAt}, not backdated by the remote wait`);
  } finally {
    removeLease(ctx.state, 'held');
  }
  const waited = await laneRun(['wait', id, '--timeout', '30s'], { env: ctx.env });
  assert.equal(waited.code, 0, waited.stderr);
});
