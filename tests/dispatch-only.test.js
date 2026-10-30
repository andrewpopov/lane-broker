import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, writeRepoConfig, gitFixture, laneSpawn, waitFor } from './helpers.js';
import { setup, tmpDir, markerCmd, linesOf, recordingCmd, hasLease } from './remote-harness.js';
import { readJsonSafe, paths } from '../src/state.js';

/**
 * BRAIN-634: `localExecution: "refused"` makes a host dispatch-only. Every test runs a payload that writes a marker file where it
 * executed; on a refused host the marker must never carry 'local'. The fake-ssh harness gives the runner its own (allowed) config.
 */

const refuse = (ctx, extra = {}) => {
  const file = path.join(ctx.home, 'config.json');
  fs.writeFileSync(file, JSON.stringify({ ...JSON.parse(fs.readFileSync(file, 'utf8')), localExecution: 'refused', ...extra }));
};
const run = (ctx, cmd, extra = [], env = ctx.env) => laneRun(['run', '--repo', 'r', '--lane', 'default', ...extra, '--', ...cmd], { env, cwd: ctx.repoDir });
const markerPath = () => path.join(tmpDir('marker'), 'where');
const ranNowhere = (marker) => assert.equal(fs.existsSync(marker), false, 'the payload must never have executed');
const commitEnv = (repoDir) => {
  gitFixture(['add', '-f', '.lane-broker.json'], repoDir);
  fs.writeFileSync(path.join(repoDir, '.env'), 'SECRET=1');
  gitFixture(['add', '-f', '.env'], repoDir);
  gitFixture(['-c', 'user.name=t', '-c', 'user.email=t@example.invalid', 'commit', '-q', '-m', 'x'], repoDir);
};

test('refused host: a remote-eligible lane with a usable runner still runs remotely', async () => {
  const ctx = setup({ ssh: 'normal' });
  refuse(ctx);
  const marker = markerPath();
  const res = await run(ctx, markerCmd(marker, 0));
  assert.equal(res.code, 0, res.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
});

test('(a) a lane NOT marked remote is dispatched anyway; with the runner down it exits 76, never local', async () => {
  const ctx = setup({ ssh: 'normal' });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  refuse(ctx);
  const okMarker = markerPath();
  assert.equal((await run(ctx, markerCmd(okMarker, 0))).code, 0);
  assert.equal(fs.readFileSync(okMarker, 'utf8'), 'remote');

  const down = setup({ ssh: 'down' });
  writeRepoConfig(down.repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  refuse(down);
  const marker = markerPath();
  const res = await run(down, markerCmd(marker, 0));
  assert.equal(res.code, 76, res.stderr);
  ranNowhere(marker);
});

test('(b) remoteQueueTimeoutMs never expires into a local fallback: no queue timeout is sent and the ticket stays on the runner', async () => {
  const ctx = setup({ ssh: 'normal', remoteQueueTimeoutMs: 1000 });
  refuse(ctx);
  fs.mkdirSync(ctx.runnerState, { recursive: true });
  fs.writeFileSync(path.join(ctx.runnerState, 'PAUSE'), 'kept busy for the test');
  const marker = markerPath();
  const started = await run(ctx, markerCmd(marker, 0), ['--detach']);
  assert.equal(started.code, 0, started.stderr);
  const id = started.stdout.trim();
  const queueDir = path.join(ctx.runnerState, 'queue');
  await waitFor(() => fs.existsSync(queueDir) && fs.readdirSync(queueDir).some((f) => f.endsWith('.json')), { timeoutMs: 30_000, intervalMs: 100 });
  const queued = JSON.parse(fs.readFileSync(path.join(queueDir, fs.readdirSync(queueDir).find((f) => f.endsWith('.json'))), 'utf8'));
  assert.equal(queued.startDeadline, undefined);
  await new Promise((r) => setTimeout(r, 2500)); // past the 1 s timeout it would otherwise have had
  assert.equal(fs.existsSync(path.join(paths(ctx.state).results, `${id}.json`)), false, 'still waiting on the runner');
  await laneRun(['cancel', id], { env: ctx.env });
  ranNowhere(marker);
});

test('(c) a snapshot-ineligible tree can never go remote: exit 69 naming localExecution, never local', async () => {
  const ctx = setup({ ssh: 'normal' });
  commitEnv(ctx.repoDir);
  refuse(ctx);
  const marker = markerPath();
  const res = await run(ctx, markerCmd(marker, 0));
  assert.equal(res.code, 69, res.stderr);
  assert.match(res.stderr, /localExecution is refused/);
  ranNowhere(marker);
});

test('(d) a remote run that dies mid-transfer (never started) exits 76, never re-runs locally', async () => {
  const ctx = setup({ ssh: 'die-midstream' });
  refuse(ctx);
  const marker = markerPath();
  const res = await run(ctx, markerCmd(marker, 0));
  assert.equal(res.code, 76, res.stderr);
  assert.match(res.stderr, /localExecution is refused/);
  ranNowhere(marker);
});

test('(e) --local with a reason and LANE_BROKER_LOCAL=1 are ignored: runs remotely, or 76 when no runner', async () => {
  const ok = setup({ ssh: 'normal' });
  refuse(ok);
  const m1 = markerPath();
  const flagged = await run(ok, markerCmd(m1, 0), ['--local', '--local-reason', 'test']);
  assert.equal(flagged.code, 0, flagged.stderr);
  assert.equal(fs.readFileSync(m1, 'utf8'), 'remote');
  assert.match(flagged.stderr, /--local ignored/);

  const down = setup({ ssh: 'down' });
  refuse(down);
  const m2 = markerPath();
  assert.equal((await run(down, markerCmd(m2, 0), ['--local', '--local-reason', 'test'])).code, 76);
  ranNowhere(m2);
  const m3 = markerPath();
  const viaEnv = await run(down, markerCmd(m3, 0), [], { ...down.env, LANE_BROKER_LOCAL: '1', LANE_BROKER_LOCAL_REASON: 'test' });
  assert.equal(viaEnv.code, 76, viaEnv.stderr);
  ranNowhere(m3);
});

test('(f) an undeclared lane allowed by the repo config (plain and template) is still remote-only', async () => {
  const ctx = setup({ ssh: 'down' });
  writeRepoConfig(ctx.repoDir, { version: 1, undeclaredLanes: 'allow', lanes: { default: { weight: 1 } } });
  refuse(ctx);
  const marker = markerPath();
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'adhoc', '--', ...markerCmd(marker, 0)], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(res.code, 76, res.stderr);
  ranNowhere(marker);

  writeRepoConfig(ctx.repoDir, { version: 1, undeclaredLanes: { as: 'default' }, lanes: { default: { weight: 1 } } });
  const m2 = markerPath();
  const res2 = await laneRun(['run', '--repo', 'r', '--lane', 'adhoc2', '--', ...markerCmd(m2, 0)], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(res2.code, 76, res2.stderr);
  ranNowhere(m2);
});

test('(g) a nested run inside a lease never executes locally: exit 69', async () => {
  const ctx = setup({ ssh: 'normal' });
  // a real local lease, taken while the host still allowed local runs
  const dir = tmpDir('nested');
  const release = path.join(dir, 'release');
  const envFile = path.join(dir, 'env.json');
  const holder = laneSpawn(
    ['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'test', '--', 'node', '-e',
      `const fs=require('fs');fs.writeFileSync(${JSON.stringify(envFile)},JSON.stringify({LANE_BROKER_LEASE:process.env.LANE_BROKER_LEASE,LANE_BROKER_KEY:process.env.LANE_BROKER_KEY}));const t=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)})){clearInterval(t);process.exit(0)}},50)`],
    { env: ctx.env, cwd: ctx.repoDir },
  );
  await waitFor(() => fs.existsSync(envFile), { timeoutMs: 60_000, intervalMs: 50 });
  const inherited = JSON.parse(fs.readFileSync(envFile, 'utf8'));
  refuse(ctx);
  const marker = markerPath();
  try {
    const res = await run(ctx, markerCmd(marker, 0), [], { ...ctx.env, ...inherited });
    assert.equal(res.code, 69, res.stderr);
    assert.match(res.stderr, /localExecution is refused/);
    ranNowhere(marker);
  } finally {
    fs.writeFileSync(release, '');
    await new Promise((r) => holder.on('close', r));
  }
});

test('(h) an exclusive lane (flag or config) exits 69, never local', async () => {
  const ctx = setup({ ssh: 'normal' });
  refuse(ctx);
  const m1 = markerPath();
  const flagged = await run(ctx, markerCmd(m1, 0), ['--exclusive', '--exclusive-purpose', 'upgrade', '--exclusive-deadline', '1m']);
  assert.equal(flagged.code, 69, flagged.stderr);
  ranNowhere(m1);
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, exclusive: true, exclusivePurpose: 'upgrade', exclusiveDeadlineMs: 60_000 } } });
  const m2 = markerPath();
  assert.equal((await run(ctx, markerCmd(m2, 0))).code, 69);
  ranNowhere(m2);
});

test('no runners configured: exit 69 (nothing can ever take it)', async () => {
  const ctx = setup({ ssh: 'normal' });
  refuse(ctx, { runners: [] });
  const marker = markerPath();
  const res = await run(ctx, markerCmd(marker, 0));
  assert.equal(res.code, 69, res.stderr);
  assert.match(res.stderr, /no runners configured/);
  ranNowhere(marker);
});

test('restart recovery: tickets queued locally before the host became dispatch-only are refused, not started', async () => {
  const ctx = setup({ ssh: 'down' });
  const dir = tmpDir('restart');
  const release = path.join(dir, 'release');
  const ranFile = path.join(dir, 'ran');
  const holder = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--local', '--local-reason', 'test', '--', ...recordingCmd(ranFile, release)], { env: ctx.env, cwd: ctx.repoDir });
  await waitFor(() => hasLease(ctx.state), { timeoutMs: 60_000, intervalMs: 50 });
  // runner down + allowed: falls back to the local queue behind the holder (remote-eligible), and a forced-local one beside it
  const fallback = await run(ctx, recordingCmd(ranFile), ['--detach']);
  const forced = await run(ctx, recordingCmd(ranFile), ['--detach', '--local', '--local-reason', 'test']);
  const ids = [fallback.stdout.trim(), forced.stdout.trim()];
  await waitFor(() => fs.existsSync(paths(ctx.state).queue) && fs.readdirSync(paths(ctx.state).queue).length >= 2, { timeoutMs: 60_000, intervalMs: 50 });
  refuse(ctx);
  const codes = [];
  try {
    for (const id of ids) codes.push((await laneRun(['wait', id, '--timeout', '20s'], { env: ctx.env })).code);
  } finally {
    fs.writeFileSync(release, '');
    await new Promise((r) => holder.on('close', r));
  }
  assert.deepEqual(codes, [76, 69]);
  assert.deepEqual(linesOf(ranFile), ['local'], 'only the pre-existing holder ever ran');
  for (const id of ids) assert.match(readJsonSafe(path.join(paths(ctx.state).results, `${id}.json`)).error, /localExecution is refused/);
});

test('lane capabilities --json advertises dispatch-only/1 only when set; lane status shows it', async () => {
  const ctx = setup({ ssh: 'normal' });
  const caps = async () => JSON.parse((await laneRun(['capabilities', '--json'], { env: ctx.env })).stdout).capabilities;
  assert.equal((await caps()).includes('dispatch-only/1'), false);
  assert.doesNotMatch((await laneRun(['status'], { env: ctx.env })).stdout, /local execution: refused/);
  refuse(ctx);
  assert.equal((await caps()).includes('dispatch-only/1'), true);
  assert.match((await laneRun(['status'], { env: ctx.env })).stdout, /local execution: refused/);
});

test('an invalid localExecution value is a config error naming the key', async () => {
  const ctx = setup({ ssh: 'normal' });
  refuse(ctx, { localExecution: 'sometimes' });
  const res = await run(ctx, markerCmd(markerPath(), 0));
  assert.match(res.stderr + res.stdout, /"localExecution" must be "allowed" or "refused"/);
});

// ---- Codex review fixes ----

const MARKER_FILE = 'local-execution-refused';

test('P1-1 fail closed: the host marker file keeps an unparseable config refused; so does LANE_BROKER_LOCAL_EXECUTION', async () => {
  const ctx = setup({ ssh: 'normal' });
  fs.writeFileSync(path.join(ctx.home, 'config.json'), '{ this is not json');
  fs.writeFileSync(path.join(ctx.home, MARKER_FILE), '');
  const m1 = markerPath();
  const res = await run(ctx, markerCmd(m1, 0));
  assert.equal(res.code, 69, res.stderr);
  assert.match(res.stderr, /localExecution is refused/);
  ranNowhere(m1);
  const caps = JSON.parse((await laneRun(['capabilities', '--json'], { env: ctx.env })).stdout).capabilities;
  assert.ok(caps.includes('dispatch-only/1'));

  fs.rmSync(path.join(ctx.home, MARKER_FILE));
  fs.rmSync(path.join(ctx.home, 'config.json'));
  const m2 = markerPath();
  const viaEnv = await run(ctx, markerCmd(m2, 0), [], { ...ctx.env, LANE_BROKER_LOCAL_EXECUTION: 'refused' });
  assert.equal(viaEnv.code, 69, viaEnv.stderr);
  ranNowhere(m2);
});

const pipelineDir = (payload) => {
  const dir = tmpDir('pipeline');
  fs.writeFileSync(path.join(dir, 'pipeline.json'), JSON.stringify({ npmCacheDir: path.join(dir, 'c'), npmUserConfig: path.join(dir, 'npmrc'), ...payload }));
  return dir;
};
const writer = (file) => ['node', '-e', `require('fs').writeFileSync(${JSON.stringify(file)}, 'ran')`];

test('P1-2 remote-pipeline refuses every phase (setup and command) on a dispatch-only host, and runs them otherwise', async () => {
  const ctx = setup({ ssh: 'normal' });
  const work = tmpDir('work');
  const control = markerPath();
  const ok = await laneRun(['remote-pipeline', pipelineDir({ workDir: work, argv: writer(control) })], { env: ctx.env });
  assert.equal(ok.code, 0, ok.stderr);
  assert.equal(fs.existsSync(control), true, 'the pipeline runs the payload when local execution is allowed');

  fs.writeFileSync(path.join(ctx.home, MARKER_FILE), '');
  const setupMarker = markerPath();
  const cmdMarker = markerPath();
  const refusedSetup = await laneRun(['remote-pipeline', pipelineDir({ workDir: work, remoteSetup: [writer(setupMarker)], argv: writer(cmdMarker) })], { env: ctx.env });
  assert.equal(refusedSetup.code, 69, refusedSetup.stderr);
  const refusedCmd = await laneRun(['remote-pipeline', pipelineDir({ workDir: work, argv: writer(cmdMarker) })], { env: ctx.env });
  assert.equal(refusedCmd.code, 69, refusedCmd.stderr);
  assert.match(refusedCmd.stderr, /localExecution is refused/);
  ranNowhere(setupMarker);
  ranNowhere(cmdMarker);
});

test('P1-3 a flip to refused between admission and spawn stops the spawn and releases the lease', async () => {
  const ctx = setup({ ssh: 'normal' });
  const dir = path.dirname(ctx.state);
  const ready = path.join(dir, 'p13-ready');
  const go = path.join(dir, 'p13-go');
  const env = { ...ctx.env, LANE_BROKER_TEST_HOLD_AT: 'local-admitted', LANE_BROKER_TEST_HOLD_READY: ready, LANE_BROKER_TEST_HOLD_GO: go };
  const marker = markerPath();
  const pending = run(ctx, markerCmd(marker, 0), ['--local', '--local-reason', 'test'], env);
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 30_000, intervalMs: 50 });
  fs.writeFileSync(path.join(ctx.home, MARKER_FILE), '');
  fs.writeFileSync(go, 'x');
  const res = await pending;
  assert.equal(res.code, 69, res.stderr);
  ranNowhere(marker);
  assert.equal(hasLease(ctx.state), false, 'lease released');
});

test('P2-4/P2-5 a refused runner intake never redispatches; a permanent rejection is 69 on a refused client, not 76', async () => {
  const ctx = setup({ ssh: 'normal' });
  refuse(ctx);
  fs.writeFileSync(path.join(ctx.runnerHome, MARKER_FILE), '');
  const marker = markerPath();
  const res = await run(ctx, markerCmd(marker, 0));
  assert.equal(res.code, 69, res.stderr);
  ranNowhere(marker);
});

test('P2-6 every runner too small for the ticket for good: 69, not a retryable 76', async () => {
  const ctx = setup({ ssh: 'normal', weight: 8 });
  refuse(ctx, { maxLaneWeight: 16 });
  const marker = markerPath();
  const res = await run(ctx, markerCmd(marker, 0));
  assert.equal(res.code, 69, res.stderr);
  ranNowhere(marker);
});

test('BRAIN-721: a dispatch-only host still refuses a host-only path, even with --local, and offers no --local option', async () => {
  const ctx = setup({ ssh: 'normal' });
  refuse(ctx);
  const hostFile = path.join(tmpDir('hostonly'), 'x.sh');
  fs.writeFileSync(hostFile, 'true');
  for (const extra of [[], ['--local', '--local-reason', 'test']]) {
    const res = await run(ctx, ['sh', hostFile], extra);
    assert.equal(res.code, 64, res.stderr);
    assert.ok(res.stderr.includes(`names ${hostFile}`), res.stderr);
    assert.doesNotMatch(res.stderr, /pass --local/);
  }
});
