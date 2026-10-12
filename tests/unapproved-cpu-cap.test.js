import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, gitFixture } from './helpers.js';
import { atomicWriteJson } from '../src/state.js';
import { runCommand } from '../src/run.js';
import { capUnapprovedCpu } from '../src/resources.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

// BRAIN-552: an unapproved request never reserves more than maxUnapprovedCpuCores; a lane is approved only at what the COMMITTED config declares.

const SHOW_GRANT = ['sh', '-c', 'echo "grant=$LANE_BROKER_CPU_CORES"'];

/** A repo whose committed `.lane-broker.json` is `committed`; `working` (when given) is then written over it, uncommitted. */
function repoFixture({ committed, working, git = true, host = {} }) {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 16, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100, ...host });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, committed);
  if (git) {
    gitFixture(['init', '-q'], repoDir);
    gitFixture(['add', '-A'], repoDir);
    gitFixture(['commit', '-q', '-m', 'x'], repoDir);
  }
  if (working) writeRepoConfig(repoDir, working);
  return { env, repoDir, base };
}
const run = (f, lane, extra = []) => laneRun(['run', '--repo', 'r', '--lane', lane, ...extra, '--', ...SHOW_GRANT], { env: f.env, cwd: f.repoDir });
const grantOf = (r) => Number(/grant=([\d.]+)/.exec(r.stdout)?.[1]);

const BIG = { version: 1, undeclaredLanes: { as: 'big' }, lanes: { big: { weight: 1, cpuCores: 9 }, small: { weight: 1, cpuCores: 4 } } };

test('config: maxUnapprovedCpuCores defaults to 4', () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.maxUnapprovedCpuCores, 4);
});

test('an undeclared lane mapped to a 9-core template is granted at most 4, and says so on stderr', async () => {
  const f = repoFixture({ committed: BIG });
  const r = await run(f, 'zirk812');
  assert.equal(r.code, 0, r.stderr);
  assert.equal(grantOf(r), 4);
  assert.match(r.stderr, /lane run: cpu 9 -> 4 \(lane "zirk812": lane "zirk812" is not declared in the committed \.lane-broker\.json; maxUnapprovedCpuCores 4\)/);
});

test('--cpu 10 on a declared 4-core lane is capped to 4', async () => {
  const f = repoFixture({ committed: BIG });
  const r = await run(f, 'small', ['--cpu', '10']);
  assert.equal(grantOf(r), 4, r.stderr);
  assert.match(r.stderr, /cpu 10 -> 4 \(lane "small": the committed \.lane-broker\.json declares lane "small" at cpu 4/);
});

test('a lane declared at 9 in the committed config keeps 9, with no cap line', async () => {
  const f = repoFixture({ committed: BIG });
  const r = await run(f, 'big');
  assert.equal(grantOf(r), 9, r.stderr);
  assert.doesNotMatch(r.stderr, /maxUnapprovedCpuCores/);
});

test('a lane raised only in the working tree is capped to max(HEAD value, 4)', async () => {
  const raised = (cpuCores) => ({ version: 1, lanes: { l: { weight: 1, cpuCores } } });
  const low = repoFixture({ committed: raised(2), working: raised(8) });
  assert.equal(grantOf(await run(low, 'l')), 4);
  const high = repoFixture({ committed: raised(6), working: raised(8) });
  const r = await run(high, 'l');
  assert.equal(grantOf(r), 6, r.stderr);
  assert.match(r.stderr, /cpu 8 -> 6 /);
});

test('no git, no committed file: capped even when the lane is declared', async () => {
  const f = repoFixture({ committed: BIG, git: false });
  const r = await run(f, 'big');
  assert.equal(grantOf(r), 4, r.stderr);
  assert.match(r.stderr, /cpu 9 -> 4 \(lane "big": there is no git checkout;/);
});

test('a git repo with no committed .lane-broker.json is capped', async () => {
  const f = repoFixture({ committed: BIG, git: false });
  gitFixture(['init', '-q'], f.repoDir);
  gitFixture(['commit', '-q', '--allow-empty', '-m', 'x'], f.repoDir);
  const r = await run(f, 'big');
  assert.equal(grantOf(r), 4, r.stderr);
  assert.match(r.stderr, /there is no committed \.lane-broker\.json/);
});

test('capUnapprovedCpu brings an elastic floor down with the claim and leaves a smaller claim alone', () => {
  const cap = (over) => capUnapprovedCpu({ cpuCores: 9, minCpuCores: 6, approvedCeiling: null, maxUnapprovedCpuCores: 4, ...over });
  assert.deepEqual(cap({}), { cpuCores: 4, minCpuCores: 4, capped: true });
  assert.deepEqual(cap({ minCpuCores: 2 }), { cpuCores: 4, minCpuCores: 2, capped: true });
  assert.deepEqual(cap({ approvedCeiling: 9 }), { cpuCores: 9, minCpuCores: 6, capped: false });
  assert.deepEqual(cap({ cpuCores: 3, minCpuCores: undefined }), { cpuCores: 3, minCpuCores: undefined, capped: false });
});

test('the remote-runner path (configRoot) does not re-cap a 9-core header', async () => {
  // gitless snapshot, like a runner's work dir: a client-side cap would find no committed file and cut the 9 to 4
  const { base, home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 16, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const workDir = path.join(base, 'snapshot');
  writeRepoConfig(workDir, BIG);
  let ticket = null;
  const spawnSupervisor = (execPath, args, opts) => {
    ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
    atomicWriteJson(ticket.resultPath, { id: ticket.id, exit: 0, signal: null, startedAt: Date.now(), endedAt: Date.now(), waitedMs: 0 });
    const silent = () => new PassThrough().end();
    return Object.assign(new EventEmitter(), { pid: process.pid, unref() {}, stdout: silent(), stderr: silent() });
  };
  const names = ['LANE_BROKER_HOME', 'LANE_BROKER_STATE', 'LANE_BROKER_LEASE', 'LANE_BROKER_LOCAL'];
  const prev = Object.fromEntries(names.map((n) => [n, process.env[n]]));
  for (const n of names) delete process.env[n];
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  let stderr = '';
  const origWrite = process.stderr.write.bind(process.stderr);
  process.stderr.write = (chunk, ...rest) => {
    stderr += chunk;
    return origWrite(chunk, ...rest);
  };
  try {
    const result = await runCommand({
      repo: 'r', lane: 'zirk812', cmd: ['true'], cwd: workDir, configRoot: workDir, repoIdentityOverride: 'r', cpuOverride: 9, allowLocalSim: true, spawnSupervisor,
    });
    assert.equal(result.exitCode, 0, stderr);
  } finally {
    process.stderr.write = origWrite;
    for (const n of names) {
      if (prev[n] === undefined) delete process.env[n];
      else process.env[n] = prev[n];
    }
  }
  assert.equal(ticket.resources.cpuCores, 9);
  assert.doesNotMatch(stderr, /maxUnapprovedCpuCores/);
});
