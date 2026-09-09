import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';
import { runCommand } from '../src/run.js';

function setup() {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  return { base, home, state, repoDir, env };
}

test('--detach prints the id and the --log path exists immediately, before the child produces output', async () => {
  const { base, repoDir, env } = setup();
  const logPath = path.join(base, 'detached.log');

  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--log', logPath, '--', 'sleep', '1'], {
    env,
    cwd: repoDir,
  });

  assert.equal(result.code, 0, `stderr: ${result.stderr}`);
  assert.match(result.stdout, /^[0-9a-f-]{36}\n$/, 'stdout must be exactly the id');
  assert.match(result.stderr, /detached [0-9a-f-]{36}; log .*detached\.log; reattach with: lane wait/);
  assert.equal(fs.existsSync(logPath), true, 'the --log file must exist immediately after --detach returns');
});

test('a supervisor that fails to spawn synchronously never prints an id and exits 1', async () => {
  const { base, repoDir, env } = setup();
  const prevCwd = process.cwd();
  process.chdir(repoDir);
  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = env.LANE_BROKER_STATE;
  try {
    const failingSpawn = () => {
      throw new Error('injected spawn failure');
    };
    let stderr = '';
    const origWrite = process.stderr.write.bind(process.stderr);
    process.stderr.write = (chunk, ...rest) => {
      stderr += chunk;
      return origWrite(chunk, ...rest);
    };
    let result;
    try {
      result = await runCommand({
        repo: 'r',
        lane: 'default',
        detach: true,
        cmd: ['true'],
        log: path.join(base, 'never.log'),
        spawnSupervisor: failingSpawn,
      });
    } finally {
      process.stderr.write = origWrite;
    }
    assert.equal(result.exitCode, 1);
    assert.match(stderr, /failed to start supervisor: injected spawn failure/);
  } finally {
    process.chdir(prevCwd);
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
  }
});

// The two `lane run --timeout` state-naming assertions (QUEUED-with-position,
// RUNNING-with-elapsed-time) that used to live here were moved to
// tests/run-not-found-grace.test.js (BRAIN-201): they raced real scheduler
// admission latency against a fixed --timeout window, and that latency was
// measured on this machine spanning ~200ms to ~68s under contention -- no
// timeout value makes that deterministic. The fake-supervisor harness in
// run-not-found-grace.test.js drives the same `describeLaneState` code path
// with the ticket's queue/lease state set directly and deterministically,
// with no dependence on real process-admission timing.
