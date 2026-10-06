import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { spawn } from 'node:child_process';
import { freshEnv } from './helpers.js';
import { waitCommand } from '../src/wait.js';
import { paths, ensureStateDirs, atomicWriteJson, bootId } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';

/** A pid that is guaranteed to be dead right now -- same technique
 *  attempts.test.js uses for `supervisorAlive` coverage. */
async function deadPid() {
  const child = spawn('node', ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

/** Directly write an attempt record with a supervisor that is already dead --
 *  bypasses `createAttempt` (which always stamps the CALLING process, i.e.
 *  this test, as the live supervisor) so the liveness check in wait.js's
 *  orphan branch is false from the very first loop iteration. */
function writeDeadAttempt(root, id, pid, executor = 'remote') {
  ensureStateDirs(root);
  const attempt = {
    id,
    generation: 0,
    executor,
    phase: executor === 'remote' ? 'probe' : 'queued',
    runner: executor === 'remote' ? 'skybox' : null,
    startedAt: Date.now(),
    supervisor: { pid, startTime: null, bootId: bootId() },
  };
  atomicWriteJson(path.join(paths(root).attempts, `${id}.json`), attempt);
}

test('BRAIN-319 orphan race: a result published in the gap between the "no result yet" read and the dead-supervisor check is honoured, not misreported as ORPHANED-REMOTE', async () => {
  const { state } = freshEnv();
  const priorHome = process.env.LANE_BROKER_HOME;
  const priorState = process.env.LANE_BROKER_STATE;
  const priorPause = process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN;
  process.env.LANE_BROKER_STATE = state;
  delete process.env.LANE_BROKER_HOME;

  const pauseFile = path.join(makeTmpDir('wait-orphan-race-'), 'release');
  process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN = pauseFile;

  try {
    const id = 'orphan-race-1';
    const pid = await deadPid();
    writeDeadAttempt(state, id, pid);

    // Start `waitCommand` with no result and a dead-supervisor attempt on
    // record -- it must land in the paused orphan-decision gap on its very
    // first loop iteration (no result yet, attempt present, supervisor dead).
    const waited = waitCommand(id);

    // While it's paused, simulate the real race: the supervisor actually
    // DID publish a real result and remove its attempt record -- it just
    // did so in the window between wait's own reads.
    const resultPath = path.join(paths(state).results, `${id}.json`);
    atomicWriteJson(resultPath, { id, exit: 0, signal: null, startedAt: Date.now() - 500, endedAt: Date.now() });
    fs.rmSync(path.join(paths(state).attempts, `${id}.json`), { force: true });

    // Release the pause.
    fs.writeFileSync(pauseFile, 'go');

    const outcome = await waited;
    assert.equal(outcome.exitCode, 0, 'a result that showed up during the gap must win -- this must not be reported as an orphan');
  } finally {
    if (priorState === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = priorState;
    if (priorHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = priorHome;
    if (priorPause === undefined) delete process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN;
    else process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN = priorPause;
  }
});

test('BRAIN-319 orphan race: a genuinely dead supervisor with no result ever showing up during the gap still reports ORPHANED-REMOTE', async () => {
  const { state } = freshEnv();
  const priorHome = process.env.LANE_BROKER_HOME;
  const priorState = process.env.LANE_BROKER_STATE;
  const priorPause = process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN;
  process.env.LANE_BROKER_STATE = state;
  delete process.env.LANE_BROKER_HOME;

  const pauseFile = path.join(makeTmpDir('wait-orphan-race-'), 'release');
  process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN = pauseFile;

  try {
    const id = 'orphan-race-2';
    const pid = await deadPid();
    writeDeadAttempt(state, id, pid);

    const waited = waitCommand(id);

    // Nothing changes during the gap -- release immediately, no result and
    // no attempt-removal show up.
    fs.writeFileSync(pauseFile, 'go');

    const outcome = await waited;
    assert.equal(outcome.exitCode, 1, 'a genuinely dead supervisor with no result must still be reported, not silently kept waiting');
  } finally {
    if (priorState === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = priorState;
    if (priorHome === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = priorHome;
    if (priorPause === undefined) delete process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN;
    else process.env.LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN = priorPause;
  }
});
