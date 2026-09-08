import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { freshEnv, writeGlobalConfig, writeRepoConfig } from './helpers.js';
import { runCommand } from '../src/run.js';
import { atomicWriteJson, bootId } from '../src/state.js';
import { enqueue, dequeueSync } from '../src/scheduler.js';
import { writeLease, LEASE_STATE, NOT_FOUND_GRACE_MS } from '../src/lease.js';

function setup() {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  return { base, home, state, repoDir, env };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Run `runCommand` under a fake supervisor that never actually spawns a
 * process. `choreograph(ticket, root)` is free to enqueue/dequeue/lease/
 * result the ticket on whatever schedule the test wants, standing in for the
 * real supervisor process so the race between `describeLaneState`'s grace
 * window and the ticket's publication is deterministic instead of depending
 * on real process-startup timing (the same timing this ticket exists to fix).
 */
async function runWithFakeSupervisor({ env, cwd, timeoutMs, choreograph }) {
  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  const prevCwd = process.cwd();
  process.env.LANE_BROKER_HOME = env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = env.LANE_BROKER_STATE;
  process.chdir(cwd);
  let stderr = '';
  const origWrite = process.stderr.write.bind(process.stderr);
  process.stderr.write = (chunk, ...rest) => {
    stderr += chunk;
    return origWrite(chunk, ...rest);
  };
  try {
    const spawnSupervisor = (execPath, args, opts) => {
      const ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
      const root = process.env.LANE_BROKER_STATE;
      Promise.resolve().then(() => choreograph(ticket, root));
      // Stand in for the real child_process handle: a pid that is always
      // alive (this test process's own) so `isPidAlive` never mistakes the
      // fake supervisor for one that crashed with nothing to show for it.
      return Object.assign(new EventEmitter(), { pid: process.pid, unref() {} });
    };
    const result = await runCommand({
      repo: 'r',
      lane: 'default',
      cmd: ['true'],
      timeoutMs,
      spawnSupervisor,
    });
    return { result, stderr };
  } finally {
    process.stderr.write = origWrite;
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
    process.chdir(prevCwd);
  }
}

test('a ticket in the dequeue-before-lease publication gap at timeout is reported RUNNING once the lease lands, never indeterminate', async () => {
  const { repoDir, env } = setup();

  const { result, stderr } = await runWithFakeSupervisor({
    env,
    cwd: repoDir,
    timeoutMs: 180,
    choreograph: async (ticket, root) => {
      // t=50: enqueued, same as a real supervisor would.
      await sleep(50);
      await enqueue(root, ticket);
      // t=150: tryStart's dequeueSync fires -- the ticket is now in neither
      // the queue nor a lease. The 180ms deadline lands inside this gap.
      await sleep(100);
      dequeueSync(root, ticket.id);
      // t=250: the lease is finally written, closing the gap.
      await sleep(100);
      writeLease(root, {
        id: ticket.id,
        key: ticket.key,
        bootId: bootId(),
        supervisorPid: process.pid,
        supervisorStart: null,
        childPgid: null,
        heartbeatAt: Date.now(),
        admittedAt: Date.now(),
        startedAt: Date.now(),
        cwd: ticket.cwd,
        cmd: ticket.cmd,
        weight: ticket.weight,
        logPath: ticket.logPath,
        resultPath: ticket.resultPath,
        state: LEASE_STATE.RUNNING,
      });
    },
  });

  assert.equal(result.exitCode, 75, `stderr: ${stderr}`);
  assert.match(stderr, /is RUNNING \(started/);
  assert.doesNotMatch(stderr, /status could not be determined/);
});

test('a lane that genuinely never appears still reports indeterminate, bounded by the grace window', async () => {
  const { repoDir, env } = setup();

  const startedAt = Date.now();
  const { result, stderr } = await runWithFakeSupervisor({
    env,
    cwd: repoDir,
    timeoutMs: 50,
    // Never enqueue, lease, or result the ticket at all.
    choreograph: async () => {},
  });
  const elapsedMs = Date.now() - startedAt;

  assert.equal(result.exitCode, 75, `stderr: ${stderr}`);
  assert.match(stderr, /status could not be determined/);
  // Proves the grace terminates rather than hangs: it must wait out the
  // full grace window before giving up, but never much longer than it.
  assert.ok(elapsedMs >= NOT_FOUND_GRACE_MS, `expected at least the grace window to elapse, got ${elapsedMs}ms`);
  assert.ok(elapsedMs < NOT_FOUND_GRACE_MS + 2000, `expected the grace window to be bounded, got ${elapsedMs}ms`);
});

test('a command that completes during the grace window is reported as finished, not indeterminate', async () => {
  const { repoDir, env } = setup();

  const { result, stderr } = await runWithFakeSupervisor({
    env,
    cwd: repoDir,
    timeoutMs: 50,
    choreograph: async (ticket, root) => {
      // Never enqueued or leased at all -- as if the command ran and
      // finished entirely inside the startup gap -- but the result lands
      // well within NOT_FOUND_GRACE_MS.
      await sleep(500);
      atomicWriteJson(ticket.resultPath, { id: ticket.id, exit: 7, signal: null, startedAt: Date.now(), endedAt: Date.now(), waitedMs: 0 });
    },
  });

  assert.equal(result.exitCode, 7, `stderr: ${stderr}`);
});

test('lane run --timeout names the lane as QUEUED with a position when it never reached the head', async () => {
  const { repoDir, env } = setup();

  const { result, stderr } = await runWithFakeSupervisor({
    env,
    cwd: repoDir,
    timeoutMs: 50,
    // Deterministic: `enqueue` is called directly on a known schedule (a
    // short, generous sleep well inside the 50ms deadline check, which only
    // fires after the run loop's first 200ms poll) and the ticket is never
    // dequeued or leased by anything -- there is no real scheduler admitting
    // it. So at the moment `describeLaneState` inspects the queue, this
    // ticket is provably the sole entry, regardless of machine load. This
    // does not race real process-admission latency the way the old
    // `--timeout 300ms` against a real spawned blocker did.
    choreograph: async (ticket, root) => {
      await sleep(20);
      await enqueue(root, ticket);
    },
  });

  assert.equal(result.exitCode, 75, `stderr: ${stderr}`);
  assert.match(stderr, /REMAINS QUEUED at position 1 of 1/);
  assert.match(stderr, /next: lane wait/);
});

test('lane run --timeout names the lane as RUNNING with an elapsed time once it has started', async () => {
  const { repoDir, env } = setup();

  const { result, stderr } = await runWithFakeSupervisor({
    env,
    cwd: repoDir,
    timeoutMs: 50,
    // Deterministic: `writeLease` is called directly, with a RUNNING state
    // and a real `startedAt`, on a known schedule (a short sleep well inside
    // the 50ms deadline check). The result file is never written, so at the
    // moment `describeLaneState` inspects the lease it is provably present
    // and RUNNING, regardless of machine load -- this does not race a real
    // child's actual start time the way the old `--timeout 300ms` against a
    // real spawned `sleep 2` did.
    choreograph: async (ticket, root) => {
      await sleep(20);
      writeLease(root, {
        id: ticket.id,
        key: ticket.key,
        bootId: bootId(),
        supervisorPid: process.pid,
        supervisorStart: null,
        childPgid: null,
        heartbeatAt: Date.now(),
        admittedAt: Date.now(),
        startedAt: Date.now(),
        cwd: ticket.cwd,
        cmd: ticket.cmd,
        weight: ticket.weight,
        logPath: ticket.logPath,
        resultPath: ticket.resultPath,
        state: LEASE_STATE.RUNNING,
      });
    },
  });

  assert.equal(result.exitCode, 75, `stderr: ${stderr}`);
  assert.match(stderr, /is RUNNING \(started/);
  assert.match(stderr, /next: lane wait/);
});
