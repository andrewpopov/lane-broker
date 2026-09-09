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
 * Drain the microtask queue via a real (never mocked) setImmediate. Node
 * processes the microtask queue to exhaustion -- including microtasks newly
 * queued while draining it -- before running any macrotask, so one await of
 * a real setImmediate is enough to let an arbitrarily deep synchronous
 * promise chain (e.g. `enqueue`'s uncontested `withLock`, or a whole
 * `describeLaneState` iteration up to its next real timer) settle, without
 * depending on how many `.then` hops it happens to take today.
 */
function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

/** A promise the test can resolve from outside, to gate a choreograph phase
 *  on an explicit signal instead of a wall-clock sleep. */
function deferred() {
  let resolve;
  const promise = new Promise((res) => {
    resolve = res;
  });
  return { promise, resolve };
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
  let choreographDone = Promise.resolve();
  try {
    const spawnSupervisor = (execPath, args, opts) => {
      const ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
      const root = process.env.LANE_BROKER_STATE;
      choreographDone = Promise.resolve().then(() => choreograph(ticket, root));
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
    // Make the fake supervisor's simulated actions -- and any error it
    // threw -- observable, instead of letting them run as an orphaned,
    // unawaited promise the test never checks in on.
    await choreographDone;
    return { result, stderr };
  } finally {
    process.stderr.write = origWrite;
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
    process.chdir(prevCwd);
  }
}

test('a ticket in the dequeue-before-lease publication gap at timeout is reported RUNNING once the lease lands, never indeterminate', async (t) => {
  const { repoDir, env } = setup();
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });

  const lease = deferred();
  const runPromise = runWithFakeSupervisor({
    env,
    cwd: repoDir,
    timeoutMs: 180,
    // State-driven, not wall-clock: enqueue+dequeue happen immediately (no
    // real scheduler is involved here -- this fake supervisor IS `tryStart`,
    // standing in for it), landing the ticket in the dequeue-before-lease
    // publication gap right away. `writeLease` is held behind an explicit
    // gate that the test only releases after it has driven the fake clock
    // past the 180ms deadline and into `describeLaneState`'s grace loop --
    // so the gap provably straddles the deadline check on every run,
    // regardless of how fast or slow the machine is.
    choreograph: async (ticket, root) => {
      await enqueue(root, ticket);
      dequeueSync(root, ticket.id);
      await lease.promise;
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

  // Let the choreograph run its synchronous-to-first-real-wait portion:
  // enqueue (an uncontested `withLock` never touches a real timer) and the
  // immediate dequeueSync, up to where it blocks on the lease gate.
  await flush();

  // Advance the fake clock past `timeoutMs` (180ms). runCommand's main loop
  // polls every 200ms, so this is the tick that fires its first deadline
  // check and drops into `describeLaneState`'s grace loop -- at this exact
  // (virtual) instant the ticket is dequeued but the lease gate is still
  // held shut.
  t.mock.timers.tick(200);
  await flush();

  // Release the gate: the lease lands while `describeLaneState` is polling,
  // the same shape as the original race but now ordered on purpose.
  lease.resolve();
  await flush();

  // Fire describeLaneState's own 50ms poll so it re-reads state and observes
  // the now-published lease.
  t.mock.timers.tick(50);
  await flush();

  const { result, stderr } = await runPromise;

  assert.equal(result.exitCode, 75, `stderr: ${stderr}`);
  assert.match(stderr, /is RUNNING \(started/);
  assert.doesNotMatch(stderr, /status could not be determined/);
});

test('a lane that genuinely never appears still reports indeterminate, bounded by the grace window', async (t) => {
  const { repoDir, env } = setup();
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });

  const runPromise = runWithFakeSupervisor({
    env,
    cwd: repoDir,
    timeoutMs: 50,
    // Never enqueue, lease, or result the ticket at all.
    choreograph: async () => {},
  });

  let settled = false;
  runPromise.then(() => {
    settled = true;
  });

  // Drive the fake clock forward on describeLaneState's own poll granularity
  // (50ms). Because the clock is virtual, the number of ticks it takes to
  // settle is a direct, deterministic measurement of the code's own
  // termination bound -- not a race against real scheduling, so there is no
  // jitter left to tolerate. Capped well above the expected settle point so
  // a genuine hang fails the test instead of the runner's own timeout.
  let elapsedMs = 0;
  for (let i = 0; i < 80 && !settled; i++) {
    t.mock.timers.tick(50);
    elapsedMs += 50;
    await flush();
  }
  assert.ok(settled, `describeLaneState did not settle after ${elapsedMs}ms of virtual time`);

  const { result, stderr } = await runPromise;

  assert.equal(result.exitCode, 75, `stderr: ${stderr}`);
  assert.match(stderr, /status could not be determined/);
  // Proves the grace terminates rather than hangs: it must wait out the full
  // grace window before giving up. The bound is tight (NOT_FOUND_GRACE_MS +
  // 300ms) because, with a virtual clock, the only slack left is two FIXED,
  // deterministic overheads rather than real-time jitter: the main run
  // loop's own 200ms poll cadence (timeoutMs=50 is shorter than that, so the
  // deadline is only actually noticed on the loop's next 200ms poll) plus
  // describeLaneState's own 50ms poll granularity.
  assert.ok(elapsedMs >= NOT_FOUND_GRACE_MS, `expected at least the grace window to elapse, got ${elapsedMs}ms (virtual)`);
  assert.ok(elapsedMs < NOT_FOUND_GRACE_MS + 300, `expected the grace window to be tightly bounded, got ${elapsedMs}ms (virtual)`);
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
