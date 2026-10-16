import { paths, ensureStateDirs, withLock, LockTimeoutError } from './state.js';
import { loadGlobalConfigOrError } from './config.js';
import { runWithDeadline, sshArgv } from './remote-client.js';
import { sweepOrphans } from './reconcile-sweep.js';
import { recordPings, collectRunnerStates, renderRunnersLines } from './runner-unreachable.js';

/** BRAIN-597: the runner heartbeat, `lane runner watch`; what it records lives in src/runner-unreachable.js. */

export const PING_DEADLINE_MS = 10_000;
export const PING_CONNECT_TIMEOUT_SEC = 5;
// A held lock is not waited out (the next launchd tick runs in 60 s), but a dead holder's takeover needs a moment inside the deadline.
const WATCH_LOCK_WAIT_MS = 500;

async function pingRunner(runner, { sshBin, env }) {
  const res = await runWithDeadline(sshBin, sshArgv(runner, 'true', { connectTimeoutSec: PING_CONNECT_TIMEOUT_SEC }), PING_DEADLINE_MS, env);
  return { name: runner.name, ok: res.code === 0 && !res.timedOut && !res.spawnError };
}

/**
 * One watcher tick. Exits quietly when another tick holds `runner-watch.lock` (state.js `withLock`). Pings every configured runner that is not `offline`,
 * whatever its schedule, records the results through the same helper reconcile uses, then runs the ORPHANED-REMOTE reconcile sweep
 * (rate-limited by its own marker, so at most one per SWEEP_INTERVAL_MS however often this runs) so `runnerLostAfterMs` settles on time.
 */
export async function runnerWatchOnce(root, { cfg, sshBin = 'ssh', env = process.env, now = Date.now() } = {}) {
  let pings;
  try {
    // The watch lock covers the pings and their record only: the sweep below takes the broker lock, which is never taken while holding it.
    pings = await withLock(
      root,
      async () => {
        const pinged = (cfg?.runners ?? []).filter((r) => !r.offline);
        const results = await Promise.all(pinged.map((runner) => pingRunner(runner, { sshBin, env })));
        await recordPings(root, results, now);
        return results;
      },
      { lockPath: paths(root).runnerWatchLock, group: 'heartbeat', timeoutMs: WATCH_LOCK_WAIT_MS },
    );
  } catch (err) {
    if (err instanceof LockTimeoutError) return { ran: false, reason: 'another tick holds the lock' };
    throw err;
  }
  const sweep = await sweepOrphans(root, { globalCfg: cfg, now, sshBin, env });
  return { ran: true, pings, sweep };
}

const say = (text) => process.stdout.write(`${text}\n`);

/** `lane runner watch`: one tick, then exit (launchd runs it every 60 s). */
export async function runnerWatchCommand() {
  const { root } = ensureStateDirs();
  const { config, error } = loadGlobalConfigOrError();
  if (error) {
    process.stderr.write(`lane runner: global config unreadable: ${error}\n`);
    return { exitCode: 1 };
  }
  await runnerWatchOnce(root, { cfg: config });
  return { exitCode: 0 };
}

/** `lane runner [status] [--json]` */
export function runnerStatusCommand({ json = false } = {}) {
  const { root } = ensureStateDirs();
  const { config, error } = loadGlobalConfigOrError();
  if (error) {
    process.stderr.write(`lane runner: global config unreadable: ${error}\n`);
    return { exitCode: 1 };
  }
  const runners = collectRunnerStates(root, config);
  if (json) say(JSON.stringify(runners, null, 2));
  else say(runners.length === 0 ? 'no runners configured' : renderRunnersLines(runners).join('\n'));
  return { exitCode: 0 };
}
