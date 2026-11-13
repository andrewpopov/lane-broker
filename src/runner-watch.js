import { paths, ensureStateDirs, withLock, LockTimeoutError } from './state.js';
import { loadGlobalConfigOrError } from './config.js';
import { runWithDeadline, sshArgv } from './remote-client.js';
import { sweepOrphans } from './reconcile-sweep.js';
import { markOffline, clearOffline } from './runner-offline.js';
import { recordPings, runnersDueForPing, collectRunnerStates, renderRunnersLines } from './runner-unreachable.js';

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
 * One watcher tick. Exits quietly when another tick holds `runner-watch.lock` (state.js `withLock`). Pings every configured runner that is not `offline` and not backed off
 * (an unreachable one waits `pingIntervalMs`, 1 min doubling to 1 h), whatever its schedule, records the results through the same helper reconcile uses, then runs the ORPHANED-REMOTE reconcile sweep
 * (rate-limited by its own marker, so at most one per SWEEP_INTERVAL_MS however often this runs) so `runnerLostAfterMs` settles on time.
 */
export async function runnerWatchOnce(root, { cfg, sshBin = 'ssh', env = process.env, now = Date.now() } = {}) {
  let pings;
  try {
    // The watch lock covers the pings and their record only: the sweep below takes the broker lock, which is never taken while holding it.
    pings = await withLock(
      root,
      async () => {
        const pinged = runnersDueForPing(root, cfg, now);
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

/** `lane runner offline|online <name>`: the operator mark of src/runner-offline.js; one line says the new state and whether it changed. */
export async function runnerMarkCommand(target, name) {
  const { root } = ensureStateDirs();
  const { config, error } = loadGlobalConfigOrError();
  if (error) {
    process.stderr.write(`lane runner: global config unreadable: ${error}\n`);
    return { exitCode: 1 };
  }
  const names = (config.runners ?? []).map((r) => r.name);
  if (!names.includes(name)) {
    process.stderr.write(`lane runner ${target}: no runner named "${name}" (configured: ${names.join(', ') || 'none'})\n`);
    return { exitCode: 2 };
  }
  if (target === 'offline') {
    const changed = await markOffline(root, name);
    say(`runner ${name}: offline (${changed ? 'changed' : 'unchanged, already marked'})`);
    return { exitCode: 0 };
  }
  const changed = await clearOffline(root, name);
  const stillOffline = loadGlobalConfigOrError().config.runners.find((r) => r.name === name)?.offline;
  if (stillOffline) {
    process.stderr.write(`lane runner online: ${name} is still offline: the global config sets offline: true; edit it (${changed ? 'the lane runner offline mark was cleared' : 'no lane runner offline mark was set'})\n`);
    return { exitCode: 1 };
  }
  say(`runner ${name}: online (${changed ? 'changed' : 'unchanged, was not marked'})`);
  return { exitCode: 0 };
}
