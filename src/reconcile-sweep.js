import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteFile, ensureStateDirs, withLock } from './state.js';
import { loadGlobalConfigOrError } from './config.js';
import { listAttempts, supervisorAlive } from './attempts.js';
import { reconcileRemoteAttempt } from './remote-reconcile.js';
import { heartbeatUpRunners } from './runner-unreachable.js';

/**
 * BRAIN-515: settle every ORPHANED-REMOTE attempt whose runner answers, without anyone typing `lane cancel`. A runner that comes
 * back after a reboot or power-off is asked about each attempt (`reconcileRemoteAttempt`, which never runs anything again). Run
 * by `lane reconcile` and, detached, by `lane status` whenever it lists an orphan.
 *
 * At most one sweep per SWEEP_INTERVAL_MS: the marker is claimed under the broker lock, so concurrent sweeps (several `lane
 * status` calls) run exactly one. Attempts are asked one at a time, oldest first; the first transport failure for a runner skips
 * that runner's remaining attempts this sweep (no repeated ssh timeouts), and other runners carry on. Exception (TPWR-674): a
 * runner the heartbeat vouches for (pinged OK within HEARTBEAT_FRESH_MS, read before the sweep, because a failed fetch starts the
 * unreachable clock) is up, so one attempt it cannot answer is that ticket's problem and must not hide the newer, finished ones.
 */
export const SWEEP_INTERVAL_MS = 5 * 60_000;
const CLAIM_LOCK_TIMEOUT_MS = 5000;

/** Test-only seam (README "Testing hooks"): park inside the claim, after the marker read, until `<dir>/go` exists. */
async function pauseInClaimForTest() {
  const dir = process.env.LANE_BROKER_TEST_PAUSE_IN_RECONCILE_CLAIM;
  if (!dir) return;
  fs.writeFileSync(path.join(dir, `ready-${process.pid}`), '');
  while (!fs.existsSync(path.join(dir, 'go'))) await new Promise((resolve) => setTimeout(resolve, 20));
}

/** Atomically (under the lock) decide whether this caller owns the sweep, and if so record it. */
async function claimSweep(root, now, force) {
  return withLock(
    root,
    async () => {
      const marker = paths(root).reconcileLastRun;
      let last = NaN;
      try {
        last = Number(fs.readFileSync(marker, 'utf8').trim());
      } catch {
        // never swept
      }
      await pauseInClaimForTest();
      if (!force && Number.isFinite(last) && now - last < SWEEP_INTERVAL_MS) return false;
      atomicWriteFile(marker, String(now));
      return true;
    },
    { timeoutMs: CLAIM_LOCK_TIMEOUT_MS },
  );
}

/** Returns `{ ran: false, reason }` or `{ ran: true, results: [{ id, runner, state, exit?, reason? }] }`; never throws. */
export async function sweepOrphans(root, { globalCfg, force = false, now = Date.now(), sshBin, env, deadlineMs = 15_000 } = {}) {
  try {
    if (!globalCfg) return { ran: false, reason: 'config unreadable' };
    if (!(await claimSweep(root, now, force))) return { ran: false, reason: 'recently swept' };

    const orphans = listAttempts(root)
      .filter((a) => a.executor === 'remote' && !supervisorAlive(a) && !a.moving)
      .sort((a, b) => (a.startedAt ?? 0) - (b.startedAt ?? 0));
    const vouchedUp = new Set(heartbeatUpRunners(root, globalCfg, now));
    const downRunners = new Set();
    const results = [];
    for (const attempt of orphans) {
      const entry = { id: attempt.id, runner: attempt.runner };
      // a runner already found down this sweep is not asked again, but the operator flag and the unreachable bound still settle (BRAIN-594)
      const assumeDown = downRunners.has(attempt.runner);
      const outcome = await reconcileRemoteAttempt(root, attempt.id, { globalCfg, sshBin, env, deadlineMs, now, assumeDown });
      if (assumeDown && !outcome.settled) {
        results.push({ ...entry, state: 'skipped-runner-down' });
        continue;
      }
      if (outcome.transport && !vouchedUp.has(attempt.runner)) downRunners.add(attempt.runner);
      results.push({
        ...entry,
        state: outcome.state,
        ...(outcome.exit === undefined ? {} : { exit: outcome.exit }),
        ...(outcome.reason === undefined ? {} : { reason: outcome.reason }),
      });
    }
    return { ran: true, results };
  } catch (err) {
    return { ran: false, reason: `sweep failed: ${err.message}` };
  }
}

/** `lane reconcile [--now] [--json]`: one sweep; one line per attempt (or the JSON result). Exit 0 unless the sweep itself threw. */
export async function reconcileCommand({ force = false, json = false } = {}) {
  try {
    const { config, error } = loadGlobalConfigOrError();
    const sweep = await sweepOrphans(ensureStateDirs().root, { globalCfg: error ? null : config, force });
    if (json) {
      process.stdout.write(`${JSON.stringify(sweep)}\n`);
    } else if (!sweep.ran) {
      process.stdout.write(
        sweep.reason === 'recently swept' ? 'lane reconcile: swept recently; use --now\n' : `lane reconcile: not run (${sweep.reason}${error ? `: ${error}` : ''})\n`,
      );
    } else {
      for (const r of sweep.results) {
        process.stdout.write(`${r.id} ${r.runner ?? '-'} ${r.state}${r.exit === undefined ? '' : ` exit ${r.exit}`}${r.reason ? ` — ${r.reason}` : ''}\n`);
      }
    }
    return { exitCode: 0 };
  } catch (err) {
    process.stderr.write(`lane reconcile: ${err.message}\n`);
    return { exitCode: 1 };
  }
}
