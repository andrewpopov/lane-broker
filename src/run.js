import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ensureStateDirs, paths, readJsonSafe } from './state.js';
import { resolveTicketConfig, ConfigError } from './config.js';
import { isPidAlive, readLease, LEASE_STATE } from './lease.js';
import { listQueue } from './scheduler.js';

const supervisorPath = fileURLToPath(new URL('./supervisor.js', import.meta.url));

function parseDuration(spec) {
  const m = String(spec).trim().match(/^(\d+(?:\.\d+)?)(ms|s|m|h)?$/);
  if (!m) throw new Error(`invalid duration "${spec}" (expected e.g. 30s, 5m, 500ms)`);
  const n = Number(m[1]);
  const unit = m[2] || 's';
  const mult = { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[unit];
  return n * mult;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function fmtAgo(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m${s % 60}s`;
}

/** Precisely name where a still-not-finished lane sits at timeout: queued at
 *  a known position, or running since a known time — never the vague "queued
 *  or running" that reads as "it's running somewhere" regardless of which. */
function describeLaneState(root, id) {
  const lease = readLease(root, id);
  if (lease && (lease.state === LEASE_STATE.RUNNING || lease.state === LEASE_STATE.ORPHANED)) {
    const startedAgo = lease.startedAt ? fmtAgo(Date.now() - lease.startedAt) : 'an unknown time';
    return `${id} is RUNNING (started ${startedAgo} ago); next: lane wait ${id} | lane cancel ${id}`;
  }
  const queue = listQueue(root);
  const idx = queue.findIndex((t) => t.id === id);
  if (idx !== -1) {
    return `${id} REMAINS QUEUED at position ${idx + 1} of ${queue.length}; next: lane wait ${id} | lane cancel ${id}`;
  }
  return `${id} status could not be determined; next: lane wait ${id} | lane cancel ${id}`;
}

/**
 * `lane run`. Returns { exitCode } — callers (the bin entrypoint) set
 * process.exitCode from it rather than calling process.exit directly, so
 * tests can invoke this in-process.
 */
export async function runCommand({
  repo,
  lane,
  weightOverride,
  detach,
  timeoutMs,
  allowLocalSim,
  cwd = process.cwd(),
  cmd,
  log,
  spawnSupervisor = spawn,
}) {
  if (!cmd || cmd.length === 0) {
    process.stderr.write('lane run: no command given (pass it after --)\n');
    return { exitCode: 2 };
  }

  let resolved;
  try {
    resolved = resolveTicketConfig({ cwd, repo, lane });
  } catch (err) {
    if (err instanceof ConfigError) {
      process.stderr.write(`lane run: ${err.message}\n`);
      return { exitCode: 64 };
    }
    throw err;
  }
  const weight = weightOverride ?? resolved.weight;

  const inheritedLease = process.env.LANE_BROKER_LEASE;
  const inheritedKey = process.env.LANE_BROKER_KEY;
  if (inheritedLease) {
    const root = ensureStateDirs().root;
    const inheritedLeaseRecord = readLease(root, inheritedLease);
    if (inheritedLeaseRecord) {
      const inheritedRepoId = inheritedKey && inheritedKey.includes(':') ? inheritedKey.slice(0, inheritedKey.indexOf(':')) : null;
      const sameKey = inheritedKey === resolved.key;
      // Reentrancy allows the exact same key, or a "prepush" lane run under
      // an inherited lease of the same repo (so the pre-push hook works
      // under any lane, not just the one it happens to nest inside).
      const prepushUnderSameRepo = resolved.lane === 'prepush' && inheritedRepoId !== null && inheritedRepoId === resolved.repoId;
      if (sameKey || prepushUnderSameRepo) {
        if (weight > inheritedLeaseRecord.weight) {
          process.stderr.write(
            `lane run: refusing to widen the inherited lease's weight (have ${inheritedLeaseRecord.weight}, ` +
              `requested ${weight}). Reentrant runs must not exceed the inherited weight.\n`,
          );
          return { exitCode: 64 };
        }
        // Reentrant: an ancestor already holds a lease covering this run. Run directly, no new acquisition.
        const child = spawn(cmd[0], cmd.slice(1), { cwd, stdio: 'inherit' });
        return new Promise((resolve) => {
          child.on('error', (err) => {
            process.stderr.write(`lane run: failed to start command: ${err.message}\n`);
            resolve({ exitCode: 1 });
          });
          child.on('exit', (code, signal) => resolve({ exitCode: code ?? (signal ? 1 : 0) }));
        });
      }
      process.stderr.write(
        `lane run: refusing to widen the inherited lease (have "${inheritedKey}", requested "${resolved.key}"). ` +
          `Reentrant runs must use the exact same lane, or "prepush" under the same repo.\n`,
      );
      return { exitCode: 64 };
    }
    // LANE_BROKER_LEASE is set but no such lease exists on disk (stale/forged
    // env var) -- fall through to normal acquisition rather than trusting it.
  }

  if (resolved.localRefused && !allowLocalSim) {
    const dsn = process.env.ROUGE_FLEET_SUBMIT_DSN;
    const submitHint = dsn
      ? `submit to the fleet instead: ${dsn}`
      : 'submit to the fleet instead (set ROUGE_FLEET_SUBMIT_DSN, or pass --allow-local-sim to run here)';
    process.stderr.write(
      `lane run: lane "${resolved.lane}" is refused for local runs by default (${submitHint}). ` +
        `Pass --allow-local-sim to override.\n`,
    );
    return { exitCode: 69 };
  }

  const root = ensureStateDirs().root;
  const id = crypto.randomUUID();
  const p = paths(root);
  const logPath = log || path.join(p.logs, `${id}.log`);
  const resultPath = path.join(p.results, `${id}.json`);

  const ticket = {
    id,
    key: resolved.key,
    repoId: resolved.repoId,
    lane: resolved.lane,
    weight,
    conflicts: resolved.conflicts,
    cwd,
    cmd,
    createdAt: Date.now(),
    logPath,
    resultPath,
  };

  // Create the log file at registration, before the supervisor is spawned,
  // so `--log <path>` exists the instant the id is printed rather than
  // waiting on the supervisor's own CappedLogWriter to open it later — a
  // caller that immediately tails --log right after a --detach id must
  // never find nothing there.
  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  fs.closeSync(fs.openSync(logPath, 'a'));

  let supervisor;
  try {
    supervisor = spawnSupervisor(
      process.execPath,
      [supervisorPath],
      {
        detached: true,
        stdio: 'ignore',
        env: {
          ...process.env,
          LANE_BROKER_TICKET: Buffer.from(JSON.stringify(ticket)).toString('base64'),
        },
      },
    );
  } catch (err) {
    process.stderr.write(`lane run: failed to start supervisor: ${err.message}\n`);
    return { exitCode: 1 };
  }
  supervisor.unref();
  let spawnFailure = null;
  supervisor.on('error', (err) => {
    spawnFailure = err;
    process.stderr.write(`lane run: failed to start supervisor: ${err.message}\n`);
  });

  if (detach) {
    // Wait for the child to actually report it started (or failed to) before
    // printing an id: printing an id for a supervisor that never started is
    // exactly the silent-success failure mode this fixes.
    await new Promise((resolve) => {
      supervisor.once('spawn', resolve);
      supervisor.once('error', resolve);
    });
    if (spawnFailure) {
      return { exitCode: 1 };
    }
    process.stdout.write(`${id}\n`);
    process.stderr.write(`lane run: detached ${id}; log ${logPath}; reattach with: lane wait ${id}\n`);
    return { exitCode: 0 };
  }

  let cancelling = false;
  const forwardCancel = () => {
    if (cancelling) return;
    cancelling = true;
    try {
      process.kill(supervisor.pid, 'SIGTERM');
    } catch {
      // supervisor already gone
    }
  };
  process.on('SIGINT', forwardCancel);
  process.on('SIGTERM', forwardCancel);

  const deadline = timeoutMs ? Date.now() + timeoutMs : null;
  try {
    for (;;) {
      const result = readJsonSafe(resultPath);
      if (result) {
        if (result.signal) {
          process.stderr.write(`lane run: command terminated by signal ${result.signal}\n`);
          return { exitCode: 1 };
        }
        return { exitCode: result.exit ?? 1 };
      }
      if (deadline && Date.now() > deadline) {
        process.stderr.write(`lane run: waited ${timeoutMs}ms, not failed — ${describeLaneState(root, id)}\n`);
        return { exitCode: 75 };
      }
      if (!isPidAlive(supervisor.pid)) {
        // The supervisor may have written its result and exited in the gap
        // between our last read and this liveness check -- re-read once
        // before concluding it died with nothing to show for it.
        const finalResult = readJsonSafe(resultPath);
        if (finalResult) {
          if (finalResult.signal) {
            process.stderr.write(`lane run: command terminated by signal ${finalResult.signal}\n`);
            return { exitCode: 1 };
          }
          return { exitCode: finalResult.exit ?? 1 };
        }
        // The supervisor exited without ever writing a result: it was cancelled
        // while still queued (or crashed) before the child ever ran.
        if (cancelling) {
          process.stderr.write(`lane run: cancelled before the lane started (id ${id})\n`);
          return { exitCode: 130 };
        }
        process.stderr.write(`lane run: supervisor exited unexpectedly with no result (id ${id})\n`);
        return { exitCode: 1 };
      }
      await sleep(200);
    }
  } finally {
    process.off('SIGINT', forwardCancel);
    process.off('SIGTERM', forwardCancel);
  }
}
