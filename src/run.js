import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ensureStateDirs, paths, readJsonSafe } from './state.js';
import { resolveTicketConfig } from './config.js';
import { isPidAlive } from './lease.js';

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

/**
 * `lane run`. Returns { exitCode } — callers (the bin entrypoint) set
 * process.exitCode from it rather than calling process.exit directly, so
 * tests can invoke this in-process.
 */
export async function runCommand({ repo, lane, weightOverride, detach, timeoutMs, allowLocalSim, cwd = process.cwd(), cmd, log }) {
  if (!cmd || cmd.length === 0) {
    process.stderr.write('lane run: no command given (pass it after --)\n');
    return { exitCode: 2 };
  }

  const resolved = resolveTicketConfig({ cwd, repo, lane });
  const weight = weightOverride ?? resolved.weight;

  const inheritedLease = process.env.LANE_BROKER_LEASE;
  const inheritedKey = process.env.LANE_BROKER_KEY;
  if (inheritedLease) {
    if (inheritedKey === resolved.key) {
      // Reentrant: same key already holds a lease. Run directly, no new acquisition.
      const child = spawn(cmd[0], cmd.slice(1), { cwd, stdio: 'inherit' });
      return new Promise((resolve) => {
        child.on('exit', (code, signal) => resolve({ exitCode: code ?? (signal ? 1 : 0) }));
      });
    }
    process.stderr.write(
      `lane run: refusing to widen the inherited lease (have "${inheritedKey}", requested "${resolved.key}"). ` +
        `Reentrant runs must use the exact same lane.\n`,
    );
    return { exitCode: 64 };
  }

  if (resolved.localRefused && !allowLocalSim) {
    process.stderr.write(
      `lane run: lane "${resolved.lane}" is refused for local runs by default (submit to the fleet: ` +
        `\${ROUGE_FLEET_SUBMIT_DSN}). Pass --allow-local-sim to override.\n`,
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

  const supervisor = spawn(
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
  supervisor.unref();

  if (detach) {
    process.stdout.write(`${id}\n`);
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
        process.stderr.write(`lane run: waited ${timeoutMs}ms, not failed — the lane is still queued or running (id ${id})\n`);
        return { exitCode: 75 };
      }
      if (!isPidAlive(supervisor.pid)) {
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
