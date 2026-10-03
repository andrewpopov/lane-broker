import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ensureStateDirs, paths, readJsonSafe } from './state.js';
import { resolveTicketConfig, reloadGlobalConfig, ConfigError } from './config.js';
import { isPidAlive, readLease, LEASE_STATE, NOT_FOUND_GRACE_MS } from './lease.js';
import { listQueue } from './scheduler.js';
import { detectResourceCapacity, leaseResources, resolveTicketResources, checkResourceBudget, localSimRefusal } from './resources.js';
import { scrubbedGitEnv } from './remote-manifest.js';

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

/** Await a readable stream reaching 'end'/'close', bounded so a stream that
 *  never ends (shouldn't happen -- the supervisor has already exited by the
 *  time this is called) can't hang `lane run` forever (BRAIN-308). */
function waitForStreamEnd(stream, timeoutMs = 2000) {
  if (!stream || stream.readableEnded || stream.destroyed) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, timeoutMs);
    const done = () => {
      clearTimeout(timer);
      resolve();
    };
    stream.once('end', done);
    stream.once('close', done);
    stream.once('error', done);
  });
}

/**
 * `git rev-parse --show-toplevel` under the same git-env scrub `remote-
 * manifest.js` uses (BRAIN-319: never let a hook's exported GIT_DIR/etc leak
 * into this). Returns null (never throws) on any failure -- a caller that
 * cannot resolve a worktree root for a remote-eligible lane must fall back
 * to running locally, not crash the run.
 */
function gitRevParse(cwd, arg) {
  try {
    return execFileSync('git', ['rev-parse', arg], {
      cwd,
      encoding: 'utf8',
      env: scrubbedGitEnv(),
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 5000,
    }).trim();
  } catch {
    return null;
  }
}

function resolveWorktreeRoot(cwd) {
  return gitRevParse(cwd, '--show-toplevel');
}

const HISTORY_COMMAND_MAX = 300;

/** Why a run of a remote-capable lane (`remote: true`) is executing locally,
 *  decided at ticket creation. A remote attempt that later falls back is
 *  recorded by the supervisor as `fallback:<reason>` instead. */
function localReasonFor(resolved, eligible, local) {
  if (resolved.remote !== true || eligible) return undefined;
  if (local) return 'forced-flag';
  if (process.env.LANE_BROKER_LOCAL === '1') return 'forced-env';
  return 'not-eligible';
}

function fmtAgo(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m${s % 60}s`;
}

/** Format a finished command's result the same way at every call site that
 *  reads one, instead of repeating the signal/exit branch three times. */
function resultExit(prefix, result) {
  if (result.signal) {
    process.stderr.write(`${prefix}: command terminated by signal ${result.signal}\n`);
    return 1;
  }
  return result.exit ?? 1;
}

/**
 * Precisely name where a still-not-finished lane sits at timeout: queued at
 * a known position, or running since a known time — never the vague "queued
 * or running" that reads as "it's running somewhere" regardless of which.
 *
 * Neither the lease nor the queue is guaranteed to have the id the instant
 * the `--timeout` deadline fires: the supervisor may not have finished
 * enqueueing yet (measured ~200ms after spawn, easily inside a short
 * --timeout), or the id may be sitting in the publication gap `tryStart`
 * (scheduler.js) leaves between dequeuing the queue record and writing the
 * lease. Re-check over NOT_FOUND_GRACE_MS (shared with `lane wait`, see
 * src/lease.js) before concluding "could not be determined" — prefer any
 * positive finding, and keep re-reading the result file too, since a command
 * that finishes and gets reaped during the grace must be reported as
 * finished, not as indeterminate. This is the price of a determinate answer
 * on an already-abnormal path: a `lane run --timeout 300ms` against a lane
 * that genuinely never appears can now take up to ~3.3s to return instead of
 * ~0.3s, bounded and only on this path.
 */
async function describeLaneState(root, id, resultPath) {
  const graceDeadline = Date.now() + NOT_FOUND_GRACE_MS;
  for (;;) {
    const result = readJsonSafe(resultPath);
    if (result) return { finished: true, result };

    const lease = readLease(root, id);
    if (lease && (lease.state === LEASE_STATE.RUNNING || lease.state === LEASE_STATE.ORPHANED)) {
      const startedAgo = lease.startedAt ? fmtAgo(Date.now() - lease.startedAt) : 'an unknown time';
      return { finished: false, text: `${id} is RUNNING (started ${startedAgo} ago); next: lane wait ${id} | lane cancel ${id}` };
    }

    const queue = listQueue(root);
    const idx = queue.findIndex((t) => t.id === id);
    if (idx !== -1) {
      return { finished: false, text: `${id} REMAINS QUEUED at position ${idx + 1} of ${queue.length}; next: lane wait ${id} | lane cancel ${id}` };
    }

    if (Date.now() >= graceDeadline) {
      return { finished: false, text: `${id} status could not be determined; next: lane wait ${id} | lane cancel ${id}` };
    }
    await sleep(50);
  }
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
  cpuOverride,
  minCpuOverride,
  memoryOverride,
  detach,
  timeoutMs,
  allowLocalSim,
  // BRAIN-320 S1d: opt-in queue timeout (relative ms) for a runner-side
  // pipeline ticket -- `lane remote-exec` passes this from the exec
  // header's `queueTimeoutMs` field (itself only present when the CLIENT
  // machine's global config set `remoteQueueTimeoutMs`, I6). Stamped into
  // an ABSOLUTE `ticket.startDeadline` at ticket-creation time below, so
  // `scheduler.js`'s `tryStart` can compare it against `Date.now()` under
  // its own lock without re-deriving "relative to when" itself. A plain
  // `lane run` never passes this, so `ticket.startDeadline` stays absent
  // and the whole S1d expiry path is a no-op for every other caller (I6).
  startDeadlineMs,
  // BRAIN-319 T3b-1: force this run local even when the lane opts into
  // `remote: true` and runners are configured. Mirrored by the
  // LANE_BROKER_LOCAL=1 env var below (bin/lane.js's `--local` flag sets
  // this option; the env var lets a nested/scripted caller force it too).
  local,
  cwd = process.cwd(),
  cmd,
  log,
  spawnSupervisor = spawn,
  // BRAIN-319 (`lane remote-exec`): resolve `.lane-broker.json` from a
  // gitless snapshot work dir without walking above it — see
  // findRepoConfigPath's own doc comment in config.js.
  configRoot,
  // BRAIN-319 T3 (C7): pin the resolved lease key's repo identity to this
  // exact value regardless of what `.git` may exist under `cwd` — see
  // resolveTicketConfig's own doc comment in config.js.
  repoIdentityOverride,
  // BRAIN-319: let a caller that already generated and durably recorded a
  // ticket id (`lane remote-exec`'s remote-id file, written before the
  // child can start) use that same id here, instead of one generated fresh
  // inside this function that the caller could never have learned in time.
  idOverride,
  // BRAIN-319: called with the ticket id as soon as it exists (before the
  // supervisor is spawned, so before any child can start). Returning
  // `false` aborts the run without ever spawning the supervisor.
  onTicketCreated,
}) {
  if (!cmd || cmd.length === 0) {
    process.stderr.write('lane run: no command given (pass it after --)\n');
    return { exitCode: 2 };
  }

  let resolved;
  try {
    resolved = resolveTicketConfig({ cwd, repo, lane, configRoot, repoIdentityOverride });
  } catch (err) {
    if (err instanceof ConfigError) {
      process.stderr.write(`lane run: ${err.message}\n`);
      return { exitCode: 64 };
    }
    throw err;
  }
  const weight = weightOverride ?? resolved.weight;
  // A bad/missing global config must never block enqueueing a lane — it
  // degrades to the default `laneNice`, the same fallback shape
  // reloadGlobalConfig gives the supervisor's own polling loop.
  const globalCfg = reloadGlobalConfig(undefined);
  const nice = resolved.nice ?? globalCfg.laneNice;
  // BRAIN-319 T3b-1: eligible for the supervisor to attempt a remote runner
  // for -- the supervisor itself does not act on this yet (T3b-2). A nested
  // run (LANE_BROKER_LEASE inherited) never goes remote regardless of the
  // lane/config, since it either reuses the ancestor's lease below or is
  // itself the remote-exec child of an already-remote run (I5: a runner
  // never re-dispatches).
  const remoteWanted =
    resolved.remote === true &&
    Array.isArray(globalCfg.runners) &&
    globalCfg.runners.length > 0 &&
    !local &&
    process.env.LANE_BROKER_LOCAL !== '1' &&
    !process.env.LANE_BROKER_LEASE;
  // Resolved eagerly (not deferred to ticket construction) so the
  // resource-budget skip below and the ticket payload agree on the exact
  // same eligibility -- a worktree root that fails to resolve must also
  // re-enable the local budget check, not just omit `ticket.remote`.
  const remoteWorktreeRoot = remoteWanted ? resolveWorktreeRoot(cwd) : null;
  const remoteEligible = remoteWanted && remoteWorktreeRoot !== null;
  const localReason = localReasonFor(resolved, remoteEligible, local);
  const headTree = gitRevParse(cwd, 'HEAD^{tree}');
  const resources = resolveTicketResources({
    weight,
    cpuCores: cpuOverride ?? resolved.cpuCores,
    minCpuCores: minCpuOverride ?? resolved.minCpuCores ?? undefined,
    memoryBytes: memoryOverride ?? resolved.memoryBytes,
    defaultMemoryBytesPerWeight: globalCfg.defaultMemoryBytesPerWeight,
  });
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
        const inheritedResources = leaseResources(inheritedLeaseRecord, globalCfg);
        if (weight > inheritedLeaseRecord.weight) {
          process.stderr.write(
            `lane run: refusing to widen the inherited lease's weight (have ${inheritedLeaseRecord.weight}, ` +
              `requested ${weight}). Reentrant runs must not exceed the inherited weight.\n`,
          );
          return { exitCode: 64 };
        }
        if ((resources.minCpuCores ?? resources.cpuCores) > inheritedResources.cpuCores || resources.memoryBytes > inheritedResources.memoryBytes) {
          process.stderr.write(
            `lane run: refusing to widen inherited resources (have ${inheritedResources.cpuCores} CPU / ` +
              `${inheritedResources.memoryBytes} bytes, requested ${resources.cpuCores} CPU / ${resources.memoryBytes} bytes).\n`,
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

  // BRAIN-320 (local-refused + remote): a `localRefused` lane that is ALSO
  // remote-eligible must try a remote runner first -- only a *fallback to
  // local execution* is refused. The supervisor re-applies this exact check
  // (via `ticket.localRefused`/`ticket.allowLocalSim` below) once/if a
  // remote attempt actually falls back, mirroring how the resource-budget
  // refusal below is skipped here and re-applied on that same fallback path.
  if (resolved.localRefused && !allowLocalSim && !remoteEligible) {
    const refusal = localSimRefusal(resolved.lane);
    process.stderr.write(refusal.message);
    return { exitCode: refusal.exitCode };
  }

  // Shadow mode is observational. In active mode, reject a request that can
  // never fit even on an otherwise idle machine instead of leaving a
  // permanent FIFO head polling forever. Reentrant runs returned above and
  // consume no additional machine reservation.
  //
  // BRAIN-319 T3b-1: skipped here when remoteEligible -- a runner may be
  // larger than this machine, so the local budget is not yet the right
  // refusal to make. The supervisor (T3b-2) re-applies this exact check via
  // `checkResourceBudget` if the run ends up falling back to local.
  const host = detectResourceCapacity();
  if (!remoteEligible) {
    const budget = checkResourceBudget({ resources, globalCfg, host });
    if (!budget.ok) {
      process.stderr.write(budget.message);
      return { exitCode: budget.exitCode };
    }
  }

  const root = ensureStateDirs().root;
  const id = idOverride || crypto.randomUUID();
  if (onTicketCreated) {
    const proceed = await onTicketCreated(id);
    if (proceed === false) {
      return { exitCode: 130 };
    }
  }
  const p = paths(root);
  const logPath = log || path.join(p.logs, `${id}.log`);
  const resultPath = path.join(p.results, `${id}.json`);

  const ticket = {
    id,
    key: resolved.key,
    repoId: resolved.repoId,
    lane: resolved.lane,
    ...(resolved.configLane ? { configLane: resolved.configLane } : {}),
    weight,
    resources,
    nice,
    maxConcurrent: resolved.maxConcurrent,
    conflicts: resolved.conflicts,
    // BRAIN-320: carried so the supervisor's remote-fallback path
    // (`fallbackOrRefuse` in supervisor.js) can re-apply the local-sim
    // refusal if/when a remote-eligible localRefused lane falls back to
    // running locally -- see the immediate-refusal comment above.
    localRefused: resolved.localRefused,
    allowLocalSim: Boolean(allowLocalSim),
    cwd,
    cmd,
    createdAt: Date.now(),
    logPath,
    resultPath,
    // Told explicitly rather than inferred from fd state on the supervisor
    // side (BRAIN-308): a `--detach` caller's own stdio is irrelevant to the
    // supervisor, which never even inherits it (stdio stays 'ignore' below).
    forwardOutput: !detach,
    // BRAIN-347: history provenance, stamped once here and read back by the
    // supervisor's single history-row builder. `headTree` is omitted outside
    // a git repo; it must never fail the run.
    command: cmd.join(' ').slice(0, HISTORY_COMMAND_MAX),
    ...(headTree ? { headTree } : {}),
    ...(localReason ? { localReason } : {}),
  };

  // BRAIN-320 S1d: stamped once, here, from the same Date.now() this
  // function otherwise uses for `createdAt` -- an absolute deadline, so a
  // supervisor that polls for a while before its first tryStart still
  // expires the ticket at the intended wall-clock time, not `startDeadlineMs`
  // after whenever the first poll happens to land.
  if (Number.isFinite(startDeadlineMs) && startDeadlineMs > 0) {
    ticket.startDeadline = ticket.createdAt + startDeadlineMs;
  }

  // BRAIN-319 T3b-1: when eligible, hand the supervisor everything it needs
  // to attempt a remote runner without re-resolving config itself --
  // `worktreeRoot`/`repoKey` in particular must be exactly what this process
  // resolved, not re-derived later against a possibly different cwd.
  // `repoKey` is `resolved.repoId` (config.js): the realpath'd git COMMON
  // dir, which is shared by every worktree of one repo, so the same repo
  // maps to the same key regardless of which worktree dispatched it --
  // `worktreeRoot` stays this specific checkout (what actually gets
  // snapshotted). If the worktree root can't be resolved, this run is not
  // remote-eligible after all: fall through with no `remote` key, exactly
  // today's ticket shape (I6).
  //
  // The supervisor does not act on this field yet -- it always runs the
  // ticket locally regardless of `runners`/`ticket.remote` until T3b-2 wires
  // the remote-dispatch/fallback executor.
  if (remoteEligible) {
    // `remoteWorktreeRoot` is git's own realpath'd toplevel; resolve `cwd`
    // the same way before diffing them, or a symlinked tmp/mount point
    // (e.g. macOS /var -> /private/var) makes an in-root cwd look like it
    // sits many directories outside the root it's actually inside.
    let resolvedCwd;
    try {
      resolvedCwd = fs.realpathSync(cwd);
    } catch {
      resolvedCwd = path.resolve(cwd);
    }
    ticket.remote = {
      worktreeRoot: remoteWorktreeRoot,
      relCwd: path.relative(remoteWorktreeRoot, resolvedCwd) || '',
      repoKey: resolved.repoId,
      weight,
      cpuCores: resources.cpuCores,
      // BRAIN-360: elastic lanes only, so a non-elastic ticket's remote payload is unchanged
      ...(resources.minCpuCores !== undefined ? { minCpuCores: resources.minCpuCores } : {}),
      memoryBytes: resources.memoryBytes,
      // BRAIN-320 S1a: carried through so the supervisor's eligibility hook
      // and (later slice) dispatchRemote's protocol-2 header see exactly what
      // this process resolved, not a re-derived value.
      remoteDeps: resolved.remoteDeps,
      remoteSetup: resolved.remoteSetup,
    };
  }

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
        // A detached caller must never hold the supervisor's pipes -- `ID=$(lane
        // run --detach ...)` has to return immediately, so only a foreground
        // (waiting) caller gets piped stdio to forward (BRAIN-308).
        stdio: detach ? 'ignore' : ['ignore', 'pipe', 'pipe'],
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

  // Foreground only, from here on: tee the supervisor's own stdout/stderr
  // (which is where the supervisor tees the child's output -- see
  // src/supervisor.js) straight through to ours, stream-separated, so a
  // waiting caller sees the child's output live instead of only its exit
  // code (BRAIN-308). `--log` keeps writing the same bytes independently.
  // `.pipe(dest, { end: false })` rather than a manual 'data' handler so
  // dest's own backpressure (its 'drain' event) is respected automatically.
  //
  // Our OWN stdout/stderr can itself break mid-run -- `lane run ... | head
  // -1` closes its read end the moment `head` has what it wants, and the
  // next write here gets an async EPIPE 'error' (pipe() does not handle
  // dest errors for you; an unhandled one would crash this process with a
  // stack trace instead of returning the lane's real exit code). On that
  // error: stop forwarding, and destroy the supervisor's read end too, so
  // its own next write EPIPEs and its ForwardWriter (src/supervisor.js)
  // stops on its own -- the lane keeps running and logging regardless.
  const stopForwarding = (readable, dest) => {
    readable.unpipe(dest);
    readable.destroy();
  };
  const onStdoutError = () => stopForwarding(supervisor.stdout, process.stdout);
  const onStderrError = () => stopForwarding(supervisor.stderr, process.stderr);
  process.stdout.on('error', onStdoutError);
  process.stderr.on('error', onStderrError);
  supervisor.stdout.pipe(process.stdout, { end: false });
  supervisor.stderr.pipe(process.stderr, { end: false });

  // Once we've actually found a result, the supervisor is confirmed exited
  // (finalizeAndExit only writes the result then process.exit()s), so its
  // stdout/stderr pipes are closing right about now -- wait (briefly; pipe
  // writes are async and this must never wedge) for the tail to actually
  // arrive before returning, so a caller piping our output never loses it.
  const finishedReturn = async (exitCode) => {
    await Promise.all([waitForStreamEnd(supervisor.stdout), waitForStreamEnd(supervisor.stderr)]);
    return { exitCode };
  };

  // Every other return path leaves the supervisor running (queued, timed
  // out, or cancelled before it ever started) -- destroy the read ends so a
  // still-running supervisor's open pipe doesn't hold our event loop open.
  const abortReturn = (exitCode) => {
    supervisor.stdout.destroy();
    supervisor.stderr.destroy();
    return { exitCode };
  };

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
        return finishedReturn(resultExit('lane run', result));
      }
      if (deadline && Date.now() > deadline) {
        const state = await describeLaneState(root, id, resultPath);
        if (state.finished) {
          return finishedReturn(resultExit('lane run', state.result));
        }
        process.stderr.write(`lane run: waited ${timeoutMs}ms, not failed — ${state.text}\n`);
        return abortReturn(75);
      }
      if (!isPidAlive(supervisor.pid)) {
        // The supervisor may have written its result and exited in the gap
        // between our last read and this liveness check -- re-read once
        // before concluding it died with nothing to show for it.
        const finalResult = readJsonSafe(resultPath);
        if (finalResult) {
          return finishedReturn(resultExit('lane run', finalResult));
        }
        // The supervisor exited without ever writing a result: it was cancelled
        // while still queued (or crashed) before the child ever ran.
        if (cancelling) {
          process.stderr.write(`lane run: cancelled before the lane started (id ${id})\n`);
          return abortReturn(130);
        }
        process.stderr.write(`lane run: supervisor exited unexpectedly with no result (id ${id})\n`);
        return abortReturn(1);
      }
      await sleep(200);
    }
  } finally {
    process.off('SIGINT', forwardCancel);
    process.off('SIGTERM', forwardCancel);
    // An in-process caller (e.g. a test calling runCommand() directly) shares
    // the real process.stdout/stderr across every call -- leaving these
    // pipes and error listeners attached past our own return would leak onto
    // whichever caller runs next.
    supervisor.stdout.unpipe(process.stdout);
    supervisor.stderr.unpipe(process.stderr);
    process.stdout.off('error', onStdoutError);
    process.stderr.off('error', onStderrError);
  }
}
