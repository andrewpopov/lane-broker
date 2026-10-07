import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { pipeline } from 'node:stream/promises';
import { atomicWriteFile, atomicWriteJson, ensureStateDirs, paths, processStartTime, readJsonSafe, stateHome, writeCancelMarkerFile, writeWithdrawMarkerFile, syncWithdrawMarkers, isWithdrawn, isCancelled, assertNotMigrating, drainBlocksIntake, testDrainAt, testHoldAt, withLock, MigrationInProgressError, isUuid, tombstonePath } from './state.js';
import { checkRemoteDepsDirsOnDisk, manifestHashOf, scrubbedGitEnv, validateRemoteDeps, verifyManifestNoGit } from './remote-manifest.js';
import { isValidRemoteDepsShape, isValidRemoteSetupShape, isValidRemoteArtifactsShape, REMOTE_ARTIFACTS_ON, loadGlobalConfig } from './config.js';
import { CAPABILITIES } from './capabilities.js';
import { NO_PROGRESS_REASON } from './no-progress.js';
import { artifactLimitsOf, collectArtifacts, encodeArtifacts, pruneStaleArtifacts } from './remote-artifacts.js';
import { remotePriorityFrom } from './priority.js';
import { detectResourceCapacity, effectiveWeightCapacity, cpuBudgetCores } from './resources.js';
import { makeReader, readHeaderLine, extractFrames, MAX_HEADER_BYTES } from './remote-stream.js';
import { runCommand } from './run.js';
import { cancelCommand } from './cancel.js';
import { collectStatus } from './status.js';
import { readLease, isSupervisorAlive } from './lease.js';
import { readAttempt } from './attempts.js';
import { listQueue, dequeueSync } from './scheduler.js';
import { REMOTE_TMP_BASE, gcRemoteTickets, removeDetached } from './gc.js';
import { sanitizeObservedCpu, sanitizeRssPeak, sanitizeCpuSeconds } from './observed.js';

// BRAIN-320 S1b (1b): the absolute path to `bin/lane.js`, so a protocol-2
// pipeline's argv (`[node, lane.js, 'remote-pipeline', ticketDir]`) is
// spawnable regardless of the runner's own PATH/cwd.
const laneBinPath = fileURLToPath(new URL('../bin/lane.js', import.meta.url));

// BRAIN-374/BRAIN-376: per-ticket TMPDIR base. /var/tmp is disk-backed per the
// FHS (/tmp is tmpfs on the runners) and short enough that a Unix socket bound
// under os.tmpdir() stays inside sun_path's 107 bytes, which a path under the
// ticket directory (86 bytes before the socket name) does not.

// BRAIN-319 T3: fixed, deterministic author/committer identity for the
// synthetic snapshot commit -- this is never a real authored change, just a
// tree real git commands (status/check-ignore/etc.) can run against.
const SYNTHETIC_GIT_ENV = {
  GIT_AUTHOR_NAME: 'lane-broker-remote-exec',
  GIT_AUTHOR_EMAIL: 'lane-broker-remote-exec@invalid',
  GIT_AUTHOR_DATE: '2020-01-01T00:00:00Z',
  GIT_COMMITTER_NAME: 'lane-broker-remote-exec',
  GIT_COMMITTER_EMAIL: 'lane-broker-remote-exec@invalid',
  GIT_COMMITTER_DATE: '2020-01-01T00:00:00Z',
};

/**
 * Give the extracted, verified snapshot a real (synthetic) git repo, so a
 * consumer whose own command shells out to git (a pre-push hook's `git
 * status --porcelain`, a test's `git check-ignore`, ...) sees a working
 * tree rather than "not a git repository". Runs with git's repo-local env
 * scrubbed (same helper `buildManifest` uses) plus a fixed author/committer
 * identity, hooks disabled, and gpg signing off. Auto-gc is off: a snapshot of
 * a few thousand files makes `git commit` start a detached `gc --auto` that keeps
 * rewriting `.git` (gc.pid, tmp_pack_*, info/refs) into the deps phase. Returns `{ok:true}` or
 * `{ok:false, reason}` -- never throws.
 */
function gitInitSnapshot(workDir) {
  const env = { ...scrubbedGitEnv(), ...SYNTHETIC_GIT_ENV };
  const common = {
    cwd: workDir,
    env,
    stdio: ['ignore', 'ignore', 'pipe'],
  };
  const gitArgs = ['-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', '-c', 'core.autocrlf=false', '-c', 'core.fileMode=true', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false'];
  try {
    execFileSync('git', [...gitArgs, 'init', '-q'], common);
    execFileSync('git', [...gitArgs, 'add', '-A'], common);
    execFileSync('git', [...gitArgs, 'commit', '-q', '-m', 'snapshot'], common);
  } catch (err) {
    const stderr = err.stderr ? err.stderr.toString('utf8').trim() : err.message;
    return { ok: false, reason: `git init/commit failed: ${stderr}` };
  }
  return { ok: true };
}

const REPO_KEY_RE = /^[A-Za-z0-9._:-]{1,200}$/;

// BRAIN-319: kept well under the platform arg/exec limits, generous for any
// real worktree snapshot (zirkbot today: ~2k files, ~29MB total).
const MAX_FILE_BYTES = 256 * 1024 * 1024;
const MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024;

/** Preflight refusal codes `run.js` already returns before a child ever
 *  starts (undeclared lane / resource budget exceeded, local-sim refusal,
 *  bad usage) -- see run.js's own early-return sites. */
const PREFLIGHT_REFUSAL_EXIT_CODES = new Set([64, 69, 2]);

export function defaultRemoteRoot() {
  return path.join(os.homedir(), '.cache', 'lane-broker', 'remote');
}

function isPositiveFiniteOrAbsent(v) {
  return v === undefined || (typeof v === 'number' && Number.isFinite(v) && v > 0);
}

/** '' or a relative posix path with no absolute component and no `..`/empty segment. */
function isValidRelCwd(v) {
  if (v === '') return true;
  if (typeof v !== 'string') return false;
  if (v.startsWith('/')) return false;
  return v.split('/').every((seg) => seg !== '' && seg !== '..');
}

/**
 * Strictly validate a `lane remote-exec` request header (BRAIN-319 T2 step
 * 2a). Every field is checked BEFORE anything is created on disk or handed
 * to the local broker -- a client-supplied value never becomes a path
 * component, a broker key, or a spawn argument without having passed here
 * first. `ticketId` is checked separately by the caller (it must be known
 * safe before it can even be used to build the ticket directory path).
 */
function validateHeaderFields(header) {
  if (header.protocol !== 1 && header.protocol !== 2) {
    return { ok: false, reason: `unsupported protocol: ${header.protocol}` };
  }
  if (!Number.isInteger(header.generation) || header.generation < 0) {
    return { ok: false, reason: 'invalid generation: must be a non-negative integer' };
  }
  if (typeof header.repoKey !== 'string' || !REPO_KEY_RE.test(header.repoKey)) {
    return { ok: false, reason: 'invalid repoKey' };
  }
  if (typeof header.lane !== 'string' || header.lane.length === 0) {
    return { ok: false, reason: 'invalid lane: must be a non-empty string' };
  }
  if (!isPositiveFiniteOrAbsent(header.weight)) return { ok: false, reason: 'invalid weight' };
  if (!isPositiveFiniteOrAbsent(header.cpuCores)) return { ok: false, reason: 'invalid cpuCores' };
  if (!isPositiveFiniteOrAbsent(header.minCpuCores)) return { ok: false, reason: 'invalid minCpuCores' };
  if (!isPositiveFiniteOrAbsent(header.memoryBytes)) return { ok: false, reason: 'invalid memoryBytes' };
  if (!Array.isArray(header.argv) || header.argv.length === 0 || !header.argv.every((a) => typeof a === 'string')) {
    return { ok: false, reason: 'invalid argv: must be a non-empty array of strings' };
  }
  if (!isValidRelCwd(header.relCwd)) return { ok: false, reason: 'invalid relCwd' };
  if (!header.manifest || !Array.isArray(header.manifest.entries)) {
    return { ok: false, reason: 'invalid manifest' };
  }
  let expectedHash;
  try {
    expectedHash = manifestHashOf(header.manifest.entries);
  } catch {
    return { ok: false, reason: 'manifest entries could not be hashed' };
  }
  if (header.manifest.manifestHash !== expectedHash) {
    return { ok: false, reason: 'manifest hash does not match its own entries' };
  }
  // BRAIN-320 S1b (1a/1d): a protocol-1 header behaves EXACTLY as today (I6)
  // -- it must never carry either option. A protocol-2 header MAY carry
  // either, each checked against the exact same shape rules `.lane-
  // broker.json` itself is validated against (src/config.js), so the two
  // ends of this field can never drift apart on what counts as valid.
  if (header.protocol === 1) {
    if (header.remoteDeps !== undefined || header.remoteSetup !== undefined) {
      return { ok: false, reason: 'protocol 1 does not support remoteDeps/remoteSetup' };
    }
  } else if (header.remoteDeps !== undefined && !isValidRemoteDepsShape(header.remoteDeps)) {
    return { ok: false, reason: 'invalid remoteDeps' };
  } else if (header.remoteSetup !== undefined && !isValidRemoteSetupShape(header.remoteSetup)) {
    return { ok: false, reason: 'invalid remoteSetup' };
  }
  // BRAIN-389: the lane's opt-out of the deps cache; only `false` is ever sent, and only with protocol 2 (deps).
  if (header.remoteDepsCache !== undefined && (header.protocol !== 2 || header.remoteDepsCache !== false)) {
    return { ok: false, reason: 'invalid remoteDepsCache' };
  }
  if (header.remoteDepsCacheRootScriptsSafe !== undefined && (header.protocol !== 2 || header.remoteDepsCacheRootScriptsSafe !== true)) {
    return { ok: false, reason: 'invalid remoteDepsCacheRootScriptsSafe' };
  }
  // BRAIN-320 S1d: opt-in on both protocols -- an old runner simply ignores
  // an unknown field, which is fine (the feature is opt-in), but THIS
  // runner, once it understands the field at all, validates it the same way
  // regardless of protocol. Absent is always fine (I6); present must be a
  // positive integer.
  if (header.queueTimeoutMs !== undefined && !(Number.isInteger(header.queueTimeoutMs) && header.queueTimeoutMs > 0)) {
    return { ok: false, reason: 'invalid queueTimeoutMs' };
  }
  // BRAIN-431: the submitter's resolved no-progress timeout (0 = kill off); additive, an older runner ignores it
  if (header.noProgressTimeoutMs !== undefined && !(Number.isInteger(header.noProgressTimeoutMs) && header.noProgressTimeoutMs >= 0)) {
    return { ok: false, reason: 'invalid noProgressTimeoutMs' };
  }
  // BRAIN-398: additive on both protocols (an older runner ignores them; the submitter checks the probe capability first).
  if (header.remoteArtifacts !== undefined && !isValidRemoteArtifactsShape(header.remoteArtifacts)) {
    return { ok: false, reason: 'invalid remoteArtifacts' };
  }
  if (header.remoteArtifactsOn !== undefined && !REMOTE_ARTIFACTS_ON.includes(header.remoteArtifactsOn)) {
    return { ok: false, reason: 'invalid remoteArtifactsOn' };
  }
  return { ok: true };
}

/** `<ticketDir>/phase` as it stood at read time -- deps|setup|command, or
 *  null if the pipeline never got far enough to write one (or this is a
 *  protocol-1 ticket, which never runs the pipeline at all). Read AFTER the
 *  broker's own structured result exists (BRAIN-320 S1b 1b), never from
 *  anything the pipeline itself reports. */
function readPhase(ticketDir) {
  try {
    return fs.readFileSync(path.join(ticketDir, 'phase'), 'utf8');
  } catch {
    return null;
  }
}

/** BRAIN-389: the pipeline's own record of its deps phase (`deps.json`), relayed in the result; absent when there was none. */
function readDeps(ticketDir) {
  return readJsonSafe(path.join(ticketDir, 'deps.json')) ?? undefined;
}

/** BRAIN-425: the runner-side phase timings: the pipeline's `phases.json` (setup, command) plus what remote-exec measured itself. */
function readPhasesMs(ticketDir, own) {
  const fromPipeline = readJsonSafe(path.join(ticketDir, 'phases.json')) ?? {};
  const merged = { ...own, ...fromPipeline };
  return Object.keys(merged).length > 0 ? merged : undefined;
}

function buildResult(header, ticketDir, { kind, exit = null, signal = null, remoteLaneId = null, reason = null, noProgressTimeoutMs, runMs, queuedMs, grantedCpuCores, observedCpu, observedRssPeakBytes, cpuSeconds, phasesMs, artifacts }) {
  const isProtocol2 = header.protocol === 2;
  return {
    protocol: isProtocol2 ? 2 : 1,
    ticketId: header.ticketId,
    generation: header.generation,
    manifestHash: header.manifest?.manifestHash ?? null,
    kind,
    exit,
    signal,
    remoteLaneId,
    reason,
    // BRAIN-431: additive; the window a 'no-progress' kill elapsed, so the submitter can say how long
    noProgressTimeoutMs,
    // Omitted entirely (not merely null) for a protocol-1 result -- JSON.stringify
    // drops an `undefined` value, so the shape stays byte-identical to before
    // this slice (I6).
    phase: isProtocol2 ? readPhase(ticketDir) : undefined,
    // BRAIN-389: additive; an older client ignores it
    deps: isProtocol2 ? readDeps(ticketDir) : undefined,
    finishedAt: Date.now(),
    // BRAIN-341: a duration on the runner's own clock (never an absolute
    // time, so clock skew cannot matter). Omitted when unknown or for an
    // older runner, so the client records waitedMs null instead of a guess.
    runMs,
    // BRAIN-363: how long this exec waited on the runner (receiving the snapshot, then the runner's queue and any deps/setup
    // phase) before the command started, also a duration on the runner's own clock. Additive; omitted by an older runner.
    queuedMs,
    // BRAIN-360: additive; only an elastic lane's runner-side grant (an old submitter ignores it)
    grantedCpuCores,
    // BRAIN-361: additive; what the lane actually used on the runner, relayed to the submitter's history
    observedCpu,
    observedRssPeakBytes,
    // BRAIN-425: additive; CPU seconds and phase wall times measured on the runner
    cpuSeconds,
    phasesMs,
    // BRAIN-398: additive summary of the stored artifacts; the bytes travel separately (`lane remote-artifacts`)
    artifacts,
  };
}

/**
 * BRAIN-398: store the files the lane declared, when this run's outcome and `remoteArtifactsOn` say to. Returns the
 * summary for the result (`undefined` when the policy skipped it). A collection failure is data, not an error: the
 * result stays exactly what the command produced.
 */
function storeArtifacts(header, ticketDir, workDir, { kind, exit, signal }) {
  if (header.remoteArtifacts === undefined || kind !== 'completed') return undefined;
  // Everything artifact-shaped, config reload included, answers for itself: it may only ever produce a warning in
  // the result, never stop the result (with the command's true exit) from being published.
  try {
    const commandSucceeded = exit === 0 && !signal && (header.protocol !== 2 || readPhase(ticketDir) === 'command');
    if ((header.remoteArtifactsOn ?? 'success') === 'success' && !commandSucceeded) return undefined;
    return collectArtifacts(workDir, header.remoteArtifacts, path.join(ticketDir, 'artifacts'), artifactLimitsOf(loadGlobalConfig()));
  } catch (err) {
    return { ok: false, reason: `could not collect artifacts: ${err.message}` };
  }
}

function writeResult(ticketDir, result) {
  atomicWriteJson(path.join(ticketDir, 'result.json'), result);
}

// BRAIN-374: the runner's own /tmp can be RAM-backed, so every phase gets a
// per-ticket TMPDIR on disk instead, removed with the work dir.
function cleanupWork(workDir, tmpDir) {
  for (const dir of [workDir, tmpDir]) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {
      // best-effort; a leaked dir under a per-ticket directory is harmless
    }
  }
}

/**
 * BRAIN-438: reclaim what earlier tickets left on this runner before taking a new one. Bounded per run and removed in a
 * detached `rm`, so the submitter's ssh session never waits on it; a failure here (lock timeout, bad config) is never a
 * reason to refuse the ticket.
 */
async function collectGarbage(root) {
  try {
    const cfg = loadGlobalConfig();
    await gcRemoteTickets({ remoteRoot: root, retentionMs: cfg.remoteTicketRetentionMs, maxTickets: cfg.remoteGcMaxTicketsPerRun, remove: removeDetached });
  } catch {
    // best-effort
  }
}

/**
 * `lane remote-exec` (BRAIN-319 T2): read a framed snapshot stream (header +
 * body, see remote-stream.js) off `stdin`, extract and verify it into a
 * fresh per-ticket work dir, then run the requested argv through THIS
 * host's own broker as a brand-new ticket -- never the reentrant path (I4),
 * regardless of what env the caller's ssh session forwarded. Writes exactly
 * one of `<root>/tickets/<ticketId>/result.json` (exit 0) or nothing (exit
 * nonzero) -- see the module-level comment on kind derivation below.
 */
export async function remoteExecCommand({ root = defaultRemoteRoot(), stdin = process.stdin } = {}) {
  const execStartedAt = Date.now();
  // BRAIN-320 review fix A: resolve once, up front -- every path derived
  // below (ticketDir, workDir, and the protocol-2 pipeline argv) is passed
  // to a child that runs with a DIFFERENT cwd (the extracted work dir, see
  // `cwd:` in the runCommand call below), so a relative `root` must never
  // reach any of those derivations un-resolved.
  root = path.resolve(root);
  // BRAIN-380: a runner mid-`lane migrate-scheduler` takes no new work. Exit 75 before reading the header or making a
  // ticket directory, so nothing is left behind for the client to reconcile. (`root` is the remote ticket root, not the broker's.)
  try {
    assertNotMigrating(stateHome());
  } catch (err) {
    if (!(err instanceof MigrationInProgressError)) throw err;
    process.stderr.write(`lane remote-exec: ${err.message}\n`);
    return { exitCode: 75 };
  }
  // BRAIN-319 I4: this process is invoked by ssh as a fresh node process
  // after any login-shell startup already ran, so scrubbing here — rather
  // than relying on `env -u` upstream — satisfies "after shell startup":
  // nothing later in this process can be handed a forwarded lease/key, and
  // the freshly spawned supervisor's child inherits LANE_BROKER_LOCAL=1
  // rather than any inherited lease identity.
  delete process.env.LANE_BROKER_LEASE;
  delete process.env.LANE_BROKER_KEY;
  delete process.env.LANE_BROKER_TICKET;
  // BRAIN-380: a remote-exec ticket's tier comes only from the dispatch header (none yet, so medium), never the runner shell.
  delete process.env.LANE_BROKER_PRIORITY;
  process.env.LANE_BROKER_LOCAL = '1';

  // BRAIN-320 review fix C: also scrub git's own repo-local env vars
  // (GIT_DIR, GIT_WORK_TREE, GIT_INDEX_FILE, ..., plus GIT_QUARANTINE_PATH)
  // out of this process's env, same list `scrubbedGitEnv` derives for
  // `buildManifest` -- otherwise every deps/setup/command child this runner
  // spawns (not just the synthetic `git init` above) inherits whatever repo
  // the runner's own shell happened to be sitting in.
  const scrubbed = scrubbedGitEnv();
  for (const name of Object.keys(process.env)) {
    if (!(name in scrubbed)) delete process.env[name];
  }

  await collectGarbage(root);

  const reader = makeReader(stdin);
  const headerResult = await readHeaderLine(reader, MAX_HEADER_BYTES);
  if (!headerResult.ok) {
    process.stderr.write(`lane remote-exec: ${headerResult.reason}\n`);
    return { exitCode: 1 };
  }
  const header = headerResult.header;

  // ticketId is checked in isolation, before it is ever used as a path
  // component: an invalid one (e.g. `../x`) must never even get as far as
  // building a ticket directory path, so there is nowhere to safely record
  // a result for it either.
  if (!header || !isUuid(header.ticketId)) {
    process.stderr.write('lane remote-exec: missing or invalid ticketId\n');
    return { exitCode: 1 };
  }

  // BRAIN-380: the check at the top ran before a (possibly slow) header read, so a drain may have started since. The check
  // and the ticket directory (the intake commit point) are ONE locked step: nothing is created after a drain publishes, and a
  // drain that publishes afterwards finds this live `lane remote-exec` in its process check and waits for it.
  testDrainAt(stateHome(), 'remote-exec');
  const ticketsDir = path.join(root, 'tickets');
  const ticketDir = path.join(ticketsDir, header.ticketId);
  try {
    await withLock(stateHome(), () => {
      assertNotMigrating(stateHome());
      // BRAIN-436: a cancel/withdraw that overtook this exec left a tombstone under this same lock, so it loses the race.
      if (fs.existsSync(tombstonePath(root, header.ticketId))) throw new TombstonedError();
      fs.mkdirSync(ticketsDir, { recursive: true });
      fs.mkdirSync(ticketDir);
    });
  } catch (err) {
    if (err instanceof TombstonedError) {
      process.stderr.write(`lane remote-exec: ticket ${header.ticketId} was cancelled before it arrived; not creating it\n`);
      return { exitCode: 1 };
    }
    if (err instanceof MigrationInProgressError) {
      process.stderr.write(`lane remote-exec: ${err.message}\n`);
      return { exitCode: 75 };
    }
    if (err.code === 'EEXIST') {
      process.stderr.write(`lane remote-exec: ticket directory already exists: ${ticketDir}\n`);
      return { exitCode: 1 };
    }
    throw err;
  }
  pruneStaleArtifacts(ticketsDir);
  await testHoldAt('remote-exec-committed');
  // Node's default for SIGHUP is to exit, but this process must outlive a dropped ssh session
  // to publish result.json; explicit cancellation goes through remote-cancel.
  process.on('SIGHUP', () => {});
  // BRAIN-339: recorded before anything slow (extraction, git init, preflight) so
  // `remote-result` can tell whether this process, the only publisher of result.json, is alive.
  atomicWriteJson(path.join(ticketDir, 'publisher.json'), {
    pid: process.pid,
    start: processStartTime(process.pid),
  });

  const fieldCheck = validateHeaderFields(header);
  if (!fieldCheck.ok) {
    writeResult(ticketDir, buildResult(header, ticketDir, { kind: 'rejected', reason: fieldCheck.reason }));
    process.stderr.write(`lane remote-exec: ${fieldCheck.reason}\n`);
    return { exitCode: 0 };
  }

  const workDir = path.join(ticketDir, 'work');
  // Every phase env is derived from this process's env (buildDepsEnv, the
  // pipeline's setup/command children, and a protocol-1 argv via runCommand),
  // so setting it once here covers them all.
  const tmpDir = fs.mkdtempSync(path.join(REMOTE_TMP_BASE, 'lb-'));
  atomicWriteFile(path.join(ticketDir, 'tmp-dir'), tmpDir);
  process.env.TMPDIR = tmpDir;
  process.env.TMP = tmpDir;
  process.env.TEMP = tmpDir;
  const snapshotStartedAt = Date.now();
  const extractResult = await extractFrames(reader, workDir, header.manifest, {
    maxFileBytes: MAX_FILE_BYTES,
    maxTotalBytes: MAX_TOTAL_BYTES,
  });
  if (!extractResult.ok) {
    writeResult(ticketDir, buildResult(header, ticketDir, { kind: 'rejected', reason: extractResult.reason }));
    process.stderr.write(`lane remote-exec: ${extractResult.reason}\n`);
    cleanupWork(workDir, tmpDir);
    return { exitCode: 0 };
  }

  // BRAIN-319 T3: give the snapshot a real (synthetic) git repo so a
  // consumer's own git commands (pre-push hook `git status`, `git
  // check-ignore`, ...) work against it -- then re-verify the tree against
  // the manifest, ignoring exactly the `.git` git init/add/commit created,
  // so a tree mutation that happened DURING that step (rather than being a
  // side effect of it) is still caught.
  const gitResult = gitInitSnapshot(workDir);
  if (!gitResult.ok) {
    writeResult(ticketDir, buildResult(header, ticketDir, { kind: 'rejected', reason: gitResult.reason }));
    process.stderr.write(`lane remote-exec: ${gitResult.reason}\n`);
    cleanupWork(workDir, tmpDir);
    return { exitCode: 0 };
  }
  const postGitVerify = verifyManifestNoGit(workDir, header.manifest, { ignoreRootGit: true });
  if (!postGitVerify.ok) {
    writeResult(ticketDir, buildResult(header, ticketDir, { kind: 'rejected', reason: postGitVerify.reason }));
    process.stderr.write(`lane remote-exec: ${postGitVerify.reason}\n`);
    cleanupWork(workDir, tmpDir);
    return { exitCode: 0 };
  }

  // BRAIN-320 S1b (1c): re-run `validateRemoteDeps` against the EXTRACTED
  // tree's manifest for a protocol-2 header carrying `remoteDeps`, then
  // lstat-walk each dir on disk -- `validateRemoteDeps` only ever inspects
  // the manifest, which cannot see what the runner's own filesystem looks
  // like right now. Any violation is a rejection, and the command (or any
  // phase of it) never runs.
  if (header.protocol === 2 && Array.isArray(header.remoteDeps)) {
    const depsManifestCheck = validateRemoteDeps(header.manifest, header.remoteDeps);
    if (!depsManifestCheck.ok) {
      writeResult(ticketDir, buildResult(header, ticketDir, { kind: 'rejected', reason: depsManifestCheck.reason }));
      process.stderr.write(`lane remote-exec: ${depsManifestCheck.reason}\n`);
      cleanupWork(workDir, tmpDir);
      return { exitCode: 0 };
    }
    const depsDiskCheck = checkRemoteDepsDirsOnDisk(workDir, header.remoteDeps);
    if (!depsDiskCheck.ok) {
      writeResult(ticketDir, buildResult(header, ticketDir, { kind: 'rejected', reason: depsDiskCheck.reason }));
      process.stderr.write(`lane remote-exec: ${depsDiskCheck.reason}\n`);
      cleanupWork(workDir, tmpDir);
      return { exitCode: 0 };
    }
  }

  const ownPhasesMs = { snapshotMs: Date.now() - snapshotStartedAt };
  const cancelledMarker = path.join(ticketDir, 'cancelled');
  const remoteLaneId = crypto.randomUUID();

  // Belt and braces alongside `configRoot`/`repoIdentityOverride` below: no
  // git invocation the child (or anything it shells out to) makes can ever
  // resolve a repo above this ticket's own directory.
  process.env.GIT_CEILING_DIRECTORIES = ticketDir;

  // BRAIN-320 S1b (1b): a protocol-2 header runs through ONE broker-owned
  // pipeline (deps -> setup -> command) as this ticket's own child, instead
  // of running the caller's argv directly -- admission, the resource grant,
  // nice, and cancellation (a process-group kill) then cover every phase
  // and every descendant. `pipeline.json` is written BEFORE runCommand is
  // ever called, so it exists the instant the pipeline's own child could
  // start reading it.
  let pipelineCmd = header.argv;
  if (header.protocol === 2) {
    const remoteRootAbs = path.resolve(root);
    const runnerCfg = loadGlobalConfig();
    atomicWriteJson(path.join(ticketDir, 'pipeline.json'), {
      workDir,
      relCwd: header.relCwd || '',
      remoteDeps: header.remoteDeps || [],
      remoteSetup: header.remoteSetup || [],
      argv: header.argv,
      npmCacheDir: path.join(remoteRootAbs, 'npm-cache'),
      npmUserConfig: path.join(remoteRootAbs, 'npmrc'),
      // BRAIN-389: the runner's own config decides whether and how far to cache; the lane can only opt out.
      depsCache: {
        enabled: header.remoteDepsCache !== false && runnerCfg.remoteDepsCache,
        rootScriptsSafe: header.remoteDepsCacheRootScriptsSafe === true,
        root: path.join(remoteRootAbs, 'deps-cache'),
        maxBytes: runnerCfg.remoteDepsCacheMaxBytes,
      },
    });
    pipelineCmd = [process.execPath, laneBinPath, 'remote-pipeline', ticketDir];
  }

  const remotePriority = remotePriorityFrom(header);
  const runOutcome = await runCommand({
    repo: header.repoKey,
    lane: header.lane,
    // BRAIN-320: a `localRefused` lane must never be refused HERE -- this
    // IS the remote runner a localRefused lane is trying to reach by being
    // dispatched at all; refusing it again on this side would make every
    // remote-eligible localRefused lane refuse twice over.
    allowLocalSim: true,
    // BRAIN-380 §6: priority comes ONLY from the validated header, never this shell's LANE_BROKER_PRIORITY. The tier is
    // what the submitter asked for; this runner's own enqueue applies its own cap. The accrued wait is anchored on this
    // runner's own priority clock in runCommand, so the submitter's timestamps are never read.
    priority: remotePriority.priority,
    priorityAccruedMs: remotePriority.accruedMs,
    weightOverride: header.weight,
    cpuOverride: header.cpuCores,
    minCpuOverride: header.minCpuCores,
    memoryOverride: header.memoryBytes,
    // An older submitter sends no timeout: the kill is opt-in, so absent means disabled, never this runner's own default.
    noProgressTimeoutOverride: Number.isFinite(header.noProgressTimeoutMs) ? header.noProgressTimeoutMs : 0,
    // Unchanged by protocol: `.lane-broker.json` resolution (via
    // `configRoot` below) walks from this same cwd, and a protocol-2
    // pipeline resolves its own phase cwds from `pipeline.json`'s absolute
    // paths regardless of what this process's own cwd happens to be.
    cwd: path.join(workDir, header.relCwd || ''),
    cmd: pipelineCmd,
    // BRAIN-319 C7: resolve `.lane-broker.json` from the snapshot itself,
    // never above it (the snapshot has no `.git` to derive a stop point from).
    configRoot: workDir,
    // BRAIN-319 T3 (C7): the synthetic git repo created above must never
    // change the lease key -- pin it to repoKey regardless.
    repoIdentityOverride: header.repoKey,
    idOverride: remoteLaneId,
    // BRAIN-320 S1d: opt-in queue timeout, relative ms, straight from the
    // validated header field -- see run.js's own doc comment on
    // `startDeadlineMs` for how it becomes an absolute deadline.
    startDeadlineMs: header.queueTimeoutMs,
    // BRAIN-319 C5: fires as soon as the id exists, before the supervisor
    // is ever spawned (run.js's own hook point) -- this is what makes
    // "remote-id exists" and "the child could have started" the same fact.
    onTicketCreated: async (id) => {
      atomicWriteFile(path.join(ticketDir, 'remote-id'), id);
      if (fs.existsSync(cancelledMarker)) return false;
      // Test-only seam (BRAIN-319): hold here, right after the ticket-local
      // `cancelled` check above has already come back false, until the named
      // file appears -- lets a test deterministically land `lane
      // remote-cancel` in the exact window this ticket-local check cannot
      // see: after it has already run, but before the supervisor (which
      // run.js has not spawned yet, since it's still awaiting this callback)
      // has enqueued or leased the ticket with the LOCAL BROKER. Only the
      // broker's own cancel marker (written by remote-cancel, checked again
      // by scheduler.js's tryStart under its admission lock) can still catch
      // a cancellation requested in this exact window.
      const pauseFile = process.env.LANE_BROKER_TEST_PAUSE_AFTER_TICKET_ID;
      if (pauseFile) {
        while (!fs.existsSync(pauseFile)) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
      }
      return true;
    },
  });

  // Kind is derived from the LOCAL broker's own structured result record for
  // remoteLaneId (BRAIN-319 C6) -- never from runOutcome.exitCode, which
  // collapses a real signal into a bare "1" and cannot distinguish "the
  // child ran and got a signal" from "the broker refused before starting
  // one". The one case with no such record at all is a preflight refusal
  // (run.js returns before ever calling idOverride/onTicketCreated), where
  // runOutcome.exitCode is the only signal there is.
  const localRoot = ensureStateDirs().root;
  const structured = readJsonSafe(path.join(paths(localRoot).results, `${remoteLaneId}.json`));
  const cancelled = fs.existsSync(cancelledMarker);

  let kind;
  let exit = null;
  let signal = null;
  let reason = null;
  let noProgressTimeoutMs;
  let runMs;
  let queuedMs;
  let grantedCpuCores;
  let observedCpu;
  let observedRssPeakBytes;
  let cpuSeconds;
  // A user cancel that landed after the expiry still wins (kind cancelled below).
  if (structured && structured.reason === 'withdrawn' && !cancelled) {
    // BRAIN-436: the submitter took this QUEUED ticket back to move it to another runner. Nothing ran (no deps, no
    // setup, no command), so `unfinished` + `reason` is the positive proof of that the client binds to.
    kind = 'unfinished';
    reason = 'withdrawn';
  } else if (structured && structured.reason === 'queue-timeout' && !cancelled) {
    // BRAIN-320 S1d: the LOCAL broker's own scheduler expired this ticket
    // before it was ever admitted -- never 'completed' (it never ran), so
    // this check comes BEFORE the general `structured` branch below. The
    // client's `classifyRemoteResult` treats 'unfinished' as unconfirmed and
    // falls back locally; `reason` carries 'queue-timeout' through for the
    // fallback log line.
    kind = 'unfinished';
    reason = 'queue-timeout';
  } else if (structured) {
    exit = structured.exit;
    signal = structured.signal;
    if (structured.reason === NO_PROGRESS_REASON && Number.isFinite(structured.noProgressTimeoutMs)) {
      reason = NO_PROGRESS_REASON;
      noProgressTimeoutMs = structured.noProgressTimeoutMs;
    }
    if (Number.isFinite(structured.grantedCpuCores)) grantedCpuCores = structured.grantedCpuCores;
    observedCpu = sanitizeObservedCpu(structured.observedCpu) ?? undefined;
    observedRssPeakBytes = sanitizeRssPeak(structured.observedRssPeakBytes);
    cpuSeconds = sanitizeCpuSeconds(structured.cpuSeconds);
    if (Number.isFinite(structured.startedAt) && Number.isFinite(structured.endedAt)) {
      runMs = Math.max(0, structured.endedAt - structured.startedAt);
    }
    if (Number.isFinite(structured.startedAt)) queuedMs = Math.max(0, structured.startedAt - execStartedAt);
    kind = cancelled ? 'cancelled' : 'completed';
  } else if (cancelled) {
    kind = 'cancelled';
  } else if (PREFLIGHT_REFUSAL_EXIT_CODES.has(runOutcome.exitCode)) {
    kind = 'refused';
    // BRAIN-319 T3a: record the actual preflight refusal code (64/69/2) so a
    // client can trust it as the exit status -- there is no structured
    // result record for a preflight refusal (run.js returns before ever
    // calling idOverride/onTicketCreated), so runOutcome.exitCode is the
    // only place this value exists.
    exit = runOutcome.exitCode;
  } else {
    kind = 'unfinished';
  }

  // a protocol-1 run has no pipeline to time its phases: the whole run is the command
  if (header.protocol !== 2 && runMs !== undefined) ownPhasesMs.commandMs = runMs;
  const artifactsStartedAt = Date.now();
  const artifacts = storeArtifacts(header, ticketDir, workDir, { kind, exit, signal });
  if (artifacts !== undefined) ownPhasesMs.artifactsMs = Date.now() - artifactsStartedAt;
  try {
    writeResult(ticketDir, buildResult(header, ticketDir, { kind, exit, signal, remoteLaneId, reason, noProgressTimeoutMs, runMs, queuedMs, grantedCpuCores, observedCpu, observedRssPeakBytes, cpuSeconds, phasesMs: readPhasesMs(ticketDir, ownPhasesMs), artifacts }));
  } finally {
    cleanupWork(workDir, tmpDir);
  }
  return { exitCode: 0 };
}

/**
 * BRAIN-405: this host's real free CPU and memory. CPU is the budget minus the larger of the cores its leases reserve and its
 * 1-minute load average (which also sees load the broker did not admit); read-only, so it never disturbs admission's CPU
 * baseline. Memory is what the OS reports available minus the broker's reserve. Null when the budgets are not enforced
 * (shadow mode) or the measurement is missing: the client then treats the runner as having room, as it does for capacity.
 */
export function probeHeadroom(resources, cfg, enforced, loadAvg1 = os.loadavg()[0]) {
  if (!enforced || !resources) return null;
  const busy = Math.max(Number.isFinite(resources.reservedCpuCores) ? resources.reservedCpuCores : 0, Number.isFinite(loadAvg1) ? loadAvg1 : 0);
  return {
    cpuCores: Math.max(0, resources.cpuBudgetCores - busy),
    memoryBytes: Number.isFinite(resources.availableMemoryBytes) ? Math.max(0, resources.availableMemoryBytes - cfg.memoryReserveBytes) : null,
  };
}

/** `lane remote-probe`: one JSON line describing this host's local broker,
 *  reusing status.js's own reader rather than a second implementation. */
export async function remoteProbeCommand({ root = defaultRemoteRoot() } = {}) {
  pruneStaleArtifacts(path.join(path.resolve(root), 'tickets'));
  const status = await collectStatus();
  const pkgPath = fileURLToPath(new URL('../package.json', import.meta.url));
  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
  // BRAIN-320 S1a (1d/1e): `protocols` is the new negotiation field ([1, 2]
  // -- this runner understands protocol 2 too); `protocol: 1` stays for a
  // client built before this field existed (I6). Capacity is STATIC (this
  // host's budget, not current load) and reuses the broker's own
  // capacity/budget math -- never re-derived here -- so the client's fit
  // check (1e) agrees with what admission would actually apply.
  const globalCfg = loadGlobalConfig();
  const host = detectResourceCapacity();
  // CPU/memory budgets are only enforced in active mode (checkResourceBudget);
  // in shadow mode they are reported as null so the client never skips on them.
  const enforced = globalCfg.schedulerMode === 'active';
  const draining = drainBlocksIntake(status.draining);
  const payload = {
    protocol: 1,
    protocols: [1, 2],
    // BRAIN-360: this runner resolves minCpuCores itself, so a submitter may judge fit by the floor
    capabilities: CAPABILITIES,
    version: pkg.version,
    // BRAIN-380 slice 4: a draining runner takes no new work. `draining` names why; `paused` is also set so a client
    // built before this field skips the runner the way it skips a paused one.
    draining,
    paused: Boolean(status.paused) || draining,
    queued: status.queued.length,
    running: status.running.length,
    // BRAIN-360: additive; the CPU the runner's leases are charged (grants, not declarations)
    reservedCpuCores: status.resources?.reservedCpuCores,
    // BRAIN-405: what a new ticket could be given RIGHT NOW (unlike `capacity`, which is static); null where unmeasurable
    headroom: probeHeadroom(status.resources, globalCfg, enforced),
    capacity: {
      weight: effectiveWeightCapacity(globalCfg, host.cpuCores),
      cpuCores: enforced ? cpuBudgetCores(host, globalCfg) : null,
      memoryBytes: enforced ? host.memoryBytes : null,
      memoryReserveBytes: enforced ? globalCfg.memoryReserveBytes : null,
    },
  };
  process.stdout.write(`${JSON.stringify(payload)}\n`);
  return { exitCode: 0 };
}

/**
 * BRAIN-339: liveness of a ticket that has no result.json. It is decided by the one process
 * that will ever publish that file -- the `remote-exec` that recorded itself in
 * `publisher.json` when it created the ticket dir: alive means pid alive with the same
 * start time (fail-closed, via `isSupervisorAlive`). The label is `queued` when the ticket's
 * runner-side lane is in the broker queue, else `running`; a client treats both as alive.
 * No publisher.json (an older runner) or a dead publisher is `gone`. Accepted limit: if
 * remote-exec dies while its detached supervisor survives, this reads `gone`, which is right
 * since nothing would publish. Never throws, never creates directories.
 */
export function remoteTicketState(ticketDir, brokerRoot) {
  const publisher = readJsonSafe(path.join(ticketDir, 'publisher.json'));
  if (!publisher) return 'gone';
  if (!isSupervisorAlive({ supervisorPid: publisher.pid, supervisorStart: publisher.start })) return 'gone';
  let laneId = null;
  try {
    laneId = fs.readFileSync(path.join(ticketDir, 'remote-id'), 'utf8').trim();
  } catch {
    // pre-admission phases: no lane yet
  }
  if (!laneId) return 'preparing';
  if (readLease(brokerRoot, laneId)) return 'admitted';
  return listQueue(brokerRoot).some((t) => t && t.id === laneId) ? 'queued' : 'preparing';
}

/** The `state` a client built before BRAIN-436 understands: alive is `queued` or `running`, so the two phases it has no
 *  word for (`preparing`, `admitted`) both read `running`, exactly as before. The precise label travels as `phase`, present only where it differs from `state`. */
const legacyState = (phase) => (phase === 'preparing' || phase === 'admitted' ? 'running' : phase);

/**
 * BRAIN-436: the whole withdrawal decision, made UNDER the broker admission lock (the one `tryStart` admits under), so a
 * ticket is withdrawn XOR admitted, never both. In order: an existing withdraw marker answers `withdrawn` (idempotent),
 * then cancelled, then a lease (`started`), then not-in-the-queue (`not-queued`). The marker write is the commit -- it is
 * fsynced, NEVER removed or rolled back, and `tryStart` refuses any ticket that has it. The dequeue after it is best-effort
 * cleanup: if it fails, the runner supervisor's own finalize dequeues (retrying) before it publishes.
 */
export async function withdrawLane(brokerRoot, laneId, { dequeue = dequeueSync } = {}) {
  return withLock(brokerRoot, async () => {
    if (isWithdrawn(brokerRoot, laneId)) {
      syncWithdrawMarkers(brokerRoot);
      return 'withdrawn';
    }
    // The supervisor clears the marker once it has published this result, which can be before the remote-exec result
    // exists; the published broker result is the same fact, so a retry in that window still answers `withdrawn`.
    const published = readJsonSafe(path.join(paths(brokerRoot).results, `${laneId}.json`));
    if (published?.reason === 'withdrawn' && published.cancelled === false) return 'withdrawn';
    try {
      assertNotMigrating(brokerRoot);
    } catch (err) {
      if (err instanceof MigrationInProgressError) return 'not-queued';
      throw err;
    }
    if (isCancelled(brokerRoot, laneId)) return 'cancelled';
    if (readLease(brokerRoot, laneId)) return 'started';
    if (!listQueue(brokerRoot).some((t) => t && t.id === laneId)) return 'not-queued';
    await testHoldAt('remote-withdraw-checked');
    writeWithdrawMarkerFile(brokerRoot, laneId);
    try {
      dequeue(brokerRoot, laneId);
    } catch {
      // the marker already decided it; the supervisor's finalize removes the file
    }
    return 'withdrawn';
  });
}

/** BRAIN-436: a cancel or withdraw for a ticket this runner has never heard of (its exec is still in flight, or never came). */
class TombstonedError extends Error {}

/**
 * Record, under the broker lock `remote-exec` creates tickets under, that `ticketId` must never be created -- unless its ticket
 * directory already exists by the time the lock is held (then the caller handles it as an ordinary ticket). Resolves true iff
 * the tombstone was written, which is the proof a later `remote-exec` for that id is refused.
 */
async function tombstoneAbsentTicket(root, ticketId) {
  return withLock(stateHome(), () => {
    if (fs.existsSync(path.join(path.resolve(root), 'tickets', ticketId))) return false;
    atomicWriteFile(tombstonePath(root, ticketId), String(Date.now()));
    return true;
  });
}

/**
 * `lane remote-withdraw <ticketId>` (BRAIN-436): take a ticket that is still QUEUED on this runner back, irreversibly, so
 * its submitter can run it elsewhere. Prints `{protocol, ticketId, action}` and exits 0 for every decided action; the
 * action is `withdrawn` only once the withdraw marker is durable. Anything else means the ticket stays here.
 */
export async function remoteWithdrawCommand(ticketId, { root = defaultRemoteRoot(), brokerRoot, dequeue } = {}) {
  if (!isUuid(ticketId)) {
    process.stderr.write('lane remote-withdraw: missing or invalid ticketId\n');
    return { exitCode: 2 };
  }
  const ticketDir = path.join(path.resolve(root), 'tickets', ticketId);
  const reply = (action) => {
    process.stdout.write(`${JSON.stringify({ protocol: 1, ticketId, action })}\n`);
    return { exitCode: 0 };
  };
  if (!fs.existsSync(ticketDir)) {
    // The exec for this id may still be on its way: make it lose the race. (A ticket that appeared meanwhile is handled below.)
    if (await tombstoneAbsentTicket(root, ticketId)) {
      process.stdout.write(`${JSON.stringify({ protocol: 1, ticketId, action: 'no-such-ticket', tombstoned: true })}\n`);
      return { exitCode: 0 };
    }
  }
  let laneId = null;
  try {
    laneId = fs.readFileSync(path.join(ticketDir, 'remote-id'), 'utf8').trim();
  } catch {
    // not enqueued yet
  }
  if (!isUuid(laneId)) return reply(readJsonSafe(path.join(ticketDir, 'result.json')) ? 'finished' : 'not-queued');
  const action = await withdrawLane(brokerRoot ?? ensureStateDirs().root, laneId, { dequeue });
  if (action !== 'not-queued') return reply(action);
  // Not queued: either the ticket is done, or it has not been enqueued yet. A finished ticket's withdraw marker is gone
  // with its other markers, so a retry still reads `withdrawn` off the published result.
  const result = readJsonSafe(path.join(ticketDir, 'result.json'));
  if (!result) return reply('not-queued');
  return reply(result.reason === 'withdrawn' && result.kind === 'unfinished' ? 'withdrawn' : 'finished');
}

/** `lane remote-result <ticketId>`: print result.json, or {missing:true, state} where state is
 *  queued|running|gone (is the publishing remote-exec alive) (BRAIN-339; `missing:true` stays for older clients). */
export async function remoteResultCommand(ticketId, { root = defaultRemoteRoot(), readResult = readJsonSafe } = {}) {
  if (!isUuid(ticketId)) {
    process.stderr.write('lane remote-result: missing or invalid ticketId\n');
    return { exitCode: 2 };
  }
  root = path.resolve(root);
  const ticketDir = path.join(root, 'tickets', ticketId);
  const resultPath = path.join(ticketDir, 'result.json');
  let record = readResult(resultPath);
  if (!record) {
    const phase = remoteTicketState(ticketDir, stateHome());
    const state = legacyState(phase);
    // The publisher may have written result.json and exited between the read above and the
    // liveness check; re-read once before concluding it died with nothing to show (as run.js does).
    record = (state === 'gone' && readResult(resultPath)) || { protocol: 1, missing: true, state, ...(phase !== state ? { phase } : {}) };
  }
  process.stdout.write(`${JSON.stringify(record)}\n`);
  return { exitCode: 0 };
}

/** `lane remote-artifacts <ticketId>` (BRAIN-398): stream the stored artifacts, framed; nonzero and silent when there are none. */
export async function remoteArtifactsCommand(ticketId, { root = defaultRemoteRoot(), stdout = process.stdout } = {}) {
  if (!isUuid(ticketId)) {
    process.stderr.write('lane remote-artifacts: missing or invalid ticketId\n');
    return { exitCode: 2 };
  }
  const stream = encodeArtifacts(path.join(path.resolve(root), 'tickets', ticketId, 'artifacts'), ticketId);
  if (!stream) {
    process.stderr.write('lane remote-artifacts: no artifacts stored for this ticket\n');
    return { exitCode: 1 };
  }
  try {
    await pipeline(stream, stdout, { end: false });
  } catch (err) {
    process.stderr.write(`lane remote-artifacts: ${err.message}\n`);
    return { exitCode: 1 };
  }
  return { exitCode: 0 };
}

/** `lane remote-artifacts-release <ticketId>`: delete the stored copies once the submitter has them. Idempotent. */
export async function remoteArtifactsReleaseCommand(ticketId, { root = defaultRemoteRoot() } = {}) {
  if (!isUuid(ticketId)) {
    process.stderr.write('lane remote-artifacts-release: missing or invalid ticketId\n');
    return { exitCode: 2 };
  }
  fs.rmSync(path.join(path.resolve(root), 'tickets', ticketId, 'artifacts'), { recursive: true, force: true });
  return { exitCode: 0 };
}

/**
 * `lane remote-cancel <ticketId>`: write the cancel marker (only if the
 * ticket directory already exists — this must never CREATE one), then, if a
 * broker ticket id has already been recorded, cancel it via the existing
 * `lane cancel` machinery. Idempotent: calling it twice, or before/after
 * `remote-id` exists, is always safe (BRAIN-319 C5's ordering guarantee).
 */
export async function remoteCancelCommand(ticketId, { root = defaultRemoteRoot() } = {}) {
  if (!isUuid(ticketId)) {
    process.stderr.write('lane remote-cancel: missing or invalid ticketId\n');
    return { exitCode: 2 };
  }
  root = path.resolve(root);
  const ticketDir = path.join(root, 'tickets', ticketId);
  if (!fs.existsSync(ticketDir)) {
    // BRAIN-436: the exec may still be in flight. The tombstone makes it refuse to create the ticket, so this cancel is a real
    // one (`cancelConfirmed`): the job can no longer start here. A ticket that appeared meanwhile falls through as an ordinary one.
    if (await tombstoneAbsentTicket(root, ticketId)) {
      process.stdout.write(`${JSON.stringify({ protocol: 1, ticketId, action: 'no-such-ticket', cancelConfirmed: true, tombstoned: true })}\n`);
      return { exitCode: 0 };
    }
  }

  atomicWriteFile(path.join(ticketDir, 'cancelled'), String(Date.now()));

  // BRAIN-319 P3 (Codex re-review, then a focused follow-up pass): report
  // three separate facts instead of one overclaiming `cancelledBroker`.
  // `cancelRequested` is "did we durably record this cancellation at the
  // broker level" (the marker write below, gated on a broker ticket id
  // actually existing yet) -- that marker is re-checked, INSIDE the same
  // admission lock, by scheduler.js's tryStart right before this ticket
  // would ever be admitted (see its own comment), so it alone is what
  // actually governs the outcome for a not-yet-registered ticket.
  // `cancelConfirmed` is ONLY true when `cancelCommand` itself reports a
  // real, already-completed action (exit 0) -- never inferred from
  // `registered`, which is read WITHOUT the admission lock: admission can
  // dequeue a ticket (moving it from "queued" to "about to be leased")
  // between that unlocked read and `cancelCommand`'s own attempt, so
  // "unregistered at read time" is not proof of anything by the time
  // `cancelCommand` actually runs. `registered` is reported purely as
  // informational context for a human reading the JSON, never something a
  // caller may act on.
  let cancelRequested = false;
  let cancelConfirmed = false;
  let registered = false;
  const remoteIdPath = path.join(ticketDir, 'remote-id');
  if (fs.existsSync(remoteIdPath)) {
    const remoteLaneId = fs.readFileSync(remoteIdPath, 'utf8').trim();
    if (isUuid(remoteLaneId)) {
      // Write the LOCAL broker's own cancel marker for this id BEFORE
      // calling cancelCommand. onTicketCreated (above) can write `remote-id`
      // well before the supervisor has enqueued or leased that same id --
      // cancelCommand only knows how to act on a queued ticket, a held
      // lease, or an attempt record, so calling it first can find none of
      // those and do nothing, silently losing the cancellation.
      const brokerRoot = ensureStateDirs().root;
      writeCancelMarkerFile(brokerRoot, remoteLaneId);
      cancelRequested = true;
      // Informational only (see comment above) -- NOT part of the
      // cancelConfirmed decision.
      registered =
        Boolean(readLease(brokerRoot, remoteLaneId)) ||
        Boolean(readAttempt(brokerRoot, remoteLaneId)) ||
        listQueue(brokerRoot).some((t) => t && t.id === remoteLaneId);
      const result = await cancelCommand(remoteLaneId);
      cancelConfirmed = result.exitCode === 0;
    }
  }
  // A ticket that has already published its result is not running: a retried cancel is confirmed, not forever unconfirmed.
  // That holds with or without a `remote-id` -- a ticket rejected or refused before it was enqueued never had one.
  if (!cancelConfirmed && fs.existsSync(path.join(ticketDir, 'result.json'))) cancelConfirmed = true;

  process.stdout.write(`${JSON.stringify({ protocol: 1, ticketId, markedCancelled: true, cancelRequested, cancelConfirmed, registered })}\n`);
  return { exitCode: 0 };
}
