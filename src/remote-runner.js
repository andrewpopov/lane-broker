import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { atomicWriteFile, atomicWriteJson, ensureStateDirs, paths, readJsonSafe } from './state.js';
import { manifestHashOf, scrubbedGitEnv, verifyManifestNoGit } from './remote-manifest.js';
import { makeReader, readHeaderLine, extractFrames } from './remote-stream.js';
import { runCommand } from './run.js';
import { cancelCommand } from './cancel.js';
import { collectStatus } from './status.js';

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
 * identity, hooks disabled, and gpg signing off. Returns `{ok:true}` or
 * `{ok:false, reason}` -- never throws.
 */
function gitInitSnapshot(workDir) {
  const env = { ...scrubbedGitEnv(), ...SYNTHETIC_GIT_ENV };
  const common = {
    cwd: workDir,
    env,
    stdio: ['ignore', 'ignore', 'pipe'],
  };
  const gitArgs = ['-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', '-c', 'core.autocrlf=false', '-c', 'core.fileMode=true'];
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

const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
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

function isUuid(v) {
  return typeof v === 'string' && UUID_RE.test(v);
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
  if (header.protocol !== 1) return { ok: false, reason: `unsupported protocol: ${header.protocol}` };
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
  return { ok: true };
}

function buildResult(header, { kind, exit = null, signal = null, remoteLaneId = null, reason = null }) {
  return {
    protocol: 1,
    ticketId: header.ticketId,
    generation: header.generation,
    manifestHash: header.manifest?.manifestHash ?? null,
    kind,
    exit,
    signal,
    remoteLaneId,
    reason,
    finishedAt: Date.now(),
  };
}

function writeResult(ticketDir, result) {
  atomicWriteJson(path.join(ticketDir, 'result.json'), result);
}

function cleanupWork(workDir) {
  try {
    fs.rmSync(workDir, { recursive: true, force: true });
  } catch {
    // best-effort; a leaked work dir under a per-ticket directory is harmless
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
  // BRAIN-319 I4: this process is invoked by ssh as a fresh node process
  // after any login-shell startup already ran, so scrubbing here — rather
  // than relying on `env -u` upstream — satisfies "after shell startup":
  // nothing later in this process can be handed a forwarded lease/key, and
  // the freshly spawned supervisor's child inherits LANE_BROKER_LOCAL=1
  // rather than any inherited lease identity.
  delete process.env.LANE_BROKER_LEASE;
  delete process.env.LANE_BROKER_KEY;
  delete process.env.LANE_BROKER_TICKET;
  process.env.LANE_BROKER_LOCAL = '1';

  const reader = makeReader(stdin);
  const headerResult = await readHeaderLine(reader, 1_000_000);
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

  const ticketsDir = path.join(root, 'tickets');
  fs.mkdirSync(ticketsDir, { recursive: true });
  const ticketDir = path.join(ticketsDir, header.ticketId);
  try {
    fs.mkdirSync(ticketDir);
  } catch (err) {
    if (err.code === 'EEXIST') {
      process.stderr.write(`lane remote-exec: ticket directory already exists: ${ticketDir}\n`);
      return { exitCode: 1 };
    }
    throw err;
  }

  const fieldCheck = validateHeaderFields(header);
  if (!fieldCheck.ok) {
    writeResult(ticketDir, buildResult(header, { kind: 'rejected', reason: fieldCheck.reason }));
    process.stderr.write(`lane remote-exec: ${fieldCheck.reason}\n`);
    return { exitCode: 0 };
  }

  const workDir = path.join(ticketDir, 'work');
  const extractResult = await extractFrames(reader, workDir, header.manifest, {
    maxFileBytes: MAX_FILE_BYTES,
    maxTotalBytes: MAX_TOTAL_BYTES,
  });
  if (!extractResult.ok) {
    writeResult(ticketDir, buildResult(header, { kind: 'rejected', reason: extractResult.reason }));
    process.stderr.write(`lane remote-exec: ${extractResult.reason}\n`);
    cleanupWork(workDir);
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
    writeResult(ticketDir, buildResult(header, { kind: 'rejected', reason: gitResult.reason }));
    process.stderr.write(`lane remote-exec: ${gitResult.reason}\n`);
    cleanupWork(workDir);
    return { exitCode: 0 };
  }
  const postGitVerify = verifyManifestNoGit(workDir, header.manifest, { ignoreRootGit: true });
  if (!postGitVerify.ok) {
    writeResult(ticketDir, buildResult(header, { kind: 'rejected', reason: postGitVerify.reason }));
    process.stderr.write(`lane remote-exec: ${postGitVerify.reason}\n`);
    cleanupWork(workDir);
    return { exitCode: 0 };
  }

  const cancelledMarker = path.join(ticketDir, 'cancelled');
  const remoteLaneId = crypto.randomUUID();

  // Belt and braces alongside `configRoot`/`repoIdentityOverride` below: no
  // git invocation the child (or anything it shells out to) makes can ever
  // resolve a repo above this ticket's own directory.
  process.env.GIT_CEILING_DIRECTORIES = ticketDir;

  const runOutcome = await runCommand({
    repo: header.repoKey,
    lane: header.lane,
    weightOverride: header.weight,
    cpuOverride: header.cpuCores,
    memoryOverride: header.memoryBytes,
    cwd: path.join(workDir, header.relCwd || ''),
    cmd: header.argv,
    // BRAIN-319 C7: resolve `.lane-broker.json` from the snapshot itself,
    // never above it (the snapshot has no `.git` to derive a stop point from).
    configRoot: workDir,
    // BRAIN-319 T3 (C7): the synthetic git repo created above must never
    // change the lease key -- pin it to repoKey regardless.
    repoIdentityOverride: header.repoKey,
    idOverride: remoteLaneId,
    // BRAIN-319 C5: fires as soon as the id exists, before the supervisor
    // is ever spawned (run.js's own hook point) -- this is what makes
    // "remote-id exists" and "the child could have started" the same fact.
    onTicketCreated: async (id) => {
      atomicWriteFile(path.join(ticketDir, 'remote-id'), id);
      if (fs.existsSync(cancelledMarker)) return false;
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
  if (structured) {
    exit = structured.exit;
    signal = structured.signal;
    kind = cancelled ? 'cancelled' : 'completed';
  } else if (cancelled) {
    kind = 'cancelled';
  } else if (PREFLIGHT_REFUSAL_EXIT_CODES.has(runOutcome.exitCode)) {
    kind = 'refused';
  } else {
    kind = 'unfinished';
  }

  writeResult(ticketDir, buildResult(header, { kind, exit, signal, remoteLaneId }));
  cleanupWork(workDir);
  return { exitCode: 0 };
}

/** `lane remote-probe`: one JSON line describing this host's local broker,
 *  reusing status.js's own reader rather than a second implementation. */
export async function remoteProbeCommand() {
  const status = await collectStatus();
  const pkgPath = fileURLToPath(new URL('../package.json', import.meta.url));
  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
  const payload = {
    protocol: 1,
    version: pkg.version,
    paused: Boolean(status.paused),
    queued: status.queued.length,
    running: status.running.length,
  };
  process.stdout.write(`${JSON.stringify(payload)}\n`);
  return { exitCode: 0 };
}

/** `lane remote-result <ticketId>`: print result.json, or {missing:true}. */
export async function remoteResultCommand(ticketId, { root = defaultRemoteRoot() } = {}) {
  if (!isUuid(ticketId)) {
    process.stderr.write('lane remote-result: missing or invalid ticketId\n');
    return { exitCode: 2 };
  }
  const resultPath = path.join(root, 'tickets', ticketId, 'result.json');
  const data = readJsonSafe(resultPath);
  process.stdout.write(`${JSON.stringify(data || { protocol: 1, missing: true })}\n`);
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
  const ticketDir = path.join(root, 'tickets', ticketId);
  if (!fs.existsSync(ticketDir)) {
    process.stdout.write(`${JSON.stringify({ protocol: 1, ticketId, action: 'no-such-ticket' })}\n`);
    return { exitCode: 0 };
  }

  atomicWriteFile(path.join(ticketDir, 'cancelled'), String(Date.now()));

  let cancelledBroker = false;
  const remoteIdPath = path.join(ticketDir, 'remote-id');
  if (fs.existsSync(remoteIdPath)) {
    const remoteLaneId = fs.readFileSync(remoteIdPath, 'utf8').trim();
    if (isUuid(remoteLaneId)) {
      const result = await cancelCommand(remoteLaneId);
      cancelledBroker = result.exitCode === 0;
    }
  }

  process.stdout.write(`${JSON.stringify({ protocol: 1, ticketId, markedCancelled: true, cancelledBroker })}\n`);
  return { exitCode: 0 };
}
