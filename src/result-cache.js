import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { scrubbedGitEnv } from './remote-manifest.js';

/**
 * BRAIN-549: the result cache. A run whose command already passed on byte-identical worktree content is answered from
 * history. The one unacceptable failure is a cached pass for code that changed, so every doubt yields NO key (the run
 * simply executes): a git failure, a submodule, an assume-unchanged/skip-worktree file, too many or too large untracked
 * files, a special file or nested repo among the untracked ones.
 */

const KEY_VERSION = 'lane-result-cache/1';
export const MAX_UNTRACKED_FILES = 2000;
export const MAX_UNTRACKED_BYTES = 64 * 1024 * 1024;
/** `git diff HEAD` and `git ls-files -v` output ceiling; beyond it execFileSync throws and the run gets no key. */
const MAX_GIT_OUTPUT_BYTES = 64 * 1024 * 1024;

function git(root, args) {
  return execFileSync('git', args, { cwd: root, env: scrubbedGitEnv(), maxBuffer: MAX_GIT_OUTPUT_BYTES, stdio: ['ignore', 'pipe', 'ignore'], timeout: 60_000 });
}

/** Length-prefixed, so no field boundary can be forged by a value containing another field. */
function digest(parts) {
  const h = crypto.createHash('sha256');
  for (const part of parts) {
    const buf = Buffer.isBuffer(part) ? part : Buffer.from(String(part));
    h.update(`${buf.length}:`);
    h.update(buf);
  }
  return h.digest('hex');
}

/** One `path\0kind\0exec\0sha` line per untracked, non-ignored file (sorted), or null when it cannot be vouched for. */
function untrackedFingerprint(root) {
  const names = git(root, ['ls-files', '-z', '-o', '--exclude-standard']).toString('utf8').split('\0').filter(Boolean);
  if (names.length > MAX_UNTRACKED_FILES) return null;
  names.sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
  let bytes = 0;
  const lines = [];
  for (const rel of names) {
    const abs = path.join(root, rel);
    const st = fs.lstatSync(abs);
    if (st.isSymbolicLink()) {
      lines.push(`${rel}\0symlink\0\0${fs.readlinkSync(abs)}`);
    } else if (st.isFile()) {
      bytes += st.size;
      if (bytes > MAX_UNTRACKED_BYTES) return null;
      lines.push(`${rel}\0file\0${(st.mode & 0o111) !== 0}\0${crypto.createHash('sha256').update(fs.readFileSync(abs)).digest('hex')}`);
    } else {
      return null; // a nested repo (listed as `dir/`), a fifo, a socket
    }
  }
  return lines.join('\n');
}

/**
 * The content key of the worktree at `cwd` for one (lane key, command): sha256 over the lane key, the command
 * fingerprint, HEAD's tree, the binary `git diff HEAD` (every tracked edit, staged or not, and mode changes) and the
 * sorted path/mode/content-hash list of untracked non-ignored files. Ignored files are NOT in it (node_modules, build
 * output), nor is the environment: the key says "same source", not "same machine". Null = never cache this run.
 */
export function computeContentKey({ cwd, key, cmdFingerprint, configIdentity = null }) {
  if (!key || !cmdFingerprint) return null;
  try {
    const [headTree, root] = git(cwd, ['rev-parse', 'HEAD^{tree}', '--show-toplevel']).toString('utf8').trim().split('\n');
    if (!headTree || !root || fs.existsSync(path.join(root, '.gitmodules'))) return null;
    // `git diff` ignores a path flagged assume-unchanged or skip-worktree, so a file edited under one would go unseen.
    const flagged = git(root, ['ls-files', '-v', '-z']).toString('utf8').split('\0').filter(Boolean).some((entry) => entry[0] !== 'H');
    if (flagged) return null;
    const diff = git(root, ['diff', 'HEAD', '--binary', '--full-index', '--no-ext-diff', '--no-textconv', '--no-color', '--ignore-submodules=none']);
    const untracked = untrackedFingerprint(root);
    if (untracked === null) return null;
    return digest([KEY_VERSION, key, cmdFingerprint, headTree, diff, untracked, ...(configIdentity ? [configIdentity] : [])]);
  } catch {
    return null;
  }
}

/**
 * Exits that describe the broker, the runner or the caller rather than the command's own verdict (usage, refusal, retryable,
 * timeout, cancel), plus 126/127: the command could not be executed or found (a remote spawn error is reported as 127), which is the machine, not the code.
 */
const NON_VERDICT_EXITS = new Set([64, 69, 75, 76, 79, 124, 126, 127, 130]);

const isNewestRowCandidate = (row, contentKey, now) =>
  Boolean(row) && row.contentKey === contentKey && !row.cachedFrom && !row.refusedFrom && Number.isFinite(row.startedAt) && Number.isFinite(row.endedAt) && row.endedAt <= now;

/** A clean exit-0 row: the only kind that may be served. */
const isServablePass = (row) => row.exit === 0 && !row.signal && !row.cancelled && !row.error && !row.reason;

/**
 * BRAIN-700: a genuine test failure, safe to refuse a byte-identical re-run for: the command itself exited non-zero on content the
 * key still described when it ended (`contentKeyVerified`), with no signal, cancel, error or watchdog reason. A remote run also must
 * have completed (not been refused or rejected) in the `command` phase, never `deps` or `setup`. Rows written before
 * `contentKeyVerified` existed never qualify.
 */
export function isQualifyingRed(row) {
  if (row.contentKeyVerified !== true) return false;
  if (!Number.isInteger(row.exit) || row.exit === 0 || row.exit >= 128 || NON_VERDICT_EXITS.has(row.exit)) return false;
  if (row.signal || row.cancelled || row.error || row.reason) return false;
  if (row.executor === 'remote' && (row.remoteKind !== 'completed' || row.remotePhase !== 'command' || row.remoteSignal)) return false;
  return true;
}

/**
 * The newest real outcome for `contentKey` decides: a servable pass inside `passTtlMs` is served, a qualifying red inside
 * `redTtlMs` is refused, anything else (a newer infra/signal/cancel row, a red that does not qualify, expiry) runs. A TTL of 0
 * turns that answer off. Cached and refused rows and rows with no usable or future timestamps are not outcomes. On equal
 * `endedAt` the later row in the file wins.
 */
export function decideReuse(rows, { contentKey, now, passTtlMs, redTtlMs }) {
  let newest = null;
  for (const row of rows) {
    if (!isNewestRowCandidate(row, contentKey, now)) continue;
    if (!newest || row.endedAt >= newest.endedAt) newest = row;
  }
  if (!newest) return { action: 'run' };
  const age = now - newest.endedAt;
  if (isServablePass(newest) && passTtlMs > 0 && age <= passTtlMs) return { action: 'serve', row: newest };
  if (isQualifyingRed(newest) && redTtlMs > 0 && age <= redTtlMs) return { action: 'refuse', row: newest };
  return { action: 'run' };
}

/**
 * BRAIN-713: the repo config a run used when it is not the worktree's own file (which the tree hash already covers), so a merged lane
 * change or the worktree override never reuses a verdict reached under another config. Null keeps a worktree-config key unchanged.
 */
export const configIdentityOf = ({ configFrom, configBlob }) =>
  configFrom === 'default-branch' ? `default-branch:${configBlob}` : configFrom === 'worktree-override' ? 'worktree-override' : null;

export const isCleanPass =(result) => result.exit === 0 && !result.signal;

/**
 * The key a finished run's history row carries (null: none), for a pass or a failure alike: it is kept only if the worktree still hashes to the key
 * taken at `lane run` time. An edit while the run was queued or ran means the result may describe other content than the key says.
 */
export function settledContentKey(ticket) {
  if (!ticket.contentKey) return null;
  const after = computeContentKey({ cwd: ticket.cwd, key: ticket.key, cmdFingerprint: ticket.cmdFingerprint, configIdentity: configIdentityOf(ticket) });
  return after === ticket.contentKey ? after : null;
}
