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
export function computeContentKey({ cwd, key, cmdFingerprint }) {
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
    return digest([KEY_VERSION, key, cmdFingerprint, headTree, diff, untracked]);
  } catch {
    return null;
  }
}

/** The newest real, clean exit-0 run with this key inside the TTL, or null. A cached row (`cachedFrom`) never counts, so a hit cannot extend a pass's life. */
export function findCachedPass(rows, { contentKey, now, ttlMs }) {
  let best = null;
  for (const row of rows) {
    if (!row || row.contentKey !== contentKey || row.cachedFrom) continue;
    if (row.exit !== 0 || row.signal || row.cancelled || row.error || row.reason) continue;
    if (!Number.isFinite(row.startedAt) || !Number.isFinite(row.endedAt) || row.endedAt > now || now - row.endedAt > ttlMs) continue;
    if (!best || row.endedAt > best.endedAt) best = row;
  }
  return best;
}

export const isCleanPass = (result) => result.exit === 0 && !result.signal;

/**
 * The key a finished run's history row carries. A pass keeps it only if the worktree still hashes to the key taken at
 * `lane run` time: an edit while the run was queued or running means the pass may describe other content than the key says.
 */
export function settledContentKey(ticket, result) {
  if (!ticket.contentKey) return undefined;
  if (!isCleanPass(result)) return ticket.contentKey;
  const after = computeContentKey({ cwd: ticket.cwd, key: ticket.key, cmdFingerprint: ticket.cmdFingerprint });
  return after === ticket.contentKey ? after : undefined;
}
