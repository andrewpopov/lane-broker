import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { artifactMatches } from './remote-artifacts.js';
import { scrubbedGitEnv } from './remote-manifest.js';

/**
 * BRAIN-642 slice A: a lane's `remoteExclude` -- tracked or untracked paths left out of the remote snapshot entirely
 * (never read, hashed, or checked for eligibility). Client-side only: the runner and the wire format never learn of it.
 * Glob semantics are `artifactMatches`' (`*` within a segment, `**` as a whole segment); a trailing `/` names a
 * directory and everything under it.
 */
export const MAX_REMOTE_EXCLUDE_PATTERNS = 64;
export const MAX_REMOTE_EXCLUDE_PATTERN_LENGTH = 256;

/** Files the runner cannot work without, whatever the lane declares. */
const LOCKFILES = ['package-lock.json', 'npm-shrinkwrap.json'];

const isDirPattern = (pattern) => pattern.endsWith('/');

/** Why `patterns` is not a valid `remoteExclude` value, or null when it is. */
export function remoteExcludeShapeProblem(patterns) {
  if (!Array.isArray(patterns) || patterns.length === 0) return 'must be a non-empty array of strings';
  if (patterns.length > MAX_REMOTE_EXCLUDE_PATTERNS) return `has more than ${MAX_REMOTE_EXCLUDE_PATTERNS} patterns`;
  const seen = new Set();
  for (const pattern of patterns) {
    if (typeof pattern !== 'string' || pattern === '') return 'patterns must be non-empty strings';
    if (pattern.length > MAX_REMOTE_EXCLUDE_PATTERN_LENGTH) return `pattern "${pattern.slice(0, 40)}..." is longer than ${MAX_REMOTE_EXCLUDE_PATTERN_LENGTH} characters`;
    if (pattern.includes('\0') || pattern.includes('\\')) return `pattern "${pattern}" has a NUL or backslash`;
    if (pattern.startsWith('!')) return `pattern "${pattern}": negation ("!") is not supported`;
    if (pattern.startsWith('/')) return `pattern "${pattern}" must be relative, not absolute`;
    const segs = (isDirPattern(pattern) ? pattern.slice(0, -1) : pattern).split('/');
    if (segs.includes('..')) return `pattern "${pattern}" must not contain ".."`;
    if (segs.some((seg) => seg === '' || seg === '.')) return `pattern "${pattern}" has an empty or "." segment`;
    if (segs.some((seg) => seg.includes('**') && seg !== '**')) return `pattern "${pattern}": "**" must be a whole segment`;
    if (seen.has(pattern)) return `pattern "${pattern}" is duplicated`;
    seen.add(pattern);
  }
  return null;
}

function patternMatchesFile(pattern, relPath) {
  if (!isDirPattern(pattern)) return artifactMatches(pattern, relPath);
  const dir = pattern.slice(0, -1);
  const segs = relPath.split('/');
  for (let n = 1; n < segs.length; n += 1) if (artifactMatches(dir, segs.slice(0, n).join('/'))) return true;
  return false;
}

/** The first pattern that excludes the file at `relPath`, or null. */
export function excludingPattern(patterns, relPath) {
  return patterns.find((pattern) => patternMatchesFile(pattern, relPath)) ?? null;
}

/**
 * A reason the lane would break if `patterns` were applied, or null, for what is knowable from the config alone: the
 * repo-root `.lane-broker.json` and a `remoteDeps` dir's package.json or lockfile must stay in the snapshot. What
 * depends on where `lane run` was invoked is `remoteExcludePreflight`'s job.
 */
export function remoteExcludeConflict(patterns, { remoteDeps = null } = {}) {
  const required = ['.lane-broker.json'];
  for (const dir of remoteDeps ?? []) {
    const prefix = dir === '.' ? '' : `${dir}/`;
    for (const name of ['package.json', ...LOCKFILES]) required.push(`${prefix}${name}`);
  }
  for (const file of required) {
    const hit = excludingPattern(patterns, file);
    if (hit) return `pattern "${hit}" would exclude ${file}, which the remote run needs`;
  }
  return null;
}

/**
 * Every `.lane-broker.json` that exists between the invocation directory and the snapshot root, as root-relative
 * paths. `findRepoConfigPath` walks up from the invocation directory, so the nearest of these is the config that
 * governs the lane; all of them are protected.
 */
export function protectedConfigPaths(worktreeRoot, relCwd) {
  const found = [];
  let dir = relCwd;
  for (;;) {
    const rel = dir === '' ? '.lane-broker.json' : `${dir}/.lane-broker.json`;
    if (fs.existsSync(path.join(worktreeRoot, rel))) found.push(rel);
    if (dir === '') return found;
    dir = dir.includes('/') ? dir.slice(0, dir.lastIndexOf('/')) : '';
  }
}

function filesUnder(worktreeRoot, relCwd) {
  const cwd = path.join(worktreeRoot, relCwd);
  const out = execFileSync('git', ['ls-files', '-z', '-c', '-o', '--exclude-standard', '--', '.'], { cwd, env: scrubbedGitEnv(), encoding: 'buffer', maxBuffer: 1024 * 1024 * 256 });
  return out.toString('utf8').split('\0').filter(Boolean).map((rel) => (relCwd === '' ? rel : `${relCwd}/${rel}`));
}

/**
 * The dispatch-time half of the exclude protections, run once `relCwd` is known and before the snapshot is built:
 * the config files that govern this invocation, the `remoteDeps` files, and the invocation directory itself (it must
 * keep at least one file after the exclusion, which catches a directory excluded by any pattern, not only by name).
 */
export function remoteExcludePreflight(patterns, { worktreeRoot, relCwd, remoteDeps = null }) {
  const staticConflict = remoteExcludeConflict(patterns, { remoteDeps });
  if (staticConflict) return staticConflict;
  for (const file of protectedConfigPaths(worktreeRoot, relCwd)) {
    const hit = excludingPattern(patterns, file);
    if (hit) return `pattern "${hit}" would exclude ${file}, a lane config that governs this run`;
  }
  const survivors = filesUnder(worktreeRoot, relCwd).filter((rel) => excludingPattern(patterns, rel) === null && fs.existsSync(path.join(worktreeRoot, rel)));
  if (survivors.length === 0) return `the patterns leave no files in the lane's working directory ${relCwd === '' ? '.' : relCwd}`;
  return null;
}

/**
 * Splits `candidates` for `buildManifest`: the paths to keep, plus what was excluded -- count, size on disk (a stat,
 * never a read), per-pattern counts, and the patterns that matched nothing (a likely typo).
 */
export function applyRemoteExclude(root, patterns, candidates) {
  const kept = [];
  const byPattern = Object.fromEntries(patterns.map((pattern) => [pattern, 0]));
  let count = 0;
  let bytes = 0;
  for (const relPath of candidates) {
    const hit = excludingPattern(patterns, relPath);
    if (hit === null) {
      kept.push(relPath);
      continue;
    }
    count += 1;
    byPattern[hit] += 1;
    try {
      const st = fs.lstatSync(path.join(root, relPath));
      if (st.isFile()) bytes += st.size;
    } catch {
      // not on disk (staged delete): nothing to count
    }
  }
  return { kept, excluded: { count, bytes, byPattern, unmatched: patterns.filter((pattern) => byPattern[pattern] === 0) } };
}

/** The dispatch-time warning lines for a manifest's `excluded` summary (empty when nothing to say). */
export function remoteExcludeWarnings(excluded) {
  if (!excluded) return [];
  const lines = [];
  if (excluded.count > 0) {
    const detail = Object.entries(excluded.byPattern).filter(([, n]) => n > 0).map(([pattern, n]) => `${pattern}: ${n}`).join(', ');
    lines.push(`lane: remote snapshot excludes ${excluded.count} files / ${(excluded.bytes / 1_000_000).toFixed(1)} MB (${detail})\n`);
  }
  for (const pattern of excluded.unmatched) lines.push(`lane: remoteExclude pattern "${pattern}" matched no files (typo?)\n`);
  return lines;
}
