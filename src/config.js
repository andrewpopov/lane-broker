import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { execFileSync } from 'node:child_process';

export const DEFAULT_GLOBAL_CONFIG = {
  version: 1,
  capacity: 2,
  loadClose: 15,
  loadOpen: 11,
  loadOpenSamples: 3,
  sampleMs: 5000,
};

export const DEFAULT_REPO_CONFIG = {
  version: 1,
  lanes: { default: { weight: 2 } },
  conflicts: [],
};

export function brokerHome() {
  return process.env.LANE_BROKER_HOME || path.join(os.homedir(), '.config', 'lane-broker');
}

/** Sanitize a repo/lane identifier down to [A-Za-z0-9_.-]. */
export function sanitizeKey(raw) {
  const s = String(raw).replace(/[^A-Za-z0-9_.-]/g, '_');
  return s.length ? s : '_';
}

function assert(cond, msg) {
  if (!cond) throw new ConfigError(msg);
}

export class ConfigError extends Error {}

function validateGlobalConfig(cfg, sourcePath) {
  assert(cfg && typeof cfg === 'object', `${sourcePath}: config must be an object`);
  assert(Number.isInteger(cfg.version), `${sourcePath}: "version" must be an integer`);
  assert(Number.isFinite(cfg.capacity) && cfg.capacity > 0, `${sourcePath}: "capacity" must be a positive number`);
  assert(Number.isFinite(cfg.loadClose) && cfg.loadClose > 0, `${sourcePath}: "loadClose" must be a positive number`);
  assert(Number.isFinite(cfg.loadOpen) && cfg.loadOpen > 0, `${sourcePath}: "loadOpen" must be a positive number`);
  assert(cfg.loadOpen < cfg.loadClose, `${sourcePath}: "loadOpen" must be less than "loadClose"`);
  assert(Number.isInteger(cfg.loadOpenSamples) && cfg.loadOpenSamples > 0, `${sourcePath}: "loadOpenSamples" must be a positive integer`);
  assert(Number.isFinite(cfg.sampleMs) && cfg.sampleMs > 0, `${sourcePath}: "sampleMs" must be a positive number`);
}

function validateRepoConfig(cfg, sourcePath) {
  assert(cfg && typeof cfg === 'object', `${sourcePath}: config must be an object`);
  assert(Number.isInteger(cfg.version), `${sourcePath}: "version" must be an integer`);
  assert(cfg.lanes && typeof cfg.lanes === 'object' && !Array.isArray(cfg.lanes), `${sourcePath}: "lanes" must be an object`);
  for (const [name, lane] of Object.entries(cfg.lanes)) {
    assert(lane && typeof lane === 'object', `${sourcePath}: lane "${name}" must be an object`);
    assert(Number.isFinite(lane.weight) && lane.weight > 0, `${sourcePath}: lane "${name}".weight must be a positive number`);
    if (lane.localRefused !== undefined) {
      assert(typeof lane.localRefused === 'boolean', `${sourcePath}: lane "${name}".localRefused must be a boolean`);
    }
  }
  if (cfg.conflicts !== undefined) {
    assert(Array.isArray(cfg.conflicts), `${sourcePath}: "conflicts" must be an array of pairs`);
    for (const pair of cfg.conflicts) {
      assert(Array.isArray(pair) && pair.length === 2, `${sourcePath}: each "conflicts" entry must be a 2-element array`);
    }
  }
}

export function loadGlobalConfig() {
  const home = brokerHome();
  const file = path.join(home, 'config.json');
  if (!fs.existsSync(file)) return { ...DEFAULT_GLOBAL_CONFIG };
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (err) {
    throw new ConfigError(`${file}: invalid JSON (${err.message})`);
  }
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, ...parsed };
  validateGlobalConfig(cfg, file);
  return cfg;
}

/**
 * Re-read the global config for a long-running process (the supervisor's
 * polling loop). A supervisor holding a stale snapshot forever is the bug
 * this exists to fix: it must notice a threshold change within one
 * `sampleMs`, not never. But it must also never crash, or silently change
 * behaviour, because someone saved a broken config while it's mid-run — an
 * operator mid-edit, a half-written file, or a typo should degrade to "keep
 * doing what you were doing" rather than take down every running lane. So
 * any read/parse/validation failure here falls back to `previous` (or
 * `DEFAULT_GLOBAL_CONFIG` if there is no previous yet) instead of throwing.
 */
export function reloadGlobalConfig(previous) {
  const fallback = previous === undefined ? { ...DEFAULT_GLOBAL_CONFIG } : previous;
  try {
    return loadGlobalConfig();
  } catch {
    return fallback;
  }
}

const repoIdentityCache = new Map();

/** Resolve `<gitFileDir>/gitdir: <path>` style pointer contents to an absolute path. */
function resolveGitFilePointer(gitFilePath, gitFileDir) {
  const contents = fs.readFileSync(gitFilePath, 'utf8');
  const match = contents.match(/^gitdir:\s*(.+)$/m);
  if (!match) return null;
  const raw = match[1].trim();
  return path.isAbsolute(raw) ? raw : path.resolve(gitFileDir, raw);
}

/** Walk the filesystem up from `cwd` looking for a `.git` dir or file, no subprocess. */
function repoIdentityFromFilesystem(cwd) {
  let dir = path.resolve(cwd);
  for (;;) {
    const gitPath = path.join(dir, '.git');
    let stat;
    try {
      stat = fs.statSync(gitPath);
    } catch {
      stat = null;
    }
    if (stat) {
      let commonDir;
      if (stat.isDirectory()) {
        commonDir = gitPath;
      } else {
        const gitDir = resolveGitFilePointer(gitPath, dir);
        if (!gitDir) return null;
        commonDir = gitDir;
        try {
          const commondirContents = fs.readFileSync(path.join(gitDir, 'commondir'), 'utf8').trim();
          if (commondirContents) {
            commonDir = path.isAbsolute(commondirContents)
              ? commondirContents
              : path.resolve(gitDir, commondirContents);
          }
        } catch {
          // no commondir file; a linked-worktree gitdir without one is its own common dir.
        }
      }
      try {
        return fs.realpathSync(commonDir);
      } catch {
        return commonDir;
      }
    }
    const parent = path.dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}

/** Resolve the repo identity via `git rev-parse`, only as a fallback for the filesystem walk. */
function repoIdentityFromGit(cwd) {
  let commonDir;
  try {
    commonDir = execFileSync('git', ['rev-parse', '--path-format=absolute', '--git-common-dir'], {
      cwd,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 5000,
    }).trim();
  } catch {
    try {
      const rel = execFileSync('git', ['rev-parse', '--git-common-dir'], {
        cwd,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
        timeout: 5000,
      }).trim();
      commonDir = path.isAbsolute(rel) ? rel : path.join(cwd, rel);
    } catch {
      return null;
    }
  }
  try {
    return fs.realpathSync(commonDir);
  } catch {
    return commonDir;
  }
}

/** Resolve the repo identity as the realpath of the git common dir (shared by worktrees). */
export function repoIdentity(cwd) {
  const resolvedCwd = path.resolve(cwd);
  if (repoIdentityCache.has(resolvedCwd)) return repoIdentityCache.get(resolvedCwd);
  const result = repoIdentityFromFilesystem(resolvedCwd) ?? repoIdentityFromGit(resolvedCwd);
  repoIdentityCache.set(resolvedCwd, result);
  return result;
}

/** Walk up from `cwd` looking for .lane-broker.json, stopping at the git common dir's worktree root. */
export function findRepoConfigPath(cwd, gitCommonDir) {
  const stopAt = gitCommonDir ? path.dirname(gitCommonDir) : null;
  let dir = path.resolve(cwd);
  for (;;) {
    const candidate = path.join(dir, '.lane-broker.json');
    if (fs.existsSync(candidate)) return candidate;
    if (stopAt && dir === stopAt) return null;
    const parent = path.dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}

export function loadRepoConfig(cwd) {
  const commonDir = repoIdentity(cwd);
  const configPath = findRepoConfigPath(cwd, commonDir);
  if (!configPath) return { ...DEFAULT_REPO_CONFIG, declared: false };
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  } catch (err) {
    throw new ConfigError(`${configPath}: invalid JSON (${err.message})`);
  }
  const cfg = { version: 1, conflicts: [], ...parsed, lanes: { ...DEFAULT_REPO_CONFIG.lanes, ...(parsed.lanes || {}) } };
  validateRepoConfig(cfg, configPath);
  return { ...cfg, declared: true };
}

/** Expand conflict pairs (with `*` wildcard) into a symmetric adjacency map of laneName -> Set<laneName>. */
export function expandConflicts(conflicts, laneNames) {
  const adj = new Map(laneNames.map((n) => [n, new Set()]));
  for (const [a, b] of conflicts) {
    const as = a === '*' ? laneNames : [a];
    const bs = b === '*' ? laneNames : [b];
    for (const x of as) {
      for (const y of bs) {
        if (x === y) continue;
        if (!adj.has(x)) adj.set(x, new Set());
        if (!adj.has(y)) adj.set(y, new Set());
        adj.get(x).add(y);
        adj.get(y).add(x);
      }
    }
  }
  return adj;
}

/**
 * Resolve everything a `lane run` invocation needs: the fully-qualified key,
 * weight, whether this is a refused local sim, and the set of conflicting
 * fully-qualified keys within this repo.
 */
export function resolveTicketConfig({ cwd, repo, lane }) {
  const repoConfig = loadRepoConfig(cwd);
  const commonDir = repoIdentity(cwd);
  // The git identity wins whenever it can be determined, so the same repo
  // always resolves to the same key regardless of whether --repo is passed
  // (or passed inconsistently across invocations); --repo is a fallback
  // label for cwds with no git identity to key on.
  const repoId = sanitizeKey(commonDir || repo || cwd);
  const laneName = lane || 'default';
  if (repoConfig.declared && !Object.prototype.hasOwnProperty.call(repoConfig.lanes, laneName)) {
    const declared = Object.keys(repoConfig.lanes).sort().join(', ');
    throw new ConfigError(`unknown lane "${laneName}"; declared: ${declared}`);
  }
  const laneCfg = repoConfig.lanes[laneName] || { weight: DEFAULT_REPO_CONFIG.lanes.default.weight };
  const laneNames = Object.keys(repoConfig.lanes);
  const adj = expandConflicts(repoConfig.conflicts || [], laneNames);
  const conflictingLaneNames = adj.has(laneName) ? [...adj.get(laneName)] : [];
  const key = `${repoId}:${sanitizeKey(laneName)}`;
  const conflicts = conflictingLaneNames.map((n) => `${repoId}:${sanitizeKey(n)}`);
  return {
    repoId,
    lane: laneName,
    key,
    weight: laneCfg.weight,
    localRefused: Boolean(laneCfg.localRefused),
    conflicts,
  };
}
