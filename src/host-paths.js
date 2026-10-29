import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// BRAIN-721: absolute paths that exist on the submitting Mac but not in the snapshot a remote runner receives.
const HOST_ONLY_PREFIXES = ['/private/', '/var/folders/', '/tmp/', '/Volumes/', '/Users/', '/opt/homebrew/', '/Library/'];
const SHELLS = new Set(['sh', 'bash', 'zsh', 'dash']);
const SCRIPT_SPLIT = /[\s'"`;&|()<>]+/;

function shellScript(argv) {
  if (!SHELLS.has(path.basename(argv[0] ?? ''))) return null;
  const flag = argv.findIndex((a, i) => i > 0 && /^-[A-Za-z]*c[A-Za-z]*$/.test(a));
  return flag === -1 ? null : argv[flag + 1] ?? null;
}

function candidates(argv) {
  const raw = [...argv];
  const script = shellScript(argv);
  if (script !== null) raw.push(...script.split(SCRIPT_SPLIT));
  const out = new Set();
  for (const token of raw) {
    if (token.includes('://')) continue;
    out.add(token);
    const eq = token.indexOf('=');
    if (token.startsWith('--') && eq !== -1) out.add(token.slice(eq + 1));
  }
  return [...out].filter((t) => t.startsWith('/'));
}

const realpathOrSelf = (p) => {
  try {
    return fs.realpathSync(p);
  } catch {
    return p;
  }
};

const isUnder = (child, root) => child === root || child.startsWith(root.endsWith('/') ? root : `${root}/`);

/** Absolute argv paths that exist only on this machine, each with whether it lies inside the worktree
 *  and, if so, the cwd-relative path to use instead. */
export function hostOnlyPaths(argv, { worktreeRoot, cwd, home = os.homedir(), exists = fs.existsSync, realpath = realpathOrSelf }) {
  const prefixes = [...HOST_ONLY_PREFIXES, `${home}/`];
  const root = realpath(worktreeRoot);
  const found = [];
  for (const token of candidates(argv)) {
    if (!prefixes.some((p) => token.startsWith(p)) || !exists(token)) continue;
    const real = realpath(token);
    const insideWorktree = [token, real].some((p) => isUnder(p, worktreeRoot) || isUnder(p, root));
    found.push({ path: token, insideWorktree, suggestion: insideWorktree ? path.relative(realpath(cwd), real) || '.' : null });
  }
  return found;
}

export function hostPathsRefusal(lane, found, { dispatchOnly }) {
  const outside = dispatchOnly ? '' : ' or pass --local --local-reason "<why>"';
  return found.map((f) =>
    f.insideWorktree
      ? `lane run: ${lane} may run on a remote runner, which gets only a snapshot of the worktree; the command names ${f.path}, an absolute path into this worktree; use ${f.suggestion} (relative to the command's working directory) instead.`
      : `lane run: ${lane} may run on a remote runner, which gets only a snapshot of the worktree; the command names ${f.path}, which exists only on this machine. Copy it into the worktree and refer to it by a relative path${outside}.`,
  );
}
