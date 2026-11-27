import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { scrubbedGitEnv } from './remote-manifest.js';

export const ADHOC_REPO = 'adhoc';
export const MAX_LOCAL_HOPS = 3;

const SEGMENT = /^[a-z0-9][a-z0-9._-]*$/;
const SCHEME_URL = /^[a-z][a-z0-9+.-]*:\/\//i;
const SCP_URL = /^(?:[^@/\s]+@)?[^:/\s]+:(?!\/\/)(.+)$/;
const REMOTE_SCHEMES = new Set(['ssh:', 'https:', 'http:', 'git:', 'git+ssh:', 'ssh+git:']);

const slugOfPath = (urlPath) => {
  const segments = urlPath.split('/').filter(Boolean);
  if (segments.length < 2) return null;
  const [owner, name] = segments.slice(-2).map((segment, i) => (i === 1 ? segment.replace(/\.git$/i, '') : segment).toLowerCase());
  return SEGMENT.test(owner) && SEGMENT.test(name) ? `${owner}/${name}` : null;
};

/** `{ slug }` for a remote URL, `{ localPath }` for a path on this machine, `null` for anything else. */
function classify(originUrl) {
  if (SCHEME_URL.test(originUrl)) {
    let url;
    try {
      url = new URL(originUrl);
    } catch {
      return null;
    }
    if (url.protocol === 'file:') return { localPath: fileURLToPath(url) };
    return REMOTE_SCHEMES.has(url.protocol) ? { slug: slugOfPath(decodeURIComponent(url.pathname)) } : null;
  }
  const scp = SCP_URL.exec(originUrl);
  if (scp && !originUrl.startsWith('/') && !originUrl.startsWith('.')) return { slug: slugOfPath(scp[1]) };
  return { localPath: originUrl };
}

/** The `remote.origin.url` of the git repo at `dir`, or `null` when `dir` has none. */
export function originUrlOf(dir) {
  try {
    return execFileSync('git', ['-C', dir, 'config', '--get', 'remote.origin.url'], {
      encoding: 'utf8', env: scrubbedGitEnv(), stdio: ['ignore', 'pipe', 'ignore'], timeout: 5000,
    }).trim() || null;
  } catch {
    return null;
  }
}

/**
 * The canonical `owner/name` of a git origin: lowercase, host and user dropped, `.git` and a trailing `/` stripped. An origin that is a local
 * path is followed through that repo's own origin, at most MAX_LOCAL_HOPS times. Whether the slug is one a runner may claim is the runner's
 * allowlist to say, so a well-formed slug is never mapped to `adhoc` here.
 */
export function repoName(originUrl, { readOrigin = originUrlOf, baseDir = process.cwd() } = {}) {
  let current = typeof originUrl === 'string' ? originUrl.trim() : '';
  let base = baseDir;
  for (let hops = 0; current !== ''; hops++) {
    const found = classify(current);
    if (found === null) return ADHOC_REPO;
    if (found.slug !== undefined) return found.slug ?? ADHOC_REPO;
    if (hops === MAX_LOCAL_HOPS) return ADHOC_REPO;
    base = path.resolve(base, found.localPath);
    if (!fs.existsSync(base)) return ADHOC_REPO;
    current = readOrigin(base)?.trim() ?? '';
  }
  return ADHOC_REPO;
}

const BROKER_VERSION = JSON.parse(fs.readFileSync(fileURLToPath(new URL('../package.json', import.meta.url)), 'utf8')).version;

/** The submitter's stamp on a pull ticket header: the name computed once at submit and this package's version. Persisted with the envelope, never recomputed. */
export const withSubmitterStamp = (header, name) => ({ ...header, ...(name === undefined ? {} : { repoName: name }), brokerVersion: BROKER_VERSION });
