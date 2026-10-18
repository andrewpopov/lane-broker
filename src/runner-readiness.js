import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * BRAIN-623: is a runner READY for a lane, beyond having room? One list of checks, `READINESS_CHECKS`; each owns a slice of both
 * halves of the round trip:
 *   - `need(lane, globalCfg)` submitter side: the part of the lane's needs (or the global config) this check cares about; a need
 *                    with a `flag` is sent to the runner as a `remote-probe` flag. A need may be an array, a string or `true`: see `hasNeed`;
 *   - `report(need)` runner side: what the runner answers in its probe payload (a check without `report` is judged from fields the probe always carries);
 *   - `gap(probe, need)` submitter side: why the probe's answer does not satisfy the need, or null.
 * An older runner ignores the flags and answers nothing, so a check whose need is non-empty treats the missing answer as a gap.
 * A check without a `flag` is not asked for: `version` reads the probe's always-present `version`, `integrity` is reported by every
 * runner of this release unasked (`runnerNeed`).
 */

/** An executable name as `requires` may spell it: no path separators, no shell syntax. */
export const EXECUTABLE_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._+-]*$/;

export function isValidRequiresShape(requires) {
  return Array.isArray(requires) && requires.every((name) => typeof name === 'string' && EXECUTABLE_NAME_RE.test(name));
}

/** True iff an executable file called `name` sits in a directory of `envPath`. */
export function isOnPath(name, envPath = process.env.PATH ?? '') {
  return envPath.split(path.delimiter).some((dir) => {
    if (!dir) return false;
    try {
      fs.accessSync(path.join(dir, name), fs.constants.X_OK);
      return fs.statSync(path.join(dir, name)).isFile();
    } catch {
      return false;
    }
  });
}

/** Is there anything to check? An empty array, null, undefined and false are not; a non-empty array, a string and `true` are. */
export const hasNeed = (need) => (Array.isArray(need) ? need.length > 0 : Boolean(need));

/** [major, minor, patch] of a plain `X.Y.Z` version (a pre-release/build suffix is ignored), or null when it is not one. */
const parseVersion = (v) => {
  const m = typeof v === 'string' ? /^(\d+)\.(\d+)\.(\d+)/.exec(v) : null;
  return m ? [Number(m[1]), Number(m[2]), Number(m[3])] : null;
};
export const VERSION_FLOOR_RE = /^\d+\.\d+\.\d+$/;

const isBelow = (version, floor) => {
  const [a, b] = [parseVersion(version), parseVersion(floor)];
  if (!a) return true;
  for (let i = 0; i < 3; i++) if (a[i] !== b[i]) return a[i] < b[i];
  return false;
};

const toolsCheck = {
  id: 'tools',
  flag: '--requires',
  /** The lane's executable names, deduplicated. */
  need: (lane) => [...new Set(lane?.requires ?? [])],
  /** Runner side: `{ <name>: true|false }` for each requested name that is a valid executable name. */
  report: (names, { envPath } = {}) => Object.fromEntries(names.filter((n) => EXECUTABLE_NAME_RE.test(n)).map((n) => [n, isOnPath(n, envPath)])),
  /** Submitter side: `{ reason, missing }` or null. */
  gap: (probe, names = []) => {
    if (names.length === 0) return null;
    const answered = probe.tools && typeof probe.tools === 'object' ? probe.tools : null;
    if (!answered) return { reason: `runner does not report tools (older lane-broker); the lane requires ${names.join(', ')}`, missing: names };
    const missing = names.filter((n) => answered[n] !== true);
    return missing.length ? { reason: `missing tool ${missing.join(', ')} (required by the lane)`, missing } : null;
  },
};

/** The lane-broker release floor: `runnerVersionFloor` in the global config (null = none). Submitter side only, read from the probe's `version`. */
const versionCheck = {
  id: 'version',
  need: (_lane, globalCfg) => globalCfg?.runnerVersionFloor ?? null,
  gap: (probe, floor) => {
    if (!hasNeed(floor) || !isBelow(probe.version, floor)) return null;
    const have = typeof probe.version === 'string' ? `lane-broker ${probe.version}` : 'lane-broker (version not reported)';
    return { reason: `${have} is below the floor ${floor}`, missing: [`lane-broker>=${floor}`] };
  },
};

/** Zero-length regular files under `bin/` and `src/` plus `package.json`, as paths relative to the package root (a truncated install). */
export function scanZeroLength(packageRoot) {
  const found = [];
  const visit = (rel) => {
    let entries;
    try {
      entries = fs.readdirSync(path.join(packageRoot, rel), { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      const child = path.join(rel, e.name);
      if (e.isDirectory()) visit(child);
      else if (e.isFile() && isEmptyFile(path.join(packageRoot, child))) found.push(child);
    }
  };
  const isEmptyFile = (file) => {
    try {
      return fs.statSync(file).size === 0;
    } catch {
      return false;
    }
  };
  visit('bin');
  visit('src');
  if (isEmptyFile(path.join(packageRoot, 'package.json'))) found.push('package.json');
  return found.sort();
}

const PACKAGE_ROOT = fileURLToPath(new URL('..', import.meta.url));
const INTEGRITY_REASON_PATHS = 3;

/** A runner whose install lost file contents (an unclean shutdown leaves zero-length files) must not be placed on. */
const integrityCheck = {
  id: 'integrity',
  need: () => true,
  runnerNeed: true,
  report: (_need, { packageRoot = PACKAGE_ROOT } = {}) => ({ zeroLength: scanZeroLength(packageRoot) }),
  // A probe with no `integrity` (an older runner) is NOT a gap: it cannot vouch either way, and the version floor is what excludes older runners.
  gap: (probe) => {
    const paths = probe.integrity?.zeroLength;
    if (!Array.isArray(paths) || paths.length === 0) return null;
    const shown = paths.slice(0, INTEGRITY_REASON_PATHS).join(', ') + (paths.length > INTEGRITY_REASON_PATHS ? ', ...' : '');
    return { reason: `truncated install: ${paths.length} zero-length file(s) (${shown})`, missing: paths };
  },
};

export const READINESS_CHECKS = [toolsCheck, versionCheck, integrityCheck];

/** Everything the lane needs of a runner, `{ <check id>: need }`, from its remote payload and the global config. */
export const readinessNeeds = (remote, globalCfg) => Object.fromEntries(READINESS_CHECKS.map((c) => [c.id, c.need(remote, globalCfg)]));

/** `remote-probe` argv for these needs (empty when nothing is needed, so a lane without needs sends today's command). */
export function readinessProbeArgs(needs) {
  return READINESS_CHECKS.flatMap((c) => (c.flag && hasNeed(needs[c.id]) ? [c.flag, needs[c.id].join(',')] : []));
}

/** Runner side: parse `remote-probe` argv (flags only) into needs. */
export function parseReadinessArgs(args) {
  return Object.fromEntries(
    READINESS_CHECKS.filter((c) => c.flag || c.runnerNeed).map((c) => {
      if (!c.flag) return [c.id, c.runnerNeed];
      const idx = args.indexOf(c.flag);
      const value = idx === -1 ? '' : args[idx + 1] ?? '';
      return [c.id, value.split(',').filter(Boolean)];
    }),
  );
}

/** Runner side: the probe-payload fields answering `needs` (nothing for a need that is empty). */
export function reportReadiness(needs, opts) {
  return Object.fromEntries(READINESS_CHECKS.filter((c) => c.report && hasNeed(needs[c.id])).map((c) => [c.id, c.report(needs[c.id], opts)]));
}

/** Submitter side: the first unmet need of `probe` as `{ check, reason, missing }`, or null when the runner is ready. */
export function readinessGap(probe, needs) {
  for (const c of READINESS_CHECKS) {
    const gap = c.gap(probe, needs[c.id]);
    if (gap) return { check: c.id, ...gap };
  }
  return null;
}
