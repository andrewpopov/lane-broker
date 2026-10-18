import fs from 'node:fs';
import path from 'node:path';

/**
 * BRAIN-623: is a runner READY for a lane, beyond having room? One list of checks, `READINESS_CHECKS`; each owns a slice of both
 * halves of the round trip:
 *   - `need(...)`    submitter side: the part of the lane's needs this check cares about, sent to the runner as `remote-probe` flags;
 *   - `report(need)` runner side: what the runner answers in its probe payload;
 *   - `gap(probe, need)` submitter side: why the probe's answer does not satisfy the need, or null.
 * An older runner ignores the flags and answers nothing, so a check whose need is non-empty treats the missing answer as a gap.
 * A later check (a lane-broker version floor, zero-length-file detection) is one more entry here.
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

const toolsCheck = {
  id: 'tools',
  flag: '--requires',
  /** The lane's executable names, deduplicated. */
  need: (lane) => [...new Set(lane?.requires ?? [])],
  /** Runner side: `{ <name>: true|false }` for each requested name that is a valid executable name. */
  report: (names, { envPath } = {}) => Object.fromEntries(names.filter((n) => EXECUTABLE_NAME_RE.test(n)).map((n) => [n, isOnPath(n, envPath)])),
  /** Submitter side: `{ reason, missing }` or null. */
  gap: (probe, names) => {
    if (names.length === 0) return null;
    const answered = probe.tools && typeof probe.tools === 'object' ? probe.tools : null;
    if (!answered) return { reason: `runner does not report tools (older lane-broker); the lane requires ${names.join(', ')}`, missing: names };
    const missing = names.filter((n) => answered[n] !== true);
    return missing.length ? { reason: `missing tool ${missing.join(', ')} (required by the lane)`, missing } : null;
  },
};

export const READINESS_CHECKS = [toolsCheck];

/** Everything the lane needs of a runner, `{ <check id>: need }`, from its remote payload. */
export const readinessNeeds = (remote) => Object.fromEntries(READINESS_CHECKS.map((c) => [c.id, c.need(remote)]));

/** `remote-probe` argv for these needs (empty when nothing is needed, so a lane without needs sends today's command). */
export function readinessProbeArgs(needs) {
  return READINESS_CHECKS.flatMap((c) => (needs[c.id]?.length ? [c.flag, needs[c.id].join(',')] : []));
}

/** Runner side: parse `remote-probe` argv (flags only) into needs. */
export function parseReadinessArgs(args) {
  return Object.fromEntries(
    READINESS_CHECKS.map((c) => {
      const idx = args.indexOf(c.flag);
      const value = idx === -1 ? '' : args[idx + 1] ?? '';
      return [c.id, value.split(',').filter(Boolean)];
    }),
  );
}

/** Runner side: the probe-payload fields answering `needs` (nothing for a need that is empty). */
export function reportReadiness(needs, opts) {
  return Object.fromEntries(READINESS_CHECKS.filter((c) => needs[c.id]?.length).map((c) => [c.id, c.report(needs[c.id], opts)]));
}

/** Submitter side: the first unmet need of `probe` as `{ check, reason, missing }`, or null when the runner is ready. */
export function readinessGap(probe, needs) {
  for (const c of READINESS_CHECKS) {
    const gap = c.gap(probe, needs[c.id] ?? []);
    if (gap) return { check: c.id, ...gap };
  }
  return null;
}
