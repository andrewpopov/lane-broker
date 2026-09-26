import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, readJsonSafe, withLock, bootId } from './state.js';
import { isPidAlive, processStartTime } from './lease.js';

/**
 * BRAIN-319 (.review-plan.md v3 C3/C4/C5): one durable ATTEMPT record per
 * remote-eligible ticket, `attempts/<id>.json`, write-temp+rename under the
 * SAME global mutex `withLock` already serializes every other piece of
 * shared state on (leases, queue, cancel markers, ...) -- an attempt record
 * and, say, a cancel marker must never be observed half-updated relative to
 * each other. `generation` is the fence: a caller only ever advances or acts
 * on the generation it last observed, so a slow/stale writer (e.g. a remote
 * dispatch that is still probing after the ticket already fell back to
 * local) can never clobber a newer state.
 *
 * This module does not itself reap or auto-remove a record whose supervisor
 * has died -- unlike a lease (see `reapIfStale` in lease.js), an orphaned
 * remote attempt must survive to be reconciled (C4: "ORPHANED-REMOTE ...
 * survives reboot ... no automatic rerun"), not be treated as gone the
 * moment the boot id changes. `supervisorAlive` only ever reports liveness;
 * callers (the supervisor, `lane status`/`wait`/`cancel` -- all future
 * slices) decide what to do with a dead answer.
 */

function attemptFile(root, id) {
  return path.join(paths(root).attempts, `${id}.json`);
}

/** Same cancel-marker convention `src/cancel.js` writes and `src/supervisor.js`
 *  (`cancelRequested`) reads: one file per ticket id under `paths(root).cancel`,
 *  existence-only (its contents are not read here, same as both of those). */
function hasCancelMarker(root, id) {
  return fs.existsSync(path.join(paths(root).cancel, id));
}

function removeAttempt(root, id) {
  try {
    fs.unlinkSync(attemptFile(root, id));
  } catch {
    // already gone
  }
}

export function readAttempt(root, id) {
  return readJsonSafe(attemptFile(root, id));
}

export function listAttempts(root) {
  const dir = paths(root).attempts;
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return [];
  }
  return names
    .filter((n) => n.endsWith('.json'))
    .map((n) => readJsonSafe(path.join(dir, n)))
    .filter(Boolean);
}

/**
 * Is the supervisor that OWNS this attempt still alive? pid + start time +
 * boot id, never pid alone (PID reuse) and never elapsed time (clock skew
 * across a reboot) -- the same liveness discipline `isSupervisorAlive`
 * (lease.js) applies to a lease, plus the boot id check up front: a boot
 * change makes any pid/start-time comparison meaningless (the number spaces
 * are for a different kernel boot), so it is checked first and short-
 * circuits straight to "not alive" rather than falling through to a
 * probe that can't mean anything. Fails closed: an indeterminate probe
 * (`processStartTime` returning `undefined`) is never treated as dead.
 */
export function supervisorAlive(attempt) {
  const sup = attempt && attempt.supervisor;
  if (!sup) return false;
  if (sup.bootId !== bootId()) return false;
  if (!isPidAlive(sup.pid)) return false;
  if (!sup.startTime) return true; // no start time captured at write time: fall back to pid-alive
  const current = processStartTime(sup.pid);
  if (current === undefined) return true; // probe failed: fail closed, keep believing alive
  if (current === null) return false; // confirmed gone
  return current === sup.startTime;
}

/** Create the initial attempt record for `id` (generation 0, executor 'remote',
 *  phase 'probe'), stamped with THIS process's own identity as the owning
 *  supervisor. `runner` is not yet known at this point in the real dispatch
 *  flow (selection happens after), so it defaults to null. */
export async function createAttempt(root, id, { runner = null } = {}) {
  const attempt = {
    id,
    generation: 0,
    executor: 'remote',
    phase: 'probe',
    runner,
    startedAt: Date.now(),
    supervisor: {
      pid: process.pid,
      startTime: processStartTime(process.pid) ?? null,
      bootId: bootId(),
    },
  };
  await withLock(root, () => {
    atomicWriteJson(attemptFile(root, id), attempt);
  });
  return attempt;
}

/** Apply `patch` to the stored record, but only if its generation is still
 *  exactly `expectedGeneration` -- the fence against a stale writer acting
 *  on state that has since moved on (e.g. already fell back to local). */
export async function updateAttempt(root, id, expectedGeneration, patch) {
  return withLock(root, () => {
    const current = readAttempt(root, id);
    if (!current || current.generation !== expectedGeneration) {
      return { ok: false };
    }
    const next = { ...current, ...patch };
    atomicWriteJson(attemptFile(root, id), next);
    return { ok: true, attempt: next };
  });
}

/**
 * Transition an attempt to a local fallback: generation+1, executor
 * 'local', phase 'queued', `fallbackReason` recorded. Refuses (never
 * advances the generation, never touches the record) once the ticket has
 * been cancelled -- C5: cancellation is checked before any fallback, not
 * just before the terminal write, so a cancelled ticket can never be
 * silently re-queued locally.
 */
export async function fallbackToLocal(root, id, reason) {
  return withLock(root, () => {
    if (hasCancelMarker(root, id)) {
      return { ok: false, cancelled: true };
    }
    const current = readAttempt(root, id);
    if (!current) return { ok: false };
    const next = { ...current, generation: current.generation + 1, executor: 'local', phase: 'queued', fallbackReason: reason };
    atomicWriteJson(attemptFile(root, id), next);
    return { ok: true, attempt: next };
  });
}

/**
 * The ONE function every terminal path (remote confirmed/cancelled, local
 * fallback finished) must go through, under the mutex, in this order (C5):
 *  1. Cancel marker present -> call `resultWriterFn({ cancelled: true })`
 *     instead of whatever outcome the caller actually observed -- a
 *     cancelled ticket's result is exit 130 regardless of the remote
 *     outcome, and the caller (owning the real result.json shape, which
 *     this module has no opinion on) is what actually renders that.
 *  2. Generation mismatch -> refuse ({ ok: false }); some other writer has
 *     already moved this attempt on, so nothing here may be published.
 *  3. Otherwise -> call `resultWriterFn({ cancelled: false })`, the caller's
 *     real terminal outcome.
 * Either way (steps 1 or 3), the attempt record is removed once
 * `resultWriterFn` returns -- the attempt is over.
 */
export async function publishTerminal(root, id, generation, resultWriterFn) {
  return withLock(root, () => {
    if (hasCancelMarker(root, id)) {
      resultWriterFn({ cancelled: true });
      removeAttempt(root, id);
      return { ok: true, cancelled: true };
    }
    const current = readAttempt(root, id);
    if (!current || current.generation !== generation) {
      return { ok: false };
    }
    resultWriterFn({ cancelled: false });
    removeAttempt(root, id);
    return { ok: true, cancelled: false };
  });
}
