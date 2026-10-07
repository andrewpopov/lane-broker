import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { isPidAlive, processStartTime, isProcessAlive } from './process-liveness.js';

export { isPidAlive, processStartTime };

export function stateHome() {
  return process.env.LANE_BROKER_STATE || path.join(os.homedir(), '.cache', 'lane-broker');
}

/** What the migration leaves where old code expects the queue DIRECTORY, so old code fails on its own: `mkdir` and every
 *  queue write underneath it hit ENOTDIR/EEXIST. New code then keeps its queue in `queue-v2/`. */
export const QUEUE_FENCE_NOTE = 'lane-broker scheduler migrated to v2; upgrade lane-broker\n';

/**
 * Has `lane migrate-scheduler` replaced the legacy `queue/` directory with the fence file? True ONLY for a regular file
 * that starts with the exact note. A symlink (even to a directory) or anything else is the legacy `queue/`, because
 * that is what old code would follow: new code must never be sent to `queue-v2/` while old code uses the same `queue`.
 */
export function queueFenced(root) {
  const file = path.join(root, 'queue');
  let fd;
  try {
    if (!fs.lstatSync(file).isFile()) return false;
    fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    const buf = Buffer.alloc(Buffer.byteLength(QUEUE_FENCE_NOTE));
    const read = fs.readSync(fd, buf, 0, buf.length, 0);
    return buf.subarray(0, read).toString('utf8') === QUEUE_FENCE_NOTE;
  } catch {
    return false;
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}

export function paths(root = stateHome()) {
  return {
    root,
    leases: path.join(root, 'leases'),
    // A getter, not a value: every queue read and write goes through it, and the answer (legacy `queue/` or `queue-v2/`)
    // is whatever the state root says at the moment of use.
    get queue() {
      return path.join(root, queueFenced(root) ? 'queue-v2' : 'queue');
    },
    logs: path.join(root, 'logs'),
    results: path.join(root, 'results'),
    cancel: path.join(root, 'cancel'),
    // BRAIN-320 S1d: a queue-timeout expiry marker, one file per ticket id --
    // kept separate from `cancel` (see `isExpired`'s doc comment) so a later
    // reader can always tell a runner-side queue-timeout expiry apart from a
    // user cancel, even though both end a queued ticket without a lease.
    expire: path.join(root, 'expire'),
    // BRAIN-436: a runner-side withdraw marker (see `writeWithdrawMarkerFile`).
    withdraw: path.join(root, 'withdraw'),
    history: path.join(root, 'history.jsonl'),
    pause: path.join(root, 'PAUSE'),
    lock: path.join(root, 'lock'),
    loadGate: path.join(root, 'load-gate.json'),
    configWarning: path.join(root, 'config-warning.json'),
    cpuSample: path.join(root, 'cpu-sample.json'),
    cpuGate: path.join(root, 'cpu-gate.json'),
    admissionLog: path.join(root, 'admission-decisions.log'),
    conflictSkipState: path.join(root, 'conflict-skip-state.json'),
    // BRAIN-249 part 2: the capacity-blocked head's own skip counter — kept
    // separate from conflictSkipState (see readCapacitySkipState's doc
    // comment in scheduler.js for why a shared counter would let the two
    // block reasons interfere with each other's allowance).
    capacitySkipState: path.join(root, 'capacity-skip-state.json'),
    // BRAIN-346: the projected-over-budget head's backfill allowance and reservation latch.
    resourceSkipState: path.join(root, 'resource-skip-state.json'),
    // BRAIN-379: when sim demand last existed (arms the sim soft lock); see src/sim-arm.js.
    simArm: path.join(root, 'sim-arm.json'),
    simArmLock: path.join(root, 'sim-arm.lock'),
    seq: path.join(root, 'seq'),
    // BRAIN-380: the scheduler fence (its presence, valid, switches priority ordering on) and the per-ticket
    // fairness records that replace the three singleton skip files behind it; see src/fairness.js.
    schedFence: path.join(root, 'sched-v2.json'),
    fairness: path.join(root, 'fairness-v2.json'),
    // BRAIN-380: broker-wide high-water mark of wall time (the priority clock); see src/priority-clock.js.
    hwm: path.join(root, 'priority-hwm.json'),
    // BRAIN-380 slice 3: present only while `lane migrate-scheduler` runs; every new-code admission entry point refuses on it.
    migrating: path.join(root, 'migrating'),
    // BRAIN-380 slice 4: present only while `lane migrate-scheduler --when-idle` waits for the broker to go quiet. Every
    // NEW intake entry point refuses on it (like `migrating`); already-queued tickets keep being admitted. Holds
    // `{pid, startTime, startedAt}` of the waiting command, so a SIGKILLed one is recognisably stale.
    draining: path.join(root, 'draining'),
    // BRAIN-380 slice 3: a queue record without `schedVersion` found behind the fence is moved here and never selected.
    queueQuarantine: path.join(root, 'queue-quarantine'),
    // BRAIN-319 T3b-2 (C3): one durable attempt record per remote-eligible
    // ticket, keyed by ticket id -- see src/attempts.js.
    attempts: path.join(root, 'attempts'),
  };
}

/** Deterministic path of a ticket's cancel marker: one file per id under
 *  `paths(root).cancel`. Single source of truth for the naming convention
 *  `cancel.js` (write), `attempts.js` and `supervisor.js` (read) all share. */
export function cancelMarkerPath(root, id) {
  return path.join(paths(root).cancel, id);
}

/** Does a cancel marker exist for `id`? Existence-only -- its contents (a
 *  timestamp, written by `cancel.js`) are never read by any consumer. */
export function isCancelled(root, id) {
  return fs.existsSync(cancelMarkerPath(root, id));
}

/** Write the cancel marker for `id`, durably and idempotently. Single writer
 *  used by every "we have accepted a cancellation" site (`cancel.js`'s
 *  `lane cancel`, `wait.js`'s Ctrl-C forwarding, `supervisor.js`'s own
 *  signal handlers) -- BRAIN-319 T3b-5: a cancellation a caller has already
 *  decided to honour must be recorded BEFORE anything else happens, so a
 *  later `publishTerminal`/`isCancelled` check can never miss it. */
export function writeCancelMarkerFile(root, id) {
  fs.mkdirSync(paths(root).cancel, { recursive: true });
  atomicWriteFile(cancelMarkerPath(root, id), String(Date.now()));
}

/** Deterministic path of a ticket's queue-timeout expiry marker (BRAIN-320 S1d) --
 *  same one-file-per-id convention as `cancelMarkerPath`, but a DISTINCT
 *  directory, so "expired" and "cancelled" can never be confused by a later
 *  reader even though both end a queued ticket the same way (never leased). */
export function expireMarkerPath(root, id) {
  return path.join(paths(root).expire, id);
}

/** Does a queue-timeout expiry marker exist for `id`? Existence-only, same
 *  contract as `isCancelled`. */
export function isExpired(root, id) {
  return fs.existsSync(expireMarkerPath(root, id));
}

/** Write the queue-timeout expiry marker for `id`, durably and idempotently --
 *  the expiry counterpart of `writeCancelMarkerFile`, written by
 *  `scheduler.js`'s `tryStart` under the same admission lock as every other
 *  decision it makes. */
export function writeExpireMarkerFile(root, id) {
  fs.mkdirSync(paths(root).expire, { recursive: true });
  atomicWriteFile(expireMarkerPath(root, id), String(Date.now()));
}

/** Deterministic path of a ticket's withdraw marker (BRAIN-436): a queued remote ticket its submitter has taken back to
 *  move it to another runner. A DISTINCT directory from cancel/expire so the three can never be confused. */
export function withdrawMarkerPath(root, id) {
  return path.join(paths(root).withdraw, id);
}

/** Does a withdraw marker exist for `id`? Existence-only, same contract as `isCancelled`. */
export function isWithdrawn(root, id) {
  return fs.existsSync(withdrawMarkerPath(root, id));
}

/** Write the withdraw marker for `id`, fsynced. It is the irreversible commit of a withdrawal: written under the
 *  admission lock, never removed except with the ticket's other markers, and `tryStart` refuses a ticket that has it. */
export function writeWithdrawMarkerFile(root, id) {
  fs.mkdirSync(paths(root).withdraw, { recursive: true });
  atomicWriteFile(withdrawMarkerPath(root, id), String(Date.now()), { fsync: true });
}

export function ensureStateDirs(root = stateHome()) {
  const p = paths(root);
  for (const dir of [p.root, p.leases, p.queue, p.logs, p.results, p.cancel, p.expire, p.attempts]) {
    fs.mkdirSync(dir, { recursive: true });
  }
  return p;
}

/**
 * Write `data` to `file` atomically: write to a temp file in the same dir, then rename. Never follows symlinks.
 * `fsync` (BRAIN-380's migration writes only) makes the result durable across power loss: the file is fsynced before
 * the rename and the directory after it, so neither the bytes nor the rename can be lost behind a later write.
 */
export function atomicWriteFile(file, data, { fsync = false } = {}) {
  const dir = path.dirname(file);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  if (fsync) {
    const fd = fs.openSync(tmp, 'wx');
    try {
      fs.writeFileSync(fd, data);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  } else {
    fs.writeFileSync(tmp, data, { flag: 'wx' });
  }
  fs.renameSync(tmp, file);
  if (fsync) fsyncDirectory(dir);
}

/** Make a rename, create or unlink inside `dir` durable. */
export function fsyncDirectory(dir) {
  const dirFd = fs.openSync(dir, 'r');
  try {
    fs.fsyncSync(dirFd);
  } finally {
    fs.closeSync(dirFd);
  }
}

export function atomicWriteJson(file, obj, opts) {
  atomicWriteFile(file, `${JSON.stringify(obj, null, 2)}\n`, opts);
}

export const DRAINING_MESSAGE = 'scheduler migration pending (draining)';

/** Thrown by `assertNotMigrating`; callers map it to exit 75 ("try again later", the same convention as a lock timeout). */
export class MigrationInProgressError extends Error {
  constructor(message = 'scheduler migration in progress') {
    super(message);
    this.name = 'MigrationInProgressError';
    this.code = 'LANE_MIGRATION_IN_PROGRESS';
  }
}

/** What `lane migrate-scheduler --when-idle` writes into PAUSE when IT pauses the broker (see `pausedByDrain`). Includes the owner's pid, so no other pause can carry it by accident. */
export const drainPauseReason = (pid) => `paused for scheduler migration (lane migrate-scheduler --when-idle, pid ${pid})`;

/**
 * The drain marker, or null when there is none. `state` is:
 *  - `live`: the `lane migrate-scheduler --when-idle` that wrote it is running (pid alive AND the same start time, which
 *    defeats pid reuse; see `isProcessAlive`);
 *  - `stale`: it parsed, and its owner is confirmed gone;
 *  - `unknown`: the file cannot be read, parsed or understood. Fails closed: intake refuses, and nothing deletes it until
 *    it can be judged (`drainBlocksIntake`, `clearStaleDrainMarker`). An operator clears one by hand: see the README.
 * `pausedByDrain` is true when that drain, not an operator, paused the broker (so recovery may resume it).
 */
export function readDrainMarker(root) {
  const file = paths(root).draining;
  let raw;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    return { state: 'unknown', pid: null, startedAt: null, startTime: null, pausedByDrain: false };
  }
  let marker;
  try {
    marker = JSON.parse(raw);
  } catch {
    marker = null;
  }
  if (marker === null || typeof marker !== 'object' || !Number.isInteger(marker.pid)) {
    return { state: 'unknown', pid: null, startedAt: null, startTime: null, pausedByDrain: false };
  }
  return {
    state: isProcessAlive(marker.pid, marker.startTime) ? 'live' : 'stale',
    pid: marker.pid,
    startedAt: marker.startedAt ?? null,
    startTime: marker.startTime ?? null,
    pausedByDrain: marker.pausedByDrain === true,
  };
}

/** Does this marker refuse new intake? Everything but a confirmed-dead owner does. */
export const drainBlocksIntake = (marker) => marker !== null && marker.state !== 'stale';

/**
 * Release the pause a drain made, if it still owns it: the marker says the drain paused the broker AND PAUSE still holds
 * that drain's own reason. A pause someone else set (or re-set) is never touched. Caller holds the broker lock.
 */
export function releaseDrainPause(root, marker) {
  const file = paths(root).pause;
  if (!marker.pausedByDrain || marker.pid === null) return false;
  let reason;
  try {
    reason = fs.readFileSync(file, 'utf8');
  } catch {
    return false;
  }
  if (reason !== drainPauseReason(marker.pid)) return false;
  fs.rmSync(file, { force: true });
  fsyncDirectory(root);
  return true;
}

/**
 * Clean up after a SIGKILLed drain: resume the broker if that drain paused it (`releaseDrainPause`), then remove its
 * marker. Only a marker that parsed AND whose owner is confirmed dead is touched; an `unknown` one stays. Caller holds
 * the broker lock: an unlocked remove could delete a marker a new drain just wrote. Returns whether one was removed.
 */
export function clearStaleDrainMarker(root) {
  const marker = readDrainMarker(root);
  if (marker?.state !== 'stale') return false;
  releaseDrainPause(root, marker);
  fs.rmSync(paths(root).draining, { force: true });
  fsyncDirectory(root);
  return true;
}

/**
 * Test seam (precedent: LANE_BROKER_TEST_PAUSE_AFTER_TICKET_ID): `LANE_BROKER_TEST_DRAIN_AT=<point>` starts a drain, owned by
 * the live pid in `LANE_BROKER_TEST_DRAIN_PID`, the moment a caller reaches `<point>`, so a test can land the drain
 * exactly in the window between an intake check and the write it guards. A no-op unless the variable is set.
 */
export function testDrainAt(root, point) {
  if (process.env.LANE_BROKER_TEST_DRAIN_AT !== point) return;
  const pid = Number(process.env.LANE_BROKER_TEST_DRAIN_PID);
  atomicWriteJson(paths(root).draining, { pid, startTime: processStartTime(pid) ?? null, startedAt: Date.now() });
}

/**
 * Test seam: `LANE_BROKER_TEST_HOLD_AT=<point>` makes a caller reaching `<point>` write `LANE_BROKER_TEST_HOLD_READY` and wait
 * for `LANE_BROKER_TEST_HOLD_GO` to exist, so a test can act at an exact point of a multi-step intake. A no-op unless set.
 */
export async function testHoldAt(point) {
  if (process.env.LANE_BROKER_TEST_HOLD_AT !== point) return;
  fs.writeFileSync(process.env.LANE_BROKER_TEST_HOLD_READY, 'x');
  while (!fs.existsSync(process.env.LANE_BROKER_TEST_HOLD_GO)) await sleep(20);
}

/**
 * The one guard every new-code intake entry point calls. Existence-only for `migrating` (the migrator's own business);
 * a drain marker refuses while its owner is alive or it cannot be judged, so a SIGKILLed `--when-idle` cannot block
 * intake for ever (a stale marker is ignored) but a half-written or damaged one never lets intake through.
 */
export function assertNotMigrating(root) {
  if (fs.existsSync(paths(root).migrating)) throw new MigrationInProgressError();
  if (drainBlocksIntake(readDrainMarker(root))) throw new MigrationInProgressError(DRAINING_MESSAGE);
}

/** An unreadable record found by a strict listing; `file` names it. */
export class UnreadableRecordError extends Error {
  constructor(file, cause) {
    super(`${file}: ${cause}`);
    this.name = 'UnreadableRecordError';
    this.file = file;
  }
}

/**
 * Fail-closed counterpart of `readJsonSafe` + `.filter(Boolean)`: every `*.json` in `dir`, sorted by name, THROWING
 * `UnreadableRecordError` for any file that cannot be read or is not a JSON object. Only a file that vanishes between
 * the listing and the read (a benign removal race) is skipped, as is a directory that does not exist.
 */
export function listJsonRecordsStrict(dir) {
  let names;
  try {
    names = fs.readdirSync(dir).sort();
  } catch (err) {
    if (err.code === 'ENOENT') return [];
    throw new UnreadableRecordError(dir, err.message);
  }
  const records = [];
  for (const name of names.filter((n) => n.endsWith('.json'))) {
    const file = path.join(dir, name);
    let record;
    try {
      record = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (err) {
      if (err.code === 'ENOENT') continue;
      throw new UnreadableRecordError(file, err.message);
    }
    if (record === null || typeof record !== 'object' || Array.isArray(record)) throw new UnreadableRecordError(file, 'not a JSON object');
    records.push(record);
  }
  return records;
}

/**
 * Encode one fingerprint value so the join below can't be forged: a plain
 * `number` is its bare `String(...)` form (a JS number's string form never
 * contains the `|` delimiter or a quote character, and this keeps the
 * fingerprint of today's all-numeric callers byte-identical to before this
 * function existed — no forced reset of already-persisted gate state).
 * Anything else (string, boolean, etc.) goes through `JSON.stringify`, which
 * is self-delimiting: it owns its surrounding quotes and escapes any
 * interior quote or backslash, so an embedded `|` inside a string value
 * stays unambiguously inside that one token instead of reading as a
 * separator. Mixing a bare number token with a quoted JSON token is also
 * how `1` and `"1"` end up encoded differently, which a delimiter-joined
 * `String(value)` on its own would not do.
 */
function encodeFingerprintValue(value) {
  return typeof value === 'number' ? String(value) : JSON.stringify(value);
}

/**
 * Fingerprint of the threshold values that govern a gate's hysteresis
 * (order matters), so a config change can be detected against shared,
 * unversioned gate state written by multiple independent readers —
 * concurrent supervisors reload their own config independently and all
 * write the same shared file, so a threshold edit lands mid-countdown for
 * some of them and not others; blending a countdown started under old
 * thresholds with samples judged under new ones can produce a decision no
 * config that was ever installed would have produced. Not a cryptographic
 * hash, just a stable, cheap-to-compute-every-poll join. Shared by every
 * gate (src/load.js, src/admission.js) so they can't drift apart by each
 * keeping a private copy.
 */
export function fingerprintOf(...values) {
  return values.map(encodeFingerprintValue).join('|');
}

export function readJsonSafe(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return null;
  }
}

export function appendHistory(root, record) {
  try {
    fs.appendFileSync(paths(root).history, `${JSON.stringify(record)}\n`);
  } catch {
    // Disk-full or similar on the history log must never fail the lane.
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Is the owner of the lock alive? Fails closed: an indeterminate probe never counts as stale. */
function isLockOwnerAlive(owner) {
  if (!owner) return false;
  if (!isPidAlive(owner.pid)) return false;
  if (!owner.start) return true; // couldn't capture a start time at acquire time; fall back to pid-alive
  const current = processStartTime(owner.pid);
  if (current === undefined) return true; // probe failed: fail closed, never steal
  if (current === null) return false; // confirmed gone
  return current === owner.start;
}

/** How long a tomb (a moved-aside dead lock dir) is kept before GC removes it. Exported for tests.
 * 7 days: the residual this trades off against is a contender suspended longer than the TTL
 * between observing a dead owner and attempting its rename — 7 days makes that a non-event on a
 * machine that reboots. */
export const TOMB_TTL_MS = 7 * 24 * 60 * 60 * 1000;

/** Deterministic tomb path for a dead owner: one per crashed holder (pid + its random token). */
function tombPath(root, ownerPid, ownerToken) {
  return path.join(root, `.lock-tomb-${ownerPid}-${ownerToken}`);
}

/**
 * Thrown by `withLock` when it gives up waiting for the global lock, on every
 * timeout path. Contention, not a fault: a caller that can simply try again
 * (a queued supervisor's poll loop) catches this class by identity, never by
 * message text.
 */
export class LockTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'LockTimeoutError';
    this.code = 'LANE_LOCK_TIMEOUT';
  }
}

// `LANE_BROKER_TEST_LOCK_TIMEOUT_MS` (README "Testing hooks") shortens the
// default acquire deadline so a test can force a timeout without holding the
// lock for 15s.
function lockTimeoutMsDefault() {
  const override = Number(process.env.LANE_BROKER_TEST_LOCK_TIMEOUT_MS);
  return Number.isFinite(override) && override > 0 ? override : 15_000;
}

/**
 * Best-effort GC of tombs older than TOMB_TTL_MS. Run once per withLock call,
 * before the acquire loop. A contender suspended for longer than the TTL
 * between reading a dead owner and attempting the takeover rename could in
 * theory displace a live lock that has since re-acquired the same tomb name
 * (vanishingly unlikely: tokens are random UUIDs) — that is the accepted
 * bound this TTL trades off against leaking tomb directories forever.
 *
 * Ages tombs by the `stolen-at` timestamp written INSIDE the directory at
 * takeover time, never by the directory's own mtime: renaming lockDir onto
 * the tomb path keeps the directory's OLD mtime (only its parent changes on a
 * rename), so a lock acquired more than TTL ago would otherwise become an
 * immediately GC-eligible tomb the instant it is moved — and deleting a fresh
 * tomb reopens the exact stale-contender hole the tomb exists to close. A
 * tomb with a missing or unparsable `stolen-at` is kept (fail safe: never
 * delete what we cannot date).
 */
function gcTombs(root) {
  let names;
  try {
    names = fs.readdirSync(root);
  } catch {
    return;
  }
  const now = Date.now();
  for (const name of names) {
    if (!name.startsWith('.lock-tomb-')) continue;
    const full = path.join(root, name);
    let raw;
    try {
      raw = fs.readFileSync(path.join(full, 'stolen-at'), 'utf8');
    } catch {
      continue; // missing stamp: keep, fail safe
    }
    // Strict: a real stamp is a positive epoch-ms integer. Number('') is 0
    // and passes isFinite, which would make an empty or truncated stamp read
    // as 1970 and GC a brand-new tomb — the exact hole this stamp closes.
    const stolenAtMs = /^\d{12,}$/.test(raw.trim()) ? Number(raw.trim()) : NaN;
    if (!Number.isFinite(stolenAtMs)) continue; // missing/empty/unparsable stamp: keep, fail safe
    if (now - stolenAtMs > TOMB_TTL_MS) {
      try {
        fs.rmSync(full, { recursive: true, force: true });
      } catch {
        // leaked; harmless, will be retried on a future GC pass
      }
    }
  }
}

/**
 * Take over a lock whose owner was just observed dead, by renaming the whole
 * lock DIRECTORY onto a tomb path. This replaces an earlier claim/tomb-FILE
 * protocol that CAS'd only on the *existence* of a fixed `owner.json` name:
 * a delayed, stale rename from a contender that observed the same dead owner
 * could land on a NEW, live owner's record (a fixed destination name means
 * "does src still exist" is not the same question as "is src still the dead
 * owner I observed"), and two independent stale contenders promoting two
 * different dead observations could both end up believing they hold the
 * lock. Renaming the DIRECTORY, keyed by a name derived from the dead
 * owner's own (pid, token), fixes both: the tomb name is deterministic per
 * dead acquisition, so a second stale contender's rename onto the SAME tomb
 * name fails atomically (rename onto an existing non-empty directory is
 * ENOTEMPTY/EEXIST) instead of silently succeeding on unrelated state.
 *
 *   1. Re-read owner.json fresh; if its token no longer matches what we
 *      observed, someone already recovered this dead lock — give up and let
 *      the caller loop.
 *   2. `renameSync(lockDir, tomb)`, tomb = `.lock-tomb-<pid>-<token>` under
 *      root (sibling of lockDir, not inside it — the rename must move the
 *      whole lockDir, so the destination can't be a path under itself).
 *      Any failure here means we lost the race: either another contender
 *      already moved this exact dead lock to the same tomb name (tomb
 *      exists, non-empty -> ENOTEMPTY/EEXIST), or lockDir is already gone or
 *      was re-acquired out from under us.
 *   3. Verify what we actually moved: read `tomb/owner.json` and confirm its
 *      token still matches. This should be unreachable — while `owner.json`
 *      names a dead pid and the tomb name is derived from that exact
 *      (pid, token) pair, nothing else can have repopulated lockDir with a
 *      DIFFERENT live owner under the same tomb name — but if it somehow
 *      happens, rename the tomb back onto lockDir rather than strand a live
 *      lock in tomb form; if that restore itself fails, throw loudly rather
 *      than silently leave the lock undiscoverable.
 *   4. Do NOT delete the tomb. It must persist: a stale contender B that
 *      observed the SAME dead owner will later attempt
 *      `renameSync(lockDir, tomb)` with the identical tomb name; because the
 *      tomb still exists and is non-empty, B's rename fails, so B can never
 *      move the live lock that a subsequent acquire will have placed back at
 *      lockDir. Deleting the tomb here is exactly the hole this protocol
 *      replaces (an earlier version deleted it immediately after a
 *      successful takeover, which reopened the same stale-contender race).
 *      Tombs are tiny (one JSON file) and are reclaimed by `gcTombs` above.
 *
 * Returns true if this call moved the dead lock dir out of the way (the
 * lockDir path is now free for anyone, including us, to acquire normally on
 * the next loop iteration); false if we lost the race. Neither outcome
 * grants ownership by itself — the caller always re-enters the normal
 * acquire path afterward.
 */
function recoverDeadLock(root, lockDir, ownerFile, observedOwner) {
  const current = readJsonSafe(ownerFile);
  if (!current || current.token !== observedOwner.token) {
    return false; // already recovered by someone else; re-evaluate from scratch
  }

  // Stamp the takeover time INSIDE the directory before moving it, so GC can
  // age the tomb by when it was taken over rather than by the directory's
  // own (stale, pre-rename) mtime. Writing into a dead owner's dir is
  // harmless; if the owner has meanwhile changed to a live one, the stray
  // file rides along until release renames the whole dir away. If the write
  // fails, a tomb without a stamp must never be created by us — skip the
  // rename and report the loss like any other failed takeover.
  try {
    // Atomic stamp: a per-contender temp file renamed onto `stolen-at`. Two
    // contenders racing plain writeFileSync on the SAME path can leave it
    // momentarily EMPTY (one truncates while the other is descheduled), and
    // an empty stamp must never read as "ancient" to GC. Rename replaces the
    // whole content or nothing.
    const stampTmp = path.join(lockDir, `stolen-at.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`);
    fs.writeFileSync(stampTmp, String(Date.now()));
    fs.renameSync(stampTmp, path.join(lockDir, 'stolen-at'));
  } catch {
    return false;
  }

  const tomb = tombPath(root, observedOwner.pid, observedOwner.token);
  try {
    fs.renameSync(lockDir, tomb);
  } catch {
    return false;
  }

  const moved = readJsonSafe(path.join(tomb, 'owner.json'));
  if (!moved || moved.token !== observedOwner.token) {
    try {
      fs.renameSync(tomb, lockDir);
    } catch (err) {
      throw new Error('lane-broker: lock takeover displaced a live lock and could not restore it');
    }
    return false;
  }

  return true;
}

/**
 * Acquire the global mutex. Ownership is published atomically: the owner
 * file is written inside a staging directory first, then the whole staging
 * directory is renamed onto the lock dir path. A rename onto an existing
 * non-empty directory fails atomically (ENOTEMPTY/EEXIST/ENOTDIR), so a
 * contender can never observe a lock dir with no owner file — the two-step
 * mkdir-then-write race this replaces is exactly what let a mid-acquire lock
 * be mistaken for stale and stolen.
 *
 * Release is: rename the whole lock dir away (one atomic syscall so the
 * "lock" path goes straight from existing-with-our-owner to gone) then `rm`
 * it, guarded by a compare-and-remove fallback keyed on our token if the
 * rename itself fails. A live holder can never be displaced under this
 * protocol (see below), so whoever reaches release is always the
 * legitimate holder.
 *
 * Recovery (taking over a lock whose owner is confirmed dead — pid + start
 * time, to defeat pid reuse, never elapsed time alone, and never on an
 * indeterminate liveness probe) is `recoverDeadLock` (above): rename the
 * whole lock DIRECTORY onto a tomb path keyed by the dead owner's own
 * (pid, token), so the CAS is on "is this still the exact dead lock I
 * observed", not merely "does a fixed-name file still exist". At most one
 * contender's rename can land on any given tomb name; every later one gets
 * ENOTEMPTY/EEXIST and has lost the race. The tomb is never deleted early —
 * see `recoverDeadLock`'s own comment for why that permanence is what makes
 * a second, stale contender's takeover of the SAME dead owner fail cleanly
 * instead of silently displacing whoever re-acquired lockDir since.
 *
 * `recoverDeadLock` never itself grants ownership: whether it moves the dead
 * lock aside or loses that race, the caller always loops back to the normal
 * acquire path above, and the lockDir path (now absent, since the dead lock
 * was moved to its tomb) is acquired by whichever contender's staging
 * rename lands first — the same single-winner rename semantics as any other
 * acquire, so no separate "I already hold it" special case exists.
 *
 * Invariants: lockDir exists <=> someone holds it, or a foreign/legacy dir
 * with no readable owner.json sits there (handled as "young", timed out and
 * reported by name rather than assumed stale). A live holder can never be
 * displaced — nothing in this protocol renames or removes lockDir except
 * its own release, or a takeover that first re-confirms the exact dead
 * (pid, token) it is acting on. "How long has this sat here" never enters a
 * takeover decision; only liveness does.
 */
export async function withLock(root, fn, { timeoutMs = lockTimeoutMsDefault(), pollMs = 25 } = {}) {
  const lockDir = paths(root).lock;
  fs.mkdirSync(root, { recursive: true });
  const ownerFile = path.join(lockDir, 'owner.json');
  const deadline = Date.now() + timeoutMs;
  const token = crypto.randomUUID();
  const start = processStartTime(process.pid);

  gcTombs(root);

  for (;;) {
    const staging = path.join(root, `.lock-stage-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
    let acquired = false;
    try {
      fs.mkdirSync(staging, { recursive: true });
      fs.writeFileSync(
        path.join(staging, 'owner.json'),
        JSON.stringify({ pid: process.pid, token, start: start === undefined ? null : start, at: Date.now() }),
      );
      fs.renameSync(staging, lockDir);
      acquired = true;
    } catch {
      // Any failure here (contention, or a transient ENOENT/EINVAL racing a
      // concurrent recoverer) must never escape as a hard error — it is
      // treated as contention and retried below.
      try {
        fs.rmSync(staging, { recursive: true, force: true });
      } catch {
        // already gone
      }
    }
    if (acquired) break;

    const owner = readJsonSafe(ownerFile);
    if (!owner) {
      // owner.json unreadable while lockDir still exists: a foreign/legacy
      // dir with no owner.json of our shape, or a transient race with a
      // concurrent recoverer's rename. Either way there is nothing to act
      // on but wait and re-evaluate; report by name rather than assume
      // stale.
      if (Date.now() > deadline) {
        throw new LockTimeoutError(
          `lane-broker: timed out waiting for the global lock at ${lockDir} — owner.json is unreadable; needs manual removal`,
        );
      }
      await sleep(pollMs);
      continue;
    }
    if (!isLockOwnerAlive(owner)) {
      // recoverDeadLock never itself grants ownership: whether it moves the
      // dead lock dir aside or loses that race, loop back to the top and
      // re-attempt the normal staging rename — the lockDir path is either
      // now free (we or someone else moved it to a tomb) or still occupied
      // by whoever won the CAS, and either way the single-winner rename
      // above is what actually decides the next owner.
      const won = recoverDeadLock(root, lockDir, ownerFile, owner);
      if (won && Date.now() > deadline) {
        // We moved a dead lock aside but our own deadline has passed: leave
        // the path free for whoever is still waiting and give up honestly.
        throw new LockTimeoutError(
          `lane-broker: timed out waiting for the global lock (recovered dead pid ${owner.pid}, but the deadline passed)`,
        );
      }
      if (won) continue;
      // Recovery lost the race or failed outright. A persistently failing
      // recovery (e.g. something occupies the tomb path and rename keeps
      // failing) must never spin the CPU with no sleep and no deadline
      // check — sleep and re-evaluate the deadline like any other
      // contention wait.
      if (Date.now() > deadline) {
        throw new LockTimeoutError(
          `lane-broker: timed out waiting for the global lock held by dead pid ${owner.pid} (recovery kept failing)`,
        );
      }
      await sleep(pollMs);
      continue;
    }
    if (Date.now() > deadline) {
      throw new LockTimeoutError(`lane-broker: timed out waiting for the global lock held by pid ${owner.pid}`);
    }
    await sleep(pollMs);
  }

  try {
    return await fn();
  } finally {
    const owner = readJsonSafe(ownerFile);
    if (owner && owner.token === token) {
      // A recursive rmSync on the live path is not atomic: it unlinks
      // owner.json, then rmdir's the now-empty lockDir as a second step. In
      // that gap a concurrent contender's rename(staging, lockDir) can land
      // on the momentarily-empty directory and succeed, so our rmdir then
      // fails on a directory someone else just repopulated (ENOTEMPTY).
      // Renaming the whole lock dir away first is one atomic syscall: the
      // "lock" path goes straight from existing-with-our-owner to gone.
      const trash = path.join(root, `.lock-release-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
      let renamedAway = false;
      try {
        fs.renameSync(lockDir, trash);
        renamedAway = true;
      } catch {
        // The rename itself failed (lockDir already gone, or some other
        // transient issue) — fall back to a guarded compare-and-remove: only
        // touch lockDir if it still shows our own token, since a contender
        // may have already acquired it.
        const current = readJsonSafe(ownerFile);
        if (current && current.token === token) {
          try {
            fs.rmSync(lockDir, { recursive: true, force: true });
          } catch {
            // already gone
          }
        }
      }
      if (renamedAway) {
        // We are released: the lockDir path is free (or already reclaimed by
        // a contender). Cleaning up the trash directory is best-effort — a
        // failure here is a leak, never a reason to touch lockDir again,
        // since a contender may already own that path by now.
        try {
          fs.rmSync(trash, { recursive: true, force: true });
        } catch {
          // leaked trash dir; ignore
        }
      }
    }
  }
}

let cachedBootId = process.env.LANE_BROKER_BOOT_ID || null;

/** A boot identifier that changes across a reboot: darwin sysctl, linux /proc/stat btime,
 *  or a computed epoch-boot-time fallback derived from os.uptime(). The fallback is rounded
 *  to the nearest 10s so two processes sampling Date.now()/os.uptime() microseconds apart
 *  agree on the same id instead of drifting by a second and reaping each other's leases. */
export function bootId() {
  if (process.env.LANE_BROKER_BOOT_ID) return process.env.LANE_BROKER_BOOT_ID;
  if (cachedBootId) return cachedBootId;
  try {
    if (process.platform === 'darwin') {
      const out = execFileSync('sysctl', ['-n', 'kern.boottime'], { encoding: 'utf8' });
      const m = out.match(/sec\s*=\s*(\d+)/);
      if (m) {
        cachedBootId = m[1];
        return cachedBootId;
      }
    } else if (process.platform === 'linux') {
      const out = fs.readFileSync('/proc/stat', 'utf8');
      const m = out.match(/^btime\s+(\d+)/m);
      if (m) {
        cachedBootId = m[1];
        return cachedBootId;
      }
    }
  } catch {
    // fall through to the computed fallback
  }
  const approxBootEpoch = Date.now() / 1000 - os.uptime();
  cachedBootId = String(Math.round(approxBootEpoch / 10) * 10);
  return cachedBootId;
}

/** Test-only access to the lock takeover's internal steps, so tests can drive the CAS directly instead of racing real processes. Not part of the public API. */
export const __test = { tombPath, recoverDeadLock, isLockOwnerAlive, gcTombs };
