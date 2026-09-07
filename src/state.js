import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

export function stateHome() {
  return process.env.LANE_BROKER_STATE || path.join(os.homedir(), '.cache', 'lane-broker');
}

export function paths(root = stateHome()) {
  return {
    root,
    leases: path.join(root, 'leases'),
    queue: path.join(root, 'queue'),
    logs: path.join(root, 'logs'),
    results: path.join(root, 'results'),
    cancel: path.join(root, 'cancel'),
    history: path.join(root, 'history.jsonl'),
    pause: path.join(root, 'PAUSE'),
    lock: path.join(root, 'lock'),
    loadGate: path.join(root, 'load-gate.json'),
    configWarning: path.join(root, 'config-warning.json'),
    cpuSample: path.join(root, 'cpu-sample.json'),
    cpuGate: path.join(root, 'cpu-gate.json'),
    admissionLog: path.join(root, 'admission-decisions.log'),
    conflictSkipState: path.join(root, 'conflict-skip-state.json'),
    seq: path.join(root, 'seq'),
  };
}

export function ensureStateDirs(root = stateHome()) {
  const p = paths(root);
  for (const dir of [p.root, p.leases, p.queue, p.logs, p.results, p.cancel]) {
    fs.mkdirSync(dir, { recursive: true });
  }
  return p;
}

/** Write `data` to `file` atomically: write to a temp file in the same dir, then rename. Never follows symlinks. */
export function atomicWriteFile(file, data) {
  const dir = path.dirname(file);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  fs.writeFileSync(tmp, data, { flag: 'wx' });
  fs.renameSync(tmp, file);
}

export function atomicWriteJson(file, obj) {
  atomicWriteFile(file, `${JSON.stringify(obj, null, 2)}\n`);
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

export function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Process start time, used to defeat PID reuse. Returns:
 *  - a string (the `ps` start-time line) if the process is alive,
 *  - `null` if `ps` ran and confirmed the pid does not exist,
 *  - `undefined` if the probe itself failed (e.g. cannot fork under load) —
 *    callers must treat this as "could not determine", never as "gone".
 * Pinned to the C locale so the output format is stable across environments.
 */
export function processStartTime(pid) {
  try {
    const out = execFileSync('ps', ['-o', 'lstart=', '-p', String(pid)], {
      encoding: 'utf8',
      env: { ...process.env, LC_ALL: 'C', LC_TIME: 'C' },
    }).trim();
    return out || null;
  } catch (err) {
    // `ps` ran and exited non-zero (pid not found) -> confirmed gone. A
    // spawn-level failure (EAGAIN under load, ENOENT, EPERM, ...) commonly
    // carries `status: null` too, so only a numeric status counts as "ran".
    if (typeof err.status === 'number') return null;
    // execFileSync itself failed to run `ps` -> indeterminate.
    return undefined;
  }
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
export async function withLock(root, fn, { timeoutMs = 15_000, pollMs = 25 } = {}) {
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
        throw new Error(
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
        throw new Error(
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
        throw new Error(
          `lane-broker: timed out waiting for the global lock held by dead pid ${owner.pid} (recovery kept failing)`,
        );
      }
      await sleep(pollMs);
      continue;
    }
    if (Date.now() > deadline) {
      throw new Error(`lane-broker: timed out waiting for the global lock held by pid ${owner.pid}`);
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
