import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, readJsonSafe, withLock, bootId, isCancelled, writeExpireMarkerFile, appendHistory, assertNotMigrating, listJsonRecordsStrict, MigrationInProgressError } from './state.js';
import { sampleAndUpdateGate, readGateState } from './load.js';
import { patchAttemptLocked, readAttempt } from './attempts.js';
import { listLeases, reapAll, writeLease, isSupervisorAlive, LEASE_STATE } from './lease.js';
import { evaluateNewAdmission, evaluateElasticAdmission, cooldownActive, sampleCpuSafe, logAdmissionDecision, logHeadBlock, logCapacityBlock, logResourceEvent, projectBusy, ticketCpuEstimate } from './admission.js';
import { readMemoryInfo } from './cpu.js';
import { captureShadowInputs, recordAllocationShadow } from './allocation-shadow.js';
import { classOf } from './allocation.js';
import { reconcileSimArm, touchSimArmFor } from './sim-arm.js';
import { advanceHwm } from './priority-clock.js';
import { DEFAULT_PRIORITY, isPriorityTier, originOrNow, priorityOf, effectiveRank, score } from './priority.js';
import { DEFAULT_GLOBAL_CONFIG } from './config.js';
import { resolveScheduler, legacyStore, fairnessStore, effectiveView, readSchedulerFence } from './fairness.js';
import { detectResourceCapacity, effectiveWeightCapacity, evaluateMemoryAdmission, resolveTicketResources } from './resources.js';

/** Leases that hold their key: RUNNING and ORPHANED both represent real,
 *  possibly-running work and must count against both conflicts and capacity. */
export const HELD_STATES = new Set([LEASE_STATE.RUNNING, LEASE_STATE.ORPHANED]);

function nextSeq(root) {
  const file = paths(root).seq;
  let n = 0;
  const cur = readJsonSafe(file);
  if (cur && Number.isFinite(cur.n)) n = cur.n;
  n += 1;
  atomicWriteJson(file, { n });
  return n;
}

function queueFile(root, seq, id) {
  return path.join(paths(root).queue, `${String(seq).padStart(12, '0')}-${id}.json`);
}

export function listQueue(root) {
  const dir = paths(root).queue;
  let names;
  try {
    names = fs.readdirSync(dir).sort();
  } catch {
    return [];
  }
  return names.filter((n) => n.endsWith('.json')).map((n) => readJsonSafe(path.join(dir, n)));
}

/** Every queue record in FIFO order, or a thrown `UnreadableRecordError` (where `listQueue` yields `null` for it). */
export function listQueueStrict(root) {
  return listJsonRecordsStrict(paths(root).queue);
}

/** Thrown when a valid scheduler fence coexists with a non-empty legacy `queue/` directory: a crash or tampering, never a state to guess about. */
export class LegacyQueueAfterFenceError extends Error {
  constructor(dir) {
    super(`lane-broker: the scheduler fence is present but the legacy queue directory ${dir} still holds tickets; refusing admission. Drain it by hand, or remove sched-v2.json to run the legacy scheduler`);
    this.name = 'LegacyQueueAfterFenceError';
  }
}

/** Caller holds the lock. New code never queues into a legacy `queue/` directory behind the fence. */
function assertQueueLayout(root) {
  if (readSchedulerFence(root).status !== 'valid') return;
  const legacy = path.join(root, 'queue');
  let names;
  try {
    names = fs.readdirSync(legacy);
  } catch {
    return; // the fence file (ENOTDIR) or nothing at all: the intended layout
  }
  if (names.length > 0) throw new LegacyQueueAfterFenceError(legacy);
}

/**
 * BRAIN-380: behind the fence, a queue record that does not carry `schedVersion` 2 can only come from a process that
 * escaped the migration's quiescence checks. It is contained, never adopted: moved to `queue-quarantine/` and logged,
 * so it is never selected, and its supervisor then finds its queue file gone and exits as cancelled. Caller holds the lock.
 * A record whose rename fails is not quarantined, so the caller must treat it as an unreadable barrier: see `fenceLegacy`.
 */
function quarantineLegacyRecords(root) {
  const dir = paths(root).queue;
  let names;
  try {
    names = fs.readdirSync(dir).sort();
  } catch {
    return;
  }
  for (const name of names.filter((n) => n.endsWith('.json'))) {
    const record = readJsonSafe(path.join(dir, name));
    if (record === null || typeof record !== 'object' || record.schedVersion === 2) continue;
    try {
      fs.mkdirSync(paths(root).queueQuarantine, { recursive: true });
      fs.renameSync(path.join(dir, name), path.join(paths(root).queueQuarantine, name));
      logResourceEvent(root, 'legacy-record-after-fence', { ticket: record.id ?? 'unknown', file: name, action: 'quarantined' });
    } catch (err) {
      logResourceEvent(root, 'legacy-record-after-fence', { ticket: record.id ?? 'unknown', file: name, action: 'quarantine-failed', error: err.code ?? 'error' });
    }
  }
}

/** Behind the fence a record without `schedVersion` 2 is never selectable, whatever became of its quarantine: it reads as a barrier. */
const fenceLegacy = (rawQueue) => rawQueue.map((t) => (t !== null && t.schedVersion !== 2 ? null : t));

function findQueueFile(root, id) {
  const dir = paths(root).queue;
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return null;
  }
  const match = names.find((n) => n.endsWith(`-${id}.json`));
  return match ? path.join(dir, match) : null;
}

/**
 * Enqueue a ticket at the tail of the global FIFO. Caller must NOT hold the lock. Returns the persisted record, whose
 * `priorityRequested`/`priorityAdmitted`/`priorityDemoted` tell the caller whether the per-repo high cap demoted it.
 */
export async function enqueue(root, ticket, { maxQueuedHighPerRepo } = DEFAULT_GLOBAL_CONFIG) {
  return withLock(root, () => {
    assertNotMigrating(root);
    assertQueueLayout(root);
    const nowEff = advanceHwm(root);
    const priorityRequested = isPriorityTier(ticket.priorityRequested) ? ticket.priorityRequested : DEFAULT_PRIORITY;
    // BRAIN-380 §7: the high cap, per broker. It shares this lock with the seq allocation and the queue write below,
    // so two racing enqueues cannot both read "no high queued". Across lanes and worktrees. A queued high whose supervisor
    // is dead does not hold the slot, but is not reaped here: enqueue must not mutate other tickets' state (tryStart reaps).
    let demoted = false;
    if (priorityRequested === 'high') {
      const queuedHigh = listQueue(root).filter(
        (t) =>
          t &&
          t.repoId === ticket.repoId &&
          t.priorityRequested === 'high' &&
          t.priorityAdmitted === 'high' &&
          isSupervisorAlive({ supervisorPid: t.supervisorPid, supervisorStart: t.supervisorStart }),
      ).length;
      demoted = queuedHigh >= maxQueuedHighPerRepo;
    }
    const seq = nextSeq(root);
    const record = {
      ...ticket,
      seq,
      createdAt: ticket.createdAt || Date.now(),
      priorityRequested,
      priorityAdmitted: demoted ? 'medium' : priorityRequested,
      priorityDemoted: demoted,
      // An origin from `lane run` was stamped under this same lock; one that is missing or ahead of the clock starts at zero age.
      prioOriginAt: originOrNow(ticket.prioOriginAt, nowEff),
      schedVersion: 2,
    };
    atomicWriteJson(queueFile(root, seq, ticket.id), record);
    touchSimArmFor(root, record);
    return record;
  });
}

/** Remove a queue record. Returns whether the ticket is now durably not
 *  queued: true when there was never a file (nothing to remove), when the
 *  file was gone by the time we tried (a benign unlink race, ENOENT, with the
 *  same end state as a successful unlink), or when the unlink succeeded.
 *  Returns false only for a genuine unlink failure (e.g. EPERM) that leaves
 *  the record on disk. A caller that logs a durable "dequeued" event must
 *  check this first, or the log claims a dequeue that never happened. */
export function dequeueSync(root, id) {
  const file = findQueueFile(root, id);
  if (!file) return true;
  try {
    fs.unlinkSync(file);
    return true;
  } catch (err) {
    return err.code === 'ENOENT';
  }
}

const REBINDING_SUFFIX = '.json.rebinding';
const SEQ_PREFIX_LENGTH = 13; // 12 padded digits and the dash

function rebindingNames(root) {
  try {
    return fs.readdirSync(paths(root).queue).filter((n) => n.endsWith(REBINDING_SUFFIX));
  } catch {
    return [];
  }
}

const rebindingFile = (root, id) => {
  const name = rebindingNames(root).find((n) => n.endsWith(`-${id}${REBINDING_SUFFIX}`));
  return name ? path.join(paths(root).queue, name) : null;
};

/** Ids of tickets provisionally withdrawn from the queue (BRAIN-405); the queue listing never shows them. */
export function listRebindingIds(root) {
  return rebindingNames(root).map((n) => n.slice(SEQ_PREFIX_LENGTH, -REBINDING_SUFFIX.length));
}

/**
 * BRAIN-405: provisionally withdraw a still-queued ticket so the caller can dispatch it to a runner. Two-phase and one locked
 * step shared with `tryStart`'s own dequeue-and-lease-write: the queue file is RENAMED to `<seq>-<id>.json.rebinding` (which
 * no queue listing reads, but which keeps the record and its seq on disk) and the attempt is marked `rebinding`, so a crash
 * from here on is recoverable (`recoverRebinding`). The record is returned only if the ticket was still queued (never started,
 * not cancelled, not gone, no migration in progress); otherwise null and nothing changed, so for any one ticket exactly one of
 * the scheduler and this wins. `restoreQueued` or `discardRebinding` ends the withdrawal.
 */
export async function withdrawQueued(root, id) {
  return withLock(root, () => {
    try {
      assertNotMigrating(root);
    } catch (err) {
      if (err instanceof MigrationInProgressError) return null;
      throw err;
    }
    if (isCancelled(root, id)) return null;
    const file = findQueueFile(root, id);
    if (!file) return null;
    const record = readJsonSafe(file);
    if (!record || record.id !== id) return null;
    const parked = file.replace(/\.json$/, REBINDING_SUFFIX);
    const legacyFairness = legacyFairnessOf(root, id);
    try {
      fs.renameSync(file, parked);
    } catch {
      return null;
    }
    try {
      if (legacyFairness) atomicWriteJson(parked, { ...record, legacyFairness });
    } catch {
      // The snapshot is written atomically, so the parked file still holds the original record: put it back in the queue.
      fs.renameSync(parked, file);
      return null;
    }
    try {
      patchAttemptLocked(root, id, { rebinding: { rebindingSince: Date.now(), supervisorPid: process.pid } });
    } catch (err) {
      fs.renameSync(parked, file);
      throw err;
    }
    return record;
  });
}

/**
 * BRAIN-405: the legacy (unfenced) scheduler keeps ONE singleton file per fairness kind, keyed by the head it describes. A
 * ticket's earned count and reservation live there only while it is the head, and the next head overwrites them, so a ticket
 * provisionally withdrawn would come back with nothing. The singletons that describe `id` are snapshotted into its parked file.
 */
function legacyFairnessOf(root, id) {
  if (resolveScheduler(root, { log: false }).v2) return null;
  const files = legacyStore(root).files;
  const held = {};
  for (const [kind, file] of Object.entries(files)) {
    const raw = readJsonSafe(file);
    if (raw && raw.headId === id) held[kind] = raw;
  }
  return Object.keys(held).length > 0 ? held : null;
}

/** BRAIN-405: end a withdrawal that provably never started a remote run: the ticket is queued again at its ORIGINAL seq. */
export async function restoreQueued(root, record) {
  return withLock(root, () => restoreLocked(root, record));
}

function restoreLocked(root, record) {
  let legacyFairness = null;
  if (!findQueueFile(root, record.id)) {
    const parked = rebindingFile(root, record.id);
    if (parked) {
      const { legacyFairness: snapshot, ...parkedRecord } = readJsonSafe(parked) ?? record;
      legacyFairness = snapshot ?? null;
      atomicWriteJson(parked.slice(0, -'.rebinding'.length), parkedRecord);
      fs.unlinkSync(parked);
    } else atomicWriteJson(queueFile(root, record.seq, record.id), record);
  }
  patchAttemptLocked(root, record.id, { rebinding: undefined });
  // it keeps its original seq, so it is the head again; what another head wrote meanwhile was only valid while it was away
  if (legacyFairness && listQueue(root)[0]?.id === record.id && !resolveScheduler(root, { log: false }).v2) {
    const files = legacyStore(root).files;
    for (const [kind, raw] of Object.entries(legacyFairness)) if (files[kind]) atomicWriteJson(files[kind], raw);
  }
}

/** BRAIN-405: the withdrawal is over because the ticket reached a terminal outcome (or is now only a remote attempt). */
export function discardRebinding(root, id) {
  const parked = rebindingFile(root, id);
  if (!parked) return;
  try {
    fs.unlinkSync(parked);
  } catch {
    // already gone
  }
}

/**
 * BRAIN-405: a withdrawal whose supervisor died. Without dispatch evidence in the attempt (it never reached `running` on a
 * runner) nothing was sent, so the ticket goes back at its original seq. With evidence the run may be live on the runner: the
 * attempt is left as the remote attempt to reconcile (ORPHANED-REMOTE) and the parked record is dropped. Caller holds the lock.
 */
export function recoverRebinding(root) {
  for (const name of rebindingNames(root)) {
    const file = path.join(paths(root).queue, name);
    const record = readJsonSafe(file);
    if (!record || isSupervisorAlive({ supervisorPid: record.supervisorPid, supervisorStart: record.supervisorStart })) continue;
    const attempt = readAttempt(root, record.id);
    const dispatched = attempt?.executor === 'remote' && attempt.phase === 'running';
    if (dispatched) discardRebinding(root, record.id);
    else restoreLocked(root, record);
    logResourceEvent(root, 'rebind-recovered', { ticket: record.id, action: dispatched ? 'remote-attempt' : 'restored', seq: record.seq });
  }
}

/**
 * BRAIN-255: whether `held` blocks `ticket` from starting, and by which lease. A declared
 * `conflicts` entry is absolute regardless of `ticket.maxConcurrent` — a lane conflicting with
 * `*` stays exclusive against every other lane no matter its own ceiling — and is checked first.
 * Same-key holders are then counted against `ticket.maxConcurrent` (default 1, resolved in
 * config.js's resolveTicketConfig — this is the load-bearing compatibility default: a ticket that
 * never declared the field behaves exactly like the pre-BRAIN-255 "always mutually exclusive on
 * the same key" rule). Only once the count of held same-key leases reaches the ceiling does an
 * additional same-key lease block; which one of them is returned as `blocker` is arbitrary (only
 * its existence, not its identity, is meaningful to any caller).
 *
 * This is the single predicate behind every "is this ticket blocked" question in the scheduler —
 * selectCandidate, selectCapacityCandidate, and tryStart's own head-conflict check below all call
 * this rather than hand-rolling a per-lease predicate, and status.js's read-only report
 * (BRAIN-249) calls it too, so `lane status` can never describe a different notion of "blocked"
 * than the scheduler actually enforces.
 */
export function blockedBy(held, ticket) {
  const sameKeyHeld = [];
  for (const lease of held) {
    if (lease.key === ticket.key) {
      sameKeyHeld.push(lease);
      continue;
    }
    if (Array.isArray(ticket.conflicts) && ticket.conflicts.includes(lease.key)) return lease;
  }
  const ceiling = Number.isInteger(ticket.maxConcurrent) && ticket.maxConcurrent >= 1 ? ticket.maxConcurrent : 1;
  return sameKeyHeld.length >= ceiling ? sameKeyHeld[sameKeyHeld.length - 1] : null;
}

/**
 * The liveness maintenance every admission decision starts with: reap/orphan
 * stale leases, and dequeue queued tickets whose supervisor already died
 * (crashed, or the machine killed it) so they never sit at the FIFO head
 * forever. `keepTicketId` is the caller's own ticket, never dequeued here.
 * Caller must hold the global lock.
 */
export function reapStale(root, keepTicketId) {
  reapAll(root, bootId());
  recoverRebinding(root);
  for (const t of listQueue(root)) {
    if (t && t.id !== keepTicketId && !isSupervisorAlive({ supervisorPid: t.supervisorPid, supervisorStart: t.supervisorStart })) {
      // Only a real dequeue is recorded: a failed unlink leaves the ticket
      // queued, and logging it would repeat the false event on every poll.
      // The queue record is gone afterwards, so history is the only durable
      // trace of the drop (BRAIN-202). Same row conventions as cancel.js.
      if (dequeueSync(root, t.id)) {
        touchSimArmFor(root, t);
        appendHistory(root, { id: t.id, key: t.key, dequeuedDeadSupervisor: true, error: 'supervisor died while queued', supervisorPid: t.supervisorPid, endedAt: Date.now(), executor: 'local' });
      }
    }
  }
}

/**
 * BRAIN-338: "would `tryStart` admit this not-yet-queued ticket right now?",
 * so the supervisor can decide between queuing on a busy runner and running
 * locally. It takes the same gates `tryStart` does, from the same helpers
 * (`reapStale`, `blockedBy`, `effectiveWeightCapacity`, `cooldownActive`,
 * `evaluateMemoryAdmission`, the persisted load gate), under the same global
 * lock. It performs the same stale-record reaping every `tryStart` poll
 * performs, but never samples CPU or advances the load gate. The CPU
 * projected-over-budget check is deliberately not repeated (sampling mutates
 * shared baselines); an optimistic "yes" only means the ticket runs locally
 * as before.
 *
 * Accepted limitation: with `admissionLoadGate` on, the persisted gate state
 * is read without sampling, so a gate one low-load sample from reopening
 * reads closed and the ticket may queue remotely instead of starting locally.
 * Returns `{ admit, reason }`.
 */
export async function couldAdmitNow(root, cfg, ticket, memoryReader = readMemoryInfo) {
  return withLock(root, () => {
    advanceHwm(root);
    reapStale(root, ticket.id);
    return couldAdmitLocked(root, cfg, ticket, memoryReader);
  });
}

function couldAdmitLocked(root, cfg, ticket, memoryReader) {
  if (listQueue(root).length > 0) return { admit: false, reason: 'queue-ahead' };
  if (fs.existsSync(paths(root).pause)) return { admit: false, reason: 'paused' };
  const held = listLeases(root).filter((l) => HELD_STATES.has(l.state));
  if (blockedBy(held, ticket)) return { admit: false, reason: 'conflict' };
  const runningWeight = held.reduce((sum, l) => sum + (l.weight || 0), 0);
  if (runningWeight + ticket.weight > effectiveWeightCapacity(cfg, detectResourceCapacity().cpuCores)) {
    return { admit: false, reason: 'capacity' };
  }
  if (cfg.admissionLoadGate && held.length > 0 && readGateState(root).closed) return { admit: false, reason: 'load-gate-closed' };
  let memInfo = null;
  try {
    memInfo = memoryReader();
  } catch {
    memInfo = null;
  }
  if (memInfo && memInfo.macPressure === 'critical') return { admit: false, reason: 'memory-critical' };
  if (cfg.schedulerMode === 'active') {
    if (cooldownActive(held, cfg)) return { admit: false, reason: 'cooldown' };
    if (memInfo) {
      const candidateResources = resolveTicketResources({
        weight: ticket.weight,
        cpuCores: ticket.resources?.cpuCores,
        memoryBytes: ticket.resources?.memoryBytes,
        defaultMemoryBytesPerWeight: cfg.defaultMemoryBytesPerWeight,
      });
      const memory = evaluateMemoryAdmission({ memoryInfo: memInfo, heldLeases: held, candidateResources, cfg });
      if (!memory.admit) return { admit: false, reason: memory.reason };
    }
  }
  return { admit: true, reason: 'ok' };
}

/** Coerce a persisted (possibly corrupt) skip-count file into a safe shape. Exported for
 *  status.js's read-only report (BRAIN-249) — it needs the same headId/count/blockedSince view
 *  tryStart uses, without duplicating this parsing. `blockedSince` (BRAIN-249) is when this
 *  headId first became the blocked head; `loggedPhase` is the last head-block phase
 *  (logHeadBlock's event) written for this head, so resolveHeadBlock below only logs on an
 *  actual transition. Both are `null` for a file written before BRAIN-249 (or any other missing/
 *  malformed value) — never defaulted to something that would read as "infinitely old" or
 *  "already logged". */
export function readSkipState(root, store = legacyStore(root), headId = null) {
  const raw = store.read('conflict', headId);
  if (!raw || typeof raw.headId !== 'string' || !Number.isFinite(raw.count) || raw.count < 0) {
    return { headId: null, count: 0, blockedSince: null, loggedPhase: null };
  }
  const blockedSince = Number.isFinite(raw.blockedSince) ? raw.blockedSince : null;
  const loggedPhase = typeof raw.loggedPhase === 'string' ? raw.loggedPhase : null;
  return { headId: raw.headId, count: raw.count, blockedSince, loggedPhase };
}

/** Record that a ticket other than the literal FIFO head is about to start
 *  ahead of it (a "skip event" — see conflictSkipLimit in tryStart). The
 *  count is keyed on the head's own id and resets automatically the next
 *  time a different ticket occupies the head slot (readSkipState above
 *  returns 0 for a headId mismatch), so no explicit reset is needed once
 *  the head itself finally starts or is cancelled. `blockedSince` and
 *  `loggedPhase` (BRAIN-249) follow the exact same reset rule as `count` —
 *  carried forward for the same head, reset for a new one — so a skip event
 *  can never silently erase what resolveHeadBlock below has already
 *  recorded about how long this head has been blocked or what it last
 *  logged.
 *
 *  Returns whether the count was durably persisted. This must fail CLOSED,
 *  not best-effort (Codex review, High): a single dropped write costing one
 *  extra skip beyond conflictSkipLimit would be harmless, but a PERSISTENT
 *  write failure (permissions, full disk) would mean the count can never
 *  reach the limit, so the starvation bound this function exists to enforce
 *  would never trip — the conflicted head could be skipped forever. The
 *  caller must refuse the skip whenever this returns false, falling back to
 *  strict FIFO (the head blocks everyone until its own conflict clears).
 *  Losing backfill on a broken disk is the correct trade against unbounded
 *  starvation. */
function recordSkip(root, store, headId, now = Date.now()) {
  const prev = readSkipState(root, store, headId);
  const sameHead = prev.headId === headId;
  const count = sameHead ? prev.count + 1 : 1;
  const blockedSince = sameHead && prev.blockedSince != null ? prev.blockedSince : now;
  const loggedPhase = sameHead ? prev.loggedPhase : null;
  try {
    store.write('conflict', headId, { headId, count, blockedSince, loggedPhase });
    return true;
  } catch {
    return false;
  }
}

/**
 * BRAIN-249: everything tryStart needs to know about a conflict-blocked
 * head's age, in one place. Three responsibilities, deliberately kept
 * together rather than split across separate read/heal/log functions,
 * because they all read and write the SAME conflict-skip-state.json record
 * and doing them separately would mean each one guessing at what the others
 * already changed this same poll:
 *
 *  1. Resolve `blockedSince` — when this exact headId first became the
 *     blocked head. A same-head record with a `blockedSince` already on
 *     disk keeps it. Anything else (a new head, or a same-head record with
 *     no timestamp at all — a file written before BRAIN-249, or one whose
 *     timestamp a prior failed write dropped) is stamped "now". A missing
 *     timestamp must NEVER read as infinitely old: that would instantly
 *     disable the starvation bound the moment a machine upgrades, treating
 *     a head that has been sitting blocked for months as freshly
 *     grace-lapsed and dumping every queued ticket behind it into backfill
 *     at once. Starting the clock at "now" instead is the safe direction —
 *     worst case the grace period takes one extra headBlockGraceMs to
 *     first lapse after an upgrade, never zero.
 *  2. Derive the phase skipExhausted needs: 'blocked' while the skip
 *     allowance still has room, 'exhausted' once it's used up but still
 *     inside headBlockGraceMs (backfill stays refused), 'resumed' once the
 *     grace period has lapsed (backfill resumes despite the exhausted
 *     count).
 *  3. Log the transition — via logHeadBlock, never per poll — the moment
 *     the phase (or the head itself) changes, and persist that phase as
 *     `loggedPhase` so the next poll can tell whether anything changed.
 *
 * The persisted write only happens when something actually changed
 * (`blockedSince` was just stamped, or the phase differs from what was last
 * logged); an unchanged poll costs one read, no write, matching every other
 * per-poll read in this file. Best-effort like recordSkip's own write in
 * spirit, but NOT fail-closed the way recordSkip's count must be: a missed
 * write here only means the clock re-stamps or the transition re-logs next
 * poll, which is always the safe direction — the grace period never
 * appears to have lapsed sooner than it actually did, and a dropped log
 * line is telemetry, not a scheduling decision.
 */
function resolveHeadBlock(root, store, cfg, headId, blocker, sameHead, skipState, skipCount, now) {
  const blockedSince = sameHead && skipState.blockedSince != null ? skipState.blockedSince : now;
  const blockedMs = now - blockedSince;
  const phase = skipCount < cfg.conflictSkipLimit ? 'blocked' : blockedMs < cfg.headBlockGraceMs ? 'exhausted' : 'resumed';
  const prevPhase = sameHead ? skipState.loggedPhase : null;
  const blockedSinceChanged = !(sameHead && skipState.blockedSince != null);
  if (phase !== prevPhase) {
    logHeadBlock(root, {
      event: phase === 'blocked' ? 'head-blocked' : phase === 'exhausted' ? 'skip-exhausted' : 'backfill-resumed',
      headId,
      blockingLeaseId: blocker.id,
      blockingKey: blocker.key,
      skipCount,
      skipLimit: cfg.conflictSkipLimit,
      graceMs: cfg.headBlockGraceMs,
      blockedMs,
    });
  }
  if (blockedSinceChanged || phase !== prevPhase) {
    try {
      store.write('conflict', headId, { headId, count: skipCount, blockedSince, loggedPhase: phase });
    } catch {
      // best-effort — see doc comment above
    }
  }
  return { blockedMs, phase };
}

/** Coerce a persisted (possibly corrupt) capacity-skip-count file into a safe shape (BRAIN-249
 *  part 2). Exported for status.js's read-only report, same reason as readSkipState above.
 *  Mirrors readSkipState's shape/tolerance, but tracks a SEPARATE counter, keyed the
 *  same way (headId + count + loggedPhase), for the capacity-blocked case. Kept in its own file
 *  (paths().capacitySkipState) rather than folded into conflict-skip-state.json: the two block
 *  reasons are mutually exclusive for a given head at a given poll (a head is either conflicted,
 *  or — if not — capacity-blocked or fine), but the SAME headId can transition between the two
 *  reasons over its lifetime (its conflict clears while capacity is still tight, or vice versa);
 *  sharing one counter would let an exhausted conflict-skip count silently exhaust the capacity
 *  allowance too, or vice versa, for a reason that never actually applied to this head. No
 *  `blockedSince`/grace field — see resolveCapacityBlock's doc comment for why the capacity path
 *  is deliberately NOT time-bounded the way the conflict path is. */
export function readCapacitySkipState(root, store = legacyStore(root), headId = null) {
  const raw = store.read('capacity', headId);
  if (!raw || typeof raw.headId !== 'string' || !Number.isFinite(raw.count) || raw.count < 0) {
    return { headId: null, count: 0, loggedPhase: null };
  }
  const loggedPhase = typeof raw.loggedPhase === 'string' ? raw.loggedPhase : null;
  return { headId: raw.headId, count: raw.count, loggedPhase };
}

/** Capacity-block counterpart of recordSkip — same fail-CLOSED discipline (Codex review
 *  precedent, see recordSkip's own doc comment above): a persistent write failure must refuse
 *  the skip rather than let it happen uncounted, or a capacity-blocked head could be backfilled
 *  past forever on a broken disk. */
function recordCapacitySkip(root, store, headId) {
  const prev = readCapacitySkipState(root, store, headId);
  const sameHead = prev.headId === headId;
  const count = sameHead ? prev.count + 1 : 1;
  const loggedPhase = sameHead ? prev.loggedPhase : null;
  try {
    store.write('capacity', headId, { headId, count, loggedPhase });
    return true;
  } catch {
    return false;
  }
}

/**
 * BRAIN-249 part 2: derive whether a capacity-blocked head's backfill allowance is exhausted,
 * and log the transition (via logCapacityBlock, never per poll) exactly like resolveHeadBlock
 * does for the conflict case — with one deliberate asymmetry: NO headBlockGraceMs equivalent.
 * A conflict may never clear on its own (a lease can legitimately run for hours with nothing
 * forcing it to finish), so refusing conflict backfill forever would starve every ticket behind
 * it — hence the time-boxed grace period. A capacity block is different in kind: it is refused
 * BECAUSE backfill would consume the very capacity the head needs, so the refusal is
 * self-terminating — running work drains, the machine empties, and the head eventually fits and
 * starts on its own. Adding a time-based "resume backfilling anyway" here would let backfill
 * consume that same capacity indefinitely and could starve the head PERMANENTLY instead of
 * temporarily. Do not add a grace period to this path.
 */
function resolveCapacityBlock(root, store, cfg, headId, headWeight, runningWeight, capacity, skipCount) {
  const phase = skipCount < cfg.conflictSkipLimit ? 'blocked' : 'exhausted';
  const state = readCapacitySkipState(root, store, headId);
  const sameHead = state.headId === headId;
  const prevPhase = sameHead ? state.loggedPhase : null;
  if (phase !== prevPhase) {
    logCapacityBlock(root, {
      event: phase === 'blocked' ? 'head-capacity-blocked' : 'capacity-backfill-refused',
      headId,
      headWeight,
      runningWeight,
      capacity,
      skipCount,
      skipLimit: cfg.conflictSkipLimit,
    });
    try {
      store.write('capacity', headId, { headId, count: skipCount, loggedPhase: phase });
    } catch {
      // best-effort — see recordCapacitySkip's doc comment for why the count itself must fail
      // closed; only this logging/dedup write is allowed to be lossy.
    }
  }
  return { phase };
}

/**
 * BRAIN-346: the projected-over-budget head's record (resource-skip-state.json):
 * `{ headId, count, reserved, inScope, deniedAt, budget, externalBusy }`. `count`/`reserved` are the
 * head's allowance and survive ANY denial kind for the same head; they are dropped only when the
 * head starts, leaves queue[0], or resourceSkipLimit is 0. `inScope` is whether the head's LATEST
 * denial was projected-over-budget (memory ok): an out-of-scope denial pauses backfill without
 * resetting the allowance. Written by the head's own denials (recordResourceDenial,
 * markResourceOutOfScope) and by a backfill's count (recordResourceBackfill). Anything missing
 * or malformed reads as null, which means "no backfill"; a missing `inScope` reads as false.
 */
export function readResourceSkipState(root, store = legacyStore(root), headId = null) {
  const raw = store.read('resource', headId);
  if (
    !raw ||
    typeof raw.headId !== 'string' ||
    !Number.isInteger(raw.count) ||
    raw.count < 0 ||
    typeof raw.reserved !== 'boolean' ||
    !Number.isFinite(raw.budget) ||
    !Number.isFinite(raw.externalBusy) ||
    !Number.isFinite(raw.deniedAt)
  ) {
    return null;
  }
  return {
    headId: raw.headId,
    count: raw.count,
    reserved: raw.reserved,
    inScope: raw.inScope === true,
    behindConflict: raw.behindConflict === true,
    deniedAt: raw.deniedAt,
    budget: raw.budget,
    externalBusy: raw.externalBusy,
    ...(Number.isInteger(raw.reservationSeq) ? { reservationSeq: raw.reservationSeq } : {}),
  };
}

/** The record for exactly this head, or null: resource backfill is active-mode only, off at
 *  resourceSkipLimit 0, and a record left by any other head is never consulted. */
function resourceRecordFor(root, store, cfg, headId, behindConflict = false) {
  if (cfg.schedulerMode !== 'active' || !(cfg.resourceSkipLimit > 0)) return null;
  const record = readResourceSkipState(root, store, headId);
  return record && record.headId === headId && record.behindConflict === behindConflict ? record : null;
}

const resourceBackfillOpen = (record, cfg) => record !== null && record.inScope && !record.reserved && record.count < cfg.resourceSkipLimit;
const resourceReserved = (record) => record !== null && record.reserved;

/** The head's own in-scope denial (projected-over-budget, memory ok): create the record for a new
 *  head, or refresh the budget/externalBusy snapshot while carrying count/reserved forward for
 *  the same head. Best-effort: a failed write leaves the old record (or none), never an allowance.
 *  `behindConflict` (BRAIN-365) marks the snapshot as a conflict-backfill candidate's denial, not
 *  the head's: only a conflicted head reads it, and the head's own next denial overwrites it. */
function recordResourceDenial(root, store, cfg, headId, cpuDecision, now, write, behindConflict = false) {
  if (!(cfg.resourceSkipLimit > 0)) return;
  const prev = readResourceSkipState(root, store, headId);
  const same = prev !== null && prev.headId === headId;
  try {
    store.write('resource', headId, {
      headId,
      count: same ? prev.count : 0,
      reserved: same ? prev.reserved : false,
      ...(same && prev.reservationSeq !== undefined ? { reservationSeq: prev.reservationSeq } : {}),
      inScope: true,
      ...(behindConflict ? { behindConflict } : {}),
      deniedAt: now,
      budget: cpuDecision.budget,
      externalBusy: cpuDecision.externalBusy,
    }, write);
  } catch {
    // best-effort — see doc comment
  }
}

/** The head's latest denial is NOT projected-over-budget: pause backfill but keep the allowance
 *  (count/reserved). Never creates a record — there is no allowance to keep for a fresh head. */
function markResourceOutOfScope(root, store, headId, now, write) {
  const prev = readResourceSkipState(root, store, headId);
  if (prev === null || prev.headId !== headId || !prev.inScope) return;
  try {
    store.write('resource', headId, { ...prev, inScope: false, deniedAt: now }, write);
  } catch {
    // best-effort: a stale inScope only lets a candidate reach its own unchanged fresh admission
  }
}

/** Count a backfill past the head and latch the reservation when the allowance is used up.
 *  Fail CLOSED like recordSkip: the caller refuses the backfill when this returns null. */
function recordResourceBackfill(root, store, cfg, record, write) {
  const count = record.count + 1;
  const reserved = count >= cfg.resourceSkipLimit;
  try {
    // Behind the fence a reservation is ordered by `reservationSeq`, drawn from the monotonic queue counter
    // when it is earned (never a timestamp, which can tie or step back). Legacy never draws one.
    const earnedSeq = store.v2 && reserved && !record.reserved ? { reservationSeq: nextSeq(root) } : {};
    store.write('resource', record.headId, { ...record, count, reserved, ...earnedSeq }, write);
    return { count, reserved };
  } catch {
    return null;
  }
}

/** The smallest CPU claim a ticket can be admitted at: its estimate, or for an elastic ticket its floor. */
function ticketCpuFloor(ticket, cfg) {
  const estimate = ticketCpuEstimate(ticket, cfg);
  const min = ticket.resources?.minCpuCores;
  return Number.isFinite(min) ? Math.min(estimate, Math.ceil(min)) : estimate;
}

/**
 * BRAIN-346: pick the backfill ticket behind a head denied projected-over-budget. Walks the queue
 * like selectCapacityCandidate (a null record stops the walk; conflicting or weight-overflowing
 * tickets are skipped) and additionally skips any ticket whose start would make the head
 * conflicted. Among tickets whose CPU claim fits the headroom the head's record was denied
 * against (the same projectBusy maths admission uses, over the CURRENT held leases), it returns
 * the smallest claim, earliest in the queue on ties. Behind the scheduler fence `queue` is the ordered
 * effective view (BRAIN-380), so "earliest" is ordered-view position and `queue[0]` is the effective head
 * (the reservation owner when one is promoted); the strict `<` below is what makes that the tie-break.
 *
 * Admission-failure recovery rule: selection is stateless. A selected ticket that fresh admission
 * then denies is simply selected again next poll; because the smallest claim always goes first, a
 * mispredicted ticket can only ever hold back tickets with an equal or larger claim, never a
 * smaller one. The head is never a candidate here (index 0).
 *
 * BRAIN-365: `headReserved` is the same walk for a CONFLICT-blocked head (see tryStart): the head
 * cannot start now, so its own claim and weight are counted as held (the BRAIN-355 reservation),
 * a ticket is skipped when its key and the head's conflict in either direction, and the record
 * is the snapshot a conflict-backfill candidate was denied against.
 *
 * An elastic ticket (BRAIN-360) is judged, and ranked, at its floor: ceil(minCpuCores).
 */
export function selectResourceCandidate(queue, held, runningWeight, weightCapacity, record, cfg, now = Date.now(), headReserved = false) {
  const headTicket = queue[0];
  const view = headReserved ? [...held, { id: headTicket.id, key: headTicket.key, weight: headTicket.weight, resources: headTicket.resources }] : held;
  const viewWeight = headReserved ? runningWeight + headTicket.weight : runningWeight;
  let best = null;
  let bestClaim = Infinity;
  for (const t of queue.slice(1)) {
    if (!t) return best;
    if (blockedBy(held, t)) continue;
    if (viewWeight + t.weight > weightCapacity) continue;
    if (headReserved ? blockedBy([{ key: t.key }], headTicket) || blockedBy([{ key: headTicket.key }], t) : blockedBy([...held, { key: t.key }], headTicket)) continue;
    const claim = ticketCpuFloor(t, cfg);
    if (projectBusy(record.externalBusy, view, claim, now, cfg) > record.budget) continue;
    if (claim < bestClaim) {
      best = t;
      bestClaim = claim;
    }
  }
  return best;
}

/**
 * Walk the FIFO queue in order and return the first ticket with no
 * conflicting held lease, or null if every queued ticket up to and
 * including the first unreadable record conflicts with something currently
 * held (or there is such a record at all).
 *
 * A ticket is skipped ONLY for a conflict, never for anything else: a
 * conflict has a natural starvation bound in the common case (it clears
 * when the conflicting job ends) — see tryStart's own conflictSkipLimit
 * enforcement for the case (Codex review finding #6) where three or more
 * conflicting keys defeat that bound by alternating which one is held.
 *
 * The load/CPU gates do NOT skip this way — a gate-blocked head has no such
 * bound and needs an aging/round-robin scheme (a separate ticket), so this
 * function never even looks past a head blocked only by those. Capacity
 * DOES now have its own analogous bound — see selectCapacityCandidate below
 * — but that is a separate walk with a separate exhaustion counter, invoked
 * only when the head does not conflict at all; this function still only
 * ever looks past a CONFLICT.
 *
 * A corrupt/unreadable queue record (listQueue represents these as `null`)
 * STOPS the walk rather than being skipped over (Codex review finding #7):
 * we cannot know what an unreadable ticket would conflict with, so treating
 * it as transparent could let something behind it start when it shouldn't.
 */
export function selectCandidate(queue, held) {
  for (const t of queue) {
    if (!t) return null;
    if (!blockedBy(held, t)) return t;
  }
  return null;
}

/**
 * Like selectCandidate, but for the head-fits-capacity case (BRAIN-249 part 2): used only when
 * the head itself does not conflict with anything held but simply does not fit under capacity
 * (e.g. a weight-8 lane against an 8-wide machine with anything else running). Walks the queue
 * for the first ticket that neither conflicts with anything held NOR would itself still exceed
 * capacity if admitted — a conflicting ticket is skipped over exactly like selectCandidate does,
 * and a ticket that has no conflict but also doesn't fit capacity is skipped over too (a lighter
 * ticket further back may still fit), rather than stopping the walk. A corrupt/unreadable record
 * (Codex review finding #7, same rationale as selectCandidate) still stops the walk outright.
 */
export function selectCapacityCandidate(queue, held, runningWeight, capacity) {
  for (const t of queue) {
    if (!t) return null;
    if (blockedBy(held, t)) continue;
    if (runningWeight + t.weight <= capacity) return t;
  }
  return null;
}

/**
 * BRAIN-379 shadow: BRAIN-355's safe-backfill verdict for every queued ticket in one pass (an unreadable record
 * stops every ticket behind it, as it stops each live selector's walk), for the shadow snapshot.
 */
function safeBackfillFlags(queue, headTicket, enabled, cannotDelayHead) {
  let readableSoFar = true;
  return queue.map((t, i) => {
    if (t === null) readableSoFar = false;
    return enabled && i > 0 && t !== null && readableSoFar && t.id !== headTicket.id && cannotDelayHead(t);
  });
}

/**
 * The single atomic transaction: a ticket starts only when it is selected
 * AND fits capacity AND the load gate is open AND the broker is not paused.
 * Selection is FIFO with three bounded skips past a head that cannot start:
 * conflict (selectCandidate), capacity (selectCapacityCandidate) and, for a
 * head denied only by projected-over-budget CPU, resource (selectResourceCandidate,
 * then a reservation). A gate-blocked head is never skipped. See the README's
 * "Fairness and backfill" section.
 *
 * Resource sampling and the admission decision share the global lock. CPU
 * sampling reads and updates cpu-sample.json, so serializing it prevents two
 * supervisors from diffing the same baseline and admitting concurrently on
 * inconsistent observations. The decision log remains outside the lock.
 *
 * The config itself is part of that locked, consistent view: `globalCfg`
 * is only the outer snapshot used for `sampleMs` between polls. Everything
 * decided inside the lock re-reads via `reloadCfg` (default: reuse
 * `globalCfg`) FIRST THING inside the callback, so a config edit that lands
 * between this function's outer read and the lock being granted can never
 * become a stale, already-superseded transition of the load/CPU gate — only
 * a transition the config in effect at decision time would actually produce.
 */
export async function tryStart(root, ticket, globalCfg, loadSampler, cpuSampler, reloadCfg = () => globalCfg, memoryReader = readMemoryInfo, writeResourceState = atomicWriteJson, shadowSeams = {}) {
  // `shadow` is what the live evaluation below hands the BRAIN-379 shadow recorder; it is only ever
  // filled with copies and flags, never read back by the live decision.
  const shadow = {};
  const decide = () => {
    const cfg = reloadCfg() || globalCfg;
    const now = Date.now();
    const nowEff = advanceHwm(root, now);
    reapStale(root, ticket.id);
    // BRAIN-380: behind a valid scheduler fence this evaluation is priority-ordered, over per-ticket fairness
    // records; otherwise it is the legacy FIFO over the singleton files, untouched. `queue` is the ONE array
    // every selector, the safe-backfill predicate and the shadow snapshot below receive.
    assertQueueLayout(root);
    const sched = resolveScheduler(root);
    const store = sched.v2 ? fairnessStore(root, sched.tickets) : legacyStore(root);
    if (sched.v2) quarantineLegacyRecords(root);
    const rawQueue = sched.v2 ? fenceLegacy(listQueue(root)) : listQueue(root);
    // A reservation exists only while resource backfill does: when it is off, release every latch, so turning it back
    // on makes a ticket earn its reservation again.
    if (sched.v2 && !(cfg.schedulerMode === 'active' && cfg.resourceSkipLimit > 0)) store.releaseReservations();
    store.prune(rawQueue, listRebindingIds(root));
    const { queue, ownerId: reservationOwnerId } = sched.v2 ? effectiveView(rawQueue, nowEff, cfg, store) : { queue: rawQueue, ownerId: null };
    const position = queue.findIndex((t) => t && t.id === ticket.id);
    if (position === -1) {
      return { result: { started: false, reason: 'not-head', position: null, queueLength: queue.length } };
    }
    // BRAIN-320 S1d: opt-in queue timeout for a remote ticket -- decided here,
    // right after `position` is resolved and BEFORE any selection/gate logic,
    // so a ticket blocked behind someone else's conflict or capacity still
    // expires on schedule instead of only ever being checked once it would
    // otherwise have been selected. `ticket.startDeadline` is only ever set
    // by run.js for a runner-side pipeline ticket dispatched with a
    // `queueTimeoutMs` header field (I6: absent for every other ticket, so
    // this branch is a no-op for them). A user cancel takes precedence: if
    // the marker is already there, this ticket is on its way to being
    // finalized as cancelled (the outer poll loop's own cancelRequested
    // check, or this same tryStart's cancel re-check further below once
    // selected) -- never overwrite that outcome with an expiry.
    if (Number.isFinite(ticket.startDeadline) && now >= ticket.startDeadline && !isCancelled(root, ticket.id)) {
      writeExpireMarkerFile(root, ticket.id);
      return { result: { started: false, reason: 'queue-timeout' } };
    }
    // Selection depends only on the queue and who currently holds a key —
    // resolve it BEFORE touching the load/CPU gates (both of which persist a
    // sample and advance their hysteresis as a side effect) or the pause
    // flag, so a ticket that isn't going anywhere this poll costs exactly
    // what it did before this ticket existed: one cheap read, no side
    // effects. Only the selected candidate pays for a real gate evaluation.
    const held = listLeases(root).filter((l) => HELD_STATES.has(l.state));
    const lastSimDemandAt = reconcileSimArm(root, queue, held, now, cfg.simArmWindowMs);
    const headTicket = queue[0];
    if (!headTicket) {
      // The literal head's own queue record is corrupt/unreadable (Codex
      // review finding #7). We cannot know what it would conflict with, so
      // nothing behind it may run either — `ticket` can never BE the null
      // head (a null record has no id to have matched `position` above).
      return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
    }
    // Computed early (moved up from the real capacity check further below)
    // because BRAIN-249 part 2 needs to know whether the HEAD itself fits
    // before selection can even be decided, not just whether the eventual
    // candidate fits.
    const runningWeight = held.reduce((sum, l) => sum + (l.weight || 0), 0);
    const cpuCores = detectResourceCapacity().cpuCores;
    const weightCapacity = effectiveWeightCapacity(cfg, cpuCores);
    const blocker = blockedBy(held, headTicket);
    const headConflicted = blocker !== null;
    // Starvation bound (Codex review finding #6): a conflict-blocked head
    // does NOT have a natural bound in general — with three or more
    // conflicting keys, alternating which one is held can keep skipping a
    // different, always-non-conflicting later ticket ahead of the head
    // forever. Once the head has been skipped conflictSkipLimit times in a
    // row, stop looking past it: it blocks like the pre-conflict-skip
    // behavior until its own conflict actually clears.
    //
    // BRAIN-249: that bound has no bound of its own — a head stuck behind a
    // genuinely long-running lease (no max-runtime exists, and one was
    // deliberately rejected — see config.js's headBlockGraceMs doc comment)
    // would be refused backfill forever, turning one blocked ticket into
    // every ticket behind it also blocked. resolveHeadBlock derives a phase
    // that additionally requires the head to have been blocked for LESS
    // than headBlockGraceMs for the exhaustion to still apply; past that,
    // backfill resumes despite the exhausted count.
    const skipState = readSkipState(root, store, headTicket.id);
    const sameHead = skipState.headId === headTicket.id;
    const skipCount = sameHead ? skipState.count : 0;
    const headBlock = headConflicted ? resolveHeadBlock(root, store, cfg, headTicket.id, blocker, sameHead, skipState, skipCount, now) : null;
    const skipExhausted = headConflicted && headBlock.phase === 'exhausted';

    // BRAIN-249 part 2: a head that does NOT conflict with anything held can
    // still be blocked — it simply doesn't FIT under capacity/held weight
    // (e.g. a weight-8 lane against an 8-wide machine with anything else
    // running: `savoro:prepush` against `capacity: 8`, the live incident
    // this covers). selectCandidate only ever looks past a CONFLICT, so a
    // head like this was previously "selected" every poll (headConflicted
    // is false) and denied at the real capacity check further below,
    // forever — nothing behind it, however light and non-conflicting, ever
    // got a turn; admission-decisions.log showed the same candidate denied
    // `capacity` on every single poll. Apply the same skip-and-exhaust bound
    // conflicts get, reusing conflictSkipLimit as the threshold but tracked
    // in its own capacitySkipState (see readCapacitySkipState's doc
    // comment) and, deliberately, with NO headBlockGraceMs equivalent (see
    // resolveCapacityBlock's doc comment for why that would be actively
    // wrong here).
    const headCapacityBlocked = !headConflicted && runningWeight + headTicket.weight > weightCapacity;
    const capacitySkipState = headCapacityBlocked ? readCapacitySkipState(root, store, headTicket.id) : null;
    const capacitySameHead = headCapacityBlocked && capacitySkipState.headId === headTicket.id;
    const capacitySkipCount = capacitySameHead ? capacitySkipState.count : 0;
    const capacityBlock = headCapacityBlocked
      ? resolveCapacityBlock(root, store, cfg, headTicket.id, headTicket.weight, runningWeight, weightCapacity, capacitySkipCount)
      : null;
    const capacitySkipExhausted = headCapacityBlocked && capacityBlock.phase === 'exhausted';

    // BRAIN-346: a head whose last in-scope denial was projected-over-budget (see its record) may
    // be backfilled past, bounded by resourceSkipLimit, then reserved. A reservation also stops
    // capacity backfill past that same head (the one cross-kind check); conflict backfill is
    // never affected — a conflict-blocked head never reaches either resource path.
    const headRecord = headConflicted ? null : resourceRecordFor(root, store, cfg, headTicket.id);
    // A reservation that is not the ACTIVE one is dormant: kept with its counters, reserving nothing (R4-3).
    const resourceRecord = sched.v2 && headRecord?.reserved && headTicket.id !== reservationOwnerId ? { ...headRecord, reserved: false } : headRecord;
    const capacityReserved = headCapacityBlocked && resourceReserved(resourceRecord);
    const resourceBackfill = !headConflicted && !headCapacityBlocked && resourceBackfillOpen(resourceRecord, cfg);
    // BRAIN-355: an exhausted conflict-blocked head still admits a ticket that provably cannot
    // delay it. The ticket polling decides for itself (admission below reserves the head's
    // resources); it is never counted as a skip.
    const cannotDelayHead = (t) =>
      !blockedBy(held, t) &&
      !blockedBy([{ key: t.key }], headTicket) &&
      !blockedBy([{ key: headTicket.key }], t) &&
      runningWeight + headTicket.weight + t.weight <= weightCapacity;
    const safeBackfillEnabled = skipExhausted && cfg.conflictSafeBackfill;
    // ROG-2181: behind a non-sim head a sim-class ticket may only start through safe backfill, on EVERY path
    // (not only once a conflict-blocked head's skips are exhausted): the proof below is what guarantees a sim never
    // delays a waiting test. Every skip walk (conflict, capacity, resource) is blind to a sim, so a long sim never
    // overtakes a test head by a skip that merely spends a bounded allowance. `conflictSafeBackfill: false` turns
    // safe backfill off, and with it every sim pass.
    const simHoldsBack = classOf(headTicket) !== 'sim';
    const simEligibleForSafeBackfill = (t) => simHoldsBack && cfg.conflictSafeBackfill && classOf(t) === 'sim';
    const safeBackfill =
      (safeBackfillEnabled || simEligibleForSafeBackfill(ticket)) &&
      ticket.id !== headTicket.id &&
      // an unreadable (null) ticket ahead of this one stops backfill, as it stops every selector's walk
      queue.slice(0, position).every(Boolean) &&
      cannotDelayHead(ticket);
    const walkQueue = simHoldsBack ? queue.filter((t, i) => i === 0 || t === null || classOf(t) !== 'sim') : queue;
    // BRAIN-365: the first non-conflicting ticket behind a conflicted head may be one that admission
    // keeps denying projected-over-budget, which would pin every smaller ticket behind it. That
    // denial leaves a behind-conflict record (below); while it stands, the walk prefers the smallest
    // ticket that fits, with the head's claim reserved, and falls back to the first non-conflicting
    // ticket (so the denied one is still re-evaluated, refreshing the record).
    const conflictRecord = headConflicted && !skipExhausted ? resourceRecordFor(root, store, cfg, headTicket.id, true) : null;
    const conflictPick = conflictRecord ? selectResourceCandidate(walkQueue, held, runningWeight, weightCapacity, conflictRecord, cfg, now, true) : null;
    // A pick that is denied for anything but the CPU projection invalidates the record it was picked
    // from (the record has no allowance worth keeping, unlike the head's own), so the next poll
    // falls back to re-evaluating the first non-conflicting ticket instead of re-picking it.
    const dropPickRecord = () => {
      if (conflictPick && ticket.id === conflictPick.id) store.dropResourceRecord(headTicket.id);
    };
    const candidate = safeBackfill
      ? ticket
      : headConflicted
      ? (skipExhausted ? (safeBackfill ? ticket : null) : conflictPick ?? selectCandidate(walkQueue, held))
      : headCapacityBlocked
        ? (capacitySkipExhausted || capacityReserved ? null : selectCapacityCandidate(walkQueue, held, runningWeight, weightCapacity))
        : resourceBackfill && ticket.id !== headTicket.id
          ? selectResourceCandidate(walkQueue, held, runningWeight, weightCapacity, resourceRecord, cfg, now)
          : headTicket;
    // BRAIN-379 shadow: a cheap snapshot, taken under the lock before anything below can write, of exactly what
    // the live selection saw. Evaluation and logging happen after the lock is released (see the end of tryStart).
    if (cfg.allocationShadow === true) {
      try {
        shadow.inputs = (shadowSeams.capture ?? captureShadowInputs)({
          queue,
          held,
          now,
          cfg,
          cpuCores,
          lastSimDemandAt,
          // a class-only skip spends the same bounded budget as a resource skip
          skipBudget: headConflicted
            ? { kind: 'conflict', limit: cfg.conflictSkipLimit, used: skipCount }
            : headCapacityBlocked
              ? { kind: 'capacity', limit: cfg.conflictSkipLimit, used: capacitySkipCount }
              : { kind: resourceRecord ? 'resource' : 'none', limit: cfg.resourceSkipLimit, used: resourceRecord ? resourceRecord.count : 0 },
          reservation: { reserved: capacityReserved || resourceReserved(resourceRecord) },
          conflicted: queue.map((t) => (t ? blockedBy(held, t) !== null : false)),
          safeBackfill: safeBackfillFlags(queue, headTicket, safeBackfillEnabled, cannotDelayHead),
          conflictPickId: conflictPick?.id ?? null,
          runningWeight,
          weightCapacity,
          paused: fs.existsSync(paths(root).pause),
        });
      } catch (err) {
        shadow.captureError = err;
      }
    }
    if (!candidate) {
      // Either nobody in the queue is eligible (selectCandidate/
      // selectCapacityCandidate found nothing), or the head's skip
      // allowance for whichever reason applies here (conflict or capacity)
      // is exhausted and we deliberately stop looking past it. Either way
      // the head is the one genuinely blocked — EXCEPT for the
      // headCapacityBlocked + "ticket is the head itself" combination,
      // handled by the deliberate no-op comment below.
      if (ticket.id !== headTicket.id) {
        return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
      }
      if (headConflicted) {
        return { result: { started: false, reason: 'conflict', with: blocker.id, key: blocker.key } };
      }
      // headCapacityBlocked, and this poll's own ticket IS the head: fall
      // through to the real capacity check further below instead of
      // returning a synthetic result here. Before BRAIN-249 part 2 existed,
      // `candidate` was unconditionally `headTicket` whenever `!headConflicted`
      // — this exact case always fell through to that real check, which sets
      // `logFields` for logAdmissionDecision. Short-circuiting here instead
      // would silently drop that admission-decision log line for a head that
      // has nothing left behind it to backfill (tests/admission.test.js's
      // "the admission decision log is written on a deny path too
      // (capacity)" pins this). The eventual denial reason is 'capacity'
      // either way — only whether it is logged differs.
    } else if (candidate.id !== ticket.id) {
      // A non-conflicting/fitting ticket earlier in the queue gets to go
      // first (or already has). This ticket just isn't up yet — same as the
      // old strict-head "not-head", just computed against the conflict/
      // capacity-skip selection instead of raw queue position.
      return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
    }
    if (fs.existsSync(paths(root).pause)) {
      const reason = fs.readFileSync(paths(root).pause, 'utf8').trim();
      return { result: { started: false, reason: 'paused', pauseReason: reason } };
    }
    const cpuSample = sampleCpuSafe(root, cpuSampler, cfg.sampleMs / 2);
    // The gate must still be SAMPLED unconditionally (its hysteresis
    // countdown depends on every poll observing a sample, closed broker or
    // not), but a gate closed by load the broker itself never generated
    // must not starve a broker that is holding nothing. `held` only counts
    // RUNNING/ORPHANED leases, so this is exactly "nothing is running or
    // might still be running" — not "nothing conflicts with this ticket"
    // and not a zero-weight reading, which a corrupt/legacy lease missing
    // `weight` could satisfy while genuinely busy. The exemption is
    // self-limiting: the instant this ticket's lease is written below,
    // held.length > 0 and the gate resumes blocking everything behind it,
    // so at most one lane ever starts on a closed gate (BRAIN-197).
    const gate = sampleAndUpdateGate(root, cfg, loadSampler);
    const brokerIdle = held.length === 0;
    // Phase 1 (ZIRK scheduler project, shadow-mode admission — see
    // src/admission.js): computed on every poll that reaches this point,
    // regardless of schedulerMode, so shadow and active log identically and
    // only differ in whether the result below is allowed to gate a start.
    // Only the CPU gate's own read-then-write of its shared counter happens
    // here, inside the lock; the sample itself was already taken above.
    let memInfo = null;
    try {
      memInfo = memoryReader();
    } catch {
      memInfo = null;
    }
    // BRAIN-355: a safe backfill must leave room for the blocked head once its conflict clears, so
    // the head's claim joins the held leases for the CPU projection and memory admission.
    const headReservation = { id: headTicket.id, key: headTicket.key, weight: headTicket.weight, resources: headTicket.resources };
    const admissionHeld = safeBackfill || conflictPick ? [...held, headReservation] : held;
    const fullDecision = evaluateNewAdmission(root, cfg, ticket, admissionHeld, cpuSample, memInfo);
    // BRAIN-360: an elastic ticket (resources.minCpuCores) denied ONLY by the CPU projection is
    // re-evaluated at smaller claims, over the same held leases (and, for a safe backfill, the same
    // head reservation), so the grant can never delay what the full claim could not. Active mode
    // only: shadow never denies, so there is nothing to relax.
    const elastic = cfg.schedulerMode === 'active' ? evaluateElasticAdmission(cfg, ticket, admissionHeld, cpuSample, memInfo, fullDecision) : null;
    const cpuDecision = elastic ? { ...elastic.decision, declaredCpuCores: fullDecision.candidateCpuCores } : fullDecision;
    if (shadow.inputs) {
      // what the LIVE evaluation that decided the outcome produced (after any elastic retry: same clock reads,
      // same charges), copied out, with the memory observation and its time, for the post-lock evaluator
      shadow.live = {
        cores: Number.isFinite(cpuSample?.cores) ? cpuSample.cores : null,
        budget: cpuDecision.budget,
        externalBusy: cpuDecision.externalBusy,
        leaseCharges: (cpuDecision.leaseCharges ?? []).map(({ id, demand }) => [id, demand]),
        cpuReason: cpuDecision.cpuReason,
        memInfo: memInfo ? { ...memInfo } : null,
        memAt: Date.now(),
        loadGateBlocking: cfg.admissionLoadGate && gate.closed && !brokerIdle,
        idleExempt: false,
      };
    }
    const grantedCpuCores = elastic ? elastic.grantedCpuCores : fullDecision.candidateCpuCores ?? resolveTicketResources({ weight: ticket.weight, cpuCores: ticket.resources?.cpuCores }).cpuCores;
    // BRAIN-207: when admissionLoadGate is false the gate is sampled and
    // logged exactly as before (its hysteresis countdown must not stall for
    // want of observation), but it is never allowed to deny — for an idle
    // broker OR one holding non-conflicting leases. `loadGateIgnored`
    // records the decisions where that actually mattered (a closed gate the
    // flag suppressed), so shadow telemetry can tell "gate never got the
    // chance to matter" from "gate genuinely never closed".
    const loadGateIgnored = !cfg.admissionLoadGate && gate.closed;
    const logBase = {
      candidateId: ticket.id,
      mode: cfg.schedulerMode,
      // BRAIN-380 §8: who the head was and how it ranked. Decision lines only; a not-head poll writes none.
      headTier: priorityOf(headTicket),
      headRank: effectiveRank(headTicket, nowEff, cfg),
      headScore: score(headTicket, nowEff, cfg),
      loadGateIgnored,
      ...cpuDecision,
      // Provenance for the memory fields above: cpuDecision carries the byte
      // arithmetic but not where the available-memory figure came from or
      // what the OS reported about pressure, which is exactly what made the
      // os.freemem() denial (BRAIN-252) undiagnosable from the log. `memInfo`
      // is null when the reader throws, and readMemoryInfo reports a null
      // macPressure off macOS or on a failed probe — formatAdmissionLog
      // renders either as 'n/a'.
      memorySource: memInfo?.source,
      macPressure: memInfo?.macPressure,
    };
    // What the CURRENT rule would decide, for telemetry (BRAIN-198's shadow
    // ledger needs to tell an ordinary admission from one only the idle
    // exemption allowed) — applies to every branch below that ends in a
    // start, not just the final one, since active-mode CPU denial can still
    // veto an idle-exempt admission. 'idle-exempt' only applies when the
    // gate itself is live (admissionLoadGate: true); with the gate
    // informational-only, every start is an ordinary 'ok', idle or not.
    const baselineReason = cfg.admissionLoadGate && gate.closed && brokerIdle ? 'idle-exempt' : 'ok';

    if (cfg.admissionLoadGate && gate.closed && !brokerIdle) {
      if (ticket.id === headTicket.id) markResourceOutOfScope(root, store, headTicket.id, now, writeResourceState);
      dropPickRecord();
      return {
        result: { started: false, reason: 'load-gate-closed', load: gate.lastLoad },
        logFields: { ...logBase, currentDecision: 'deny', currentReason: 'load-gate-closed' },
      };
    }
    if (runningWeight + ticket.weight > weightCapacity) {
      if (ticket.id === headTicket.id) markResourceOutOfScope(root, store, headTicket.id, now, writeResourceState);
      dropPickRecord();
      return {
        result: { started: false, reason: 'capacity', runningWeight, capacity: weightCapacity },
        logFields: { ...logBase, currentDecision: 'deny', currentReason: 'capacity' },
      };
    }
    // Memory brake (BRAIN-207): a best-effort, never-throwing reader; only
    // an explicit 'critical' reading denies (applies to an idle broker too
    // — this is a hard machine-health brake, not something the idle
    // exemption should ever bypass). Any other value, a null reading, or a
    // throwing reader all admit — same fail-open tolerance as the CPU/load
    // samplers elsewhere in this function.
    if (memInfo && memInfo.macPressure === 'critical') {
      if (ticket.id === headTicket.id) markResourceOutOfScope(root, store, headTicket.id, now, writeResourceState);
      dropPickRecord();
      return {
        result: { started: false, reason: 'memory-critical', macPressure: memInfo.macPressure },
        logFields: { ...logBase, currentDecision: 'deny', currentReason: 'memory-critical' },
      };
    }
    // Phase 1's predicate is an ADDITIONAL constraint, never a replacement
    // for the conflict/capacity checks above, and it only ever gates a start
    // in 'active' mode — 'shadow' always falls through to the exact
    // admission the current rule would have made.
    const headPolling = ticket.id === headTicket.id;
    const resourceDenied =
      cfg.schedulerMode === 'active' && !cpuDecision.admit && cpuDecision.cpuReason === 'projected-over-budget' && cpuDecision.memoryReason === 'ok';
    // BRAIN-346 idle exemption: the head on a fully idle broker (orphaned leases count as held)
    // whose projection overshoots the budget by at most resourceIdleOvershootCores starts anyway
    // — ambient load alone can make a big lane unsatisfiable with nothing running, with or
    // without anyone queued behind it. Same lock transaction as the lease write below, so two
    // supervisors never both exempt. Only a pure CPU-projection denial qualifies: a joint memory
    // denial never does.
    const idleExempt =
      resourceDenied &&
      headPolling &&
      held.length === 0 &&
      cfg.resourceIdleOvershootCores > 0 &&
      cpuDecision.projectedBusy - cpuDecision.budget <= cfg.resourceIdleOvershootCores;
    if (shadow.live) shadow.live.idleExempt = idleExempt;
    if (cfg.schedulerMode === 'active' && !cpuDecision.admit && !idleExempt) {
      if (headPolling) {
        // Cooldown is the head's own backfill echoing back (each admission starts one), so it
        // changes nothing. Any other out-of-scope denial pauses backfill but keeps the allowance.
        if (resourceDenied) recordResourceDenial(root, store, cfg, headTicket.id, cpuDecision, now, writeResourceState);
        else if (cpuDecision.cpuReason !== 'cooldown') markResourceOutOfScope(root, store, headTicket.id, now, writeResourceState);
      } else if (headConflicted && !skipExhausted && resourceDenied && !safeBackfill) {
        recordResourceDenial(root, store, cfg, headTicket.id, cpuDecision, now, writeResourceState, true);
      } else if (cpuDecision.cpuReason !== 'cooldown') {
        dropPickRecord();
      }
      return {
        result: {
          started: false,
          reason: cpuDecision.cpuReason !== 'ok' ? 'cpu-admission' : 'memory-admission',
          cpuReason: cpuDecision.cpuReason,
          memoryReason: cpuDecision.memoryReason,
          projectedBusy: cpuDecision.projectedBusy,
          budget: cpuDecision.budget,
        },
        logFields: { ...logBase, currentDecision: 'start', currentReason: baselineReason },
      };
    }
    let recorded = null;
    if (ticket.id !== headTicket.id && headConflicted && !safeBackfill) {
      // This ticket is genuinely skipping ahead of the still-blocked head —
      // count it toward conflictSkipLimit above. Fail CLOSED: if the count
      // can't be durably recorded, refuse the skip rather than let it
      // happen uncounted (see recordSkip's doc comment).
      if (!recordSkip(root, store, headTicket.id, now)) {
        return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
      }
    } else if (ticket.id !== headTicket.id && headCapacityBlocked) {
      // BRAIN-249 part 2: same event, same fail-CLOSED discipline as the
      // conflict branch above, for a ticket genuinely skipping ahead of a
      // head that doesn't fit capacity (see recordCapacitySkip's doc
      // comment).
      if (!recordCapacitySkip(root, store, headTicket.id)) {
        return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
      }
    } else if (ticket.id !== headTicket.id && !safeBackfill && resourceBackfill) {
      // BRAIN-346: same event, same fail-CLOSED discipline; a refused write never restarts the allowance.
      recorded = recordResourceBackfill(root, store, cfg, resourceRecord, writeResourceState);
      if (!recorded) {
        return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
      }
    }
    // BRAIN-319: re-check the cancel marker here, INSIDE the same lock as the
    // dequeue-and-lease-write below, right before this ticket is actually
    // admitted to run -- this is the last point before the caller (see
    // supervisor.js) spawns the child with no further await in between. A
    // marker written between the outer, unlocked cancelRequested() check in
    // supervisor.js's poll loop and this lock being granted would otherwise
    // never be re-observed until after the child had already started;
    // checking it again here makes "the marker exists" and "the ticket gets
    // admitted" atomic. Deliberately does not dequeue: leaving the ticket in
    // the queue lets the very next poll's own cancelRequested() check (which
    // already dequeues and finalizes through publishTerminal) do that, so
    // there is exactly one dequeue-and-finalize code path rather than two.
    if (isCancelled(root, ticket.id)) {
      return {
        result: { started: false, reason: 'cancelled' },
        logFields: { ...logBase, currentDecision: 'deny', currentReason: 'cancelled' },
      };
    }
    dequeueSync(root, ticket.id);
    // BRAIN-380 §8: what the scheduler saw at admission, from this ticket's own queue record (the supervisor's copy of
    // the ticket predates the cap), carried on the lease and then into its terminal history row.
    const queued = queue[position];
    const lease = {
      id: ticket.id,
      priorityRequested: isPriorityTier(queued.priorityRequested) ? queued.priorityRequested : DEFAULT_PRIORITY,
      priorityAdmitted: priorityOf(queued),
      priorityDemoted: queued.priorityDemoted === true,
      effectiveRankAtStart: effectiveRank(queued, nowEff, cfg),
      scoreAtStart: score(queued, nowEff, cfg),
      key: ticket.key,
      bootId: bootId(),
      supervisorPid: ticket.supervisorPid,
      supervisorStart: ticket.supervisorStart,
      childPgid: null,
      heartbeatAt: now,
      // Stamped on the same write that admits the lease so the admission
      // cooldown (admission.js's cooldownActive) can be derived from held
      // leases directly, instead of a separate admission-state.json file
      // and its own write inside the lock.
      admittedAt: now,
      cwd: ticket.cwd,
      cmd: ticket.cmd,
      weight: ticket.weight,
      resources: ticket.resources,
      // BRAIN-360: elastic lanes only; what admission actually charged. `resources.cpuCores` stays the declaration.
      ...(ticket.resources?.minCpuCores !== undefined ? { grantedCpuCores } : {}),
      // BRAIN-255: carried onto the lease (not just the ticket) so a
      // held-lease-only view — status.js's report, or a later poll's
      // `blockedBy` call against a DIFFERENT ticket of the same key — can
      // still see the ceiling this holder was admitted under.
      maxConcurrent: ticket.maxConcurrent,
      // BRAIN-379: allocation class, so a held-lease-only view can charge the lease to its class.
      class: ticket.class,
      logPath: ticket.logPath,
      resultPath: ticket.resultPath,
      state: LEASE_STATE.RUNNING,
    };
    writeLease(root, lease);
    touchSimArmFor(root, lease, now);
    if (safeBackfill) {
      logHeadBlock(root, {
        event: 'safe-backfill',
        headId: headTicket.id,
        candidateId: ticket.id,
        blockingLeaseId: blocker?.id ?? 'none',
        blockingKey: blocker?.key ?? 'none',
        skipCount,
        skipLimit: cfg.conflictSkipLimit,
        graceMs: cfg.headBlockGraceMs,
        blockedMs: headBlock?.blockedMs ?? 0,
      });
    }
    const events = [];
    if (recorded) {
      events.push(['resource-backfill-start', { skipPast: headTicket.id, count: recorded.count }]);
      if (recorded.reserved) events.push(['resource-reserved', { headId: headTicket.id, count: recorded.count, limit: cfg.resourceSkipLimit }]);
    }
    if (elastic) {
      events.push(['elastic-grant', { candidateId: ticket.id, declared: fullDecision.candidateCpuCores, granted: grantedCpuCores, min: ticket.resources.minCpuCores }]);
    }
    if (idleExempt) {
      events.push(['resource-idle-exempt', { headId: headTicket.id, overshoot: (cpuDecision.projectedBusy - cpuDecision.budget).toFixed(2) }]);
    }
    // Legacy: the head's own start clears the singleton. Behind the fence every starting ticket's records leave
    // with it (a displaced ticket may start as a backfill), and with them any reservation it owned.
    if (sched.v2) store.depart(ticket.id);
    else if (headPolling) store.dropResourceRecord();
    return {
      result: { started: true, lease },
      logFields: { ...logBase, currentDecision: 'start', currentReason: idleExempt ? 'resource-idle-exempt' : baselineReason },
      events,
    };
  };
  const { result, logFields, events } = await withLock(root, decide);

  // BRAIN-379: evaluated and logged after the lock is released, so shadow never lengthens a live admission or release.
  // One record per poll cycle (the head's poll) plus one per real admission; a non-head poll that starts nothing adds
  // no information (its verdict is in the head record's candidate list).
  if ((shadow.inputs && (shadow.inputs.queue[0]?.id === ticket.id || result.started === true)) || shadow.captureError) {
    recordAllocationShadow({ root, shadow, result, pollerId: ticket.id, evaluator: shadowSeams.evaluator });
  }

  // Pure telemetry: nothing reads this back to make a decision, so it never
  // needs to be atomic with anything above — write it after the lock has
  // already been released, on both the admit and deny paths.
  if (logFields) logAdmissionDecision(root, logFields);
  for (const [event, fields] of events || []) logResourceEvent(root, event, fields);
  return result;
}
