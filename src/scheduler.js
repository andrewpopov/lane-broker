import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, readJsonSafe, withLock, bootId } from './state.js';
import { sampleAndUpdateGate } from './load.js';
import { listLeases, reapAll, writeLease, isSupervisorAlive, LEASE_STATE } from './lease.js';
import { evaluateNewAdmission, sampleCpuSafe, logAdmissionDecision, logHeadBlock, logCapacityBlock } from './admission.js';
import { readMemoryInfo } from './cpu.js';
import { detectResourceCapacity, effectiveWeightCapacity } from './resources.js';

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

/** Enqueue a ticket at the tail of the global FIFO. Caller must NOT hold the lock. */
export async function enqueue(root, ticket) {
  return withLock(root, () => {
    const seq = nextSeq(root);
    const record = { ...ticket, seq, createdAt: ticket.createdAt || Date.now() };
    atomicWriteJson(queueFile(root, seq, ticket.id), record);
    return record;
  });
}

export function dequeueSync(root, id) {
  const file = findQueueFile(root, id);
  if (file) {
    try {
      fs.unlinkSync(file);
    } catch {
      // already gone
    }
  }
}

/** Whether `lease` blocks `ticket` from starting: same key, or declared conflict. Exported for
 *  status.js's read-only "why is the queue not moving" report (BRAIN-249) — the exact same
 *  predicate tryStart uses to decide, so the report can never describe a different notion of
 *  "conflict" than the scheduler actually enforces. */
export function conflicts(ticket, lease) {
  if (lease.key === ticket.key) return true;
  return Array.isArray(ticket.conflicts) && ticket.conflicts.includes(lease.key);
}

/** Coerce a persisted (possibly corrupt) skip-count file into a safe shape. Exported for
 *  status.js's read-only report (BRAIN-249) — it needs the same headId/count/blockedSince view
 *  tryStart uses, without duplicating this parsing. `blockedSince` (BRAIN-249) is when this
 *  headId first became the blocked head; `loggedPhase` is the last head-block phase
 *  (logHeadBlock's event) written for this head, so resolveHeadBlock below only logs on an
 *  actual transition. Both are `null` for a file written before BRAIN-249 (or any other missing/
 *  malformed value) — never defaulted to something that would read as "infinitely old" or
 *  "already logged". */
export function readSkipState(root) {
  const raw = readJsonSafe(paths(root).conflictSkipState);
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
function recordSkip(root, headId, now = Date.now()) {
  const prev = readSkipState(root);
  const sameHead = prev.headId === headId;
  const count = sameHead ? prev.count + 1 : 1;
  const blockedSince = sameHead && prev.blockedSince != null ? prev.blockedSince : now;
  const loggedPhase = sameHead ? prev.loggedPhase : null;
  try {
    atomicWriteJson(paths(root).conflictSkipState, { headId, count, blockedSince, loggedPhase });
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
function resolveHeadBlock(root, cfg, headId, blocker, sameHead, skipState, skipCount, now) {
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
      atomicWriteJson(paths(root).conflictSkipState, { headId, count: skipCount, blockedSince, loggedPhase: phase });
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
export function readCapacitySkipState(root) {
  const raw = readJsonSafe(paths(root).capacitySkipState);
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
function recordCapacitySkip(root, headId) {
  const prev = readCapacitySkipState(root);
  const sameHead = prev.headId === headId;
  const count = sameHead ? prev.count + 1 : 1;
  const loggedPhase = sameHead ? prev.loggedPhase : null;
  try {
    atomicWriteJson(paths(root).capacitySkipState, { headId, count, loggedPhase });
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
function resolveCapacityBlock(root, cfg, headId, headWeight, runningWeight, capacity, skipCount) {
  const phase = skipCount < cfg.conflictSkipLimit ? 'blocked' : 'exhausted';
  const state = readCapacitySkipState(root);
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
      atomicWriteJson(paths(root).capacitySkipState, { headId, count: skipCount, loggedPhase: phase });
    } catch {
      // best-effort — see recordCapacitySkip's doc comment for why the count itself must fail
      // closed; only this logging/dedup write is allowed to be lossy.
    }
  }
  return { phase };
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
    if (!held.some((l) => conflicts(t, l))) return t;
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
    if (held.some((l) => conflicts(t, l))) continue;
    if (runningWeight + t.weight <= capacity) return t;
  }
  return null;
}

/**
 * The single atomic transaction: a ticket starts only when it is selected
 * per selectCandidate() above AND fits capacity AND the load gate is open
 * AND the broker is not paused. No backfill behind a capacity- or
 * gate-blocked head — only a conflict-blocked head is skipped.
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
export async function tryStart(root, ticket, globalCfg, loadSampler, cpuSampler, reloadCfg = () => globalCfg, memoryReader = readMemoryInfo) {
  const { result, logFields } = await withLock(root, () => {
    const cfg = reloadCfg() || globalCfg;
    const now = Date.now();
    reapAll(root, bootId());
    // A queued ticket whose supervisor already died (crashed, or the machine
    // killed it) must not sit at the FIFO head forever — dequeue it before
    // checking who's next.
    for (const t of listQueue(root)) {
      if (t && t.id !== ticket.id && !isSupervisorAlive({ supervisorPid: t.supervisorPid, supervisorStart: t.supervisorStart })) {
        dequeueSync(root, t.id);
      }
    }
    const queue = listQueue(root);
    const position = queue.findIndex((t) => t && t.id === ticket.id);
    if (position === -1) {
      return { result: { started: false, reason: 'not-head', position: null, queueLength: queue.length } };
    }
    // Selection depends only on the queue and who currently holds a key —
    // resolve it BEFORE touching the load/CPU gates (both of which persist a
    // sample and advance their hysteresis as a side effect) or the pause
    // flag, so a ticket that isn't going anywhere this poll costs exactly
    // what it did before this ticket existed: one cheap read, no side
    // effects. Only the selected candidate pays for a real gate evaluation.
    const held = listLeases(root).filter((l) => HELD_STATES.has(l.state));
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
    const weightCapacity = effectiveWeightCapacity(cfg, detectResourceCapacity().cpuCores);
    const headConflicted = held.some((l) => conflicts(headTicket, l));
    const blocker = headConflicted ? held.find((l) => conflicts(headTicket, l)) : null;
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
    const skipState = readSkipState(root);
    const sameHead = skipState.headId === headTicket.id;
    const skipCount = sameHead ? skipState.count : 0;
    const headBlock = headConflicted ? resolveHeadBlock(root, cfg, headTicket.id, blocker, sameHead, skipState, skipCount, now) : null;
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
    const capacitySkipState = headCapacityBlocked ? readCapacitySkipState(root) : null;
    const capacitySameHead = headCapacityBlocked && capacitySkipState.headId === headTicket.id;
    const capacitySkipCount = capacitySameHead ? capacitySkipState.count : 0;
    const capacityBlock = headCapacityBlocked
      ? resolveCapacityBlock(root, cfg, headTicket.id, headTicket.weight, runningWeight, weightCapacity, capacitySkipCount)
      : null;
    const capacitySkipExhausted = headCapacityBlocked && capacityBlock.phase === 'exhausted';

    const candidate = headConflicted
      ? (skipExhausted ? null : selectCandidate(queue, held))
      : headCapacityBlocked
        ? (capacitySkipExhausted ? null : selectCapacityCandidate(queue, held, runningWeight, weightCapacity))
        : headTicket;
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
    const cpuSample = sampleCpuSafe(root, cpuSampler);
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
    const cpuDecision = evaluateNewAdmission(root, cfg, ticket, held, cpuSample, memInfo);
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
      return {
        result: { started: false, reason: 'load-gate-closed', load: gate.lastLoad },
        logFields: { ...logBase, currentDecision: 'deny', currentReason: 'load-gate-closed' },
      };
    }
    if (runningWeight + ticket.weight > weightCapacity) {
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
      return {
        result: { started: false, reason: 'memory-critical', macPressure: memInfo.macPressure },
        logFields: { ...logBase, currentDecision: 'deny', currentReason: 'memory-critical' },
      };
    }
    // Phase 1's predicate is an ADDITIONAL constraint, never a replacement
    // for the conflict/capacity checks above, and it only ever gates a start
    // in 'active' mode — 'shadow' always falls through to the exact
    // admission the current rule would have made.
    if (cfg.schedulerMode === 'active' && !cpuDecision.admit) {
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
    if (ticket.id !== headTicket.id && headConflicted) {
      // This ticket is genuinely skipping ahead of the still-blocked head —
      // count it toward conflictSkipLimit above. Fail CLOSED: if the count
      // can't be durably recorded, refuse the skip rather than let it
      // happen uncounted (see recordSkip's doc comment).
      if (!recordSkip(root, headTicket.id, now)) {
        return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
      }
    } else if (ticket.id !== headTicket.id && headCapacityBlocked) {
      // BRAIN-249 part 2: same event, same fail-CLOSED discipline as the
      // conflict branch above, for a ticket genuinely skipping ahead of a
      // head that doesn't fit capacity (see recordCapacitySkip's doc
      // comment).
      if (!recordCapacitySkip(root, headTicket.id)) {
        return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
      }
    }
    dequeueSync(root, ticket.id);
    const lease = {
      id: ticket.id,
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
      logPath: ticket.logPath,
      resultPath: ticket.resultPath,
      state: LEASE_STATE.RUNNING,
    };
    writeLease(root, lease);
    return { result: { started: true, lease }, logFields: { ...logBase, currentDecision: 'start', currentReason: baselineReason } };
  });

  // Pure telemetry: nothing reads this back to make a decision, so it never
  // needs to be atomic with anything above — write it after the lock has
  // already been released, on both the admit and deny paths.
  if (logFields) logAdmissionDecision(root, logFields);
  return result;
}
