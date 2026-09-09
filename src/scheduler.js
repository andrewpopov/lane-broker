import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, readJsonSafe, withLock, bootId, appendHistory } from './state.js';
import { sampleAndUpdateGate } from './load.js';
import { listLeases, reapAll, writeLease, isSupervisorAlive, LEASE_STATE } from './lease.js';
import { evaluateNewAdmission, sampleCpuSafe, logAdmissionDecision } from './admission.js';
import { readMemoryInfo } from './cpu.js';

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

/** Remove a queue record. Returns whether the ticket is now durably not
 *  queued: true when there was never a file (nothing to remove), when the
 *  file was gone by the time we tried (a benign unlink race — ENOENT — the
 *  end state is identical to a successful unlink), or when the unlink
 *  actually succeeded. Returns false only for a genuine unlink failure
 *  (e.g. permissions, a busy/locked file) that leaves the record on disk —
 *  callers that log a durable "this ticket was dequeued" event must check
 *  this before doing so, or the log can claim a dequeue that never
 *  happened while the ticket stays queued. */
export function dequeueSync(root, id) {
  const file = findQueueFile(root, id);
  if (!file) return true;
  try {
    fs.unlinkSync(file);
    return true;
  } catch (err) {
    if (err.code === 'ENOENT') return true; // already gone
    return false;
  }
}

function conflicts(ticket, lease) {
  if (lease.key === ticket.key) return true;
  return Array.isArray(ticket.conflicts) && ticket.conflicts.includes(lease.key);
}

/** Coerce a persisted (possibly corrupt) skip-count file into a safe shape. */
function readSkipState(root) {
  const raw = readJsonSafe(paths(root).conflictSkipState);
  if (!raw || typeof raw.headId !== 'string' || !Number.isFinite(raw.count) || raw.count < 0) {
    return { headId: null, count: 0 };
  }
  return raw;
}

/** Record that a ticket other than the literal FIFO head is about to start
 *  ahead of it (a "skip event" — see conflictSkipLimit in tryStart). The
 *  count is keyed on the head's own id and resets automatically the next
 *  time a different ticket occupies the head slot (readSkipState above
 *  returns 0 for a headId mismatch), so no explicit reset is needed once
 *  the head itself finally starts or is cancelled.
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
function recordSkip(root, headId) {
  const prev = readSkipState(root);
  const count = prev.headId === headId ? prev.count + 1 : 1;
  try {
    atomicWriteJson(paths(root).conflictSkipState, { headId, count });
    return true;
  } catch {
    return false;
  }
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
 * Capacity and the load/CPU gates do NOT skip this way — a capacity- or
 * gate-blocked head has no such bound and needs an aging/round-robin scheme
 * (a separate ticket), so this function never even looks past a head
 * blocked only by those.
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
 * Restricted backfill selection used ONLY once the head's skip allowance is
 * exhausted (skipExhausted in tryStart). selectCandidate's own filter --
 * "not conflicting with anything currently HELD" -- is not enough here: the
 * Codex counterexample that got the first version of this fix rejected
 * showed that admitting anything held-conflict-free can renew the head's
 * blocker set forever. Head A conflicts with declared keys B and C; while B
 * is held, C is free to start; B drains; a fresh B arrives and IS held-
 * conflict-free (nothing currently held conflicts with it, since the old B
 * already drained) so it starts too, alternating B/C admissions and never
 * letting A's conflict genuinely clear. Every admission in that cycle was
 * legal under selectCandidate's rule and A starves anyway.
 *
 * The additional constraint here is condition (a) from BRAIN-202: a
 * candidate is eligible only if it could never itself become one of the
 * head's blockers -- i.e. a lease at the candidate's key would not conflict
 * with the head. That's exactly conflicts(headTicket, lease) -- the SAME
 * predicate that decided headConflicted above -- applied to a hypothetical
 * lease carrying the candidate's key. It is intentionally NOT hand-rolled:
 * conflicts() already knows how to read a ticket's declared conflicts list,
 * and duplicating that logic here would be the kind of parallel special
 * case that drifts the moment the declaration format changes.
 *
 * Because this filter is strictly NARROWER than selectCandidate's (every
 * candidate it admits also passes the held-conflict check below), every
 * backfill admitted from here on can only drain the set of the head's
 * blockers, never add to it -- restoring the natural starvation bound the
 * unrestricted version relies on, without needing conflictSkipLimit to fire
 * a second time.
 *
 * Capacity/reservation (condition (b)) is deliberately NOT applied inside
 * this loop. This function only decides WHICH ticket is eligible, walking
 * the FIFO queue in the same order and stopping at the same barriers as
 * selectCandidate (a corrupt/unreadable record stops the walk rather than
 * being skipped over, same as Codex review finding #7). Folding a capacity
 * check in here would turn a bounded selection into a search for a smaller
 * candidate further down the queue if the first eligible one doesn't fit --
 * explicitly rejected in review, since it would change selection order.
 * The reservation inequality is enforced exactly once in tryStart, against
 * the single candidate this function returns, and a failure there denies
 * the poll outright instead of trying anyone else.
 */
function selectBackfillCandidate(queue, held, headTicket) {
  for (const t of queue) {
    if (!t) return null;
    if (conflicts(headTicket, { key: t.key })) continue;
    if (held.some((l) => conflicts(t, l))) continue;
    return t;
  }
  return null;
}

/**
 * The single atomic transaction: a ticket starts only when it is selected
 * per selectCandidate() above AND fits capacity AND the load gate is open
 * AND the broker is not paused. No backfill behind a capacity- or
 * gate-blocked head — only a conflict-blocked head is skipped.
 *
 * BRAIN-202: once the conflicted head's skip allowance is exhausted,
 * selection switches to selectBackfillCandidate's restricted rule instead
 * of stopping entirely (see that function's doc comment for condition (a),
 * and the headReserve arithmetic below for condition (b)). The combined
 * guarantee this buys is honest, not absolute -- three qualifications:
 *   - NOT retroactive. The inequality only constrains admissions made from
 *     this poll forward; a backfill already admitted before the head became
 *     exhausted may already be sitting in the capacity being reserved.
 *   - The load gate and `lane pause` are unaffected and still apply exactly
 *     as before -- a restricted backfill candidate still has to pass both,
 *     same as any other candidate reaching this point.
 *   - Interacts with BRAIN-197's idle exemption: that exemption only fires
 *     when `held.length === 0`. A restricted backfill keeps something held,
 *     so if the load gate closes while it runs, the conflicted head loses
 *     the idle exemption it would otherwise have gotten once the broker
 *     drained to true idle. This is a real, accepted change to the head's
 *     liveness conditions -- not worked around here, on purpose (see the
 *     plan this ticket shipped from).
 *
 * Shadow-mode bookkeeping must not lengthen the hold on the global lock,
 * the most contended resource in the system: the CPU sample is taken
 * BEFORE the lock and passed in, and the admission decision log (pure
 * telemetry nothing reads back) is written AFTER the lock releases, on
 * every path. The CPU sample IS a read-then-write on shared state
 * (cpu-sample.json) — see the correctness note on sampleHostCpu in cpu.js
 * for the residual race this accepts and why, and what changes if
 * schedulerMode ever moves to 'active'. What stays inside this lock is
 * exactly what reads-then-writes shared gate state where a torn
 * interleaving between two supervisors could produce a decision no config
 * ever installed would have produced: the load gate and the CPU gate's
 * consecutive-under counter (see admission.js's evaluateNewAdmission).
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
  // Every poll samples, regardless of whether this ticket turns out to be
  // the one actually evaluated below (that requires the locked, consistent
  // queue/lease view this function doesn't have yet). See cpu.js's
  // sampleHostCpu for how the sidecar write stays monotonic despite racing
  // outside this lock.
  const cpuSample = sampleCpuSafe(root, cpuSampler);

  const { result, logFields } = await withLock(root, () => {
    const cfg = reloadCfg() || globalCfg;
    reapAll(root, bootId());
    // A queued ticket whose supervisor already died (crashed, or the machine
    // killed it) must not sit at the FIFO head forever — dequeue it before
    // checking who's next.
    for (const t of listQueue(root)) {
      if (t && t.id !== ticket.id && !isSupervisorAlive({ supervisorPid: t.supervisorPid, supervisorStart: t.supervisorStart })) {
        const removed = dequeueSync(root, t.id);
        // Only record the event when the record is actually gone. A failed
        // unlink (dequeueSync returns false) leaves the ticket queued, so
        // logging the dequeue here would be false — worse, the SAME false
        // event would then be appended again on every subsequent poll,
        // since the ticket is still sitting at the head with a dead
        // supervisor. The queue record being gone is what makes this the
        // only durable record of the drop (BRAIN-202), not just stderr of
        // whichever poll happened to notice. appendHistory is the same
        // durable, best-effort ledger cancel.js and supervisor.js already
        // use for "this id is done and here's why".
        if (removed) {
          appendHistory(root, { id: t.id, key: t.key, dequeuedDeadSupervisor: true, supervisorPid: t.supervisorPid, at: Date.now() });
        }
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
    const headConflicted = held.some((l) => conflicts(headTicket, l));
    // Starvation bound (Codex review finding #6): a conflict-blocked head
    // does NOT have a natural bound in general — with three or more
    // conflicting keys, alternating which one is held can keep skipping a
    // different, always-non-conflicting later ticket ahead of the head
    // forever. Once the head has been skipped conflictSkipLimit times in a
    // row, stop looking past it: it blocks like the pre-conflict-skip
    // behavior until its own conflict actually clears.
    const skipState = readSkipState(root);
    const skipCount = skipState.headId === headTicket.id ? skipState.count : 0;
    const skipExhausted = headConflicted && skipCount >= cfg.conflictSkipLimit;
    // Once exhausted, this is no longer "no backfill at all" (BRAIN-202):
    // selectBackfillCandidate's restricted rule still lets a ticket run
    // behind the head, as long as it can never become one of the head's
    // own blockers -- see that function's doc comment for why this is
    // narrower than, not a relaxation of, the ordinary selectCandidate path.
    const candidate = !headConflicted ? headTicket : skipExhausted ? selectBackfillCandidate(queue, held, headTicket) : selectCandidate(queue, held);
    if (skipExhausted && ticket.id === headTicket.id) {
      // Once exhausted, whatever selectBackfillCandidate found (if anything)
      // is a bounded exception carved out for a ticket that CANNOT renew the
      // head's blocker (condition (a) — see that function's doc comment),
      // never evidence that the head's own conflict has cleared. Reporting
      // 'not-head' here (as the generic branches below would, since some
      // other ticket IS the selected candidate) would be a worse diagnostic
      // than pre-BRAIN-202: it reads as "someone valid is ahead of you,
      // wait your turn" when the truth is the head is still genuinely
      // conflict-blocked regardless of what runs alongside it. This check
      // is scoped to skipExhausted only — before exhaustion, an ordinary
      // skip-ahead candidate really does mean "not your turn yet" and
      // 'not-head' remains the right, unchanged answer (see selectCandidate
      // and the first test in tests/conflict-skip.test.js).
      const blocker = held.find((l) => conflicts(headTicket, l));
      return { result: { started: false, reason: 'conflict', with: blocker.id, key: blocker.key } };
    }
    if (!candidate) {
      // Either nobody in the queue is conflict-free (selectCandidate found
      // nothing, which implies the head itself conflicts too), or the
      // restricted backfill search found nothing eligible either. Either
      // way the head is the one genuinely blocked here.
      const blocker = held.find((l) => conflicts(headTicket, l));
      if (ticket.id === headTicket.id) {
        return { result: { started: false, reason: 'conflict', with: blocker.id, key: blocker.key } };
      }
      return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
    }
    if (candidate.id !== ticket.id) {
      // A non-conflicting ticket earlier in the queue gets to go first (or
      // already has). This ticket just isn't up yet — same as the old
      // strict-head "not-head", just computed against the conflict-skip
      // selection instead of raw queue position.
      return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
    }
    if (fs.existsSync(paths(root).pause)) {
      const reason = fs.readFileSync(paths(root).pause, 'utf8').trim();
      return { result: { started: false, reason: 'paused', pauseReason: reason } };
    }
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
    const cpuDecision = evaluateNewAdmission(root, cfg, ticket, held, cpuSample);
    // BRAIN-207: when admissionLoadGate is false the gate is sampled and
    // logged exactly as before (its hysteresis countdown must not stall for
    // want of observation), but it is never allowed to deny — for an idle
    // broker OR one holding non-conflicting leases. `loadGateIgnored`
    // records the decisions where that actually mattered (a closed gate the
    // flag suppressed), so shadow telemetry can tell "gate never got the
    // chance to matter" from "gate genuinely never closed".
    const loadGateIgnored = !cfg.admissionLoadGate && gate.closed;
    const logBase = { candidateId: ticket.id, mode: cfg.schedulerMode, loadGateIgnored, ...cpuDecision };
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
    const runningWeight = held.reduce((sum, l) => sum + (l.weight || 0), 0);
    // Condition (b) from BRAIN-202: a backfill admitted while the head's
    // skip allowance is exhausted must leave room for the head's OWN
    // weight too, or condition (a) above is protecting a head that can
    // never actually get a slot once its conflict clears. This reservation
    // is NOT retroactive -- it only constrains what's admitted from this
    // poll forward; backfill admitted before the head became exhausted may
    // already occupy the capacity being reserved for, and this ticket's own
    // weight is already counted once in runningWeight+ticket.weight, so the
    // extra term below is exactly headTicket's weight, never double-counted
    // (headTicket.weight is 0 when candidate.id === headTicket.id, i.e.
    // the head is its own candidate and reserves nothing against itself). A
    // weight-0 head reserves nothing here -- arithmetically fine, but it
    // also protects nothing, since any capacity at all satisfies the
    // inequality. Note this also does nothing for a head whose OWN weight
    // exceeds capacity outright: no backfill can ever satisfy the
    // inequality, so that head stalls exactly as it would with zero
    // backfill running -- a pre-existing impossible-head condition, not
    // something this ticket introduces or fixes.
    const headReserve = skipExhausted && candidate.id !== headTicket.id ? headTicket.weight : 0;
    if (runningWeight + ticket.weight + headReserve > cfg.capacity) {
      return {
        result: { started: false, reason: 'capacity', runningWeight, capacity: cfg.capacity },
        logFields: { ...logBase, currentDecision: 'deny', currentReason: 'capacity' },
      };
    }
    // Memory brake (BRAIN-207): a best-effort, never-throwing reader; only
    // an explicit 'critical' reading denies (applies to an idle broker too
    // — this is a hard machine-health brake, not something the idle
    // exemption should ever bypass). Any other value, a null reading, or a
    // throwing reader all admit — same fail-open tolerance as the CPU/load
    // samplers elsewhere in this function.
    let memInfo = null;
    try {
      memInfo = memoryReader();
    } catch {
      memInfo = null;
    }
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
          reason: 'cpu-admission',
          cpuReason: cpuDecision.reason,
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
      if (!recordSkip(root, headTicket.id)) {
        return { result: { started: false, reason: 'not-head', position: position + 1, queueLength: queue.length } };
      }
    }
    dequeueSync(root, ticket.id);
    const now = Date.now();
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
