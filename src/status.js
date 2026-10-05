import fs from 'node:fs';
import path from 'node:path';
import { ensureStateDirs, paths, withLock, bootId, readJsonSafe, readDrainMarker } from './state.js';
import { listLeases, reapAll, LEASE_STATE } from './lease.js';
import { listQueue, HELD_STATES, blockedBy, readSkipState, readCapacitySkipState, readResourceSkipState } from './scheduler.js';
import { cpuBudget, projectBusy, ticketCpuEstimate } from './admission.js';
import { classLocks, simArmed, usedByClass } from './allocation.js';
import { queuedClaimsByClass } from './allocation-shadow.js';
import { readLastSimDemandAt } from './sim-arm.js';
import { listAttempts, supervisorAlive } from './attempts.js';
import { readGateState } from './load.js';
import { readMemorySample, classifyMemorySample } from './memory.js';
import { loadGlobalConfig } from './config.js';
import { effectiveNow } from './priority-clock.js';
import { resolveScheduler, legacyStore, fairnessStore, effectiveView } from './fairness.js';
import { priorityOf, effectiveRank, tierName } from './priority.js';
import { leaseOverrun } from './observed.js';
import { detectResourceCapacity, effectiveWeightCapacity, leaseResources, leaseCpuCores } from './resources.js';

/** Holder pid of the global lock, read directly off disk — used to name the
 *  holder in the "couldn't take the lock" diagnostic without re-taking it. */
function currentLockHolderPid(root) {
  const owner = readJsonSafe(path.join(paths(root).lock, 'owner.json'));
  return owner ? owner.pid : null;
}

/**
 * BRAIN-249: "why is the queue not moving" — read-only, so unlike
 * scheduler.js's resolveHeadBlock/resolveCapacityBlock this never writes
 * either skip-state file (a `lane status` call must never mutate scheduling
 * state); a legacy conflict file with no `blockedSince` is treated as
 * "starting now" here too, purely for this one render, without persisting
 * it — the next real tryStart poll does the actual healing. Returns null
 * whenever there's nothing to report: no queue, the head neither conflicts
 * nor fails to fit capacity, or its skip allowance for whichever reason
 * applies isn't actually exhausted yet (selection is still finding a way to
 * make progress, so this isn't the stalled case this report exists for).
 *
 * `kind` distinguishes the two mutually-exclusive block reasons for
 * renderStatusText: 'conflict' carries the time-bounded blockedMs/graceMs/
 * refused shape (see resolveHeadBlock), 'capacity' does not — BRAIN-249
 * part 2 deliberately has no grace period for capacity (see
 * resolveCapacityBlock's doc comment in scheduler.js), so there is nothing
 * to count down.
 */
function computeHeadBlock(root, store, cfg, queue, leases, now, weightCapacity) {
  const head = queue[0];
  if (!head) return null;
  const held = leases.filter((l) => HELD_STATES.has(l.state));
  const runningWeight = held.reduce((s, l) => s + (l.weight || 0), 0);
  const blocker = blockedBy(held, head);
  if (blocker) {
    const skipState = readSkipState(root, store, head.id);
    const sameHead = skipState.headId === head.id;
    const skipCount = sameHead ? skipState.count : 0;
    if (skipCount < cfg.conflictSkipLimit) return null;
    const blockedSince = sameHead && skipState.blockedSince != null ? skipState.blockedSince : now;
    const blockedMs = now - blockedSince;
    return {
      kind: 'conflict',
      headId: head.id,
      blockingLeaseId: blocker.id,
      blockingKey: blocker.key,
      skipCount,
      skipLimit: cfg.conflictSkipLimit,
      graceMs: cfg.headBlockGraceMs,
      blockedMs,
      refused: blockedMs < cfg.headBlockGraceMs,
    };
  }
  // The EFFECTIVE capacity, never raw cfg.capacity: with `capacity: 'auto'` the raw value is a
  // string, the comparison is always false, and a capacity-blocked head was never reported.
  if (runningWeight + head.weight > weightCapacity) {
    const capState = readCapacitySkipState(root, store, head.id);
    const sameHead = capState.headId === head.id;
    const skipCount = sameHead ? capState.count : 0;
    if (skipCount < cfg.conflictSkipLimit) return null;
    return {
      kind: 'capacity',
      headId: head.id,
      headWeight: head.weight,
      runningWeight,
      capacity: weightCapacity,
      skipCount,
      skipLimit: cfg.conflictSkipLimit,
    };
  }
  return null;
}

/**
 * BRAIN-346: read-only view of resource-skip-state.json for the current head. `projectedBusy` is
 * recomputed from the record's ambient snapshot against the leases held NOW, with the same
 * projectBusy maths admission uses; `budget` is the one the head was last denied against.
 * Null when there is no head, the record belongs to another head, or backfill is off.
 */
function computeResourceBlock(root, store, cfg, head, held, now) {
  if (!head || !(cfg.resourceSkipLimit > 0)) return null;
  const record = readResourceSkipState(root, store, head.id);
  if (!record || record.headId !== head.id) return null;
  return {
    headId: head.id,
    count: record.count,
    limit: cfg.resourceSkipLimit,
    reserved: record.reserved,
    projectedBusy: projectBusy(record.externalBusy, held, ticketCpuEstimate(head, cfg), now, cfg),
    budget: record.budget,
    deniedAgeMs: now - record.deniedAt,
  };
}

/** Report-only (BRAIN-202): five minutes without a change to a lane's own log
 *  FILE mtime is flagged in `lane status`. `elapsed` and `heartbeat-age`
 *  describe the SUPERVISOR, which stays healthy while a child goes quiet, so
 *  nothing else surfaced it. This is NOT a "no output" detector:
 *  `CappedLogWriter` (src/supervisor.js) switches to discard mode on cap or
 *  write failure, so the file can stop changing while the child keeps
 *  writing. Nor is it a "hung" detector (a quiet compile looks identical to a
 *  wedged process from mtime alone), so it must never feed an auto-cancel. */
export const LOG_STALE_MS = 5 * 60 * 1000;

/** Age in ms of the last write to `logPath`, or null if unset or unreadable.
 *  Never throws: a lease with no log yet must not crash `lane status`. */
function logMtimeAgeMs(logPath) {
  if (!logPath) return null;
  try {
    return Date.now() - fs.statSync(logPath).mtimeMs;
  } catch {
    return null;
  }
}

export async function collectStatus({ lockTimeoutMs = 5000 } = {}) {
  const root = ensureStateDirs().root;
  const cfg = loadGlobalConfig();
  let lockError = null;
  try {
    await withLock(root, () => reapAll(root, bootId()), { timeoutMs: lockTimeoutMs });
  } catch (err) {
    // The lock is only needed to reap stale leases before reporting; the
    // leases/queue/gate files underneath are readable without it. A timeout
    // here must be surfaced, never swallowed into a silent blank report.
    lockError = { message: err.message, holderPid: currentLockHolderPid(root) };
  }

  const leases = listLeases(root);
  // BRAIN-380: behind a valid scheduler fence status runs the scheduler's own pipeline, read-only: the persisted
  // clock (never advanced here), orderQueue, then the reservation owner's promotion. Positions below come from
  // that one array, and the skip/reservation views read the per-ticket store, never the singleton files.
  const sched = resolveScheduler(root, { log: false });
  const store = sched.v2 ? fairnessStore(root, sched.tickets) : legacyStore(root);
  const rawQueue = listQueue(root);
  const gate = readGateState(root);
  const p = paths(root);
  const paused = fs.existsSync(p.pause) ? fs.readFileSync(p.pause, 'utf8').trim() : null;
  const draining = readDrainMarker(root);
  const configWarning = readJsonSafe(p.configWarning);

  const now = Date.now();
  const resourceCapacity = detectResourceCapacity();
  const running = leases
    .filter((l) => l.state === LEASE_STATE.RUNNING || l.state === LEASE_STATE.ORPHANED)
    .map((l) => ({
      id: l.id,
      key: l.key,
      state: l.state,
      pid: l.childPgid,
      elapsedMs: l.startedAt ? now - l.startedAt : null,
      heartbeatAgeMs: l.heartbeatAt ? now - l.heartbeatAt : null,
      logAgeMs: logMtimeAgeMs(l.logPath),
      log: l.logPath,
      weight: l.weight,
      // BRAIN-255: default 1 for a lease written before this field existed,
      // matching resolveTicketConfig's own compatibility default.
      maxConcurrent: l.maxConcurrent ?? 1,
      resources: leaseResources(l, cfg),
      ...(Number.isFinite(l.grantedCpuCores) ? { declaredCpuCores: l.resources?.cpuCores ?? l.weight, grantedCpuCores: l.grantedCpuCores } : {}),
      observedCpuCores: l.observedCpuCores ?? null,
      // BRAIN-361: report-only; never read by admission
      bookedCpuCores: leaseCpuCores(l) ?? null,
      overrun: leaseOverrun(l),
      observedMemoryBytes: l.observedMemoryBytes ?? null,
    }));

  const runningWeight = leases.filter((l) => HELD_STATES.has(l.state)).reduce((s, l) => s + (l.weight || 0), 0);
  const held = leases.filter((l) => HELD_STATES.has(l.state));
  const reservedCpuCores = held.reduce((sum, lease) => sum + leaseResources(lease, cfg).cpuCores, 0);
  const reservedMemoryBytes = held.reduce((sum, lease) => sum + leaseResources(lease, cfg).memoryBytes, 0);

  // BRAIN-380: read-only view of the priority clock (status never writes the mark). Without
  // a valid scheduler fence the broker runs the legacy FIFO scheduler, so the queue below
  // stays FIFO and the rank is informational: what the ticket's age would be worth.
  const nowEff = effectiveNow(root, now);
  const { queue, ownerId: reservationOwnerId } = sched.v2 ? effectiveView(rawQueue, nowEff, cfg, store) : { queue: rawQueue, ownerId: null };
  const queued = queue.map((t, i) => {
    const rank = effectiveRank(t, nowEff, cfg);
    const tier = priorityOf(t);
    return {
      id: t.id,
      key: t.key,
      position: i + 1,
      waitedMs: now - t.createdAt,
      priority: tier,
      ...(t.id === reservationOwnerId ? { reservationOwner: true } : {}),
      ...(tierName(rank) !== tier ? { effectiveRank: tierName(rank) } : {}),
      resources: leaseResources(t, cfg),
    };
  });

  // BRAIN-319 T3b-4 (C4): an attempt still mid remote-dispatch (executor
  // 'remote') is neither a lease nor a queue entry -- `used`/`reservedCpu
  // Cores`/`reservedMemoryBytes` above are derived from `leases` alone, so
  // a remote attempt intentionally never counts toward local capacity or
  // resource reservations. An attempt that has already fallen back
  // (executor 'local') is excluded here since it shows up as a normal
  // queued/running entry instead once `enqueue()` runs (or, in the gap
  // between fallback and that enqueue landing, as neither -- a narrow,
  // accepted window no worse than any other ticket's own enqueue race).
  const remote = listAttempts(root)
    .filter((a) => a.executor === 'remote')
    .map((a) => ({
      id: a.id,
      runner: a.runner,
      phase: a.phase,
      elapsedMs: a.startedAt ? now - a.startedAt : null,
      orphaned: !supervisorAlive(a),
    }));

  return {
    capacity: effectiveWeightCapacity(cfg, resourceCapacity.cpuCores),
    used: runningWeight,
    resources: {
      source: resourceCapacity.source,
      cpuCores: resourceCapacity.cpuCores,
      cpuBudgetCores: Math.max(
        0,
        Math.min(
          resourceCapacity.cpuCores - cfg.cpuReserveCores,
          (cfg.cpuAdmissionPercent / 100) * resourceCapacity.cpuCores,
        ),
      ),
      reservedCpuCores,
      memoryBytes: resourceCapacity.memoryBytes,
      availableMemoryBytes: resourceCapacity.availableMemoryBytes,
      memoryBudgetBytes: Math.max(0, resourceCapacity.memoryBytes - cfg.memoryReserveBytes),
      reservedMemoryBytes,
      mode: cfg.schedulerMode,
    },
    paused,
    draining,
    priority: sched.v2 ? { active: true, mode: 'v2', nowEff, reservationOwner: reservationOwnerId } : { active: false, mode: 'legacy', nowEff },
    ...(cfg.allocationShadow ? { allocation: computeAllocation(root, cfg, queue.filter(Boolean), held, resourceCapacity.cpuCores, now) } : {}),
    // BRAIN-249: null unless the queue is genuinely stalled (a conflict-
    // blocked head whose skip allowance is exhausted) — see computeHeadBlock.
    headBlock: computeHeadBlock(root, store, cfg, queue, leases, now, effectiveWeightCapacity(cfg, resourceCapacity.cpuCores)),
    // BRAIN-346: the projected-over-budget head's backfill allowance / reservation, if any.
    resourceBlock: computeResourceBlock(root, store, cfg, queue[0], held, now),
    configWarning: configWarning ? { message: configWarning.message, firstAt: configWarning.firstAt, lastAt: configWarning.lastAt } : null,
    loadGate: {
      closed: gate.closed,
      lastLoad: gate.lastLoad ?? null,
      sampleAgeMs: gate.lastSampleAt ? now - gate.lastSampleAt : null,
      consecutiveUnder: gate.consecutiveUnder || 0,
      loadClose: cfg.loadClose,
      loadOpen: cfg.loadOpen,
      loadOpenSamples: cfg.loadOpenSamples,
      // BRAIN-207: whether a closed gate is actually allowed to deny a
      // start. false means the gate line below is informational only —
      // still sampled and reported, never enforced.
      admission: Boolean(cfg.admissionLoadGate),
    },
    // BRAIN-211: swap/compressor headroom, sampled fresh on every status
    // call. Purely informational — never an admission input; see memory.js.
    // null whenever the sample itself is unavailable (non-macOS host, or a
    // failed probe), never a fabricated reading.
    memory: classifyMemorySample(readMemorySample()),
    running,
    queued,
    remote,
    lockError,
  };
}

/** BRAIN-249: how old a load-gate sample can be before `lane status` flags it
 *  as stale rather than current. Fixed, not derived from any one broker's
 *  configured sampleMs (default 5s) — an operator reading "sampled 173m ago"
 *  needs the same stale/fresh call regardless of what sampleMs happens to be
 *  set to on this machine, and a healthy queue samples on essentially every
 *  poll, so anything this many multiples older can only mean nothing has
 *  been selected in that whole span. */
const STALE_SAMPLE_MS = 5 * 60 * 1000;

/** BRAIN-379 (shadow): per-class CPU used, soft-lock target and queued claim. Report-only; read-only like the rest of status. */
function computeAllocation(root, cfg, queue, held, cpuCores, now) {
  const B = cpuBudget({ cores: cpuCores }, cfg);
  const used = usedByClass(held, now, cfg);
  const queued = queuedClaimsByClass(queue, cfg);
  const lastSimDemandAt = readLastSimDemandAt(root);
  const armed = simArmed({ now, lastSimDemandAt, simQueued: queued.sim > 0, simCharged: held.some((l) => l.class === 'sim'), simArmWindowMs: cfg.simArmWindowMs });
  const target = classLocks({ B, armed });
  return { shadow: true, budgetCores: B, armed, lastSimDemandAt, test: { used: used.test, target: target.L_t, queued: queued.test }, sim: { used: used.sim, target: target.L_s, queued: queued.sim } };
}

/** BRAIN-360: shown only when admission granted less CPU than the lane declared. */
function elasticNote(r) {
  return r.grantedCpuCores < r.declaredCpuCores ? `  cpu=${r.grantedCpuCores}/${r.declaredCpuCores} (elastic)` : '';
}

/** BRAIN-361: `  cpu 7.80/4` observed vs booked, so an under-booked lease is visible at a glance. */
function observedNote(r) {
  if (!Number.isFinite(r.observedCpuCores) || !Number.isFinite(r.bookedCpuCores)) return '';
  return `  cpu ${r.observedCpuCores.toFixed(2)}/${r.bookedCpuCores}`;
}

function fmtMs(ms) {
  if (ms == null) return '-';
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m${s % 60}s`;
}

function fmtMB(bytes) {
  if (bytes == null || !Number.isFinite(bytes)) return '?';
  return `${(bytes / (1024 * 1024)).toFixed(0)}MB`;
}

/** BRAIN-211/BRAIN-273: the memory line is informational-only, always —
 *  there is no admission flag to gate it on like loadGate.admission, since
 *  nothing ever reads this reading back for a start decision. The verdict
 *  (HEALTHY/TIGHT/EXHAUSTED) is driven by actual availability
 *  (classifyMemorySample); swap and compressor are appended only as
 *  footprint context, explicitly labeled as such — see memory.js's
 *  AVAILABLE_TIGHT_PCT comment for why swap can never drive the verdict. */
function renderMemoryLine(memory) {
  if (!memory) {
    return 'memory: unavailable (no vm_stat availability sample — see BRAIN-273) [informational]';
  }
  const swapFootprint =
    memory.swapUsedPct == null
      ? ''
      : ` swap footprint ${fmtMB(memory.swapUsedBytes)}/${fmtMB(memory.swapTotalBytes)} ${memory.swapUsedPct.toFixed(1)}%,`;
  const compressor = memory.compressorBytes == null ? '' : ` compressor ${fmtMB(memory.compressorBytes)}`;
  const context = `${swapFootprint}${compressor}`.trim();
  return (
    `memory: ${memory.level.toUpperCase()} (available ${fmtMB(memory.availableBytes)}/${fmtMB(memory.totalMemoryBytes)}` +
    ` ${memory.availablePct.toFixed(1)}%${context ? `; ${context}` : ''}) [informational]`
  );
}

export function renderStatusText(status) {
  const lines = [];
  lines.push(`capacity: ${status.used}/${status.capacity} used`);
  if (status.resources) {
    lines.push(
      `resources: CPU ${status.resources.reservedCpuCores.toFixed(2)}/${status.resources.cpuBudgetCores.toFixed(2)} cores, ` +
        `memory ${fmtMB(status.resources.reservedMemoryBytes)}/${fmtMB(status.resources.memoryBudgetBytes)} reserved, ` +
        `${fmtMB(status.resources.availableMemoryBytes)} currently available (${status.resources.source}, ${status.resources.mode})`,
    );
  }
  const informationalSuffix = status.loadGate.admission ? '' : ' [informational]';
  if (status.loadGate.lastLoad == null && status.loadGate.sampleAgeMs == null) {
    lines.push(`load gate: open (no sample yet — samples are taken when a ticket reaches the queue head)${informationalSuffix}`);
  } else {
    // BRAIN-249: sampleAndUpdateGate only runs once a candidate is actually
    // SELECTED (see tryStart's own comment on why) — a queue that never
    // selects anything (every ticket behind a conflict-blocked, skip-
    // exhausted head) samples nothing, so this reading only gets older.
    // Rendering an hours-old sample as plain "load gate: open" is exactly
    // what made a stalled queue look like a crashed sampler in the BRAIN-249
    // incident; flag it instead, reusing the same "samples are taken when a
    // ticket reaches the queue head" explanation as the no-sample-yet branch
    // above rather than inventing new wording for the same underlying fact.
    const stale = status.loadGate.sampleAgeMs != null && status.loadGate.sampleAgeMs > STALE_SAMPLE_MS;
    const staleNote = stale
      ? ' — STALE: samples are taken when a ticket reaches the queue head, so this means nothing has been selected in that long, not that sampling crashed'
      : '';
    lines.push(
      `load gate: ${status.loadGate.closed ? 'CLOSED' : 'open'}` +
        ` (load ${status.loadGate.lastLoad ?? '?'}, sampled ${fmtMs(status.loadGate.sampleAgeMs)} ago` +
        `, close>${status.loadGate.loadClose}, open<${status.loadGate.loadOpen} x${status.loadGate.loadOpenSamples}` +
        `, consecutive-under ${status.loadGate.consecutiveUnder})${staleNote}${informationalSuffix}`,
    );
  }
  if (status.headBlock) {
    const hb = status.headBlock;
    if (hb.kind === 'capacity') {
      // BRAIN-249 part 2: no grace/refused distinction here — capacity
      // backfill has no time-based resume (see resolveCapacityBlock's doc
      // comment in scheduler.js), so this is always "refused until running
      // work drains", never "refused for N more".
      lines.push(
        `queue stalled: head ${hb.headId} (weight ${hb.headWeight}) does not fit capacity ` +
          `(${hb.runningWeight}/${hb.capacity} running); skip allowance exhausted (${hb.skipCount}/${hb.skipLimit}), ` +
          `backfill refused until running work drains`,
      );
    } else {
      lines.push(
        hb.refused
          ? `queue stalled: head ${hb.headId} is blocked by lease ${hb.blockingLeaseId} (key ${hb.blockingKey}); ` +
              `skip allowance exhausted (${hb.skipCount}/${hb.skipLimit}), backfill refused for ` +
              `${fmtMs(hb.graceMs - hb.blockedMs)} more (grace ${fmtMs(hb.graceMs)})`
          : `queue: head ${hb.headId} is still blocked by lease ${hb.blockingLeaseId} (key ${hb.blockingKey}), but the ` +
              `${fmtMs(hb.graceMs)} grace period has lapsed — backfill resumed past it`,
      );
    }
  }
  if (status.resourceBlock) {
    const rb = status.resourceBlock;
    lines.push(
      `resource-blocked: head ${rb.headId} projects ${rb.projectedBusy.toFixed(2)} > budget ${rb.budget.toFixed(2)} cores ` +
        `(denied ${fmtMs(rb.deniedAgeMs)} ago); backfill ${rb.count}/${rb.limit}` +
        (rb.reserved ? ', RESERVED — nothing else is admitted past it' : ''),
    );
  }
  if (status.allocation) {
    const a = status.allocation;
    const cls = (name, c) => `${name} used ${c.used.toFixed(2)}/target ${c.target.toFixed(2)} queued ${c.queued.toFixed(2)}`;
    lines.push(`allocation (shadow): ${cls('test', a.test)}; ${cls('sim', a.sim)}; sims ${a.armed ? 'armed' : 'unarmed'}`);
  }
  lines.push(renderMemoryLine(status.memory));
  if (status.priority && !status.priority.active) {
    lines.push('priority: inactive (legacy scheduler; run lane migrate-scheduler)');
  } else if (status.priority) {
    lines.push(`priority: active${status.priority.reservationOwner ? ` (reservation owner ${status.priority.reservationOwner} promoted to the front)` : ''}`);
  }
  if (status.draining?.live) {
    lines.push(`draining for scheduler migration (pid ${status.draining.pid}, since ${new Date(status.draining.startedAt).toISOString()}): new tickets are refused, queued ones still run`);
  } else if (status.draining) {
    lines.push(`drain marker is stale (pid ${status.draining.pid ?? 'unknown'} is gone); the next lane run or lane migrate-scheduler clears it`);
  }
  lines.push(`pause: ${status.paused ? `PAUSED — ${status.paused}` : 'not paused'}`);
  if (status.configWarning) {
    lines.push(
      `config: WARNING — some supervisor(s) cannot read the global config and are running on their last known-good ` +
        `values: ${status.configWarning.message} (since ${fmtMs(Date.now() - status.configWarning.firstAt)} ago)`,
    );
  }
  lines.push('');
  lines.push('RUNNING:');
  if (status.running.length === 0) {
    lines.push('  (none)');
  } else {
    for (const r of status.running) {
      const flag = (r.state === 'ORPHANED' ? ' [ORPHANED]' : '') + (r.overrun ? ` [OVERRUN peak ${r.overrun.observedPeak} for ${fmtMs(r.overrun.sinceMs)}]` : '');
      // BRAIN-255: only shown once it's non-default, so "2 running" reads
      // differently against a maxConcurrent: 8 lane than a plain exclusive
      // one, without cluttering the common (ceiling 1) case.
      const ceiling = r.maxConcurrent > 1 ? `  ceiling=${r.maxConcurrent}` : '';
      const logFlag = r.logAgeMs != null && r.logAgeMs >= LOG_STALE_MS ? `  log file unchanged for ${fmtMs(r.logAgeMs)}` : '';
      lines.push(
        `  ${r.id}  key=${r.key}  pid=${r.pid ?? '-'}  elapsed=${fmtMs(r.elapsedMs)}  ` +
          `heartbeat-age=${fmtMs(r.heartbeatAgeMs)}  log=${r.log}${logFlag}${flag}${ceiling}${elasticNote(r)}${observedNote(r)}`,
      );
    }
  }
  lines.push('');
  lines.push(status.priority?.active ? 'QUEUE (priority order):' : 'QUEUE (FIFO):');
  if (status.queued.length === 0) {
    lines.push('  (none)');
  } else {
    for (const q of status.queued) {
      const tier = q.priority ? `  priority=${q.priority}${q.effectiveRank ? ` (aged to ${q.effectiveRank})` : ''}` : '';
      lines.push(`  #${q.position} ${q.id}  key=${q.key}  waited=${fmtMs(q.waitedMs)}${tier}${q.reservationOwner ? '  [reservation owner]' : ''}`);
    }
  }
  if (status.remote) {
    lines.push('');
    lines.push('REMOTE:');
    if (status.remote.length === 0) {
      lines.push('  (none)');
    } else {
      for (const r of status.remote) {
        const flag = r.orphaned ? `  [ORPHANED-REMOTE] — reconcile with: lane cancel ${r.id}` : '';
        lines.push(`  ${r.id}  runner=${r.runner ?? '-'}  phase=${r.phase}  elapsed=${fmtMs(r.elapsedMs)}${flag}`);
      }
    }
  }
  return lines.join('\n');
}

/** Write `text` to `stream` and resolve only once the write has actually
 *  been accepted by the underlying fd. `process.stdout.write` on a pipe is
 *  asynchronous and does not block the event loop on its own — if the
 *  process's exit code is already set and nothing else is keeping the loop
 *  alive, node can exit before a large or slow-draining write reaches the
 *  reader, so a caller under load can walk away with zero bytes written
 *  despite a clean exit code. Awaiting the write's own callback is what
 *  actually orders "reported" after "delivered". */
function writeAndFlush(stream, text) {
  return new Promise((resolve, reject) => {
    stream.write(text, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

export async function statusCommand({ json } = {}) {
  const root = ensureStateDirs().root;
  const status = await collectStatus();

  // The lock diagnostic goes to stderr in BOTH modes, before either branch:
  // a --json caller that only reads stdout still gets `lockError` in the
  // payload, but a human watching the terminal must see the warning too.
  if (status.lockError) {
    process.stderr.write(
      `lane status: could not take the broker lock within 5s (held by pid ${status.lockError.holderPid ?? 'unknown'}); ` +
        `showing the last persisted state, which may be stale\n`,
    );
  }

  if (json) {
    const text = `${JSON.stringify(status, null, 2)}\n`;
    if (status.capacity == null) {
      process.stderr.write(`lane status: internal error — rendered an empty report; state root ${root}\n`);
      return { exitCode: 1 };
    }
    await writeAndFlush(process.stdout, text);
    return { exitCode: status.lockError ? 1 : 0 };
  }

  const text = `${renderStatusText(status)}\n`;
  if (!text || !text.includes('capacity:')) {
    process.stderr.write(`lane status: internal error — rendered an empty report; state root ${root}\n`);
    return { exitCode: 1 };
  }
  await writeAndFlush(process.stdout, text);
  return { exitCode: status.lockError ? 1 : 0 };
}
