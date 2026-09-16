import fs from 'node:fs';
import path from 'node:path';
import { ensureStateDirs, paths, withLock, bootId, readJsonSafe } from './state.js';
import { listLeases, reapAll, LEASE_STATE } from './lease.js';
import { listQueue, HELD_STATES, blockedBy, readSkipState, readCapacitySkipState } from './scheduler.js';
import { readGateState } from './load.js';
import { readMemorySample, classifyMemorySample } from './memory.js';
import { loadGlobalConfig } from './config.js';
import { detectResourceCapacity, effectiveWeightCapacity, leaseResources } from './resources.js';

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
function computeHeadBlock(root, cfg, queue, leases, now) {
  const head = queue[0];
  if (!head) return null;
  const held = leases.filter((l) => HELD_STATES.has(l.state));
  const runningWeight = held.reduce((s, l) => s + (l.weight || 0), 0);
  const blocker = blockedBy(held, head);
  if (blocker) {
    const skipState = readSkipState(root);
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
  if (runningWeight + head.weight > cfg.capacity) {
    const capState = readCapacitySkipState(root);
    const sameHead = capState.headId === head.id;
    const skipCount = sameHead ? capState.count : 0;
    if (skipCount < cfg.conflictSkipLimit) return null;
    return {
      kind: 'capacity',
      headId: head.id,
      headWeight: head.weight,
      runningWeight,
      capacity: cfg.capacity,
      skipCount,
      skipLimit: cfg.conflictSkipLimit,
    };
  }
  return null;
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
  const queue = listQueue(root);
  const gate = readGateState(root);
  const p = paths(root);
  const paused = fs.existsSync(p.pause) ? fs.readFileSync(p.pause, 'utf8').trim() : null;
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
      log: l.logPath,
      weight: l.weight,
      // BRAIN-255: default 1 for a lease written before this field existed,
      // matching resolveTicketConfig's own compatibility default.
      maxConcurrent: l.maxConcurrent ?? 1,
      resources: leaseResources(l, cfg),
      observedCpuCores: l.observedCpuCores ?? null,
      observedMemoryBytes: l.observedMemoryBytes ?? null,
    }));

  const runningWeight = leases.filter((l) => HELD_STATES.has(l.state)).reduce((s, l) => s + (l.weight || 0), 0);
  const held = leases.filter((l) => HELD_STATES.has(l.state));
  const reservedCpuCores = held.reduce((sum, lease) => sum + leaseResources(lease, cfg).cpuCores, 0);
  const reservedMemoryBytes = held.reduce((sum, lease) => sum + leaseResources(lease, cfg).memoryBytes, 0);

  const queued = queue.map((t, i) => ({
    id: t.id,
    key: t.key,
    position: i + 1,
    waitedMs: now - t.createdAt,
    resources: leaseResources(t, cfg),
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
    // BRAIN-249: null unless the queue is genuinely stalled (a conflict-
    // blocked head whose skip allowance is exhausted) — see computeHeadBlock.
    headBlock: computeHeadBlock(root, cfg, queue, leases, now),
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
  lines.push(renderMemoryLine(status.memory));
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
      const flag = r.state === 'ORPHANED' ? ' [ORPHANED]' : '';
      // BRAIN-255: only shown once it's non-default, so "2 running" reads
      // differently against a maxConcurrent: 8 lane than a plain exclusive
      // one, without cluttering the common (ceiling 1) case.
      const ceiling = r.maxConcurrent > 1 ? `  ceiling=${r.maxConcurrent}` : '';
      lines.push(
        `  ${r.id}  key=${r.key}  pid=${r.pid ?? '-'}  elapsed=${fmtMs(r.elapsedMs)}  ` +
          `heartbeat-age=${fmtMs(r.heartbeatAgeMs)}  log=${r.log}${flag}${ceiling}`,
      );
    }
  }
  lines.push('');
  lines.push('QUEUE (FIFO):');
  if (status.queued.length === 0) {
    lines.push('  (none)');
  } else {
    for (const q of status.queued) {
      lines.push(`  #${q.position} ${q.id}  key=${q.key}  waited=${fmtMs(q.waitedMs)}`);
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
