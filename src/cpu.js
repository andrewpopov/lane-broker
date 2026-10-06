import fs from 'node:fs';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { paths, atomicWriteJson, readJsonSafe } from './state.js';
import { detectResourceCapacity } from './resources.js';
import { readProcCpuRows, procSnapshot, preemptibleCores } from './preemptible.js';

/**
 * Test-only override, the CPU-gate equivalent of load.js's readLoadAvg /
 * LANE_BROKER_LOADAVG_FILE: when LANE_BROKER_CPU_BUSY_FILE is set, its first
 * line is "hostBusyCores,cores[,preemptibleBusyCores]" and sampleHostCpu returns that directly
 * instead of diffing real os.cpus() snapshots. Real integration tests spawn
 * an actual detached supervisor (a separate process), so there is no way to
 * inject a fake cpuSampler function across that boundary — this lets the
 * CPU gate's hysteresis be driven deterministically end-to-end the same way
 * the load gate already is.
 */
function readCpuBusyOverride() {
  const file = process.env.LANE_BROKER_CPU_BUSY_FILE;
  if (!file) return null;
  try {
    const first = fs.readFileSync(file, 'utf8').split('\n')[0].trim();
    const [busyStr, coresStr, preemptibleStr] = first.split(',');
    const hostBusyCores = Number(busyStr);
    const cores = Number(coresStr);
    const preemptibleBusyCores = preemptibleStr === undefined ? 0 : Number(preemptibleStr);
    if (Number.isFinite(hostBusyCores) && Number.isFinite(cores) && cores > 0 && Number.isFinite(preemptibleBusyCores)) {
      return { hostBusyCores, preemptibleBusyCores, cores, stale: false, sampledAt: Date.now() };
    }
  } catch {
    // fall through to the real sampler
  }
  return null;
}

function cpuTimes(cpu) {
  // A missing/malformed `times` field yields NaN rather than throwing —
  // computeBusyCores below rejects any non-finite entry before using it, so
  // this never fabricates a number, it just can't produce a real one.
  const t = cpu && cpu.times;
  if (!t) return { idle: NaN, total: NaN };
  return { idle: t.idle, total: t.user + t.nice + t.sys + t.idle + t.irq };
}

// A delta averaged over more than this masks a real, live spike (or a dip)
// instead of reflecting it — e.g. a snapshot from hours ago that happens to
// still have the same core count and advancing counters. Codex review
// finding #3: without this, computeBusyCores treated any old-but-plausible
// snapshot as fresh.
const MAX_SAMPLE_GAP_MS = 60_000;

/**
 * Pure delta arithmetic between two os.cpus()-shaped snapshots (each
 * `{ at, cpus: [{idle, total}, ...] }`). Busy cores = sum over cores of
 * (totalDelta - idleDelta) / totalDelta — a fraction-of-a-core-busy figure
 * per core, independent of wall-clock drift between the two samples.
 *
 * Reports `stale: true` (hostBusyCores: null) whenever the delta can't be
 * trusted: no previous snapshot, a core-count change, a gap longer than
 * MAX_SAMPLE_GAP_MS, a non-finite/malformed per-core entry (Codex review
 * finding #5 — a valid-but-garbled persisted snapshot must never produce a
 * silent NaN that then compares as "always admits"), or every core's
 * total-time counter failing to advance (two samples taken back-to-back, or
 * a clock anomaly) — never a divide-by-zero or a fabricated number.
 */
export function computeBusyCores(prev, snapshot) {
  if (
    !prev ||
    !snapshot ||
    !Array.isArray(prev.cpus) ||
    !Array.isArray(snapshot.cpus) ||
    prev.cpus.length !== snapshot.cpus.length
  ) {
    return { hostBusyCores: null, stale: true };
  }
  if (!Number.isFinite(prev.at) || !Number.isFinite(snapshot.at)) {
    return { hostBusyCores: null, stale: true };
  }
  const elapsed = snapshot.at - prev.at;
  // Zero elapsed is a legitimate reading (Date.now() has ~1ms resolution;
  // two calls can land in the same millisecond), so only a NEGATIVE gap
  // (clock went backwards) or one past MAX_SAMPLE_GAP_MS is rejected here.
  if (elapsed < 0 || elapsed > MAX_SAMPLE_GAP_MS) {
    return { hostBusyCores: null, stale: true };
  }
  let busySum = 0;
  let sawDelta = false;
  for (let i = 0; i < snapshot.cpus.length; i += 1) {
    const p = prev.cpus[i];
    const s = snapshot.cpus[i];
    if (!p || !s || !Number.isFinite(p.total) || !Number.isFinite(p.idle) || !Number.isFinite(s.total) || !Number.isFinite(s.idle)) {
      continue; // malformed entry: skip it rather than propagate a NaN
    }
    const totalDelta = s.total - p.total;
    const idleDelta = s.idle - p.idle;
    if (totalDelta <= 0) continue; // clock skew, or no progress since the last sample
    sawDelta = true;
    busySum += (totalDelta - idleDelta) / totalDelta;
  }
  if (!sawDelta) return { hostBusyCores: null, stale: true };
  return { hostBusyCores: busySum, stale: false };
}

/**
 * Sample host busy cores from the delta between this call's os.cpus() and
 * the previous one, persisted in a sidecar so successive polls can
 * difference them. NEVER sleeps to collect a second sample itself — each
 * `tryStart` poll (already on its own ~sampleMs cadence) supplies the next
 * point, so this never holds the global lock waiting on time to pass.
 *
 * This IS a read-then-write transaction on shared state — cpu-sample.json —
 * called deliberately OUTSIDE the global lock (see scheduler.js's tryStart)
 * because the measured lock-hold cost of including it was not worth paying
 * while this whole predicate is telemetry-only (schedulerMode: 'shadow').
 * That means two supervisors CAN race this: both read the same `prev`, and
 * without the guard below could commit snapshots out of chronological
 * order, regressing the persisted baseline and corrupting a later caller's
 * delta. The write is made monotonic against that: immediately before
 * committing, re-read whatever is currently on disk and skip the write if
 * it already holds a snapshot at least as new as ours. This narrows the
 * race to the (much smaller) gap between that re-read and the write itself
 * — it does not eliminate it. If schedulerMode ever moves to 'active', this
 * sample and its write need to move under the same lock as the CPU gate
 * update (evaluateNewAdmission), the same way the load gate already is.
 * The write itself stays best-effort (Codex review finding #1): a
 * permissions error, full disk, or any other failure here must never make
 * this throw or abort admission.
 */
/**
 * BRAIN-346: true only for a VALIDATED read whose counters did not move at all since `prev`: the
 * same raw core count, and every core's total and idle counters exactly equal. `===` is false for
 * NaN, so a malformed entry never qualifies, and a regressing counter (clock skew, reset) never
 * does either. Only this shape may reuse the last measurement; every other stale reading
 * (malformed, regressing, topology change, too old, no baseline) stays unavailable.
 */
function countersUnchanged(prev, snapshot) {
  if (!prev || !Array.isArray(prev.cpus) || prev.cpus.length === 0 || prev.cpus.length !== snapshot.cpus.length) return false;
  if (!Number.isFinite(prev.at) || snapshot.at - prev.at < 0 || snapshot.at - prev.at > MAX_SAMPLE_GAP_MS) return false;
  return snapshot.cpus.every((s, i) => prev.cpus[i] && prev.cpus[i].total === s.total && prev.cpus[i].idle === s.idle);
}

/**
 * `preemptible` (BRAIN-428, src/preemptible.js): `{ laneNice, niceMin, heldLeases }`, absent = nothing preemptible.
 * The per-process readings ride in the same sidecar as the host counters, so both deltas cover the SAME window.
 */
export function sampleHostCpu(root, cpus = os.cpus(), { reuseWindowMs = 0, preemptible = null, readProcs = readProcCpuRows } = {}) {
  const override = readCpuBusyOverride();
  if (override) return override;
  const file = paths(root).cpuSample;
  const prev = readJsonSafe(file);
  const now = Date.now();
  const snapshot = { at: now, cpus: cpus.map(cpuTimes) };
  const { hostBusyCores, stale } = computeBusyCores(prev, snapshot);
  let procRows = null;
  if (preemptible && preemptible.niceMin > 0) {
    try {
      procRows = readProcs();
    } catch {
      procRows = null; // unreadable table: nothing is counted as preemptible
    }
    snapshot.procs = procSnapshot(procRows);
  }
  const capacity = detectResourceCapacity({ parallelism: cpus.length });
  const measured = Number.isFinite(hostBusyCores) ? Math.min(hostBusyCores, capacity.cpuCores) : hostBusyCores;
  // BRAIN-346: the head and a backfill candidate now sample back to back, and os.cpus() counters
  // advance in coarse ticks, so the second poll often sees no advance and reads "stale" — which
  // admission treats as "unavailable, admit on an idle broker". Carry the last VALID measurement
  // in the sidecar and reuse it while it is younger than reuseWindowMs, flagged `reused` so the
  // CPU gate's hysteresis does not count it as a new observation.
  // It survives ONLY a legitimate unchanged-counter read that follows a valid measurement. Any
  // other stale read (malformed, regressing, topology change, no baseline) drops it: that read is
  // about to become the persisted baseline, and carrying the old measurement over it would let a
  // repeat of the same bad counters match `countersUnchanged` and reuse a pre-fault figure.
  const unchanged = stale && countersUnchanged(prev, snapshot);
  const preemptibleRaw = !stale && preemptible ? preemptibleCores({ rows: procRows, prevProcs: prev?.procs, windowMs: now - prev.at, ...preemptible }) : 0;
  const preemptibleBusyCores = Number.isFinite(measured) ? Math.min(preemptibleRaw, measured) : 0;
  const lastValid = !stale && Number.isFinite(measured) ? { hostBusyCores: measured, preemptibleBusyCores, cores: capacity.cpuCores, at: now } : unchanged ? prev.lastValid : undefined;
  if (lastValid) snapshot.lastValid = lastValid;
  try {
    const latest = readJsonSafe(file);
    if (!latest || !Number.isFinite(latest.at) || latest.at < snapshot.at) {
      atomicWriteJson(file, snapshot);
    }
  } catch {
    // best-effort: a failed sidecar write must never abort admission
  }
  const reusable =
    unchanged &&
    prev.lastValid &&
    Number.isFinite(prev.lastValid.hostBusyCores) &&
    prev.lastValid.cores === capacity.cpuCores &&
    Number.isFinite(prev.lastValid.at) &&
    now - prev.lastValid.at >= 0 &&
    now - prev.lastValid.at < reuseWindowMs;
  if (reusable) {
    return { hostBusyCores: prev.lastValid.hostBusyCores, preemptibleBusyCores: prev.lastValid.preemptibleBusyCores ?? 0, cores: capacity.cpuCores, stale: false, reused: true, sampledAt: now, source: capacity.source };
  }
  return {
    hostBusyCores: measured,
    preemptibleBusyCores,
    cores: capacity.cpuCores,
    stale,
    sampledAt: now,
    source: capacity.source,
  };
}

/**
 * Best-effort available-memory + (on macOS) memory-pressure reading, for a
 * fail-safe only (ZIRK scheduler phase 1 does not gate admission on this
 * yet — see src/admission.js). Never throws: a failed pressure probe
 * reports `null`, exactly like a missing CPU sample does elsewhere here.
 */
export function readMemoryInfo(exec = execFileSync) {
  const capacity = detectResourceCapacity();
  const availableBytes = capacity.availableMemoryBytes;
  let macPressure = null;
  if (process.platform === 'darwin') {
    try {
      // 2s bound (Codex pre-merge review): this now runs INSIDE tryStart's
      // global lock (see scheduler.js), so a hung `sysctl` must never be
      // able to hold the lock open indefinitely — a timeout is treated
      // exactly like any other probe failure (macPressure stays null).
      const out = exec('sysctl', ['-n', 'kern.memorystatus_vm_pressure_level'], {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
        timeout: 2000,
      }).trim();
      const level = Number(out);
      if (level === 1) macPressure = 'normal';
      else if (level === 2) macPressure = 'warn';
      else if (level === 4) macPressure = 'critical';
      else if (Number.isFinite(level)) macPressure = `level-${level}`;
    } catch {
      macPressure = null; // sysctl missing/failed: never let this crash a poll
    }
  }
  return { availableBytes, totalBytes: capacity.memoryBytes, macPressure, source: capacity.source };
}

/** Parse `ps -A -o pid=,ppid=,pgid=,pcpu=,rss=` text into rows; unparseable lines are dropped. */
export function parseProcessTable(text) {
  const rows = [];
  for (const line of String(text ?? '').split('\n')) {
    const f = line.trim().split(/\s+/).map(Number);
    if (f.length !== 5 || !f.every(Number.isFinite)) continue;
    const [pid, ppid, pgid, pcpu, rss] = f;
    rows.push({ pid, ppid, pgid, pcpu, rss });
  }
  return rows;
}

/**
 * BRAIN-353: a lease's processes are its whole process group PLUS every
 * descendant (ppid walk) of any member. A command that spawns workers with
 * `detached: true` puts them in NEW process groups, so a pgid-only sum sees ~0
 * while they burn cores. Processes whose pid is in stopPids or whose pgid is in
 * stopPgids (other leases' supervisors and child groups, for a nested `lane run`)
 * are never selected nor descended into, so a nested lease is not counted twice;
 * the lease's own pgid is never a stop. Returns { cores, memoryBytes }, or null when no
 * process is in the group (unknown/dead id) — never a fabricated zero.
 */
export function selectLeaseTree(rows, pgid, { stopPids = new Set(), stopPgids = new Set() } = {}) {
  const stopped = (r) => stopPids.has(r.pid) || (r.pgid !== pgid && stopPgids.has(r.pgid));
  const selected = new Set(rows.filter((r) => r.pgid === pgid && !stopped(r)).map((r) => r.pid));
  if (selected.size === 0) return null;
  const childrenOf = new Map();
  for (const r of rows) if (!stopped(r)) childrenOf.set(r.ppid, [...(childrenOf.get(r.ppid) ?? []), r.pid]);
  const stack = [...selected];
  while (stack.length > 0) {
    for (const child of childrenOf.get(stack.pop()) ?? []) {
      if (!selected.has(child)) {
        selected.add(child);
        stack.push(child);
      }
    }
  }
  let pcpu = 0;
  let rssKib = 0;
  for (const r of rows) {
    if (!selected.has(r.pid)) continue;
    pcpu += Math.max(0, r.pcpu);
    rssKib += Math.max(0, r.rss);
  }
  return { cores: pcpu / 100, memoryBytes: rssKib * 1024 };
}

/**
 * Best-effort observed CPU (cores) and RSS (bytes) of one lease's whole
 * descendant tree from a single `ps` snapshot. Null (never throws) when the id
 * is unknown, the probe fails, or no process belongs to it. 2s bound: this runs
 * synchronously INSIDE the supervisor heartbeat, so a hung `ps` must never
 * stall heartbeats/cancellation — a timeout is just another probe failure.
 * Telemetry only (BRAIN-207), folded into leaseDemand in src/admission.js.
 */
export function observeLeaseTree(pgid, { stopPids, stopPgids } = {}, exec = execFileSync) {
  if (!pgid) return null;
  try {
    const out = exec('ps', ['-A', '-o', 'pid=,ppid=,pgid=,pcpu=,rss='], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 2000,
    });
    return selectLeaseTree(parseProcessTable(out), Number(pgid), { stopPids, stopPgids });
  } catch {
    return null;
  }
}

export function observedGroupCpuCores(pgid, exec = execFileSync) {
  return observeLeaseTree(pgid, {}, exec)?.cores ?? null;
}

export function observedGroupMemoryBytes(pgid, exec = execFileSync) {
  return observeLeaseTree(pgid, {}, exec)?.memoryBytes ?? null;
}
