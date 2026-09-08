import fs from 'node:fs';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { paths, atomicWriteJson, readJsonSafe } from './state.js';

/**
 * Test-only override, the CPU-gate equivalent of load.js's readLoadAvg /
 * LANE_BROKER_LOADAVG_FILE: when LANE_BROKER_CPU_BUSY_FILE is set, its first
 * line is "hostBusyCores,cores" and sampleHostCpu returns that directly
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
    const [busyStr, coresStr] = first.split(',');
    const hostBusyCores = Number(busyStr);
    const cores = Number(coresStr);
    if (Number.isFinite(hostBusyCores) && Number.isFinite(cores) && cores > 0) {
      return { hostBusyCores, cores, stale: false, sampledAt: Date.now() };
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
export function sampleHostCpu(root, cpus = os.cpus()) {
  const override = readCpuBusyOverride();
  if (override) return override;
  const file = paths(root).cpuSample;
  const prev = readJsonSafe(file);
  const now = Date.now();
  const snapshot = { at: now, cpus: cpus.map(cpuTimes) };
  try {
    const latest = readJsonSafe(file);
    if (!latest || !Number.isFinite(latest.at) || latest.at < snapshot.at) {
      atomicWriteJson(file, snapshot);
    }
  } catch {
    // best-effort: a failed sidecar write must never abort admission
  }
  const { hostBusyCores, stale } = computeBusyCores(prev, snapshot);
  return { hostBusyCores, cores: cpus.length, stale, sampledAt: now };
}

/**
 * Best-effort available-memory + (on macOS) memory-pressure reading, for a
 * fail-safe only (ZIRK scheduler phase 1 does not gate admission on this
 * yet — see src/admission.js). Never throws: a failed pressure probe
 * reports `null`, exactly like a missing CPU sample does elsewhere here.
 */
export function readMemoryInfo(exec = execFileSync) {
  const availableBytes = os.freemem();
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
  return { availableBytes, macPressure };
}

/**
 * Pure parse of `ps -o %cpu=`'s raw text into a cores-busy figure, split out
 * from observedGroupCpuCores below so it's unit-testable without shelling
 * out. `Number('')` is `0`, so a blank/whitespace-only line (there's always
 * at least a trailing newline, and `ps` can emit blank rows) must be
 * filtered out BEFORE the Number() coercion, not after — otherwise it reads
 * as a real, valid zero-percent process instead of "nothing usable here",
 * and a probe that returned no rows at all would wrongly report 0 (a
 * legitimate reading) rather than null (no observation).
 */
export function parseGroupCpuOutput(text) {
  const values = text
    .split('\n')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .map(Number)
    .filter((n) => Number.isFinite(n));
  if (values.length === 0) return null;
  return values.reduce((sum, v) => sum + v, 0) / 100;
}

/**
 * Best-effort observed CPU (in cores) for one lease's process group, summing
 * `ps`'s %cpu across every process in the group. Returns null (never
 * throws) when the pgid is unknown, the probe itself fails, or the probe
 * returns nothing usable — the same fail-safe shape as processStartTime()
 * in state.js. Wired into the supervisor heartbeat (BRAIN-207): telemetry
 * only, folded into leaseDemand's max(observed, cold) in src/admission.js.
 */
export function observedGroupCpuCores(pgid, exec = execFileSync) {
  if (!pgid) return null;
  try {
    // 2s bound (Codex pre-merge review): this runs synchronously INSIDE the
    // supervisor heartbeat, so a hung `ps` must never be able to stall
    // heartbeats/cancellation indefinitely — a timeout is treated exactly
    // like any other probe failure (null, same as an unknown pgid).
    const out = exec('ps', ['-o', '%cpu=', '-g', String(pgid)], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 2000,
    });
    return parseGroupCpuOutput(out);
  } catch {
    return null;
  }
}
