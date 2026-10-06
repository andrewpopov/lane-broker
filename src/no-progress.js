import { performance } from 'node:perf_hooks';
import { readProcCpuRows } from './preemptible.js';

/**
 * BRAIN-431: a run that makes no progress holds its slot indefinitely (a vitest pool that hung mid-run sat 100 minutes).
 * Every heartbeat is one observation of the lease's tree; an interval is IDLE when the command wrote no output, no process
 * joined or left the tree, and the tree's CPU delta stayed under NO_PROGRESS_CPU_SECONDS_PER_MINUTE scaled to the interval.
 * Anything else, and anything we could not observe, is progress and restarts the streak. The streak's length is the stall
 * signal (always recorded); the kill is opt-in (`noProgressTimeoutMs` > 0) and needs the streak itself to reach it.
 * CPU is per-process cumulative time by identity (src/preemptible.js; on Linux including reaped children's cutime/cstime),
 * never `ps` pcpu, a lifetime average that stays high long after a process went idle. Timing is monotonic.
 */
export const NO_PROGRESS_REASON = 'no-progress';
export const NO_PROGRESS_EXIT = 124;
export const NO_PROGRESS_CPU_SECONDS_PER_MINUTE = 0.05;

const identity = (row) => `${row.pid}:${row.token}`;
const cpuOf = (row) => row.cpuSec + (row.childCpuSec ?? 0);

export class NoProgressWatchdog {
  /** @param timeoutMs streak length that triggers the kill; 0 (or less) records the stall signal but never kills */
  constructor({ timeoutMs, readRows = readProcCpuRows, clock = () => performance.now() }) {
    this.timeoutMs = timeoutMs;
    this.readRows = readRows;
    this.clock = clock;
    this.outputBytes = 0;
    this.baseline = null; // { at, outputBytes, cpu: identity -> cumulative CPU seconds } of the previous observation
    this.idleSince = null;
    this.maxIdleMs = 0;
  }

  get killEnabled() {
    return this.timeoutMs > 0;
  }

  noteOutput(byteCount) {
    this.outputBytes += byteCount;
  }

  /**
   * One observation. `treePids` is the lease's live process set (leader included), or null when the descendant scan
   * failed. Returns the current idle streak in ms (0 after any progress or unobserved gap); the first observation only
   * establishes the baseline and is never idle.
   */
  observe(treePids) {
    const now = this.clock();
    const current = this.#snapshot(treePids);
    const previous = this.baseline;
    this.baseline = current ? { at: now, outputBytes: this.outputBytes, cpu: current } : null;
    if (!current || !previous || !this.#idleBetween(previous, current, now)) {
      this.idleSince = now;
      return 0;
    }
    const idleMs = now - this.idleSince;
    this.maxIdleMs = Math.max(this.maxIdleMs, idleMs);
    return idleMs;
  }

  /** True when the streak has reached the opt-in timeout. */
  shouldKill(idleMs) {
    return this.killEnabled && idleMs >= this.timeoutMs;
  }

  /** identity -> cumulative CPU of every tree member, or null when any of it could not be read. */
  #snapshot(treePids) {
    if (!treePids) return null;
    let rows;
    try {
      rows = this.readRows();
    } catch {
      return null;
    }
    const byPid = new Map(rows.map((r) => [r.pid, r]));
    const cpu = new Map();
    for (const pid of treePids) {
      const row = byPid.get(pid);
      if (!row) return null; // a member we know of has no row: unobserved, not idle
      cpu.set(identity(row), cpuOf(row));
    }
    return cpu;
  }

  #idleBetween(previous, current, now) {
    if (previous.outputBytes !== this.outputBytes) return false;
    if (previous.cpu.size !== current.size) return false;
    let delta = 0;
    for (const [key, cpu] of current) {
      const before = previous.cpu.get(key);
      if (before === undefined) return false; // a new process in the tree is activity
      delta += Math.max(0, cpu - before);
    }
    return delta <= (NO_PROGRESS_CPU_SECONDS_PER_MINUTE / 60) * ((now - previous.at) / 1000);
  }
}

/** "15m", "90s", "1h30m": a window length for the kill message. */
export function formatWindow(ms) {
  const totalSeconds = Math.round(ms / 1000);
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const minutes = Math.round(totalSeconds / 60);
  return minutes < 60 ? `${minutes}m` : `${Math.floor(minutes / 60)}h${minutes % 60 ? `${minutes % 60}m` : ''}`;
}

/** The submitter-facing explanation, identical for a local run and a relayed remote one. */
export const noProgressMessage = (timeoutMs) => `lane run: no progress for ${formatWindow(timeoutMs)} (no output, no CPU) — killed\n`;
