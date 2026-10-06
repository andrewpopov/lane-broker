import { readProcCpuRows } from './preemptible.js';

/**
 * BRAIN-431: a run that makes no progress holds its slot forever (a vitest worker pool that hung mid-run sat 100 minutes).
 * "No progress" means BOTH hold over the whole timeout window: the command wrote 0 bytes of output, and its process tree
 * used under NO_PROGRESS_CPU_SECONDS_PER_MINUTE of CPU. CPU is read as per-process cumulative-time deltas
 * (src/preemptible.js), never `ps` pcpu, which is a lifetime average that stays high long after a process went idle.
 */
export const NO_PROGRESS_REASON = 'no-progress';
export const NO_PROGRESS_EXIT = 124;
export const NO_PROGRESS_CPU_SECONDS_PER_MINUTE = 0.05;

const identity = (row) => `${row.pid}:${row.token}`;

export class NoProgressWatchdog {
  /** @param timeoutMs window length; 0 (or less) disables the watchdog */
  constructor({ timeoutMs, now = Date.now(), readRows = readProcCpuRows }) {
    this.timeoutMs = timeoutMs;
    this.readRows = readRows;
    this.outputBytes = 0;
    this.samples = [{ at: now, cpuSeconds: 0, outputBytes: 0 }];
    this.cumulativeCpu = 0;
    this.previous = null; // identity -> cumulative CPU seconds at the previous read
  }

  get enabled() {
    return this.timeoutMs > 0;
  }

  noteOutput(byteCount) {
    this.outputBytes += byteCount;
  }

  /**
   * One heartbeat tick. `treePids` are the lease's live processes (leader included). True when the run has shown no
   * output and negligible CPU for at least `timeoutMs`. An unreadable process table is never "idle".
   */
  stalled(treePids, now = Date.now()) {
    if (!this.enabled) return false;
    const last = this.samples[this.samples.length - 1];
    if (this.outputBytes !== last.outputBytes) return this.#restart(now);
    let rows;
    try {
      rows = this.readRows();
    } catch {
      return this.#restart(now);
    }
    const members = new Set(treePids);
    const current = new Map();
    let delta = 0;
    for (const row of rows) {
      if (!members.has(row.pid)) continue;
      const key = identity(row);
      current.set(key, row.cpuSec);
      if (this.previous) delta += Math.max(0, row.cpuSec - (this.previous.get(key) ?? 0));
    }
    this.previous = current;
    this.cumulativeCpu += delta;
    this.samples.push({ at: now, cpuSeconds: this.cumulativeCpu, outputBytes: this.outputBytes });
    // keep exactly one sample at or before the window start: the baseline the window is measured against
    while (this.samples.length > 1 && this.samples[1].at <= now - this.timeoutMs) this.samples.shift();
    const base = this.samples[0];
    if (base.at > now - this.timeoutMs) return false;
    const minutes = (now - base.at) / 60_000;
    return this.cumulativeCpu - base.cpuSeconds < NO_PROGRESS_CPU_SECONDS_PER_MINUTE * minutes;
  }

  /** Output or an unreadable table: begin a fresh window; always "not stalled". */
  #restart(now) {
    this.samples = [{ at: now, cpuSeconds: this.cumulativeCpu, outputBytes: this.outputBytes }];
    this.previous = null;
    return false;
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
