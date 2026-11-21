import fs from 'node:fs';
import { NoProgressWatchdog, NO_PROGRESS_REASON, SILENCE_REASON, formatWindow, noProgressMessage, silenceMessage } from '../no-progress.js';
import { DescendantTracker } from '../descendants.js';
import { readLease } from '../lease.js';

export const RUN_LIMIT_REASON = 'run-limit';
export const runLimitMessage = (limitMs) => `lane run: still running after ${formatWindow(limitMs)} — killed (run limit)\n`;

/** A header value (absent = undefined) overrides the runner's global, and an explicit 0 is the lane's own "off". */
export const windowOf = (fromHeader, fromConfig) => fromHeader ?? fromConfig ?? 0;

/**
 * The command's own process tree, as the supervisor's watchdog samples it: the command leader the lease names, plus every descendant or
 * lease-marked process recorded BY IDENTITY, so a worker that daemonised and reparented to PID 1 stays in. The supervisor and broker
 * housekeeping are not in it. Null while the lease is not readable (an unobserved interval, never an idle one).
 */
export class WorkloadTree {
  #tracker = null;

  /** `readLeaseId` is asked until it answers: the lane id is written only once the runner's supervisor has enqueued. */
  constructor({ root, readLeaseId }) {
    Object.assign(this, { root, readLeaseId });
  }

  pids() {
    if (this.#tracker === null) {
      const leaseId = this.readLeaseId();
      const lease = leaseId ? readLease(this.root, leaseId) : null;
      if (!lease?.childPgid) return null;
      this.#tracker = DescendantTracker.fromSnapshot(lease.childPgid, lease.descendants, { leaseId });
    }
    const rows = this.#tracker.scan();
    return rows ? [this.#tracker.rootPid, ...this.#tracker.live(rows)] : null;
  }
}

/**
 * PKG-397: the agent's own backstop for a pull lane, beside the runner-side supervisor's watchdog (which a hung supervisor never runs).
 * The shared NoProgressWatchdog watches the workload tree, with growth of the lane's log as its output; `maxRunMs` is a wall-clock cap.
 * `check()` returns the kill to apply, or null. The CPU scan is shared by the caller and taken only when `needsCpu`.
 */
export class RunGuard {
  constructor({ startedAt, now, logFile, maxRunMs, noProgressTimeoutMs, silenceTimeoutMs, tree }) {
    Object.assign(this, { startedAt, now, logFile, maxRunMs, tree });
    this.logBytes = 0;
    this.cpuRows = [];
    this.watchdog = new NoProgressWatchdog({ timeoutMs: noProgressTimeoutMs, silenceTimeoutMs, readRows: () => this.cpuRows });
    this.watchdog.start();
  }

  get needsCpu() {
    return this.watchdog.killEnabled;
  }

  check(cpuRows) {
    if (this.now() - this.startedAt >= this.maxRunMs) return { reason: RUN_LIMIT_REASON, runLimitMs: this.maxRunMs, message: runLimitMessage(this.maxRunMs) };
    this.#noteOutput();
    const { watchdog } = this;
    if (watchdog.shouldKillSilent()) return { reason: SILENCE_REASON, silenceTimeoutMs: watchdog.silenceTimeoutMs, message: silenceMessage(watchdog.silenceTimeoutMs) };
    if (!this.needsCpu) return null;
    this.cpuRows = cpuRows ?? [];
    const idleMs = watchdog.observe(cpuRows ? this.tree.pids() : null);
    return watchdog.shouldKill(idleMs) ? { reason: NO_PROGRESS_REASON, noProgressTimeoutMs: watchdog.timeoutMs, message: noProgressMessage(watchdog.timeoutMs) } : null;
  }

  #noteOutput() {
    let size;
    try {
      size = fs.statSync(this.logFile).size;
    } catch {
      return;
    }
    if (size > this.logBytes) this.watchdog.noteOutput(size - this.logBytes);
    this.logBytes = size;
  }
}
