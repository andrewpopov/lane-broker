import fs from 'node:fs';
import { NoProgressWatchdog, NO_PROGRESS_REASON, SILENCE_REASON, formatWindow, noProgressMessage, silenceMessage } from '../no-progress.js';
import { readProcCpuRows } from '../preemptible.js';

export const RUN_LIMIT_REASON = 'run-limit';
export const runLimitMessage = (limitMs) => `lane run: still running after ${formatWindow(limitMs)} — killed (run limit)\n`;

/** The pids of `rootPid` and everything below it in `rows`. */
export function treeOf(rows, rootPid) {
  const childrenOf = new Map();
  for (const row of rows) childrenOf.set(row.ppid, [...(childrenOf.get(row.ppid) ?? []), row.pid]);
  const tree = [];
  for (const queue = [rootPid]; queue.length > 0;) {
    const pid = queue.pop();
    if (tree.includes(pid)) continue;
    tree.push(pid);
    queue.push(...(childrenOf.get(pid) ?? []));
  }
  return tree;
}

/**
 * PKG-397: the agent's own backstop for a pull lane, beside the runner-side supervisor's watchdog (which a hung supervisor never runs).
 * The shared NoProgressWatchdog watches the execution's process tree, with growth of the lane's log as its output; `maxRunMs` is a wall-clock cap.
 * `check()` returns the kill to apply, or null.
 */
export class RunGuard {
  constructor({ pid, startedAt, now, logFile, maxRunMs, noProgressTimeoutMs = 0, silenceTimeoutMs = 0, readRows = readProcCpuRows }) {
    Object.assign(this, { pid, startedAt, now, logFile, maxRunMs, readRows });
    this.rows = [];
    this.logBytes = 0;
    this.watchdog = new NoProgressWatchdog({ timeoutMs: noProgressTimeoutMs, silenceTimeoutMs, readRows: () => this.rows });
    this.watchdog.start();
  }

  check() {
    if (this.now() - this.startedAt >= this.maxRunMs) return { reason: RUN_LIMIT_REASON, runLimitMs: this.maxRunMs, message: runLimitMessage(this.maxRunMs) };
    this.#noteOutput();
    const { watchdog } = this;
    if (watchdog.shouldKillSilent()) return { reason: SILENCE_REASON, silenceTimeoutMs: watchdog.silenceTimeoutMs, message: silenceMessage(watchdog.silenceTimeoutMs) };
    let tree = null;
    try {
      this.rows = this.readRows();
      tree = treeOf(this.rows, this.pid);
    } catch {
      this.rows = [];
    }
    const idleMs = watchdog.observe(tree);
    return watchdog.shouldKill(idleMs) ? { reason: NO_PROGRESS_REASON, noProgressTimeoutMs: watchdog.timeoutMs, message: noProgressMessage(watchdog.timeoutMs) } : null;
  }

  #noteOutput() {
    let size = this.logBytes;
    try {
      size = fs.statSync(this.logFile).size;
    } catch {
      return;
    }
    if (size > this.logBytes) this.watchdog.noteOutput(size - this.logBytes);
    this.logBytes = size;
  }
}
