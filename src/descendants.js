import fs from 'node:fs';
import { execFileSync } from 'node:child_process';

/**
 * BRAIN-419: a lane command can start servers that leave the leader's process group (Playwright's `webServer` via
 * turbo/pnpm calls setsid), so killing the group alone strands them. The supervisor records every process it sees
 * descended from the leader while the lease runs, each with a start token, and reaps the ones still alive with the
 * SAME token at cancel / leader exit. A recorded pid whose token changed was reused by an unrelated process.
 */

/** Linux: /proc/<pid>/stat -> { pid, ppid, token = starttime (field 22) }. comm may hold spaces/parens, so split after the last ')'. */
export function parseProcStat(pid, text) {
  const close = text.lastIndexOf(')');
  if (close < 0) return null;
  const fields = text.slice(close + 2).split(' '); // fields[0] is state (field 3)
  const ppid = Number(fields[1]);
  const token = fields[19]; // field 22
  if (!Number.isFinite(ppid) || !token) return null;
  return { pid, ppid, token };
}

/** macOS/BSD: `ps -A -o pid=,ppid=,lstart=` lines; lstart is "Mon Oct  6 12:00:00 2026", used verbatim as the token. */
export function parsePsTable(text) {
  const rows = [];
  for (const line of String(text ?? '').split('\n')) {
    const m = /^\s*(\d+)\s+(\d+)\s+(\S.*\S)\s*$/.exec(line);
    if (m) rows.push({ pid: Number(m[1]), ppid: Number(m[2]), token: m[3].replace(/\s+/g, ' ') });
  }
  return rows;
}

/** Whole process table as [{ pid, ppid, token }]; throws when it cannot be read (callers treat that as "unknown", not "empty"). */
export function readProcessTable() {
  if (process.platform === 'linux') {
    const rows = [];
    for (const name of fs.readdirSync('/proc')) {
      if (!/^\d+$/.test(name)) continue;
      try {
        const row = parseProcStat(Number(name), fs.readFileSync(`/proc/${name}/stat`, 'utf8'));
        if (row) rows.push(row);
      } catch {
        // exited between readdir and read
      }
    }
    return rows;
  }
  const out = execFileSync('ps', ['-A', '-o', 'pid=,ppid=,lstart='], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
    timeout: 2000,
    env: { ...process.env, TZ: 'UTC0', LC_ALL: 'C', LC_TIME: 'C' },
  });
  return parsePsTable(out);
}

const sleepMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class DescendantTracker {
  /**
   * @param rootPid the leader's pid; it is walked from but never itself recorded (its group is killed as before)
   * @param protectedPids pids that must never be recorded or killed (the supervisor and its ancestors)
   */
  constructor(rootPid, { readTable = readProcessTable, protectedPids = [] } = {}) {
    this.rootPid = rootPid;
    this.readTable = readTable;
    this.protectedPids = new Set(protectedPids);
    this.recorded = new Map(); // pid -> token
    this.rootToken = null; // learned on the first scan that sees the leader, so a reused leader pid is never walked
  }

  /** Record every process currently descended from the leader OR from an already-recorded, still-live member (covers a
   *  member that was reparented to init after the leader exited). Returns the table read, or null if unreadable. */
  scan() {
    let rows;
    try {
      rows = this.readTable();
    } catch {
      return null;
    }
    const childrenOf = new Map();
    for (const r of rows) childrenOf.set(r.ppid, [...(childrenOf.get(r.ppid) ?? []), r]);
    const tokenOf = new Map(rows.map((r) => [r.pid, r.token]));
    if (this.rootToken === null && this.rootPid != null) this.rootToken = tokenOf.get(this.rootPid) ?? null;
    const stack = this.rootToken !== null && tokenOf.get(this.rootPid) === this.rootToken ? [this.rootPid] : [];
    for (const [pid, token] of this.recorded) if (tokenOf.get(pid) === token) stack.push(pid);
    const seen = new Set(stack);
    while (stack.length > 0) {
      for (const child of childrenOf.get(stack.pop()) ?? []) {
        if (seen.has(child.pid) || this.protectedPids.has(child.pid)) continue;
        seen.add(child.pid);
        stack.push(child.pid);
        if (!this.recorded.has(child.pid)) this.recorded.set(child.pid, child.token);
      }
    }
    return rows;
  }

  /** Recorded pids still running with the token they were recorded under. */
  live(rows = this.readTable()) {
    const tokenOf = new Map(rows.map((r) => [r.pid, r.token]));
    return [...this.recorded].filter(([pid, token]) => tokenOf.get(pid) === token).map(([pid]) => pid);
  }

  /** Serializable form for the lease, so `lane cancel` of an orphaned lease can reap them without this supervisor. */
  snapshot() {
    return [...this.recorded].map(([pid, token]) => ({ pid, token }));
  }

  static fromSnapshot(rootPid, snapshot, options) {
    const tracker = new DescendantTracker(rootPid, options);
    for (const { pid, token } of snapshot ?? []) tracker.recorded.set(pid, token);
    return tracker;
  }

  /**
   * TERM every live recorded descendant, wait up to graceMs, KILL survivors, wait for them to be gone.
   * Returns how many were alive when TERMed. An unreadable process table reaps nothing (never guess).
   */
  async reap({ graceMs, kill = process.kill.bind(process), sleep = sleepMs } = {}) {
    this.scan();
    const alive = () => {
      try {
        return this.live();
      } catch {
        return [];
      }
    };
    const targets = alive();
    const signalAll = (signal) => {
      for (const pid of alive()) {
        try {
          kill(pid, signal);
        } catch {
          // gone
        }
      }
    };
    signalAll('SIGTERM');
    const deadline = Date.now() + graceMs;
    while (Date.now() < deadline && alive().length > 0) await sleep(100);
    if (alive().length > 0) {
      signalAll('SIGKILL');
      const killDeadline = Date.now() + 5000;
      while (Date.now() < killDeadline && alive().length > 0) await sleep(50);
    }
    return targets.length;
  }
}
