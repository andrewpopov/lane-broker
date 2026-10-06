import fs from 'node:fs';
import { execFileSync } from 'node:child_process';

/**
 * BRAIN-419: a lane command can start servers that leave the leader's process group (Playwright's `webServer` via
 * turbo/pnpm calls setsid), so killing the group alone strands them. The supervisor records every process it sees
 * in the lease's tree while the lease runs, each with a start token, and reaps the ones still alive with the SAME
 * token at cancel / leader exit. A recorded pid whose token changed was reused by an unrelated process.
 *
 * A process is in the tree when it descends from the leader (ppid walk), descends from a recorded member, or carries
 * `LANE_BROKER_LEASE=<lease id>` in its environment (inherited from the leader, so it survives reparenting to init).
 */

const CLK_TCK = 100; // USER_HZ: fixed at 100 on every Linux ABI userspace sees
const PAGE_KIB = 4;
const READ_ATTEMPTS = 3;
const KILL_WAIT_MS = 3000;

function leaseMarker(leaseId) {
  return `LANE_BROKER_LEASE=${leaseId}`;
}

/** Linux: /proc/<pid>/stat. comm may hold spaces/parens, so split after the last ')'. `uptimeSec` turns starttime into pcpu like ps does. */
export function parseProcStat(pid, text, uptimeSec = 0) {
  const close = text.lastIndexOf(')');
  if (close < 0) return null;
  const f = text.slice(close + 2).split(' '); // f[0] is state (field 3)
  const ppid = Number(f[1]);
  const pgid = Number(f[2]);
  const token = f[19]; // field 22, starttime in ticks
  if (![ppid, pgid].every(Number.isFinite) || !token) return null;
  const cpuSec = (Number(f[11]) + Number(f[12])) / CLK_TCK; // utime + stime
  const elapsed = uptimeSec - Number(token) / CLK_TCK;
  const pcpu = elapsed > 0 && Number.isFinite(cpuSec) ? (cpuSec / elapsed) * 100 : 0;
  return { pid, ppid, pgid, pcpu, rss: (Number(f[21]) || 0) * PAGE_KIB, token, marked: false };
}

const PS_LINE = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+(\d+)\s+(\w{3}\s+\w{3}\s+\d+\s+[\d:]{8}\s+\d{4})\s*(.*)$/;

/** macOS: `ps -A -E -ww -o pid=,ppid=,pgid=,pcpu=,rss=,lstart=,command=`; with -E the command column carries the environment. */
export function parsePsTable(text, leaseId) {
  const rows = [];
  for (const line of String(text ?? '').split('\n')) {
    const m = PS_LINE.exec(line);
    if (!m) continue;
    rows.push({
      pid: Number(m[1]),
      ppid: Number(m[2]),
      pgid: Number(m[3]),
      pcpu: Number(m[4]),
      rss: Number(m[5]),
      token: m[6].replace(/\s+/g, ' '),
      marked: leaseId !== undefined && m[7].split(/\s+/).includes(leaseMarker(leaseId)),
    });
  }
  return rows;
}

function readProcRow(name, leaseId, uptimeSec) {
  try {
    const row = parseProcStat(Number(name), fs.readFileSync(`/proc/${name}/stat`, 'utf8'), uptimeSec);
    if (!row) return null;
    if (leaseId !== undefined) {
      try {
        row.marked = fs.readFileSync(`/proc/${name}/environ`, 'utf8').split('\0').includes(leaseMarker(leaseId));
      } catch {
        // not ours to read (other uid) or exited: not marked
      }
    }
    return row;
  } catch {
    return null; // exited between readdir and read
  }
}

/**
 * The whole process table as one read: [{ pid, ppid, pgid, pcpu, rss, token, marked }]. `marked` is true when the
 * process's environment carries this lease's marker. Throws when the table cannot be read (callers treat that as
 * "unknown", never "empty"). Note macOS hides the environment of SIP-protected binaries (e.g. /bin/sleep) even from
 * `ps -E`; those are still found by the ppid walk.
 */
export function readProcessTable(leaseId) {
  if (process.platform === 'linux') {
    const uptimeSec = Number(fs.readFileSync('/proc/uptime', 'utf8').split(' ')[0]);
    return fs
      .readdirSync('/proc')
      .filter((name) => /^\d+$/.test(name))
      .map((name) => readProcRow(name, leaseId, uptimeSec))
      .filter(Boolean);
  }
  const out = execFileSync('ps', ['-A', '-E', '-ww', '-o', 'pid=,ppid=,pgid=,pcpu=,rss=,lstart=,command='], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
    timeout: 2000,
    maxBuffer: 256 * 1024 * 1024,
    env: { ...process.env, TZ: 'UTC0', LC_ALL: 'C', LC_TIME: 'C' },
  });
  return parsePsTable(out, leaseId);
}

const sleepMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class DescendantTracker {
  /**
   * @param rootPid the leader's pid (null when only a snapshot is known); walked from while its token still matches
   * @param leaseId lease whose `LANE_BROKER_LEASE` marker identifies tree members regardless of ancestry
   * @param protectedPids pids never recorded or signalled; this process and its parent are always protected
   * @param readTable source of table rows; defaults to the real table for `leaseId`
   */
  constructor(rootPid, { leaseId, readTable, protectedPids = [] } = {}) {
    this.rootPid = rootPid;
    this.leaseId = leaseId;
    this.readTable = readTable ?? (() => readProcessTable(leaseId));
    this.protectedPids = new Set([process.pid, process.ppid, ...protectedPids]);
    this.recorded = new Map(); // pid -> token
    this.rootToken = null; // learned on the first scan that sees the leader, so a reused leader pid is never walked
  }

  static fromSnapshot(rootPid, snapshot, options) {
    const tracker = new DescendantTracker(rootPid, options);
    for (const { pid, token } of snapshot ?? []) if (!tracker.protectedPids.has(pid)) tracker.recorded.set(pid, token);
    return tracker;
  }

  /** One table read, retried a few times; null when it still fails. */
  readRows() {
    for (let attempt = 0; attempt < READ_ATTEMPTS; attempt += 1) {
      try {
        return this.readTable();
      } catch {
        // retry
      }
    }
    return null;
  }

  /**
   * Record every process in the tree (see the file comment) and return the rows read, or null if the table was
   * unreadable. A walk/marker hit refreshes the recorded token: only membership proves a pid is ours, so a reused
   * pid that is again a real descendant must not keep its stale token.
   */
  scan(rows = this.readRows()) {
    if (!rows) return null;
    const childrenOf = new Map();
    for (const r of rows) childrenOf.set(r.ppid, [...(childrenOf.get(r.ppid) ?? []), r]);
    const tokenOf = new Map(rows.map((r) => [r.pid, r.token]));
    if (this.rootToken === null && this.rootPid != null) this.rootToken = tokenOf.get(this.rootPid) ?? null;
    const stack = this.rootToken !== null && tokenOf.get(this.rootPid) === this.rootToken ? [this.rootPid] : [];
    for (const [pid, token] of this.recorded) if (tokenOf.get(pid) === token) stack.push(pid);
    const seen = new Set(stack);
    const admit = (row) => {
      this.recorded.set(row.pid, row.token);
      if (!seen.has(row.pid)) {
        seen.add(row.pid);
        stack.push(row.pid);
      }
    };
    for (const r of rows) if (r.marked && !this.protectedPids.has(r.pid)) admit(r);
    while (stack.length > 0) {
      for (const child of childrenOf.get(stack.pop()) ?? []) {
        if (this.protectedPids.has(child.pid)) continue;
        if (!seen.has(child.pid) || this.recorded.get(child.pid) !== child.token) admit(child);
      }
    }
    return rows;
  }

  /** Recorded pids still running with the token they were recorded under. */
  live(rows) {
    const tokenOf = new Map(rows.map((r) => [r.pid, r.token]));
    return [...this.recorded]
      .filter(([pid, token]) => !this.protectedPids.has(pid) && tokenOf.get(pid) === token)
      .map(([pid]) => pid);
  }

  /** Serializable form for the lease, so a canceller without this supervisor can still reap them. */
  snapshot() {
    return [...this.recorded].map(([pid, token]) => ({ pid, token }));
  }

  /** Send `signal` once, synchronously, to every live member in `rows`; returns the pids signalled. */
  signalLive(rows, signal, kill = process.kill.bind(process)) {
    const pids = this.live(rows).filter((pid) => pid > 1);
    for (const pid of pids) {
      try {
        kill(pid, signal);
      } catch {
        // gone
      }
    }
    return pids;
  }

  /**
   * TERM every live member, rescanning each iteration (a replacement spawned by a TERM handler is a member too),
   * wait up to graceMs, then KILL whatever remains for up to KILL_WAIT_MS. Never longer than graceMs + KILL_WAIT_MS
   * plus one table read, so a caller that budgets for both is safe.
   * @returns {{ signalled: number, survivors: number[], complete: boolean }} complete is false when the table could
   *   not be read at the end or when members survived the KILL
   */
  async reap({ graceMs, killWaitMs = KILL_WAIT_MS, kill = process.kill.bind(process), sleep = sleepMs } = {}) {
    const signalled = new Set();
    const sent = { SIGTERM: new Set(), SIGKILL: new Set() };
    const sweep = (signal) => {
      const rows = this.scan();
      if (!rows) return null;
      const live = this.live(rows);
      for (const pid of live) {
        const key = `${pid}:${this.recorded.get(pid)}`;
        if (sent[signal].has(key) || !(pid > 1)) continue;
        sent[signal].add(key);
        signalled.add(pid);
        try {
          kill(pid, signal);
        } catch {
          // gone
        }
      }
      return live;
    };
    const run = async (signal, deadline) => {
      let live = null;
      for (;;) {
        live = sweep(signal);
        if ((live !== null && live.length === 0) || Date.now() >= deadline) return live;
        await sleep(signal === 'SIGTERM' ? 100 : 50);
      }
    };
    const termDeadline = Date.now() + graceMs;
    let last = await run('SIGTERM', termDeadline);
    if (last === null || last.length > 0) last = await run('SIGKILL', termDeadline + killWaitMs);
    const survivors = last ?? [];
    return { signalled: signalled.size, survivors, complete: last !== null && survivors.length === 0 };
  }
}

/** The tracker for a lease whose supervisor is gone: its last recorded snapshot plus the lease marker. Shared by
 *  `lane cancel` (to reap) and the stale-lease check (to see that evidence is still alive). */
export function trackerForLease(lease) {
  return DescendantTracker.fromSnapshot(null, lease.descendants, { leaseId: lease.id });
}

/** Does a lease whose supervisor is gone still have live recorded/marked members? Fails closed: unreadable = yes. */
export function hasLiveMembers(tracker) {
  const rows = tracker.scan();
  return rows === null ? true : tracker.live(rows).length > 0;
}

/** Log fragment for a reap result: `descendants-reaped=N`, plus `descendants-reap-incomplete survivors=…` when not complete. */
export function reapLogLine(id, result) {
  const base = `lane-broker-reap id=${id} descendants-reaped=${result.signalled}`;
  if (result.complete) return `${base}\n`;
  const survivors = result.survivors.length > 0 ? result.survivors.join(',') : 'unknown';
  return `${base}\nlane-broker-reap id=${id} descendants-reap-incomplete survivors=${survivors}\n`;
}
