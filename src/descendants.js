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

const READ_ATTEMPTS = 3;
const KILL_WAIT_MS = 3000;

let cachedUnits = null;

/** Kernel units /proc reports in: page size (statm/stat rss) and clock ticks (utime/stime/starttime). Read from getconf
 *  and cached once BOTH reads succeed; a failure returns 4096 / 100 for that call only, so the next call retries. */
export function procUnits(run = execFileSync) {
  if (cachedUnits) return cachedUnits;
  let ok = true;
  const conf = (name, fallback) => {
    try {
      const value = Number(run('getconf', [name], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 2000 }).trim());
      if (Number.isFinite(value) && value > 0) return value;
    } catch {
      // fall through
    }
    ok = false;
    return fallback;
  };
  const units = { pageKib: conf('PAGESIZE', 4096) / 1024, clkTck: conf('CLK_TCK', 100) };
  if (ok) cachedUnits = units;
  return units;
}

/** Test seam: forget the cached units. */
export function resetProcUnitsCache() {
  cachedUnits = null;
}

export function leaseMarker(leaseId) {
  return `LANE_BROKER_LEASE=${leaseId}`;
}

/** Linux: /proc/<pid>/stat. comm may hold spaces/parens, so split after the last ')'. `uptimeSec` turns starttime into pcpu like ps does. */
export function parseProcStat(pid, text, uptimeSec = 0, { pageKib, clkTck } = procUnits()) {
  const close = text.lastIndexOf(')');
  if (close < 0) return null;
  const f = text.slice(close + 2).split(' '); // f[0] is state (field 3)
  const ppid = Number(f[1]);
  const pgid = Number(f[2]);
  const token = f[19]; // field 22, starttime in ticks
  if (![ppid, pgid].every(Number.isFinite) || !token) return null;
  const cpuSec = (Number(f[11]) + Number(f[12])) / clkTck; // utime + stime
  const elapsed = uptimeSec - Number(token) / clkTck;
  const pcpu = elapsed > 0 && Number.isFinite(cpuSec) ? (cpuSec / elapsed) * 100 : 0;
  return { pid, ppid, pgid, pcpu, rss: (Number(f[21]) || 0) * pageKib, token, marked: false };
}

const PS_LINE = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+(\d+)\s+(\w{3}\s+\w{3}\s+\d+\s+[\d:]{8}\s+\d{4})\s*(.*)$/;
const PS_COMMAND_LINE = /^\s*(\d+)\s*(.*)$/;

/** `ps -o pid=,command=` text -> pid -> command (with `-E`, argv followed by the environment). */
export function parseCommandMap(text) {
  const map = new Map();
  for (const line of String(text ?? '').split('\n')) {
    const m = PS_COMMAND_LINE.exec(line);
    if (m) map.set(Number(m[1]), m[2].trim());
  }
  return map;
}

/** The environment part of a `-E` command line: what follows the plain command as an exact prefix, else '' (it changed between reads). */
export function envSuffix(command, withEnv) {
  return withEnv !== undefined && withEnv.startsWith(command) ? withEnv.slice(command.length) : '';
}

/**
 * macOS: `ps -A -ww -o pid=,ppid=,pgid=,pcpu=,rss=,lstart=,command=` (`text`) plus, when a lease marker is wanted,
 * `ps -A -E -ww -o pid=,command=` (`envText`). With -E the command column is the argv FOLLOWED BY the environment, so
 * each pid's plain command from the first listing is stripped as an exact prefix and only the remainder is matched:
 * an argument that merely spells the marker is not a member. A pid whose -E command does not start with its plain
 * command (it changed between the two reads) is unmarked.
 */
export function parsePsTable(text, leaseId, envText = '') {
  const envOf = parseCommandMap(envText);
  const rows = [];
  for (const line of String(text ?? '').split('\n')) {
    const m = PS_LINE.exec(line);
    if (!m) continue;
    const pid = Number(m[1]);
    const command = m[7].trim();
    const withEnv = envOf.get(pid);
    const env = envSuffix(command, withEnv);
    rows.push({
      pid,
      ppid: Number(m[2]),
      pgid: Number(m[3]),
      pcpu: Number(m[4]),
      rss: Number(m[5]),
      token: m[6].replace(/\s+/g, ' '),
      marked: leaseId !== undefined && env.split(/\s+/).includes(leaseMarker(leaseId)),
    });
  }
  return rows;
}

const GONE = new Set(['ENOENT', 'ESRCH']);

/** A process we know exists but could not read: never counted as exited, never signalled (its token is unverifiable). */
const unknownRow = (pid) => ({ pid, ppid: -1, pgid: -1, pcpu: 0, rss: 0, token: null, marked: false, unknown: true });

/** One /proc entry. Only ENOENT/ESRCH mean "gone"; any other read error (EACCES, EIO, ...) is an unknown row. */
export function readProcRow(name, leaseId, uptimeSec, fsApi = fs, units) {
  const pid = Number(name);
  let text;
  try {
    text = fsApi.readFileSync(`/proc/${name}/stat`, 'utf8');
  } catch (err) {
    return GONE.has(err?.code) ? null : unknownRow(pid);
  }
  const row = parseProcStat(pid, text, uptimeSec, units);
  if (!row) return unknownRow(pid);
  if (leaseId !== undefined) {
    try {
      row.marked = fsApi.readFileSync(`/proc/${name}/environ`, 'utf8').split('\0').includes(leaseMarker(leaseId));
    } catch {
      // another uid's environ is unreadable by design; such a process is found by the ppid walk, not the marker
    }
  }
  return row;
}

/**
 * The whole process table as one read: [{ pid, ppid, pgid, pcpu, rss, token, marked }]. `marked` is true when the
 * process's environment carries this lease's marker. Throws when the table cannot be read (callers treat that as
 * "unknown", never "empty". Note macOS hides the environment of SIP-protected binaries (e.g. /bin/sleep) even from
 * `ps -E`; those are found only by the ppid walk, and only if a scan saw them before their parent exited.
 */
export function readProcessTable(leaseId) {
  if (process.platform === 'linux') {
    const uptimeSec = Number(fs.readFileSync('/proc/uptime', 'utf8').split(' ')[0]);
    const units = procUnits(); // once per scan: a failed getconf is retried next scan, never once per pid
    return fs
      .readdirSync('/proc')
      .filter((name) => /^\d+$/.test(name))
      .map((name) => readProcRow(name, leaseId, uptimeSec, fs, units))
      .filter(Boolean);
  }
  const ps = (args) =>
    execFileSync('ps', args, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 2000,
      maxBuffer: 256 * 1024 * 1024,
      env: { ...process.env, TZ: 'UTC0', LC_ALL: 'C', LC_TIME: 'C' },
    });
  const out = ps(['-A', '-ww', '-o', 'pid=,ppid=,pgid=,pcpu=,rss=,lstart=,command=']);
  const envOut = leaseId === undefined ? '' : ps(['-A', '-E', '-ww', '-o', 'pid=,command=']);
  return parsePsTable(out, leaseId, envOut);
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

  /** Recorded pids still running with the token they were recorded under, plus any whose row is unreadable
   *  (unknown is never "exited"). */
  live(rows) {
    const rowOf = new Map(rows.map((r) => [r.pid, r]));
    return [...this.recorded]
      .filter(([pid, token]) => !this.protectedPids.has(pid) && (rowOf.get(pid)?.token === token || rowOf.get(pid)?.unknown === true))
      .map(([pid]) => pid);
  }

  /** Serializable form for the lease, so a canceller without this supervisor can still reap them. */
  snapshot() {
    return [...this.recorded].map(([pid, token]) => ({ pid, token }));
  }

  /** Send `signal` to every live member in `rows` that is not already in `sent` (a Set of "pid:token" keys, updated);
   *  returns the pids signalled. An unreadable (unknown) member is never signalled: its token cannot be verified. */
  signalLive(rows, signal, kill = process.kill.bind(process), sent = new Set()) {
    const unknown = new Set(rows.filter((r) => r.unknown).map((r) => r.pid));
    const pids = [];
    for (const pid of this.live(rows)) {
      const key = `${pid}:${this.recorded.get(pid)}`;
      if (!(pid > 1) || unknown.has(pid) || sent.has(key)) continue;
      sent.add(key);
      pids.push(pid);
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
      for (const pid of this.signalLive(rows, signal, kill, sent[signal])) signalled.add(pid);
      return this.live(rows);
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
export function trackerForLease(lease, options = {}) {
  return DescendantTracker.fromSnapshot(null, lease.descendants, { leaseId: lease.id, ...options });
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
