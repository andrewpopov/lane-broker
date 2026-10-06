import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import { DescendantTracker, procUnits } from './descendants.js';

/**
 * BRAIN-428: CPU that yields to an admitted lane, measured per process BY IDENTITY over the same window as the host
 * busy sample. A process is preemptible when its nice is STRICTLY GREATER than the nice the candidate lane will run at
 * (the kernel runs the lane ahead of it; equal or lower nice is a peer or better, and counts as ordinary load), it is
 * not part of any held lease's tree (a lane's own CPU is never discounted, whatever its nice), and it has a previous
 * reading to diff against (a process seen for the first time contributes 0, never a guess). Anything that cannot be
 * determined -- unreadable process table, a held lease whose tree is not known yet -- counts nothing as preemptible.
 */

const GONE = new Set(['ENOENT', 'ESRCH']);

/** Linux /proc/<pid>/stat -> { pid, ppid, pgid, nice, cpuSec, token }. comm may hold spaces/parens: split after the last ')'. */
export function parseProcCpuStat(pid, text, clkTck) {
  const close = text.lastIndexOf(')');
  if (close < 0) return null;
  const f = text.slice(close + 2).split(' '); // f[0] is state (field 3)
  const ppid = Number(f[1]);
  const pgid = Number(f[2]);
  const cpuSec = (Number(f[11]) + Number(f[12])) / clkTck; // utime + stime (fields 14/15)
  const nice = Number(f[16]); // field 19
  const token = f[19]; // field 22, starttime in ticks -- the same token descendants.js records
  if (![ppid, pgid, cpuSec, nice].every(Number.isFinite) || !token) return null;
  return { pid, ppid, pgid, nice, cpuSec, token };
}

/** macOS `ps` cumulative CPU time: [[dd-]hh:]mm:ss[.cc] -> seconds, or null. */
export function parsePsCpuTime(text) {
  const m = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)$/.exec(text);
  if (!m) return null;
  return Number(m[1] ?? 0) * 86400 + Number(m[2] ?? 0) * 3600 + Number(m[3]) * 60 + Number(m[4]);
}

const PS_CPU_LINE = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(-?\d+)\s+(\S+)\s+(\w{3}\s+\w{3}\s+\d+\s+[\d:]{8}\s+\d{4})\s*$/;

/** macOS `ps -A -o pid=,ppid=,pgid=,nice=,time=,lstart=` -> rows; lstart (normalised as descendants.js does) is the start token. */
export function parsePsCpuTable(text) {
  const rows = [];
  for (const line of String(text ?? '').split('\n')) {
    const m = PS_CPU_LINE.exec(line);
    const cpuSec = m ? parsePsCpuTime(m[5]) : null;
    if (!m || cpuSec === null) continue;
    rows.push({ pid: Number(m[1]), ppid: Number(m[2]), pgid: Number(m[3]), nice: Number(m[4]), cpuSec, token: m[6].replace(/\s+/g, ' ') });
  }
  return rows;
}

/** One read of every process's identity, nice and cumulative CPU seconds. Throws when the table cannot be read. */
export function readProcCpuRows({ platform = process.platform, exec = execFileSync, fsApi = fs, units } = {}) {
  if (platform === 'linux') {
    const { clkTck } = units ?? procUnits();
    return fsApi
      .readdirSync('/proc')
      .filter((name) => /^\d+$/.test(name))
      .map((name) => {
        try {
          return parseProcCpuStat(Number(name), fsApi.readFileSync(`/proc/${name}/stat`, 'utf8'), clkTck);
        } catch (err) {
          if (GONE.has(err?.code)) return null; // exited mid-scan; a process we cannot read is never counted
          return null;
        }
      })
      .filter(Boolean);
  }
  const out = exec('ps', ['-A', '-o', 'pid=,ppid=,pgid=,nice=,time=,lstart='], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
    timeout: 2000,
    maxBuffer: 256 * 1024 * 1024,
    env: { ...process.env, TZ: 'UTC0', LC_ALL: 'C', LC_TIME: 'C' },
  });
  return parsePsCpuTable(out);
}

const keyOf = (row) => `${row.pid}:${row.token}`;

/** The previous-sample form kept in the cpu-sample sidecar: identity -> cumulative CPU seconds. */
export function procSnapshot(rows) {
  return rows ? Object.fromEntries(rows.map((r) => [keyOf(r), r.cpuSec])) : {};
}

/** pids belonging to held leases' trees (process group, ppid descendants, recorded descendants); null = cannot tell. */
function leasePids(rows, heldLeases) {
  const members = new Set();
  for (const lease of heldLeases) {
    if (!Number.isFinite(lease.childPgid)) return null; // tree not known yet (still spawning): fail closed
    const tracker = DescendantTracker.fromSnapshot(lease.childPgid, lease.descendants, { leaseId: lease.id });
    const scanned = tracker.scan(rows.map((r) => ({ ...r, marked: false })));
    if (!scanned) return null;
    for (const pid of tracker.live(scanned)) members.add(pid);
    members.add(lease.childPgid);
    for (const r of rows) if (r.pgid === lease.childPgid) members.add(r.pid);
  }
  return members;
}

/**
 * Cores of preemptible CPU over the window since `prevProcs` was taken: the summed CPU-time delta of every
 * qualifying process divided by the wall-clock window. `rows` null (unreadable table) or an unknown lease tree gives 0.
 */
export function preemptibleCores({ rows, prevProcs, windowMs, laneNice, niceMin = 1, heldLeases = [] }) {
  if (!rows || !prevProcs || !(windowMs > 0) || !(niceMin > 0)) return 0;
  const members = leasePids(rows, heldLeases);
  if (members === null) return 0;
  let cpuSec = 0;
  for (const row of rows) {
    if (row.nice <= laneNice || row.nice < niceMin || members.has(row.pid)) continue;
    const before = prevProcs[keyOf(row)];
    if (!Number.isFinite(before)) continue; // new or pid-reused process: no previous reading, contributes nothing
    cpuSec += Math.max(0, row.cpuSec - before);
  }
  return cpuSec / (windowMs / 1000);
}
