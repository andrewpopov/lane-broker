import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { paths, atomicWriteJson, readJsonSafe } from './state.js';
import { leasePids, preemptibleCores, readLeaseMarkers, readProcCpuRows, procSnapshot, withDeltas } from './preemptible.js';
import { freshObservedCores, nonPreemptibleBusy } from './admission.js';
import { parseCommandMap } from './descendants.js';

/**
 * BRAIN-463: who is burning the CPU that no lease accounts for. Admission already subtracts it as `externalBusy`;
 * this names the process trees behind it from the per-process window the CPU sampler already stores in its sidecar
 * (`lastValid.procWindow`), so `lane status` adds no process scan of its own beyond a command lookup for the few
 * pids it reports.
 */

export const TOP_N = 5;
/** A tree below this many cores is noise, not a culprit. */
const MIN_REPORT_CORES = 0.05;
/** External busy at or above this share of the budget, with projected-over-budget denials, is "starved". */
export const STARVED_BUSY_SHARE = 0.25;
const CMD_MAX = 100;

/** Processes a tree is never grouped up into: init, shells, terminals, login/ssh plumbing. */
const BOUNDARY_COMMANDS = new Set(['launchd', 'init', 'systemd', 'sh', 'bash', 'zsh', 'fish', 'dash', 'ksh', 'tcsh', 'login', 'sshd', 'tmux', 'screen', 'Terminal', 'iTerm2', 'su', 'sudo']);

const commandName = (command) => path.basename((command ?? '').split(/\s+/)[0] ?? '').replace(/^-/, '');

/** pid -> full command line for `pids` (one `ps` on macOS, /proc on Linux); missing entries are simply absent. */
export function readCommands(pids, { platform = process.platform, exec = execFileSync, fsApi = fs } = {}) {
  const out = new Map();
  if (pids.length === 0) return out;
  if (platform === 'linux') {
    for (const pid of pids) {
      try {
        const text = fsApi.readFileSync(`/proc/${pid}/cmdline`, 'utf8').split('\0').join(' ').trim();
        if (text) out.set(pid, text);
      } catch {
        // exited, or another uid
      }
    }
    return out;
  }
  try {
    return parseCommandMap(exec('ps', ['-ww', '-p', pids.join(','), '-o', 'pid=,command='], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 2000, env: { ...process.env, LC_ALL: 'C' } }));
  } catch {
    return out;
  }
}

/** Linux only (/proc/<pid>/cwd is free there); macOS has no fast path (`lsof` costs seconds), so cwd is omitted. */
export function readCwd(pid, { platform = process.platform, fsApi = fs } = {}) {
  if (platform !== 'linux') return undefined;
  try {
    return fsApi.readlinkSync(`/proc/${pid}/cwd`);
  } catch {
    return undefined;
  }
}

/**
 * The top external process trees in `procWindow` ({ rows with deltaSec, windowMs }): CPU is summed over a busy
 * process and everything it spawned, reported against the topmost ancestor that is still outside every held lease
 * and is not a boundary process (init, a shell, a terminal). Lease trees, lease supervisors, this process and kernel
 * threads never appear. Returns null when the answer cannot be trusted (a held lease whose tree is not known yet).
 */
export function topExternalCpu({ procWindow, heldLeases = [], limit = TOP_N, readCmds = readCommands, cwdOf = readCwd, readMarkers = readLeaseMarkers, selfPid = process.pid }) {
  if (!procWindow?.rows || !(procWindow.windowMs > 0)) return [];
  const { rows, windowMs } = procWindow;
  const members = leasePids(rows, heldLeases);
  if (members === null) return null;
  const excluded = new Set(members);
  excluded.add(selfPid);
  for (const lease of heldLeases) if (Number.isFinite(lease.supervisorPid)) excluded.add(lease.supervisorPid);
  const byPid = new Map(rows.map((r) => [r.pid, r]));
  const isKernel = (r) => r.pid === 2 || r.ppid === 2;
  const busy = rows.filter((r) => r.deltaSec > 0 && !excluded.has(r.pid) && !isKernel(r));
  const chainOf = (row) => {
    const chain = [row];
    for (let r = byPid.get(row.ppid); r && !chain.includes(r) && chain.length < 64; r = byPid.get(r.ppid)) chain.push(r);
    return chain;
  };
  const chains = busy.map(chainOf);
  const commands = readCmds([...new Set(chains.flat().map((r) => r.pid))]);
  const blocks = (r) => excluded.has(r.pid) || isKernel(r) || r.pid <= 1 || BOUNDARY_COMMANDS.has(commandName(commands.get(r.pid)));
  const groups = new Map();
  for (const chain of chains) {
    let rootIdx = 0;
    while (rootIdx + 1 < chain.length && !blocks(chain[rootIdx + 1])) rootIdx++;
    const root = chain[rootIdx];
    groups.set(root.pid, (groups.get(root.pid) ?? 0) + chain[0].deltaSec);
  }
  let ranked = [...groups].map(([pid, sec]) => ({ pid, cores: sec / (windowMs / 1000) })).filter((g) => g.cores >= MIN_REPORT_CORES).sort((a, b) => b.cores - a.cores);
  if (heldLeases.length > 0) {
    // an escapee of a lease (setsid before its descendants were recorded) carries the lease marker in its environment
    const marked = readMarkers(ranked.map((g) => g.pid), heldLeases.map((l) => l.id));
    ranked = ranked.filter((g) => marked.get(g.pid) !== true);
  }
  return ranked.slice(0, limit).map((g) => {
    const cmd = (commands.get(g.pid) ?? '?').slice(0, CMD_MAX);
    const cwd = cwdOf(g.pid);
    return { pid: g.pid, cores: Number(g.cores.toFixed(2)), cmd, ...(cwd ? { cwd } : {}) };
  });
}

/**
 * Status-side window for when the sampler stored none (discounting off) or its window is old: two process-table
 * reads `gapMs` apart. This runs only in `lane status`, outside the broker lock, never on an admission path.
 * Null when the table cannot be read.
 */
export async function scanProcWindow({ readRows = readProcCpuRows, gapMs = 250, sleep = (ms) => new Promise((r) => setTimeout(r, ms)), clock = Date.now } = {}) {
  try {
    const first = readRows();
    const t0 = clock();
    await sleep(gapMs);
    const second = readRows();
    return { rows: withDeltas(second, procSnapshot(first)), windowMs: clock() - t0 };
  } catch {
    return null;
  }
}

/** Cores the window's non-lease processes burned (this process excluded), preemptible-discounted as admission does. Null when a lease tree is unknown. */
export function windowExternalCores(procWindow, heldLeases, cfg, selfPid = process.pid) {
  if (!procWindow?.rows || !(procWindow.windowMs > 0)) return null;
  const members = leasePids(procWindow.rows, heldLeases);
  if (members === null) return null;
  const sec = procWindow.rows.reduce((sum, r) => sum + (r.deltaSec > 0 && !members.has(r.pid) && r.pid !== selfPid ? r.deltaSec : 0), 0);
  const raw = sec / (procWindow.windowMs / 1000);
  // the same discount admission applies (nice classification in preemptibleCores, share in nonPreemptibleBusy)
  const preemptible = Math.min(preemptibleCores({ ...procWindow, laneNice: cfg.laneNice, niceMin: cfg.preemptibleNiceMin, heldLeases }), raw);
  return nonPreemptibleBusy({ hostBusyCores: raw, preemptibleBusyCores: preemptible }, cfg);
}

/** Host cores in use that no held lease observably accounts for, as admission's `externalBusy` computes it (preemptible discount included, from the sample's stored `preemptibleBusyCores`). */
export function externalBusyCores(cpuSample, heldLeases, cfg, now = Date.now()) {
  if (!cpuSample || !Number.isFinite(cpuSample.hostBusyCores)) return null;
  const brokerObserved = heldLeases.reduce((sum, l) => sum + (freshObservedCores(l, now) ?? 0), 0);
  return Math.max(0, nonPreemptibleBusy(cpuSample, cfg) - brokerObserved);
}

/** How long a starved record may go unrefreshed before it no longer describes the present (denials refresh it every poll). */
export const starvedStaleMs = (cfg) => Math.max(30_000, 6 * cfg.sampleMs);

/**
 * Admission denial (projected-over-budget, memory ok) under the lock: start or refresh the starved record when
 * external busy is a real share of the budget, clear it when the denial is down to the lanes themselves.
 * Best-effort like every other skip-state write.
 */
export function recordExternalDenial(root, cfg, cpuDecision, now) {
  try {
    const file = paths(root).externalStarved;
    const external = cpuDecision.externalBusy;
    if (!(cpuDecision.budget > 0) || !Number.isFinite(external) || external < STARVED_BUSY_SHARE * cpuDecision.budget) {
      fs.rmSync(file, { force: true });
      return;
    }
    const prev = readJsonSafe(file);
    const continuing = Number.isFinite(prev?.since) && Number.isFinite(prev?.lastAt) && now - prev.lastAt <= starvedStaleMs(cfg);
    if (continuing && now - prev.lastAt < cfg.sampleMs) return; // refreshed at most once per sample interval, not on every denial
    atomicWriteJson(file, { since: continuing ? prev.since : now, lastAt: now, externalBusy: external, budget: cpuDecision.budget });
  } catch {
    // best-effort
  }
}

/** An admission ends the starvation. */
export function clearExternalStarved(root) {
  try {
    fs.rmSync(paths(root).externalStarved, { force: true });
  } catch {
    // best-effort
  }
}

/** Milliseconds admission has been starved by external CPU, or null (no record, or not refreshed lately). */
export function starvedForMs(root, cfg, now = Date.now()) {
  const rec = readJsonSafe(paths(root).externalStarved);
  if (!Number.isFinite(rec?.since) || !Number.isFinite(rec?.lastAt) || now - rec.lastAt > starvedStaleMs(cfg)) return null;
  return Math.max(0, now - rec.since);
}
