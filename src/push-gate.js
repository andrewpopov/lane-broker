import crypto from 'node:crypto';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { scrubbedGitEnv } from './remote-manifest.js';
import { isCancelled, readSupersededBy } from './state.js';
import { LEASE_STATE } from './lease.js';

const MAX_HOPS = 8;
const PS_LINE = /^\s*(\d+)\s+(\w{3} \w{3}\s+\d+ \d\d:\d\d:\d\d \d{4})\s+(.*)$/;
const OPTIONS_WITH_VALUE = new Set(['-c', '-C']);

export const PUSH_GATE_LANE = /^prepush(-|$)/;

function psRow(pid) {
  const out = execFileSync('ps', ['-o', 'ppid=,lstart=,args=', '-p', String(pid)], {
    encoding: 'utf8',
    env: { ...process.env, LC_ALL: 'C', LC_TIME: 'C' },
    stdio: ['ignore', 'pipe', 'ignore'],
    timeout: 5000,
  });
  const match = PS_LINE.exec(out.trim());
  if (!match) throw new Error(`unparseable ps row for ${pid}`);
  return { ppid: Number(match[1]), start: match[2], args: match[3].split(/\s+/) };
}

function pushArgsOf(args) {
  if (path.basename(args[0] ?? '') !== 'git') return null;
  for (let i = 1; i < args.length; i += 1) {
    if (OPTIONS_WITH_VALUE.has(args[i])) i += 1;
    else if (!args[i].startsWith('-')) return args[i] === 'push' ? args.slice(i + 1) : null;
  }
  return null;
}

const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');

function callerOf(worktree, env) {
  return env.CLAUDE_CODE_SESSION_ID || env.CODEX_THREAD_ID || env.CODEX_SESSION_ID || `worktree:${worktree}`;
}

// Only the nearest git ancestor counts (so `git hook run pre-push` is not a push). Raw args are never kept: a URL can carry a token.
export function detectPushGate({ cwd = process.cwd(), env = process.env, ppid = process.ppid } = {}) {
  try {
    for (let pid = ppid, hops = 0; pid > 1 && hops < MAX_HOPS; hops += 1) {
      const row = psRow(pid);
      const pushArgs = pushArgsOf(row.args);
      if (path.basename(row.args[0] ?? '') === 'git') {
        if (!pushArgs) return null;
        const [worktree, branch] = execFileSync('git', ['rev-parse', '--show-toplevel', '--abbrev-ref', 'HEAD'], {
          cwd,
          encoding: 'utf8',
          env: scrubbedGitEnv(),
          stdio: ['ignore', 'pipe', 'ignore'],
          timeout: 5000,
        })
          .trim()
          .split('\n');
        const gitStartEpoch = Date.parse(row.start);
        if (!worktree || !branch || !Number.isFinite(gitStartEpoch)) return null;
        return {
          gitPid: pid,
          gitStart: row.start,
          gitStartEpoch,
          clientPid: process.pid,
          clientStart: psRow(process.pid).start,
          caller: callerOf(worktree, env),
          worktree,
          branch,
          pushArgsHash: sha256(pushArgs.join(' ')),
        };
      }
      pid = row.ppid;
    }
    return null;
  } catch {
    return null;
  }
}

// Never createdAt: hook setup can reorder lane submission.
export function pushOrder(a, b) {
  return a.gitStartEpoch - b.gitStartEpoch || a.gitPid - b.gitPid;
}

export function isSuperseded(result) {
  return result.reason === 'superseded';
}

export function isRepeatOfPush(a, b) {
  return a.worktree === b.worktree && a.branch === b.branch && a.pushArgsHash === b.pushArgsHash && a.gitPid !== b.gitPid;
}

/**
 * Map of every held queued gate id to the id of the other push of the same caller it waits behind. A RUNNING gate holds whatever
 * its order (an older push that enqueues late must not run beside a younger one already started); among queued gates the
 * earlier push holds. Siblings of one push (same gitPid), orphaned leases and gates already cancelled or superseded hold nothing.
 * One pass per caller over gates sorted once, each ticket's markers read at most once: linear in stamped tickets.
 */
export function pushHolds(root, queue, leases) {
  const live = new Map();
  const isLive = (id) => {
    if (!live.has(id)) live.set(id, !isCancelled(root, id) && !readSupersededBy(root, id));
    return live.get(id);
  };
  const byCaller = new Map();
  const group = (caller) => byCaller.get(caller) ?? byCaller.set(caller, { running: [], queued: [] }).get(caller);
  for (const lease of leases) if (lease.pushGate && lease.state === LEASE_STATE.RUNNING && isLive(lease.id)) group(lease.pushGate.caller).running.push(lease);
  for (const ticket of queue) if (ticket?.pushGate) group(ticket.pushGate.caller).queued.push(ticket);
  const holds = new Map();
  for (const { running, queued } of byCaller.values()) {
    const firstRunning = running[0];
    const otherRunning = firstRunning && running.find((l) => l.pushGate.gitPid !== firstRunning.pushGate.gitPid);
    let earliest;
    let earliestOfAnotherPush;
    for (const ticket of queued.sort((x, y) => pushOrder(x.pushGate, y.pushGate))) {
      const differs = (other) => other && other.pushGate.gitPid !== ticket.pushGate.gitPid;
      const runningHolder = differs(firstRunning) ? firstRunning : otherRunning;
      const holder = runningHolder ?? (differs(earliest) ? earliest : earliestOfAnotherPush);
      if (holder) holds.set(ticket.id, holder.id);
      if (!isLive(ticket.id)) continue;
      if (!earliest) earliest = ticket;
      else if (!earliestOfAnotherPush && ticket.pushGate.gitPid !== earliest.pushGate.gitPid) earliestOfAnotherPush = ticket;
    }
  }
  return holds;
}

export function pushHoldBehind(root, ticket, queue, leases) {
  return pushHolds(root, queue.some((t) => t?.id === ticket.id) ? queue : [...queue, ticket], leases).get(ticket.id) ?? null;
}
