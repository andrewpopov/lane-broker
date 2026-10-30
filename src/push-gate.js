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
 * The id of the other push of the same caller that `ticket` waits behind, or null. A RUNNING gate holds whatever its order (an
 * older push that enqueues late must not run beside a younger one already started); among queued gates the earlier push holds.
 * Siblings of one push (same gitPid), orphaned leases and gates already cancelled or superseded hold nothing.
 */
export function pushHoldBehind(root, ticket, queue, leases) {
  const holds = (other) =>
    other.id !== ticket.id && other.pushGate?.caller === ticket.pushGate.caller && other.pushGate.gitPid !== ticket.pushGate.gitPid && !isCancelled(root, other.id) && !readSupersededBy(root, other.id);
  const running = leases.find((lease) => lease.state === LEASE_STATE.RUNNING && holds(lease));
  if (running) return running.id;
  const earlier = queue.filter((other) => other && holds(other) && pushOrder(other.pushGate, ticket.pushGate) < 0).sort((a, b) => pushOrder(a.pushGate, b.pushGate));
  return earlier[0]?.id ?? null;
}

/** Map of every held queued gate id to the id it waits behind. */
export function pushHolds(root, queue, leases) {
  const holds = new Map();
  for (const ticket of queue) {
    const behind = ticket?.pushGate ? pushHoldBehind(root, ticket, queue, leases) : null;
    if (behind) holds.set(ticket.id, behind);
  }
  return holds;
}
