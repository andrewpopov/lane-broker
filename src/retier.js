import fs from 'node:fs';
import path from 'node:path';
import { ensureStateDirs, LockTimeoutError, MigrationInProgressError } from './state.js';
import { retierQueued, LegacyQueueAfterFenceError } from './scheduler.js';
import { isPriorityTier, PRIORITY_TIERS } from './priority.js';
import { loadGlobalConfigOrError, reloadGlobalConfig, DEFAULT_GLOBAL_CONFIG } from './config.js';
import { defaultRemoteRoot } from './remote-runner.js';

/** The exit code for each refusal reason (plans/exit-codes.md): 80 no such / finished, 81 wrong state, 85 supervisor gone, 89 legacy scheduler. */
const REFUSAL_EXIT = {
  'legacy-scheduler': 89,
  unknown: 80,
  finished: 80,
  running: 81,
  remote: 81,
  'remote-owned': 81,
  rebinding: 75,
  'supervisor-gone': 85,
  'unreadable-record': 1,
  'record-mismatch': 1,
  'legacy-record': 1,
};

const RESUBMIT = 'lane cancel it and resubmit with --priority';

/** Why a refusal reads the way it does, one stderr phrase per reason. `detail` is the reason's own sub-reason, when it has one. */
function refusalText(reason, detail) {
  switch (reason) {
    case 'legacy-scheduler':
      return `legacy scheduler (${detail}); priority reorders nothing until lane migrate-scheduler`;
    case 'unknown':
      return 'no such queued ticket';
    case 'finished':
      return `already finished (${detail})`;
    case 'running':
      return 'running; only a queued ticket can be re-tiered';
    case 'remote':
      return `queued on a runner; ${RESUBMIT}`;
    case 'remote-owned':
      return `owned by submitter (remote ticket ${detail}); ${RESUBMIT} there`;
    case 'rebinding':
      return 'being moved to a runner right now; retry in a moment';
    case 'supervisor-gone':
      return 'its supervisor is gone';
    case 'legacy-record':
      return 'its queue record predates the priority scheduler';
    default:
      return 'its queue record is unreadable or inconsistent';
  }
}

/**
 * A runner-side remote-exec ticket is owned by its submitter: `remote-exec` records the broker ticket id in
 * `<remote root>/tickets/<submitter ticket id>/remote-id`. Returns that submitter ticket id, or null for a ticket of this host.
 */
export function remoteOwnerOf(id, remoteRoot = defaultRemoteRoot()) {
  return remoteOwnerMap(remoteRoot).get(id) ?? null;
}

/** Most submitter ticket dirs one status call will read; a runner's remote root is pruned long before this. */
const MAX_OWNER_SCAN = 5000;

/** broker ticket id -> submitter ticket id, from one scan of `<remote root>/tickets/*\/remote-id`. An unreadable dir is skipped, never thrown. */
export function remoteOwnerMap(remoteRoot = defaultRemoteRoot()) {
  const ticketsDir = path.join(remoteRoot, 'tickets');
  const owners = new Map();
  let names;
  try {
    names = fs.readdirSync(ticketsDir).slice(0, MAX_OWNER_SCAN);
  } catch {
    return owners;
  }
  for (const name of names) {
    try {
      const id = fs.readFileSync(path.join(ticketsDir, name, 'remote-id'), 'utf8').trim();
      if (id && !owners.has(id)) owners.set(id, name);
    } catch {
      // unreadable ticket dir: no owner for it
    }
  }
  return owners;
}

/** `lane priority <id> high|medium|low`: re-tier a ticket that is still queued on this host, keeping its place and age. */
export async function priorityCommand(id, tier, { remoteRoot } = {}) {
  if (!isPriorityTier(tier)) {
    process.stderr.write(`lane priority: invalid tier "${tier}": expected ${PRIORITY_TIERS.join('|')}\n`);
    return { exitCode: 64 };
  }
  const root = ensureStateDirs().root;
  const { config } = loadGlobalConfigOrError();
  let result;
  try {
    result = await retierQueued(root, id, tier, config ?? DEFAULT_GLOBAL_CONFIG, {
      reloadConfig: (previous) => reloadGlobalConfig(previous),
      remoteOwnerOf: (ticketId) => remoteOwnerOf(ticketId, remoteRoot),
    });
  } catch (err) {
    if (err instanceof LockTimeoutError || err instanceof MigrationInProgressError) {
      process.stderr.write(`lane priority: ${err.message}; nothing changed, retry shortly\n`);
      return { exitCode: 75 };
    }
    if (err instanceof LegacyQueueAfterFenceError) {
      process.stderr.write(`lane priority: ${err.message}\n`);
      return { exitCode: 1 };
    }
    throw err;
  }
  if (result.outcome === 'refused') {
    process.stderr.write(`lane priority: refused ${id}: ${refusalText(result.reason, result.detail)}\n`);
    return { exitCode: REFUSAL_EXIT[result.reason] };
  }
  const { before, after } = result;
  if (result.outcome === 'unchanged') {
    process.stdout.write(`lane priority: ${id} already ${after.priorityAdmitted}${after.priorityDemoted ? ' (high requested, demoted)' : ''}; nothing changed\n`);
    return { exitCode: 0 };
  }
  const demotion = after.priorityDemoted ? ' (high requested, demoted to medium: repo already has a queued high ticket)' : '';
  process.stdout.write(`lane priority: ${id} ${before.priorityAdmitted} -> ${after.priorityAdmitted}${demotion}\n`);
  return { exitCode: 0 };
}
