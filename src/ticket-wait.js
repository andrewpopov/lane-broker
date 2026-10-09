import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, readJsonSafe } from './state.js';
import { ADMISSION_LOG_REFRESH_MS } from './admission.js';

/**
 * BRAIN-504 part 3: what each queued ticket is waiting on, recorded from the scheduler's own `tryStart` outcome (never
 * re-derived). One file per ticket, `<state root>/waits/<id>.json`, written only by the supervisor polling that ticket,
 * after the global lock is released. Pure telemetry: nothing here is read back to make an admission decision.
 */

/** Outcomes that end a ticket's wait: it finished without ever starting. (`not-head` with no position means the ticket is not queued at all.) */
const ENDED = new Set(['withdrawn', 'queue-timeout', 'cancelled']);

const waitFile = (root, id) => path.join(paths(root).waits, `${id}.json`);

/** The record's `detail`: the result's own fields minus `started` and `reason`. */
function detailOf(result) {
  const { started, reason, ...detail } = result;
  return detail;
}

export function readTicketWait(root, id) {
  return readJsonSafe(waitFile(root, id));
}

export function removeTicketWait(root, id) {
  try {
    fs.unlinkSync(waitFile(root, id));
  } catch {
    // already gone, or unwritable: telemetry only
  }
}

/**
 * Record (or clear) a ticket's wait from the result its `tryStart` just returned. Writes only when the reason changed or
 * the record is older than ADMISSION_LOG_REFRESH_MS, so a poll loop does not rewrite the file every cycle. `since` is
 * kept while the reason is unchanged. Best-effort: a failure never fails admission.
 */
export function recordTicketWait(root, ticketId, result, now = Date.now()) {
  try {
    if (result.started || ENDED.has(result.reason) || (result.reason === 'not-head' && result.position == null)) {
      removeTicketWait(root, ticketId);
      return;
    }
    const previous = readTicketWait(root, ticketId);
    const sameReason = previous?.reason === result.reason;
    if (sameReason && Number.isFinite(previous.at) && now - previous.at < ADMISSION_LOG_REFRESH_MS) return;
    atomicWriteJson(waitFile(root, ticketId), {
      ticketId,
      reason: result.reason,
      detail: detailOf(result),
      since: sameReason && Number.isFinite(previous.since) ? previous.since : now,
      at: now,
    });
  } catch {
    // best-effort, same as the admission log
  }
}

/** Delete wait files whose ticket is neither queued nor rebinding. One readdir; nothing is read per file. */
export function pruneTicketWaits(root, liveIds) {
  const live = new Set(liveIds);
  let names;
  try {
    names = fs.readdirSync(paths(root).waits);
  } catch {
    return;
  }
  for (const name of names) {
    if (name.endsWith('.json') && !live.has(name.slice(0, -'.json'.length))) removeTicketWait(root, name.slice(0, -'.json'.length));
  }
}

const num2 = (n) => (Number.isFinite(n) ? n.toFixed(2) : '?');

/** Human label for a recorded wait; `detail` is the result's own fields (see recordTicketWait). */
export function waitLabel(reason, detail = {}) {
  switch (reason) {
    case 'capacity':
      return `weight (${detail.runningWeight}/${detail.capacity} slots in use)`;
    case 'cpu-admission':
      return `CPU budget (${detail.cpuReason}: projected ${num2(detail.projectedBusy)} > budget ${num2(detail.budget)} cores)`;
    case 'memory-admission':
      return `memory budget (${detail.memoryReason})`;
    case 'load-gate-closed':
      return `the load gate (load ${detail.load})`;
    case 'memory-critical':
      return `memory pressure (${detail.macPressure})`;
    case 'class-cap':
      return `a class cap (${detail.capReason})`;
    case 'conflict':
      return `a conflicting lease (${detail.key})`;
    case 'paused':
      return `the broker pause (${detail.pauseReason})`;
    case 'elastic-below-floor':
      return "CPU budget below this lane's minCpuCores";
    case 'exclusive-held':
    case 'exclusive-head':
    case 'exclusive-draining':
      return `an exclusive lease (${reason})`;
    case 'not-head':
      return `${(detail.position ?? 1) - 1} ticket(s) ahead (position ${detail.position}/${detail.queueLength})`;
    default:
      return reason;
  }
}
