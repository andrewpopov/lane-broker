import path from 'node:path';
import { classifyPullTerminal, isTransientError } from './pull-ticket.js';
import { pullServices } from './pull-services.js';
import { atomicWriteJson, paths } from './state.js';

const TERMINAL_STATES = new Set(['finished', 'cancelled', 'lost', 'withdrawn']);
const WITHDRAWN_EXIT = 76;
const WAITING_EXIT = 75;
const UNPROVEN_EXIT = 1;

/** A runner result is bound to the ticket the row itself names: this machine has no manifest of the original run to check it against, and must not invent one. */
const selfBound = (row) => ({ protocol: row.result?.protocol, ticketId: row.id, generation: row.result?.generation, manifestHash: row.result?.manifestHash });

/** What a terminal row of a ticket this machine never ran comes to: the exit to report and the reason to say. */
export function adoptedOutcome(row) {
  if (row.state === 'withdrawn') return { exitCode: WITHDRAWN_EXIT, note: `ticket ${row.id} was withdrawn from the queue before it ran; resubmit with a new key` };
  const decision = classifyPullTerminal(row, selfBound(row));
  if (decision.kind === 'unproven') return { exitCode: UNPROVEN_EXIT, note: `ticket ${row.id} was cancelled without proof that it stopped`, decision };
  const how = decision.kind === 'finished' ? 'finished' : decision.kind === 'cancelled' ? 'was cancelled' : `was lost (${decision.reason})`;
  return { exitCode: decision.exit, note: `ticket ${row.id} ${how} with exit ${decision.exit}`, decision };
}

/** The result a finished adopted ticket leaves here, so a later `lane run` or `lane wait` with the key answers from it. No history row: this machine ran nothing. */
function publishAdopted(root, row, { exitCode, decision }) {
  const parse = (value) => (value ? Date.parse(value) : null);
  atomicWriteJson(path.join(paths(root).results, `${row.id}.json`), {
    id: row.id, exit: exitCode, signal: null, executor: 'remote', transport: 'pull', runner: row.host ?? null, adoptedFromQueue: true,
    startedAt: parse(row.started_at), endedAt: parse(row.finished_at) ?? Date.now(), ...(decision?.reason ? { reason: decision.reason } : {}),
  }, { fsync: true });
}

/**
 * Observes a queue ticket this machine did not submit (or has no record of) until it ends, and reports what the queue holds. Nothing is placed, no snapshot is
 * prepared and no supervisor starts: the ticket runs, or ran, exactly once, wherever it was submitted. Returns `{exitCode, note}`.
 */
export async function attachToHeldTicket({ cfg, root, held, timeoutMs, pollMs = 2000, servicesFor = pullServices, now = Date.now, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)) }) {
  const services = servicesFor(cfg, root);
  if (!services.available) return { exitCode: WAITING_EXIT, note: `cannot observe ticket ${held.id}: pull dispatch is not available here (${services.reason})` };
  const deadline = timeoutMs === undefined ? Infinity : now() + timeoutMs;
  for (;;) {
    let row;
    try {
      row = await services.queue.get(held.id);
    } catch (err) {
      if (!isTransientError(err)) return { exitCode: UNPROVEN_EXIT, note: `cannot observe ticket ${held.id}: ${err.message}` };
      row = undefined;
    }
    if (row === null) return { exitCode: UNPROVEN_EXIT, note: `ticket ${held.id} is no longer on the queue` };
    if (row !== undefined && TERMINAL_STATES.has(row.state)) {
      const outcome = adoptedOutcome(row);
      if (outcome.decision !== undefined && outcome.decision.kind !== 'unproven') publishAdopted(root, row, outcome);
      return outcome;
    }
    if (now() >= deadline) return { exitCode: WAITING_EXIT, note: `waited ${timeoutMs}ms; ticket ${held.id} is still ${row?.state ?? 'unreachable'} (not failed)` };
    await sleep(pollMs);
  }
}
