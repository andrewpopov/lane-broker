import fs from 'node:fs';
import { readAttempt } from './attempts.js';
import { clearOwedCancel, owedCancelIds, readOwedCancel, touchOwedCancel } from './pull-envelope.js';
import { readLedger } from './pull-ledger.js';
import { isTransientError, stepPullTicket } from './pull-ticket.js';

const CANCEL_STEP_PASSES = 3;
const RECONCILE_STEP_PASSES = 3;
export const OWED_CANCEL_PHASE_MS = 30_000;
const SETTLED_STATE = { finished: 'adopted', cancelled: 'cancelled', lost: 'lost' };

function reconcileOutcome(step) {
  if (step.state === 'settled') {
    return step.already ? { settled: true, state: 'already-settled' } : { settled: true, state: SETTLED_STATE[step.decision.kind], exit: step.decision.exit };
  }
  if (step.state === 'unresolved') return { settled: false, state: 'ambiguous', reason: step.reason };
  if (step.state === 'stale' || step.state === 'gone' || step.state === 'withdrawn' || step.state === 'local') return { settled: false, state: 'skipped', reason: `pull ticket is ${step.state}` };
  return { settled: false, state: 'running', reason: `step ${step.state}` };
}

const MIN_STEP_MS = 250;
const TIMED_OUT = { settled: false, state: 'unreachable', reason: 'wait timeout reached', timedOut: true };

// the caller's `deadlineAt` (ms epoch) bounds how long it waits and whether a step is started; the step keeps its own budget, since a smaller one would skip objects the next pass must fetch again
async function stepWithin(ctx, deadlineAt) {
  if (deadlineAt === undefined) return stepPullTicket(ctx);
  const remaining = deadlineAt - Date.now();
  if (remaining < MIN_STEP_MS) return null;
  let timer;
  const expiry = new Promise((resolve) => { timer = setTimeout(() => resolve(null), remaining); });
  return Promise.race([stepPullTicket(ctx), expiry]).finally(() => clearTimeout(timer));
}

// an outage is `unreachable`, never a verdict
export async function reconcilePullAttempt(attempt, ctxFor, { deadlineAt } = {}) {
  try {
    let step = null;
    // `waiting` (next 0) means the step just did something that decides the next one (a replayed enqueue recorded), so the answer is one step further, not a whole sweep interval away
    for (let pass = 0; pass < RECONCILE_STEP_PASSES; pass++) {
      step = await stepWithin(ctxFor(attempt), deadlineAt);
      if (step === null || step.state !== 'waiting') break;
    }
    return step === null ? TIMED_OUT : reconcileOutcome(step);
  } catch (err) {
    if (isTransientError(err)) return { settled: false, state: 'unreachable', reason: err.message };
    throw err;
  }
}

const decisionLine = (id, decision) => {
  if (decision.kind === 'cancelled') return `lane cancel: cancelled pull attempt ${id}\n`;
  if (decision.kind === 'finished') return `lane cancel: ${id} had already finished (exit ${decision.exit}); nothing to cancel\n`;
  return `lane cancel: ${id} settled as lost (${decision.reason})\n`;
};

export async function cancelPullAttempt(id, attempt, ctxFor) {
  const ctx = { ...ctxFor(attempt), cancelRequested: () => true };
  let step;
  try {
    for (let pass = 0; pass < CANCEL_STEP_PASSES; pass++) {
      step = await stepPullTicket(ctx);
      if (step.state !== 'waiting') break;
    }
  } catch (err) {
    process.stderr.write(`lane cancel: ${id} is still held: ${err.message}; retry lane cancel once the queue answers\n`);
    return { exitCode: 1 };
  }
  if (step.state === 'settled') {
    process.stdout.write(step.already ? `lane cancel: ${id} was already settled; nothing to cancel\n` : decisionLine(id, step.decision));
    return { exitCode: 0 };
  }
  process.stderr.write(`lane cancel: ${id} is still held: the queue has not proven the stop (${step.reason ?? step.state}); retry lane cancel once the runner answers\n`);
  return { exitCode: 1 };
}

export async function reconstructLedgered(root, id, resultPath, ctxFor, { deadlineAt } = {}) {
  const entry = readLedger(root, id);
  if (entry === null || readAttempt(root, id) || fs.existsSync(resultPath)) return false;
  try {
    return (await stepWithin(ctxFor({ id, generation: entry.generation }), deadlineAt))?.state === 'settled';
  } catch (err) {
    if (isTransientError(err)) return false;
    throw err;
  }
}

/** Sends every cancel a ticket owes its queue row (see `abandonUnreachableQueue`); a marker goes only once the queue answered, an outage leaves it for the next sweep. */
export async function sendOwedCancels(root, ctxFor, { phaseMs = OWED_CANCEL_PHASE_MS, now = Date.now } = {}) {
  const results = [];
  const deadline = now() + phaseMs;
  // the markers tried longest ago go first, so one that never delivers cannot keep the rest out of a phase that only reaches a few
  const lastTried = (id) => readOwedCancel(root, id)?.lastTriedAt ?? 0;
  for (const id of owedCancelIds(root).sort((a, b) => lastTried(a) - lastTried(b) || a.localeCompare(b))) {
    if (now() >= deadline) {
      results.push({ id, state: 'deferred', reason: 'the owed-cancel phase is out of time' });
      continue;
    }
    try {
      const owed = readOwedCancel(root, id);
      if (!(await touchOwedCancel(root, id, { lastTriedAt: now() }))) continue;
      await withDeadline(ctxFor({ id, generation: owed?.generation ?? 0 }).queue.cancel(id), Math.max(1, deadline - now()));
      await clearOwedCancel(root, id);
      results.push({ id, state: 'cancel-sent' });
    } catch (err) {
      results.push({ id, state: isTransientError(err) ? 'unreachable' : 'failed', reason: err.message });
    }
  }
  return results;
}

function withDeadline(promise, ms) {
  let timer;
  const expiry = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`no answer within ${ms}ms`)), ms); });
  return Promise.race([promise, expiry]).finally(() => clearTimeout(timer));
}
