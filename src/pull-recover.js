import fs from 'node:fs';
import { readAttempt } from './attempts.js';
import { readLedger } from './pull-ledger.js';
import { isTransientError, stepPullTicket } from './pull-ticket.js';

const CANCEL_STEP_PASSES = 3;
const SETTLED_STATE = { finished: 'adopted', cancelled: 'cancelled', lost: 'lost' };

function reconcileOutcome(step) {
  if (step.state === 'settled') {
    return step.already ? { settled: true, state: 'already-settled' } : { settled: true, state: SETTLED_STATE[step.decision.kind], exit: step.decision.exit };
  }
  if (step.state === 'unresolved') return { settled: false, state: 'ambiguous', reason: step.reason };
  if (step.state === 'stale' || step.state === 'gone' || step.state === 'withdrawn') return { settled: false, state: 'skipped', reason: `pull ticket is ${step.state}` };
  return { settled: false, state: 'running' };
}

// an outage is `unreachable`, never a verdict
export async function reconcilePullAttempt(attempt, ctxFor) {
  try {
    return reconcileOutcome(await stepPullTicket(ctxFor(attempt)));
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

export async function reconstructLedgered(root, id, resultPath, ctxFor) {
  const entry = readLedger(root, id);
  if (entry === null || readAttempt(root, id) || fs.existsSync(resultPath)) return false;
  try {
    return (await stepPullTicket(ctxFor({ id, generation: entry.generation }))).state === 'settled';
  } catch (err) {
    if (isTransientError(err)) return false;
    throw err;
  }
}
