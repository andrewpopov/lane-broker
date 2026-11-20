import { paths, readJsonSafe, stateHome, isTicketId } from './state.js';
import { readLease } from './lease.js';
import { listQueue } from './scheduler.js';
import { readAttempt } from './attempts.js';
import { readPending } from './pending.js';
import { readLedger } from './pull-ledger.js';

export const RESULT_NOT_FINISHED_EXIT = 82;
export const RESULT_UNKNOWN_EXIT = 83;

/** Whether `id` is a ticket the broker knows of, using the same state `lane wait` counts as "found" (read-only). */
function isKnownTicket(root, id) {
  return Boolean(readLease(root, id) || readAttempt(root, id) || readPending(root, id) || listQueue(root).some((t) => t && t.id === id) || readLedger(root, id) !== null);
}

/** `lane result <id> [--json]`: print a finished job's stored result record verbatim. Reads state only. */
export function resultCommand(args, root = stateHome()) {
  const json = args.includes('--json');
  const operands = args.filter((a) => a !== '--json');
  const unknownOption = operands.find((a) => a.startsWith('-'));
  if (unknownOption !== undefined || operands.length !== 1 || !isTicketId(operands[0])) {
    process.stderr.write('lane result: usage: lane result <id> [--json] (exactly one valid ticket id)\n');
    return { exitCode: 2 };
  }
  const id = operands[0];
  const result = readJsonSafe(`${paths(root).results}/${id}.json`);
  if (result) {
    process.stdout.write(json ? `${JSON.stringify(result)}\n` : `lane result: ${id} ${result.signal ? `terminated by signal ${result.signal}` : `exited ${result.exit}`}${result.reused ? ' (reused)' : ''}\n`);
    return { exitCode: 0 };
  }
  if (isKnownTicket(root, id)) {
    process.stderr.write(`lane result: ${id} has not finished yet\n`);
    return { exitCode: RESULT_NOT_FINISHED_EXIT };
  }
  process.stderr.write(`lane result: ${id} is not a known job\n`);
  return { exitCode: RESULT_UNKNOWN_EXIT };
}
