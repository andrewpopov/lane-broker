import fs from 'node:fs';
import path from 'node:path';
import { paths, stateHome, isTicketId, UnreadableRecordError } from './state.js';
import { leaseFile } from './lease.js';
import { listQueueStrict } from './scheduler.js';
import { pendingFile } from './pending.js';
import { ledgerFile, peekLedger } from './pull-ledger.js';

export const RESULT_NOT_FINISHED_EXIT = 82;
export const RESULT_UNKNOWN_EXIT = 83;
export const RESULT_UNREADABLE_EXIT = 84;

const PASSES = 3;
const PASS_PAUSE_MS = 5;

/** The parsed JSON in `file`, null only when it does not exist; anything unreadable or malformed throws `UnreadableRecordError`. */
function readRecordStrict(file) {
  let text;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw new UnreadableRecordError(file, err.message);
  }
  try {
    return JSON.parse(text);
  } catch (err) {
    throw new UnreadableRecordError(file, err.message);
  }
}

function peekLedgerStrict(root, id) {
  try {
    return peekLedger(root, id) !== null;
  } catch (err) {
    throw new UnreadableRecordError(ledgerFile(root, id), err.message);
  }
}

const sleepMs = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

/** The default reads, all lock-free; each throws `UnreadableRecordError` rather than report an unreadable file as absent. */
export const defaultReaders = {
  result: (root, id) => readRecordStrict(`${paths(root).results}/${id}.json`),
  /** Whether `id` is a ticket the broker knows of, using the same state `lane wait` counts as "found". */
  live: (root, id) =>
    [leaseFile(root, id), path.join(paths(root).attempts, `${id}.json`), pendingFile(root, id)].some((file) => readRecordStrict(file)) ||
    listQueueStrict(root).some((t) => t.id === id) ||
    peekLedgerStrict(root, id),
  pause: () => sleepMs(PASS_PAUSE_MS),
};

/**
 * The supervisor writes the result and only then removes the lease, and admission moves a ticket between queue, pending
 * and lease, so one pass can see neither. Result, live state, result again, a few times, before concluding "unknown".
 */
function lookup(root, id, readers) {
  for (let pass = 1; ; pass += 1) {
    const result = readers.result(root, id);
    if (result) return { result };
    if (readers.live(root, id)) return { live: true };
    if (pass === PASSES) break;
    readers.pause();
  }
  const result = readers.result(root, id);
  return result ? { result } : { live: false };
}

/** `lane result <id> [--json]`: print a finished job's stored result record verbatim. Reads state only, takes no lock. */
export function resultCommand(args, root = stateHome(), readers = defaultReaders) {
  const json = args.includes('--json');
  const operands = args.filter((a) => a !== '--json');
  const unknownOption = operands.find((a) => a.startsWith('-'));
  if (unknownOption !== undefined || operands.length !== 1 || !isTicketId(operands[0])) {
    process.stderr.write('lane result: usage: lane result <id> [--json] (exactly one valid ticket id)\n');
    return { exitCode: 2 };
  }
  const id = operands[0];
  let found;
  try {
    found = lookup(root, id, readers);
  } catch (err) {
    if (!(err instanceof UnreadableRecordError)) throw err;
    process.stderr.write(`lane result: state unreadable: ${err.message}\n`);
    return { exitCode: RESULT_UNREADABLE_EXIT };
  }
  if (found.result) {
    const { result } = found;
    process.stdout.write(json ? `${JSON.stringify(result)}\n` : `lane result: ${id} ${result.signal ? `terminated by signal ${result.signal}` : `exited ${result.exit}`}${result.reused ? ' (reused)' : ''}\n`);
    return { exitCode: 0 };
  }
  if (found.live) {
    process.stderr.write(`lane result: ${id} has not finished yet\n`);
    return { exitCode: RESULT_NOT_FINISHED_EXIT };
  }
  process.stderr.write(`lane result: ${id} is not a known job\n`);
  return { exitCode: RESULT_UNKNOWN_EXIT };
}
