import fs from 'node:fs';
import path from 'node:path';
import { readAttempt } from './attempts.js';
import { enqueueAckFile } from './pull-enqueue.js';
import { ledgerFile } from './pull-ledger.js';
import { pendingFile } from './pending.js';
import { existingTicket } from './scheduler.js';
import { paths } from './state.js';

const PER_ID_DIRS = ['cancel', 'expire', 'withdraw', 'superseded', 'released', 'waits', 'logs'];
const claimsDir = (root) => path.join(root, 'submission-claims');

/** Whether this machine holds any per-id artifact for `id`: a ticket, attempt, result, pull directory, ledger entry (which GC can leave alone), pending record, marker, log or claim. */
export function hasLocalRecord(root, id) {
  if (existingTicket(root, id) !== null || readAttempt(root, id) !== null) return true;
  const files = [path.join(paths(root).pull, id), ledgerFile(root, id), enqueueAckFile(root, id), pendingFile(root, id), path.join(claimsDir(root), id)];
  if (files.some((file) => fs.existsSync(file))) return true;
  return PER_ID_DIRS.some((dir) => {
    try {
      return fs.readdirSync(path.join(root, dir)).some((name) => name === id || name.startsWith(`${id}.`));
    } catch {
      return false;
    }
  });
}

/** Takes the id for one placed submission, atomically: true for the one caller that created the claim, false for any other. */
export function claimSubmissionId(root, id) {
  fs.mkdirSync(claimsDir(root), { recursive: true });
  try {
    fs.writeFileSync(path.join(claimsDir(root), id), `${process.pid}\n`, { flag: 'wx' });
    return true;
  } catch (err) {
    if (err.code === 'EEXIST') return false;
    throw err;
  }
}
