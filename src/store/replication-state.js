import fs from 'node:fs';
import path from 'node:path';
import { atomicWriteJson } from '../state.js';

export const REPLICATION_STATE_FILE = 'replication.json';
export const REPLICATION_STATE_FORMAT = 2;

/**
 * The watermark file `{format: 2, seq, offset, failedRounds, lastRoundAt}`: every journal record up to `seq` (ending at
 * byte `offset`) is verified at the replica. Any other shape is refused rather than guessed at: an offset of 0 beside a
 * nonzero seq would make the replicator misread the journal.
 */
export function readReplicationState(root) {
  const file = path.join(root, REPLICATION_STATE_FILE);
  let text;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return { replicatedSeq: 0, replicatedOffset: 0, failedRounds: 0, lastRoundAt: null };
    throw err;
  }
  let s = null;
  try {
    s = JSON.parse(text);
  } catch {
    // falls through to the refusal below
  }
  if (!s || s.format !== REPLICATION_STATE_FORMAT || !Number.isInteger(s.seq) || !Number.isInteger(s.offset)) {
    throw new Error(`${file} is not a format-${REPLICATION_STATE_FORMAT} watermark; recompute it with: lane-store rebuild-watermark --seq <last verified seq> --root ${root}`);
  }
  return { replicatedSeq: s.seq, replicatedOffset: s.offset, failedRounds: s.failedRounds ?? 0, lastRoundAt: s.lastRoundAt ?? null };
}

export function writeReplicationState(root, state) {
  atomicWriteJson(
    path.join(root, REPLICATION_STATE_FILE),
    { format: REPLICATION_STATE_FORMAT, seq: state.replicatedSeq, offset: state.replicatedOffset, failedRounds: state.failedRounds, lastRoundAt: state.lastRoundAt },
    { fsync: true },
  );
}
