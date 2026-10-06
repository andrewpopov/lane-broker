import path from 'node:path';
import { atomicWriteJson, readJsonSafe } from '../state.js';

export const REPLICATION_STATE_FILE = 'replication.json';

/** The watermark: every journal record up to `replicatedSeq` (ending at byte `replicatedOffset`) is hash-verified at the replica. */
export function readReplicationState(root) {
  const s = readJsonSafe(path.join(root, REPLICATION_STATE_FILE)) ?? {};
  return {
    replicatedSeq: s.replicatedSeq ?? 0,
    replicatedOffset: s.replicatedOffset ?? 0,
    failedRounds: s.failedRounds ?? 0,
    lastRoundAt: s.lastRoundAt ?? null,
  };
}

export function writeReplicationState(root, state) {
  atomicWriteJson(path.join(root, REPLICATION_STATE_FILE), state, { fsync: true });
}
