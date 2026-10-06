import fs from 'node:fs';
import path from 'node:path';
import { atomicWriteJson, readJsonSafe } from '../state.js';
import { readJournal, journalHeadSeq } from './journal.js';

export const REPLICATION_STATE_FILE = 'replication.json';

export function readReplicationState(root) {
  const s = readJsonSafe(path.join(root, REPLICATION_STATE_FILE)) ?? {};
  return { replicatedSeq: s.replicatedSeq ?? 0, failedRounds: s.failedRounds ?? 0, lastRoundAt: s.lastRoundAt ?? null };
}

function writeState(root, state) {
  atomicWriteJson(path.join(root, REPLICATION_STATE_FILE), state, { fsync: true });
}

/**
 * The monitored recovery point (spec 8.2): age of the first journal entry above `replicatedSeq`, 0 when none.
 * Computed from the journal and watermark on every call, so it keeps growing while the replicator is dead.
 */
export function replicationLag(root, now = Date.now()) {
  const state = readReplicationState(root);
  const [first] = readJournal(root, state.replicatedSeq, 1);
  return {
    oldestUnreplicatedAgeSeconds: first ? Math.max(0, (now - first.createdAt) / 1000) : 0,
    replicatedSeq: state.replicatedSeq,
    headSeq: journalHeadSeq(root),
    failedRounds: state.failedRounds,
  };
}

/**
 * Ships journal entries to a replica in `seq` order. The watermark (`replicatedSeq`) moves past an entry only after
 * the replica has re-read the object from its own disk and reported the journal's sha256 and size. `replica` is
 * `{ putObject(entry, file), verifyObject(relPath) }` (a StoreClient). Shipping is idempotent, so a crash at any
 * point just re-ships from the persisted watermark.
 */
export class Replicator {
  constructor({ root, replica, now = Date.now, batch = 256, onShipped }) {
    Object.assign(this, { root: path.resolve(root), replica, now, batch, onShipped });
  }

  async ship(entry) {
    const file = path.join(this.root, ...entry.path.split('/'));
    if (!fs.existsSync(file)) return 'gone'; // retention removed it before it was shipped
    await this.replica.putObject(entry, file);
    const seen = await this.replica.verifyObject(entry.path);
    if (!seen || seen.sha256 !== entry.sha256 || seen.size !== entry.size) {
      throw new Error(`replica verification failed for seq ${entry.seq} ${entry.path}`);
    }
    return 'shipped';
  }

  /** Drain the journal to its current end. Entries committed while this runs have higher seq and are picked up by the same loop. */
  async runOnce() {
    const state = readReplicationState(this.root);
    let shipped = 0;
    let gone = 0;
    try {
      for (;;) {
        const entries = readJournal(this.root, state.replicatedSeq, this.batch);
        if (!entries.length) break;
        for (const entry of entries) {
          if ((await this.ship(entry)) === 'gone') gone += 1;
          else shipped += 1;
          state.replicatedSeq = entry.seq;
          writeState(this.root, state);
          if (this.onShipped) await this.onShipped(entry);
        }
      }
    } catch (err) {
      writeState(this.root, { ...state, failedRounds: state.failedRounds + 1, lastRoundAt: this.now() });
      return { ok: false, shipped, gone, error: err.message, replicatedSeq: state.replicatedSeq };
    }
    writeState(this.root, { ...state, failedRounds: 0, lastRoundAt: this.now() });
    return { ok: true, shipped, gone, replicatedSeq: state.replicatedSeq };
  }
}
