import fs from 'node:fs';
import path from 'node:path';
import { readJournalFrom, iterateJournal, DEFAULT_CHUNK_BYTES } from './journal.js';
import { readReplicationState, writeReplicationState, REPLICATION_STATE_FILE } from './replication-state.js';
import { manifestRelPath } from './ids.js';

export { readReplicationState, REPLICATION_STATE_FILE };

/**
 * The monitored recovery point (spec 8.2) computed from files, for tools and tests: age of the first journal record
 * above the watermark, 0 when none. The running store computes the same number from its in-memory tail.
 */
export function replicationLag(root, now = Date.now()) {
  const state = readReplicationState(root);
  const [first] = readJournalFrom(root, state.replicatedOffset, { expectSeq: state.replicatedSeq + 1 });
  return {
    oldestUnreplicatedAgeSeconds: first ? Math.max(0, (now - first.createdAt) / 1000) : 0,
    replicatedSeq: state.replicatedSeq,
    failedRounds: state.failedRounds,
  };
}

/**
 * Ships journal records to a replica in `seq` order, reading forward from the watermark's byte offset in bounded
 * chunks. Every record kind is applied and then CHECKED at the replica (objects: the replica re-reads its disk and must
 * report the journal's sha256/size; deletions: the object must be absent; terminal/pin: the replica's job state must
 * match) before the watermark moves past it. `replica` is a StoreClient. Shipping is idempotent, so a crash anywhere just
 * re-ships from the persisted watermark.
 *
 * An object missing on the primary is NOT a skip: the only legitimate absence is retention, which leaves a journaled
 * `delete` record after it. Anything else (disk loss, an operator rm) blocks the watermark and the age keeps growing.
 */
export class Replicator {
  constructor({ root, replica, now = Date.now, chunkBytes = DEFAULT_CHUNK_BYTES, onShipped }) {
    Object.assign(this, { root: path.resolve(root), replica, now, chunkBytes, onShipped });
  }

  deletedLater(entry) {
    for (const rec of iterateJournal(this.root, entry.end, { maxBytes: this.chunkBytes })) {
      if (rec.kind === 'delete' && rec.path === entry.path) return true;
    }
    return false;
  }

  async shipObject(entry) {
    const file = path.join(this.root, ...entry.path.split('/'));
    if (!fs.existsSync(file)) {
      if (this.deletedLater(entry)) return 'gone';
      throw new Error(`seq ${entry.seq} ${entry.path} is missing on the primary and no journaled deletion explains it`);
    }
    await this.replica.putObject(entry, file);
    const seen = await this.replica.verifyObject(entry.path);
    if (!seen || seen.sha256 !== entry.sha256 || seen.size !== entry.size) {
      throw new Error(`replica verification failed for seq ${entry.seq} ${entry.path}`);
    }
    return 'shipped';
  }

  async shipState(entry) {
    const field = entry.kind === 'pin' ? 'pinned' : 'terminalAt';
    const applied = entry.kind === 'pin' ? await this.replica.setPin(entry.job, entry.pinned) : await this.replica.setTerminal(entry.job, entry.terminalAt);
    const meta = await this.replica.jobMeta(entry.job);
    const jobGoneHere = !fs.existsSync(path.join(this.root, ...manifestRelPath(entry.job).split('/')));
    if (meta ? meta[field] !== entry[field] : !(jobGoneHere && !applied)) {
      throw new Error(`replica job state differs for seq ${entry.seq} ${entry.kind} ${entry.job}`);
    }
    return 'shipped';
  }

  async shipDelete(entry) {
    await this.replica.deleteObject(entry.path);
    if (await this.replica.verifyObject(entry.path)) throw new Error(`replica still holds ${entry.path} (seq ${entry.seq})`);
    return 'shipped';
  }

  ship(entry) {
    if (entry.kind === 'blob' || entry.kind === 'manifest') return this.shipObject(entry);
    if (entry.kind === 'delete') return this.shipDelete(entry);
    if (entry.kind === 'terminal' || entry.kind === 'pin') return this.shipState(entry);
    throw new Error(`unknown journal record kind ${entry.kind} at seq ${entry.seq}`);
  }

  /** Drain the journal to its current end. Records committed while this runs have higher seq and are picked up by the same loop. */
  async runOnce() {
    const state = readReplicationState(this.root);
    let shipped = 0;
    let gone = 0;
    try {
      for (;;) {
        const entries = readJournalFrom(this.root, state.replicatedOffset, { maxBytes: this.chunkBytes, expectSeq: state.replicatedSeq + 1 });
        if (!entries.length) break;
        for (const entry of entries) {
          if ((await this.ship(entry)) === 'gone') gone += 1;
          else shipped += 1;
          state.replicatedSeq = entry.seq;
          state.replicatedOffset = entry.end;
          writeReplicationState(this.root, state);
          if (this.onShipped) await this.onShipped(entry);
        }
      }
    } catch (err) {
      writeReplicationState(this.root, { ...state, failedRounds: state.failedRounds + 1, lastRoundAt: this.now() });
      return { ok: false, shipped, gone, error: err.message, replicatedSeq: state.replicatedSeq };
    }
    writeReplicationState(this.root, { ...state, failedRounds: 0, lastRoundAt: this.now() });
    return { ok: true, shipped, gone, replicatedSeq: state.replicatedSeq };
  }
}
