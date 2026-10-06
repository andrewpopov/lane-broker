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
 * Recompute the watermark for operators: find the end offset of journal record `seq` with one scan and write a format-2
 * state. Used after an unrecognised watermark file is refused.
 */
export function rebuildWatermark(root, seq) {
  let offset = 0;
  if (seq > 0) {
    let found = false;
    for (const rec of iterateJournal(root)) {
      if (rec.seq === seq) {
        offset = rec.end;
        found = true;
        break;
      }
    }
    if (!found) throw new Error(`journal has no record with seq ${seq}`);
  }
  const state = { replicatedSeq: seq, replicatedOffset: offset, failedRounds: 0, lastRoundAt: null };
  writeReplicationState(root, state);
  return state;
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

  /** A COMMITTED delete record for `rel` after this record (the journal reader never sees uncommitted bytes). */
  committedDeletion(rel, fromOffset) {
    for (const rec of iterateJournal(this.root, fromOffset, { maxBytes: this.chunkBytes })) {
      if (rec.kind === 'delete' && rec.path === rel) return rec;
    }
    return null;
  }

  /**
   * `rel` is absent on the primary. That is only legitimate when retention committed a delete for it; the replica is
   * then brought to the same state NOW (the delete applied and checked absent there), so the watermark moves past this
   * record only over a replica state that has been verified. No committed delete, or a replica that cannot delete: block.
   */
  async confirmAbsent(entry, rel) {
    if (!this.committedDeletion(rel, entry.end)) {
      throw new Error(`seq ${entry.seq} ${rel} is absent on the primary and no committed deletion explains it`);
    }
    await this.replica.deleteObject(rel);
    if (await this.replica.verifyObject(rel)) throw new Error(`replica still holds ${rel} (seq ${entry.seq})`);
    return 'gone';
  }

  async shipObject(entry) {
    const file = path.join(this.root, ...entry.path.split('/'));
    if (!fs.existsSync(file)) return this.confirmAbsent(entry, entry.path);
    await this.replica.putObject(entry, file);
    const seen = await this.replica.verifyObject(entry.path);
    if (!seen || seen.sha256 !== entry.sha256 || seen.size !== entry.size) {
      throw new Error(`replica verification failed for seq ${entry.seq} ${entry.path}`);
    }
    return 'shipped';
  }

  async shipState(entry) {
    const field = entry.kind === 'pin' ? 'pinned' : 'terminalAt';
    const manifestRel = manifestRelPath(entry.job);
    const applied = entry.kind === 'pin' ? await this.replica.setPin(entry.job, entry.pinned) : await this.replica.setTerminal(entry.job, entry.terminalAt);
    if (!applied) {
      if (fs.existsSync(path.join(this.root, ...manifestRel.split('/')))) {
        throw new Error(`replica has no job ${entry.job} for seq ${entry.seq} but the primary does`);
      }
      return this.confirmAbsent(entry, manifestRel);
    }
    const meta = await this.replica.jobMeta(entry.job);
    if (!meta || meta[field] !== entry[field]) throw new Error(`replica job state differs for seq ${entry.seq} ${entry.kind} ${entry.job}`);
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
