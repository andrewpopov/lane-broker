import fs from 'node:fs';
import path from 'node:path';
import { iterateJournal, DEFAULT_CHUNK_BYTES } from './journal.js';
import { acquireStoreLock } from './lock.js';
import { readReplicationState, writeReplicationState, REPLICATION_STATE_FILE } from './replication-state.js';
import { manifestRelPath } from './ids.js';

export { readReplicationState, REPLICATION_STATE_FILE };

/**
 * The monitored recovery point (spec 8.2): age of the first committed journal record above the watermark, 0 when none.
 * (The running store computes the same number from its in-memory tail for /metrics.)
 */
export function replicationLag(store, now = Date.now()) {
  const state = readReplicationState(store.root);
  const [first] = store.journal.read(state.replicatedOffset, { expectSeq: state.replicatedSeq + 1 });
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
  const lock = acquireStoreLock(root); // offline only: refuses while a server holds the store
  try {
    return rebuildLocked(root, seq);
  } finally {
    lock.release();
  }
}

function rebuildLocked(root, seq) {
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
 * Ships journal records to a replica in `seq` order, reading forward from the watermark's byte offset in bounded chunks.
 * Only information is shipped: puts (blobs, manifests) and pin/terminal state. Deletions are NEVER replicated (the replica
 * runs its own time-based retention), so nothing the replica holds can be destroyed on the primary's say-so. Each shipped
 * record is CHECKED at the replica (objects: the replica re-reads its disk and must report the journal's sha256/size;
 * state: the replica's job state must match) before the watermark moves past it. Shipping is idempotent, so a crash
 * anywhere just re-ships from the persisted watermark.
 *
 * A put whose object is missing on the primary is skipped only when retention committed a `delete` for it later in the
 * journal; otherwise it is a loss (or an unexplained absence) and the watermark blocks there.
 */
export class Replicator {
  /** `store` is the serving ObjectStore: the replicator is a loop INSIDE that process and reads the journal only through
   *  `store.journal`, which stops at the committed offset. */
  constructor({ store, replica, now = Date.now, chunkBytes = DEFAULT_CHUNK_BYTES, onShipped }) {
    Object.assign(this, { store, root: store.root, replica, now, chunkBytes, onShipped, running: false, current: null, stopped: false });
  }

  /** A COMMITTED delete record for `rel` after this record. */
  committedDeletion(rel, fromOffset) {
    for (const rec of this.store.journal.iterate(fromOffset, { maxBytes: this.chunkBytes })) {
      if (rec.kind === 'delete' && rec.path === rel) return rec;
    }
    return null;
  }

  async shipObject(entry) {
    const file = path.join(this.root, ...entry.path.split('/'));
    if (!fs.existsSync(file)) {
      if (this.committedDeletion(entry.path, entry.end)) return 'skipped'; // retention removed it; the replica deletes nothing for us
      throw new Error(`seq ${entry.seq} ${entry.path} is missing on the primary and no committed deletion explains it`);
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
    const manifestRel = manifestRelPath(entry.job);
    const applied = entry.kind === 'pin' ? await this.replica.setPin(entry.job, entry.pinned) : await this.replica.setTerminal(entry.job, entry.terminalAt);
    if (!applied) {
      // the replica has no such job: its own retention swept it (only possible once terminal), or it never arrived
      const here = fs.existsSync(path.join(this.root, ...manifestRel.split('/')));
      if (here && this.store.meta(entry.job).terminalAt != null) return 'skipped';
      if (!here && this.committedDeletion(manifestRel, entry.end)) return 'skipped';
      throw new Error(`replica has no job ${entry.job} for seq ${entry.seq} ${entry.kind} and nothing explains it`);
    }
    const meta = await this.replica.jobMeta(entry.job);
    if (!meta || meta[field] !== entry[field]) throw new Error(`replica job state differs for seq ${entry.seq} ${entry.kind} ${entry.job}`);
    return 'shipped';
  }

  ship(entry) {
    if (entry.kind === 'blob' || entry.kind === 'manifest') return this.shipObject(entry);
    if (entry.kind === 'terminal' || entry.kind === 'pin') return this.shipState(entry);
    if (entry.kind === 'delete' || entry.kind === 'delete-intent' || entry.kind === 'delete-cancel') return 'local'; // never shipped
    throw new Error(`unknown journal record kind ${entry.kind} at seq ${entry.seq}`);
  }

  /** Drain the journal to its current end. Records committed while this runs have higher seq and are picked up by the same loop. */
  async runOnce() {
    if (this.running || this.stopped) return { ok: true, skipped: true };
    this.running = true;
    this.current = this.round();
    try {
      return await this.current;
    } finally {
      this.running = false;
    }
  }

  /** Refuse new rounds and wait for the one in flight (bounded by the client's request deadline). */
  async stop() {
    this.stopped = true;
    await this.current?.catch(() => {});
  }

  async round() {
    const state = readReplicationState(this.root);
    let shipped = 0;
    let gone = 0;
    try {
      for (;;) {
        const entries = this.store.journal.read(state.replicatedOffset, { maxBytes: this.chunkBytes, expectSeq: state.replicatedSeq + 1 });
        if (!entries.length) break;
        for (const entry of entries) {
          const outcome = await this.ship(entry);
          if (outcome === 'shipped') shipped += 1;
          else gone += outcome === 'skipped' ? 1 : 0;
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

/** The primary's replication loop: one round every `intervalMs`, in the serving process. Returns `{replicator, stop}`. */
export function startReplication({ store, replica, intervalMs, now = Date.now, log = console.error }) {
  const replicator = new Replicator({ store, replica, now });
  const timer = setInterval(() => {
    replicator.runOnce().then((r) => {
      if (!r.ok) log(`lane-store: replication round failed: ${r.error}`);
    }, (err) => log(`lane-store: replication round crashed: ${err.message}`));
  }, intervalMs);
  timer.unref();
  return {
    replicator,
    stop: async () => {
      clearInterval(timer);
      await replicator.stop();
    },
  };
}
