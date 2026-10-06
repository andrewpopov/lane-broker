import fs from 'node:fs';
import path from 'node:path';
import { fsyncDirectory } from '../state.js';
import { readReplicationState } from './replication-state.js';

/**
 * `journal.log`: one JSON line `{seq, kind, path, sha256, size, createdAt, ...}` per committed state change, appended
 * and fsynced AFTER the change itself is durable (spec 8.2). Kinds: `blob`, `manifest` (objects), `terminal`, `pin`
 * (job retention state) and `delete` (retention removed an object). seq is 1-based and gapless.
 *
 * A sequence number is allocated only once the write and fsync both succeeded; a failed append is truncated back to
 * the pre-append offset, so a retry reuses the same seq and no line is ever torn or duplicated.
 */
export const JOURNAL_FILE = 'journal.log';
export const DEFAULT_CHUNK_BYTES = 256 * 1024;

export function journalPath(root) {
  return path.join(root, JOURNAL_FILE);
}

function writeAll(fd, buf) {
  let off = 0;
  while (off < buf.length) {
    const n = fs.writeSync(fd, buf, off, buf.length - off);
    if (!(n > 0)) throw new Error('journal write made no progress');
    off += n;
  }
}

/**
 * Complete records starting at byte `offset`, reading at most `maxBytes` (a torn trailing line is ignored). Each
 * record carries `offset` (its start) and `end`. `expectSeq`, when given, must be the first record's seq: a watermark
 * that no longer matches the file is a hard error, never silently resynchronised.
 */
export function readJournalFrom(root, offset = 0, { maxBytes = DEFAULT_CHUNK_BYTES, expectSeq } = {}) {
  let fd;
  try {
    fd = fs.openSync(journalPath(root), 'r');
  } catch (err) {
    if (err.code === 'ENOENT') return [];
    throw err;
  }
  try {
    const size = fs.fstatSync(fd).size;
    if (offset >= size) return [];
    const buf = Buffer.alloc(Math.min(maxBytes, size - offset));
    let got = 0;
    while (got < buf.length) {
      const n = fs.readSync(fd, buf, got, buf.length - got, offset + got);
      if (n === 0) break;
      got += n;
    }
    const lastNl = buf.subarray(0, got).lastIndexOf(0x0a);
    if (lastNl < 0) {
      if (got === maxBytes) throw new Error('journal record larger than the read chunk');
      return [];
    }
    const out = [];
    let pos = 0;
    while (pos <= lastNl) {
      const nl = buf.indexOf(0x0a, pos);
      const rec = JSON.parse(buf.toString('utf8', pos, nl));
      const want = out.length ? out[out.length - 1].seq + 1 : expectSeq;
      if (want !== undefined && rec.seq !== want) throw new Error(`journal out of step at byte ${offset + pos}: seq ${rec.seq}, expected ${want}`);
      out.push({ ...rec, offset: offset + pos, end: offset + nl + 1 });
      pos = nl + 1;
    }
    return out;
  } finally {
    fs.closeSync(fd);
  }
}

/** Every record from `offset`, read in bounded chunks. */
export function* iterateJournal(root, offset = 0, opts = {}) {
  let at = offset;
  for (;;) {
    const recs = readJournalFrom(root, at, opts);
    if (!recs.length) return;
    yield* recs;
    at = recs[recs.length - 1].end;
  }
}

/** Records with `seq > afterSeq` (a scan from the start: for tools and tests, not for the request path). */
export function readJournal(root, afterSeq = 0, limit = Infinity) {
  const out = [];
  for (const rec of iterateJournal(root)) {
    if (rec.seq <= afterSeq) continue;
    if (out.length >= limit) break;
    out.push(rec);
  }
  return out;
}

/** path -> kind of the LAST record naming it. An object is journaled iff that kind is `blob` or `manifest`. */
export function lastKindByPath(root) {
  const last = new Map();
  for (const rec of iterateJournal(root)) if (rec.path) last.set(rec.path, rec.kind);
  return last;
}

export function journalHeadSeq(root) {
  let seq = 0;
  for (const rec of iterateJournal(root)) seq = rec.seq;
  return seq;
}

/** The single in-process writer. Appends are synchronous, so they are serialised by the event loop. */
export class Journal {
  constructor(root) {
    this.root = root;
    this.file = journalPath(root);
    fs.mkdirSync(root, { recursive: true });
    const existed = fs.existsSync(this.file);
    if (existed) this.dropTornTail();
    this.offset = existed ? fs.statSync(this.file).size : 0;
    this.seq = existed ? this.tailSeq() : 0;
    this.pending = []; // {seq, createdAt} of records above the watermark: all /metrics needs, so it never reads the file
    const mark = readReplicationState(root);
    if (mark.replicatedOffset > this.offset) throw new Error('replication watermark is beyond the end of the journal');
    for (const rec of iterateJournal(root, mark.replicatedOffset)) {
      if (rec.seq > mark.replicatedSeq) this.pending.push({ seq: rec.seq, createdAt: rec.createdAt });
    }
    this.fd = fs.openSync(this.file, 'a', 0o640);
    this.broken = false;
    if (!existed) fsyncDirectory(root);
  }

  tailSeq() {
    if (this.offset === 0) return 0;
    const fd = fs.openSync(this.file, 'r');
    try {
      const probe = Buffer.alloc(Math.min(this.offset, DEFAULT_CHUNK_BYTES));
      fs.readSync(fd, probe, 0, probe.length, this.offset - probe.length);
      const lines = probe.toString('utf8').split('\n');
      return JSON.parse(lines[lines.length - 2]).seq;
    } finally {
      fs.closeSync(fd);
    }
  }

  dropTornTail() {
    const size = fs.statSync(this.file).size;
    const fd = fs.openSync(this.file, 'r+');
    try {
      const probe = Buffer.alloc(Math.min(size, DEFAULT_CHUNK_BYTES));
      fs.readSync(fd, probe, 0, probe.length, size - probe.length);
      if (probe.length && probe[probe.length - 1] !== 0x0a) {
        const nl = probe.lastIndexOf(0x0a);
        if (nl < 0 && probe.length === size) fs.ftruncateSync(fd, 0);
        else if (nl < 0) throw new Error('journal tail has no newline within the probe window');
        else fs.ftruncateSync(fd, size - probe.length + nl + 1);
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  append(fields) {
    if (this.broken) throw new Error('journal is unusable after a failed rollback; restart the store');
    const rec = { seq: this.seq + 1, ...fields };
    const buf = Buffer.from(`${JSON.stringify(rec)}\n`, 'utf8');
    try {
      writeAll(this.fd, buf);
      fs.fsyncSync(this.fd);
    } catch (err) {
      try {
        fs.ftruncateSync(this.fd, this.offset);
      } catch {
        this.broken = true;
      }
      throw err;
    }
    this.seq = rec.seq;
    this.offset += buf.length;
    this.pending.push({ seq: rec.seq, createdAt: rec.createdAt });
    if (this.pending.length % 8192 === 0) this.oldestPendingCreatedAt();
    return { ...rec, offset: this.offset - buf.length, end: this.offset };
  }

  /** `createdAt` of the first record above the watermark (re-read from replication.json), or null when fully replicated. */
  oldestPendingCreatedAt() {
    const { replicatedSeq } = readReplicationState(this.root);
    let drop = 0;
    while (drop < this.pending.length && this.pending[drop].seq <= replicatedSeq) drop += 1;
    if (drop) this.pending.splice(0, drop);
    return this.pending.length ? this.pending[0].createdAt : null;
  }

  close() {
    fs.closeSync(this.fd);
  }
}
