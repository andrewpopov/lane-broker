import fs from 'node:fs';
import path from 'node:path';
import { fsyncDirectory } from '../state.js';

/**
 * `journal.log`: one JSON line `{seq, kind, path, sha256, size, createdAt}` per committed object, appended and
 * fsynced AFTER the object itself is durable (spec 8.2). seq is 1-based and gapless, so line N holds seq N and a
 * reader can seek by slicing lines. A crash between object commit and journal append leaves an object with no
 * record; the weekly full compare (compare.js) finds and re-journals those.
 */
export const JOURNAL_FILE = 'journal.log';

export function journalPath(root) {
  return path.join(root, JOURNAL_FILE);
}

function completeLines(root) {
  let text;
  try {
    text = fs.readFileSync(journalPath(root), 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return [];
    throw err;
  }
  const lines = text.split('\n');
  lines.pop(); // text after the last newline is a torn append (or '' when the file ends cleanly)
  return lines;
}

function parseLine(line, expectedSeq) {
  const rec = JSON.parse(line);
  if (rec.seq !== expectedSeq) throw new Error(`journal corrupt: line ${expectedSeq} holds seq ${rec.seq}`);
  return rec;
}

/** Records with `seq > afterSeq`, in order, at most `limit`. Safe to call from another process while the store appends. */
export function readJournal(root, afterSeq = 0, limit = Infinity) {
  const lines = completeLines(root);
  const out = [];
  for (let i = afterSeq; i < lines.length && out.length < limit; i += 1) out.push(parseLine(lines[i], i + 1));
  return out;
}

export function journalHeadSeq(root) {
  return completeLines(root).length;
}

/** The single in-process writer. Appends are synchronous, so they are serialised by the event loop. */
export class Journal {
  constructor(root) {
    this.root = root;
    this.file = journalPath(root);
    fs.mkdirSync(root, { recursive: true });
    const existed = fs.existsSync(this.file);
    if (existed) {
      const buf = fs.readFileSync(this.file);
      const keep = buf.lastIndexOf(0x0a) + 1;
      if (keep < buf.length) fs.truncateSync(this.file, keep); // drop a torn trailing append from a crash
    }
    this.seq = journalHeadSeq(root);
    this.fd = fs.openSync(this.file, 'a', 0o640);
    if (!existed) fsyncDirectory(root);
  }

  append({ kind, path: relPath, sha256, size, createdAt }) {
    const rec = { seq: this.seq + 1, kind, path: relPath, sha256, size, createdAt };
    fs.writeSync(this.fd, `${JSON.stringify(rec)}\n`);
    fs.fsyncSync(this.fd);
    this.seq = rec.seq;
    return rec;
  }

  close() {
    fs.closeSync(this.fd);
  }
}
