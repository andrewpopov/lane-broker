import fs from 'node:fs';
import path from 'node:path';
import { atomicWriteJson, fsyncFile, readJsonSafe } from './state.js';
import { pullTicketDir } from './pull-envelope.js';

export const INITIAL_ACKED_SEQ = -1;

export const cursorFile = (root, id) => path.join(pullTicketDir(root, id), 'log-cursor.json');

export const readCursor = (root, id) => readJsonSafe(cursorFile(root, id)) ?? null;

export const writeCursor = (root, id, cursor) => atomicWriteJson(cursorFile(root, id), cursor, { fsync: true });

export const fileSize = (logPath) => {
  try {
    return fs.statSync(logPath).size;
  } catch (err) {
    if (err.code === 'ENOENT') return 0;
    throw err;
  }
};

export const newCursor = (epoch, logPath) => ({ epoch, ackedSeq: INITIAL_ACKED_SEQ, logBytes: fileSize(logPath), epochStartBytes: fileSize(logPath) });

// runs before any CappedLogWriter exists (it caches size and truncated state at construction); never truncates upward
export function reconcileLogFile(root, id, logPath, cursor) {
  const size = fileSize(logPath);
  if (size > cursor.logBytes) {
    fs.truncateSync(logPath, cursor.logBytes);
    fsyncFile(logPath);
    return cursor;
  }
  if (size === cursor.logBytes) return cursor;
  const lowered = { ...cursor, logBytes: size, epochStartBytes: Math.min(cursor.epochStartBytes, size), logLossDetected: true };
  writeCursor(root, id, lowered);
  return lowered;
}

// the file's size, not writer.bytes, which stops tracking the file once the cap remainder and truncation marker are written
export function commitCursor(root, id, logPath, cursor, patch = {}) {
  const next = { ...cursor, ...patch, logBytes: fileSize(logPath) };
  writeCursor(root, id, next);
  return next;
}
