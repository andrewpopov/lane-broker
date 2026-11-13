import fs from 'node:fs';
import path from 'node:path';

/**
 * Records, in order, every fsync (sync or callback form), every rename destination and every closed-over file path an fsync hit,
 * by wrapping the `fs` functions the code under test calls. An fd the code opened through `fs.openSync` maps back to its path; one
 * a stream opened is reported by inode. Call `stop()` (a `finally`) to restore the originals.
 *
 * `withLock` fsyncs `lock/owner.json`, `lock/` and the lock's parent right after the rename that publishes the lock. Those belong to lock
 * acquisition, not to the operation under test, so they are not recorded: a spy asking "what did this operation fsync" must not see them.
 */
export function recordFsyncs() {
  const events = [];
  const pathOfFd = new Map();
  const original = { openSync: fs.openSync, closeSync: fs.closeSync, fsyncSync: fs.fsyncSync, fsync: fs.fsync, renameSync: fs.renameSync };
  const inoOf = (fd) => fs.fstatSync(fd).ino;
  let pendingLockFsyncs = [];
  const isLockFsync = (fd) => {
    const hit = pendingLockFsyncs[0] !== undefined && pathOfFd.get(fd) === pendingLockFsyncs[0];
    if (hit) pendingLockFsyncs.shift();
    else pendingLockFsyncs = [];
    return hit;
  };
  fs.openSync = (target, ...rest) => {
    const fd = original.openSync(target, ...rest);
    pathOfFd.set(fd, String(target));
    return fd;
  };
  fs.closeSync = (fd) => {
    pathOfFd.delete(fd);
    return original.closeSync(fd);
  };
  fs.fsyncSync = (fd) => {
    if (isLockFsync(fd)) return original.fsyncSync(fd);
    events.push({ op: 'fsync', path: pathOfFd.get(fd), ino: inoOf(fd) });
    return original.fsyncSync(fd);
  };
  fs.fsync = (fd, cb) => {
    pendingLockFsyncs = [];
    events.push({ op: 'fsync', path: pathOfFd.get(fd), ino: inoOf(fd) });
    return original.fsync(fd, cb);
  };
  fs.renameSync = (from, to) => {
    const lockPublished = path.basename(String(from)).startsWith('.lock-stage-') && path.basename(String(to)) === 'lock';
    pendingLockFsyncs = lockPublished ? [path.join(String(to), 'owner.json'), String(to), path.dirname(String(to))] : [];
    events.push({ op: 'rename', path: String(to) });
    return original.renameSync(from, to);
  };
  return {
    events,
    fsyncedPaths: () => events.filter((e) => e.op === 'fsync' && e.path).map((e) => e.path),
    indexOf: (op, target) => events.findIndex((e) => e.op === op && e.path === target),
    stop() {
      Object.assign(fs, original);
    },
  };
}
