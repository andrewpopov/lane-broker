import fs from 'node:fs';

/**
 * Records, in order, every fsync (sync or callback form), every rename destination and every closed-over file path an fsync hit,
 * by wrapping the `fs` functions the code under test calls. An fd the code opened through `fs.openSync` maps back to its path; one
 * a stream opened is reported by inode. Call `stop()` (a `finally`) to restore the originals.
 */
export function recordFsyncs() {
  const events = [];
  const pathOfFd = new Map();
  const original = { openSync: fs.openSync, closeSync: fs.closeSync, fsyncSync: fs.fsyncSync, fsync: fs.fsync, renameSync: fs.renameSync };
  const inoOf = (fd) => fs.fstatSync(fd).ino;
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
    events.push({ op: 'fsync', path: pathOfFd.get(fd), ino: inoOf(fd) });
    return original.fsyncSync(fd);
  };
  fs.fsync = (fd, cb) => {
    events.push({ op: 'fsync', path: pathOfFd.get(fd), ino: inoOf(fd) });
    return original.fsync(fd, cb);
  };
  fs.renameSync = (from, to) => {
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
