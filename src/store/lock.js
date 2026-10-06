import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import net from 'node:net';

export const LOCK_FILE = 'store.lock';

/** The lock path no longer names the file this process linked: another process owns (or reclaimed) the store. */
export class LockLostError extends Error {
  constructor(message) {
    super(message);
    this.name = 'LockLostError';
  }
}

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === 'EPERM';
  }
}

function readLock(file) {
  try {
    const owner = JSON.parse(fs.readFileSync(file, 'utf8'));
    return Number.isInteger(owner.pid) && typeof owner.token === 'string' ? owner : null;
  } catch {
    return null;
  }
}

/**
 * The owner keeps an open fd on its lock file. `assertHeld()` compares that fd's inode/device with whatever `store.lock`
 * names NOW and throws LockLostError on a mismatch or a missing path: a displaced owner (whose lock was reclaimed out from
 * under it) is fenced from writing, whatever happened during the reclaim race.
 */
function holder(file, mine) {
  const fd = fs.openSync(file, 'r');
  const held = fs.fstatSync(fd);
  let closed = false;
  return {
    assertHeld() {
      let now;
      try {
        now = fs.statSync(file);
      } catch (err) {
        if (err.code === 'ENOENT') throw new LockLostError(`store lock ${file} is gone`);
        throw err;
      }
      if (now.ino !== held.ino || now.dev !== held.dev) throw new LockLostError(`store lock ${file} now belongs to another process`);
    },
    release() {
      if (closed) return;
      closed = true;
      try {
        if (readLock(file)?.token === mine.token) fs.rmSync(file, { force: true });
      } finally {
        fs.closeSync(fd);
      }
    },
  };
}

/**
 * Exclusive ownership of a store root, taken atomically WITH its content: `{pid, token}` is written to a private temp file
 * and hard-linked to `store.lock` (`link` fails with EEXIST if the lock exists), so the lock file never exists empty or
 * partial. A lock whose pid is dead is reclaimed by renaming it aside (exactly one reclaimer's rename succeeds), re-reading
 * the renamed file to confirm it really was the stale lock, and retrying the link. The serving process holds the lock for
 * its life; an offline tool that must read the journal directly takes it too, so it refuses while a server runs.
 */
export function acquireStoreLock(root) {
  const file = path.join(root, LOCK_FILE);
  const mine = { pid: process.pid, token: crypto.randomBytes(8).toString('hex') };
  const temp = `${file}.tmp.${process.pid}.${mine.token}`;
  fs.writeFileSync(temp, JSON.stringify(mine), { flag: 'wx', mode: 0o640 });
  try {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        fs.linkSync(temp, file);
        return holder(file, mine);
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
      }
      const seen = readLock(file);
      if (!seen) throw new Error(`store lock ${file} is unreadable; remove it by hand once no server is running`);
      if (alive(seen.pid)) throw new Error(`store ${root} is in use by pid ${seen.pid}`);
      const aside = `${file}.stale.${process.pid}`;
      try {
        fs.renameSync(file, aside);
      } catch (err) {
        if (err.code === 'ENOENT') continue; // another reclaimer won; retry the link
        throw err;
      }
      const taken = readLock(aside);
      if (taken?.token !== seen.token) {
        // we renamed a lock that was not the stale one we judged (a live owner re-acquired in between): put it back
        try {
          fs.linkSync(aside, file);
        } catch {
          // someone else already owns the lock now; the displaced owner will fail its release check
        }
        fs.rmSync(aside, { force: true });
        throw new Error(`store ${root} lock changed hands while it was being reclaimed; retry`);
      }
      fs.rmSync(aside, { force: true });
    }
    throw new Error(`could not take the store lock on ${root}`);
  } finally {
    fs.rmSync(temp, { force: true });
  }
}

/** `abstract-socket` on Linux (the production hosts), the best-effort `file` lock everywhere else (macOS dev and tests). */
export function lockMode(platform = process.platform) {
  return platform === 'linux' ? 'abstract-socket' : 'file';
}

/**
 * Linux: the kernel holds the lock. A process binds a Linux ABSTRACT Unix socket named after the store root; `bind` is atomic,
 * EADDRINUSE means a live owner, and the kernel releases the name the instant the process dies (even SIGKILL). No stale
 * state, no reclaim, no check-then-write race, so `assertHeld` has nothing to check. Abstract sockets exist only on Linux.
 */
export function acquireKernelLock(root) {
  const name = `\0lane-store:${crypto.createHash('sha256').update(fs.realpathSync(root)).digest('hex').slice(0, 32)}`;
  return new Promise((resolve, reject) => {
    const server = net.createServer((conn) => conn.destroy());
    server.once('error', (err) => {
      reject(err.code === 'EADDRINUSE' ? new Error(`store ${root} is in use by another live process`) : err);
    });
    server.listen(name, () => {
      server.unref();
      resolve({
        assertHeld() {},
        release: () => (server.listening ? new Promise((done) => server.close(() => done())) : Promise.resolve()),
      });
    });
  });
}

/** Take the platform's store lock. The handle's `release()` may return a promise: await it before re-opening the root. */
export async function acquireStoreLockAsync(root) {
  return lockMode() === 'abstract-socket' ? acquireKernelLock(root) : acquireStoreLock(root);
}
