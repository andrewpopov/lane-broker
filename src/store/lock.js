import fs from 'node:fs';
import path from 'node:path';

export const LOCK_FILE = 'store.lock';

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === 'EPERM';
  }
}

/**
 * Exclusive ownership of a store root. The serving process holds it for its whole life; an offline tool (e.g.
 * `rebuild-watermark`) that must read the journal directly takes it too, so it refuses while a server is running. A lock
 * whose pid is dead is stale and is taken over.
 */
export function acquireStoreLock(root) {
  const file = path.join(root, LOCK_FILE);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const fd = fs.openSync(file, 'wx', 0o640);
      fs.writeSync(fd, String(process.pid));
      fs.closeSync(fd);
      return { release: () => fs.rmSync(file, { force: true }) };
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      const pid = Number.parseInt(fs.readFileSync(file, 'utf8'), 10);
      if (Number.isInteger(pid) && alive(pid)) throw new Error(`store ${root} is in use by pid ${pid}`);
      fs.rmSync(file, { force: true });
    }
  }
  throw new Error(`could not take the store lock on ${root}`);
}
