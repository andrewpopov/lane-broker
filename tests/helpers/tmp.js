import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { isPidAlive } from '../../src/process-liveness.js';

/**
 * The one way a test makes a scratch directory (BRAIN-420). Every dir is removed again when the
 * test (if a context `t` is passed) or the test-file process ends, pass or fail, and is recorded
 * in a per-run manifest so tests/zz-tmp-leak-guard.test.js can prove nothing survived.
 *
 * `node --test` runs each file in its own child process; all of them share one parent, so the
 * manifest is keyed by process.ppid. process.on('exit') runs for a clean end, a failed test and an
 * uncaught exception alike.
 */
const created = new Set();
let exitHookInstalled = false;

export const manifestPath = (runnerPid = process.ppid) => path.join(os.tmpdir(), `lane-broker-tmp-manifest-${runnerPid}`);

// Some fixtures are read-only on purpose (a deps cache, an artifacts dir the test made unwritable),
// and rmSync cannot unlink inside a read-only directory.
function makeWritable(entry) {
  const stat = fs.lstatSync(entry);
  if (stat.isSymbolicLink()) return;
  fs.chmodSync(entry, stat.mode | 0o700);
  if (stat.isDirectory()) for (const name of fs.readdirSync(entry)) makeWritable(path.join(entry, name));
}

// A test's own child (supervisor, runner) can still be writing into its state dir as the test
// process exits, which makes the first rmSync lose with ENOTEMPTY; retry lets that child finish.
const RM = { recursive: true, force: true, maxRetries: 20, retryDelay: 100 };

export function removeTmpDir(dir) {
  try {
    // No retries on the first pass: a read-only tree fails with EPERM, which rmSync retries for the
    // full maxRetries x retryDelay (2s a dir) before makeWritable below could ever help.
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    try {
      makeWritable(dir);
      fs.rmSync(dir, RM);
    } catch {
      // left in place: the leak guard reports it by name
    }
  }
}

// Tests end while a `lane run` they detached (or never awaited) is still finishing, and that child
// writes history/results into its state dir after the dir is gone, resurrecting it. A child's
// environment carries the dir path (LANE_BROKER_HOME/STATE, cwd args), so wait until no other
// process mentions any of our dirs, then remove. Bounded: a stuck child must not hang the suite.
const SETTLE_MS = 2000;
const sleepMs = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

// pids of other processes whose argv or environment names one of our dirs. A process matches only by naming a
// mkdtemp path unique to this run; this process and its ancestors are never returned.
function processesMentioning(dirs) {
  let listing;
  try {
    listing = execFileSync('ps', ['axeww', '-o', 'pid=,ppid=,command='], { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 });
  } catch {
    return [];
  }
  const rows = listing.split('\n').map((line) => {
    const [pid, ppid] = line.trim().split(/\s+/, 2).map(Number);
    return { pid, ppid, line };
  }).filter((row) => row.pid);
  const parentOf = new Map(rows.map((row) => [row.pid, row.ppid]));
  const protectedPids = new Set([process.pid]);
  for (let pid = parentOf.get(process.pid); pid > 1 && !protectedPids.has(pid); pid = parentOf.get(pid)) protectedPids.add(pid);
  return rows.filter((row) => !protectedPids.has(row.pid) && dirs.some((dir) => row.line.includes(dir))).map((row) => row.pid);
}

function killStragglers(pids) {
  for (const pid of pids) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // already gone
    }
  }
  const deadline = Date.now() + 1000;
  while (Date.now() < deadline && pids.some(isPidAlive)) sleepMs(25);
}

function removeAll() {
  const dirs = [...created];
  const deadline = Date.now() + SETTLE_MS;
  while (Date.now() < deadline && processesMentioning(dirs).length > 0) sleepMs(100);
  killStragglers(processesMentioning(dirs));
  for (const dir of dirs) removeTmpDir(dir);
  created.clear();
}

export function makeTmpDir(prefix, t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  created.add(dir);
  fs.appendFileSync(manifestPath(), `${dir}\n`);
  if (t) t.after(() => removeTmpDir(dir));
  if (!exitHookInstalled) {
    exitHookInstalled = true;
    process.on('exit', removeAll);
  }
  return dir;
}
