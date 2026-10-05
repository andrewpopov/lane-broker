import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { paths, stateHome, withLock, atomicWriteJson, atomicWriteFile, fsyncDirectory, queueFenced, readJsonSafe, isPidAlive, UnreadableRecordError, QUEUE_FENCE_NOTE } from './state.js';
import { listLeasesStrict, LEASE_STATE } from './lease.js';
import { listAttemptsStrict } from './attempts.js';
import { listQueueStrict } from './scheduler.js';
import { readSchedulerFence, resolveScheduler, SCHEDULER_V2_VERSION } from './fairness.js';
import { logResourceEvent } from './admission.js';

/**
 * BRAIN-380 slice 3: `lane migrate-scheduler`, the explicit, drained, fail-closed cutover that writes the scheduler
 * fence (`sched-v2.json`) slice 2 checks for. Everything runs in ONE hold of the broker lock:
 *
 *   marker (`migrating`) -> quiescence checks -> fairness-v2.json -> sched-v2.json (the commit point) -> archive the
 *   legacy singleton files -> remove the marker.
 *
 * The marker makes every new-code admission entry point refuse with exit 75 (`assertNotMigrating`). The checks read
 * leases, attempts and the queue with the STRICT listings, which throw on an unreadable record where the lenient ones
 * skip it: a record the command cannot read is a record it cannot prove drained. Both cutover writes fsync the file and
 * its directory, so the fence can never be durable ahead of the fairness file it depends on.
 */

/** Scripts a lane-broker process runs. `bin/lane.js` also covers the `remote-exec` and `remote-pipeline` subcommands. */
const LANE_SCRIPT = /(^|\/)(bin\/lane\.js|src\/supervisor\.js|src\/remote-pipeline\.js)$/;
const COMMAND_PREVIEW_CHARS = 160;

/** The process table as `{pid, ppid, command}` rows. Throws if it cannot be read (the caller fails closed). */
export function readProcessTable() {
  const out = execFileSync('ps', ['-axww', '-o', 'pid=,ppid=,command='], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  const rows = [];
  for (const line of out.split('\n')) {
    const m = line.match(/^\s*(\d+)\s+(\d+)\s+(.*\S)\s*$/);
    if (m) rows.push({ pid: Number(m[1]), ppid: Number(m[2]), command: m[3] });
  }
  return rows;
}

/**
 * Does this command line run lane-broker, whatever its version or install path? EVERY argv token is examined, so
 * `node --require x.js /old/bin/lane.js` and a wrapper script are caught as well as `node bin/lane.js`. A token counts
 * when it, or the file it resolves to through symlinks, ends in one of the lane scripts; a `lane` token that cannot be
 * resolved counts too (unknown is treated as old). This is defence in depth: the queue fence (see `fenceLegacyQueue`)
 * is what actually stops old code, including a process launched after this table was read.
 */
export function runsLaneBroker(command) {
  return command
    .trim()
    .split(/\s+/)
    .some((token) => {
      if (LANE_SCRIPT.test(token)) return true;
      if (!token.includes('/') && token !== 'lane') return false;
      try {
        return LANE_SCRIPT.test(fs.realpathSync(token));
      } catch {
        return /(^|\/)lane$/.test(token);
      }
    });
}

/** Pids of `pid` and every ancestor, from the table itself. */
function selfAndAncestors(rows, pid) {
  const parentOf = new Map(rows.map((r) => [r.pid, r.ppid]));
  const ids = new Set();
  for (let at = pid; at && !ids.has(at); at = parentOf.get(at)) ids.add(at);
  return ids;
}

const preview = (command) => (command.length > COMMAND_PREVIEW_CHARS ? `${command.slice(0, COMMAND_PREVIEW_CHARS)}...` : command);

/** One entry per precondition: `{name, failures: [string]}`. Never short-circuits, so a dry run reports every one. */
function checkPreconditions(root, { readProcesses, pid }) {
  const p = paths(root);
  const checks = [];
  const check = (name, fn) => {
    try {
      checks.push({ name, failures: fn() });
    } catch (err) {
      if (!(err instanceof UnreadableRecordError)) throw err;
      checks.push({ name, failures: [`unreadable ${name} record ${err.message}`] });
    }
  };

  check('paused', () => (fs.existsSync(p.pause) ? [] : ['the broker is not paused; run `lane pause` first']));
  check('queue layout', () => {
    const type = queueEntryType(root);
    return ['directory', 'absent', 'fence'].includes(type) ? [] : [`queue/ is ${type}; old and new code would not agree on the queue. Fix it by hand before migrating`];
  });
  check('queue', () => {
    const queued = listQueueStrict(root);
    return queued.length === 0 ? [] : [`queue is not empty: ${queued.length} queued ticket(s): ${queued.map((t) => t.id ?? '(no id)').join(', ')}`];
  });
  check('lease', () =>
    listLeasesStrict(root)
      .filter((l) => l.state !== LEASE_STATE.DONE)
      .map((l) => `lease ${l.id ?? '(no id)'} is ${l.state ?? 'in an unknown state'}`),
  );
  // publishTerminal removes an attempt record when the attempt ends, so a record that is still on disk is not terminal.
  check('attempt', () => listAttemptsStrict(root).map((a) => `attempt ${a.id ?? '(no id)'} is still in phase ${a.phase ?? 'unknown'} (supervisor pid ${a.supervisor?.pid ?? 'unknown'})`));
  check('process', () => {
    let rows;
    try {
      rows = readProcesses();
    } catch (err) {
      return [`the process table could not be read (${err.message}); refusing rather than assuming no lane process is alive`];
    }
    const excluded = selfAndAncestors(rows, pid);
    return rows.filter((r) => !excluded.has(r.pid) && runsLaneBroker(r.command)).map((r) => `lane process pid ${r.pid} is alive: ${preview(r.command)}`);
  });
  return checks;
}

/** A marker left by a migrator that crashed is taken over; one whose owner is alive is another migration in flight. */
function liveMigrator(root, pid) {
  const marker = readJsonSafe(paths(root).migrating);
  return fs.existsSync(paths(root).migrating) && Number.isInteger(marker?.pid) && marker.pid !== pid && isPidAlive(marker.pid) ? marker.pid : null;
}

/**
 * The fence OLD code fails on by itself. Called under the lock at a drained point, after fairness-v2.json is durable:
 * the (empty) legacy `queue/` directory is renamed to `queue.legacy-<ts>/` and a regular FILE named `queue` takes its
 * place. Old code's `mkdir queue` and every queue write beneath it then fail (EEXIST/ENOTDIR), and its `listQueue` sees
 * nothing, so an old process can never get a ticket into the queue and therefore never be admitted (old `tryStart`
 * starts only a ticket it finds in the queue). New code keeps its queue in `queue-v2/` (`paths().queue`). `seq` stays
 * shared. Idempotent: a `queue` that is already the file means an earlier run got this far.
 */
export function fenceLegacyQueue(root, stamp, { afterRename = () => {} } = {}) {
  const legacy = path.join(root, 'queue');
  const pending = path.join(root, 'queue.fence.tmp');
  const type = queueEntryType(root);
  if (type === 'fence') {
    fsyncDirectory(root);
    return; // an earlier run got this far
  }
  if (type !== 'directory' && type !== 'absent') throw new Error(`queue is ${type}, not a directory`);
  fs.mkdirSync(path.join(root, 'queue-v2'), { recursive: true });
  // The fence file is made durable BEFORE the directory moves, so the window with neither is two renames wide.
  atomicWriteFile(pending, QUEUE_FENCE_NOTE, { fsync: true });
  if (type === 'directory') {
    let aside = `${legacy}.legacy-${stamp}`;
    for (let n = 2; fs.existsSync(aside); n += 1) aside = `${legacy}.legacy-${stamp}-${n}`;
    fs.renameSync(legacy, aside);
    afterRename();
  }
  fs.renameSync(pending, legacy);
  fsyncDirectory(root);
}

/** `queue` as the cutover finds it: `directory` (legacy), `absent`, `fence` (ours), or a description of what else is there. */
function queueEntryType(root) {
  const entry = path.join(root, 'queue');
  let st;
  try {
    st = fs.lstatSync(entry);
  } catch (err) {
    if (err.code === 'ENOENT') return 'absent';
    throw err;
  }
  if (st.isDirectory()) return 'directory';
  if (st.isSymbolicLink()) return 'a symlink';
  if (st.isFile()) return queueFenced(root) ? 'fence' : 'a file that is not the scheduler fence';
  return 'neither a directory nor a regular file';
}

/** Remove the `migrating` marker durably. */
function removeMarker(root) {
  fs.rmSync(paths(root).migrating, { force: true });
  fsyncDirectory(root);
}

function archiveSingletons(root, stamp) {
  const p = paths(root);
  const archived = [];
  for (const file of [p.conflictSkipState, p.capacitySkipState, p.resourceSkipState]) {
    if (!fs.existsSync(file)) continue;
    fs.renameSync(file, `${file}.migrated-${stamp}`);
    archived.push(file);
  }
  return archived;
}

/**
 * Core of the command. Resolves `{status, checks}` where status is `already-migrated`, `fairness-invalid` (a valid
 * fence over an invalid fairness file: the broker is running legacy), `refused`, `ready` (dry run, all preconditions
 * hold) or `migrated`. `afterFairness` (before the queue fence), `afterQueueRename` (between the fence's two renames) and `afterFence` are test seams.
 */
export async function migrateScheduler(root, { dryRun = false, readProcesses = readProcessTable, afterFairness = () => {}, afterFence = () => {}, afterQueueRename = () => {}, pid = process.pid, now = Date.now } = {}) {
  const p = paths(root);
  return withLock(root, async () => {
    if (readSchedulerFence(root).status === 'valid') {
      const v2 = resolveScheduler(root, { log: false }).v2;
      // A migrator that crashed after the fence (the commit point) leaves its marker behind, which would refuse every
      // admission for ever. The fence is durable, so the migration is done: finish the cleanup.
      const recovered = !dryRun && fs.existsSync(p.migrating);
      if (recovered) removeMarker(root);
      return { status: v2 ? (recovered ? 'recovered' : 'already-migrated') : 'fairness-invalid', checks: [] };
    }
    const other = liveMigrator(root, pid);
    if (other !== null) return { status: 'refused', checks: [{ name: 'migrator', failures: [`another lane migrate-scheduler is running (pid ${other})`] }] };
    if (!dryRun) atomicWriteJson(p.migrating, { pid, startedAt: now() }, { fsync: true });
    try {
      const checks = checkPreconditions(root, { readProcesses, pid });
      if (checks.some((c) => c.failures.length > 0)) return { status: 'refused', checks };
      if (dryRun) return { status: 'ready', checks };
      // Nothing is queued, leased or attempting, so the new store starts empty: the legacy singleton files are archived, not converted.
      atomicWriteJson(p.fairness, { version: SCHEDULER_V2_VERSION, tickets: {} }, { fsync: true });
      const stamp = new Date(now()).toISOString().replace(/[:.]/g, '-');
      await afterFairness();
      fenceLegacyQueue(root, stamp, { afterRename: afterQueueRename });
      atomicWriteJson(p.schedFence, { version: SCHEDULER_V2_VERSION, migratedAt: now() }, { fsync: true });
      await afterFence();
      const archived = archiveSingletons(root, stamp);
      logResourceEvent(root, 'scheduler-migrated', { archived: archived.length });
      return { status: 'migrated', checks, archived };
    } finally {
      if (!dryRun) removeMarker(root);
    }
  });
}

/** `lane migrate-scheduler [--dry-run]`. */
export async function migrateSchedulerCommand({ dryRun = false, root = stateHome(), ...seams } = {}) {
  if (!fs.existsSync(root)) {
    process.stderr.write(`lane migrate-scheduler: no broker state at ${root}\n`);
    return { exitCode: 1 };
  }
  const result = await migrateScheduler(root, { dryRun, ...seams });
  const out = (line) => process.stdout.write(`${line}\n`);
  const err = (line) => process.stderr.write(`${line}\n`);
  switch (result.status) {
    case 'already-migrated':
      out('lane migrate-scheduler: already migrated (valid scheduler fence present); nothing to do');
      return { exitCode: 0 };
    case 'recovered':
      out('lane migrate-scheduler: already migrated (recovered): a valid scheduler fence was present and the stale migrating marker is removed');
      return { exitCode: 0 };
    case 'fairness-invalid':
      err('lane migrate-scheduler: a valid fence is present but fairness-v2.json is invalid, so the broker is running the legacy scheduler; repair or remove fairness-v2.json by hand');
      return { exitCode: 1 };
    case 'refused': {
      for (const c of result.checks) {
        if (dryRun && c.failures.length === 0) out(`ok: ${c.name}`);
        for (const failure of c.failures) err(`lane migrate-scheduler: refused: ${failure}`);
      }
      err(dryRun ? 'lane migrate-scheduler: dry run: preconditions not met' : 'lane migrate-scheduler: not migrated; the broker is still paused and the migrating marker is removed');
      return { exitCode: 1 };
    }
    case 'ready':
      for (const c of result.checks) out(`ok: ${c.name}`);
      out('lane migrate-scheduler: dry run: all preconditions hold; nothing was changed');
      return { exitCode: 0 };
    default:
      out(`lane migrate-scheduler: migrated; ${result.archived.length} legacy skip file(s) archived. The broker is still paused: run \`lane resume\``);
      return { exitCode: 0 };
  }
}
