import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { paths, stateHome, withLock, atomicWriteJson, atomicWriteFile, fsyncDirectory, queueFenced, readJsonSafe, isPidAlive, processStartTime, readDrainMarker, clearStaleDrainMarker, drainPauseReason, UnreadableRecordError, QUEUE_FENCE_NOTE } from './state.js';
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

/** Preconditions that go away by themselves once the broker is idle; every other failure (an unreadable record, a bad queue layout, another migrator) is permanent. */
const TRANSIENT_CHECKS = new Set(['queue', 'lease', 'attempt', 'process']);

/**
 * `{checks, counts}`: one check per precondition, `{name, failures: [string]}`, never short-circuiting so a dry run
 * reports every one; `counts` is what is still busy (`queued`, `running` leases, `attempts`, `processes`), for `--when-idle`'s progress line.
 */
function checkPreconditions(root, { readProcesses, pid }) {
  const p = paths(root);
  const checks = [];
  const counts = { queued: 0, running: 0, attempts: 0, processes: 0 };
  const check = (name, fn) => {
    try {
      checks.push({ name, failures: fn() });
    } catch (err) {
      if (!(err instanceof UnreadableRecordError)) throw err;
      checks.push({ name, failures: [`unreadable ${name} record ${err.message}`], unreadable: true });
    }
  };

  check('paused', () => (fs.existsSync(p.pause) ? [] : ['the broker is not paused; run `lane pause` first']));
  check('queue layout', () => {
    const type = queueEntryType(root);
    return ['directory', 'absent', 'fence'].includes(type) ? [] : [`queue/ is ${type}; old and new code would not agree on the queue. Fix it by hand before migrating`];
  });
  check('queue', () => {
    const queued = listQueueStrict(root);
    counts.queued = queued.length;
    return queued.length === 0 ? [] : [`queue is not empty: ${queued.length} queued ticket(s): ${queued.map((t) => t.id ?? '(no id)').join(', ')}`];
  });
  check('lease', () => {
    const busy = listLeasesStrict(root).filter((l) => l.state !== LEASE_STATE.DONE);
    counts.running = busy.length;
    return busy.map((l) => `lease ${l.id ?? '(no id)'} is ${l.state ?? 'in an unknown state'}`);
  });
  // publishTerminal removes an attempt record when the attempt ends, so a record that is still on disk is not terminal.
  check('attempt', () => {
    const attempts = listAttemptsStrict(root);
    counts.attempts = attempts.length;
    return attempts.map((a) => `attempt ${a.id ?? '(no id)'} is still in phase ${a.phase ?? 'unknown'} (supervisor pid ${a.supervisor?.pid ?? 'unknown'})`);
  });
  let rows = null;
  check('process table', () => {
    try {
      rows = readProcesses();
      return [];
    } catch (err) {
      return [`the process table could not be read (${err.message}); refusing rather than assuming no lane process is alive`];
    }
  });
  // Its own check, outside TRANSIENT_CHECKS, so `--when-idle` fails fast on an unreadable table instead of waiting for a process that may not exist.
  if (rows) {
    check('process', () => {
      const excluded = selfAndAncestors(rows, pid);
      const alive = rows.filter((r) => !excluded.has(r.pid) && runsLaneBroker(r.command));
      counts.processes = alive.length;
      return alive.map((r) => `lane process pid ${r.pid} is alive: ${preview(r.command)}`);
    });
  }
  return { checks, counts };
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

/** A failing check that clears by itself once the broker is idle. An unreadable record never does. */
const isTransient = (check) => TRANSIENT_CHECKS.has(check.name) && !check.unreadable;

/** A refusal whose every failure is something that clears by itself once the broker is idle. */
const stillBusy = (result) => result.status === 'refused' && result.checks.every((c) => c.failures.length === 0 || isTransient(c));

/**
 * (Re)write the drain marker from an `identity` (`{pid, startTime, startedAt}`) captured and validated ONCE, by `startDrain`.
 * Never probes the process again, and refuses a missing start time: a marker without one could read as live for ever after
 * pid reuse. Caller holds the broker lock.
 */
function writeDrainMarker(root, { pid, startTime, startedAt }, { pausedByDrain = false } = {}) {
  if (!startTime) throw new Error(`refusing to write a drain marker for pid ${pid} without a start time`);
  atomicWriteJson(paths(root).draining, { pid, startTime, startedAt, ...(pausedByDrain ? { pausedByDrain: true } : {}) }, { fsync: true });
}

/** Remove the drain marker if `pid` wrote it. Caller holds the broker lock. */
function removeDrainMarker(root, pid) {
  if (readDrainMarker(root)?.pid !== pid) return;
  fs.rmSync(paths(root).draining, { force: true });
  fsyncDirectory(root);
}

/**
 * Core of the command. Resolves `{status, checks}` where status is `already-migrated`, `fairness-invalid` (a valid
 * fence over an invalid fairness file: the broker is running legacy), `refused`, `ready` (dry run, all preconditions
 * hold) or `migrated`. `afterFairness` (before the queue fence), `afterQueueRename` (between the fence's two renames) and `afterFence` are test seams.
 *
 * `fromDrain` (`--when-idle`, at quiescence; the drain's identity, see `writeDrainMarker`) makes the pause part of the same lock hold: it pauses the broker if it is
 * not paused, and on EVERY outcome (migrated, refused, thrown) resumes it again if this call paused it, then removes
 * the drain marker (unless the refusal is only "busy again", see `stillBusy`), so nothing can be admitted between the
 * quiescence check and the resume.
 */
export async function migrateScheduler(root, { dryRun = false, fromDrain = false, readProcesses = readProcessTable, afterFairness = () => {}, afterFence = () => {}, afterQueueRename = () => {}, pid = process.pid, now = Date.now } = {}) {
  const p = paths(root);
  return withLock(root, async () => {
    let pausedHere = false;
    const migrateLocked = async () => {
      if (readSchedulerFence(root).status === 'valid') {
        const v2 = resolveScheduler(root, { log: false }).v2;
        // A migrator that crashed after the fence (the commit point) leaves its marker behind, which would refuse every
        // admission for ever. The fence is durable, so the migration is done: finish the cleanup.
        const recovered = !dryRun && fs.existsSync(p.migrating);
        if (recovered) removeMarker(root);
        if (!dryRun) clearStaleDrainMarker(root); // a drain killed after the fence may still hold the pause it made
        return { status: v2 ? (recovered ? 'recovered' : 'already-migrated') : 'fairness-invalid', checks: [] };
      }
      const other = liveMigrator(root, pid);
      if (other !== null) return { status: 'refused', checks: [{ name: 'migrator', failures: [`another lane migrate-scheduler is running (pid ${other})`] }] };
      if (fromDrain && !fs.existsSync(p.pause)) {
        // Ownership is durable BEFORE the pause exists: a drain killed from here on is recognised by its marker and its own
        // reason in PAUSE (`releaseDrainPause`), and a pause somebody else set never matches either.
        writeDrainMarker(root, fromDrain, { pausedByDrain: true });
        atomicWriteFile(p.pause, drainPauseReason(pid), { fsync: true });
        pausedHere = true;
      }
      if (!dryRun) atomicWriteJson(p.migrating, { pid, startedAt: now() }, { fsync: true });
      try {
        const { checks, counts } = checkPreconditions(root, { readProcesses, pid });
        if (checks.some((c) => c.failures.length > 0)) return { status: 'refused', checks, counts };
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
    };
    let result;
    try {
      result = await migrateLocked();
    } finally {
      if (pausedHere) {
        fs.rmSync(p.pause, { force: true });
        fsyncDirectory(root);
      }
      // Kept only when the broker turned out to be busy again, so the drain loop can keep waiting under the same marker
      // (which no longer owns a pause).
      if (fromDrain) {
        if (!(result && stillBusy(result))) removeDrainMarker(root, pid);
        else if (pausedHere) writeDrainMarker(root, fromDrain);
      }
    }
    return fromDrain ? { ...result, resumed: pausedHere } : result;
  });
}

export const DRAIN_POLL_MS = 5_000;
export const DRAIN_TIMEOUT_MS = 6 * 3_600_000;

function abortableSleep(ms, signal) {
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
  });
}

/** Take the drain marker under the lock. Refuses while another migrator or drain is alive; a stale marker (dead owner) is taken over. */
async function startDrain(root, { pid, now, readStartTime }) {
  return withLock(root, () => {
    // Without its own start time a marker could not tell a reused pid from this process, so it could read as live for ever.
    const startTime = readStartTime(pid); // the one probe: this value is what the marker carries
    if (!startTime) return { refused: `cannot read this process's start time (ps failed for pid ${pid}); not starting a drain, no marker written` };
    const other = liveMigrator(root, pid);
    if (other !== null) return { refused: `another lane migrate-scheduler is running (pid ${other})` };
    const existing = readDrainMarker(root);
    if (existing?.state === 'unknown') return { refused: `the drain marker ${paths(root).draining} cannot be read or parsed, so it cannot be judged; if no lane migrate-scheduler --when-idle is running, remove that file and run again` };
    if (existing?.state === 'live' && existing.pid !== pid) return { refused: `another lane migrate-scheduler --when-idle is draining (pid ${existing.pid})` };
    const tookOverStale = clearStaleDrainMarker(root); // also resumes a pause that dead drain made
    const identity = { pid, startTime, startedAt: now() };
    writeDrainMarker(root, identity);
    return { tookOverStale, identity };
  });
}

/**
 * `--when-idle`: stop NEW intake (the drain marker; see `assertNotMigrating`) while admission keeps serving what is already
 * queued, wait until the broker is quiescent (the migration's own preconditions, minus "paused"), then pause, migrate and
 * resume in ONE lock hold (`migrateScheduler` with `fromDrain`). Resolves like `migrateScheduler`, plus `timeout` and
 * `aborted`. Whatever the outcome the drain marker is gone and a pause this command made is undone; only a SIGKILL
 * leaves a marker, and `readDrainMarker` recognises that one as stale.
 */
export async function drainMigrateScheduler(root, { timeoutMs = DRAIN_TIMEOUT_MS, pollMs = DRAIN_POLL_MS, signal = new AbortController().signal, log = () => {}, sleep = abortableSleep, readProcesses = readProcessTable, pid = process.pid, now = Date.now, readStartTime = processStartTime, ...seams } = {}) {
  if (readSchedulerFence(root).status === 'valid') return migrateScheduler(root, { readProcesses, pid, now });
  const started = await startDrain(root, { pid, now, readStartTime });
  if (started.refused) return { status: 'refused', checks: [{ name: 'migrator', failures: [started.refused] }] };
  if (started.tookOverStale) log('lane migrate-scheduler: took over a stale drain marker left by a killed run');
  const deadline = now() + timeoutMs;
  let lastLine = '';
  let warnedPaused = false;
  try {
    for (;;) {
      if (signal.aborted) return { status: 'aborted' };
      const { checks, counts } = checkPreconditions(root, { readProcesses, pid });
      const failing = checks.filter((c) => c.name !== 'paused' && c.failures.length > 0);
      if (failing.some((c) => !isTransient(c))) return { status: 'refused', checks: failing };
      const line = `lane migrate-scheduler: draining: queued ${counts.queued}, running ${counts.running}, attempts ${counts.attempts}, lane processes ${counts.processes}`;
      if (line !== lastLine) log(line);
      lastLine = line;
      if (counts.queued > 0 && fs.existsSync(paths(root).pause) && !warnedPaused) {
        warnedPaused = true;
        log('lane migrate-scheduler: the broker is paused, so queued tickets will not start until `lane resume`; waiting');
      }
      if (failing.length === 0) {
        const result = await migrateScheduler(root, { readProcesses, pid, now, fromDrain: started.identity, ...seams });
        if (!stillBusy(result)) return result;
      }
      if (now() >= deadline) return { status: 'timeout', counts, checks: failing };
      await sleep(Math.min(pollMs, Math.max(0, deadline - now())), signal);
    }
  } finally {
    if (readDrainMarker(root)?.pid === pid) await withLock(root, () => removeDrainMarker(root, pid));
  }
}

const humanDuration = (ms) => (ms % 3_600_000 === 0 ? `${ms / 3_600_000}h` : ms % 60_000 === 0 ? `${ms / 60_000}m` : `${ms / 1000}s`);

/** `lane migrate-scheduler [--dry-run | --when-idle [--timeout <duration>]]`. */
export async function migrateSchedulerCommand({ dryRun = false, whenIdle = false, timeoutMs = DRAIN_TIMEOUT_MS, root = stateHome(), ...seams } = {}) {
  if (!fs.existsSync(root)) {
    process.stderr.write(`lane migrate-scheduler: no broker state at ${root}\n`);
    return { exitCode: 1 };
  }
  const out = (line) => process.stdout.write(`${line}\n`);
  const err = (line) => process.stderr.write(`${line}\n`);
  let result;
  let caught = null;
  if (whenIdle) {
    const controller = new AbortController();
    const handlers = ['SIGINT', 'SIGTERM'].map((name) => [name, () => ((caught ??= name), controller.abort())]);
    for (const [name, handler] of handlers) process.on(name, handler);
    try {
      result = await drainMigrateScheduler(root, { timeoutMs, signal: controller.signal, log: out, ...seams });
    } finally {
      for (const [name, handler] of handlers) process.off(name, handler);
    }
  } else {
    result = await migrateScheduler(root, { dryRun, ...seams });
  }
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
    case 'timeout':
      err(`lane migrate-scheduler: not migrated: the broker was not idle after ${humanDuration(timeoutMs)} (queued ${result.counts.queued}, running ${result.counts.running}, attempts ${result.counts.attempts}, lane processes ${result.counts.processes}); the drain marker is removed and the pause state is unchanged`);
      for (const c of result.checks) for (const failure of c.failures) err(`lane migrate-scheduler: still busy: ${failure}`);
      return { exitCode: 1 };
    case 'aborted':
      err(`lane migrate-scheduler: interrupted by ${caught ?? 'a signal'}; not migrated; the drain marker is removed and the pause state is unchanged`);
      return { exitCode: caught === 'SIGINT' ? 130 : 143 };
    case 'refused': {
      for (const c of result.checks) {
        if (dryRun && c.failures.length === 0) out(`ok: ${c.name}`);
        for (const failure of c.failures) err(`lane migrate-scheduler: refused: ${failure}`);
      }
      if (dryRun) err('lane migrate-scheduler: dry run: preconditions not met');
      else if (whenIdle) err('lane migrate-scheduler: not migrated; the drain and migrating markers are removed and the pause state is unchanged');
      else err('lane migrate-scheduler: not migrated; the broker is still paused and the migrating marker is removed');
      return { exitCode: 1 };
    }
    case 'ready':
      for (const c of result.checks) out(`ok: ${c.name}`);
      out('lane migrate-scheduler: dry run: all preconditions hold; nothing was changed');
      return { exitCode: 0 };
    default:
      if (!whenIdle) out(`lane migrate-scheduler: migrated; ${result.archived.length} legacy skip file(s) archived. The broker is still paused: run \`lane resume\``);
      else out(`lane migrate-scheduler: migrated; ${result.archived.length} legacy skip file(s) archived. ${result.resumed ? 'The broker is resumed.' : 'The broker was already paused and stays paused.'}`);
      return { exitCode: 0 };
  }
}
