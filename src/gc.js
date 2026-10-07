import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { atomicWriteFile, isCancelled, isExpired, isUuid, isWithdrawn, listJsonRecordsStrict, paths, readJsonSafe, stateHome, tombstonePath, withLock } from './state.js';
import { isSupervisorAlive } from './lease.js';

/**
 * BRAIN-438: garbage collection of what runs leave behind. Two sweeps, both of which only ever remove what nothing can
 * still be using:
 *
 *  - `gcRemoteTickets`: on a runner, the `work/` and tmp dirs of finished tickets (a ticket whose `remote-exec` was killed
 *    never reached `cleanupWork`), whole ticket directories past retention, and expired tombstones. Run at the start of
 *    every `remote-exec` and from `lane gc`.
 *  - `gcLogsAndResults`: `logs/` and `results/` files past `logRetentionMs`. Run from `lane gc` and, at most once a day,
 *    from a supervisor's start (`maybeDailyGc`).
 *
 * Both decide under the broker lock (`withLock`, the lock `remote-exec` creates tickets and tombstones under). Neither
 * removes anything while holding it: a doomed directory is first RENAMED into `<remoteRoot>/gc-trash` (one atomic, fast
 * step inside the lock) and deleted afterwards, so a multi-GB `rm` never blocks admission.
 *
 * TOMBSTONES (BRAIN-436) must outlive any possible late `remote-exec` for that id. A tombstone is written only when the
 * ticket directory does NOT exist, and `remote-exec` refuses an id that has one. So there are two ways GC could re-open an
 * id to a late exec: delete the tombstone, or delete a ticket directory whose id is tombstoned. It does neither before
 * `remoteTicketRetentionMs`: a tombstoned id's ticket directory is never touched while the tombstone exists, and the
 * tombstone is removed only once its own file is older than the retention. A late exec is a submitter's ssh session
 * still streaming a snapshot; the cancel that wrote the tombstone came from that same submitter giving up, and
 * `ConnectTimeout` plus the submitter process's own lifetime bound that session to minutes. Config validation floors the
 * retention at one day so no setting can shrink the margin to anything near that.
 */

/** Per-ticket TMPDIR base of a runner (BRAIN-374/376): /var/tmp is disk-backed, unlike a possibly RAM-backed /tmp. */
export const REMOTE_TMP_BASE = '/var/tmp';
const TMP_DIR_FILE = 'tmp-dir';
const DAY_MS = 86_400_000;

const lockOptions = { timeoutMs: 2000 };

function trashDirOf(remoteRoot) {
  return path.join(remoteRoot, 'gc-trash');
}

/** Delete synchronously; for `lane gc`, where the operator is waiting for the result anyway. */
export function removeNow(targets) {
  for (const target of targets) {
    try {
      fs.rmSync(target, { recursive: true, force: true });
    } catch {
      // best-effort; whatever is left is found again next run
    }
  }
}

/** Delete in a detached `rm`, for `remote-exec`: the submitter's ssh session must not wait on a multi-GB removal. */
export function removeDetached(targets) {
  if (targets.length === 0) return;
  const child = spawn('rm', ['-rf', '--', ...targets], { detached: true, stdio: 'ignore' });
  child.on('error', () => {});
  child.unref();
}

function ids(records) {
  return new Set(records.map((r) => r.id));
}

/** Every id that has a lease, a queue entry, or (for the logs sweep) an attempt record. Throws if any record is unreadable. */
function liveIds(root, { withAttempts = false } = {}) {
  const p = paths(root);
  const live = ids(listJsonRecordsStrict(p.leases));
  for (const id of ids(listJsonRecordsStrict(p.queue))) live.add(id);
  if (withAttempts) for (const id of ids(listJsonRecordsStrict(p.attempts))) live.add(id);
  return live;
}

function readTrimmed(file) {
  try {
    return fs.readFileSync(file, 'utf8').trim();
  } catch {
    return null;
  }
}

/**
 * Why this ticket directory must not be touched, or null. Fails closed: anything that could still be a live reader or
 * writer, or a marker a later request still consults, keeps the ticket.
 */
function liveReason(ticketDir, brokerRoot, live) {
  const publisher = readJsonSafe(path.join(ticketDir, 'publisher.json'));
  if (publisher && isSupervisorAlive({ supervisorPid: publisher.pid, supervisorStart: publisher.start })) return 'publisher-alive';
  const laneId = readTrimmed(path.join(ticketDir, 'remote-id'));
  if (!laneId) return null;
  if (live.has(laneId)) return 'leased-or-queued';
  if (isCancelled(brokerRoot, laneId) || isWithdrawn(brokerRoot, laneId) || isExpired(brokerRoot, laneId)) return 'marker-pending';
  return null;
}

/** The tmp dir `remote-exec` recorded for this ticket, if it is the shape `remote-exec` creates. */
function recordedTmpDir(ticketDir) {
  const recorded = readTrimmed(path.join(ticketDir, TMP_DIR_FILE));
  if (!recorded || !path.isAbsolute(recorded)) return null;
  return path.dirname(recorded) === REMOTE_TMP_BASE && path.basename(recorded).startsWith('lb-') ? recorded : null;
}

function isTerminal(ticketDir) {
  const result = readJsonSafe(path.join(ticketDir, 'result.json'));
  return Boolean(result) && typeof result === 'object';
}

function exists(file) {
  return fs.existsSync(file);
}

/**
 * Sweep a runner's `<remoteRoot>/tickets` and `<remoteRoot>/tombstones`. At most `maxTickets` ticket directories are
 * acted on per call (`bounded` reports that more may remain). Never throws except where the lock cannot be taken or a
 * broker record is unreadable; callers on a hot path catch that and carry on.
 *
 * Per ticket, with no live reason (see liveReason) and no tombstone:
 *   - older than `retentionMs`: the whole directory goes;
 *   - else, a terminal `result.json`: its `work/` and tmp dir go (the result and artifacts stay for the submitter).
 */
export async function gcRemoteTickets({ remoteRoot, brokerRoot = stateHome(), retentionMs, maxTickets = Infinity, now = Date.now(), dryRun = false, remove = removeNow }) {
  const summary = { tickets: 0, work: 0, tombstones: 0, skippedLive: 0, bounded: false };
  const ticketsDir = path.join(remoteRoot, 'tickets');
  const tombstonesDir = path.join(remoteRoot, 'tombstones');
  const trashDir = trashDirOf(remoteRoot);
  const doomedTmpDirs = [];

  await withLock(
    brokerRoot,
    () => {
      let names = [];
      try {
        names = fs.readdirSync(ticketsDir).filter(isUuid);
      } catch {
        // no tickets yet
      }
      const live = names.length > 0 ? liveIds(brokerRoot) : new Set();
      let acted = 0;
      for (const name of names) {
        if (acted >= maxTickets) {
          summary.bounded = true;
          break;
        }
        const ticketDir = path.join(ticketsDir, name);
        const stat = fs.lstatSync(ticketDir, { throwIfNoEntry: false });
        if (!stat?.isDirectory()) continue;
        if (exists(tombstonePath(remoteRoot, name))) continue;
        if (liveReason(ticketDir, brokerRoot, live)) {
          summary.skippedLive += 1;
          continue;
        }
        const expired = now - stat.mtimeMs >= retentionMs;
        const workDir = path.join(ticketDir, 'work');
        const tmpDir = recordedTmpDir(ticketDir);
        if (!expired && !(isTerminal(ticketDir) && (exists(workDir) || tmpDir))) continue;
        acted += 1;
        if (expired) summary.tickets += 1;
        else summary.work += 1;
        if (tmpDir) doomedTmpDirs.push(tmpDir);
        if (dryRun) continue;
        fs.mkdirSync(trashDir, { recursive: true });
        const doomed = expired ? ticketDir : workDir;
        if (exists(doomed)) fs.renameSync(doomed, path.join(trashDir, `${name}-${crypto.randomBytes(4).toString('hex')}`));
        if (!expired) fs.rmSync(path.join(ticketDir, TMP_DIR_FILE), { force: true });
      }

      let tombstones = [];
      try {
        tombstones = fs.readdirSync(tombstonesDir);
      } catch {
        // none written
      }
      for (const name of tombstones) {
        const file = path.join(tombstonesDir, name);
        const stat = fs.statSync(file, { throwIfNoEntry: false });
        if (!stat || now - stat.mtimeMs < retentionMs) continue;
        summary.tombstones += 1;
        if (!dryRun) fs.rmSync(file, { force: true });
      }
    },
    lockOptions,
  );

  if (!dryRun) {
    let trashed = [];
    try {
      trashed = fs.readdirSync(trashDir).map((n) => path.join(trashDir, n));
    } catch {
      // nothing renamed yet
    }
    remove([...trashed, ...doomedTmpDirs]);
  }
  return summary;
}

/**
 * Prune `logs/*` and `results/*` older than `retentionMs`, at most `maxFiles` per call. A file is named `<id>.<ext>`; the
 * ids with a lease, a queue entry or an attempt record are read once, under the lock, and their files are never removed
 * however old. Deleting happens after the lock is released, which is safe: an id cannot become live after that read
 * unless it is new, and a new id's files are brand new.
 */
export async function gcLogsAndResults(root, { retentionMs, maxFiles = Infinity, now = Date.now(), dryRun = false }) {
  const summary = { logs: 0, results: 0, skippedLive: 0, bounded: false };
  const live = await withLock(root, () => liveIds(root, { withAttempts: true }), lockOptions);
  const p = paths(root);
  for (const [kind, dir] of [['logs', p.logs], ['results', p.results]]) {
    let entries = [];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      if (summary.logs + summary.results >= maxFiles) {
        summary.bounded = true;
        return summary;
      }
      const file = path.join(dir, entry.name);
      const stat = fs.statSync(file, { throwIfNoEntry: false });
      if (!stat || now - stat.mtimeMs < retentionMs) continue;
      if (live.has(path.parse(entry.name).name)) {
        summary.skippedLive += 1;
        continue;
      }
      summary[kind] += 1;
      if (!dryRun) fs.rmSync(file, { force: true });
    }
  }
  return summary;
}

/**
 * The supervisor's hook: run `gcLogsAndResults` at most once a day. The marker is claimed BEFORE the sweep so concurrent
 * supervisors do not all sweep, and released again if the sweep hit its bound so the next supervisor carries on.
 * Returns the summary, or null when it was not due. Never throws.
 */
export async function maybeDailyGc(root, cfg, now = Date.now()) {
  const marker = paths(root).gcLastRun;
  const last = Number(readTrimmed(marker));
  if (Number.isFinite(last) && now - last < DAY_MS) return null;
  try {
    atomicWriteFile(marker, String(now));
    const summary = await gcLogsAndResults(root, { retentionMs: cfg.logRetentionMs, maxFiles: cfg.logGcMaxFilesPerRun, now });
    if (summary.bounded) fs.rmSync(marker, { force: true });
    return summary;
  } catch {
    fs.rmSync(marker, { force: true });
    return null;
  }
}

/** `lane gc [--root <remoteRoot>] [--dry-run] [--max <n>]`: both sweeps, unbounded unless `--max`, one JSON line out. */
export async function gcCommand(args, { remoteRoot, cfg, brokerRoot = stateHome() }) {
  const dryRun = args.includes('--dry-run');
  const maxIdx = args.indexOf('--max');
  const max = maxIdx >= 0 ? Number(args[maxIdx + 1]) : Infinity;
  if (!(max > 0)) {
    process.stderr.write('lane gc: --max must be a positive number\n');
    return { exitCode: 2 };
  }
  try {
    const remote = await gcRemoteTickets({ remoteRoot, brokerRoot, retentionMs: cfg.remoteTicketRetentionMs, maxTickets: max, dryRun });
    const logs = await gcLogsAndResults(brokerRoot, { retentionMs: cfg.logRetentionMs, maxFiles: max, dryRun });
    if (!dryRun && !logs.bounded) atomicWriteFile(paths(brokerRoot).gcLastRun, String(Date.now()));
    process.stdout.write(`${JSON.stringify({ dryRun, remote, logs })}\n`);
    return { exitCode: 0 };
  } catch (err) {
    process.stderr.write(`lane gc: ${err.message}\n`);
    return { exitCode: 1 };
  }
}
