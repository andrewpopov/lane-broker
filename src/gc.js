import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { atomicWriteFile, isCancelled, isExpired, isUuid, isWithdrawn, listJsonRecordsStrict, paths, readJsonSafe, stateHome, tombstonePath, withLock } from './state.js';
import { isSupervisorAlive } from './lease.js';
import { writeBrokerLog } from './admission.js';

/**
 * BRAIN-438: garbage collection of what runs leave behind. Two sweeps, both of which only ever remove what nothing can
 * still be using:
 *
 *  - `gcRemoteTickets`: on a runner, the `work/` and tmp dirs of finished tickets (a ticket whose `remote-exec` was killed
 *    never reached `cleanupWork`), and whole ticket directories past retention. Run at the start of
 *    every `remote-exec` and from `lane gc`.
 *  - `gcLogsAndResults`: `logs/` and `results/` files past `logRetentionMs`. Run from `lane gc` and, at most once a day,
 *    from a supervisor's start (`maybeDailyGc`).
 *
 * Both decide under the broker lock (`withLock`, the lock `remote-exec` creates tickets and tombstones under). Neither
 * removes anything while holding it: a doomed directory is first RENAMED into `<remoteRoot>/gc-trash` (one atomic, fast
 * step inside the lock) and deleted afterwards, so a multi-GB `rm` never blocks admission.
 *
 * TOMBSTONES (BRAIN-436) are NEVER pruned in this version. A tombstone is the only thing refusing a `remote-exec` for an id
 * that was cancelled before it arrived, and nothing bounds how late such an exec can be (a suspended process, a long-lived
 * ssh session: `ConnectTimeout` covers neither). Expiring one by age would let that exec recreate and run a cancelled
 * ticket. They are tiny files, so the leak is cheap; a ticket directory whose id is tombstoned is never touched either.
 *
 * Nothing outside the runner root is ever deleted: `gc-trash`, `tickets`, and the state root's `logs` and `results` must be real
 * directories inside their root (a symlink is refused; a refused logs/results sweep is skipped with one warning line), and
 * only real directories inside a verified `gc-trash` are removed. A recorded tmp dir is deleted only if it is a real `lb-*`
 * directory directly under the tmp base AND carries an `.lane-broker-owner` marker naming this very ticket (a ticket id is
 * unique, so no other live ticket can own the same path); legacy tmp dirs without a marker are never deleted. Only
 * broker-generated names (`logs/<uuid>.log`, `results/<uuid>.json`) are ever pruned: a custom `--log` path is never touched.
 *
 * THREAT MODEL: accidental configuration (a symlinked directory, a stale or wrong recorded path, a custom log path) and
 * ordinary concurrency between broker processes. A same-uid adversary who swaps a directory for a symlink DURING a sweep is out
 * of scope: they can already delete anything that uid owns. `lstat` / no-follow is used wherever it is cheap, no further.
 */

/** Per-ticket TMPDIR base of a runner (BRAIN-374/376): /var/tmp is disk-backed, unlike a possibly RAM-backed /tmp. */
export const REMOTE_TMP_BASE = '/var/tmp';
const TMP_DIR_FILE = 'tmp-dir';
/** Written inside a ticket's tmp dir by `remote-exec`; its content is the ticket id that owns the directory. */
export const TMP_OWNER_FILE = '.lane-broker-owner';
const GENERATED_NAME = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const DAY_MS = 86_400_000;

const lockOptions = { timeoutMs: 2000 };

function trashDirOf(remoteRoot) {
  return path.join(remoteRoot, 'gc-trash');
}

class UnsafeGcPathError extends Error {}

/** Throws unless `dir` is absent or a real directory (not a symlink) whose realpath is under the realpath of `root`. */
function assertRealDirUnder(root, dir) {
  const stat = fs.lstatSync(dir, { throwIfNoEntry: false });
  if (!stat) return;
  if (!stat.isDirectory()) throw new UnsafeGcPathError(`${dir} is a symlink or not a directory; refusing to collect under it`);
  const rootReal = fs.realpathSync(root);
  const real = fs.realpathSync(dir);
  if (!real.startsWith(rootReal + path.sep)) throw new UnsafeGcPathError(`${dir} resolves outside ${root}; refusing to collect under it`);
}

/** The real directories inside a verified trash dir: what is left to delete from earlier runs. Symlinks and files are never listed. */
function trashLeftovers(trashDir) {
  try {
    return fs.readdirSync(trashDir, { withFileTypes: true }).filter((e) => e.isDirectory()).map((e) => path.join(trashDir, e.name));
  } catch {
    return [];
  }
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
  const live = ids([...listJsonRecordsStrict(p.leases), ...listJsonRecordsStrict(p.queue)]);
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

/**
 * The tmp dir `remote-exec` recorded for this ticket, only if it provably belongs to it: a real (non-symlink) `lb-*`
 * directory directly under the tmp base whose owner marker names `ticketId`. Null otherwise (the caller counts it skipped).
 */
function ownedTmpDir(ticketDir, ticketId) {
  const recorded = readTrimmed(path.join(ticketDir, TMP_DIR_FILE));
  if (!recorded || !path.isAbsolute(recorded) || !path.basename(recorded).startsWith('lb-')) return null;
  try {
    if (!fs.lstatSync(recorded).isDirectory()) return null;
    if (path.dirname(fs.realpathSync(recorded)) !== fs.realpathSync(REMOTE_TMP_BASE)) return null;
    const marker = path.join(recorded, TMP_OWNER_FILE);
    if (!fs.lstatSync(marker).isFile()) return null;
    return readTrimmed(marker) === ticketId ? recorded : null;
  } catch {
    return null;
  }
}

function isTerminal(ticketDir) {
  const result = readJsonSafe(path.join(ticketDir, 'result.json'));
  return Boolean(result) && typeof result === 'object';
}

function exists(file) {
  return fs.existsSync(file);
}

/**
 * Sweep a runner's `<remoteRoot>/tickets`. At most `maxExamined` directories are looked at and `maxTickets` acted on per call,
 * resuming after the previous call's last-examined directory (`gc-cursor`) so all are eventually visited;
 * acted on per call (`bounded` reports that more may remain). Never throws except where the lock cannot be taken or a
 * broker record is unreadable; callers on a hot path catch that and carry on.
 *
 * Per ticket, with no live reason (see liveReason) and no tombstone:
 *   - older than `retentionMs`: the whole directory goes;
 *   - else, a terminal `result.json`: its `work/` and tmp dir go (the result and artifacts stay for the submitter).
 */
export async function gcRemoteTickets({ remoteRoot, brokerRoot = stateHome(), retentionMs, maxTickets = Infinity, maxExamined = Infinity, now = Date.now(), dryRun = false, remove = removeNow }) {
  const summary = { tickets: 0, work: 0, examined: 0, skippedLive: 0, tmpSkipped: 0, bounded: false };
  const ticketsDir = path.join(remoteRoot, 'tickets');
  const cursorFile = path.join(remoteRoot, 'gc-cursor');
  const trashDir = trashDirOf(remoteRoot);
  const doomedTmpDirs = [];
  assertRealDirUnder(remoteRoot, ticketsDir);
  assertRealDirUnder(remoteRoot, trashDir);
  let lastExamined = null;

  await withLock(
    brokerRoot,
    () => {
      let names = [];
      try {
        names = fs.readdirSync(ticketsDir).filter(isUuid).sort();
      } catch {
        // no tickets yet
      }
      // Resume after the last directory the previous run examined, so every directory is eventually visited.
      const cursor = readTrimmed(cursorFile);
      const resumeAt = names.findIndex((n) => n > (cursor ?? ''));
      if (resumeAt > 0) names = [...names.slice(resumeAt), ...names.slice(0, resumeAt)];
      const live = names.length > 0 ? liveIds(brokerRoot) : new Set();
      let acted = 0;
      for (const name of names) {
        if (acted >= maxTickets || summary.examined >= maxExamined) {
          summary.bounded = true;
          break;
        }
        summary.examined += 1;
        lastExamined = name;
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
        const recordedTmp = exists(path.join(ticketDir, TMP_DIR_FILE));
        const tmpDir = recordedTmp ? ownedTmpDir(ticketDir, name) : null;
        if (!expired && !(isTerminal(ticketDir) && (exists(workDir) || tmpDir))) continue;
        acted += 1;
        if (expired) summary.tickets += 1;
        else summary.work += 1;
        if (tmpDir) doomedTmpDirs.push(tmpDir);
        else if (recordedTmp) summary.tmpSkipped += 1;
        if (dryRun) continue;
        fs.mkdirSync(trashDir, { recursive: true });
        const doomed = expired ? ticketDir : workDir;
        if (exists(doomed)) fs.renameSync(doomed, path.join(trashDir, `${name}-${crypto.randomBytes(4).toString('hex')}`));
        if (!expired) fs.rmSync(path.join(ticketDir, TMP_DIR_FILE), { force: true });
      }
    },
    lockOptions,
  );

  if (!dryRun) {
    // Only real directories inside the verified trash dir (this sweep's renames plus earlier leftovers) are deleted.
    remove([...trashLeftovers(trashDir), ...doomedTmpDirs]);
    try {
      if (summary.bounded && lastExamined) atomicWriteFile(cursorFile, lastExamined);
      else fs.rmSync(cursorFile, { force: true });
    } catch {
      // the cursor is an optimisation: without it the next run starts from the top
    }
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
      assertRealDirUnder(root, dir);
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (err) {
      if (err instanceof UnsafeGcPathError) writeBrokerLog(root, `lane-broker-gc warning: ${err.message}\n`);
      continue;
    }
    const generated = kind === 'logs' ? '.log' : '.json';
    for (const entry of entries) {
      // Dirent comes from the directory listing without following links: a symlink is not a file here.
      if (!entry.isFile() || path.extname(entry.name) !== generated || !GENERATED_NAME.test(path.parse(entry.name).name)) continue;
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
