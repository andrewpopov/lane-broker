import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, readJsonSafe, withLock } from './state.js';
import { runnerAvailability } from './runner-schedule.js';

/**
 * BRAIN-594/597: the ONE record of "first failed contact" per runner, `runner-unreachable.json`.
 *
 *   { "<runner>": <ms epoch of the first failed contact of the current streak>,
 *     "__heartbeat": { "at": <ms epoch of the last `lane runner watch` tick>, "runners": { "<runner>": { lastOkAt, lastPingAt } } } }
 *
 * Writers: `reconcileRemoteAttempt` (a failed fetch starts the clock, any answer clears it) and `lane runner watch` (a ping does the
 * same), both through `recordContact`/`recordPings` below, under `runner-unreachable.lock` (state.js `withLock`, group `heartbeat`:
 * never taken while holding the broker lock, nor the broker lock while holding this one). The runner entries are the BRAIN-594 shape
 * unchanged, so a reader that only looks up `file[name]` still works; `__heartbeat` is extra.
 */

export const HEARTBEAT_KEY = '__heartbeat';
export const HEARTBEAT_FRESH_MS = 5 * 60_000;
const LOCK_WAIT_MS = 5000;

const emptyHeartbeat = () => ({ at: null, runners: {} });

/** Runner name -> ms epoch of its first failed contact in the current streak. */
export function readUnreachable(root) {
  const raw = readJsonSafe(paths(root).runnerUnreachable) ?? {};
  return Object.fromEntries(Object.entries(raw).filter(([key, value]) => key !== HEARTBEAT_KEY && Number.isFinite(value)));
}

export function readHeartbeat(root) {
  const beat = (readJsonSafe(paths(root).runnerUnreachable) ?? {})[HEARTBEAT_KEY];
  return beat && typeof beat === 'object' ? { ...emptyHeartbeat(), ...beat } : emptyHeartbeat();
}

/** Apply one contact result to the parsed file: the clock keeps its first time on a failure and is removed on an answer. */
function applyContact(file, name, ok, at) {
  if (ok) delete file[name];
  else if (!Number.isFinite(file[name])) file[name] = at;
}

/** Test-only seam (README "Testing hooks"): park inside the critical section, after the read, until `<dir>/go` exists. */
async function pauseInUpdateForTest() {
  const dir = process.env.LANE_BROKER_TEST_PAUSE_IN_UNREACHABLE_UPDATE;
  if (!dir) return;
  fs.writeFileSync(path.join(dir, `ready-${process.pid}`), '');
  while (!fs.existsSync(path.join(dir, 'go'))) await new Promise((resolve) => setTimeout(resolve, 20));
}

/** `deadlineAt` (ms epoch, optional) bounds the wait for the lock; a lock still held then throws `LockTimeoutError`. */
async function updateFile(root, mutate, { deadlineAt } = {}) {
  const timeoutMs = deadlineAt === undefined ? LOCK_WAIT_MS : Math.max(0, Math.min(LOCK_WAIT_MS, deadlineAt - Date.now()));
  return withLock(
    root,
    async () => {
      const file = readJsonSafe(paths(root).runnerUnreachable) ?? {};
      const before = JSON.stringify(file);
      await pauseInUpdateForTest();
      const result = mutate(file);
      if (JSON.stringify(file) !== before) atomicWriteJson(paths(root).runnerUnreachable, file);
      return result;
    },
    { lockPath: paths(root).runnerUnreachableLock, group: 'heartbeat', timeoutMs },
  );
}

/** Reconcile's contact result: a runner answered (`ok`) or gave no usable answer. Returns the first-failed-contact time while it is failing, else null. */
export const recordContact = (root, name, ok, now, opts) =>
  updateFile(
    root,
    (file) => {
      applyContact(file, name, ok, now);
      return ok ? null : file[name];
    },
    opts,
  );

/** One watcher tick's pings, `[{ name, ok }]`, in a single locked write: the same clock, plus the heartbeat (tick time, per-runner last-ok/last-ping). */
export const recordPings = (root, pings, now) =>
  updateFile(root, (file) => {
    const beat = { ...emptyHeartbeat(), ...(file[HEARTBEAT_KEY] ?? {}) };
    const runners = {};
    for (const { name, ok } of pings) {
      applyContact(file, name, ok, now);
      runners[name] = { lastOkAt: ok ? now : beat.runners?.[name]?.lastOkAt ?? null, lastPingAt: now };
    }
    file[HEARTBEAT_KEY] = { at: now, runners };
  });

/** HH:MM in the local zone. */
export const clock = (ms) => new Date(ms).toTimeString().slice(0, 5);
const ageOf = (ms) => (ms < 60_000 ? `${Math.round(ms / 1000)}s` : ms < 3_600_000 ? `${Math.round(ms / 60_000)}m` : `${Math.round(ms / 3_600_000)}h`);

/** The heartbeat age in ms, or null when no watcher tick was ever recorded. */
const heartbeatAgeMs = (beat, now) => (Number.isFinite(beat.at) ? Math.max(0, now - beat.at) : null);

/**
 * `{ [runner]: 'unreachable since HH:MM' }` for every runner placement must skip without probing: not `offline` (skipped on its own),
 * first failed contact at least `runnerDownAfterMs` old, and the file fresh (a watcher tick within HEARTBEAT_FRESH_MS), so a dead
 * watcher can never keep a runner out of placement on an old clock.
 */
export function downRunnerReasons(root, cfg, now = Date.now()) {
  const after = cfg?.runnerDownAfterMs;
  if (!(after > 0)) return {};
  const age = heartbeatAgeMs(readHeartbeat(root), now);
  if (age === null || age > HEARTBEAT_FRESH_MS) return {};
  const unreachable = readUnreachable(root);
  const reasons = {};
  for (const runner of cfg.runners ?? []) {
    const since = unreachable[runner.name];
    if (!runner.offline && since !== undefined && now - since >= after) reasons[runner.name] = `unreachable since ${clock(since)}`;
  }
  return reasons;
}

/** One entry per configured runner, for `lane status`, `lane runner status` and the status JSON. */
export function collectRunnerStates(root, cfg, now = Date.now()) {
  const runners = cfg?.runners ?? [];
  if (runners.length === 0) return [];
  const beat = readHeartbeat(root);
  const unreachable = readUnreachable(root);
  const down = downRunnerReasons(root, cfg, now);
  const ageMs = heartbeatAgeMs(beat, now);
  return runners.map((runner) => {
    const since = unreachable[runner.name] ?? null;
    const offSchedule = !runnerAvailability(runner.schedule, now).available;
    const state = runner.offline ? 'offline' : Object.hasOwn(down, runner.name) ? 'down' : since !== null ? 'unreachable' : offSchedule ? 'off-schedule' : 'up';
    return {
      name: runner.name,
      state,
      unreachableSince: since,
      lostSettleAt: since !== null && cfg.runnerLostAfterMs > 0 && !runner.offline ? since + cfg.runnerLostAfterMs : null,
      lastOkAt: beat.runners[runner.name]?.lastOkAt ?? null,
      lastPingAt: beat.runners[runner.name]?.lastPingAt ?? null,
      offSchedule,
      watcherRunning: ageMs !== null && ageMs <= HEARTBEAT_FRESH_MS,
      stateAgeMs: ageMs,
    };
  });
}

const STATE_TEXT = {
  up: () => 'up',
  offline: () => 'offline',
  'off-schedule': () => 'off-schedule',
  unreachable: (r) => `unreachable since ${clock(r.unreachableSince)}`,
  down: (r) => `down (placement skips; unreachable since ${clock(r.unreachableSince)})`,
};

/** The RUNNERS block shared by `lane status` and `lane runner status`; empty when no runner is configured. */
export function renderRunnersLines(runners, now = Date.now()) {
  if (!runners || runners.length === 0) return [];
  const lines = ['RUNNERS:'];
  if (!runners[0].watcherRunning) {
    lines.push(`  watcher not running (state ${runners[0].stateAgeMs === null ? 'never written' : `${Math.round(runners[0].stateAgeMs / 60_000)}m old`}); \`lane runner watch\` should run every 60 s`);
  }
  for (const r of runners) {
    const lastOk = r.lastOkAt === null ? 'never' : `${ageOf(Math.max(0, now - r.lastOkAt))} ago`;
    const settle = r.lostSettleAt === null ? '' : `  lost-settle ${clock(r.lostSettleAt)}`;
    lines.push(`  ${r.name}  ${STATE_TEXT[r.state](r)}${r.offSchedule && r.state !== 'off-schedule' ? ' (off-schedule)' : ''}  last-ok ${lastOk}${settle}`);
  }
  return lines;
}
