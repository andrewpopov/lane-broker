import { paths, atomicWriteJson, readJsonSafe, withLock } from './state.js';

// BRAIN-1057: `{ "<runner>": { at, by } }`; written by `lane runner offline|online` so the provisioned config is never edited, overlaid once at config load.

const LOCK_WAIT_MS = 5000;

export function readOfflineMarks(root) {
  const raw = readJsonSafe(paths(root).runnerOffline);
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  return Object.fromEntries(Object.entries(raw).filter(([, mark]) => Number.isFinite(mark?.at)));
}

export function applyOfflineMarks(cfg, marks) {
  if (!Array.isArray(cfg.runners)) return cfg;
  const runners = cfg.runners.map((runner) => {
    if (runner.offline === true) return { ...runner, offlineSource: 'config' };
    const mark = marks[runner.name];
    return mark ? { ...runner, offline: true, offlineSource: 'cli', offlineSince: mark.at } : runner;
  });
  return { ...cfg, runners };
}

const withMarks = (root, mutate) =>
  withLock(
    root,
    async () => {
      const marks = readOfflineMarks(root);
      const changed = mutate(marks);
      if (changed) atomicWriteJson(paths(root).runnerOffline, marks);
      return changed;
    },
    { lockPath: paths(root).runnerOfflineLock, group: 'heartbeat', timeoutMs: LOCK_WAIT_MS },
  );

export const markOffline = (root, name, now = Date.now()) =>
  withMarks(root, (marks) => {
    if (Object.hasOwn(marks, name)) return false;
    marks[name] = { at: now, by: 'cli' };
    return true;
  });

export const clearOffline = (root, name) =>
  withMarks(root, (marks) => {
    if (!Object.hasOwn(marks, name)) return false;
    delete marks[name];
    return true;
  });
