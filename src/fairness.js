import fs from 'node:fs';
import { paths, atomicWriteJson, readJsonSafe } from './state.js';
import { logResourceEvent } from './admission.js';
import { orderQueue, promoteReservationOwner } from './priority.js';

/**
 * BRAIN-380 slice 2: the scheduler fence and the per-ticket fairness store.
 *
 * Priority ordering is live only while a VALID `sched-v2.json` sits in the state root (a later slice adds the
 * command that writes it). Without one, scheduler.js runs the legacy algorithm over the three singleton skip
 * files, byte for byte. With one, the same skip logic runs over `fairness-v2.json`, which keys every record by
 * ticket id so that a priority or aging displacement of the head neither resets nor charges a counter.
 */

export const SCHEDULER_V2_VERSION = 2;
const REASONS = ['conflict', 'capacity', 'resource'];

const isPlainObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

/** `{status: 'missing'}`, `{status: 'valid'}` or `{status: 'invalid', reason}`; `reason` is one grep-able token. */
export function readSchedulerFence(root) {
  let text;
  try {
    text = fs.readFileSync(paths(root).schedFence, 'utf8');
  } catch (err) {
    return err.code === 'ENOENT' ? { status: 'missing' } : { status: 'invalid', reason: 'fence-unreadable' };
  }
  let raw;
  try {
    raw = JSON.parse(text);
  } catch {
    return { status: 'invalid', reason: 'fence-not-json' };
  }
  if (!isPlainObject(raw) || !Number.isFinite(raw.migratedAt)) return { status: 'invalid', reason: 'fence-bad-schema' };
  if (raw.version !== SCHEDULER_V2_VERSION) return { status: 'invalid', reason: 'fence-bad-version' };
  return { status: 'valid' };
}

const absent = (value) => value === undefined || value === null;
const optional = (test) => (value) => absent(value) || test(value);
const finite = (value) => Number.isFinite(value);
const bool = (value) => typeof value === 'boolean';
const str = (value) => typeof value === 'string';

// The field types each selector reads (scheduler.js readSkipState/readCapacitySkipState/readResourceSkipState and
// fairness.js reservations). A present field of the wrong type invalidates the whole file rather than being half-read.
const FIELD_TYPES = {
  conflict: { blockedSince: optional(finite), graceStartedAt: optional(finite), loggedPhase: optional(str) },
  capacity: { loggedPhase: optional(str) },
  resource: {
    reserved: bool,
    reservationSeq: optional((v) => Number.isInteger(v) && v >= 0),
    inScope: optional(bool),
    behindConflict: optional(bool),
    deniedAt: finite,
    budget: finite,
    externalBusy: finite,
  },
};

function validRecord(reason, rec) {
  if (!isPlainObject(rec) || rec.reason !== reason || !Number.isInteger(rec.skipsCharged) || rec.skipsCharged < 0) return false;
  if (!Object.entries(FIELD_TYPES[reason]).every(([field, ok]) => ok(rec[field]))) return false;
  // a latched reservation is ordered by its seq, so one without it can't be placed
  return !(reason === 'resource' && rec.reserved === true && !Number.isInteger(rec.reservationSeq));
}

function validFairness(raw) {
  if (!isPlainObject(raw) || raw.version !== SCHEDULER_V2_VERSION) return 'fairness-bad-version';
  if (!isPlainObject(raw.tickets)) return 'fairness-bad-schema';
  for (const perReason of Object.values(raw.tickets)) {
    if (!isPlainObject(perReason)) return 'fairness-bad-schema';
    for (const [reason, rec] of Object.entries(perReason)) {
      if (!REASONS.includes(reason) || !validRecord(reason, rec)) return 'fairness-bad-schema';
    }
  }
  return null;
}

/** The migrator writes the fairness file before the fence, so a valid fence over a missing file is damage, not an empty store. */
function loadFairness(root) {
  let text;
  try {
    text = fs.readFileSync(paths(root).fairness, 'utf8');
  } catch (err) {
    return { invalid: err.code === 'ENOENT' ? 'fairness-missing' : 'fairness-unreadable' };
  }
  let raw;
  try {
    raw = JSON.parse(text);
  } catch {
    return { invalid: 'fairness-not-json' };
  }
  const invalid = validFairness(raw);
  return invalid ? { invalid } : { tickets: raw.tickets };
}

const loggedInvalid = new Set();

/**
 * Which scheduler runs this evaluation: `{v2: false}` (no fence, or an invalid fence/fairness file, which also logs
 * `scheduler-fence-invalid` once per process and root) or `{v2: true, tickets}`. `log: false` is for `lane status`.
 */
export function resolveScheduler(root, { log = true } = {}) {
  const fence = readSchedulerFence(root);
  if (fence.status === 'missing') return { v2: false };
  let reason = fence.reason;
  let tickets;
  if (fence.status === 'valid') {
    const loaded = loadFairness(root);
    reason = loaded.invalid;
    tickets = loaded.tickets;
  }
  if (reason) {
    if (log && !loggedInvalid.has(`${root}\0${reason}`)) {
      loggedInvalid.add(`${root}\0${reason}`);
      logResourceEvent(root, 'scheduler-fence-invalid', { reason });
    }
    return { v2: false };
  }
  return { v2: true, tickets };
}

/** The singleton files, exactly as the legacy scheduler reads and writes them. */
export function legacyStore(root) {
  const p = paths(root);
  const file = { conflict: p.conflictSkipState, capacity: p.capacitySkipState, resource: p.resourceSkipState };
  return {
    v2: false,
    root,
    files: file,
    read: (kind) => readJsonSafe(file[kind]),
    write: (kind, headId, raw, write = atomicWriteJson) => write(file[kind], raw),
    dropResourceRecord: () => {
      try {
        fs.unlinkSync(file.resource);
      } catch {
        // already gone
      }
    },
    depart: () => {},
    prune: () => {},
    reservations: () => [],
  };
}

// The legacy raw shape (`count`, `headId`) <-> the per-ticket record (`skipsCharged`, `reason`).
function toRecord(kind, raw) {
  const { headId, count, ...rest } = raw;
  if (kind === 'conflict') {
    const { blockedSince, ...other } = rest;
    return { reason: kind, skipsCharged: count, blockedSince, graceStartedAt: blockedSince, ...other };
  }
  return { reason: kind, skipsCharged: count, ...rest };
}

function toRaw(kind, headId, rec) {
  const { skipsCharged, graceStartedAt, blockedSince, ...withReason } = rec;
  const { reason: _reason, ...rest } = withReason;
  if (kind === 'conflict') return { headId, count: skipsCharged, blockedSince: graceStartedAt ?? blockedSince, ...rest };
  return { headId, count: skipsCharged, ...rest };
}

/**
 * The per-ticket store: `fairness-v2.json`, `{version, tickets: {<id>: {conflict?, capacity?, resource?}}}`.
 * A ticket's records leave only when the ticket does (`depart` on start, `prune` for cancel/expiry/reap), never
 * because another ticket became head. Same read/write contract as `legacyStore`, so the skip logic is shared.
 * Must be used under the broker lock; `read` never touches the singleton files.
 */
export function fairnessStore(root, tickets) {
  const persist = (next, write = atomicWriteJson) => {
    write(paths(root).fairness, { version: SCHEDULER_V2_VERSION, tickets: next });
  };
  return {
    v2: true,
    root,
    read: (kind, headId) => {
      const rec = Object.hasOwn(tickets, headId) ? tickets[headId][kind] : undefined;
      return rec ? toRaw(kind, headId, rec) : null;
    },
    write: (kind, headId, raw, write = atomicWriteJson) => {
      const next = { ...tickets, [headId]: { ...(Object.hasOwn(tickets, headId) ? tickets[headId] : {}), [kind]: toRecord(kind, raw) } };
      persist(next, write);
      Object.assign(tickets, next);
    },
    /** Legacy unlinks the singleton (above). Here a denied behind-conflict pick invalidates only the snapshot; the
     *  ticket's allowance and counters stay. */
    dropResourceRecord: (headId) => {
      const rec = Object.hasOwn(tickets, headId) ? tickets[headId].resource : undefined;
      if (!rec) return;
      const next = { ...tickets, [headId]: { ...tickets[headId], resource: { ...rec, behindConflict: false, inScope: false } } };
      try {
        persist(next);
        Object.assign(tickets, next);
      } catch {
        // best-effort: the next poll re-derives it
      }
    },
    /** The ticket started: its records go. Best-effort, `prune` finishes the job on the next evaluation. */
    depart: (ticketId) => {
      if (!Object.hasOwn(tickets, ticketId)) return;
      const next = { ...tickets };
      delete next[ticketId];
      try {
        persist(next);
        delete tickets[ticketId];
      } catch {
        // see above
      }
    },
    /** Drop the records of every ticket no longer queued (cancelled, expired, reaped). An unreadable queue record has no
     *  id, so with one present nothing can be proven departed and nothing is dropped. */
    prune: (rawQueue, provisional = []) => {
      if (rawQueue.includes(null)) return;
      // a ticket provisionally withdrawn for a remote dispatch (BRAIN-405) may be restored: it keeps its records
      const queued = new Set([...rawQueue.map((t) => t.id), ...provisional]);
      const next = Object.fromEntries(Object.entries(tickets).filter(([id]) => queued.has(id)));
      if (Object.keys(next).length === Object.keys(tickets).length) return;
      try {
        persist(next);
        for (const id of Object.keys(tickets)) if (!queued.has(id)) delete tickets[id];
      } catch {
        // best-effort
      }
    },
    /** The reservation limit is off: each latched ticket's whole resource record (counter and latch) is dropped, so re-enabling
     *  it makes the ticket earn its allowance and then a reservation from scratch. */
    releaseReservations: () => {
      const held = Object.entries(tickets).filter(([, perReason]) => perReason.resource?.reserved === true);
      if (held.length === 0) return;
      const next = { ...tickets };
      for (const [id, perReason] of held) {
        const { resource: _released, ...others } = perReason;
        next[id] = others;
      }
      try {
        persist(next);
        Object.assign(tickets, next);
      } catch {
        // best-effort: nothing reads a latch while the limit is off, and the next evaluation retries
      }
    },
    reservations: () =>
      Object.entries(tickets)
        .filter(([, perReason]) => perReason.resource?.reserved === true && Number.isInteger(perReason.resource.reservationSeq))
        .map(([id, perReason]) => ({ id, seq: perReason.resource.reservationSeq })),
  };
}

/**
 * The ONE array every selector walks in a priority evaluation (R4-2): the raw queue ordered by score within each
 * run of readable records, then the active reservation owner, if any, moved to index 0. `ownerId` is that owner.
 * A reservation exists only while resource backfill does (active mode, `resourceSkipLimit` > 0); `tryStart` releases the
 * latches when it is off. `eligible(ticket)` is the class-eligibility filter (R4-3): an owner it rejects has its
 * reservation suspended for this evaluation. It defaults to allow-all because BRAIN-379 allocation is shadow-only; wire
 * it to BRAIN-379's enforcement when that goes live.
 */
export function effectiveView(rawQueue, nowEff, cfg, store, eligible = () => true, { simsAfterTests = false } = {}) {
  const ordered = orderQueue(rawQueue, nowEff, cfg, simsAfterTests);
  const reservable = cfg.schedulerMode === 'active' && cfg.resourceSkipLimit > 0;
  const live = reservable ? store.reservations().filter(({ id }) => rawQueue.some((t) => t !== null && t.id === id && eligible(t))) : [];
  return promoteReservationOwner(ordered, live);
}
