import fs from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { atomicWriteJson, ensureDurableDir, fsyncDirectory, paths, readJsonSafe, withLock } from './state.js';

export const ENVELOPE_VERSION = 1;
const TICKET_ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const REQUIRED_FIELDS = ['logicalId', 'binding', 'header', 'repoKey', 'lane', 'laneKey', 'cpuCores', 'memoryBytes', 'requiresTags', 'priority'];
const REQUIRED_BINDING_FIELDS = ['protocol', 'generation', 'manifestHash'];

export const pullTicketDir = (root, id) => path.join(paths(root).pull, id);
const LOCK_TIMEOUT_MS = 500;

const TICKET_DIR_NAME = /^[0-9a-f-]{36}$/;
export const owedCancelFile = (root, id) => path.join(pullTicketDir(root, id), 'cancel-owed.json');

export const envelopeFile = (root, id) => path.join(pullTicketDir(root, id), 'envelope.json');

// group 'broker' so updateAttempt/publishTerminal nest inside; never take it from a global-lock section (assertLockOrder can't see that order)
export function withTicketLock(root, id, fn, { lockTimeoutMs = LOCK_TIMEOUT_MS } = {}) {
  if (!TICKET_ID_RE.test(id)) throw new Error('pull ticket id must be a ticket uuid');
  ensureDurableDir(pullTicketDir(root, id), { boundary: root });
  return withLock(root, fn, { lockPath: path.join(pullTicketDir(root, id), 'recover.lock'), group: 'broker', timeoutMs: lockTimeoutMs });
}

function validate(envelope) {
  if (envelope?.v !== ENVELOPE_VERSION) throw new Error(`pull envelope version ${envelope?.v} is not ${ENVELOPE_VERSION}`);
  if (!TICKET_ID_RE.test(envelope.logicalId ?? '')) throw new Error('pull envelope logicalId must be a ticket uuid');
  const missing = REQUIRED_FIELDS.filter((field) => envelope[field] === undefined);
  const missingBinding = REQUIRED_BINDING_FIELDS.filter((field) => envelope.binding?.[field] === undefined);
  if (missing.length + missingBinding.length > 0) throw new Error(`pull envelope lacks ${[...missing, ...missingBinding.map((f) => `binding.${f}`)].join(', ')}`);
  return envelope;
}

export function readEnvelope(root, id) {
  let text;
  try {
    text = fs.readFileSync(envelopeFile(root, id), 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
  const envelope = validate(JSON.parse(text));
  ensureDurableDir(pullTicketDir(root, id), { boundary: root });
  return envelope;
}

function writeEnvelope(root, envelope) {
  ensureDurableDir(pullTicketDir(root, envelope.logicalId), { boundary: root });
  atomicWriteJson(envelopeFile(root, envelope.logicalId), envelope, { fsync: true });
}

export async function createEnvelope(root, fields, { now = Date.now, lockTimeoutMs } = {}) {
  const envelope = validate({ ...fields, v: ENVELOPE_VERSION, createdAt: now() });
  return withTicketLock(root, envelope.logicalId, () => {
    if (fs.existsSync(envelopeFile(root, envelope.logicalId))) throw Object.assign(new Error(`pull envelope for ${envelope.logicalId} already exists`), { code: 'EEXIST' });
    writeEnvelope(root, envelope);
    return envelope;
  }, { lockTimeoutMs });
}

/** A snapshot key never holds two contents, so a descriptor already recorded may be recorded again but never replaced. */
export async function recordSnapshot(root, id, snapshot, { lockTimeoutMs } = {}) {
  return withTicketLock(root, id, () => {
    const envelope = readEnvelope(root, id);
    if (envelope === null) throw new Error(`no pull envelope for ${id}`);
    if (envelope.snapshot !== undefined) {
      if (!isDeepStrictEqual(envelope.snapshot, snapshot)) throw new Error(`pull envelope for ${id} already holds a different snapshot`);
      return envelope;
    }
    const next = { ...envelope, snapshot };
    writeEnvelope(root, next);
    return next;
  }, { lockTimeoutMs });
}

/** The cancel a ticket owes its queue row, kept apart from the attempt so it outlives the published result. */
export const writeOwedCancel = (root, id, owed) => atomicWriteJson(owedCancelFile(root, id), owed, { fsync: true });

export function owedCancelIds(root) {
  try {
    return fs.readdirSync(paths(root).pull).filter((name) => TICKET_DIR_NAME.test(name) && fs.existsSync(owedCancelFile(root, name)));
  } catch {
    return [];
  }
}

/** Updates a marker that still exists, under the ticket lock; a marker that was delivered or expired meanwhile is never written back, and neither is its directory. Returns whether it wrote. */
export async function touchOwedCancel(root, id, patch) {
  if (!fs.existsSync(pullTicketDir(root, id))) return false;
  return withTicketLock(root, id, () => {
    const current = readJsonSafe(owedCancelFile(root, id));
    if (current === null) return false;
    writeOwedCancel(root, id, { ...current, ...patch });
    return true;
  });
}

export const readOwedCancel = (root, id) => readJsonSafe(owedCancelFile(root, id));

export const clearOwedCancel = (root, id) => withTicketLock(root, id, () => {
  fs.rmSync(owedCancelFile(root, id), { force: true });
  fsyncDirectory(pullTicketDir(root, id));
});
