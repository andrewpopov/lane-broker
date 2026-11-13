import fs from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { atomicWriteJson, ensureDurableDir, paths, withLock } from './state.js';

export const ENVELOPE_VERSION = 1;
const TICKET_ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const REQUIRED_FIELDS = ['logicalId', 'binding', 'nonce', 'header', 'repoKey', 'lane', 'laneKey', 'cpuCores', 'memoryBytes', 'requiresTags', 'priority'];
const REQUIRED_BINDING_FIELDS = ['protocol', 'generation', 'manifestHash'];

export const pullTicketDir = (root, id) => path.join(paths(root).pull, id);
const LOCK_TIMEOUT_MS = 500;

export const envelopeFile = (root, id) => path.join(pullTicketDir(root, id), 'envelope.json');

// group 'broker', the one updateAttempt and publishTerminal take, so a step may call them while holding this lock
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
