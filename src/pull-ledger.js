import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { ensureDurableDir, fsyncDirectory, paths } from './state.js';

export const LEDGER_VERSION = 1;

export const ledgerDir = (root) => paths(root).pullLedger;
export const ledgerFile = (root, id) => path.join(ledgerDir(root), `${id}.json`);

// a queue-authored decision (release, reap, SQL cancel) has no token or body, so it is keyed (epoch, kind, source|proof|reason)
export function identityOf(entry) {
  const { decision } = entry;
  return [entry.epoch ?? null, entry.attemptRef ?? null, entry.resultSha256 ?? null, entry.resultSha256 ? null : [decision.kind, decision.source ?? decision.proof ?? decision.reason ?? null]];
}

const sameAcceptance = (a, b) => isDeepStrictEqual(identityOf(a), identityOf(b)) && isDeepStrictEqual(a.decision, b.decision);

function validate(entry) {
  if (entry?.v !== LEDGER_VERSION) throw new Error(`pull ledger entry version ${entry?.v} is not ${LEDGER_VERSION}`);
  if (typeof entry.decision?.kind !== 'string') throw new Error('pull ledger entry has no decision');
  return entry;
}

// the creator may have died before its directory fsync, so a reader makes the link durable before anyone acts on the entry
export function readLedger(root, id) {
  let text;
  try {
    text = fs.readFileSync(ledgerFile(root, id), 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
  ensureDurableDir(ledgerDir(root), { boundary: root });
  return validate(JSON.parse(text));
}

// link fails with EEXIST when any entry exists, so exactly one of any number of racing callers creates it; the stored entry always stands
export function ledgerAccept(root, entry, { hooks = {} } = {}) {
  const stamped = { v: LEDGER_VERSION, ...entry, acceptedAt: entry.acceptedAt ?? Date.now() };
  const dir = ledgerDir(root);
  ensureDurableDir(dir, { boundary: root });
  const tmp = path.join(dir, `.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`);
  const fd = fs.openSync(tmp, 'wx');
  try {
    fs.writeFileSync(fd, `${JSON.stringify(stamped, null, 2)}\n`);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  try {
    fs.linkSync(tmp, ledgerFile(root, entry.logicalId));
  } catch (err) {
    fs.rmSync(tmp, { force: true });
    if (err.code !== 'EEXIST') throw err;
    const stored = readLedger(root, entry.logicalId);
    return { created: false, entry: stored, same: sameAcceptance(stored, stamped) };
  }
  hooks.afterLink?.();
  fs.rmSync(tmp, { force: true });
  fsyncDirectory(dir);
  return { created: true, entry: stamped, same: true };
}
