import fs from 'node:fs';
import path from 'node:path';
import {
  atomicWriteJson, ensureDurableDir, executionIdOf, fsyncDirectory, isExecutionId, isUuid, kernelBootId, processStartTime, readJsonSafe, withLock,
} from '../state.js';
import { isProcessAlive } from '../process-liveness.js';

export class AgentStoreError extends Error {}
export class AgentOwnedError extends Error {}

export const SUBPHASES = ['FETCHING', 'STARTING', 'SPAWNING', 'RUNNING', 'SETTLING', 'OUTBOX', 'KILLING', 'DONE'];
const OUTBOX_KINDS = ['finish', 'release'];

export function agentPaths(remoteRoot) {
  const root = path.join(path.resolve(remoteRoot), 'agent');
  return {
    root, ownerLock: path.join(root, 'owner.lock'), owner: path.join(root, 'owner.json'), status: path.join(root, 'status.json'), pendingClaim: path.join(root, 'pending-claim.json'),
    executions: path.join(root, 'executions'), out: path.join(root, 'out'), spool: path.join(root, 'spool'),
  };
}

// Every startup fsyncs each component up to the remote root's parent, new or not: a dead process may never have synced its link.
export function ensureAgentDirs(remoteRoot) {
  const p = agentPaths(remoteRoot);
  const boundary = path.dirname(path.resolve(remoteRoot));
  for (const dir of [p.executions, p.out, p.spool]) ensureDurableDir(dir, { boundary });
  return p;
}

function ownerIsLive(owner) {
  if (!Number.isInteger(owner?.pid)) return false;
  const bootNow = kernelBootId();
  if (owner.kernelBootId && bootNow && owner.kernelBootId !== bootNow) return false;
  return isProcessAlive(owner.pid, owner.start);
}

export function readOwner(remoteRoot) {
  const owner = readJsonSafe(agentPaths(remoteRoot).owner);
  return { owner, live: ownerIsLive(owner) };
}

export function withOwnerLock(remoteRoot, fn) {
  const { root, ownerLock } = agentPaths(remoteRoot);
  return withLock(root, fn, { lockPath: ownerLock, group: 'agent' });
}

export async function claimOwner(remoteRoot, { pid = process.pid } = {}) {
  const p = agentPaths(remoteRoot);
  await withOwnerLock(remoteRoot, async () => {
    const { owner, live } = readOwner(remoteRoot);
    if (live && owner.pid !== pid) throw new AgentOwnedError(`another lane agent (pid ${owner.pid}) owns ${p.root}`);
    atomicWriteJson(p.owner, { pid, start: processStartTime(pid) ?? null, kernelBootId: kernelBootId() }, { fsync: true });
  });
}

export function writeStatus(remoteRoot, status) {
  atomicWriteJson(agentPaths(remoteRoot).status, { pid: process.pid, at: Date.now(), ...status });
}

export function readAgentStatus(remoteRoot) {
  const { live } = readOwner(remoteRoot);
  return live ? { state: 'running', ...readJsonSafe(agentPaths(remoteRoot).status) } : { state: 'no-owner' };
}

export function newExecutionRecord(claim, { now = Date.now() } = {}) {
  const { id, epoch, token, repo_key: repoKey, lane, lane_key: laneKey, binding, header, snapshot } = claim;
  return {
    id, epoch, token, execId: executionIdOf(id, epoch), claim: { repoKey, lane, laneKey, binding, header, snapshot },
    subphase: 'FETCHING', claimedAt: now, kernelBootId: null,
  };
}

function assertRecord(record, where) {
  const fail = (why) => { throw new AgentStoreError(`${where}: ${why}`); };
  if (!isUuid(record?.id) || !Number.isSafeInteger(record.epoch) || !isUuid(record.token)) fail('id, epoch and token must be a uuid, an integer and a uuid');
  if (record.execId !== executionIdOf(record.id, record.epoch)) fail(`execId ${record.execId} does not match ${record.id} e${record.epoch}`);
  if (!SUBPHASES.includes(record.subphase)) fail(`unknown subphase ${record.subphase}`);
  if (record.subphase === 'OUTBOX' && !OUTBOX_KINDS.includes(record.outbox?.kind)) fail('an OUTBOX record needs outbox.kind finish or release');
}

export class ExecutionTable {
  #records = new Map();

  constructor(remoteRoot) {
    this.remoteRoot = remoteRoot;
    const dir = agentPaths(remoteRoot).executions;
    for (const name of fs.readdirSync(dir).filter((n) => n.endsWith('.json')).sort()) {
      const file = path.join(dir, name);
      const record = readJsonSafe(file);
      if (record === null) throw new AgentStoreError(`${file}: unreadable or not JSON`);
      assertRecord(record, file);
      if (`${record.execId}.json` !== name) throw new AgentStoreError(`${file}: holds ${record.execId}`);
      this.#records.set(record.execId, record);
    }
  }

  #file(execId) {
    if (!isExecutionId(execId)) throw new AgentStoreError(`not an execution id: ${execId}`);
    return path.join(agentPaths(this.remoteRoot).executions, `${execId}.json`);
  }

  get(execId) {
    const record = this.#records.get(execId);
    return record && structuredClone(record);
  }

  all() {
    return [...this.#records.values()].map((r) => structuredClone(r));
  }

  put(record) {
    assertRecord(record, `record ${record?.execId}`);
    const copy = structuredClone(record);
    atomicWriteJson(this.#file(copy.execId), copy, { fsync: true });
    this.#records.set(copy.execId, copy);
  }

  remove(execId) {
    fs.rmSync(this.#file(execId), { force: true });
    fsyncDirectory(agentPaths(this.remoteRoot).executions);
    this.#records.delete(execId);
  }
}
