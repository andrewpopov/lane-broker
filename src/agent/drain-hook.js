import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { AGENT_CONFIG_DIR, loadAgentConfig } from './config.js';
import { ExecutionTable, readAgentStatus } from './store.js';
import { DRAIN_MODES, clearRemoteDrain, readDrainMarkers, writeRemoteDrain } from '../state.js';
import { listLeasesStrict, LEASE_STATE } from '../lease.js';
import { listQueueStrict } from '../scheduler.js';
import { remoteOwnerMapStrict } from '../retier.js';

export const BOSUN_OWNER = 'bosun';
const NOT_YET = 75;
const VERBS = new Set(['--apply', '--lift', '--status']);
const FLAGS_WITH_VALUE = new Set(['--root', '--config']);
const flagValue = (args, flag) => { const at = args.indexOf(flag); return at >= 0 ? args[at + 1] : undefined; };

class UsageError extends Error {}

function drainRequest(env) {
  const id = env.BOSUN_DRAIN_ID;
  if (!id) throw new UsageError('BOSUN_DRAIN_ID is required');
  const mode = env.BOSUN_DRAIN_MODE;
  if (!DRAIN_MODES.includes(mode)) throw new UsageError(`BOSUN_DRAIN_MODE must be ${DRAIN_MODES.join(' or ')}`);
  const expires = env.BOSUN_DRAIN_EXPIRES_AT;
  const expiresAt = expires ? Date.parse(expires) : undefined;
  if (expiresAt !== undefined && !Number.isFinite(expiresAt)) throw new UsageError('BOSUN_DRAIN_EXPIRES_AT must be an ISO timestamp');
  return { id, mode, reason: env.BOSUN_DRAIN_REASON ?? '', expiresAt };
}

/** Every execution record on disk is DONE; a runner that never ran an agent has no directory, which is no records. */
function recordsAllDone(remoteRoot) {
  try {
    return new ExecutionTable(remoteRoot).all().every((r) => r.subphase === 'DONE');
  } catch (err) {
    if (err.code === 'ENOENT') return true;
    throw err;
  }
}

/** Any running or queued ticket here that a submitter owns. Whatever cannot be read leaves ownership unknown, and unknown counts as work left. */
async function remoteWorkLeft(remoteRoot, { stateDir }) {
  try {
    const entries = [...listLeasesStrict(stateDir).filter((l) => l.state !== LEASE_STATE.DONE), ...listQueueStrict(stateDir)];
    if (entries.length === 0) return false;
    const owners = remoteOwnerMapStrict(remoteRoot);
    return entries.some((entry) => owners.has(entry.id));
  } catch {
    return true;
  }
}

const bosunMarker = (stateDir) => readDrainMarkers(stateDir).find((m) => m.owner === BOSUN_OWNER);
const isRequested = (marker, request) => marker?.drainId === request.id && marker.mode === request.mode;
const observedBy = (agent, marker) => agent.state === 'no-owner' || (agent.drains ?? []).some((d) => d.owner === BOSUN_OWNER && d.id === marker.drainId && d.applied === marker.applied);

/** The verdict for the marker as first read; `drainStatus` re-reads the marker before it lets a 0 stand. */
async function observedVerdict({ request, remoteRoot, config, readStatus, remoteWork }, marker) {
  if (!observedBy(readStatus(remoteRoot), marker)) return NOT_YET;
  if (request.mode === 'interrupt') return 0;
  return recordsAllDone(remoteRoot) && !await remoteWork(remoteRoot, config) ? 0 : NOT_YET;
}

/** Exit 0 once this application of this drain is in force for its mode, 75 until then, 1 if the marker is not this drain's. */
async function drainStatus(context) {
  const { request, config, err } = context;
  const notInForce = () => {
    err.write(`lane drain-hook: no bosun drain ${request.id} (${request.mode}) is in force\n`);
    return 1;
  };
  const marker = bosunMarker(config.stateDir);
  if (!isRequested(marker, request)) return notInForce();
  const verdict = await observedVerdict(context, marker);
  if (verdict !== 0) return verdict;
  const after = bosunMarker(config.stateDir);
  if (!isRequested(after, request)) return notInForce();
  return after.applied === marker.applied ? 0 : NOT_YET;
}

/**
 * `lane drain-hook --apply|--lift|--status [--config <dir>] [--root <remoteRoot>]`: the verbs bosun runs for a module whose drain is `kind: exec`.
 * The drain id, mode, reason and expiry arrive as BOSUN_DRAIN_* variables. Exit 0 is done, 75 is not yet (`--status` only), 1 is an error, 2 is usage.
 * The marker goes in the state dir the agent's config names, owner `bosun`; the agent keeps running and stops claiming.
 */
export async function drainHookCommand(args, { remoteRoot, env = process.env, configDir = flagValue(args, '--config') ?? AGENT_CONFIG_DIR, loadConfig = () => loadAgentConfig({ file: path.join(configDir, 'config.json') }), readStatus = readAgentStatus, remoteWork = remoteWorkLeft, err = process.stderr } = {}) {
  const verbs = args.filter((a) => VERBS.has(a));
  const unknown = args.find((a, i) => a.startsWith('--') && !VERBS.has(a) && !FLAGS_WITH_VALUE.has(a) && !FLAGS_WITH_VALUE.has(args[i - 1]));
  if (verbs.length !== 1 || unknown !== undefined) {
    err.write(`lane drain-hook: ${unknown !== undefined ? `unknown flag "${unknown}"` : 'exactly one of --apply, --lift, --status is required'}\n`);
    return { exitCode: 2 };
  }
  try {
    const config = loadConfig();
    if (verbs[0] === '--lift') {
      await clearRemoteDrain(config.stateDir, BOSUN_OWNER);
      return { exitCode: 0 };
    }
    const request = drainRequest(env);
    if (verbs[0] === '--apply') {
      await writeRemoteDrain(config.stateDir, request.reason, { owner: BOSUN_OWNER, expiresAt: request.expiresAt, drainId: request.id, mode: request.mode, applied: randomUUID() });
      return { exitCode: 0 };
    }
    return { exitCode: await drainStatus({ request, remoteRoot, config, readStatus, remoteWork, err }) };
  } catch (error) {
    err.write(`lane drain-hook: ${error.message}\n`);
    return { exitCode: error instanceof UsageError ? 2 : 1 };
  }
}
