import { NO_PROGRESS_REASON, SILENCE_REASON } from './no-progress.js';
import { readEnvelope } from './pull-envelope.js';
import { remoteCpuAllocation } from './remote-client.js';
import { relayedDeps, relayedTransfer, relayedUsage } from './remote-relay.js';
import { recordSettledResult, relayedPhases } from './supervisor.js';

// exactly what `historyRow` and `settledContentKey` read from the ticket, so a result published by anyone lands as the supervisor's would
const ENRICHED_FIELDS = ['id', 'key', 'repoId', 'lane', 'configLane', 'configFrom', 'configBlob', 'localFirst', 'exclusive', 'weight', 'resources', 'declaredCpuCores', 'command', 'cmdFingerprint', 'headTree', 'contentKey', 'retryReason', 'pushGate', 'localReason', 'localReasonText', 'cwd'];

export function publicationOf(enriched, { placement, attemptStartedAt, refusalExit }) {
  return {
    logPath: enriched.logPath,
    resultPath: enriched.resultPath,
    createdAt: enriched.createdAt,
    placement: placement ?? null,
    attemptStartedAt,
    refusalExit,
    enriched: Object.fromEntries(ENRICHED_FIELDS.filter((field) => enriched[field] !== undefined).map((field) => [field, enriched[field]])),
  };
}

export function pullFinishedResult(publication, { result, decision, host, acceptedAt }, _attempt, extra) {
  const endedAt = acceptedAt ?? Date.now();
  const runMs = Number.isFinite(result.runMs) && result.runMs >= 0 ? result.runMs : null;
  const waited = runMs === null ? null : Math.max(0, endedAt - publication.createdAt - runMs);
  const noProgress = result.reason === NO_PROGRESS_REASON && Number.isFinite(result.noProgressTimeoutMs);
  const silence = result.reason === SILENCE_REASON && Number.isFinite(result.silenceTimeoutMs);
  return {
    id: publication.enriched.id,
    exit: decision.exit,
    signal: null,
    executor: 'remote',
    transport: 'pull',
    runner: host,
    ...remoteCpuAllocation(result),
    ...relayedUsage(result),
    ...relayedDeps(result),
    ...relayedTransfer(null, result),
    ...relayedPhases({ result }),
    ...extra,
    ...(noProgress ? { reason: NO_PROGRESS_REASON, noProgressTimeoutMs: result.noProgressTimeoutMs } : {}),
    ...(silence ? { reason: SILENCE_REASON, silenceTimeoutMs: result.silenceTimeoutMs } : {}),
    remoteKind: result.kind,
    remotePhase: decision.phase ?? null,
    ...(result.signal ? { remoteSignal: result.signal } : {}),
    startedAt: waited === null ? publication.attemptStartedAt : publication.createdAt + waited,
    ...(runMs === null ? {} : { runMs }),
    endedAt,
    waitedMs: waited,
  };
}

/** The step-context fields a settling pull ticket publishes through; one definition for the supervisor that wrote the block and every recovery caller that reads it. */
export function publicationBindings(root, publication) {
  const enriched = { ...publication.enriched, logPath: publication.logPath, resultPath: publication.resultPath, createdAt: publication.createdAt };
  return {
    logPath: publication.logPath,
    resultPath: publication.resultPath,
    writeResult: (result, { fsync = false, rebuilt = false } = {}) => recordSettledResult(root, enriched, result, { fsync, rebuilt, placement: publication.placement }),
    buildFinishedResult: (entry, attempt, extra) => pullFinishedResult(publication, entry, attempt, extra),
  };
}

export function publicationBindingsFor(root, id) {
  const publication = readEnvelope(root, id)?.publication;
  return publication === undefined ? {} : publicationBindings(root, publication);
}
