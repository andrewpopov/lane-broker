import { sanitizeObservedCpu, sanitizeRssPeak, sanitizeCpuSeconds } from './observed.js';

/** BRAIN-361: a runner's usage fields, each kept only when well-formed; a malformed one is dropped, never thrown on. */
export function relayedUsage(result) {
  const observedCpu = sanitizeObservedCpu(result?.observedCpu);
  const rssPeak = sanitizeRssPeak(result?.observedRssPeakBytes);
  const cpuSeconds = sanitizeCpuSeconds(result?.cpuSeconds);
  const cgroupPeak = sanitizeRssPeak(result?.observedCgroupMemoryPeakBytes);
  return {
    ...(observedCpu ? { observedCpu } : {}),
    ...(rssPeak !== undefined ? { observedRssPeakBytes: rssPeak } : {}),
    ...(cpuSeconds !== undefined ? { cpuSeconds } : {}),
    ...(cgroupPeak !== undefined ? { observedCgroupMemoryPeakBytes: cgroupPeak } : {}),
  };
}

/** BRAIN-389: the runner's deps-phase outcome (hit|miss|skip) and wall time, each kept only when well-formed. */
export function relayedDeps(result) {
  const deps = result?.deps;
  if (!deps || !['hit', 'miss', 'skip'].includes(deps.outcome)) return {};
  return { depsCache: deps.outcome, ...(Number.isFinite(deps.ms) && deps.ms >= 0 ? { depsMs: deps.ms } : {}) };
}

const mb = (bytes) => (bytes / 1_000_000).toFixed(1);

/**
 * BRAIN-642: the one per-dispatch stderr line for a snapshot upload. `stats` is dispatchRemote's `onTransferStats` payload;
 * `excluded` is the manifest's exclude summary (absent without `remoteExclude`). Parts that do not apply are omitted.
 */
export function snapshotLine(runnerName, stats, excluded) {
  const parts = [`tree ${mb(stats.treeBytes)} MB`, `sent ${mb(stats.sentBytes)} MB${stats.mode === 'gzip' ? ' (gzip)' : ''}`];
  if (stats.cacheFiles > 0) parts.push(`cache ${stats.cachedFiles}/${stats.cacheFiles} files (${((100 * stats.cachedFiles) / stats.cacheFiles).toFixed(1)}%)`);
  if (excluded) parts.push(`excluded ${excluded.count}`);
  return `lane: snapshot -> ${runnerName}: ${parts.join(', ')}\n`;
}

const count = (n) => Number.isInteger(n) && n >= 0;

/** BRAIN-642: the client's upload byte counts and the runner's blob-store counts, each kept only when well-formed. */
export function relayedTransfer(stats, result) {
  const blob = result?.blobStats;
  return {
    ...(stats && count(stats.treeBytes) ? { snapshotRawBytes: stats.treeBytes } : {}),
    ...(stats && count(stats.sentBytes) ? { snapshotSentBytes: stats.sentBytes } : {}),
    ...(blob && count(blob.hits) ? { blobHits: blob.hits } : {}),
    ...(blob && count(blob.misses) ? { blobMisses: blob.misses } : {}),
    ...(blob && count(blob.corrupt) ? { blobCorrupt: blob.corrupt } : {}),
  };
}
