import { sanitizeObservedCpu, sanitizeRssPeak, sanitizeCpuSeconds } from './observed.js';

/** BRAIN-361: a runner's usage fields, each kept only when well-formed; a malformed one is dropped, never thrown on. */
export function relayedUsage(result) {
  const observedCpu = sanitizeObservedCpu(result?.observedCpu);
  const rssPeak = sanitizeRssPeak(result?.observedRssPeakBytes);
  const cpuSeconds = sanitizeCpuSeconds(result?.cpuSeconds);
  return {
    ...(observedCpu ? { observedCpu } : {}),
    ...(rssPeak !== undefined ? { observedRssPeakBytes: rssPeak } : {}),
    ...(cpuSeconds !== undefined ? { cpuSeconds } : {}),
  };
}

/** BRAIN-389: the runner's deps-phase outcome (hit|miss|skip) and wall time, each kept only when well-formed. */
export function relayedDeps(result) {
  const deps = result?.deps;
  if (!deps || !['hit', 'miss', 'skip'].includes(deps.outcome)) return {};
  return { depsCache: deps.outcome, ...(Number.isFinite(deps.ms) && deps.ms >= 0 ? { depsMs: deps.ms } : {}) };
}
