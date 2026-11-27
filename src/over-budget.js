import { OVER_BUDGET_POLICY, overBudgetVerdict } from '@andrewpopov/bosun/module';

/**
 * BRAIN-1372, shadow only: bosun's over-budget verdict for one lease, its own reservation against its tree's observed use. Returns the fields to stamp
 * on the lease and, the first time a streak reaches `kill`, the line to log. Nothing is signalled, and a failure only skips the verdict.
 */
export function overBudgetShadow(lease, { cores, memoryBytes, now, sampleMs }, verdictOf = overBudgetVerdict) {
  try {
    return shadowVerdict(lease, { cores, memoryBytes, now, sampleMs }, verdictOf);
  } catch {
    return { fields: {}, line: null };
  }
}

function shadowVerdict(lease, { cores, memoryBytes, now, sampleMs }, verdictOf) {
  const sample = { at: now, grant: { cpu: lease.resources?.cpuCores, mem: lease.resources?.memoryBytes }, observed: { cpu: cores ?? undefined, mem: memoryBytes ?? undefined } };
  const result = verdictOf(lease.overBudget ?? {}, sample, { ...OVER_BUDGET_POLICY, maxGapMs: 2 * sampleMs });
  const wouldKill = result.verdict === 'kill' ? result.reason : null;
  const line = wouldKill && !lease.overBudgetWouldKill ? `lane: over-budget (shadow): would kill ${lease.id}: ${wouldKill}\n` : null;
  return { fields: { overBudget: result.state, overBudgetWouldKill: wouldKill }, line };
}
