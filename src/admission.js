import fs from 'node:fs';
import { paths, atomicWriteJson, readJsonSafe, fingerprintOf } from './state.js';
import { sampleHostCpu } from './cpu.js';
import { peekCpuEstimates, lookupEstimate, clampEstimate } from './cpu-estimates.js';
import { evaluateMemoryAdmission, resolveTicketResources, leaseCpuCores, elasticClaimRange } from './resources.js';

/**
 * Cold-start CPU-core estimate for a lease with no observed measurement yet.
 * A lane's `weight` is already expressed in the same units as `capacity`
 * (see config.js's DEFAULT_REPO_CONFIG / DEFAULT_GLOBAL_CONFIG — a
 * default-weight-2 lane against a default capacity-2 box is meant to
 * saturate it), so reading weight directly as an approximate core count is
 * the cheapest estimate that is already consistent with the rest of the
 * config. Historical per-lane profiles (phase 3) replace this with a
 * measured number; this stays a named, swappable function so that slots in
 * without changing any call site.
 */
export function coldStartEstimate(weight) {
  return Math.max(0, Number(weight) || 0);
}

/**
 * BRAIN-433: the CPU a NEW claim of `declared` cores is charged for, and where that figure came from: the workload's
 * history estimate (cpu-estimates.js: p90 peak of compatible runs, capped at the declaration) when enabled and the
 * snapshot is fresh and matches `cfg`, otherwise the declaration. Never reads the history file (callers
 * refreshCpuEstimates before taking the lock). Used for a candidate and for a lease's cold demand ONLY.
 */
export function cpuEstimateBasis(ref, declared, cfg, now = Date.now()) {
  const cold = coldStartEstimate(declared);
  if (cfg?.historyDemandEnabled !== true) return { cores: cold, source: 'declared' };
  const found = lookupEstimate(peekCpuEstimates(cfg, now), ref);
  return found ? { cores: clampEstimate(found.p90, cold), source: `history:${found.level}` } : { cores: cold, source: 'declared' };
}

/**
 * How long an observed-CPU reading (src/cpu.js's observedGroupCpuCores, via
 * the supervisor heartbeat) stays usable before leaseDemand falls back to
 * the cold-start estimate. Codex pre-merge review: without this, a lease
 * whose job goes idle (or a probe that stops updating for any reason) keeps
 * its LAST observed figure forever — a one-time 7-core reading on a now-idle
 * lease would deny admission indefinitely in 'active' mode, and this is what
 * makes `admissionLoadGate: true` fail to restore byte-for-byte pre-BRAIN-207
 * behaviour (leaseDemand's bias note is a live admission input, not just
 * telemetry, once schedulerMode is 'active'). The supervisor heartbeat runs
 * every `sampleMs` (default 5000ms, see config.js's DEFAULT_GLOBAL_CONFIG) —
 * so this default TTL is 6x the DEFAULT heartbeat interval, not 3x (an
 * operator raising sampleMs shortens that margin accordingly); it exists to
 * survive a couple of missed/slow heartbeats, not to track a fast-changing
 * live figure.
 */
export const OBSERVATION_TTL_MS = 30_000;

/**
 * leaseDemand = max(observed process-group CPU if available, fresh, and
 * non-negative, the cold-start estimate). `lease.observedCpuCores` /
 * `lease.observedAt` are OPTIONAL fields — an old lease file with neither
 * falls through to the cold-start branch, same fail-safe shape as every
 * other optional lease field in this codebase.
 */
/** The lease's observed CPU (cores) if it is a finite, non-negative reading
 *  no older than OBSERVATION_TTL_MS; otherwise null. The ONE freshness rule
 *  both consumers below share — reservedSum (leaseDemand) and the
 *  externalBusy subtraction — so a stale reading can neither keep denying
 *  nor keep under-counting ambient load. */
export function freshObservedCores(lease, now = Date.now()) {
  const observed = lease.observedCpuCores;
  const observedAt = lease.observedAt;
  const fresh = Number.isFinite(observedAt) && now - observedAt <= OBSERVATION_TTL_MS;
  return fresh && Number.isFinite(observed) && observed >= 0 ? observed : null;
}

/** BRAIN-354: how many observations a lease keeps (src/supervisor.js's applyHeartbeatObservation). */
export const OBSERVED_HISTORY_MAX = 64;

/** Current observation plus the trailing history within windowMs, as plain core readings. */
function recentObservedCores(lease, now, windowMs, current) {
  const history = Array.isArray(lease.observedCpuHistory) ? lease.observedCpuHistory : [];
  const inWindow = history.filter((h) => h && Number.isFinite(h.at) && Number.isFinite(h.cores) && h.cores >= 0 && now - h.at <= windowMs);
  return { count: inWindow.length, peak: Math.max(current, ...inWindow.map((h) => h.cores)) };
}

/**
 * BRAIN-354: a lease's CPU demand and the basis it was charged on. A SETTLED lease (admitted at
 * least settleMs ago, fresh observation, >= 2 observations in the trailing window) is charged
 * max(observedNow, clamp(recentPeak * headroom, cold * floorFraction, cold)) instead of its full
 * booking; everything else — including cfg without the settledDemand knobs, and old leases with no
 * observedCpuHistory — keeps max(observed, cold) / cold.
 */
export function leaseDemandBasis(lease, now = Date.now(), cfg) {
  const cold = coldStartEstimate(leaseCpuCores(lease) ?? lease.weight);
  // BRAIN-433: only the cold charge (no observation, or an unsettled one) uses the history estimate; the settled peak allowance below is main's, uncapped by it.
  const estimated = cpuEstimateBasis(lease, cold, cfg, now).cores;
  const observed = freshObservedCores(lease, now);
  if (observed === null) return { demand: estimated, basis: 'cold' };
  const unsettled = { demand: Math.max(observed, estimated), basis: 'cold' };
  if (cfg?.settledDemandEnabled !== true) return unsettled;
  if (!Number.isFinite(lease.admittedAt) || now - lease.admittedAt < cfg.settledDemandSettleMs) return unsettled;
  const { count, peak } = recentObservedCores(lease, now, cfg.settledDemandWindowMs, observed);
  if (count < 2) return unsettled;
  const padded = Math.min(cold, Math.max(cold * cfg.settledDemandFloorFraction, peak * cfg.settledDemandHeadroom));
  return { demand: Math.max(observed, padded), basis: 'settled' };
}

export function leaseDemand(lease, now = Date.now(), cfg) {
  return leaseDemandBasis(lease, now, cfg).demand;
}

/**
 * Pure hysteresis transition for the CPU gate, structurally identical to
 * load.js's updateGateState: closes immediately at/above cpuClosePercent;
 * once closed, reopens only after cpuOpenSamples consecutive samples below
 * cpuOpenPercent. A sample at or above cpuOpenPercent (but below
 * cpuClosePercent) resets the consecutive-under counter without reopening.
 *
 * Stamped with a fingerprint of its own thresholds, same rationale and
 * mechanism as load.js's gate (see fingerprintOf in state.js): concurrent
 * supervisors reload config independently and all write this one shared
 * cpu-gate.json, so a threshold edit lands mid-countdown for some of them
 * and not others. Without the fingerprint reset, two supervisors reloading
 * at different instants during a cpuOpenPercent/cpuClosePercent/
 * cpuOpenSamples change would each advance the same shared counter under
 * different thresholds, producing a decision no single config would have
 * produced. `closed` is left untouched on a fingerprint change: a config
 * edit is not evidence the CPU got quieter.
 */
export function updateCpuGateState(prev, busyPercent, cfg) {
  const { cpuClosePercent, cpuOpenPercent, cpuOpenSamples } = cfg;
  const fingerprint = fingerprintOf(cpuClosePercent, cpuOpenPercent, cpuOpenSamples);
  const state = prev ? { ...prev } : { closed: false, consecutiveUnder: 0 };
  if (state.fingerprint !== fingerprint) {
    state.consecutiveUnder = 0;
    state.fingerprint = fingerprint;
  }
  if (busyPercent >= cpuClosePercent) {
    state.closed = true;
    state.consecutiveUnder = 0;
  } else if (state.closed) {
    if (busyPercent < cpuOpenPercent) {
      state.consecutiveUnder += 1;
      if (state.consecutiveUnder >= cpuOpenSamples) {
        state.closed = false;
        state.consecutiveUnder = 0;
      }
    } else {
      state.consecutiveUnder = 0;
    }
  }
  state.lastBusyPercent = busyPercent;
  state.lastSampleAt = Date.now();
  return state;
}

/** Coerce a persisted (possibly corrupt) gate-state file into a safe shape.
 *  Codex review finding #5: a malformed `consecutiveUnder` (NaN, negative,
 *  non-numeric) must never get compared against cpuOpenSamples directly — a
 *  stuck NaN comparison always reads false, which reads as "gate can never
 *  reopen". The fingerprint must be carried through as-is (not defaulted to
 *  a fixed value): updateCpuGateState below compares it against the
 *  freshly-computed one to decide whether to reset the counter, so losing
 *  it here would force a reset on every single poll. */
function sanitizeGateState(raw) {
  return {
    // Strict `=== true`, not a truthy coercion: a corrupt field holding the
    // STRING "not-a-boolean" would otherwise coerce truthy and read as
    // "gate closed".
    closed: raw ? raw.closed === true : false,
    consecutiveUnder: raw && Number.isFinite(raw.consecutiveUnder) && raw.consecutiveUnder >= 0 ? raw.consecutiveUnder : 0,
    fingerprint: raw && typeof raw.fingerprint === 'string' ? raw.fingerprint : null,
  };
}

/** Update + persist the CPU gate from a cpu.js sample. A missing/stale
 *  sample leaves the persisted gate state untouched — hysteresis must never
 *  be perturbed by the absence of evidence. The write is best-effort (Codex
 *  review finding #1): a failed write here must never abort admission.
 *  Caller must hold the global lock. */
export function sampleAndUpdateCpuGate(root, cfg, cpuSample) {
  const file = paths(root).cpuGate;
  const prev = sanitizeGateState(readJsonSafe(file));
  // BRAIN-346: a reused measurement is the SAME observation again — hysteresis advances once per
  // new observation, never once per poll that happened to re-read it.
  // The gate STATE is not advanced by a re-read, but this caller's own reading (its discount is candidate-specific)
  // is still checked against the close threshold, so another candidate's more generous discount cannot hold it open.
  if (cpuSample?.reused) {
    const overClose = Number.isFinite(cpuSample.hostBusyCores) && cpuSample.cores > 0 && (nonPreemptibleBusy(cpuSample, cfg) / cpuSample.cores) * 100 >= cfg.cpuClosePercent;
    return overClose ? { ...prev, closed: true } : prev;
  }
  if (
    !cpuSample ||
    cpuSample.stale ||
    !Number.isFinite(cpuSample.hostBusyCores) ||
    !Number.isFinite(cpuSample.cores) ||
    cpuSample.cores <= 0
  ) {
    return prev;
  }
  const busyPercent = (nonPreemptibleBusy(cpuSample, cfg) / cpuSample.cores) * 100;
  const next = updateCpuGateState(prev, busyPercent, cfg);
  try {
    atomicWriteJson(file, next);
  } catch {
    // best-effort: a failed gate-state write must never abort admission
  }
  return next;
}

/**
 * Is the admission cooldown currently blocking a new admission? Derived
 * directly from the held leases' own `admittedAt` field (stamped by
 * scheduler.js on the exact same write that admits a lease) rather than a
 * separate admission-state.json + its own write — there is nothing here
 * that needs to be atomic with anything else, so it costs nothing extra
 * inside the lock. A lease with no `admittedAt` (an old lease file from
 * before this field existed) simply never counts toward the cooldown,
 * same fail-open tolerance as every other OPTIONAL lease field in this
 * codebase. This also folds in the old "clear early once the previously
 * admitted lease is no longer held" behavior for free: a finished lease is
 * no longer in `heldLeases` at all, so it can't hold the cooldown open.
 */
export function cooldownActive(heldLeases, cfg, now = Date.now()) {
  return heldLeases.some((l) => Number.isFinite(l.admittedAt) && now - l.admittedAt < cfg.admissionCooldownMs);
}

/**
 * KNOWN BIAS, documented per Codex review finding #4, partially corrected by
 * BRAIN-207: externalBusy subtracts a lease's *observed* CPU
 * (src/cpu.js's observedGroupCpuCores, wired into the supervisor heartbeat),
 * so hostBusyCores no longer double-counts that lease's own reservation once
 * an observation exists. But a lease still shows this bias until its FIRST
 * heartbeat lands an observation — a cold-start lease's estimate is added to
 * reservedSum with nothing yet subtracted from hostBusyCores on its behalf,
 * the same systematic double count as before for that lease specifically.
 * Harmless while schedulerMode stays 'shadow' (the decision is never
 * enforced), but 'active' would still over-deny for any freshly-admitted
 * lease. This is deliberately loud: every admission log line below carries
 * `bias=self-subtracted-when-observed` so the remaining cold-start gap can't
 * be missed.
 */
/**
 * BRAIN-428: host busy cores with preemptible load discounted. cpu.js reports `preemptibleBusyCores` per process,
 * by identity, for processes the candidate lane outranks (strictly higher nice) and that are not lane processes.
 * The kernel runs the lane ahead of them, so they yield; only `preemptibleShare` (default 0.8) of it is counted as
 * available, so a truly saturated host is never read as empty. Everything else counts in full.
 */
export function nonPreemptibleBusy(cpuSample, cfg) {
  const preemptible = Math.min(Math.max(0, cpuSample.preemptibleBusyCores ?? 0), cpuSample.hostBusyCores);
  const share = (cfg.preemptibleNiceMin ?? 0) > 0 ? (cfg.preemptibleShare ?? 0) : 0;
  return Math.max(0, cpuSample.hostBusyCores - share * preemptible);
}

export const KNOWN_BIAS_NOTE = 'self-subtracted-when-observed';

/**
 * The CPU projection, shared by admission (evaluateCpuAdmission below) and BRAIN-346's backfill
 * selection (scheduler.js's selectResourceCandidate), so the two can never disagree about what
 * "fits" means: busy = ambient load + every held lease's demand + the candidate's claim.
 */
export function projectBusy(externalBusy, heldLeases, candidateEstimate, now = Date.now(), cfg) {
  return externalBusy + heldLeases.reduce((sum, l) => sum + leaseDemand(l, now, cfg), 0) + candidateEstimate;
}

/** The CPU-core budget for one sample: the smaller of the percent budget and all-cores-minus-reserve. */
export function cpuBudget(cpuSample, cfg) {
  const percentBudget = (cfg.cpuAdmissionPercent / 100) * cpuSample.cores;
  const reserveBudget = Math.max(0, cpuSample.cores - (cfg.cpuReserveCores ?? 0));
  return Math.min(percentBudget, reserveBudget);
}

/** A ticket's CPU claim as admission computes it (resolved resources, history-or-declared estimate), with its source. */
export function ticketCpuEstimateBasis(ticket, cfg, now = Date.now()) {
  const resources = resolveTicketResources({
    weight: ticket.weight,
    cpuCores: ticket.resources?.cpuCores,
    memoryBytes: ticket.resources?.memoryBytes,
    defaultMemoryBytesPerWeight: cfg.defaultMemoryBytesPerWeight,
  });
  return cpuEstimateBasis(ticket, resources.cpuCores, cfg, now);
}

export function ticketCpuEstimate(ticket, cfg, now = Date.now()) {
  return ticketCpuEstimateBasis(ticket, cfg, now).cores;
}

/**
 * The new admission predicate (phase 1), pure function of its inputs so it
 * can be unit-tested without touching disk or the real CPU:
 *
 *   externalBusy  = max(0, hostBusyCores - sum(observed broker CPU))
 *   projectedBusy = externalBusy + sum(leaseDemand) + candidateEstimate
 *   admit only if projectedBusy <= cpuAdmissionPercent/100 * cores
 *
 * plus the CPU gate and the admission cooldown, each able to independently
 * veto. A missing/stale CPU sample bypasses all of that: allow one job
 * through when nothing is currently held (so a cold broker isn't wedged
 * forever waiting on a sample that needs a first admission to exist), deny
 * otherwise until a valid sample arrives. See KNOWN_BIAS_NOTE above for a
 * bias this does NOT correct.
 */
export function evaluateCpuAdmission({ cpuSample, heldLeases, candidateWeight, candidateRef, candidateResources, cpuGateState, cooldownBlocked, cfg, now = Date.now() }) {
  if (!cpuSample || cpuSample.stale || !Number.isFinite(cpuSample.hostBusyCores)) {
    if (heldLeases.length === 0) {
      return { admit: true, reason: 'sample-unavailable-empty', externalBusy: null, projectedBusy: null, budget: null };
    }
    return { admit: false, reason: 'sample-unavailable-held', externalBusy: null, projectedBusy: null, budget: null };
  }

  const brokerObserved = heldLeases.reduce((sum, l) => sum + (freshObservedCores(l, now) ?? 0), 0);
  const externalBusy = Math.max(0, nonPreemptibleBusy(cpuSample, cfg) - brokerObserved);
  const { cores: candidateEstimate, source: candidateEstimateSource } = cpuEstimateBasis(candidateRef, candidateResources?.cpuCores ?? candidateWeight, cfg, now);
  const projectedBusy = projectBusy(externalBusy, heldLeases, candidateEstimate, now, cfg);
  const budget = cpuBudget(cpuSample, cfg);
  const leaseCharges = heldLeases.map((l) => ({ id: l.id, ...leaseDemandBasis(l, now, cfg) }));
  const leaseDemands = leaseCharges.map(({ id, demand, basis }) => `${String(id).slice(0, 8)}:${fmt(demand)}(${basis})`);

  if (cpuGateState.closed) {
    return { admit: false, reason: 'cpu-gate-closed', externalBusy, projectedBusy, budget, leaseDemands, leaseCharges, candidateEstimate, candidateEstimateSource };
  }
  if (cooldownBlocked) {
    return { admit: false, reason: 'cooldown', externalBusy, projectedBusy, budget, leaseDemands, leaseCharges, candidateEstimate, candidateEstimateSource };
  }
  if (projectedBusy > budget) {
    return { admit: false, reason: 'projected-over-budget', externalBusy, projectedBusy, budget, leaseDemands, leaseCharges, candidateEstimate, candidateEstimateSource };
  }
  return { admit: true, reason: 'ok', externalBusy, projectedBusy, budget, leaseDemands, leaseCharges, candidateEstimate, candidateEstimateSource };
}

/** The "nothing usable is known" fallback shared by a missing/stale sample
 *  and an outright exception (see evaluateNewAdmission below) — identical
 *  fail-safe shape either way: admit only when nothing is currently held. */
function unavailableDecision(heldLeases, reason) {
  return {
    admit: heldLeases.length === 0,
    reason,
    externalBusy: null,
    projectedBusy: null,
    budget: null,
    hostBusyCores: null,
    cores: null,
    sampleStale: true,
    cpuGateClosed: false,
    cooldownBlocked: false,
  };
}

/**
 * Best-effort host CPU sample, wrapped so a sampler failure is treated
 * identically to a missing sample and NEVER throws. The scheduler calls it
 * while holding the global lock because sampleHostCpu updates shared sample
 * state used by an active admission decision.
 */
export function sampleCpuSafe(root, cpuSampler = sampleHostCpu, reuseWindowMs = 0, preemptible = null) {
  try {
    return cpuSampler(root, undefined, { reuseWindowMs, preemptible }) || null;
  } catch {
    return null; // sampler failure: treated identically to a missing sample
  }
}

/**
 * Glue: update the CPU gate from an already-taken sample, check the
 * cooldown, and run the predicate above — everything the new rule needs for
 * one candidate on one poll. Caller must hold the global lock.
 *
 * The whole body is wrapped in try/catch (Codex review finding #1): every
 * read/write this touches is already best-effort internally, but this is a
 * deliberate second layer — NOTHING in the phase-1 CPU path may ever throw
 * out of here, because in shadow mode this must be completely unable to
 * affect what the current rule decides, and even in active mode a crash
 * here must degrade to the same fail-safe as a missing sample, never abort
 * tryStart (which would kill the caller's detached supervisor).
 */
export function evaluateNewAdmission(root, cfg, ticket, heldLeases, cpuSample, memoryInfo) {
  try {
    const cpuGateState = sampleAndUpdateCpuGate(root, cfg, cpuSample);
    const blocked = cooldownActive(heldLeases, cfg);
    return decideAdmission(cfg, ticket, heldLeases, cpuSample, memoryInfo, cpuGateState, blocked);
  } catch {
    return unavailableDecision(heldLeases, 'admission-error');
  }
}

/** The pure CPU + memory decision for one claim, given an already-updated gate and cooldown. */
function decideAdmission(cfg, ticket, heldLeases, cpuSample, memoryInfo, cpuGateState, blocked) {
  const candidateResources = resolveTicketResources({
    weight: ticket.weight,
    cpuCores: ticket.resources?.cpuCores,
    memoryBytes: ticket.resources?.memoryBytes,
    defaultMemoryBytesPerWeight: cfg.defaultMemoryBytesPerWeight,
  });
  const cpuResult = evaluateCpuAdmission({
    cpuSample,
    heldLeases,
    candidateWeight: ticket.weight,
    candidateRef: ticket,
    candidateResources,
    cpuGateState,
    cooldownBlocked: blocked,
    cfg,
  });
  const memoryResult = memoryInfo
    ? evaluateMemoryAdmission({ memoryInfo, heldLeases, candidateResources, cfg })
    : { admit: true, reason: 'not-sampled', projectedAvailableBytes: null, memoryBudgetBytes: null };
  return {
    ...cpuResult,
    admit: cpuResult.admit && memoryResult.admit,
    reason: !cpuResult.admit ? cpuResult.reason : !memoryResult.admit ? memoryResult.reason : cpuResult.reason,
    cpuReason: cpuResult.reason,
    memoryReason: memoryResult.reason,
    candidateCpuCores: candidateResources.cpuCores,
    candidateMemoryBytes: candidateResources.memoryBytes,
    projectedAvailableBytes: memoryResult.projectedAvailableBytes,
    memoryBudgetBytes: memoryResult.memoryBudgetBytes,
    hostBusyCores: cpuSample ? cpuSample.hostBusyCores : null,
    preemptibleBusyCores: cpuSample ? cpuSample.preemptibleBusyCores ?? 0 : null,
    cores: cpuSample ? cpuSample.cores : null,
    sampleStale: cpuSample ? cpuSample.stale : true,
    cpuGateClosed: cpuGateState.closed,
    cooldownBlocked: blocked,
  };
}

/**
 * BRAIN-360: a denial that more CPU headroom alone would cure -- the CPU projection (gate open, no
 * cooldown: evaluateCpuAdmission reports those first) is the ONLY thing that said no, and memory
 * said yes. Everything else (memory, a closed CPU gate, cooldown, an unavailable sample, an
 * admission error) is never elastic, and neither are the checks that live outside this predicate
 * (conflict, weight capacity, load gate, pause), which tryStart evaluates on their own.
 */
export function isCpuOnlyDenial(decision) {
  return decision.admit === false && decision.cpuReason === 'projected-over-budget' && decision.memoryReason === 'ok';
}

/**
 * BRAIN-360: when `fullDecision` is a CPU-only denial and the ticket declares `resources.minCpuCores`,
 * re-run the COMPLETE predicate (CPU projection + memory, same gate state, cooldown, held leases and
 * sample as the full claim) at each smaller INTEGER claim (elasticClaimRange), largest first, and return the first that admits:
 * `{ decision, grantedCpuCores }`. Memory keeps the full declared claim. Null when not elastic, not
 * a CPU-only denial, or nothing down to the floor fits -- the caller then keeps `fullDecision`
 * (and with it the pre-existing wait / backfill / idle-exemption behaviour) untouched.
 */
export function evaluateElasticAdmission(cfg, ticket, heldLeases, cpuSample, memoryInfo, fullDecision) {
  try {
    if (!isCpuOnlyDenial(fullDecision)) return null;
    const declared = resolveTicketResources({ weight: ticket.weight, cpuCores: ticket.resources?.cpuCores });
    // headroom: what the budget has left once everything but this ticket's own claim is counted
    const headroom = fullDecision.budget - (fullDecision.projectedBusy - fullDecision.candidateEstimate);
    const range = elasticClaimRange({ cpuCores: declared.cpuCores, minCpuCores: ticket.resources?.minCpuCores, headroom });
    if (!range) return null;
    for (let claim = range.hi; claim >= range.lo; claim -= 1) {
      const resized = { ...ticket, resources: { ...ticket.resources, cpuCores: claim } };
      const decision = decideAdmission(cfg, resized, heldLeases, cpuSample, memoryInfo, { closed: fullDecision.cpuGateClosed }, fullDecision.cooldownBlocked);
      if (decision.admit) return { decision, grantedCpuCores: claim };
    }
  } catch {
    // same fail-safe as evaluateNewAdmission: an error is never an elastic grant
  }
  return null;
}

function fmt(n) {
  return Number.isFinite(n) ? n.toFixed(2) : 'n/a';
}

/**
 * One grep-able, tally-able line per poll: `lane-broker-admission` is the
 * fixed token to grep on; every field is `key=value` so a later pass can
 * `awk -F= `/`cut` any of them out. `current` is the decision + reason the
 * rule running TODAY made; `new` is what phase 1's predicate would have
 * decided, always computed, never gating outside `schedulerMode=active`.
 * `bias` is always present — see KNOWN_BIAS_NOTE above.
 *
 * `memorySource`/`macPressure` (BRAIN-252) exist because the byte
 * arithmetic alone could not explain a memory denial: while os.freemem()
 * was making the gate unsatisfiable, the line showed a plausible-looking
 * projectedAvailableBytes and said nothing about where that figure came
 * from or what the OS itself reported about pressure. They are appended at
 * the end, immediately before `bias`, so no field anyone already greps
 * moves or changes meaning.
 */
export function formatAdmissionLog(f) {
  return [
    'lane-broker-admission',
    `candidate=${f.candidateId}`,
    `mode=${f.mode}`,
    `current=${f.currentDecision}:${f.currentReason}`,
    `loadGateIgnored=${Boolean(f.loadGateIgnored)}`,
    `new=${f.admit ? 'admit' : 'deny'}:${f.reason}`,
    `sample=${f.sampleStale ? 'stale' : 'ok'}`,
    `hostBusyCores=${fmt(f.hostBusyCores)}`,
    `cores=${f.cores ?? 'n/a'}`,
    `externalBusy=${fmt(f.externalBusy)}`,
    // only when the feature contributes, so the line is byte-identical to before wherever it does not
    ...(f.preemptibleBusyCores > 0 ? [`preemptibleBusy=${fmt(f.preemptibleBusyCores)}`] : []),
    `projectedBusy=${fmt(f.projectedBusy)}`,
    `budget=${fmt(f.budget)}`,
    `candidateCpu=${fmt(f.candidateCpuCores)}`,
    ...(f.declaredCpuCores !== undefined ? [`declaredCpu=${fmt(f.declaredCpuCores)}`] : []),
    `candidateEstimate=${fmt(f.candidateEstimate)}(${f.candidateEstimateSource ?? 'declared'})`,
    `candidateMemoryBytes=${f.candidateMemoryBytes ?? 'n/a'}`,
    `projectedAvailableBytes=${f.projectedAvailableBytes ?? 'n/a'}`,
    `memoryBudgetBytes=${f.memoryBudgetBytes ?? 'n/a'}`,
    `cpuGate=${f.cpuGateClosed ? 'closed' : 'open'}`,
    `cooldown=${f.cooldownBlocked ? 'blocked' : 'clear'}`,
    `memorySource=${f.memorySource ?? 'n/a'}`,
    `macPressure=${f.macPressure ?? 'n/a'}`,
    `leaseDemand=${f.leaseDemands?.length ? f.leaseDemands.join(',') : 'none'}`,
    `headTier=${f.headTier ?? 'n/a'}`,
    `headRank=${f.headRank ?? 'n/a'}`,
    `headScore=${fmt(f.headScore)}`,
    ...(f.reservation ? [`reservation=${f.reservation}`, `futileCause=${f.futileCause}`, `headCpu=${fmt(f.headCpu)}`] : []),
    `bias=${KNOWN_BIAS_NOTE}`,
  ].join(' ');
}

/**
 * Shared low-level writer for every broker-side log line, admission
 * decisions and BRAIN-249's head-block lines alike: append to a real file
 * under the broker's state root (alongside the other admission sidecars),
 * best-effort. The file is the one durable, greppable record. Never stderr:
 * a foreground `lane run` forwards the supervisor's stderr to its caller
 * (BRAIN-308), where it belongs to the child's own output, and an admission
 * line fires once per poll while a candidate waits. Never throws, since a
 * logging failure must never affect scheduling.
 */
export function writeBrokerLog(root, line) {
  try {
    fs.appendFileSync(paths(root).admissionLog, line);
  } catch {
    // best-effort — disk-full or similar must never affect scheduling, same tolerance as appendHistory in state.js
  }
}

/**
 * Write one admission-decision line. Pure telemetry — nothing reads it back
 * to make a decision — so the caller (scheduler.js) calls this AFTER
 * releasing the global lock, on both the admit and deny paths, never from
 * inside it.
 */
export function logAdmissionDecision(root, fields) {
  writeBrokerLog(root, `${formatAdmissionLog(fields)}\n`);
}

/**
 * BRAIN-249: one grep-able line (`lane-broker-head-block`) for a conflict-
 * blocked FIFO head, so a stall that never even reaches CPU/gate admission
 * evaluation (nothing is ever selected there — see logAdmissionDecision's
 * doc comment: that log line only fires once a candidate is chosen) still
 * leaves a trace. Written to the SAME admission-decisions.log via
 * writeBrokerLog above rather than a second sidecar file, so investigating a
 * stall and investigating a CPU-admission decision are both one `grep
 * admission-decisions.log` away.
 *
 * Bounded by state TRANSITIONS, never by poll: scheduler.js's
 * resolveHeadBlock only calls this when the phase actually changes for the
 * current head (a new head starts being blocked, its skip allowance becomes
 * exhausted, or the grace period lapses and backfill resumes) — never once
 * per poll, or a multi-hour stall would write one line per queued ticket per
 * poll forever.
 */
export function formatHeadBlockLog(f) {
  return [
    'lane-broker-head-block',
    `event=${f.event}`,
    `headId=${f.headId}`,
    ...(f.candidateId ? [`candidate=${f.candidateId}`] : []),
    `blockingLease=${f.blockingLeaseId}`,
    `blockingKey=${f.blockingKey}`,
    `skip=${f.skipCount}/${f.skipLimit}`,
    `graceMs=${f.graceMs}`,
    `blockedMs=${Math.round(f.blockedMs)}`,
  ].join(' ');
}

export function logHeadBlock(root, fields) {
  writeBrokerLog(root, `${formatHeadBlockLog(fields)}\n`);
}

/**
 * BRAIN-249 part 2: the same `lane-broker-head-block` grep token, transition-
 * only discipline, and log destination as formatHeadBlockLog/logHeadBlock
 * above, for the CAPACITY-blocked head case instead of the conflict case —
 * a head that doesn't conflict with anything held but simply doesn't fit
 * under capacity. Distinct fields, because there is no single blocking
 * lease to name here: the head is blocked by the SUM of everything
 * currently held, so this reports `headWeight`/`runningWeight`/`capacity`
 * instead of a blocking lease id/key. No `graceMs`/`blockedMs` fields —
 * see scheduler.js's resolveCapacityBlock for why this path is
 * deliberately NOT time-bounded the way the conflict path is.
 */
export function formatCapacityBlockLog(f) {
  return [
    'lane-broker-head-block',
    `event=${f.event}`,
    `headId=${f.headId}`,
    `headWeight=${f.headWeight}`,
    `runningWeight=${f.runningWeight}`,
    `capacity=${f.capacity}`,
    `skip=${f.skipCount}/${f.skipLimit}`,
  ].join(' ');
}

export function logCapacityBlock(root, fields) {
  writeBrokerLog(root, `${formatCapacityBlockLog(fields)}\n`);
}

/**
 * BRAIN-346: explicit events for resource backfill. `current=start:ok` in the admission log is
 * not an admission signal for these, so each is its own `lane-broker-head-block` line, written
 * only after the lease it describes has been published. `fields` are rendered as key=value pairs
 * in insertion order.
 */
export function formatResourceEventLog(event, fields) {
  return ['lane-broker-head-block', `event=${event}`, ...Object.entries(fields).map(([k, v]) => `${k}=${v}`)].join(' ');
}

export function logResourceEvent(root, event, fields) {
  writeBrokerLog(root, `${formatResourceEventLog(event, fields)}\n`);
}
