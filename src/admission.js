import fs from 'node:fs';
import { paths, atomicWriteJson, readJsonSafe, fingerprintOf } from './state.js';
import { sampleHostCpu } from './cpu.js';

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
 * leaseDemand = max(observed process-group CPU if available and fresh, the
 * cold-start estimate). `lease.observedCpuCores` is an OPTIONAL field —
 * phase 1 never writes it (see src/cpu.js's observedGroupCpuCores, which
 * exists but isn't wired into the lease lifecycle yet: "for now the
 * cold-start estimate from weight is enough"), so every existing lease file
 * on disk loads fine and falls through to the cold-start branch. When a
 * later phase starts populating it, this is the only function that needs
 * to change.
 */
export function leaseDemand(lease) {
  const cold = coldStartEstimate(lease.weight);
  const observed = lease.observedCpuCores;
  if (Number.isFinite(observed) && observed >= 0) return Math.max(observed, cold);
  return cold;
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
  if (
    !cpuSample ||
    cpuSample.stale ||
    !Number.isFinite(cpuSample.hostBusyCores) ||
    !Number.isFinite(cpuSample.cores) ||
    cpuSample.cores <= 0
  ) {
    return prev;
  }
  const busyPercent = (cpuSample.hostBusyCores / cpuSample.cores) * 100;
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
export const KNOWN_BIAS_NOTE = 'self-subtracted-when-observed';

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
export function evaluateCpuAdmission({ cpuSample, heldLeases, candidateWeight, cpuGateState, cooldownBlocked, cfg }) {
  if (!cpuSample || cpuSample.stale || !Number.isFinite(cpuSample.hostBusyCores)) {
    if (heldLeases.length === 0) {
      return { admit: true, reason: 'sample-unavailable-empty', externalBusy: null, projectedBusy: null, budget: null };
    }
    return { admit: false, reason: 'sample-unavailable-held', externalBusy: null, projectedBusy: null, budget: null };
  }

  const brokerObserved = heldLeases.reduce((sum, l) => {
    const observed = l.observedCpuCores;
    return sum + (Number.isFinite(observed) && observed >= 0 ? observed : 0);
  }, 0);
  const externalBusy = Math.max(0, cpuSample.hostBusyCores - brokerObserved);
  const reservedSum = heldLeases.reduce((sum, l) => sum + leaseDemand(l), 0);
  const candidateEstimate = coldStartEstimate(candidateWeight);
  const projectedBusy = externalBusy + reservedSum + candidateEstimate;
  const budget = (cfg.cpuAdmissionPercent / 100) * cpuSample.cores;

  if (cpuGateState.closed) {
    return { admit: false, reason: 'cpu-gate-closed', externalBusy, projectedBusy, budget };
  }
  if (cooldownBlocked) {
    return { admit: false, reason: 'cooldown', externalBusy, projectedBusy, budget };
  }
  if (projectedBusy > budget) {
    return { admit: false, reason: 'projected-over-budget', externalBusy, projectedBusy, budget };
  }
  return { admit: true, reason: 'ok', externalBusy, projectedBusy, budget };
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
 * identically to a missing sample and NEVER throws. Deliberately takes no
 * lock and does no gate/cooldown work. Its cpu-sample.json sidecar IS
 * shared, read-then-write state — see the correctness note on
 * sampleHostCpu in cpu.js for the residual race this deliberately accepts
 * (a monotonic-write guard, not a lock) and why: this is telemetry-only
 * while schedulerMode stays 'shadow', and the measured lock-hold cost of
 * including it inside the lock was not worth paying for that. Called
 * BEFORE tryStart acquires the global mutex.
 */
export function sampleCpuSafe(root, cpuSampler = sampleHostCpu) {
  try {
    return cpuSampler(root) || null;
  } catch {
    return null; // sampler failure: treated identically to a missing sample
  }
}

/**
 * Glue: update the CPU gate from an already-taken sample, check the
 * cooldown, and run the predicate above — everything the new rule needs for
 * one candidate on one poll. `cpuSample` must come from sampleCpuSafe()
 * called BEFORE the caller acquired the global lock (see its doc comment) —
 * this function only does the part that DOES need to be atomic with the
 * admission decision: the CPU gate's read-then-write of its shared,
 * consecutive-under counter. Caller must hold the global lock.
 *
 * The whole body is wrapped in try/catch (Codex review finding #1): every
 * read/write this touches is already best-effort internally, but this is a
 * deliberate second layer — NOTHING in the phase-1 CPU path may ever throw
 * out of here, because in shadow mode this must be completely unable to
 * affect what the current rule decides, and even in active mode a crash
 * here must degrade to the same fail-safe as a missing sample, never abort
 * tryStart (which would kill the caller's detached supervisor).
 */
export function evaluateNewAdmission(root, cfg, ticket, heldLeases, cpuSample) {
  try {
    const cpuGateState = sampleAndUpdateCpuGate(root, cfg, cpuSample);
    const blocked = cooldownActive(heldLeases, cfg);
    const result = evaluateCpuAdmission({
      cpuSample,
      heldLeases,
      candidateWeight: ticket.weight,
      cpuGateState,
      cooldownBlocked: blocked,
      cfg,
    });
    return {
      ...result,
      hostBusyCores: cpuSample ? cpuSample.hostBusyCores : null,
      cores: cpuSample ? cpuSample.cores : null,
      sampleStale: cpuSample ? cpuSample.stale : true,
      cpuGateClosed: cpuGateState.closed,
      cooldownBlocked: blocked,
    };
  } catch {
    return unavailableDecision(heldLeases, 'admission-error');
  }
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
    `projectedBusy=${fmt(f.projectedBusy)}`,
    `budget=${fmt(f.budget)}`,
    `cpuGate=${f.cpuGateClosed ? 'closed' : 'open'}`,
    `cooldown=${f.cooldownBlocked ? 'blocked' : 'clear'}`,
    `bias=${KNOWN_BIAS_NOTE}`,
  ].join(' ');
}

/**
 * Write one decision line. Codex review finding #2: production supervisors
 * are spawned with stdio: 'ignore' (run.js), so stderr-only logging is
 * silently discarded on every real run — shadow mode would have no readable
 * output at all. Append to a real file under the broker's state root
 * (alongside the other admission sidecars) in addition to stderr, both
 * best-effort: neither ever throws, since a logging failure must never
 * affect scheduling. Pure telemetry — nothing reads it back to make a
 * decision — so the caller (scheduler.js) calls this AFTER releasing the
 * global lock, on both the admit and deny paths, never from inside it.
 */
export function logAdmissionDecision(root, fields) {
  const line = `${formatAdmissionLog(fields)}\n`;
  try {
    process.stderr.write(line);
  } catch {
    // best-effort
  }
  try {
    fs.appendFileSync(paths(root).admissionLog, line);
  } catch {
    // best-effort — disk-full or similar must never affect scheduling, same tolerance as appendHistory in state.js
  }
}
