import { paths, atomicWriteJson, readJsonSafe } from './state.js';
import { leaseSilence } from './observed.js';
import { cpuBudget, leaseDemand, freshObservedCores, writeBrokerLog, formatResourceEventLog } from './admission.js';
import { leaseCpuCores } from './resources.js';

/**
 * BRAIN-532 step 2: the CPU credit admission WOULD grant for held leases that sit CPU-idle (shadow mode: reported and
 * recorded, never applied). Step 3 flips `idleCreditMode` to 'active'.
 *
 * A lease is eligible once its observed interval CPU has stayed under `idleCreditBelowFraction` x its grant for
 * `idleCreditAfterMs` (observed.js's streak anchors, which a single busy reading drops: revocation is immediate, no
 * hysteresis). Its credit is what admission charges it (never above the grant) minus max(what it uses, its elastic
 * floor `resources.minCpuCores`). The total is capped at host cores minus the CPU budget, so a credited booking can
 * never exceed the machine. CPU only: memory is never credited.
 */
export function idleCreditCores(heldLeases, cpuSample, cfg, now = Date.now()) {
  const perLease = [];
  let raw = 0;
  if (cpuSample && Number.isFinite(cpuSample.cores) && cpuSample.cores > 0) {
    const budget = cpuBudget(cpuSample, cfg);
    for (const lease of heldLeases) {
      const grant = leaseCpuCores(lease);
      const interval = lease.cpuIntervalCores;
      const idle =
        Number.isFinite(grant) && grant > 0 && Number.isFinite(interval) && interval < cfg.idleCreditBelowFraction * grant && leaseSilence(lease, now, cfg).idleMs >= cfg.idleCreditAfterMs;
      if (!idle) continue;
      const floor = Number.isFinite(lease.resources?.minCpuCores) ? lease.resources.minCpuCores : 0;
      const used = Math.max(freshObservedCores(lease, now) ?? interval, interval, floor);
      const credit = Math.max(0, Math.min(grant, leaseDemand(lease, now, cfg, budget)) - used);
      if (credit > 0) {
        perLease.push({ ticketId: lease.id, credit });
        raw += credit;
      }
    }
    const cap = Math.max(0, cpuSample.cores - budget);
    return { total: Math.min(raw, cap), perLease };
  }
  return { total: 0, perLease };
}

/** Would a CPU-projection denial (`decision`, evaluateCpuAdmission's shape) be an admit with `credit.total` cores less booked? Memory is never credited. */
export function wouldAdmitWithCredit(decision, credit) {
  return decision.admit === false && decision.cpuReason === 'projected-over-budget' && ['ok', 'not-sampled'].includes(decision.memoryReason) && credit.total > 0 && decision.projectedBusy - credit.total <= decision.budget;
}

/**
 * Shadow bookkeeping for the queue head, one record in `idle-credit-shadow.json`: logged once per transition (would-admit
 * began, would-admit ended) and once when the head really starts after a would-admit (how much earlier it could have). Telemetry only.
 */
export function recordIdleCreditShadow(root, { ticket, wouldAdmit, creditTotal, started = false, now = Date.now() }) {
  try {
    const file = paths(root).idleCreditShadow;
    const prev = readJsonSafe(file);
    const open = prev?.ticketId === ticket.id && Number.isFinite(prev.since) ? prev : null;
    const event = (name, extra) => writeBrokerLog(root, `${formatResourceEventLog(name, { ticket: ticket.id, ...extra })}\n`);
    const waitedMs = Math.round(now - (ticket.createdAt ?? now));
    if (started) {
      if (!open) return;
      event('idle-credit-shadow-admit', { shadowWouldAdmit: true, creditTotal: open.creditTotal.toFixed(2), waitedMs, earlierMs: Math.round(now - open.since) });
      atomicWriteJson(file, {});
      return;
    }
    if (wouldAdmit && !open) {
      event('idle-credit-shadow', { shadowWouldAdmit: true, creditTotal: creditTotal.toFixed(2), waitedMs });
      atomicWriteJson(file, { ticketId: ticket.id, since: now, creditTotal });
    } else if (!wouldAdmit && open) {
      event('idle-credit-shadow', { shadowWouldAdmit: false, creditTotal: creditTotal.toFixed(2), waitedMs });
      atomicWriteJson(file, {});
    }
  } catch {
    // telemetry only: never fails admission
  }
}
