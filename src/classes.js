import fs from 'node:fs';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { paths } from './state.js';
import { readSchedulerFence } from './fairness.js';
import { classOf } from './allocation.js';
import { cpuBudgetCores, leaseResources, resolveTicketResources } from './resources.js';

/** BRAIN-321: per-machine, per-class reservation caps. `lane capabilities --json` advertises this string. */
export const CLASSES_CAPABILITY = 'classes/1';

/** The env var that carries a sim lease's `classEnforcement` (JSON) to the leased child. ONE name, here. */
export const CLASS_ENFORCEMENT_ENV = 'LANE_BROKER_CLASS_ENFORCEMENT';

/**
 * The global config's `classes` block, resolved. `valid: false` (a broken block, or a reload that failed while a block was
 * in force) makes the sim class ineligible: it never falls back to "no caps". An absent block is `mode: 'off'`, valid.
 */
export function resolveClasses(cfg, root) {
  if (cfg.classesInvalid !== undefined) {
    return { mode: 'off', valid: false, error: cfg.classesInvalid, configHash: null, stateRoot: root, test: null, sim: null };
  }
  const classes = cfg.classes;
  if (classes === undefined) return { mode: 'off', valid: true, configHash: null, stateRoot: root, test: null, sim: null };
  const test = { reserveCores: classes.test?.reserveCores ?? 0 };
  const sim = { capCores: classes.sim?.capCores ?? 0, maxTickets: classes.sim?.maxTickets ?? 0, capMemoryBytes: classes.sim?.capMemoryBytes ?? 0 };
  const canonical = JSON.stringify({ mode: classes.mode, test, sim });
  const configHash = crypto.createHash('sha256').update(canonical).digest('hex').slice(0, 16);
  return { mode: classes.mode, valid: true, configHash, stateRoot: root, test, sim };
}

/** The scheduler-generation marker (the BRAIN-430 fence's `migratedAt`) when this root has one, else the package version. */
export function schedulerGeneration(root) {
  if (readSchedulerFence(root).status === 'valid') {
    try {
      return `sched-v2@${JSON.parse(fs.readFileSync(paths(root).schedFence, 'utf8')).migratedAt}`;
    } catch {
      // fall through: the fence was valid a moment ago and is gone or torn now
    }
  }
  const pkg = JSON.parse(fs.readFileSync(fileURLToPath(new URL('../package.json', import.meta.url)), 'utf8'));
  return `pkg-${pkg.version}`;
}

/** What a granted sim lease records and its child sees: only ever non-null for an active, valid `classes` block. */
export function classEnforcementOf(classes, root) {
  if (classes.mode !== 'active' || !classes.valid) return null;
  return { mode: classes.mode, generation: schedulerGeneration(root), stateRoot: classes.stateRoot, configHash: classes.configHash };
}

/** Booked sim reservations: the granted claims of the live sim leases, never demand estimates or observed CPU. */
export function bookedSims(held, cfg) {
  const booked = { cores: 0, memoryBytes: 0, count: 0 };
  for (const lease of held) {
    if (classOf(lease) !== 'sim') continue;
    const r = leaseResources(lease, cfg);
    booked.cores += r.cpuCores;
    booked.memoryBytes += r.memoryBytes;
    booked.count += 1;
  }
  return booked;
}

/** The most CPU cores a further sim may book right now: under capCores and out of the test class's reserved cores. */
export function simCoreCeiling(classes, booked, host, cfg) {
  const B = cpuBudgetCores(host, cfg);
  return Math.min(classes.sim.capCores, B - classes.test.reserveCores) - booked.cores;
}

/** Why a sim granted exactly `grant` (`{cores, memoryBytes}`) would breach a cap, or null. */
export function simCapReason(classes, booked, grant, host, cfg) {
  if (booked.count >= classes.sim.maxTickets) return 'max-tickets';
  if (booked.cores + grant.cores > classes.sim.capCores) return 'cap-cores';
  if (booked.memoryBytes + grant.memoryBytes > classes.sim.capMemoryBytes) return 'cap-memory';
  if (grant.cores > simCoreCeiling(classes, booked, host, cfg)) return 'test-reserve';
  return null;
}

/**
 * The smallest grant this queued sim could ever get: its elastic floor, else its declared claim. A sim that breaches a cap
 * even at that is cap-ineligible, whatever else is running.
 */
export function smallestSimGrant(ticket, cfg) {
  const declared = resolveTicketResources({ weight: ticket.weight, cpuCores: ticket.resources?.cpuCores, memoryBytes: ticket.resources?.memoryBytes, defaultMemoryBytesPerWeight: cfg.defaultMemoryBytesPerWeight });
  const min = ticket.resources?.minCpuCores;
  return { cores: Number.isFinite(min) ? Math.min(declared.cpuCores, Math.ceil(min)) : declared.cpuCores, memoryBytes: declared.memoryBytes };
}

/** A lane that declares `classEnforcement: "required"` is only granted under live caps. */
export const isEnforcementRequired = (ticket) => ticket?.classEnforcement === 'required';

/** Whether the live caps are in force: an `active` mode and a config that loaded cleanly. */
export const enforcementAvailable = (classes) => classes.mode === 'active' && classes.valid;

/** Reasons that bar a ticket in every mode; any other reason is a cap verdict, enforced in `active` only. */
export const ALWAYS_ENFORCED_REASONS = new Set(['config-invalid', 'enforcement-unavailable']);

/**
 * `Map(ticketId -> reason)` for every queued ticket the class policy forbids to start: a `required` ticket without live caps
 * ('enforcement-unavailable'), any sim under a broken config ('config-invalid'), and a sim the caps refuse. The caller decides
 * which of these to enforce (the cap verdicts only in `active`; `shadow` just reports them).
 */
export function capDeniedSims(rawQueue, classes, held, host, cfg) {
  const denied = new Map();
  const booked = classes.valid && classes.mode !== 'off' ? bookedSims(held, cfg) : null;
  for (const t of rawQueue) {
    if (t === null) continue;
    if (isEnforcementRequired(t) && !enforcementAvailable(classes)) denied.set(t.id, 'enforcement-unavailable');
    else if (classOf(t) !== 'sim') continue;
    else if (!classes.valid) denied.set(t.id, 'config-invalid');
    else if (booked) {
      const reason = simCapReason(classes, booked, smallestSimGrant(t, cfg), host, cfg);
      if (reason) denied.set(t.id, reason);
    }
  }
  return denied;
}

/**
 * An elastic sim's claim, lowered to what the caps leave (whole cores, like every elastic grant) but never below its floor.
 * An immutable claim is never shrunk; a claim that cannot fit is returned as declared, and the final-grant check denies it.
 */
export function capSimClaim(ticket, classes, booked, host, cfg) {
  const min = ticket.resources?.minCpuCores;
  if (classOf(ticket) !== 'sim' || !Number.isFinite(min) || !classes.valid || classes.mode !== 'active') return ticket;
  const ceiling = Math.floor(simCoreCeiling(classes, booked, host, cfg));
  if (!(ticket.resources.cpuCores > ceiling) || ceiling < Math.ceil(min)) return ticket;
  const { minCpuCores, ...claim } = ticket.resources;
  return { ...ticket, resources: { ...claim, cpuCores: ceiling, ...(minCpuCores < ceiling ? { minCpuCores } : {}) } };
}
